//! Hub-owned concrete capability runtimes over `botster-core` contracts.
//!
//! Core owns request, event, handle, and error shapes. The hub owns concrete
//! local policy: scope roots, plugin-data paths, exact grants, operation
//! limits, policy-gated HTTP execution, and plugin cleanup.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fs;
use std::io;
use std::path::{Component, Path, PathBuf};
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use botster_core::{
    Capability, CapabilityOperation, CapabilityOperationCompleted, CapabilityOperationFailure,
    CapabilityOperationId, CapabilityOperationResult, CapabilityResourceEvent,
    CapabilityResourceId, CapabilityRuntimeError, CapabilityRuntimeErrorKind,
    CapabilityRuntimeEvent, CapabilityRuntimeHandle, CapabilityRuntimeRequest, CapabilitySet,
    CapabilitySurface, CapabilityTimerEvent, FilesystemCapabilityGrant, FilesystemCapabilityLimits,
    FilesystemCapabilityPermissions, FilesystemCapabilityRequest, FilesystemCapabilityResult,
    FilesystemEntry, FilesystemEntryKind, FilesystemMetadata, FilesystemOperation,
    HttpCapabilityEndpointPolicy, HttpCapabilityResponse, HttpCapabilityRuntime,
    HttpCapabilityRuntimeConfig, HttpCapabilityTransport, HttpHeader, HttpTransportRequest,
    InMemoryWebSocketCapabilityRuntime, PluginCancellationToken, PluginCapabilityRuntime,
    PluginCleanupResult, PluginKey, PluginResourceKind, PluginResourceRef, PluginStoreBackend,
    PluginStoreCapabilityRequest, PluginStoreEntry, PluginStoreKey, PluginStoreLimits,
    PluginStoreOperation, PluginStoreRecord, PluginStoreResult, RequestId, ScopedRelativePath,
    TimerCapabilityRequest, WebSocketCapabilityRuntimeConfig, apply_plugin_store_merge_patch,
    plugin_store_payload_bytes,
};
use botster_core::{
    KeyedStore, MAX_RANGE_BYTES, MAX_RANGE_ITEMS, Namespace, RedbStore, StoreError, StoreOp,
};
use serde::{Deserialize, Serialize};

use crate::config::HubConfig;

const DEFAULT_FILESYSTEM_SCOPE: &str = "workspace";
const DEFAULT_CAPABILITY_EVENT_CAPACITY: usize = 256;
const DEFAULT_CAPABILITY_OPERATION_CAPACITY: usize = 128;
const DEFAULT_HTTP_TIMEOUT_MS: u64 = 5_000;
/// Plugin database file under the Hub data directory.
const PLUGIN_DB_FILE: &str = "plugin-db.redb";

/// Hub-owned concrete capability runtime.
pub struct HubCapabilityRuntime {
    grants: CapabilitySet,
    filesystem_grants: BTreeMap<String, HubFilesystemScope>,
    plugin_store: Arc<KeyedPluginStore>,
    plugin_store_limits: PluginStoreLimits,
    http: HttpCapabilityRuntime,
    websocket: InMemoryWebSocketCapabilityRuntime,
    timers: BTreeMap<CapabilityResourceId, HubTimer>,
    pending_events: BTreeMap<String, VecDeque<CapabilityRuntimeEvent>>,
    completions_sender: mpsc::Sender<HubCapabilityCompletion>,
    completions_receiver: mpsc::Receiver<HubCapabilityCompletion>,
    operation_capacity: usize,
    event_capacity: usize,
}

pub(crate) struct PreparedPluginStoreOperation {
    backend: Arc<KeyedPluginStore>,
    plugin_key: PluginKey,
    operation: PluginStoreOperation,
    limits: PluginStoreLimits,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "operation", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum PluginStoreBatchMutation {
    Set {
        key: PluginStoreKey,
        #[serde(default = "default_plugin_store_schema_version")]
        schema_version: u64,
        payload: serde_json::Value,
        expected_revision: u64,
    },
    Patch {
        key: PluginStoreKey,
        patch: serde_json::Value,
        expected_revision: u64,
    },
    Delete {
        key: PluginStoreKey,
        expected_revision: u64,
    },
}

impl PluginStoreBatchMutation {
    fn key(&self) -> &PluginStoreKey {
        match self {
            Self::Set { key, .. } | Self::Patch { key, .. } | Self::Delete { key, .. } => key,
        }
    }
}

fn default_plugin_store_schema_version() -> u64 {
    1
}

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "operation", rename_all = "snake_case")]
pub(crate) enum PluginStoreBatchMutationResult {
    Set { record: PluginStoreRecord },
    Patch { record: PluginStoreRecord },
    Delete { key: PluginStoreKey, revision: u64 },
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct PluginStoreBatchResult {
    pub(crate) ok: bool,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub(crate) results: Vec<PluginStoreBatchMutationResult>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) error_kind: Option<CapabilityRuntimeErrorKind>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) message: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) mutation_index: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) key: Option<PluginStoreKey>,
}

impl PluginStoreBatchResult {
    pub(crate) fn failure(
        error: CapabilityRuntimeError,
        mutation_index: Option<usize>,
        key: Option<PluginStoreKey>,
    ) -> Self {
        Self {
            ok: false,
            results: Vec::new(),
            error_kind: Some(error.kind),
            message: Some(error.message),
            mutation_index,
            key,
        }
    }

    fn success(results: Vec<PluginStoreBatchMutationResult>) -> Self {
        Self {
            ok: true,
            results,
            error_kind: None,
            message: None,
            mutation_index: None,
            key: None,
        }
    }
}

pub(crate) struct PreparedPluginStoreBatch {
    backend: Arc<KeyedPluginStore>,
    plugin_key: PluginKey,
    mutations: Vec<PluginStoreBatchMutation>,
    limits: PluginStoreLimits,
}

impl PreparedPluginStoreBatch {
    pub(crate) fn execute(self) -> PluginStoreBatchResult {
        self.backend
            .batch(&self.plugin_key, self.mutations, self.limits)
    }
}

impl PreparedPluginStoreOperation {
    pub(crate) fn execute(self) -> Result<PluginStoreResult, CapabilityRuntimeError> {
        execute_plugin_store(
            self.backend.as_ref(),
            &self.plugin_key,
            self.operation,
            self.limits,
        )
    }
}

impl HubCapabilityRuntime {
    /// Build the local concrete runtime from explicit hub config.
    ///
    /// # Errors
    /// Returns an error when the plugin database under the data directory
    /// cannot be opened.
    pub fn from_config(config: &HubConfig) -> Result<Self, CapabilityRuntimeError> {
        let plugin_store = Arc::new(KeyedPluginStore::open(
            &config.data_directory.join(PLUGIN_DB_FILE),
        )?);
        let grants = default_hub_capability_grants();
        let filesystem_grants = BTreeMap::from([(
            DEFAULT_FILESYSTEM_SCOPE.to_string(),
            HubFilesystemScope {
                root: config
                    .data_directory
                    .join("capability-scopes")
                    .join("workspace"),
                grant: FilesystemCapabilityGrant {
                    scope_id: DEFAULT_FILESYSTEM_SCOPE.to_string(),
                    permissions: FilesystemCapabilityPermissions {
                        read: true,
                        write: true,
                        list: true,
                        stat: true,
                        remove: true,
                    },
                    limits: Some(FilesystemCapabilityLimits {
                        max_read_bytes: Some(1024 * 1024),
                        max_write_bytes: Some(1024 * 1024),
                        max_list_entries: Some(1024),
                    }),
                },
            },
        )]);
        let endpoint_policy =
            HttpCapabilityEndpointPolicy::new(["http", "https"], ["localhost", "127.0.0.1"]);
        let http = HttpCapabilityRuntime::new(
            grants.clone(),
            endpoint_policy,
            HttpCapabilityRuntimeConfig::default(),
            Arc::new(RealHttpTransport::default()),
        );
        let websocket = InMemoryWebSocketCapabilityRuntime::new(
            WebSocketCapabilityRuntimeConfig::new(grants.clone(), 256, 256, 256),
        );
        let (completions_sender, completions_receiver) = mpsc::channel();

        Ok(Self {
            grants,
            filesystem_grants,
            plugin_store,
            plugin_store_limits: PluginStoreLimits::default(),
            http,
            websocket,
            timers: BTreeMap::new(),
            pending_events: BTreeMap::new(),
            completions_sender,
            completions_receiver,
            operation_capacity: DEFAULT_CAPABILITY_OPERATION_CAPACITY,
            event_capacity: DEFAULT_CAPABILITY_EVENT_CAPACITY,
        })
    }

    /// Return the exact scoped grants accepted by the local runtime.
    #[must_use]
    pub fn granted_capabilities(&self) -> &CapabilitySet {
        &self.grants
    }

    /// Return the current number of Hub-owned timer resources.
    ///
    /// This aggregate diagnostic deliberately omits owner and resource
    /// identities so lifecycle proof does not expose plugin-private state.
    #[must_use]
    pub fn active_timer_resource_count(&self) -> usize {
        self.timers.len()
    }

    /// Drain due timer events using a deterministic logical millisecond clock.
    pub fn drain_events_at(
        &mut self,
        plugin_key: &PluginKey,
        now_ms: u64,
    ) -> Result<Vec<CapabilityRuntimeEvent>, CapabilityRuntimeError> {
        self.enqueue_due_timers(plugin_key, now_ms)?;
        self.drain_events(plugin_key)
    }

    fn submit_filesystem(
        &mut self,
        request: CapabilityRuntimeRequest,
        filesystem: FilesystemCapabilityRequest,
    ) -> Result<CapabilityRuntimeHandle, CapabilityRuntimeError> {
        self.ensure_runtime_capacity(&request)?;
        let scope = self
            .filesystem_grants
            .get(&filesystem.scope_id)
            .ok_or_else(|| {
                CapabilityRuntimeError::new(
                    CapabilityRuntimeErrorKind::CapabilityDenied,
                    "filesystem scope is not granted by this hub",
                )
            })?;
        if !self.grants.contains(&request.required_capability()) {
            return Err(CapabilityRuntimeError::new(
                CapabilityRuntimeErrorKind::CapabilityDenied,
                "plugin lacks required filesystem scope capability",
            ));
        }
        if !filesystem.operation.path().is_scoped_relative() {
            return Err(CapabilityRuntimeError::new(
                CapabilityRuntimeErrorKind::InvalidRequest,
                "filesystem path must stay below its scope",
            ));
        }
        if !scope.grant.permissions.allows(&filesystem.operation) {
            return Err(CapabilityRuntimeError::new(
                CapabilityRuntimeErrorKind::CapabilityDenied,
                "filesystem operation is not permitted for this scope",
            ));
        }

        let operation_id = request.operation_id.clone();
        let plugin_key = request.plugin_key.clone();
        let resource = request.resource_ref(CapabilityResourceId(operation_id.0.clone()));
        let worker = FilesystemWorkerRequest {
            plugin_key: plugin_key.clone(),
            operation_id: operation_id.clone(),
            scope_root: scope.root.clone(),
            operation: filesystem.operation,
            limits: merge_filesystem_limits(filesystem.limits, scope.grant.limits.clone()),
        };
        let sender = self.completions_sender.clone();
        std::thread::Builder::new()
            .name("botster-hub-filesystem-capability".to_string())
            .spawn(move || {
                let result = execute_filesystem(worker);
                let _ = sender.send(HubCapabilityCompletion {
                    plugin_key,
                    operation_id,
                    result,
                });
            })
            .map_err(|error| {
                CapabilityRuntimeError::new(
                    CapabilityRuntimeErrorKind::RuntimeStopped,
                    format!("failed to start filesystem capability worker: {error}"),
                )
            })?;

        let required_capability = request.required_capability();
        Ok(CapabilityRuntimeHandle {
            plugin_key: request.plugin_key,
            operation_id: request.operation_id,
            resource: Some(resource),
            required_capability,
        })
    }

    fn submit_plugin_store(
        &mut self,
        request: CapabilityRuntimeRequest,
        store: PluginStoreCapabilityRequest,
    ) -> Result<CapabilityRuntimeHandle, CapabilityRuntimeError> {
        self.ensure_runtime_capacity(&request)?;
        let prepared = self.prepare_plugin_store(&request.plugin_key, store)?;

        let operation_id = request.operation_id.clone();
        let plugin_key = request.plugin_key.clone();
        let resource = request.resource_ref(CapabilityResourceId(operation_id.0.clone()));
        let sender = self.completions_sender.clone();
        std::thread::Builder::new()
            .name("botster-hub-plugin-store-capability".to_string())
            .spawn(move || {
                let result = prepared
                    .execute()
                    .map(CapabilityOperationResult::PluginStore);
                let _ = sender.send(HubCapabilityCompletion {
                    plugin_key,
                    operation_id,
                    result,
                });
            })
            .map_err(|error| {
                CapabilityRuntimeError::new(
                    CapabilityRuntimeErrorKind::RuntimeStopped,
                    format!("failed to start plugin-store capability worker: {error}"),
                )
            })?;

        let required_capability = request.required_capability();
        Ok(CapabilityRuntimeHandle {
            plugin_key: request.plugin_key,
            operation_id: request.operation_id,
            resource: Some(resource),
            required_capability,
        })
    }

    pub(crate) fn prepare_plugin_store(
        &self,
        plugin_key: &PluginKey,
        store: PluginStoreCapabilityRequest,
    ) -> Result<PreparedPluginStoreOperation, CapabilityRuntimeError> {
        self.ensure_plugin_namespace_grant(plugin_key, &store.namespace)?;
        validate_store_operation(&store.operation)?;

        Ok(PreparedPluginStoreOperation {
            backend: self.plugin_store.clone(),
            plugin_key: plugin_key.clone(),
            operation: store.operation,
            limits: self.plugin_store_limits,
        })
    }

    pub(crate) fn prepare_plugin_store_batch(
        &self,
        plugin_key: &PluginKey,
        namespace: &str,
        mutations: Vec<PluginStoreBatchMutation>,
    ) -> Result<PreparedPluginStoreBatch, CapabilityRuntimeError> {
        self.ensure_plugin_namespace_grant(plugin_key, namespace)?;

        Ok(PreparedPluginStoreBatch {
            backend: self.plugin_store.clone(),
            plugin_key: plugin_key.clone(),
            mutations,
            limits: self.plugin_store_limits,
        })
    }

    fn submit_timer(
        &mut self,
        request: CapabilityRuntimeRequest,
        timer: TimerCapabilityRequest,
    ) -> Result<CapabilityRuntimeHandle, CapabilityRuntimeError> {
        if !self.grants.contains(&request.required_capability()) {
            return Err(CapabilityRuntimeError::new(
                CapabilityRuntimeErrorKind::CapabilityDenied,
                "plugin lacks timer callback capability",
            ));
        }

        match timer {
            TimerCapabilityRequest::Once { delay_ms } => self.open_timer(request, delay_ms, None),
            TimerCapabilityRequest::Interval { interval_ms } => {
                self.open_timer(request, interval_ms, Some(interval_ms))
            }
            TimerCapabilityRequest::Cancel { resource_id } => {
                self.cancel_timer_resource(request, resource_id)
            }
        }
    }

    fn open_timer(
        &mut self,
        request: CapabilityRuntimeRequest,
        delay_ms: u64,
        interval_ms: Option<u64>,
    ) -> Result<CapabilityRuntimeHandle, CapabilityRuntimeError> {
        if delay_ms == 0 {
            return Err(CapabilityRuntimeError::new(
                CapabilityRuntimeErrorKind::InvalidRequest,
                "timer delay must be greater than zero",
            ));
        }

        let resource = request.resource_ref(CapabilityResourceId(format!(
            "timer-{}",
            request.operation_id.0
        )));
        let resource_id = CapabilityResourceId(resource.resource_id.clone());
        self.ensure_event_capacity(&request.plugin_key, 2)?;
        self.timers.insert(
            resource_id,
            HubTimer {
                plugin_key: request.plugin_key.clone(),
                resource: resource.clone(),
                next_fire_ms: delay_ms,
                interval_ms,
                sequence: 0,
            },
        );
        self.push_local_event(CapabilityRuntimeEvent::ResourceOpened(
            CapabilityResourceEvent {
                plugin_key: request.plugin_key.clone(),
                operation_id: request.operation_id.clone(),
                resource: resource.clone(),
            },
        ));
        self.push_local_event(CapabilityRuntimeEvent::Completed(
            CapabilityOperationCompleted {
                plugin_key: request.plugin_key.clone(),
                operation_id: request.operation_id.clone(),
                result: None,
            },
        ));

        let required_capability = request.required_capability();
        Ok(CapabilityRuntimeHandle {
            plugin_key: request.plugin_key,
            operation_id: request.operation_id,
            resource: Some(resource),
            required_capability,
        })
    }

    fn cancel_timer_resource(
        &mut self,
        request: CapabilityRuntimeRequest,
        resource_id: CapabilityResourceId,
    ) -> Result<CapabilityRuntimeHandle, CapabilityRuntimeError> {
        let timer = self.timers.remove(&resource_id).ok_or_else(|| {
            CapabilityRuntimeError::new(
                CapabilityRuntimeErrorKind::ResourceNotFound,
                "timer resource is not open",
            )
        })?;
        if timer.plugin_key != request.plugin_key {
            return Err(CapabilityRuntimeError::new(
                CapabilityRuntimeErrorKind::ResourceNotFound,
                "timer resource is not owned by this plugin",
            ));
        }
        self.ensure_event_capacity(&request.plugin_key, 2)?;
        self.push_local_event(CapabilityRuntimeEvent::ResourceReleased(
            CapabilityResourceEvent {
                plugin_key: request.plugin_key.clone(),
                operation_id: request.operation_id.clone(),
                resource: timer.resource.clone(),
            },
        ));
        self.push_local_event(CapabilityRuntimeEvent::Completed(
            CapabilityOperationCompleted {
                plugin_key: request.plugin_key.clone(),
                operation_id: request.operation_id.clone(),
                result: None,
            },
        ));

        let required_capability = request.required_capability();
        Ok(CapabilityRuntimeHandle {
            plugin_key: request.plugin_key,
            operation_id: request.operation_id,
            resource: Some(timer.resource),
            required_capability,
        })
    }

    fn enqueue_due_timers(
        &mut self,
        plugin_key: &PluginKey,
        now_ms: u64,
    ) -> Result<(), CapabilityRuntimeError> {
        let due = self
            .timers
            .iter()
            .filter(|(_, timer)| &timer.plugin_key == plugin_key && timer.next_fire_ms <= now_ms)
            .map(|(resource_id, _)| resource_id.clone())
            .collect::<Vec<_>>();
        self.ensure_event_capacity(plugin_key, due.len())?;

        for resource_id in due {
            let mut remove = false;
            let mut event = None;
            if let Some(timer) = self.timers.get_mut(&resource_id) {
                timer.sequence += 1;
                event = Some(CapabilityRuntimeEvent::TimerFired(CapabilityTimerEvent {
                    resource: timer.resource.clone(),
                    sequence: timer.sequence,
                }));
                if let Some(interval_ms) = timer.interval_ms {
                    timer.next_fire_ms = now_ms.saturating_add(interval_ms);
                } else {
                    remove = true;
                }
            }
            if let Some(event) = event {
                self.push_local_event(event);
            }
            if remove {
                self.timers.remove(&resource_id);
            }
        }

        Ok(())
    }

    fn ensure_runtime_capacity(
        &mut self,
        request: &CapabilityRuntimeRequest,
    ) -> Result<(), CapabilityRuntimeError> {
        self.drain_worker_completions()?;
        let queue_len = self.local_event_len(&request.plugin_key);
        if queue_len >= self.operation_capacity {
            if queue_len < self.event_capacity {
                self.push_local_event(CapabilityRuntimeEvent::Backpressure(
                    request.backpressure(self.operation_capacity, queue_len),
                ));
            }
            return Err(CapabilityRuntimeError::new(
                CapabilityRuntimeErrorKind::Backpressured,
                "hub capability runtime queue is at capacity",
            ));
        }
        Ok(())
    }

    fn ensure_event_capacity(
        &self,
        plugin_key: &PluginKey,
        additional: usize,
    ) -> Result<(), CapabilityRuntimeError> {
        if self.local_event_len(plugin_key).saturating_add(additional) > self.event_capacity {
            return Err(CapabilityRuntimeError::new(
                CapabilityRuntimeErrorKind::Backpressured,
                "hub capability runtime event queue is at capacity",
            ));
        }
        Ok(())
    }

    fn ensure_plugin_namespace_grant(
        &self,
        plugin_key: &PluginKey,
        namespace: &str,
    ) -> Result<(), CapabilityRuntimeError> {
        let required = scoped_capability(CapabilitySurface::PluginDb, namespace);
        if namespace != plugin_key.0 || !self.grants.contains(&required) {
            return Err(CapabilityRuntimeError::new(
                CapabilityRuntimeErrorKind::CapabilityDenied,
                "plugin-store namespace must exactly match the plugin key",
            ));
        }
        Ok(())
    }

    fn drain_worker_completions(&mut self) -> Result<(), CapabilityRuntimeError> {
        while let Ok(completion) = self.completions_receiver.try_recv() {
            self.ensure_event_capacity(&completion.plugin_key, 1)?;
            let event = match completion.result {
                Ok(result) => CapabilityRuntimeEvent::Completed(CapabilityOperationCompleted {
                    plugin_key: completion.plugin_key,
                    operation_id: completion.operation_id,
                    result: Some(result),
                }),
                Err(error) => CapabilityRuntimeEvent::Failed(CapabilityOperationFailure {
                    plugin_key: completion.plugin_key,
                    operation_id: completion.operation_id,
                    error_kind: error.kind,
                    reason: error.message,
                }),
            };
            self.push_local_event(event);
        }
        Ok(())
    }

    fn drain_local_events(
        &mut self,
        plugin_key: &PluginKey,
    ) -> Result<Vec<CapabilityRuntimeEvent>, CapabilityRuntimeError> {
        self.drain_worker_completions()?;
        Ok(self
            .pending_events
            .remove(&plugin_key.0)
            .map(|events| events.into_iter().collect())
            .unwrap_or_default())
    }

    fn local_event_len(&self, plugin_key: &PluginKey) -> usize {
        self.pending_events
            .get(&plugin_key.0)
            .map(VecDeque::len)
            .unwrap_or_default()
    }

    fn push_local_event(&mut self, event: CapabilityRuntimeEvent) {
        if let Some(plugin_key) = event_plugin_key(&event) {
            self.pending_events
                .entry(plugin_key.0)
                .or_default()
                .push_back(event);
        }
    }
}

impl PluginCapabilityRuntime for HubCapabilityRuntime {
    fn submit(
        &mut self,
        request: CapabilityRuntimeRequest,
    ) -> Result<CapabilityRuntimeHandle, CapabilityRuntimeError> {
        match request.operation.clone() {
            CapabilityOperation::Http(_) => self.http.submit(request),
            CapabilityOperation::WebSocket(_) => self.websocket.submit(request),
            CapabilityOperation::Watch(_) => Err(CapabilityRuntimeError::new(
                CapabilityRuntimeErrorKind::InvalidRequest,
                "file watch capability runtime is not enabled by this hub adapter",
            )),
            CapabilityOperation::Filesystem(filesystem) => {
                self.submit_filesystem(request, filesystem)
            }
            CapabilityOperation::PluginStore(store) => self.submit_plugin_store(request, store),
            CapabilityOperation::Timer(timer) => self.submit_timer(request, timer),
        }
    }

    fn cancel(
        &mut self,
        plugin_key: &PluginKey,
        operation_id: &CapabilityOperationId,
    ) -> Result<(), CapabilityRuntimeError> {
        self.http
            .cancel(plugin_key, operation_id)
            .or_else(|_| self.websocket.cancel(plugin_key, operation_id))
    }

    fn release_resource(
        &mut self,
        resource: PluginResourceRef,
    ) -> Result<(), CapabilityRuntimeError> {
        match resource.kind {
            PluginResourceKind::NetworkConnection => self.websocket.release_resource(resource),
            PluginResourceKind::Timer => self
                .cancel_timer_resource(
                    CapabilityRuntimeRequest {
                        plugin_key: resource.plugin_key.clone(),
                        operation_id: CapabilityOperationId(format!(
                            "release:{}",
                            resource.resource_id
                        )),
                        operation: CapabilityOperation::Timer(TimerCapabilityRequest::Cancel {
                            resource_id: CapabilityResourceId(resource.resource_id.clone()),
                        }),
                        timeout_ms: 1,
                        callback: None,
                    },
                    CapabilityResourceId(resource.resource_id.clone()),
                )
                .map(|_| ()),
            _ => Ok(()),
        }
    }

    fn drain_events(
        &mut self,
        plugin_key: &PluginKey,
    ) -> Result<Vec<CapabilityRuntimeEvent>, CapabilityRuntimeError> {
        let mut events = self.drain_local_events(plugin_key)?;
        events.extend(self.http.drain_events(plugin_key)?);
        events.extend(self.websocket.drain_events(plugin_key)?);
        Ok(events)
    }

    fn cleanup_plugin(
        &mut self,
        plugin_key: &PluginKey,
    ) -> Result<PluginCleanupResult, CapabilityRuntimeError> {
        let local_removed = self
            .timers
            .iter()
            .filter(|(_, timer)| &timer.plugin_key == plugin_key)
            .map(|(resource_id, timer)| (resource_id.clone(), timer.resource.clone()))
            .collect::<Vec<_>>();
        for (resource_id, _) in &local_removed {
            self.timers.remove(resource_id);
        }
        self.pending_events.remove(&plugin_key.0);

        let mut removed_resources = local_removed
            .into_iter()
            .map(|(_, resource)| resource)
            .collect::<Vec<_>>();
        let http_cleanup = self.http.cleanup_plugin(plugin_key)?;
        let websocket_cleanup = self.websocket.cleanup_plugin(plugin_key)?;
        removed_resources.extend(http_cleanup.removed_resources);
        removed_resources.extend(websocket_cleanup.removed_resources);

        Ok(PluginCleanupResult {
            request_id: RequestId(format!("capability-cleanup:{}", plugin_key.0)),
            plugin_key: plugin_key.clone(),
            removed_descriptors: Vec::new(),
            removed_resources,
        })
    }
}

#[derive(Clone)]
struct HubFilesystemScope {
    root: PathBuf,
    grant: FilesystemCapabilityGrant,
}

struct HubTimer {
    plugin_key: PluginKey,
    resource: PluginResourceRef,
    next_fire_ms: u64,
    interval_ms: Option<u64>,
    sequence: u64,
}

struct HubCapabilityCompletion {
    plugin_key: PluginKey,
    operation_id: CapabilityOperationId,
    result: Result<CapabilityOperationResult, CapabilityRuntimeError>,
}

struct FilesystemWorkerRequest {
    plugin_key: PluginKey,
    operation_id: CapabilityOperationId,
    scope_root: PathBuf,
    operation: FilesystemOperation,
    limits: Option<FilesystemCapabilityLimits>,
}

fn execute_filesystem(
    request: FilesystemWorkerRequest,
) -> Result<CapabilityOperationResult, CapabilityRuntimeError> {
    let result = match request.operation {
        FilesystemOperation::Read { path } => {
            let target = resolve_scoped_path(&request.scope_root, &path)?;
            let bytes = fs::read(&target).map_err(backend_error)?;
            if let Some(limit) = request.limits.and_then(|limits| limits.max_read_bytes)
                && bytes.len() as u64 > limit
            {
                return Err(CapabilityRuntimeError::new(
                    CapabilityRuntimeErrorKind::InvalidRequest,
                    "filesystem read exceeds configured limit",
                ));
            }
            FilesystemCapabilityResult::Read { path, bytes }
        }
        FilesystemOperation::Write { path, bytes } => {
            if let Some(limit) = request.limits.and_then(|limits| limits.max_write_bytes)
                && bytes.len() as u64 > limit
            {
                return Err(CapabilityRuntimeError::new(
                    CapabilityRuntimeErrorKind::InvalidRequest,
                    "filesystem write exceeds configured limit",
                ));
            }
            let target = resolve_scoped_path(&request.scope_root, &path)?;
            if let Some(parent) = target.parent() {
                fs::create_dir_all(parent).map_err(backend_error)?;
            }
            fs::write(&target, &bytes).map_err(backend_error)?;
            FilesystemCapabilityResult::Write {
                path,
                bytes_written: bytes.len() as u64,
                atomic: false,
            }
        }
        FilesystemOperation::List { path } => {
            let target = resolve_scoped_path(&request.scope_root, &path)?;
            let mut entries = fs::read_dir(&target)
                .map_err(backend_error)?
                .map(|entry| entry.map_err(backend_error).and_then(filesystem_entry))
                .collect::<Result<Vec<_>, _>>()?;
            entries.sort_by(|left, right| left.path.cmp(&right.path));
            if let Some(limit) = request.limits.and_then(|limits| limits.max_list_entries)
                && entries.len() as u64 > limit
            {
                return Err(CapabilityRuntimeError::new(
                    CapabilityRuntimeErrorKind::InvalidRequest,
                    "filesystem list exceeds configured limit",
                ));
            }
            FilesystemCapabilityResult::List { path, entries }
        }
        FilesystemOperation::Stat { path } => {
            let target = resolve_scoped_path(&request.scope_root, &path)?;
            let metadata = fs::symlink_metadata(&target).map_err(backend_error)?;
            FilesystemCapabilityResult::Stat {
                path,
                metadata: metadata_for(&metadata),
            }
        }
        FilesystemOperation::Remove { path } => {
            let target = resolve_scoped_path(&request.scope_root, &path)?;
            let metadata = fs::symlink_metadata(&target).map_err(backend_error)?;
            if metadata.is_dir() {
                fs::remove_dir(&target).map_err(backend_error)?;
            } else {
                fs::remove_file(&target).map_err(backend_error)?;
            }
            FilesystemCapabilityResult::Remove { path }
        }
    };

    let _ = (request.plugin_key, request.operation_id);
    Ok(CapabilityOperationResult::Filesystem(result))
}

fn backend_error(error: std::io::Error) -> CapabilityRuntimeError {
    CapabilityRuntimeError::new(
        CapabilityRuntimeErrorKind::BackendFailed,
        format!("local capability backend failed: {error}"),
    )
}

fn filesystem_entry(entry: fs::DirEntry) -> Result<FilesystemEntry, CapabilityRuntimeError> {
    let metadata = entry.metadata().map_err(backend_error)?;
    let file_name = entry.file_name().to_string_lossy().to_string();
    Ok(FilesystemEntry {
        path: ScopedRelativePath(file_name),
        kind: metadata_kind(&metadata),
        size_bytes: metadata.is_file().then_some(metadata.len()),
    })
}

fn metadata_for(metadata: &fs::Metadata) -> FilesystemMetadata {
    FilesystemMetadata {
        kind: metadata_kind(metadata),
        size_bytes: metadata.is_file().then_some(metadata.len()),
        readonly: metadata.permissions().readonly(),
    }
}

fn metadata_kind(metadata: &fs::Metadata) -> FilesystemEntryKind {
    if metadata.is_file() {
        FilesystemEntryKind::File
    } else if metadata.is_dir() {
        FilesystemEntryKind::Directory
    } else if metadata.file_type().is_symlink() {
        FilesystemEntryKind::Symlink
    } else {
        FilesystemEntryKind::Other
    }
}

fn resolve_scoped_path(
    scope_root: &Path,
    path: &ScopedRelativePath,
) -> Result<PathBuf, CapabilityRuntimeError> {
    if !path.is_scoped_relative() {
        return Err(CapabilityRuntimeError::new(
            CapabilityRuntimeErrorKind::InvalidRequest,
            "filesystem path must stay below its scope",
        ));
    }
    let relative = Path::new(&path.0);
    if relative.components().any(|component| {
        matches!(
            component,
            Component::RootDir | Component::Prefix(_) | Component::ParentDir
        )
    }) {
        return Err(CapabilityRuntimeError::new(
            CapabilityRuntimeErrorKind::InvalidRequest,
            "filesystem path must stay below its scope",
        ));
    }
    Ok(scope_root.join(relative))
}

fn merge_filesystem_limits(
    request: Option<FilesystemCapabilityLimits>,
    grant: Option<FilesystemCapabilityLimits>,
) -> Option<FilesystemCapabilityLimits> {
    match (request, grant) {
        (None, None) => None,
        (Some(limits), None) | (None, Some(limits)) => Some(limits),
        (Some(request), Some(grant)) => Some(FilesystemCapabilityLimits {
            max_read_bytes: min_optional_limit(request.max_read_bytes, grant.max_read_bytes),
            max_write_bytes: min_optional_limit(request.max_write_bytes, grant.max_write_bytes),
            max_list_entries: min_optional_limit(request.max_list_entries, grant.max_list_entries),
        }),
    }
}

fn min_optional_limit(request: Option<u64>, grant: Option<u64>) -> Option<u64> {
    match (request, grant) {
        (Some(request), Some(grant)) => Some(request.min(grant)),
        (Some(limit), None) | (None, Some(limit)) => Some(limit),
        (None, None) => None,
    }
}

/// `plugin_db` records over Core's keyed store: one namespace per plugin,
/// one JSON [`PluginStoreRecord`] per key.
///
/// Every mutation is one atomic Core batch. Reads page the namespace with
/// Core's range bounds. Revision, quota, and merge-patch rules stay in the
/// Hub helpers below; Core owns durability and the byte limits of the store.
pub(crate) struct KeyedPluginStore {
    store: Box<dyn KeyedStore>,
    lock: Mutex<()>,
}

impl std::fmt::Debug for KeyedPluginStore {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("KeyedPluginStore")
            .finish_non_exhaustive()
    }
}

fn store_error(error: StoreError) -> CapabilityRuntimeError {
    CapabilityRuntimeError::new(
        CapabilityRuntimeErrorKind::BackendFailed,
        format!("plugin database failed: {error}"),
    )
}

impl KeyedPluginStore {
    fn open(path: &Path) -> Result<Self, CapabilityRuntimeError> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(|error| {
                CapabilityRuntimeError::new(
                    CapabilityRuntimeErrorKind::BackendFailed,
                    format!("plugin database directory could not be created: {error}"),
                )
            })?;
        }
        let store = RedbStore::open(path).map_err(store_error)?;
        Ok(Self::over(Box::new(store)))
    }

    fn over(store: Box<dyn KeyedStore>) -> Self {
        Self {
            store,
            lock: Mutex::new(()),
        }
    }

    /// Exact plugin database namespace for one plugin identity.
    ///
    /// Package admission bounds the name only by uniqueness and the reserved
    /// `hub` owner, so two admitted identities must never share a namespace.
    /// The encoding is injective; an identity too long for the namespace
    /// limit is a typed error, not a collision.
    fn namespace(plugin_key: &PluginKey) -> Result<Namespace, CapabilityRuntimeError> {
        Namespace::new(&plugin_namespace_text(plugin_key)).map_err(|error| {
            CapabilityRuntimeError::new(
                CapabilityRuntimeErrorKind::InvalidRequest,
                format!("plugin key is not a valid plugin database namespace: {error}"),
            )
        })
    }

    fn decode_record(bytes: &[u8]) -> Result<PluginStoreRecord, CapabilityRuntimeError> {
        serde_json::from_slice(bytes).map_err(|error| {
            CapabilityRuntimeError::new(
                CapabilityRuntimeErrorKind::BackendFailed,
                format!("plugin database record could not be decoded: {error}"),
            )
        })
    }

    fn encode_record(record: &PluginStoreRecord) -> Result<Vec<u8>, CapabilityRuntimeError> {
        serde_json::to_vec(record).map_err(|error| {
            CapabilityRuntimeError::new(
                CapabilityRuntimeErrorKind::BackendFailed,
                format!("plugin database record could not be encoded: {error}"),
            )
        })
    }

    fn read_record(
        &self,
        namespace: &Namespace,
        key: &PluginStoreKey,
    ) -> Result<Option<PluginStoreRecord>, CapabilityRuntimeError> {
        self.store
            .get(namespace, key.0.as_bytes())
            .map_err(store_error)?
            .map(|bytes| Self::decode_record(&bytes))
            .transpose()
    }

    /// Every record in one plugin namespace, in key order.
    fn read_records(
        &self,
        namespace: &Namespace,
    ) -> Result<BTreeMap<PluginStoreKey, PluginStoreRecord>, CapabilityRuntimeError> {
        let mut records = BTreeMap::new();
        let mut after: Option<Vec<u8>> = None;
        loop {
            let page = self
                .store
                .range(
                    namespace,
                    b"",
                    after.as_deref(),
                    MAX_RANGE_ITEMS,
                    MAX_RANGE_BYTES,
                )
                .map_err(store_error)?;
            let next = page.next_after().map(<[u8]>::to_vec);
            for item in page.items {
                let record = Self::decode_record(&item.value)?;
                records.insert(record.key.clone(), record);
            }
            match next {
                Some(next) => after = Some(next),
                None => return Ok(records),
            }
        }
    }

    fn write_record(
        &self,
        namespace: &Namespace,
        record: &PluginStoreRecord,
    ) -> Result<(), CapabilityRuntimeError> {
        self.store
            .batch(
                namespace,
                &[StoreOp::Put {
                    key: record.key.0.as_bytes().to_vec(),
                    value: Self::encode_record(record)?,
                }],
            )
            .map_err(store_error)
    }

    fn batch(
        &self,
        plugin_key: &PluginKey,
        mutations: Vec<PluginStoreBatchMutation>,
        limits: PluginStoreLimits,
    ) -> PluginStoreBatchResult {
        let _guard = self.lock.lock().expect("plugin store lock poisoned");
        let namespace = match Self::namespace(plugin_key) {
            Ok(namespace) => namespace,
            Err(error) => return PluginStoreBatchResult::failure(error, None, None),
        };
        let records = match self.read_records(&namespace) {
            Ok(records) => records,
            Err(error) => return PluginStoreBatchResult::failure(error, None, None),
        };
        let (candidate, results) =
            match apply_plugin_store_batch(plugin_key, records, &mutations, limits) {
                Ok(candidate) => candidate,
                Err(failure) => return failure,
            };
        let mut ops = Vec::with_capacity(mutations.len());
        for mutation in &mutations {
            let key = mutation.key();
            let op = match candidate.get(key) {
                Some(record) => match Self::encode_record(record) {
                    Ok(value) => StoreOp::Put {
                        key: key.0.as_bytes().to_vec(),
                        value,
                    },
                    Err(error) => return PluginStoreBatchResult::failure(error, None, None),
                },
                None => StoreOp::Delete {
                    key: key.0.as_bytes().to_vec(),
                },
            };
            ops.push(op);
        }
        match self.store.batch(&namespace, &ops) {
            Ok(()) => PluginStoreBatchResult::success(results),
            Err(error) => PluginStoreBatchResult::failure(store_error(error), None, None),
        }
    }
}

impl PluginStoreBackend for KeyedPluginStore {
    fn get(
        &self,
        plugin_key: &PluginKey,
        key: &PluginStoreKey,
    ) -> Result<Option<PluginStoreRecord>, CapabilityRuntimeError> {
        let _guard = self.lock.lock().expect("plugin store lock poisoned");
        let namespace = Self::namespace(plugin_key)?;
        self.read_record(&namespace, key)
    }

    fn set(
        &self,
        plugin_key: &PluginKey,
        key: PluginStoreKey,
        schema_version: u64,
        payload: serde_json::Value,
        expected_revision: Option<u64>,
        limits: PluginStoreLimits,
    ) -> Result<PluginStoreRecord, CapabilityRuntimeError> {
        let _guard = self.lock.lock().expect("plugin store lock poisoned");
        let namespace = Self::namespace(plugin_key)?;
        let records = self.read_records(&namespace)?;
        let revision = revision_for_write(records.get(&key), expected_revision)?;
        enforce_plugin_store_limits(&records, &key, &payload, limits)?;
        let record = PluginStoreRecord {
            plugin_key: plugin_key.clone(),
            key,
            schema_version,
            revision,
            payload,
        };
        self.write_record(&namespace, &record)?;
        Ok(record)
    }

    fn delete(
        &self,
        plugin_key: &PluginKey,
        key: &PluginStoreKey,
    ) -> Result<PluginStoreRecord, CapabilityRuntimeError> {
        let _guard = self.lock.lock().expect("plugin store lock poisoned");
        let namespace = Self::namespace(plugin_key)?;
        let record = self.read_record(&namespace, key)?.ok_or_else(|| {
            CapabilityRuntimeError::new(
                CapabilityRuntimeErrorKind::StoreNotFound,
                "plugin-store record was not found",
            )
        })?;
        self.store
            .batch(
                &namespace,
                &[StoreOp::Delete {
                    key: key.0.as_bytes().to_vec(),
                }],
            )
            .map_err(store_error)?;
        Ok(record)
    }

    fn list(
        &self,
        plugin_key: &PluginKey,
        prefix: Option<&str>,
    ) -> Result<Vec<PluginStoreEntry>, CapabilityRuntimeError> {
        let _guard = self.lock.lock().expect("plugin store lock poisoned");
        let namespace = Self::namespace(plugin_key)?;
        Ok(self
            .read_records(&namespace)?
            .values()
            .filter(|record| {
                prefix
                    .map(|prefix| record.key.0.starts_with(prefix))
                    .unwrap_or(true)
            })
            .map(PluginStoreEntry::from)
            .collect())
    }

    fn patch(
        &self,
        plugin_key: &PluginKey,
        key: &PluginStoreKey,
        patch: serde_json::Value,
        expected_revision: Option<u64>,
        limits: PluginStoreLimits,
    ) -> Result<PluginStoreRecord, CapabilityRuntimeError> {
        let _guard = self.lock.lock().expect("plugin store lock poisoned");
        let namespace = Self::namespace(plugin_key)?;
        let records = self.read_records(&namespace)?;
        let current = records.get(key).cloned().ok_or_else(|| {
            CapabilityRuntimeError::new(
                CapabilityRuntimeErrorKind::StoreNotFound,
                "plugin-store record was not found",
            )
        })?;
        let revision = revision_for_write(Some(&current), expected_revision)?;
        let mut payload = current.payload.clone();
        apply_plugin_store_merge_patch(&mut payload, &patch)?;
        enforce_plugin_store_limits(&records, key, &payload, limits)?;
        let record = PluginStoreRecord {
            revision,
            payload,
            ..current
        };
        self.write_record(&namespace, &record)?;
        Ok(record)
    }
}

fn apply_plugin_store_batch(
    plugin_key: &PluginKey,
    mut records: BTreeMap<PluginStoreKey, PluginStoreRecord>,
    mutations: &[PluginStoreBatchMutation],
    limits: PluginStoreLimits,
) -> Result<
    (
        BTreeMap<PluginStoreKey, PluginStoreRecord>,
        Vec<PluginStoreBatchMutationResult>,
    ),
    PluginStoreBatchResult,
> {
    if mutations.is_empty() {
        return Err(PluginStoreBatchResult::failure(
            CapabilityRuntimeError::new(
                CapabilityRuntimeErrorKind::InvalidRequest,
                "plugin-store batch requires at least one mutation",
            ),
            None,
            None,
        ));
    }
    let mut seen = BTreeSet::new();
    let mut results = Vec::with_capacity(mutations.len());
    for (index, mutation) in mutations.iter().enumerate() {
        let key = mutation.key();
        if !key.is_valid() {
            return Err(batch_mutation_failure(
                CapabilityRuntimeError::new(
                    CapabilityRuntimeErrorKind::InvalidRequest,
                    "plugin-store key is invalid",
                ),
                index,
                key,
            ));
        }
        if !seen.insert(key.clone()) {
            return Err(batch_mutation_failure(
                CapabilityRuntimeError::new(
                    CapabilityRuntimeErrorKind::InvalidRequest,
                    "plugin-store batch contains duplicate keys",
                ),
                index,
                key,
            ));
        }

        let result = match mutation {
            PluginStoreBatchMutation::Set {
                key,
                schema_version,
                payload,
                expected_revision,
            } => {
                let revision = revision_for_write(records.get(key), Some(*expected_revision))
                    .map_err(|error| batch_mutation_failure(error, index, key))?;
                let record = PluginStoreRecord {
                    plugin_key: plugin_key.clone(),
                    key: key.clone(),
                    schema_version: *schema_version,
                    revision,
                    payload: payload.clone(),
                };
                records.insert(key.clone(), record.clone());
                PluginStoreBatchMutationResult::Set { record }
            }
            PluginStoreBatchMutation::Patch {
                key,
                patch,
                expected_revision,
            } => {
                let current = records.get(key).cloned().ok_or_else(|| {
                    batch_mutation_failure(
                        CapabilityRuntimeError::new(
                            CapabilityRuntimeErrorKind::StoreNotFound,
                            "plugin-store record was not found",
                        ),
                        index,
                        key,
                    )
                })?;
                let revision = revision_for_write(Some(&current), Some(*expected_revision))
                    .map_err(|error| batch_mutation_failure(error, index, key))?;
                let mut payload = current.payload.clone();
                apply_plugin_store_merge_patch(&mut payload, patch)
                    .map_err(|error| batch_mutation_failure(error, index, key))?;
                let record = PluginStoreRecord {
                    revision,
                    payload,
                    ..current
                };
                records.insert(key.clone(), record.clone());
                PluginStoreBatchMutationResult::Patch { record }
            }
            PluginStoreBatchMutation::Delete {
                key,
                expected_revision,
            } => {
                let current = records.get(key).cloned().ok_or_else(|| {
                    batch_mutation_failure(
                        CapabilityRuntimeError::new(
                            CapabilityRuntimeErrorKind::StoreNotFound,
                            "plugin-store record was not found",
                        ),
                        index,
                        key,
                    )
                })?;
                revision_for_write(Some(&current), Some(*expected_revision))
                    .map_err(|error| batch_mutation_failure(error, index, key))?;
                records.remove(key);
                PluginStoreBatchMutationResult::Delete {
                    key: key.clone(),
                    revision: current.revision,
                }
            }
        };
        let written_record = match &result {
            PluginStoreBatchMutationResult::Set { record }
            | PluginStoreBatchMutationResult::Patch { record } => Some(record),
            PluginStoreBatchMutationResult::Delete { .. } => None,
        };
        if written_record.is_some_and(|record| record.payload_bytes() > limits.max_record_bytes) {
            return Err(batch_mutation_failure(
                CapabilityRuntimeError::new(
                    CapabilityRuntimeErrorKind::QuotaExceeded,
                    "plugin-store record exceeds max_record_bytes",
                ),
                index,
                key,
            ));
        }
        results.push(result);
    }

    enforce_plugin_store_snapshot_limits(&records, limits)
        .map_err(|error| PluginStoreBatchResult::failure(error, None, None))?;
    Ok((records, results))
}

fn batch_mutation_failure(
    error: CapabilityRuntimeError,
    index: usize,
    key: &PluginStoreKey,
) -> PluginStoreBatchResult {
    PluginStoreBatchResult::failure(error, Some(index + 1), Some(key.clone()))
}

fn enforce_plugin_store_snapshot_limits(
    records: &BTreeMap<PluginStoreKey, PluginStoreRecord>,
    limits: PluginStoreLimits,
) -> Result<(), CapabilityRuntimeError> {
    if records.len() > limits.max_plugin_keys {
        return Err(CapabilityRuntimeError::new(
            CapabilityRuntimeErrorKind::QuotaExceeded,
            "plugin-store namespace exceeds max_plugin_keys",
        ));
    }
    if records
        .values()
        .map(PluginStoreRecord::payload_bytes)
        .sum::<usize>()
        > limits.max_plugin_bytes
    {
        return Err(CapabilityRuntimeError::new(
            CapabilityRuntimeErrorKind::QuotaExceeded,
            "plugin-store namespace exceeds max_plugin_bytes",
        ));
    }
    Ok(())
}

fn execute_plugin_store(
    backend: &dyn PluginStoreBackend,
    plugin_key: &PluginKey,
    operation: PluginStoreOperation,
    limits: PluginStoreLimits,
) -> Result<PluginStoreResult, CapabilityRuntimeError> {
    match operation {
        PluginStoreOperation::Get { key } => backend
            .get(plugin_key, &key)?
            .map(|record| PluginStoreResult::Record { record })
            .ok_or_else(|| {
                CapabilityRuntimeError::new(
                    CapabilityRuntimeErrorKind::StoreNotFound,
                    "plugin-store record was not found",
                )
            }),
        PluginStoreOperation::Set {
            key,
            schema_version,
            payload,
            expected_revision,
        } => backend
            .set(
                plugin_key,
                key,
                schema_version,
                payload,
                expected_revision,
                limits,
            )
            .map(|record| PluginStoreResult::Written { record }),
        PluginStoreOperation::Delete { key } => {
            backend
                .delete(plugin_key, &key)
                .map(|record| PluginStoreResult::Deleted {
                    key: record.key,
                    revision: record.revision,
                })
        }
        PluginStoreOperation::List { prefix } => backend
            .list(plugin_key, prefix.as_deref())
            .map(|entries| PluginStoreResult::List { entries }),
        PluginStoreOperation::Patch {
            key,
            patch,
            expected_revision,
        } => backend
            .patch(plugin_key, &key, patch, expected_revision, limits)
            .map(|record| PluginStoreResult::Written { record }),
    }
}

fn validate_store_operation(
    operation: &PluginStoreOperation,
) -> Result<(), CapabilityRuntimeError> {
    let key = match operation {
        PluginStoreOperation::Get { key }
        | PluginStoreOperation::Set { key, .. }
        | PluginStoreOperation::Delete { key }
        | PluginStoreOperation::Patch { key, .. } => Some(key),
        PluginStoreOperation::List { .. } => None,
    };
    if let Some(key) = key
        && !key.is_valid()
    {
        return Err(CapabilityRuntimeError::new(
            CapabilityRuntimeErrorKind::InvalidRequest,
            "plugin-store key is invalid",
        ));
    }
    Ok(())
}

fn revision_for_write(
    current: Option<&PluginStoreRecord>,
    expected_revision: Option<u64>,
) -> Result<u64, CapabilityRuntimeError> {
    match (current, expected_revision) {
        (Some(record), Some(expected)) if record.revision != expected => {
            Err(CapabilityRuntimeError::new(
                CapabilityRuntimeErrorKind::RevisionConflict,
                "plugin-store revision did not match expected revision",
            ))
        }
        (None, Some(expected)) if expected != 0 => Err(CapabilityRuntimeError::new(
            CapabilityRuntimeErrorKind::RevisionConflict,
            "plugin-store create expected revision must be 0",
        )),
        (Some(record), _) => Ok(record.revision + 1),
        (None, _) => Ok(1),
    }
}

fn enforce_plugin_store_limits(
    records: &BTreeMap<PluginStoreKey, PluginStoreRecord>,
    key: &PluginStoreKey,
    replacement_payload: &serde_json::Value,
    limits: PluginStoreLimits,
) -> Result<(), CapabilityRuntimeError> {
    let replacement_bytes = plugin_store_payload_bytes(replacement_payload);
    if replacement_bytes > limits.max_record_bytes {
        return Err(CapabilityRuntimeError::new(
            CapabilityRuntimeErrorKind::QuotaExceeded,
            "plugin-store record exceeds max_record_bytes",
        ));
    }
    if !records.contains_key(key) && records.len() + 1 > limits.max_plugin_keys {
        return Err(CapabilityRuntimeError::new(
            CapabilityRuntimeErrorKind::QuotaExceeded,
            "plugin-store namespace exceeds max_plugin_keys",
        ));
    }
    let current_bytes = records
        .iter()
        .filter(|(record_key, _)| *record_key != key)
        .map(|(_, record)| record.payload_bytes())
        .sum::<usize>();
    if current_bytes.saturating_add(replacement_bytes) > limits.max_plugin_bytes {
        return Err(CapabilityRuntimeError::new(
            CapabilityRuntimeErrorKind::QuotaExceeded,
            "plugin-store namespace exceeds max_plugin_bytes",
        ));
    }
    Ok(())
}

struct RealHttpTransport {
    agent: ureq::Agent,
    policy: HubHttpTransportPolicy,
}

impl Default for RealHttpTransport {
    fn default() -> Self {
        let timeout = Duration::from_millis(DEFAULT_HTTP_TIMEOUT_MS);
        let agent = ureq::Agent::config_builder()
            .http_status_as_error(false)
            .max_redirects(0)
            .proxy(None)
            .timeout_global(Some(timeout))
            .timeout_connect(Some(timeout))
            .timeout_recv_response(Some(timeout))
            .timeout_recv_body(Some(timeout))
            .build()
            .into();
        Self {
            agent,
            policy: HubHttpTransportPolicy::default(),
        }
    }
}

impl HttpCapabilityTransport for RealHttpTransport {
    fn execute(
        &self,
        request: HttpTransportRequest,
        cancellation: PluginCancellationToken,
    ) -> Result<HttpCapabilityResponse, CapabilityRuntimeError> {
        self.policy.validate_request(&request)?;
        if cancellation.is_cancelled() {
            return Err(CapabilityRuntimeError::new(
                CapabilityRuntimeErrorKind::Cancelled,
                "HTTP capability operation was cancelled",
            ));
        }

        let mut builder = ureq::http::Request::builder()
            .method(request_method(&request)?)
            .uri(request_endpoint(&request));
        for header in request_headers(&request) {
            builder = builder.header(&header.name, &header.value);
        }
        let http_request = builder.body(request_body(&request)).map_err(|_| {
            CapabilityRuntimeError::new(
                CapabilityRuntimeErrorKind::InvalidRequest,
                "HTTP request could not be built from admitted capability input",
            )
        })?;

        let mut response = self.agent.run(http_request).map_err(transport_error)?;
        if cancellation.is_cancelled() {
            return Err(CapabilityRuntimeError::new(
                CapabilityRuntimeErrorKind::Cancelled,
                "HTTP capability operation was cancelled",
            ));
        }

        let status = response.status().as_u16();
        let headers = response_headers(&response)?;
        let body = response
            .body_mut()
            .with_config()
            .limit(request.max_response_body_bytes.saturating_add(1) as u64)
            .read_to_vec()
            .map_err(transport_error)?;
        let response = HttpCapabilityResponse {
            status,
            headers,
            body,
        };
        HttpCapabilityRuntime::validate_response(
            &HttpCapabilityRuntimeConfig {
                max_response_body_bytes: request.max_response_body_bytes,
                max_header_count: request.max_header_count,
                max_header_name_bytes: request.max_header_name_bytes,
                max_header_value_bytes: request.max_header_value_bytes,
                ..HttpCapabilityRuntimeConfig::default()
            },
            &response,
        )?;
        Ok(response)
    }
}

struct HubHttpTransportPolicy {
    allowed_methods: BTreeSet<&'static str>,
    allowed_request_headers: BTreeSet<&'static str>,
    denied_sensitive_headers: BTreeSet<&'static str>,
}

impl Default for HubHttpTransportPolicy {
    fn default() -> Self {
        Self {
            allowed_methods: BTreeSet::from(["GET", "POST"]),
            allowed_request_headers: BTreeSet::from(["accept", "content-type", "user-agent"]),
            denied_sensitive_headers: BTreeSet::from([
                "authorization",
                "cookie",
                "proxy-authorization",
                "set-cookie",
            ]),
        }
    }
}

impl HubHttpTransportPolicy {
    fn validate_request(
        &self,
        request: &HttpTransportRequest,
    ) -> Result<(), CapabilityRuntimeError> {
        let method = request_method_text(request);
        if !self.allowed_methods.contains(method.as_str()) {
            return Err(CapabilityRuntimeError::new(
                CapabilityRuntimeErrorKind::CapabilityDenied,
                "HTTP method is not allowed by this hub",
            ));
        }

        for header in request_headers(request) {
            let name = header.name.to_ascii_lowercase();
            if self.denied_sensitive_headers.contains(name.as_str()) {
                return Err(CapabilityRuntimeError::new(
                    CapabilityRuntimeErrorKind::CapabilityDenied,
                    "HTTP request header is not allowed by this hub",
                ));
            }
            if !self.allowed_request_headers.contains(name.as_str()) {
                return Err(CapabilityRuntimeError::new(
                    CapabilityRuntimeErrorKind::CapabilityDenied,
                    "HTTP request header is not allowed by this hub",
                ));
            }
        }

        Ok(())
    }
}

fn request_method(
    request: &HttpTransportRequest,
) -> Result<ureq::http::Method, CapabilityRuntimeError> {
    request_method_text(request).parse().map_err(|_| {
        CapabilityRuntimeError::new(
            CapabilityRuntimeErrorKind::InvalidRequest,
            "HTTP method could not be parsed after admission",
        )
    })
}

fn request_method_text(request: &HttpTransportRequest) -> String {
    let CapabilityOperation::Http(http) = &request.runtime_request.operation else {
        return String::new();
    };
    http.method.trim().to_ascii_uppercase()
}

fn request_endpoint(request: &HttpTransportRequest) -> &str {
    let CapabilityOperation::Http(http) = &request.runtime_request.operation else {
        return "";
    };
    http.endpoint.as_str()
}

fn request_headers(request: &HttpTransportRequest) -> &[HttpHeader] {
    let CapabilityOperation::Http(http) = &request.runtime_request.operation else {
        return &[];
    };
    http.headers.as_slice()
}

fn request_body(request: &HttpTransportRequest) -> Vec<u8> {
    let CapabilityOperation::Http(http) = &request.runtime_request.operation else {
        return Vec::new();
    };
    http.body.clone()
}

fn response_headers(
    response: &ureq::http::Response<ureq::Body>,
) -> Result<Vec<HttpHeader>, CapabilityRuntimeError> {
    response
        .headers()
        .iter()
        .map(|(name, value)| {
            let value = value.to_str().map_err(|_| {
                CapabilityRuntimeError::new(
                    CapabilityRuntimeErrorKind::InvalidRequest,
                    "HTTP response header value is not valid text",
                )
            })?;
            Ok(HttpHeader {
                name: name.as_str().to_string(),
                value: value.to_string(),
            })
        })
        .collect()
}

fn transport_error(error: ureq::Error) -> CapabilityRuntimeError {
    let kind = match error {
        ureq::Error::Timeout(_) => CapabilityRuntimeErrorKind::TimedOut,
        ureq::Error::BodyExceedsLimit(_) => CapabilityRuntimeErrorKind::InvalidRequest,
        _ => CapabilityRuntimeErrorKind::BackendFailed,
    };
    CapabilityRuntimeError::new(kind, sanitized_transport_error(error))
}

fn sanitized_transport_error(error: ureq::Error) -> String {
    match error {
        ureq::Error::Timeout(_) => "HTTP request timed out".to_string(),
        ureq::Error::HostNotFound => "HTTP host could not be resolved".to_string(),
        ureq::Error::ConnectionFailed => "HTTP connection failed".to_string(),
        ureq::Error::BodyExceedsLimit(_) => {
            "HTTP response body exceeds configured limit".to_string()
        }
        ureq::Error::Io(error) => sanitized_io_error(error),
        _ => "HTTP transport failed".to_string(),
    }
}

fn sanitized_io_error(error: io::Error) -> String {
    match error.kind() {
        io::ErrorKind::TimedOut => "HTTP request timed out".to_string(),
        io::ErrorKind::ConnectionRefused => "HTTP connection refused".to_string(),
        io::ErrorKind::ConnectionReset => "HTTP connection reset".to_string(),
        io::ErrorKind::ConnectionAborted => "HTTP connection aborted".to_string(),
        io::ErrorKind::NotConnected => "HTTP connection was not established".to_string(),
        io::ErrorKind::UnexpectedEof => {
            "HTTP connection closed before response completed".to_string()
        }
        _ => "HTTP transport I/O failed".to_string(),
    }
}

/// Injective namespace text for one plugin identity.
///
/// Lowercase ASCII letters, digits, and `-` pass through. Every other byte of
/// the UTF-8 identity is written as `_` followed by two lowercase hex digits,
/// including `_` itself, so no two identities produce the same text and the
/// output uses only characters the Core namespace accepts. The text never
/// starts with `.` because `.` is always escaped.
fn plugin_namespace_text(plugin_key: &PluginKey) -> String {
    let mut text = String::with_capacity(plugin_key.0.len());
    for byte in plugin_key.0.bytes() {
        if byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-' {
            text.push(char::from(byte));
        } else {
            text.push_str(&format!("_{byte:02x}"));
        }
    }
    text
}

fn scoped_capability(surface: CapabilitySurface, scope: impl Into<String>) -> Capability {
    Capability {
        surface,
        scope: Some(scope.into()),
    }
}

fn default_hub_capability_grants() -> CapabilitySet {
    BTreeSet::from([
        scoped_capability(CapabilitySurface::Network, "http"),
        scoped_capability(CapabilitySurface::Network, "websocket"),
        scoped_capability(CapabilitySurface::Filesystem, DEFAULT_FILESYSTEM_SCOPE),
        scoped_capability(CapabilitySurface::PluginDb, "project-pipelines"),
        scoped_capability(CapabilitySurface::PluginDb, "botster-workspaces"),
        scoped_capability(CapabilitySurface::Timers, "callbacks"),
    ])
}

fn event_plugin_key(event: &CapabilityRuntimeEvent) -> Option<PluginKey> {
    match event {
        CapabilityRuntimeEvent::Completed(event) => Some(event.plugin_key.clone()),
        CapabilityRuntimeEvent::ResourceOpened(event)
        | CapabilityRuntimeEvent::ResourceReleased(event) => Some(event.plugin_key.clone()),
        CapabilityRuntimeEvent::WebSocketMessage(event) => Some(event.resource.plugin_key.clone()),
        CapabilityRuntimeEvent::Watch(event) => Some(event.resource.plugin_key.clone()),
        CapabilityRuntimeEvent::TimerFired(event) => Some(event.resource.plugin_key.clone()),
        CapabilityRuntimeEvent::TimedOut(event)
        | CapabilityRuntimeEvent::Cancelled(event)
        | CapabilityRuntimeEvent::Failed(event) => Some(event.plugin_key.clone()),
        CapabilityRuntimeEvent::Backpressure(event) => event.route.plugin_key.clone(),
        CapabilityRuntimeEvent::CleanupCompleted(event) => Some(event.plugin_key.clone()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Admitted package names are bounded only by uniqueness and the reserved
    /// `hub` owner, so the namespace text must be injective.
    #[test]
    fn plugin_namespace_text_is_injective_for_admitted_identities() {
        let identities = [
            "a/b",
            "a_b",
            "a b",
            "a:b",
            "a.b",
            "A_b",
            "a_5fb",
            "a-b",
            ".hidden",
            "..",
            "über",
            "u_c3_bcber",
        ];
        let mut seen = std::collections::BTreeSet::new();
        for identity in identities {
            let text = plugin_namespace_text(&PluginKey(identity.to_string()));
            assert!(
                seen.insert(text.clone()),
                "{identity:?} collided on namespace text {text:?}"
            );
            Namespace::new(&text)
                .unwrap_or_else(|error| panic!("{identity:?} -> {text:?}: {error}"));
        }
        assert_eq!(
            plugin_namespace_text(&PluginKey("project-pipelines".into())),
            "project-pipelines"
        );
        assert_eq!(plugin_namespace_text(&PluginKey("a/b".into())), "a_2fb");
        assert_eq!(plugin_namespace_text(&PluginKey("a_b".into())), "a_5fb");
    }
    use std::time::{SystemTime, UNIX_EPOCH};

    fn test_backend(name: &str) -> Arc<KeyedPluginStore> {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time after epoch")
            .as_nanos();
        let root = std::env::temp_dir().join(format!(
            "botster-plugin-store-{name}-{}-{nonce}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&root);
        Arc::new(KeyedPluginStore::open(&root.join(PLUGIN_DB_FILE)).expect("open plugin db"))
    }

    fn set_mutation(
        key: &str,
        value: serde_json::Value,
        expected_revision: u64,
    ) -> PluginStoreBatchMutation {
        PluginStoreBatchMutation::Set {
            key: PluginStoreKey(key.to_string()),
            schema_version: 1,
            payload: value,
            expected_revision,
        }
    }

    #[test]
    fn plugin_store_batch_applies_ordered_set_patch_delete_and_rejects_typed_invalid_candidates() {
        let backend = test_backend("candidate");
        let plugin_key = PluginKey("project-pipelines".to_string());
        for (key, payload) in [
            ("ticket", serde_json::json!({ "status": "open" })),
            ("obsolete", serde_json::json!({ "present": true })),
        ] {
            backend
                .set(
                    &plugin_key,
                    PluginStoreKey(key.to_string()),
                    1,
                    payload,
                    Some(0),
                    PluginStoreLimits::default(),
                )
                .expect("seed record");
        }

        let result = backend.batch(
            &plugin_key,
            vec![
                PluginStoreBatchMutation::Patch {
                    key: PluginStoreKey("ticket".to_string()),
                    patch: serde_json::json!({ "status": "active" }),
                    expected_revision: 1,
                },
                PluginStoreBatchMutation::Delete {
                    key: PluginStoreKey("obsolete".to_string()),
                    expected_revision: 1,
                },
                set_mutation("run", serde_json::json!({ "status": "active" }), 0),
            ],
            PluginStoreLimits::default(),
        );
        assert!(result.ok);
        assert!(matches!(
            result.results.as_slice(),
            [
                PluginStoreBatchMutationResult::Patch { .. },
                PluginStoreBatchMutationResult::Delete { .. },
                PluginStoreBatchMutationResult::Set { .. }
            ]
        ));
        let records = backend
            .read_records(&KeyedPluginStore::namespace(&plugin_key).expect("namespace"))
            .expect("read committed batch");
        assert_eq!(records.len(), 2);
        assert_eq!(records[&PluginStoreKey("ticket".to_string())].revision, 2);
        assert!(!records.contains_key(&PluginStoreKey("obsolete".to_string())));

        for (mutations, expected_kind) in [
            (
                vec![
                    set_mutation("duplicate", serde_json::json!({ "value": 1 }), 0),
                    set_mutation("duplicate", serde_json::json!({ "value": 2 }), 0),
                ],
                CapabilityRuntimeErrorKind::InvalidRequest,
            ),
            (
                vec![PluginStoreBatchMutation::Patch {
                    key: PluginStoreKey("ticket".to_string()),
                    patch: serde_json::json!("invalid"),
                    expected_revision: 2,
                }],
                CapabilityRuntimeErrorKind::PatchFailed,
            ),
        ] {
            let failed = backend.batch(&plugin_key, mutations, PluginStoreLimits::default());
            assert!(!failed.ok);
            assert_eq!(failed.error_kind, Some(expected_kind));
            assert!(failed.mutation_index.is_some());
            assert!(failed.key.is_some());
            assert_eq!(
                backend
                    .read_records(&KeyedPluginStore::namespace(&plugin_key).expect("namespace"))
                    .expect("read unchanged batch"),
                records
            );
        }

        let oversized = backend.batch(
            &plugin_key,
            vec![
                set_mutation(
                    "oversized",
                    serde_json::json!({ "value": "x".repeat(65 * 1024) }),
                    0,
                ),
                set_mutation("small", serde_json::json!({ "value": 1 }), 0),
            ],
            PluginStoreLimits::default(),
        );
        assert!(!oversized.ok);
        assert_eq!(
            oversized.error_kind,
            Some(CapabilityRuntimeErrorKind::QuotaExceeded)
        );
        assert_eq!(oversized.mutation_index, Some(1));
        assert_eq!(oversized.key, Some(PluginStoreKey("oversized".to_string())));
        assert_eq!(
            backend
                .read_records(&KeyedPluginStore::namespace(&plugin_key).expect("namespace"))
                .expect("read after per-record quota failure"),
            records
        );

        let empty = backend.batch(&plugin_key, Vec::new(), PluginStoreLimits::default());
        assert!(!empty.ok);
        assert_eq!(
            empty.error_kind,
            Some(CapabilityRuntimeErrorKind::InvalidRequest)
        );
        assert_eq!(
            backend
                .read_records(&KeyedPluginStore::namespace(&plugin_key).expect("namespace"))
                .expect("read after empty batch"),
            records
        );
    }

    #[test]
    fn plugin_store_batch_enforces_final_snapshot_quotas_without_false_mutation_attribution() {
        let aggregate_backend = test_backend("aggregate-quota");
        let plugin_key = PluginKey("project-pipelines".to_string());
        let aggregate = aggregate_backend.batch(
            &plugin_key,
            vec![
                set_mutation("first", serde_json::json!({ "value": "12345" }), 0),
                set_mutation("second", serde_json::json!({ "value": "67890" }), 0),
            ],
            PluginStoreLimits {
                max_record_bytes: 100,
                max_plugin_keys: 10,
                max_plugin_bytes: 20,
            },
        );
        assert!(!aggregate.ok);
        assert_eq!(
            aggregate.error_kind,
            Some(CapabilityRuntimeErrorKind::QuotaExceeded)
        );
        assert_eq!(aggregate.mutation_index, None);
        assert_eq!(aggregate.key, None);
        assert!(
            aggregate_backend
                .read_records(&KeyedPluginStore::namespace(&plugin_key).expect("namespace"))
                .expect("read after aggregate quota failure")
                .is_empty()
        );

        let replacement_backend = test_backend("key-quota-replacement");
        replacement_backend
            .set(
                &plugin_key,
                PluginStoreKey("old".to_string()),
                1,
                serde_json::json!({ "value": "old" }),
                Some(0),
                PluginStoreLimits::default(),
            )
            .expect("seed record at key quota");
        let replacement = replacement_backend.batch(
            &plugin_key,
            vec![
                PluginStoreBatchMutation::Delete {
                    key: PluginStoreKey("old".to_string()),
                    expected_revision: 1,
                },
                set_mutation("new", serde_json::json!({ "value": "new" }), 0),
            ],
            PluginStoreLimits {
                max_record_bytes: 100,
                max_plugin_keys: 1,
                max_plugin_bytes: 100,
            },
        );
        assert!(replacement.ok);
        let records = replacement_backend
            .read_records(&KeyedPluginStore::namespace(&plugin_key).expect("namespace"))
            .expect("read legal replacement at key quota");
        assert_eq!(records.len(), 1);
        assert!(records.contains_key(&PluginStoreKey("new".to_string())));
    }
}
