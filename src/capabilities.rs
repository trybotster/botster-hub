//! Hub-owned concrete capability runtimes over `botster-core` contracts.
//!
//! Core owns request, event, handle, and error shapes. The hub owns concrete
//! local policy: scope roots, plugin-data paths, exact grants, operation
//! limits, bounded local network stubs, and plugin cleanup.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fs;
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
    HttpCapabilityRuntimeConfig, HttpCapabilityTransport, HttpTransportRequest,
    InMemoryWebSocketCapabilityRuntime, PluginCancellationToken, PluginCapabilityRuntime,
    PluginCleanupResult, PluginKey, PluginResourceKind, PluginResourceRef, PluginStoreBackend,
    PluginStoreCapabilityRequest, PluginStoreEntry, PluginStoreKey, PluginStoreLimits,
    PluginStoreOperation, PluginStoreRecord, PluginStoreResult, RequestId, ScopedRelativePath,
    TimerCapabilityRequest, WebSocketCapabilityRuntimeConfig, apply_plugin_store_merge_patch,
    plugin_store_payload_bytes,
};

use crate::config::HubConfig;

const DEFAULT_FILESYSTEM_SCOPE: &str = "workspace";
const DEFAULT_CAPABILITY_EVENT_CAPACITY: usize = 256;
const DEFAULT_CAPABILITY_OPERATION_CAPACITY: usize = 128;

/// Hub-owned concrete capability runtime.
pub struct HubCapabilityRuntime {
    grants: CapabilitySet,
    filesystem_grants: BTreeMap<String, HubFilesystemScope>,
    plugin_store: Arc<LocalPluginStoreBackend>,
    plugin_store_limits: PluginStoreLimits,
    http: HttpCapabilityRuntime,
    websocket: InMemoryWebSocketCapabilityRuntime,
    timers: BTreeMap<CapabilityResourceId, HubTimer>,
    pending_events: VecDeque<CapabilityRuntimeEvent>,
    completions_sender: mpsc::Sender<HubCapabilityCompletion>,
    completions_receiver: mpsc::Receiver<HubCapabilityCompletion>,
    operation_capacity: usize,
    event_capacity: usize,
}

impl HubCapabilityRuntime {
    /// Build the local concrete runtime from explicit hub config.
    #[must_use]
    pub fn from_config(config: &HubConfig) -> Self {
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
        let endpoint_policy = HttpCapabilityEndpointPolicy::new(
            ["http", "https"],
            ["localhost", "127.0.0.1", "example.invalid"],
        );
        let http = HttpCapabilityRuntime::new(
            grants.clone(),
            endpoint_policy,
            HttpCapabilityRuntimeConfig::default(),
            Arc::new(LocalStubHttpTransport),
        );
        let websocket = InMemoryWebSocketCapabilityRuntime::new(
            WebSocketCapabilityRuntimeConfig::new(grants.clone(), 256, 256, 256),
        );
        let (completions_sender, completions_receiver) = mpsc::channel();

        Self {
            grants,
            filesystem_grants,
            plugin_store: Arc::new(LocalPluginStoreBackend::new(
                config.data_directory.join("plugin-data"),
            )),
            plugin_store_limits: PluginStoreLimits::default(),
            http,
            websocket,
            timers: BTreeMap::new(),
            pending_events: VecDeque::new(),
            completions_sender,
            completions_receiver,
            operation_capacity: DEFAULT_CAPABILITY_OPERATION_CAPACITY,
            event_capacity: DEFAULT_CAPABILITY_EVENT_CAPACITY,
        }
    }

    /// Return the exact scoped grants accepted by the local runtime.
    #[must_use]
    pub fn granted_capabilities(&self) -> &CapabilitySet {
        &self.grants
    }

    /// Return the hub-owned plugin store root.
    #[must_use]
    pub fn plugin_store_root(&self) -> &Path {
        self.plugin_store.root()
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
        self.ensure_plugin_namespace_grant(&request.plugin_key, &store.namespace)?;
        validate_store_operation(&store.operation)?;

        let operation_id = request.operation_id.clone();
        let plugin_key = request.plugin_key.clone();
        let resource = request.resource_ref(CapabilityResourceId(operation_id.0.clone()));
        let backend = self.plugin_store.clone();
        let limits = self.plugin_store_limits;
        let sender = self.completions_sender.clone();
        std::thread::Builder::new()
            .name("botster-hub-plugin-store-capability".to_string())
            .spawn(move || {
                let result =
                    execute_plugin_store(backend.as_ref(), &plugin_key, store.operation, limits)
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
        self.ensure_event_capacity(2)?;
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
        self.pending_events
            .push_back(CapabilityRuntimeEvent::ResourceOpened(
                CapabilityResourceEvent {
                    plugin_key: request.plugin_key.clone(),
                    operation_id: request.operation_id.clone(),
                    resource: resource.clone(),
                },
            ));
        self.pending_events
            .push_back(CapabilityRuntimeEvent::Completed(
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
        self.ensure_event_capacity(2)?;
        self.pending_events
            .push_back(CapabilityRuntimeEvent::ResourceReleased(
                CapabilityResourceEvent {
                    plugin_key: request.plugin_key.clone(),
                    operation_id: request.operation_id.clone(),
                    resource: timer.resource.clone(),
                },
            ));
        self.pending_events
            .push_back(CapabilityRuntimeEvent::Completed(
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
        self.ensure_event_capacity(due.len())?;

        for resource_id in due {
            let mut remove = false;
            if let Some(timer) = self.timers.get_mut(&resource_id) {
                timer.sequence += 1;
                self.pending_events
                    .push_back(CapabilityRuntimeEvent::TimerFired(CapabilityTimerEvent {
                        resource: timer.resource.clone(),
                        sequence: timer.sequence,
                    }));
                if let Some(interval_ms) = timer.interval_ms {
                    timer.next_fire_ms = now_ms.saturating_add(interval_ms);
                } else {
                    remove = true;
                }
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
        if self.pending_events.len() >= self.operation_capacity {
            self.pending_events
                .push_back(CapabilityRuntimeEvent::Backpressure(
                    request.backpressure(self.operation_capacity, self.pending_events.len()),
                ));
            return Err(CapabilityRuntimeError::new(
                CapabilityRuntimeErrorKind::Backpressured,
                "hub capability runtime queue is at capacity",
            ));
        }
        Ok(())
    }

    fn ensure_event_capacity(&self, additional: usize) -> Result<(), CapabilityRuntimeError> {
        if self.pending_events.len().saturating_add(additional) > self.event_capacity {
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
            self.ensure_event_capacity(1)?;
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
            self.pending_events.push_back(event);
        }
        Ok(())
    }

    fn drain_local_events(
        &mut self,
        plugin_key: &PluginKey,
    ) -> Result<Vec<CapabilityRuntimeEvent>, CapabilityRuntimeError> {
        self.drain_worker_completions()?;
        let mut events = Vec::new();
        let mut retained = VecDeque::new();
        while let Some(event) = self.pending_events.pop_front() {
            if event_plugin_key(&event).as_ref() == Some(plugin_key) {
                events.push(event);
            } else {
                retained.push_back(event);
            }
        }
        self.pending_events = retained;
        Ok(events)
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
        self.pending_events
            .retain(|event| event_plugin_key(event).as_ref() != Some(plugin_key));

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

#[derive(Debug)]
struct LocalPluginStoreBackend {
    root: PathBuf,
    lock: Mutex<()>,
}

impl LocalPluginStoreBackend {
    fn new(root: PathBuf) -> Self {
        Self {
            root,
            lock: Mutex::new(()),
        }
    }

    fn root(&self) -> &Path {
        &self.root
    }

    fn namespace_dir(&self, plugin_key: &PluginKey) -> PathBuf {
        self.root.join(sanitize_plugin_key(plugin_key))
    }

    fn record_path(&self, plugin_key: &PluginKey, key: &PluginStoreKey) -> PathBuf {
        self.namespace_dir(plugin_key)
            .join(format!("{}.json", encode_key(&key.0)))
    }

    fn read_records(
        &self,
        plugin_key: &PluginKey,
    ) -> Result<BTreeMap<PluginStoreKey, PluginStoreRecord>, CapabilityRuntimeError> {
        let namespace = self.namespace_dir(plugin_key);
        if !namespace.exists() {
            return Ok(BTreeMap::new());
        }

        let mut records = BTreeMap::new();
        for entry in fs::read_dir(namespace).map_err(backend_error)? {
            let entry = entry.map_err(backend_error)?;
            if entry.path().extension().and_then(|value| value.to_str()) != Some("json") {
                continue;
            }
            let bytes = fs::read(entry.path()).map_err(backend_error)?;
            let record = serde_json::from_slice::<PluginStoreRecord>(&bytes).map_err(|error| {
                CapabilityRuntimeError::new(
                    CapabilityRuntimeErrorKind::BackendFailed,
                    format!("plugin-store record could not be decoded: {error}"),
                )
            })?;
            records.insert(record.key.clone(), record);
        }
        Ok(records)
    }

    fn write_record(&self, record: &PluginStoreRecord) -> Result<(), CapabilityRuntimeError> {
        let namespace = self.namespace_dir(&record.plugin_key);
        fs::create_dir_all(&namespace).map_err(backend_error)?;
        let bytes = serde_json::to_vec_pretty(record).map_err(|error| {
            CapabilityRuntimeError::new(
                CapabilityRuntimeErrorKind::BackendFailed,
                format!("plugin-store record could not be encoded: {error}"),
            )
        })?;
        fs::write(self.record_path(&record.plugin_key, &record.key), bytes).map_err(backend_error)
    }
}

impl PluginStoreBackend for LocalPluginStoreBackend {
    fn get(
        &self,
        plugin_key: &PluginKey,
        key: &PluginStoreKey,
    ) -> Result<Option<PluginStoreRecord>, CapabilityRuntimeError> {
        let _guard = self.lock.lock().expect("plugin store lock poisoned");
        Ok(self.read_records(plugin_key)?.get(key).cloned())
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
        let records = self.read_records(plugin_key)?;
        let revision = revision_for_write(records.get(&key), expected_revision)?;
        enforce_plugin_store_limits(&records, &key, &payload, limits)?;
        let record = PluginStoreRecord {
            plugin_key: plugin_key.clone(),
            key,
            schema_version,
            revision,
            payload,
        };
        self.write_record(&record)?;
        Ok(record)
    }

    fn delete(
        &self,
        plugin_key: &PluginKey,
        key: &PluginStoreKey,
    ) -> Result<PluginStoreRecord, CapabilityRuntimeError> {
        let _guard = self.lock.lock().expect("plugin store lock poisoned");
        let record = self
            .read_records(plugin_key)?
            .get(key)
            .cloned()
            .ok_or_else(|| {
                CapabilityRuntimeError::new(
                    CapabilityRuntimeErrorKind::StoreNotFound,
                    "plugin-store record was not found",
                )
            })?;
        fs::remove_file(self.record_path(plugin_key, key)).map_err(backend_error)?;
        Ok(record)
    }

    fn list(
        &self,
        plugin_key: &PluginKey,
        prefix: Option<&str>,
    ) -> Result<Vec<PluginStoreEntry>, CapabilityRuntimeError> {
        let _guard = self.lock.lock().expect("plugin store lock poisoned");
        Ok(self
            .read_records(plugin_key)?
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
        let records = self.read_records(plugin_key)?;
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
        self.write_record(&record)?;
        Ok(record)
    }
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

struct LocalStubHttpTransport;

impl HttpCapabilityTransport for LocalStubHttpTransport {
    fn execute(
        &self,
        request: HttpTransportRequest,
        cancellation: PluginCancellationToken,
    ) -> Result<HttpCapabilityResponse, CapabilityRuntimeError> {
        if cancellation.is_cancelled() {
            return Err(CapabilityRuntimeError::new(
                CapabilityRuntimeErrorKind::Cancelled,
                "HTTP capability operation was cancelled",
            ));
        }
        std::thread::sleep(Duration::from_millis(1));
        let response = HttpCapabilityResponse {
            status: 204,
            headers: Vec::new(),
            body: Vec::new(),
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

fn sanitize_plugin_key(plugin_key: &PluginKey) -> String {
    let sanitized = plugin_key
        .0
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || matches!(character, '-' | '_' | '.') {
                character
            } else {
                '_'
            }
        })
        .collect::<String>();

    if sanitized == "." || sanitized == ".." || sanitized.starts_with('.') {
        format!("_{sanitized}")
    } else {
        sanitized
    }
}

fn encode_key(key: &str) -> String {
    key.as_bytes()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn backend_error(error: std::io::Error) -> CapabilityRuntimeError {
    CapabilityRuntimeError::new(
        CapabilityRuntimeErrorKind::BackendFailed,
        format!("local capability backend failed: {error}"),
    )
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
