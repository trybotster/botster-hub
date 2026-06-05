#![cfg(unix)]

use std::io::{Read, Write};
use std::net::TcpListener;
use std::thread;
use std::time::{Duration, Instant};

use botster_core::{
    Capability, CapabilityOperation, CapabilityOperationId, CapabilityOperationResult,
    CapabilityRuntimeErrorKind, CapabilityRuntimeEvent, CapabilityRuntimeRequest,
    CapabilitySurface, ClientId, CoreSessionMetadata, FilesystemCapabilityLimits,
    FilesystemCapabilityRequest, FilesystemCapabilityResult, FilesystemOperation,
    HttpCapabilityRequest, HttpHeader, PluginKey, PluginStoreCapabilityRequest, PluginStoreKey,
    PluginStoreOperation, PluginStoreResult, RequestId, ResizePayload, ScopedRelativePath,
    SessionId, SessionSpawnRequest, SpawnEnvironment, SpawnWorkingDirectory, SubscriptionId,
    TimerCapabilityRequest, TransportEgress, WebSocketCapabilityRequest,
};
use botster_hub::{
    DataDirectoryOption, HostIdentityOptions, HubRuntime, HubStartupOptions, RuntimeEnvironment,
    SessionDefaults, TransportBindings,
};

mod support;
use support::ensure_session_worker_binary;

fn explicit_runtime(name: &str) -> HubRuntime {
    ensure_session_worker_binary();
    let config = HubStartupOptions {
        host: HostIdentityOptions {
            id: format!("hub-capability-{name}"),
            display_name: "Hub Capability Runtime Test".to_string(),
            fingerprint: None,
        },
        data_directory: DataDirectoryOption::Explicit(
            format!("target/botster-hub-test-data/capability-runtime/{name}").into(),
        ),
        session_defaults: SessionDefaults {
            shell: "/bin/sh".to_string(),
            working_directory: Some(".".into()),
            initial_rows: 24,
            initial_cols: 80,
        },
        transports: TransportBindings {
            local_socket: None,
            tcp: Vec::new(),
        },
        ..HubStartupOptions::default()
    }
    .build_config_for_environment(&RuntimeEnvironment::from_values(None, None, None))
    .expect("explicit runtime config should build");

    HubRuntime::new(config)
}

fn request(
    plugin: &str,
    operation_id: &str,
    operation: CapabilityOperation,
) -> CapabilityRuntimeRequest {
    CapabilityRuntimeRequest {
        plugin_key: PluginKey(plugin.to_string()),
        operation_id: CapabilityOperationId(operation_id.to_string()),
        operation,
        timeout_ms: 1_000,
        callback: None,
    }
}

fn drain_until_completed(
    runtime: &mut HubRuntime,
    plugin_key: &PluginKey,
) -> Vec<CapabilityRuntimeEvent> {
    let deadline = Instant::now() + Duration::from_secs(3);
    let mut observed = Vec::new();

    while Instant::now() < deadline {
        observed.extend(
            runtime
                .drain_capability_events(plugin_key)
                .expect("drain capability events"),
        );
        if observed
            .iter()
            .any(|event| matches!(event, CapabilityRuntimeEvent::Completed(_)))
        {
            return observed;
        }
        thread::sleep(Duration::from_millis(10));
    }

    panic!("timed out waiting for capability completion: {observed:?}");
}

fn drain_until_failed(
    runtime: &mut HubRuntime,
    plugin_key: &PluginKey,
) -> Vec<CapabilityRuntimeEvent> {
    let deadline = Instant::now() + Duration::from_secs(3);
    let mut observed = Vec::new();

    while Instant::now() < deadline {
        observed.extend(
            runtime
                .drain_capability_events(plugin_key)
                .expect("drain capability events"),
        );
        if observed
            .iter()
            .any(|event| matches!(event, CapabilityRuntimeEvent::Failed(_)))
        {
            return observed;
        }
        thread::sleep(Duration::from_millis(10));
    }

    panic!("timed out waiting for capability failure: {observed:?}");
}

fn drain_until_terminal(
    runtime: &mut HubRuntime,
    plugin_key: &PluginKey,
) -> Vec<CapabilityRuntimeEvent> {
    let deadline = Instant::now() + Duration::from_secs(3);
    let mut observed = Vec::new();

    while Instant::now() < deadline {
        observed.extend(
            runtime
                .drain_capability_events(plugin_key)
                .expect("drain capability events"),
        );
        if observed.iter().any(|event| {
            matches!(
                event,
                CapabilityRuntimeEvent::Completed(_)
                    | CapabilityRuntimeEvent::Failed(_)
                    | CapabilityRuntimeEvent::TimedOut(_)
                    | CapabilityRuntimeEvent::Cancelled(_)
            )
        }) {
            return observed;
        }
        thread::sleep(Duration::from_millis(10));
    }

    panic!("timed out waiting for terminal capability event: {observed:?}");
}

struct LocalHttpServer {
    endpoint: String,
    join: thread::JoinHandle<Vec<u8>>,
}

impl LocalHttpServer {
    fn start(response: impl Into<Vec<u8>>, delay: Duration) -> Self {
        let response = response.into();
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind local HTTP server");
        let endpoint = format!(
            "http://127.0.0.1:{}/status",
            listener.local_addr().expect("local address").port()
        );
        let join = thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("accept local HTTP request");
            let mut request = Vec::new();
            let mut buffer = [0; 1024];
            loop {
                let bytes = stream.read(&mut buffer).expect("read request");
                if bytes == 0 {
                    break;
                }
                request.extend_from_slice(&buffer[..bytes]);
                if request.windows(4).any(|window| window == b"\r\n\r\n") {
                    break;
                }
            }
            if !delay.is_zero() {
                thread::sleep(delay);
            }
            stream.write_all(&response).expect("write response");
            request
        });

        Self { endpoint, join }
    }

    fn join(self) -> Vec<u8> {
        self.join.join().expect("local HTTP server thread")
    }
}

fn spawn_request(config: &botster_hub::HubConfig) -> SessionSpawnRequest {
    SessionSpawnRequest {
        request_id: RequestId("capability-hot-path-spawn".to_string()),
        session_id: SessionId("capability-hot-path-session".to_string()),
        executable: config.session_defaults.shell.clone(),
        arguments: vec![
            "-c".to_string(),
            "printf 'ready\\n'; while IFS= read -r line; do printf 'echo:%s\\n' \"$line\"; done"
                .to_string(),
        ],
        working_directory: SpawnWorkingDirectory {
            path: config
                .session_defaults
                .working_directory
                .as_deref()
                .expect("test config has working directory")
                .display()
                .to_string(),
        },
        environment: SpawnEnvironment::default(),
        initial_pty_size: Some(ResizePayload {
            rows: config.session_defaults.initial_rows,
            cols: config.session_defaults.initial_cols,
        }),
    }
}

fn drain_session_until(
    runtime: &mut HubRuntime,
    session_id: &SessionId,
    needle: &[u8],
    logical_clock: &mut u64,
) {
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut observed = Vec::new();

    while Instant::now() < deadline {
        let output = runtime
            .drain_runtime_once(session_id, *logical_clock)
            .expect("drain runtime");
        *logical_clock += 1;
        for (_, frame) in output.client_egress {
            if let TransportEgress::TerminalOutput { data, .. } = frame {
                observed.extend(data);
            }
        }
        if observed
            .windows(needle.len())
            .any(|window| window == needle)
        {
            return;
        }
        thread::sleep(Duration::from_millis(20));
    }

    panic!(
        "timed out waiting for {:?}",
        String::from_utf8_lossy(needle)
    );
}

#[test]
fn hub_runtime_serves_allowed_scoped_filesystem_requests_and_denies_escape_paths() {
    let mut runtime = explicit_runtime("filesystem");
    let plugin_key = PluginKey("project-pipelines".to_string());
    let write = request(
        &plugin_key.0,
        "write-settings",
        CapabilityOperation::Filesystem(FilesystemCapabilityRequest {
            scope_id: "workspace".to_string(),
            operation: FilesystemOperation::Write {
                path: ScopedRelativePath("settings/state.json".to_string()),
                bytes: br#"{"ok":true}"#.to_vec(),
            },
            limits: None,
        }),
    );

    let handle = runtime
        .submit_capability_request(write)
        .expect("allowed filesystem request should submit");
    assert_eq!(
        handle.required_capability,
        Capability {
            surface: CapabilitySurface::Filesystem,
            scope: Some("workspace".to_string()),
        }
    );
    let events = drain_until_completed(&mut runtime, &plugin_key);
    assert!(events.iter().any(|event| matches!(
        event,
        CapabilityRuntimeEvent::Completed(completed)
            if matches!(
                completed.result,
                Some(CapabilityOperationResult::Filesystem(
                    FilesystemCapabilityResult::Write { bytes_written: 11, .. }
                ))
            )
    )));

    let denied = runtime
        .submit_capability_request(request(
            &plugin_key.0,
            "escape",
            CapabilityOperation::Filesystem(FilesystemCapabilityRequest {
                scope_id: "workspace".to_string(),
                operation: FilesystemOperation::Read {
                    path: ScopedRelativePath("../secret".to_string()),
                },
                limits: None,
            }),
        ))
        .expect_err("parent traversal should be denied before I/O");
    assert_eq!(denied.kind, CapabilityRuntimeErrorKind::InvalidRequest);

    let unknown_scope = runtime
        .submit_capability_request(request(
            &plugin_key.0,
            "unknown-scope",
            CapabilityOperation::Filesystem(FilesystemCapabilityRequest {
                scope_id: "plugin-source".to_string(),
                operation: FilesystemOperation::List {
                    path: ScopedRelativePath(".".to_string()),
                },
                limits: None,
            }),
        ))
        .expect_err("unknown scope should be denied");
    assert_eq!(
        unknown_scope.kind,
        CapabilityRuntimeErrorKind::CapabilityDenied
    );

    runtime
        .submit_capability_request(request(
            &plugin_key.0,
            "wide-write-limit",
            CapabilityOperation::Filesystem(FilesystemCapabilityRequest {
                scope_id: "workspace".to_string(),
                operation: FilesystemOperation::Write {
                    path: ScopedRelativePath("settings/too-large.bin".to_string()),
                    bytes: vec![b'x'; 1024 * 1024 + 1],
                },
                limits: Some(FilesystemCapabilityLimits {
                    max_read_bytes: None,
                    max_write_bytes: Some(2 * 1024 * 1024),
                    max_list_entries: None,
                }),
            }),
        ))
        .expect("wide request limit still submits for worker enforcement");
    let failed = drain_until_failed(&mut runtime, &plugin_key);
    assert!(failed.iter().any(|event| matches!(
        event,
        CapabilityRuntimeEvent::Failed(failure)
            if failure.error_kind == CapabilityRuntimeErrorKind::InvalidRequest
                && failure.reason.contains("write exceeds configured limit")
    )));
}

#[test]
fn hub_runtime_stores_plugin_json_under_plugin_data_and_enforces_namespace() {
    let mut runtime = explicit_runtime("plugin-store");
    let plugin_key = PluginKey("project-pipelines".to_string());
    let store_root = runtime
        .capability_runtime()
        .lock()
        .expect("hub capability runtime lock")
        .plugin_store_root()
        .to_path_buf();

    runtime
        .submit_capability_request(request(
            &plugin_key.0,
            "store-set",
            CapabilityOperation::PluginStore(PluginStoreCapabilityRequest {
                namespace: plugin_key.0.clone(),
                operation: PluginStoreOperation::Set {
                    key: PluginStoreKey("tickets/open".to_string()),
                    schema_version: 1,
                    payload: serde_json::json!({ "count": 1 }),
                    expected_revision: None,
                },
            }),
        ))
        .expect("allowed plugin-store set should submit");
    let events = drain_until_completed(&mut runtime, &plugin_key);
    assert!(events.iter().any(|event| matches!(
        event,
        CapabilityRuntimeEvent::Completed(completed)
            if matches!(
                completed.result,
                Some(CapabilityOperationResult::PluginStore(
                    PluginStoreResult::Written { .. }
                ))
            )
    )));
    assert!(store_root.ends_with("plugin-data"));
    assert!(store_root.join("project-pipelines").exists());

    let denied = runtime
        .submit_capability_request(request(
            &plugin_key.0,
            "wrong-namespace",
            CapabilityOperation::PluginStore(PluginStoreCapabilityRequest {
                namespace: "other-plugin".to_string(),
                operation: PluginStoreOperation::List { prefix: None },
            }),
        ))
        .expect_err("plugin store namespace mismatch should be denied");
    assert_eq!(denied.kind, CapabilityRuntimeErrorKind::CapabilityDenied);
}

#[test]
fn hub_runtime_executes_admitted_http_through_real_loopback_transport() {
    let server = LocalHttpServer::start(
        b"HTTP/1.1 201 Created\r\nContent-Type: text/plain\r\nX-Botster-Test: real\r\nContent-Length: 11\r\nConnection: close\r\n\r\nhello-world",
        Duration::ZERO,
    );
    let mut runtime = explicit_runtime("http-real-loopback");
    let plugin_key = PluginKey("project-pipelines".to_string());

    let http_handle = runtime
        .submit_capability_request(request(
            &plugin_key.0,
            "http-probe",
            CapabilityOperation::Http(HttpCapabilityRequest {
                method: "GET".to_string(),
                endpoint: server.endpoint.clone(),
                headers: Vec::new(),
                body: Vec::new(),
            }),
        ))
        .expect("allowed local HTTP request should submit");
    assert_eq!(
        http_handle.required_capability,
        Capability {
            surface: CapabilitySurface::Network,
            scope: Some("http".to_string()),
        }
    );
    let http_events = drain_until_completed(&mut runtime, &plugin_key);
    assert!(http_events.iter().any(|event| matches!(
        event,
        CapabilityRuntimeEvent::Completed(completed)
            if matches!(
                &completed.result,
                Some(CapabilityOperationResult::Http(response))
                    if response.status == 201
                        && response.body == b"hello-world"
                        && response.headers.iter().any(|header| {
                            header.name.eq_ignore_ascii_case("x-botster-test")
                                && header.value == "real"
                        })
            )
    )));
    let observed_request = server.join();
    assert!(String::from_utf8_lossy(&observed_request).starts_with("GET /status HTTP/1.1"));
}

#[test]
fn hub_runtime_denies_http_hosts_methods_and_sensitive_headers() {
    let mut runtime = explicit_runtime("http-policy-denials");
    let plugin_key = PluginKey("project-pipelines".to_string());

    let denied_host = runtime
        .submit_capability_request(request(
            &plugin_key.0,
            "denied-host",
            CapabilityOperation::Http(HttpCapabilityRequest {
                method: "GET".to_string(),
                endpoint: "https://example.invalid/status".to_string(),
                headers: Vec::new(),
                body: Vec::new(),
            }),
        ))
        .expect_err("non-loopback host should be denied before transport");
    assert_eq!(
        denied_host.kind,
        CapabilityRuntimeErrorKind::CapabilityDenied
    );

    runtime
        .submit_capability_request(request(
            &plugin_key.0,
            "denied-method",
            CapabilityOperation::Http(HttpCapabilityRequest {
                method: "DELETE".to_string(),
                endpoint: "http://127.0.0.1:9/status".to_string(),
                headers: Vec::new(),
                body: Vec::new(),
            }),
        ))
        .expect("core admits loopback request before hub method policy");
    let method_events = drain_until_failed(&mut runtime, &plugin_key);
    assert!(method_events.iter().any(|event| matches!(
        event,
        CapabilityRuntimeEvent::Failed(failure)
            if failure.operation_id.0 == "denied-method"
                && failure.error_kind == CapabilityRuntimeErrorKind::CapabilityDenied
                && failure.reason == "HTTP method is not allowed by this hub"
    )));

    runtime
        .submit_capability_request(request(
            &plugin_key.0,
            "denied-sensitive-header",
            CapabilityOperation::Http(HttpCapabilityRequest {
                method: "GET".to_string(),
                endpoint: "http://127.0.0.1:9/status".to_string(),
                headers: vec![HttpHeader {
                    name: "Authorization".to_string(),
                    value: "sensitive-test-value".to_string(),
                }],
                body: Vec::new(),
            }),
        ))
        .expect("core admits syntactically valid header before hub header policy");
    let header_events = drain_until_failed(&mut runtime, &plugin_key);
    assert!(header_events.iter().any(|event| matches!(
        event,
        CapabilityRuntimeEvent::Failed(failure)
            if failure.operation_id.0 == "denied-sensitive-header"
                && failure.error_kind == CapabilityRuntimeErrorKind::CapabilityDenied
                && failure.reason == "HTTP request header is not allowed by this hub"
    )));
    assert!(
        !format!("{header_events:?}").contains("sensitive-test-value"),
        "failure events must not echo sensitive header values"
    );
}

#[test]
fn hub_runtime_enforces_http_body_limits() {
    let mut response = b"HTTP/1.1 200 OK\r\nContent-Type: application/octet-stream\r\nContent-Length: 4194305\r\nConnection: close\r\n\r\n".to_vec();
    response.extend(vec![b'x'; 4 * 1024 * 1024 + 1]);
    let server = LocalHttpServer::start(response, Duration::ZERO);
    let mut runtime = explicit_runtime("http-body-limits");
    let plugin_key = PluginKey("project-pipelines".to_string());

    let oversized_request = runtime
        .submit_capability_request(request(
            &plugin_key.0,
            "oversized-request",
            CapabilityOperation::Http(HttpCapabilityRequest {
                method: "POST".to_string(),
                endpoint: "http://127.0.0.1:9/status".to_string(),
                headers: Vec::new(),
                body: vec![b'x'; 1024 * 1024 + 1],
            }),
        ))
        .expect_err("oversized HTTP request body should fail before transport");
    assert_eq!(
        oversized_request.kind,
        CapabilityRuntimeErrorKind::InvalidRequest
    );

    runtime
        .submit_capability_request(request(
            &plugin_key.0,
            "oversized-response",
            CapabilityOperation::Http(HttpCapabilityRequest {
                method: "GET".to_string(),
                endpoint: server.endpoint.clone(),
                headers: Vec::new(),
                body: Vec::new(),
            }),
        ))
        .expect("loopback request should submit for response limit enforcement");
    let response_events = drain_until_failed(&mut runtime, &plugin_key);
    assert!(response_events.iter().any(|event| matches!(
        event,
        CapabilityRuntimeEvent::Failed(failure)
            if failure.operation_id.0 == "oversized-response"
                && failure.error_kind == CapabilityRuntimeErrorKind::InvalidRequest
                && failure.reason == "HTTP response body exceeds configured limit"
    )));
    let _ = server.join();
}

#[test]
fn hub_runtime_reports_bounded_http_failures_without_blocking_hot_path() {
    let server = LocalHttpServer::start(
        b"HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: 4\r\nConnection: close\r\n\r\nslow",
        Duration::from_millis(500),
    );
    let mut runtime = explicit_runtime("http-hot-path");
    let plugin_key = PluginKey("project-pipelines".to_string());
    let spawn = spawn_request(runtime.config());
    let session_id = spawn.session_id.clone();
    let client_id = ClientId("capability-http-client".to_string());
    let subscription_id = SubscriptionId("capability-http-subscription".to_string());
    let mut logical_clock = 100;

    runtime
        .spawn_session(spawn, CoreSessionMetadata::new(), logical_clock)
        .expect("spawn through core");
    logical_clock += 1;
    runtime
        .attach_client(
            client_id.clone(),
            session_id.clone(),
            subscription_id,
            logical_clock,
        )
        .expect("attach through core");
    logical_clock += 1;

    runtime
        .submit_capability_request(request(
            &plugin_key.0,
            "slow-http",
            CapabilityOperation::Http(HttpCapabilityRequest {
                method: "GET".to_string(),
                endpoint: server.endpoint.clone(),
                headers: Vec::new(),
                body: Vec::new(),
            }),
        ))
        .expect("slow loopback HTTP should submit");

    drain_session_until(&mut runtime, &session_id, b"ready", &mut logical_clock);
    runtime
        .write_bytes(
            client_id,
            session_id.clone(),
            b"ping-http-capability\n".to_vec(),
            logical_clock,
        )
        .expect("write while HTTP work is pending");
    logical_clock += 1;
    drain_session_until(
        &mut runtime,
        &session_id,
        b"echo:ping-http-capability",
        &mut logical_clock,
    );
    let http_events = drain_until_terminal(&mut runtime, &plugin_key);
    assert!(http_events.iter().any(|event| matches!(
        event,
        CapabilityRuntimeEvent::Completed(completed)
            if completed.operation_id.0 == "slow-http"
                && matches!(
                    &completed.result,
                    Some(CapabilityOperationResult::Http(response)) if response.status == 200
                )
    )));
    let _ = server.join();
}

#[test]
fn hub_runtime_keeps_session_hot_path_responsive_during_failing_http_transport() {
    let server = LocalHttpServer::start(b"not an http response\r\n", Duration::from_millis(500));
    let mut runtime = explicit_runtime("http-failure-hot-path");
    let plugin_key = PluginKey("project-pipelines".to_string());
    let spawn = spawn_request(runtime.config());
    let session_id = spawn.session_id.clone();
    let client_id = ClientId("capability-http-failure-client".to_string());
    let subscription_id = SubscriptionId("capability-http-failure-subscription".to_string());
    let mut logical_clock = 100;

    runtime
        .spawn_session(spawn, CoreSessionMetadata::new(), logical_clock)
        .expect("spawn through core");
    logical_clock += 1;
    runtime
        .attach_client(
            client_id.clone(),
            session_id.clone(),
            subscription_id,
            logical_clock,
        )
        .expect("attach through core");
    logical_clock += 1;

    runtime
        .submit_capability_request(request(
            &plugin_key.0,
            "failing-http",
            CapabilityOperation::Http(HttpCapabilityRequest {
                method: "GET".to_string(),
                endpoint: server.endpoint.clone(),
                headers: Vec::new(),
                body: Vec::new(),
            }),
        ))
        .expect("malformed loopback HTTP request should submit before transport failure");

    drain_session_until(&mut runtime, &session_id, b"ready", &mut logical_clock);
    runtime
        .write_bytes(
            client_id,
            session_id.clone(),
            b"ping-http-failure-capability\n".to_vec(),
            logical_clock,
        )
        .expect("write while failing HTTP work is pending");
    logical_clock += 1;
    drain_session_until(
        &mut runtime,
        &session_id,
        b"echo:ping-http-failure-capability",
        &mut logical_clock,
    );

    let http_events = drain_until_terminal(&mut runtime, &plugin_key);
    assert!(http_events.iter().any(|event| matches!(
        event,
        CapabilityRuntimeEvent::Failed(failure)
            if failure.operation_id.0 == "failing-http"
                && failure.error_kind == CapabilityRuntimeErrorKind::BackendFailed
                && failure.reason == "HTTP transport failed"
    )));
    let _ = server.join();
}

#[test]
fn hub_runtime_preserves_in_memory_websocket_stub() {
    let mut runtime = explicit_runtime("websocket-stub");
    let plugin_key = PluginKey("project-pipelines".to_string());

    let websocket_handle = runtime
        .submit_capability_request(request(
            &plugin_key.0,
            "ws-connect",
            CapabilityOperation::WebSocket(WebSocketCapabilityRequest::Connect {
                endpoint: "wss://example.invalid/events".to_string(),
                protocols: Vec::new(),
            }),
        ))
        .expect("allowed in-memory websocket should submit");
    assert_eq!(
        websocket_handle.required_capability,
        Capability {
            surface: CapabilitySurface::Network,
            scope: Some("websocket".to_string()),
        }
    );
    let websocket_events = runtime
        .drain_capability_events(&plugin_key)
        .expect("drain websocket events");
    assert!(
        websocket_events
            .iter()
            .any(|event| matches!(event, CapabilityRuntimeEvent::ResourceOpened(_)))
    );
    assert!(
        websocket_events
            .iter()
            .any(|event| matches!(event, CapabilityRuntimeEvent::Completed(_)))
    );
}

#[test]
fn hub_runtime_schedules_cancels_and_cleans_up_timers() {
    let mut runtime = explicit_runtime("timers");
    let plugin_key = PluginKey("project-pipelines".to_string());
    let handle = runtime
        .submit_capability_request(request(
            &plugin_key.0,
            "reminder",
            CapabilityOperation::Timer(TimerCapabilityRequest::Once { delay_ms: 10 }),
        ))
        .expect("timer should submit");
    assert_eq!(
        handle.required_capability,
        Capability {
            surface: CapabilitySurface::Timers,
            scope: Some("callbacks".to_string()),
        }
    );
    let early = runtime
        .drain_capability_events_at(&plugin_key, 9)
        .expect("drain early timer events");
    assert!(
        !early
            .iter()
            .any(|event| matches!(event, CapabilityRuntimeEvent::TimerFired(_)))
    );
    let due = runtime
        .drain_capability_events_at(&plugin_key, 10)
        .expect("drain due timer events");
    assert!(
        due.iter()
            .any(|event| matches!(event, CapabilityRuntimeEvent::TimerFired(_)))
    );

    let interval = runtime
        .submit_capability_request(request(
            &plugin_key.0,
            "interval",
            CapabilityOperation::Timer(TimerCapabilityRequest::Interval { interval_ms: 5 }),
        ))
        .expect("interval timer should submit");
    let resource = interval.resource.expect("interval timer has resource");
    runtime
        .release_capability_resource(resource)
        .expect("release timer resource");
    let after_cancel = runtime
        .drain_capability_events_at(&plugin_key, 20)
        .expect("drain after cancel");
    assert!(
        !after_cancel
            .iter()
            .any(|event| matches!(event, CapabilityRuntimeEvent::TimerFired(_)))
    );
}

#[test]
fn capability_operations_do_not_block_session_hot_path() {
    let mut runtime = explicit_runtime("hot-path");
    let plugin_key = PluginKey("project-pipelines".to_string());
    let spawn = spawn_request(runtime.config());
    let session_id = spawn.session_id.clone();
    let client_id = ClientId("capability-client".to_string());
    let subscription_id = SubscriptionId("capability-subscription".to_string());
    let mut logical_clock = 100;

    runtime
        .spawn_session(spawn, CoreSessionMetadata::new(), logical_clock)
        .expect("spawn through core");
    logical_clock += 1;
    runtime
        .attach_client(
            client_id.clone(),
            session_id.clone(),
            subscription_id,
            logical_clock,
        )
        .expect("attach through core");
    logical_clock += 1;

    for index in 0..32 {
        runtime
            .submit_capability_request(request(
                &plugin_key.0,
                &format!("fs-{index}"),
                CapabilityOperation::Filesystem(FilesystemCapabilityRequest {
                    scope_id: "workspace".to_string(),
                    operation: FilesystemOperation::Write {
                        path: ScopedRelativePath(format!("bulk/{index}.txt")),
                        bytes: vec![b'x'; 1024],
                    },
                    limits: None,
                }),
            ))
            .expect("filesystem work should enqueue");
    }

    drain_session_until(&mut runtime, &session_id, b"ready", &mut logical_clock);
    runtime
        .write_bytes(
            client_id,
            session_id.clone(),
            b"ping-capability\n".to_vec(),
            logical_clock,
        )
        .expect("write while capability work is pending");
    logical_clock += 1;
    drain_session_until(
        &mut runtime,
        &session_id,
        b"echo:ping-capability",
        &mut logical_clock,
    );
}

#[test]
fn unload_cleans_up_capability_resources_for_plugin() {
    let mut runtime = explicit_runtime("cleanup");
    let plugin_key = PluginKey("project-pipelines".to_string());
    let timer = runtime
        .submit_capability_request(request(
            &plugin_key.0,
            "cleanup-timer",
            CapabilityOperation::Timer(TimerCapabilityRequest::Interval { interval_ms: 5 }),
        ))
        .expect("timer should submit");
    assert!(timer.resource.is_some());

    let cleanup = runtime
        .cleanup_plugin_capabilities(&plugin_key)
        .expect("capability cleanup should succeed");
    assert_eq!(cleanup.plugin_key, plugin_key);
    assert!(!cleanup.removed_resources.is_empty());

    let after_cleanup = runtime
        .drain_capability_events_at(&PluginKey("project-pipelines".to_string()), 10)
        .expect("drain after cleanup");
    assert!(
        after_cleanup
            .iter()
            .all(|event| !matches!(event, CapabilityRuntimeEvent::TimerFired(_)))
    );
}
