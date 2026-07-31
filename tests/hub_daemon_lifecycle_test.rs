#![cfg(unix)]

use std::collections::{BTreeMap, VecDeque};
use std::fs;
use std::io::{self, BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::os::fd::AsRawFd;
use std::os::unix::fs::PermissionsExt;
use std::os::unix::net::{UnixListener, UnixStream};
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::sync::{
    Arc, Mutex, OnceLock,
    atomic::{AtomicBool, Ordering},
    mpsc,
};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use botster_core::{
    AesGcmEnvelope, AesGcmKey, Capability, CapabilitySurface, CoreSessionMetadata,
    ExtensionEntrypoint, ExtensionKind, ExtensionRuntime, HostProfileMetadata,
    HostProfilePolicySection, PackageSource, ProcessIdentity, RequestId, ResizePayload, SessionId,
    SessionSpawnRequest, SpawnEnvironment, SpawnWorkingDirectory, SubscriptionId, decrypt_aes_gcm,
    encrypt_aes_gcm,
};
use botster_core_daemon::{RegistryRecord, SessionRegistry};
use botster_hub::{
    CoreEngineOptions, DataDirectoryOption, FileHubStateStore, HostIdentityOptions, HubClientApi,
    HubClientEvent, HubClientRequest, HubClientResponseBody, HubDaemon, HubDaemonState,
    HubPackageManifest, HubStartupOptions, HubStateLoadSource, HubStateStore,
    LOCAL_RUNTIME_DAEMON_READINESS_BUDGET, PackageAdmissionPolicy, PackageProvenance,
    PackageRegistry, RuntimeEnvironment, SessionDefaults, SpawnTarget, TransportBindings,
};
use portable_pty::{CommandBuilder, MasterPty, PtySize, native_pty_system};
use webrtc::data_channel::{DataChannel, DataChannelEvent, RTCDataChannelInit};
use webrtc::peer_connection::{
    PeerConnection, PeerConnectionBuilder, PeerConnectionEventHandler, RTCIceGatheringState,
    RTCPeerConnectionState, RTCSessionDescription,
};
use webrtc::runtime::{
    Receiver as AsyncReceiver, Sender as AsyncSender, block_on, channel, default_runtime, sleep,
    timeout,
};

mod support;
use support::{
    ensure_session_worker_binary, recovering_mutex_guard, validate_cli_daemon_shutdown,
    wait_for_cli_daemon_shutdown,
};

static REAL_DAEMON_TEST_LOCK: OnceLock<Mutex<()>> = OnceLock::new();
const OPERATOR_CONSOLE_READINESS_LIVENESS_BACKSTOP: Duration = Duration::from_secs(60);
const OPERATOR_CONSOLE_READER_DRAIN_BACKSTOP: Duration = Duration::from_secs(2);
const OPERATOR_CONSOLE_OUTPUT_PROGRESS_BACKSTOP: Duration = LOCAL_RUNTIME_DAEMON_READINESS_BUDGET;
const DETERMINISTIC_FOREGROUND_INTERRUPT_SCRIPT: &str = "trap '' INT; node -e 'process.on(\"SIGINT\", () => process.exit(130)); console.log(\"foreground-forward-ready\"); setInterval(() => {}, 1000)' & child=$!; wait \"$child\"";
const BOTSTER_WEB_READINESS_LIVENESS_BACKSTOP: Duration = Duration::from_secs(60);
const BOTSTER_WEB_READINESS_STARTUP_DELAY_MS: u64 = 3_000;
const STALLED_ATTACH_MIN_BUFFERED_STDOUT_BYTES: usize = 8 * 1024;
const STALLED_ATTACH_STABLE_SAMPLES: usize = 5;
const LOCAL_WEBRTC_SENDER_TERMINAL_RECORD_FILE: &str = "local-webrtc-sender-terminal.json";
const LOCAL_WEBRTC_SENDER_TERMINAL_RECORD_MAX_BYTES: usize = 4096;
const TEST_CLOSE_LOCAL_WEBRTC_OPERATION_ENV: &str = "BOTSTER_HUB_TEST_CLOSE_LOCAL_WEBRTC_OPERATION";
const TEST_LOCAL_RUNTIME_READINESS_BUDGET_MS_ENV: &str =
    "BOTSTER_HUB_TEST_LOCAL_RUNTIME_READINESS_BUDGET_MS";

fn unique_test_dir(name: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time after epoch")
        .as_nanos();
    PathBuf::from("target")
        .join("botster-hub-test-data")
        .join("daemon")
        .join(name)
        .join(nanos.to_string())
}

fn unique_short_test_dir(name: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time after epoch")
        .as_nanos();
    PathBuf::from("/tmp").join(format!("bh-{name}-{nanos}"))
}

fn explicit_config(data_directory: impl Into<PathBuf>) -> botster_hub::HubConfig {
    ensure_session_worker_binary();
    HubStartupOptions {
        host: HostIdentityOptions {
            id: "hub-daemon-test".to_string(),
            display_name: "Hub Daemon Test".to_string(),
            fingerprint: None,
        },
        data_directory: DataDirectoryOption::Explicit(data_directory.into()),
        session_defaults: SessionDefaults {
            shell: "/bin/sh".to_string(),
            working_directory: Some(".".into()),
            initial_rows: 24,
            initial_cols: 80,
        },
        transports: TransportBindings {
            ..TransportBindings::default()
        },
        ..HubStartupOptions::default()
    }
    .build_config_for_environment(&RuntimeEnvironment::from_values(None, None))
    .expect("explicit daemon config should build")
}

fn empty_registry() -> PackageRegistry {
    PackageRegistry::new(Vec::<Capability>::new().into_iter().collect())
}

fn spawn_request(config: &botster_hub::HubConfig) -> SessionSpawnRequest {
    SessionSpawnRequest {
        request_id: RequestId("hub-daemon-spawn".to_string()),
        session_id: SessionId("hub-daemon-session".to_string()),
        executable: config.session_defaults.shell.clone(),
        arguments: vec![
            "-c".to_string(),
            "printf 'daemon-ready\\n'; sleep 1".to_string(),
        ],
        working_directory: SpawnWorkingDirectory {
            path: config
                .session_defaults
                .working_directory
                .as_deref()
                .expect("test config has explicit working directory")
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

fn drain_until_client_output(
    api: &HubClientApi,
    runtime: &mut botster_hub::HubRuntime,
    packages: &PackageRegistry,
    session_id: &SessionId,
    needle: &[u8],
    logical_clock: &mut u64,
) -> Vec<HubClientEvent> {
    let mut observed = Vec::new();
    for _ in 0..100 {
        let response = api
            .handle_request(
                runtime,
                packages,
                HubClientRequest::DrainRuntime {
                    request_id: RequestId("hub-daemon-drain".to_string()),
                    session_id: session_id.clone(),
                    last_output_at: *logical_clock,
                },
            )
            .expect("drain through hub client api");
        *logical_clock += 1;
        let HubClientResponseBody::Events(events) = response.body else {
            panic!("drain should return events");
        };
        observed.extend(events);

        if observed.iter().any(|event| {
            matches!(
                event,
                HubClientEvent::TerminalOutput { data, .. }
                    if data.windows(needle.len()).any(|window| window == needle)
            )
        }) {
            return observed;
        }

        std::thread::sleep(std::time::Duration::from_millis(20));
    }

    panic!(
        "timed out waiting for {:?} in client output",
        String::from_utf8_lossy(needle)
    );
}

struct LocalWebrtcOffererHandler {
    gather_complete_tx: AsyncSender<()>,
    connected_tx: AsyncSender<()>,
}

#[async_trait::async_trait]
impl PeerConnectionEventHandler for LocalWebrtcOffererHandler {
    async fn on_ice_gathering_state_change(&self, state: RTCIceGatheringState) {
        if state == RTCIceGatheringState::Complete {
            let _ = self.gather_complete_tx.try_send(());
        }
    }

    async fn on_connection_state_change(&self, state: RTCPeerConnectionState) {
        if state == RTCPeerConnectionState::Connected {
            let _ = self.connected_tx.try_send(());
        }
    }
}

struct LocalWebrtcOfferPeer {
    peer: Box<dyn PeerConnection>,
    data_channel: Arc<dyn DataChannel>,
    connected_rx: AsyncReceiver<()>,
    data_channel_open_rx: AsyncReceiver<()>,
    data_channel_message_rx: AsyncReceiver<String>,
    pending_entity_frames: VecDeque<botster_hub_client::DaemonEntityFrame>,
}

impl LocalWebrtcOfferPeer {
    async fn create_offer() -> Result<(Self, serde_json::Value), Box<dyn std::error::Error>> {
        let runtime =
            default_runtime().ok_or_else(|| std::io::Error::other("no async runtime found"))?;
        let (gather_complete_tx, mut gather_complete_rx) = channel::<()>(1);
        let (connected_tx, connected_rx) = channel::<()>(1);
        let (data_channel_open_tx, data_channel_open_rx) = channel::<()>(1);
        let (data_channel_message_tx, data_channel_message_rx) = channel::<String>(256);
        let handler = Arc::new(LocalWebrtcOffererHandler {
            gather_complete_tx,
            connected_tx,
        });
        let peer = PeerConnectionBuilder::new()
            .with_handler(handler)
            .with_runtime(runtime.clone())
            .with_udp_addrs(vec!["127.0.0.1:0".to_string()])
            .build()
            .await?;
        let data_channel = peer
            .create_data_channel(
                "botster-client",
                Some(RTCDataChannelInit {
                    ordered: true,
                    max_retransmits: None,
                    max_packet_life_time: None,
                    ..Default::default()
                }),
            )
            .await?;
        assert!(data_channel.ordered().await?);
        assert_eq!(data_channel.max_retransmits().await?, None);
        assert_eq!(data_channel.max_packet_life_time().await?, None);

        {
            let data_channel = data_channel.clone();
            let open_tx = data_channel_open_tx.clone();
            let message_tx = data_channel_message_tx.clone();
            runtime.spawn(Box::pin(async move {
                while let Some(event) = data_channel.poll().await {
                    match event {
                        DataChannelEvent::OnOpen => {
                            let _ = open_tx.try_send(());
                        }
                        DataChannelEvent::OnMessage(message) => {
                            if let Ok(text) = String::from_utf8(message.data.to_vec()) {
                                let _ = message_tx.try_send(text);
                            }
                        }
                        DataChannelEvent::OnClose => break,
                        _ => {}
                    }
                }
            }));
        }

        let offer = peer.create_offer(None).await?;
        peer.set_local_description(offer).await?;
        let _ = timeout(Duration::from_secs(5), gather_complete_rx.recv()).await;
        let offer = peer
            .local_description()
            .await
            .ok_or_else(|| std::io::Error::other("offer local description missing"))?;
        let offer = serde_json::to_value(offer)?;

        Ok((
            Self {
                peer: Box::new(peer),
                data_channel,
                connected_rx,
                data_channel_open_rx,
                data_channel_message_rx,
                pending_entity_frames: VecDeque::new(),
            },
            offer,
        ))
    }

    async fn accept_answer(
        &mut self,
        answer: serde_json::Value,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let answer = serde_json::from_value::<RTCSessionDescription>(answer)?;
        self.peer.set_remote_description(answer).await?;
        timeout(Duration::from_secs(15), self.connected_rx.recv())
            .await
            .map_err(|_| std::io::Error::other("timed out waiting for WebRTC connection"))?;
        timeout(Duration::from_secs(10), self.data_channel_open_rx.recv())
            .await
            .map_err(|_| std::io::Error::other("timed out waiting for data channel open"))?;
        Ok(())
    }

    async fn encrypted_request(
        &mut self,
        key: &AesGcmKey,
        request: &botster_hub_client::DaemonRequest,
    ) -> Result<botster_hub_client::DaemonResponse, Box<dyn std::error::Error>> {
        Ok(self.encrypted_request_with_metrics(key, request).await?.0)
    }

    async fn encrypted_request_with_metrics(
        &mut self,
        key: &AesGcmKey,
        request: &botster_hub_client::DaemonRequest,
    ) -> Result<
        (
            botster_hub_client::DaemonResponse,
            LocalWebrtcResponseMetrics,
        ),
        Box<dyn std::error::Error>,
    > {
        let plaintext = serde_json::to_vec(request)?;
        let envelope = encrypt_aes_gcm(key, &plaintext, 1)?;
        self.data_channel
            .send_text(&serde_json::to_string(&envelope)?)
            .await?;
        loop {
            let (delivery_kind, plaintext, metrics) = self.receive_delivery(key).await?;
            match delivery_kind {
                botster_hub_client::DaemonLocalWebrtcDeliveryKind::DaemonResponse => {
                    return Ok((serde_json::from_slice(&plaintext)?, metrics));
                }
                botster_hub_client::DaemonLocalWebrtcDeliveryKind::DaemonEntityFrame => {
                    self.pending_entity_frames
                        .push_back(serde_json::from_slice(&plaintext)?);
                }
            }
        }
    }

    async fn next_entity_frame(
        &mut self,
        key: &AesGcmKey,
    ) -> Result<botster_hub_client::DaemonEntityFrame, Box<dyn std::error::Error>> {
        if let Some(frame) = self.pending_entity_frames.pop_front() {
            return Ok(frame);
        }
        let (delivery_kind, plaintext, _) = self.receive_delivery(key).await?;
        match delivery_kind {
            botster_hub_client::DaemonLocalWebrtcDeliveryKind::DaemonEntityFrame => {
                Ok(serde_json::from_slice(&plaintext)?)
            }
            botster_hub_client::DaemonLocalWebrtcDeliveryKind::DaemonResponse => {
                Err(std::io::Error::other(
                    "received uncorrelated daemon response while waiting for entity frame",
                )
                .into())
            }
        }
    }

    async fn receive_delivery(
        &mut self,
        key: &AesGcmKey,
    ) -> Result<
        (
            botster_hub_client::DaemonLocalWebrtcDeliveryKind,
            Vec<u8>,
            LocalWebrtcResponseMetrics,
        ),
        Box<dyn std::error::Error>,
    > {
        let mut encrypted = String::new();
        let mut delivery_kind = None;
        let mut message_id = None;
        let mut expected_chunk_count = None;
        let mut maximum_frame_bytes = 0;
        let mut next_chunk_index = 0;
        loop {
            let response =
                match timeout(Duration::from_secs(10), self.data_channel_message_rx.recv()).await {
                    Ok(Some(response)) => response,
                    Ok(None) => {
                        return Err(local_webrtc_response_progress_error(
                            "channel_closed",
                            message_id.as_deref(),
                            next_chunk_index,
                            expected_chunk_count,
                        )
                        .into());
                    }
                    Err(_) => {
                        return Err(local_webrtc_response_progress_error(
                            "response_timeout",
                            message_id.as_deref(),
                            next_chunk_index,
                            expected_chunk_count,
                        )
                        .into());
                    }
                };
            maximum_frame_bytes = maximum_frame_bytes.max(response.len());
            assert!(
                response.len() < botster_hub_client::LOCAL_WEBRTC_MAX_FRAME_BYTES,
                "response frame exceeded 64 KiB"
            );
            let chunk = serde_json::from_str::<botster_hub_client::DaemonLocalWebrtcDeliveryChunk>(
                &response,
            )?;
            assert_eq!(
                chunk.version,
                botster_hub_client::LOCAL_WEBRTC_DELIVERY_CHUNK_VERSION
            );
            if let Some(delivery_kind) = delivery_kind {
                assert_eq!(delivery_kind, chunk.delivery_kind);
            } else {
                delivery_kind = Some(chunk.delivery_kind);
            }
            assert_eq!(chunk.chunk_index, next_chunk_index);
            if let Some(message_id) = &message_id {
                assert_eq!(message_id, &chunk.message_id);
            } else {
                message_id = Some(chunk.message_id.clone());
                expected_chunk_count = Some(chunk.chunk_count);
            }
            assert_eq!(expected_chunk_count, Some(chunk.chunk_count));
            encrypted.push_str(&chunk.payload);
            next_chunk_index += 1;
            if chunk.chunk_index + 1 == chunk.chunk_count {
                assert_eq!(encrypted.len(), chunk.total_bytes as usize);
                break;
            }
        }
        let envelope_bytes = encrypted.len();
        let chunk_count = expected_chunk_count.unwrap_or(0) as usize;
        let envelope = serde_json::from_str::<AesGcmEnvelope>(&encrypted)?;
        let plaintext = decrypt_aes_gcm(key, &envelope)?;
        Ok((
            delivery_kind.expect("complete delivery declares a kind"),
            plaintext,
            LocalWebrtcResponseMetrics {
                envelope_bytes,
                chunk_count,
                maximum_frame_bytes,
            },
        ))
    }
}

#[derive(Debug)]
struct LocalWebrtcResponseMetrics {
    envelope_bytes: usize,
    chunk_count: usize,
    maximum_frame_bytes: usize,
}

fn local_webrtc_response_progress_error(
    cause: &str,
    message_id: Option<&str>,
    next_chunk_index: u32,
    expected_chunk_count: Option<u32>,
) -> std::io::Error {
    std::io::Error::other(format!(
        "local WebRTC response incomplete: cause={cause} message_id={} next_chunk={} expected_chunks={}",
        message_id.unwrap_or("pending"),
        next_chunk_index,
        expected_chunk_count.map_or_else(|| "pending".to_string(), |count| count.to_string()),
    ))
}

fn local_webrtc_stream_key(secret: &str) -> AesGcmKey {
    let hex = secret
        .strip_prefix("secret-")
        .expect("local WebRTC secret prefix");
    let bytes = decode_hex_bytes(hex).expect("local WebRTC secret hex");
    AesGcmKey::from_slice(&bytes).expect("local WebRTC secret is an AES-GCM key")
}

async fn open_local_webrtc_peer(
    endpoint: &botster_hub_client::DaemonEndpoint,
    bootstrap: &botster_hub_client::DaemonLocalWebrtcBootstrap,
) -> (LocalWebrtcOfferPeer, AesGcmKey) {
    let stream_key = local_webrtc_stream_key(&bootstrap.grant_secret);
    let (mut offer_peer, offer) = LocalWebrtcOfferPeer::create_offer()
        .await
        .expect("create WebRTC offer peer");
    let signal = botster_hub_client::request(
        endpoint,
        botster_hub_client::DaemonRequest::LocalWebrtcSignal {
            grant_id: bootstrap.grant_id.clone(),
            grant_secret: bootstrap.grant_secret.clone(),
            origin: bootstrap.expected_origin.clone(),
            offer,
        },
    )
    .expect("signal local WebRTC offer");
    assert_eq!(
        signal.kind,
        botster_hub_client::DaemonResponseKind::LocalWebrtcAnswer
    );
    let answer = signal
        .local_webrtc_answer
        .as_ref()
        .expect("signal response includes WebRTC answer")
        .answer
        .clone();
    offer_peer
        .accept_answer(answer)
        .await
        .expect("offer peer accepts answer and opens channel");
    (offer_peer, stream_key)
}

#[test]
fn generated_daemon_protocol_mirrors_core_aes_gcm_envelope_fields() {
    let envelope = AesGcmEnvelope {
        nonce: "base64-nonce".to_string(),
        ciphertext: "base64-ciphertext".to_string(),
        version: 1,
    };
    let value = serde_json::to_value(envelope).expect("core AES-GCM envelope serializes");
    let fields = value
        .as_object()
        .expect("core AES-GCM envelope serializes as object")
        .keys()
        .cloned()
        .collect::<Vec<_>>();
    assert_eq!(fields, vec!["ciphertext", "nonce", "version"]);

    let artifact = fs::read_to_string("crates/botster-hub-client/generated/daemon-protocol.ts")
        .expect("generated daemon protocol artifact is readable");
    let interface = generated_typescript_interface(&artifact, "AesGcmEnvelope");
    assert!(interface.contains("  nonce: string;"));
    assert!(interface.contains("  ciphertext: string;"));
    assert!(interface.contains("  version: number;"));
}

fn generated_typescript_interface(artifact: &str, name: &str) -> String {
    let start = artifact
        .find(&format!("export interface {name} {{"))
        .unwrap_or_else(|| panic!("generated daemon protocol should export {name}"));
    let rest = &artifact[start..];
    let end = rest
        .find("\n}\n")
        .unwrap_or_else(|| panic!("generated daemon protocol should close {name}"));
    rest[..end + 3].to_string()
}

fn assert_no_raw_html_ui_fields(value: &serde_json::Value) {
    match value {
        serde_json::Value::Object(object) => {
            for forbidden in ["html", "raw_html", "inner_html", "srcdoc"] {
                assert!(
                    !object.contains_key(forbidden),
                    "iframe render must expose an asset URL reference instead of raw HTML field {forbidden}"
                );
            }
            for child in object.values() {
                assert_no_raw_html_ui_fields(child);
            }
        }
        serde_json::Value::Array(items) => {
            for item in items {
                assert_no_raw_html_ui_fields(item);
            }
        }
        _ => {}
    }
}

fn decode_hex_bytes(encoded: &str) -> Option<Vec<u8>> {
    if !encoded.len().is_multiple_of(2) {
        return None;
    }
    let mut output = Vec::with_capacity(encoded.len() / 2);
    for pair in encoded.as_bytes().chunks_exact(2) {
        let high = decode_hex_nibble(pair[0])?;
        let low = decode_hex_nibble(pair[1])?;
        output.push((high << 4) | low);
    }
    Some(output)
}

fn decode_hex_nibble(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

fn package_provenance() -> PackageProvenance {
    PackageProvenance {
        source: "https://example.invalid/botster/packages/provider".to_string(),
        checksum: Some("sha256:daemon-test".to_string()),
    }
}

fn provider_manifest() -> HubPackageManifest {
    let capabilities = vec![Capability {
        surface: CapabilitySurface::Surfaces,
        scope: None,
    }];

    HubPackageManifest {
        name: "daemon.provider".to_string(),
        version: "1.0.0".to_string(),
        kind: ExtensionKind::Provider,
        botster: ">=0.1.0".to_string(),
        source: Some(PackageSource::Git {
            repo: "https://example.invalid/botster/provider.git".to_string(),
            reference: "v1.0.0".to_string(),
        }),
        capabilities: capabilities.clone(),
        entrypoints: vec![ExtensionEntrypoint {
            runtime: ExtensionRuntime::Process,
            path: "bin/provider".to_string(),
            bootstrap: true,
        }],
        dependencies: Vec::new(),
        features: Vec::new(),
        host_profile: Some(HostProfileMetadata {
            profile_id: "daemon-provider".to_string(),
            compatibility: ">=0.1.0".to_string(),
            precedence: 10,
            required_providers: Vec::new(),
            required_capabilities: capabilities,
            policy_sections: vec![HostProfilePolicySection::Providers],
        }),
        configuration: None,
        surfaces: Vec::new(),
        runnable_entrypoints: Vec::new(),
        navigation: Vec::new(),
    }
}

fn write_local_plugin_package(root: &Path) {
    fs::create_dir_all(root).expect("create local package root");
    fs::create_dir_all(root.join("bin")).expect("create local package bin");
    fs::write(root.join("plugin.lua"), "return botster.register({})\n")
        .expect("write plugin entrypoint");
    fs::write(root.join("bin/botster-web"), "#!/bin/sh\n")
        .expect("write runnable package entrypoint");
    fs::write(
        root.join("botster-package.json"),
        r#"{
  "name": "runtime.plugin",
  "version": "1.0.0",
  "kind": "plugin",
  "botster": ">=0.1.0",
  "source": { "type": "path", "path": "." },
  "capabilities": [
    { "surface": "surfaces" }
  ],
  "entrypoints": [
    { "runtime": "lua", "path": "plugin.lua", "bootstrap": false }
  ],
  "runnable_entrypoints": [
    {
      "id": "web",
      "kind": "web_app",
      "command": "bin/botster-web",
      "args": ["--host", "127.0.0.1"],
      "working_directory": { "policy": "package_root" },
      "environment": [
        { "name": "BOTSTER_WEB_PORT", "required": false, "default": "5173" }
      ],
      "launch_mode": "background",
      "capabilities": [
        { "surface": "network", "scope": "localhost" }
      ],
      "may_supervise": true
    }
  ]
}
"#,
    )
    .expect("write local package manifest");
}

fn write_managed_git_session_package(root: &Path) {
    fs::create_dir_all(root.join("bin")).expect("create managed Git package root");
    fs::write(
        root.join("plugin.lua"),
        r#"
return botster.register({
  tools = {{
    name = "managed_git.live_spawn",
    description = "Exercise the live Hub managed Git session path.",
    handler = "live_spawn",
    call = function(args)
      return botster.capabilities.session_templates.ensure_worktree_and_spawn(args)
    end,
  }},
})
"#,
    )
    .expect("write managed Git plugin");
    let script = root.join("bin/init.sh");
    fs::write(
        &script,
        "#!/bin/sh\nprintf 'live-managed\\n' > live-managed.txt\n",
    )
    .expect("write managed Git session command");
    fs::set_permissions(&script, fs::Permissions::from_mode(0o755))
        .expect("make managed Git session command executable");
    let source_root = fs::canonicalize(root).expect("canonical managed Git package root");
    fs::write(
        root.join("botster-package.json"),
        serde_json::to_vec_pretty(&serde_json::json!({
            "name": "managed-git.live-plugin",
            "version": "1.0.0",
            "kind": "plugin",
            "botster": ">=0.1.0",
            "source": { "type": "path", "path": source_root },
            "capabilities": [
                { "surface": "mcp" },
                {
                    "surface": "session_actions",
                    "scope": "session_template_managed_git_spawn"
                }
            ],
            "entrypoints": [
                { "runtime": "lua", "path": "plugin.lua", "bootstrap": false }
            ],
            "session_templates": [{
                "id": "init",
                "command": "bin/init.sh",
                "target_id": "tgt_live_managed"
            }]
        }))
        .expect("serialize managed Git package"),
    )
    .expect("write managed Git package manifest");
}

fn write_configurable_local_plugin_package(root: &Path) {
    fs::create_dir_all(root).expect("create configurable package root");
    fs::write(root.join("plugin.lua"), "return botster.register({})\n")
        .expect("write plugin entrypoint");
    fs::write(
        root.join("botster-package.json"),
        r#"{
  "name": "configurable.plugin",
  "version": "1.0.0",
  "kind": "plugin",
  "botster": ">=0.1.0",
  "source": { "type": "path", "path": "." },
  "capabilities": [
    { "surface": "surfaces" }
  ],
  "entrypoints": [
    { "runtime": "lua", "path": "plugin.lua", "bootstrap": false }
  ],
  "surfaces": [{
    "id": "config.home",
    "kind": "app",
    "title": "Config Home",
    "description": "Configuration workbench",
    "icon": "settings",
    "order": 10,
    "category": "configuration",
    "supports": ["render", "action"]
  }],
  "configuration": {
    "fields": [
      {
        "key": "endpoint",
        "type": "url",
        "label": "Endpoint",
        "required": true
      },
      {
        "key": "mode",
        "type": "select",
        "label": "Mode",
        "default": { "type": "select", "value": "read" },
        "options": [
          { "value": "read", "label": "Read" }
        ]
      },
      {
        "key": "api_token",
        "type": "secret",
        "label": "API token",
        "required": true,
        "default": { "type": "secret", "state": "unset" }
      }
    ]
  }
}
"#,
    )
    .expect("write configurable package manifest");
}

fn write_explicit_navigation_local_plugin_package(root: &Path) {
    fs::create_dir_all(root).expect("create navigation package root");
    fs::write(root.join("plugin.lua"), "return botster.register({})\n")
        .expect("write plugin entrypoint");
    fs::write(
        root.join("botster-package.json"),
        r#"{
  "name": "navigation.plugin",
  "version": "1.0.0",
  "kind": "plugin",
  "botster": ">=0.1.0",
  "source": { "type": "path", "path": "." },
  "capabilities": [
    { "surface": "surfaces" }
  ],
  "entrypoints": [
    { "runtime": "lua", "path": "plugin.lua", "bootstrap": false }
  ],
  "surfaces": [{
    "id": "workbench",
    "kind": "app",
    "title": "Workbench",
    "description": "Navigation workbench",
    "icon": "workflow",
    "order": 100,
    "category": "workflows",
    "supports": ["render", "action"]
  }],
  "navigation": [{
    "id": "primary",
    "label": "Primary Workbench",
    "icon": "workflow",
    "description": "Open the workbench",
    "target": { "kind": "surface", "surface_id": "workbench" }
  }]
}
"#,
    )
    .expect("write explicit navigation package manifest");
}

fn write_iframe_surface_local_plugin_package(root: &Path) {
    fs::create_dir_all(root.join("assets")).expect("create iframe package assets");
    fs::write(root.join("assets/preview.html"), "<main>Preview</main>\n")
        .expect("write iframe asset");
    fs::write(
        root.join("plugin.lua"),
        r#"local function render_preview(_arguments)
  return {
    type = "iframe",
    id = "preview-frame",
    props = {
      src = "/packages/iframe.plugin/assets/preview.html",
      title = "Preview"
    }
  }
end

return botster.register({
  handlers = {
    {
      id = "preview_surface",
      kind = "surface_route",
      descriptor_id = "preview",
      descriptor = {
        title = "Preview",
        surface_id = "preview",
      },
      call = render_preview,
    },
  },
})
"#,
    )
    .expect("write iframe plugin entrypoint");
    fs::write(
        root.join("botster-package.json"),
        r#"{
  "name": "iframe.plugin",
  "version": "1.0.0",
  "kind": "plugin",
  "botster": ">=0.1.0",
  "source": { "type": "path", "path": "." },
  "capabilities": [
    { "surface": "surfaces" }
  ],
  "entrypoints": [
    { "runtime": "lua", "path": "plugin.lua", "bootstrap": false }
  ],
  "surfaces": [{
    "id": "preview",
    "kind": "app",
    "title": "Preview",
    "description": "Iframe preview",
    "icon": "panel-top",
    "order": 30,
    "category": "previews",
    "supports": ["render"]
  }]
}
"#,
    )
    .expect("write iframe package manifest");
}

fn write_project_pipelines_availability_package(root: &Path) {
    fs::create_dir_all(root).expect("create project pipelines package root");
    fs::write(root.join("plugin.lua"), "return botster.register({})\n")
        .expect("write plugin entrypoint");
    fs::write(
        root.join("botster-package.json"),
        r#"{
  "name": "project-pipelines",
  "version": "1.0.0",
  "kind": "plugin",
  "botster": ">=0.1.0",
  "source": { "type": "path", "path": "." },
  "capabilities": [
    { "surface": "surfaces" },
    { "surface": "mcp" },
    { "surface": "plugin_db", "scope": "project-pipelines" }
  ],
  "entrypoints": [
    { "runtime": "lua", "path": "plugin.lua", "bootstrap": false }
  ],
  "dependencies": [
    {
      "id": "github-provider",
      "package": "github-provider",
      "kind": "optional",
      "feature": "github_pr_lifecycle",
      "requirements": [
        { "type": "provider", "provider": "github-provider" }
      ]
    }
  ],
  "features": [
    {
      "id": "local_pipelines",
      "label": "Local pipelines"
    },
    {
      "id": "github_pr_lifecycle",
      "label": "GitHub PR lifecycle",
      "dependencies": ["github-provider"],
      "requirements": [
        { "type": "config", "key": "github_app_id" },
        { "type": "auth", "key": "github_token" }
      ]
    }
  ]
}
"#,
    )
    .expect("write project pipelines availability manifest");
}

fn write_required_dependency_package(root: &Path) {
    fs::create_dir_all(root).expect("create required dependency package root");
    fs::write(root.join("plugin.lua"), "return botster.register({})\n")
        .expect("write required dependency plugin entrypoint");
    fs::write(
        root.join("botster-package.json"),
        r#"{
  "name": "dependency-blocked.plugin",
  "version": "1.0.0",
  "kind": "plugin",
  "botster": ">=0.1.0",
  "source": { "type": "path", "path": "." },
  "capabilities": [
    { "surface": "surfaces" }
  ],
  "entrypoints": [
    { "runtime": "lua", "path": "plugin.lua", "bootstrap": false }
  ],
  "dependencies": [
    {
      "id": "github-provider",
      "package": "github-provider",
      "kind": "required",
      "requirements": [
        { "type": "provider", "provider": "github-provider" }
      ]
    }
  ]
}
"#,
    )
    .expect("write required dependency package manifest");
}

fn write_supervised_package(root: &Path, package_name: &str, command: &str, args: &[&str]) {
    fs::create_dir_all(root).expect("create supervised package root");
    fs::write(root.join("plugin.lua"), "return botster.register({})\n")
        .expect("write plugin entrypoint");
    let manifest = serde_json::json!({
        "name": package_name,
        "version": "1.0.0",
        "kind": "plugin",
        "botster": ">=0.1.0",
        "source": { "type": "path", "path": "." },
        "capabilities": [{ "surface": "surfaces" }],
        "entrypoints": [
            { "runtime": "lua", "path": "plugin.lua", "bootstrap": false }
        ],
        "runnable_entrypoints": [{
            "id": "web",
            "kind": "web_app",
            "command": command,
            "args": args,
            "working_directory": { "policy": "package_root" },
            "launch_mode": "background",
            "capabilities": [{ "surface": "network", "scope": "localhost" }],
            "may_supervise": true
        }]
    });
    fs::write(
        root.join("botster-package.json"),
        serde_json::to_string_pretty(&manifest).expect("serialize supervised manifest"),
    )
    .expect("write supervised package manifest");
}

fn write_session_template_context_package(root: &Path) {
    fs::create_dir_all(root.join("bin")).expect("create session template package root");
    fs::write(root.join("plugin.lua"), "return botster.register({})\n")
        .expect("write session template plugin entrypoint");
    let script = root.join("bin/init.sh");
    fs::write(
        &script,
        "#!/bin/sh\nprintf 'started\\n' > context-started.txt\n\"$BOTSTER_HUB_BIN\" context --key prompt > context-output.json 2> context-error.txt\nsleep 1\n",
    )
    .expect("write session template script");
    let mut permissions = fs::metadata(&script)
        .expect("script metadata")
        .permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(&script, permissions).expect("chmod session template script");
    let manifest = serde_json::json!({
        "name": "runtime.session-template",
        "version": "1.0.0",
        "kind": "plugin",
        "botster": ">=0.1.0",
        "source": { "type": "path", "path": "." },
        "capabilities": [{ "surface": "surfaces" }],
        "entrypoints": [
            { "runtime": "lua", "path": "plugin.lua", "bootstrap": false }
        ],
        "session_templates": [{
            "id": "init",
            "command": "bin/init.sh",
            "context": ["prompt"],
            "allowed_environment_overrides": ["BOTSTER_MODE"],
            "environment": { "BOTSTER_MODE": "daemon" }
        }]
    });
    fs::write(
        root.join("botster-package.json"),
        serde_json::to_string_pretty(&manifest).expect("serialize session template manifest"),
    )
    .expect("write session template package manifest");
}

fn write_app_registry_package(root: &Path) {
    fs::create_dir_all(root).expect("create app registry package root");
    fs::write(root.join("plugin.lua"), "return botster.register({})\n")
        .expect("write plugin entrypoint");
    let manifest = serde_json::json!({
        "name": "runtime.apps",
        "version": "1.0.0",
        "kind": "plugin",
        "botster": ">=0.1.0",
        "source": { "type": "path", "path": "." },
        "capabilities": [{ "surface": "surfaces" }],
        "entrypoints": [
            { "runtime": "lua", "path": "plugin.lua", "bootstrap": false }
        ],
        "runnable_entrypoints": [
            {
                "id": "web",
                "kind": "web_app",
                "command": "sh",
                "args": ["-c", "echo 'http://127.0.0.1:59999'; printf '%s\n' '{\"entrypoint_id\":\"web\",\"process_state\":\"running\",\"local_url\":\"http://127.0.0.1:49152\"}' > \"$BOTSTER_ENTRYPOINT_LAUNCH_RESULT\"; while true; do sleep 1; done"],
                "working_directory": { "policy": "package_root" },
                "launch_mode": "background",
                "readiness": { "result_fields": ["local_url"] },
                "capabilities": [{ "surface": "network", "scope": "localhost" }],
                "may_supervise": true
            },
            {
                "id": "terminal",
                "kind": "terminal_app",
                "command": "sh",
                "args": ["-c", "echo terminal"],
                "working_directory": { "policy": "package_root" },
                "launch_mode": "foreground_stdio",
                "may_supervise": true
            }
        ]
    });
    fs::write(
        root.join("botster-package.json"),
        serde_json::to_string_pretty(&manifest).expect("serialize app registry manifest"),
    )
    .expect("write app registry package manifest");
}

fn write_reloadable_app_package(root: &Path, version: &str, local_url: &str) {
    write_reloadable_app_package_named(root, "runtime.reloadable", version, local_url);
}

fn write_reloadable_app_package_named(root: &Path, name: &str, version: &str, local_url: &str) {
    fs::create_dir_all(root).expect("create reloadable app package root");
    fs::write(root.join("plugin.lua"), "return botster.register({})\n")
        .expect("write reloadable app plugin entrypoint");
    let command = format!(
        "printf '%s\n' '{{\"entrypoint_id\":\"web\",\"process_state\":\"running\",\"local_url\":\"{local_url}\"}}' > \"$BOTSTER_ENTRYPOINT_LAUNCH_RESULT\"; while true; do sleep 1; done"
    );
    let manifest = serde_json::json!({
        "name": name,
        "version": version,
        "kind": "plugin",
        "botster": ">=0.1.0",
        "source": { "type": "path", "path": "." },
        "capabilities": [{ "surface": "surfaces" }],
        "entrypoints": [
            { "runtime": "lua", "path": "plugin.lua", "bootstrap": false }
        ],
        "runnable_entrypoints": [{
            "id": "web",
            "kind": "web_app",
            "command": "sh",
            "args": ["-c", command],
            "working_directory": { "policy": "package_root" },
            "launch_mode": "background",
            "readiness": { "result_fields": ["local_url"] },
            "capabilities": [{ "surface": "network", "scope": "localhost" }],
            "may_supervise": true
        }]
    });
    fs::write(
        root.join("botster-package.json"),
        serde_json::to_string_pretty(&manifest).expect("serialize reloadable app package manifest"),
    )
    .expect("write reloadable app package manifest");
}

fn write_hub_env_web_app_package(root: &Path) {
    fs::create_dir_all(root).expect("create hub-env web package root");
    fs::write(root.join("plugin.lua"), "return botster.register({})\n")
        .expect("write hub-env web package core entrypoint");
    fs::write(
        root.join("verify-hub-connection.mjs"),
        r#"import fs from 'node:fs';

const connection = JSON.parse(process.env.BOTSTER_HUB_CONNECTION || 'null');
if (connection?.transport?.type !== 'unix_socket') {
  throw new Error('BOTSTER_HUB_CONNECTION must declare a unix_socket transport');
}
if (!connection.transport.path.startsWith('/') || !fs.existsSync(connection.transport.path)) {
  throw new Error('BOTSTER_HUB_CONNECTION must carry the active absolute socket path');
}
if (!process.env.PACKAGE_DATA_DIR || !fs.statSync(process.env.PACKAGE_DATA_DIR).isDirectory()) {
  throw new Error('PACKAGE_DATA_DIR must carry the active Hub data directory');
}
if (process.env.BOTSTER_WEB_MODE !== 'daemon-default') {
  throw new Error('manifest environment defaults must be preserved');
}
fs.writeFileSync(process.env.BOTSTER_ENTRYPOINT_LAUNCH_RESULT, JSON.stringify({
  entrypoint_id: 'web',
  process_state: 'running',
  local_url: 'http://127.0.0.1:49153',
}));
setInterval(() => {}, 1000);
"#,
    )
    .expect("write hub connection verifier");
    let manifest = serde_json::json!({
        "name": "runtime.hub-env",
        "version": "1.0.0",
        "kind": "plugin",
        "botster": ">=0.1.0",
        "source": { "type": "path", "path": "." },
        "capabilities": [{ "surface": "surfaces" }],
        "entrypoints": [
            { "runtime": "lua", "path": "plugin.lua", "bootstrap": false }
        ],
        "runnable_entrypoints": [{
            "id": "web",
            "kind": "web_app",
            "command": "node",
            "args": [
                "verify-hub-connection.mjs"
            ],
            "working_directory": { "policy": "package_root" },
            "injections": [
                {
                    "kind": "hub_connection",
                    "target": {
                        "type": "environment",
                        "name": "BOTSTER_HUB_CONNECTION"
                    },
                    "required": true
                },
                {
                    "kind": "data_dir",
                    "target": {
                        "type": "environment",
                        "name": "PACKAGE_DATA_DIR"
                    },
                    "required": true
                }
            ],
            "environment": [
                { "name": "BOTSTER_WEB_MODE", "required": false, "default": "daemon-default" }
            ],
            "launch_mode": "background",
            "readiness": { "result_fields": ["local_url"] },
            "capabilities": [{ "surface": "network", "scope": "localhost" }],
            "may_supervise": true
        }]
    });
    fs::write(
        root.join("botster-package.json"),
        serde_json::to_string_pretty(&manifest).expect("serialize hub-env web package manifest"),
    )
    .expect("write hub-env web package manifest");
}

fn write_botster_tui_package(root: &Path) {
    write_botster_tui_package_with_script(
        root,
        "test -n \"$BOTSTER_HUB_CONNECTION\" && test -n \"$BOTSTER_HUB_DATA_DIR\" && printf 'botster-tui-fixture\\n'",
    );
}

fn write_botster_tui_package_with_script(root: &Path, script: &str) {
    fs::create_dir_all(root).expect("create botster-tui package root");
    let manifest = serde_json::json!({
        "name": "botster-tui",
        "version": "1.0.0",
        "kind": "plugin",
        "botster": ">=0.1.0",
        "source": { "type": "path", "path": "." },
        "capabilities": [{ "surface": "surfaces" }],
        "entrypoints": [],
        "runnable_entrypoints": [{
            "id": "botster-tui",
            "kind": "terminal_app",
            "command": "sh",
            "args": ["-c", script],
            "working_directory": { "policy": "package_root" },
            "injections": [
                {
                    "kind": "hub_connection",
                    "target": {
                        "type": "environment",
                        "name": "BOTSTER_HUB_CONNECTION"
                    },
                    "required": true
                },
                {
                    "kind": "data_dir",
                    "target": {
                        "type": "environment",
                        "name": "BOTSTER_HUB_DATA_DIR"
                    },
                    "required": true
                }
            ],
            "environment": [
                { "name": "BOTSTER_TUI_MODE", "required": false, "default": "headless" }
            ],
            "launch_mode": "foreground_stdio"
        }]
    });
    fs::write(
        root.join("botster-package.json"),
        serde_json::to_string_pretty(&manifest).expect("serialize botster-tui manifest"),
    )
    .expect("write botster-tui manifest");
}

fn write_botster_web_package(root: &Path) {
    fs::create_dir_all(root.join("scripts")).expect("create botster-web package root");
    fs::write(root.join("plugin.lua"), "return botster.register({})\n")
        .expect("write botster-web core entrypoint");
    fs::write(
        root.join("scripts").join("local-package-server.mjs"),
        r#"
import fs from 'fs';
import http from 'http';
import net from 'net';

const port = Number(process.env.BOTSTER_WEB_PORT || '0');
const connection = JSON.parse(process.env.BOTSTER_HUB_CONNECTION || 'null');
const socket = connection?.transport?.type === 'unix_socket'
  ? connection.transport.path
  : undefined;
const dataDir = process.env.BOTSTER_HUB_DATA_DIR;
const launchResult = process.env.BOTSTER_ENTRYPOINT_LAUNCH_RESULT;
const source = socket ? 'socket' : (dataDir ? 'data_dir' : 'spawned');
const mode = socket || dataDir ? 'existing_hub' : 'spawned_hub';
const startupDelayMs = Number(process.env.BOTSTER_WEB_TEST_STARTUP_DELAY_MS || '0');
const connections = new Map();
let boundPort = null;

function currentRequirement() {
  return {
    protocol: 'botster-hub-daemon-v1',
    minimum_protocol_version: 1,
    required_features: [
      'sessions',
      'terminal_streaming',
      'resize',
      'plugin_surface_render',
      'plugin_surface_action',
    ],
    minimum_conformance_fixture_revision: 1,
    client_name: 'botster-web-production-runtime-fixture',
  };
}

function readLine(connection) {
  const newline = connection.buffer.indexOf('\n');
  if (newline >= 0) {
    const line = connection.buffer.slice(0, newline);
    connection.buffer = connection.buffer.slice(newline + 1);
    return Promise.resolve(line);
  }

  return new Promise((resolve, reject) => {
    const onData = (chunk) => {
      connection.buffer += chunk.toString('utf8');
      const newline = connection.buffer.indexOf('\n');
      if (newline < 0) {
        return;
      }
      cleanup();
      const line = connection.buffer.slice(0, newline);
      connection.buffer = connection.buffer.slice(newline + 1);
      resolve(line);
    };
    const onError = (error) => {
      cleanup();
      reject(error);
    };
    const cleanup = () => {
      connection.stream.off('data', onData);
      connection.stream.off('error', onError);
    };
    connection.stream.on('data', onData);
    connection.stream.once('error', onError);
  });
}

async function connectDaemon() {
  if (!socket) {
    throw new Error('BOTSTER_HUB_CONNECTION does not contain a Unix socket');
  }
  const stream = net.createConnection(socket);
  const connection = { stream, buffer: '' };
  try {
    await new Promise((resolve, reject) => {
      stream.once('connect', resolve);
      stream.once('error', reject);
    });
    stream.write(JSON.stringify({
      protocol: 'botster-hub-daemon-v1',
      compatibility: currentRequirement(),
    }) + '\n');
    const helloAck = JSON.parse(await readLine(connection));
    if (helloAck.protocol !== 'botster-hub-daemon-v1' || !helloAck.compatibility) {
      throw new Error(`unexpected daemon hello ack: ${JSON.stringify(helloAck)}`);
    }
    return connection;
  } catch (error) {
    stream.destroy();
    throw error;
  }
}

async function probeDaemon() {
  let connection = null;
  try {
    connection = await connectDaemon();
    connection.stream.write(JSON.stringify({ type: 'status' }) + '\n');
    const response = JSON.parse(await readLine(connection));
    if (response.kind !== 'status' || !response.status) {
      throw new Error(`unexpected daemon status response: ${JSON.stringify(response)}`);
    }
  } finally {
    connection?.stream.destroy();
  }
}

function currentSocketExists() {
  return socket ? fs.existsSync(socket) : false;
}

async function daemonRequest(payload) {
  const connectionId = payload.connection_id || null;
  let connection = connectionId ? connections.get(connectionId) : null;
  if (!connection) {
    connection = await connectDaemon();
    if (connectionId) {
      connections.set(connectionId, connection);
    }
  }

  connection.stream.write(JSON.stringify(payload.request) + '\n');
  const response = JSON.parse(await readLine(connection));

  if (!connectionId || payload.close === true) {
    connection.stream.end();
    if (connectionId) {
      connections.delete(connectionId);
    }
  }

  return response;
}

const server = http.createServer(async (request, response) => {
  if (request.url === '/') {
    try {
      let bootstrap = null;
      if (socket && fs.existsSync(socket)) {
        const origin = `http://${request.headers.host}`;
        try {
          const daemonResponse = await daemonRequest({
            request: {
              type: 'issue_local_webrtc_bootstrap',
              package_name: 'botster-web',
              entrypoint_id: 'web-client',
              origin,
            },
          });
          bootstrap = daemonResponse.local_webrtc_bootstrap || null;
          if (!bootstrap) {
            throw new Error(`missing local WebRTC bootstrap: ${JSON.stringify(daemonResponse)}`);
          }
        } catch (error) {
          if (!String(error && error.message ? error.message : error).includes('ENOENT')) {
            throw error;
          }
        }
      }
      response.writeHead(200, { 'content-type': 'text/html; charset=utf-8' });
      response.end(`<!doctype html><html><head><title>Botster Web</title><script>globalThis.__BOTSTER_LOCAL_WEBRTC_BOOTSTRAP__ = ${JSON.stringify(bootstrap)};</script></head><body><main id="root">botster-web packaged UI</main><script type="module" src="/assets/index.js"></script></body></html>`);
    } catch (error) {
      response.writeHead(502, { 'content-type': 'text/plain; charset=utf-8' });
      response.end(String(error && error.message ? error.message : error));
    }
    return;
  }
  if (request.url !== '/health') {
    response.writeHead(404);
    response.end('not found');
    return;
  }
  let daemonReady = false;
  let error = null;
  try {
    await probeDaemon();
    daemonReady = true;
  } catch (probeError) {
    error = String(probeError && probeError.message ? probeError.message : probeError).slice(0, 240);
  }
  const socketExists = currentSocketExists();
  response.writeHead(200, { 'content-type': 'application/json' });
  response.end(JSON.stringify({
    ok: mode === 'existing_hub' && source === 'socket' && daemonReady,
    mode,
    source,
    port: boundPort,
    socketExists,
    daemonReady,
    error,
  }));
});

const listen = () => server.listen(port, '127.0.0.1', () => {
  boundPort = server.address().port;
  console.log(`web_listening=http://127.0.0.1:${boundPort}`);
  if (launchResult) {
    fs.writeFileSync(launchResult, JSON.stringify({
      entrypoint_id: 'web-client',
      process_state: 'running',
      local_url: `http://127.0.0.1:${boundPort}/`,
    }));
  }
});

if (startupDelayMs > 0) {
  setTimeout(listen, startupDelayMs);
} else {
  listen();
}
"#,
    )
    .expect("write botster-web package server script");
    let manifest = serde_json::json!({
        "name": "botster-web",
        "version": "1.0.0",
        "kind": "plugin",
        "botster": ">=0.1.0",
        "source": { "type": "path", "path": "." },
        "capabilities": [{ "surface": "surfaces" }],
        "entrypoints": [
            { "runtime": "lua", "path": "plugin.lua", "bootstrap": false }
        ],
        "runnable_entrypoints": [{
            "id": "web-client",
            "kind": "web_app",
            "command": "node",
            "args": ["scripts/local-package-server.mjs"],
            "working_directory": { "policy": "package_root" },
            "injections": [
                {
                    "kind": "hub_connection",
                    "target": {
                        "type": "environment",
                        "name": "BOTSTER_HUB_CONNECTION"
                    },
                    "required": true
                },
                {
                    "kind": "data_dir",
                    "target": {
                        "type": "environment",
                        "name": "BOTSTER_HUB_DATA_DIR"
                    },
                    "required": true
                }
            ],
            "environment": [
                { "name": "BOTSTER_WEB_PORT", "required": false, "default": "0" },
                { "name": "BOTSTER_WEB_TEST_STARTUP_DELAY_MS", "required": false }
            ],
            "launch_mode": "background",
            "readiness": { "result_fields": ["local_url"] },
            "capabilities": [{ "surface": "network", "scope": "localhost" }],
            "may_supervise": true
        }]
    });
    fs::write(
        root.join("botster-package.json"),
        serde_json::to_string_pretty(&manifest).expect("serialize botster-web manifest"),
    )
    .expect("write botster-web manifest");
}

fn rewrite_botster_web_entrypoint(
    root: &Path,
    version: &str,
    script_name: &str,
    marker_name: &str,
) {
    let original = fs::read_to_string(root.join("scripts/local-package-server.mjs"))
        .expect("read original botster-web entrypoint");
    let original = original
        .strip_prefix("#!/usr/bin/env node\n")
        .unwrap_or(&original);
    let marker = format!(
        "fs.writeFileSync(new URL('../{marker_name}', import.meta.url), 'refreshed');\nconst port ="
    );
    let refreshed = format!(
        "#!/usr/bin/env node\n{}",
        original.replacen("const port =", &marker, 1)
    );
    let script_path = root.join("scripts").join(script_name);
    fs::write(&script_path, refreshed).expect("write refreshed botster-web entrypoint");
    fs::set_permissions(&script_path, fs::Permissions::from_mode(0o755))
        .expect("make refreshed botster-web entrypoint executable");

    let manifest_path = root.join("botster-package.json");
    let mut manifest: serde_json::Value = serde_json::from_str(
        &fs::read_to_string(&manifest_path).expect("read botster-web manifest"),
    )
    .expect("parse botster-web manifest");
    manifest["version"] = serde_json::Value::String(version.to_string());
    manifest["runnable_entrypoints"][0]["command"] =
        serde_json::Value::String(format!("scripts/{script_name}"));
    fs::write(
        manifest_path,
        serde_json::to_string_pretty(&manifest).expect("serialize refreshed botster-web manifest"),
    )
    .expect("write refreshed botster-web manifest");
}

fn enable_supervised_package(data_dir: &Path, package_dir: &Path) {
    let response = botster_hub::daemon_transport_request(
        &explicit_config(data_dir),
        botster_hub::DaemonRequest::EnablePackageLocalPath {
            path: package_dir.to_path_buf(),
        },
    )
    .expect("enable supervised package");
    assert_eq!(
        response.kind,
        botster_hub::DaemonResponseKind::PackageDecision
    );
}

fn package_entrypoint<'a>(
    response: &'a botster_hub::DaemonResponse,
    package_name: &str,
) -> &'a botster_hub::DaemonPackageRunnableEntrypoint {
    response
        .packages
        .iter()
        .find(|package| package.package_name == package_name)
        .expect("response includes package")
        .runnable_entrypoints
        .iter()
        .find(|entrypoint| entrypoint.id == "web")
        .expect("response includes web entrypoint")
}

fn app_row<'a>(
    response: &'a botster_hub::DaemonResponse,
    entrypoint_id: &str,
) -> &'a botster_hub::DaemonApp {
    response
        .apps
        .iter()
        .find(|app| app.entrypoint_id == entrypoint_id)
        .unwrap_or_else(|| panic!("response includes app for entrypoint {entrypoint_id}"))
}

fn package_route<'a>(
    routes: &'a [botster_hub_client::DaemonPackageRouteDescriptor],
    route_id: &str,
) -> &'a botster_hub_client::DaemonPackageRouteDescriptor {
    routes
        .iter()
        .find(|route| route.route_id == route_id)
        .unwrap_or_else(|| panic!("response includes package route {route_id}"))
}

fn package_navigation<'a>(
    entries: &'a [botster_hub_client::DaemonPackageNavigationEntry],
    package_name: &str,
    item_id: &str,
) -> &'a botster_hub_client::DaemonPackageNavigationEntry {
    entries
        .iter()
        .find(|entry| entry.package_name == package_name && entry.item_id == item_id)
        .unwrap_or_else(|| panic!("response includes navigation {package_name}/{item_id}"))
}

fn wait_for_app_local_url(
    data_dir: &Path,
    entrypoint_id: &str,
    expected_url: &str,
) -> botster_hub::DaemonResponse {
    let mut last_response = None;
    for _ in 0..50 {
        let response = botster_hub::daemon_transport_request(
            &explicit_config(data_dir),
            botster_hub::DaemonRequest::ListApps,
        )
        .expect("list apps while waiting for local url");
        if app_row(&response, entrypoint_id)
            .launch_target
            .local_url
            .as_deref()
            == Some(expected_url)
        {
            return response;
        }
        last_response = Some(response);
        thread::sleep(Duration::from_millis(20));
    }
    let response = last_response.expect("at least one list apps response");
    panic!(
        "expected app {entrypoint_id} local_url {expected_url}, got {:?}",
        app_row(&response, entrypoint_id).launch_target.local_url
    );
}

fn package_action<'a>(
    actions: &'a [botster_hub::DaemonPackageActionState],
    action_id: &str,
) -> &'a botster_hub::DaemonPackageActionState {
    actions
        .iter()
        .find(|action| action.action_id == action_id)
        .unwrap_or_else(|| panic!("response includes {action_id} action"))
}

fn process_exists(pid: u32) -> bool {
    unsafe { libc::kill(pid as libc::pid_t, 0) == 0 }
}

struct ChildCleanup {
    child: Child,
}

impl ChildCleanup {
    fn spawn_non_botster_decoy() -> Self {
        let child = Command::new("sleep")
            .arg("30")
            .spawn()
            .expect("spawn non-botster decoy process");
        Self { child }
    }

    fn id(&self) -> u32 {
        self.child.id()
    }

    fn assert_alive(&mut self) {
        assert!(
            self.child
                .try_wait()
                .expect("poll non-botster decoy")
                .is_none(),
            "non-botster decoy process should remain alive"
        );
        assert!(
            process_exists(self.id()),
            "non-botster decoy pid should still exist"
        );
    }
}

impl Drop for ChildCleanup {
    fn drop(&mut self) {
        if self.child.try_wait().ok().flatten().is_none() {
            let _ = self.child.kill();
        }
        let _ = self.child.wait();
    }
}

#[test]
fn botster_web_health_rejects_stale_daemon_socket_file() {
    let data_dir = unique_short_test_dir("web-health-stale-socket");
    let package_dir = unique_test_dir("web-health-stale-socket-package");
    fs::create_dir_all(&data_dir).expect("create stale socket data directory");
    write_botster_web_package(&package_dir);
    let socket_path = data_dir.join("hub.sock");
    let stale_listener = UnixListener::bind(&socket_path).expect("bind stale daemon socket");
    drop(stale_listener);
    assert!(
        socket_path.exists(),
        "stale daemon socket file should remain"
    );

    let listener_port = unused_loopback_port();
    let connection = serde_json::json!({
        "transport": {
            "type": "unix_socket",
            "path": socket_path
        }
    });
    let child = Command::new("node")
        .arg("scripts/local-package-server.mjs")
        .current_dir(&package_dir)
        .env("BOTSTER_HUB_CONNECTION", connection.to_string())
        .env("BOTSTER_HUB_DATA_DIR", &data_dir)
        .env("BOTSTER_WEB_PORT", listener_port.to_string())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn botster-web package server against stale socket");
    let mut child = ChildCleanup { child };
    let mut listening = String::new();
    BufReader::new(
        child
            .child
            .stdout
            .take()
            .expect("botster-web package server stdout"),
    )
    .read_line(&mut listening)
    .expect("read botster-web listening marker");
    assert_eq!(
        listening.trim(),
        format!("web_listening=http://127.0.0.1:{listener_port}")
    );

    let health = read_json_health(&format!("http://127.0.0.1:{listener_port}"));
    assert_eq!(health["ok"], false, "stale socket health: {health}");
    assert_eq!(health["socketExists"], true);
    assert_eq!(health["daemonReady"], false);
    assert!(
        health["error"]
            .as_str()
            .is_some_and(|error| error.contains("ECONNREFUSED")),
        "stale socket health should report protocol failure: {health}"
    );
}

fn wait_for_process_exit(pid: u32) {
    for _ in 0..100 {
        if !process_exists(pid) {
            return;
        }
        thread::sleep(Duration::from_millis(20));
    }
    panic!("process {pid} still exists");
}

fn read_json_health(url: &str) -> serde_json::Value {
    let (_, body) = read_http_path(url, "/health");
    serde_json::from_str(body.trim()).expect("health JSON")
}

fn read_http_path(url: &str, path: &str) -> (String, String) {
    let port = url
        .strip_prefix("http://127.0.0.1:")
        .expect("local HTTP URL")
        .parse::<u16>()
        .expect("HTTP port");
    let mut stream = TcpStream::connect(("127.0.0.1", port)).expect("connect HTTP endpoint");
    let request =
        format!("GET {path} HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nConnection: close\r\n\r\n");
    stream
        .write_all(request.as_bytes())
        .expect("write HTTP request");
    let mut response = String::new();
    stream
        .read_to_string(&mut response)
        .expect("read HTTP response");
    let (headers, body) = response.split_once("\r\n\r\n").expect("HTTP response body");
    let body = if headers
        .to_ascii_lowercase()
        .contains("transfer-encoding: chunked")
    {
        decode_chunked_http_body(body)
    } else {
        body.to_string()
    };
    (headers.to_string(), body)
}

fn botster_web_page_bootstrap(web_origin: &str) -> botster_hub_client::DaemonLocalWebrtcBootstrap {
    let (headers, body) = read_http_path(web_origin, "/");
    assert!(
        headers.starts_with("HTTP/1.1 200") || headers.starts_with("HTTP/1.0 200"),
        "botster-web page returned non-200: {headers} body={body}"
    );
    assert!(
        headers
            .to_ascii_lowercase()
            .contains("content-type: text/html"),
        "botster-web page should be HTML: {headers}"
    );
    let marker = "globalThis.__BOTSTER_LOCAL_WEBRTC_BOOTSTRAP__ = ";
    let start = body
        .find(marker)
        .map(|index| index + marker.len())
        .expect("HTML page includes local WebRTC bootstrap global");
    let rest = &body[start..];
    let end = rest
        .find(";</script>")
        .expect("HTML bootstrap script terminates");
    serde_json::from_str(&rest[..end]).expect("HTML bootstrap JSON")
}

fn log_botster_web_phase(test_started: Instant, phase: &str) {
    let unix_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time after epoch")
        .as_millis();
    eprintln!(
        "botster_web_reload_phase phase={phase} unix_ms={unix_ms} elapsed_ms={}",
        test_started.elapsed().as_millis()
    );
}

fn probe_botster_web_health(web_origin: &str) -> Result<serde_json::Value, String> {
    let port = web_origin
        .strip_prefix("http://127.0.0.1:")
        .expect("local HTTP URL")
        .parse::<u16>()
        .expect("HTTP port");

    let mut stream = TcpStream::connect(("127.0.0.1", port))
        .map_err(|error| format!("connect error: {error}"))?;
    stream
        .set_read_timeout(Some(Duration::from_secs(1)))
        .map_err(|error| format!("set read timeout: {error}"))?;
    let request =
        format!("GET /health HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nConnection: close\r\n\r\n");
    stream
        .write_all(request.as_bytes())
        .map_err(|error| format!("write error: {error}"))?;
    let mut response = String::new();
    stream
        .read_to_string(&mut response)
        .map_err(|error| format!("read error: {error}"))?;
    let (headers, body) = response
        .split_once("\r\n\r\n")
        .ok_or_else(|| format!("missing HTTP response body: {response:?}"))?;
    let body = if headers
        .to_ascii_lowercase()
        .contains("transfer-encoding: chunked")
    {
        decode_chunked_http_body(body)
    } else {
        body.to_string()
    };
    if !headers.starts_with("HTTP/1.1 200") && !headers.starts_with("HTTP/1.0 200") {
        return Err(format!("non-200 response: {headers} body={body}"));
    }
    let health: serde_json::Value = serde_json::from_str(body.trim())
        .map_err(|error| format!("invalid health JSON: {error}; body={body}"))?;
    let expected = serde_json::json!({
        "ok": true,
        "mode": "existing_hub",
        "source": "socket",
        "port": port,
        "socketExists": true,
        "daemonReady": true,
        "error": null,
    });
    if expected
        .as_object()
        .expect("expected health object")
        .iter()
        .any(|(key, value)| health.get(key) != Some(value))
    {
        return Err(format!(
            "unexpected health response: {health}; expected={expected}"
        ));
    }
    Ok(health)
}

fn wait_for_botster_web_readiness(
    endpoint: &botster_hub_client::DaemonEndpoint,
    web_origin: &str,
    expected_local_url: &str,
    test_started: Instant,
) -> botster_hub_client::DaemonResponse {
    let wait_started = Instant::now();
    let mut health_ready = false;
    let mut last_health = "not probed".to_string();
    let mut last_apps: String;

    loop {
        match botster_hub_client::request(endpoint, botster_hub_client::DaemonRequest::ListApps) {
            Ok(response) => {
                last_apps = format!("{response:#?}");
                if let Some(app) = response.apps.iter().find(|app| {
                    app.package_name == "botster-web" && app.entrypoint_id == "web-client"
                }) {
                    if matches!(
                        app.lifecycle_state.as_str(),
                        "exited" | "failed" | "stopped"
                    ) {
                        let entrypoint_status = botster_hub_client::request(
                            endpoint,
                            botster_hub_client::DaemonRequest::PackageEntrypointStatus {
                                package_name: "botster-web".to_string(),
                                entrypoint_id: "web-client".to_string(),
                            },
                        );
                        let daemon_status = botster_hub_client::request(
                            endpoint,
                            botster_hub_client::DaemonRequest::Status,
                        );
                        panic!(
                            "botster-web package server reached terminal state while waiting for readiness; elapsed_ms={} expected_local_url={expected_local_url} app={app:#?} entrypoint_status={entrypoint_status:#?} daemon_status={daemon_status:#?} last_health={last_health}",
                            wait_started.elapsed().as_millis()
                        );
                    }
                    if let Some(actual_url) = app.launch_target.local_url.as_deref() {
                        assert_eq!(
                            actual_url,
                            expected_local_url,
                            "botster-web app published unexpected local_url after {}ms; app={app:#?}",
                            wait_started.elapsed().as_millis()
                        );
                        if health_ready {
                            log_botster_web_phase(test_started, "local_url_published");
                            return response;
                        }
                    }
                }
            }
            Err(error) => {
                last_apps = format!("ListApps request error: {error:#?}");
            }
        }

        if !health_ready {
            match probe_botster_web_health(web_origin) {
                Ok(health) => {
                    health_ready = true;
                    last_health = health.to_string();
                    log_botster_web_phase(test_started, "health_ready");
                }
                Err(error) => last_health = error,
            }
        }

        if wait_started.elapsed() >= BOTSTER_WEB_READINESS_LIVENESS_BACKSTOP {
            let entrypoint_status = botster_hub_client::request(
                endpoint,
                botster_hub_client::DaemonRequest::PackageEntrypointStatus {
                    package_name: "botster-web".to_string(),
                    entrypoint_id: "web-client".to_string(),
                },
            );
            let daemon_status =
                botster_hub_client::request(endpoint, botster_hub_client::DaemonRequest::Status);
            panic!(
                "botster-web package server liveness backstop expired without readiness; elapsed_ms={} health_ready={health_ready} expected_local_url={expected_local_url} last_health={last_health} last_apps={last_apps} entrypoint_status={entrypoint_status:#?} daemon_status={daemon_status:#?}",
                wait_started.elapsed().as_millis()
            );
        }
        thread::sleep(Duration::from_millis(20));
    }
}

fn unused_loopback_port() -> u16 {
    TcpListener::bind(("127.0.0.1", 0))
        .expect("bind unused loopback port")
        .local_addr()
        .expect("local addr")
        .port()
}

fn decode_chunked_http_body(body: &str) -> String {
    let mut rest = body;
    let mut decoded = String::new();
    loop {
        let (size_line, after_size) = rest.split_once("\r\n").expect("chunk size");
        let size = usize::from_str_radix(size_line.trim(), 16).expect("hex chunk size");
        if size == 0 {
            return decoded;
        }
        decoded.push_str(&after_size[..size]);
        rest = &after_size[size + 2..];
    }
}

fn write_local_process_plugin_package(root: &Path) {
    fs::create_dir_all(root.join("bin")).expect("create process package root");
    fs::write(root.join("bin").join("plugin"), "#!/bin/sh\n").expect("write process entrypoint");
    fs::write(
        root.join("botster-package.json"),
        r#"{
  "name": "runtime.process-plugin",
  "version": "1.0.0",
  "kind": "plugin",
  "botster": ">=0.1.0",
  "source": { "type": "path", "path": "." },
  "capabilities": [
    { "surface": "surfaces" }
  ],
  "entrypoints": [
    { "runtime": "process", "path": "bin/plugin", "bootstrap": false }
  ]
}
"#,
    )
    .expect("write local process package manifest");
}

fn write_declared_surface_plugin_package(root: &Path) {
    fs::create_dir_all(root).expect("create declared surface package root");
    fs::write(root.join("plugin.lua"), "return botster.register({})\n")
        .expect("write plugin entrypoint");
    fs::write(
        root.join("botster-package.json"),
        r#"{
  "name": "runtime.surface-plugin",
  "version": "1.0.0",
  "kind": "plugin",
  "botster": ">=0.1.0",
  "source": { "type": "path", "path": "." },
  "capabilities": [
    { "surface": "surfaces" }
  ],
  "entrypoints": [
    { "runtime": "lua", "path": "plugin.lua", "bootstrap": false }
  ],
  "surfaces": [
    {
      "id": "runtime.surface.home",
      "kind": "app",
      "title": "Runtime Surface",
      "description": "Surface descriptor fixture",
      "icon": "workflow",
      "order": 20,
      "category": "runtime",
      "supports": ["render", "action"]
    },
    {
      "id": "runtime.surface.settings",
      "kind": "settings",
      "title": "Runtime Settings",
      "supports": ["render"]
    }
  ]
}
"#,
    )
    .expect("write declared surface package manifest");
}

fn write_invalid_local_package(root: &Path) {
    fs::create_dir_all(root).expect("create invalid package root");
    fs::write(root.join("botster-package.json"), "{ invalid json\n")
        .expect("write invalid manifest");
}

fn write_incompatible_local_package(root: &Path) {
    fs::create_dir_all(root).expect("create incompatible package root");
    fs::write(root.join("plugin.lua"), "return botster.register({})\n")
        .expect("write plugin entrypoint");
    fs::write(
        root.join("botster-package.json"),
        r#"{
  "name": "runtime.incompatible-plugin",
  "version": "1.0.0",
  "kind": "plugin",
  "botster": ">=999.0.0",
  "source": { "type": "path", "path": "." },
  "capabilities": [
    { "surface": "surfaces" }
  ],
  "entrypoints": [
    { "runtime": "lua", "path": "plugin.lua", "bootstrap": false }
  ]
}
"#,
    )
    .expect("write incompatible package manifest");
}

fn write_denied_capability_local_package(root: &Path) {
    fs::create_dir_all(root).expect("create denied capability package root");
    fs::write(root.join("plugin.lua"), "return botster.register({})\n")
        .expect("write plugin entrypoint");
    fs::write(
        root.join("botster-package.json"),
        r#"{
  "name": "runtime.denied-plugin",
  "version": "1.0.0",
  "kind": "plugin",
  "botster": ">=0.1.0",
  "source": { "type": "path", "path": "." },
  "capabilities": [
    { "surface": "filesystem", "scope": "home" }
  ],
  "entrypoints": [
    { "runtime": "lua", "path": "plugin.lua", "bootstrap": false }
  ]
}
"#,
    )
    .expect("write denied capability package manifest");
}

fn write_botster_workspaces_local_package(root: &Path, plugin_db_scope: &str) {
    fs::create_dir_all(root).expect("create botster-workspaces package root");
    fs::write(
        root.join("plugin.lua"),
        r#"local function workspace_id(arguments)
  if type(arguments.workspace_id) == "string" and arguments.workspace_id ~= "" then
    return arguments.workspace_id
  end
  return "workspace-local-1"
end

local function create(arguments)
  local target_id = arguments.target_id
  local target_validation = nil
  if type(target_id) == "string" and target_id ~= "" then
    target_validation = botster.capabilities.spawn_targets.validate({ target_id = target_id })
    if not target_validation.ok then
      return { ok = false, status = target_validation.status, target_id = target_id }
    end
  else
    target_id = nil
  end
  local workspace = {
    id = workspace_id(arguments),
    name = arguments.name or "Local Workspace",
    status = "created",
    target_id = target_id,
  }
  botster.capabilities.plugin_db.set({
    key = "workspace/" .. workspace.id,
    schema_version = 1,
    payload = workspace,
  })
  return { ok = true, workspace = workspace }
end

local function use_workspace(arguments)
  local id = workspace_id(arguments)
  local record = botster.capabilities.plugin_db.get({ key = "workspace/" .. id })
  local workspace = record.record.payload
  if type(arguments.target_id) == "string" and arguments.target_id ~= "" then
    local validation = botster.capabilities.spawn_targets.validate({ target_id = arguments.target_id })
    if not validation.ok then
      return { ok = false, status = validation.status, target_id = arguments.target_id }
    end
    workspace.target_id = arguments.target_id
  end
  workspace.status = "used"
  botster.capabilities.plugin_db.set({
    key = "workspace/" .. workspace.id,
    schema_version = 1,
    payload = workspace,
  })
  return { ok = true, workspace = workspace }
end

local function validate_target(arguments)
  local target_id = arguments.target_id
  if type(target_id) ~= "string" or target_id == "" then
    return { ok = false, status = "missing_argument" }
  end
  return botster.capabilities.spawn_targets.validate({ target_id = target_id })
end

local function render_workspaces(_arguments)
  return {
    type = "panel",
    id = "botster-workspaces-panel",
    props = {
      title = "Workspaces",
    },
    children = {
      {
        type = "text",
        id = "botster-workspaces-title",
        props = {
          text = "Workspaces",
        },
      },
    },
  }
end

return botster.register({
  handlers = {
    {
      id = "workspaces_surface",
      kind = "surface_route",
      descriptor_id = "workspaces",
      descriptor = {
        title = "Workspaces",
        surface_id = "workspaces",
      },
      call = render_workspaces,
    },
  },
  tools = {
    {
      name = "botster_workspaces.create",
      description = "Create a constrained local workspace.",
      input_schema = {
        type = "object",
        properties = {
          workspace_id = { type = "string" },
          name = { type = "string" },
          target_id = { type = "string" },
        },
        additionalProperties = false,
      },
      handler = "create",
      call = create,
    },
    {
      name = "botster_workspaces.use",
      description = "Use a constrained local workspace.",
      input_schema = {
        type = "object",
        properties = {
          workspace_id = { type = "string" },
          target_id = { type = "string" },
        },
        additionalProperties = false,
      },
      handler = "use",
      call = use_workspace,
    },
    {
      name = "botster_workspaces.validate_target",
      description = "Validate a hub-owned spawn target reference for a workspace.",
      input_schema = {
        type = "object",
        properties = {
          target_id = { type = "string" },
        },
        required = { "target_id" },
        additionalProperties = false,
      },
      handler = "validate_target",
      call = validate_target,
    },
  },
})
"#,
    )
    .expect("write plugin entrypoint");
    fs::write(
        root.join("botster-package.json"),
        format!(
            r#"{{
  "name": "botster-workspaces",
  "version": "1.0.0",
  "kind": "plugin",
  "botster": ">=0.1.0",
  "source": {{ "type": "path", "path": "." }},
  "capabilities": [
    {{ "surface": "mcp" }},
    {{ "surface": "plugin_db", "scope": "{plugin_db_scope}" }},
    {{ "surface": "surfaces" }},
    {{ "surface": "filesystem", "scope": "workspace" }}
  ],
  "surfaces": [
    {{
      "id": "workspaces",
      "kind": "app",
      "title": "Workspaces",
      "supports": ["render"]
    }}
  ],
  "entrypoints": [
    {{ "runtime": "lua", "path": "plugin.lua", "bootstrap": false }}
  ]
}}
"#
        ),
    )
    .expect("write botster-workspaces package manifest");
}

fn command_output_text(output: &Output) -> String {
    format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    )
}

struct OperatorConsolePty {
    child: Box<dyn portable_pty::Child + Send + Sync>,
    master: Box<dyn MasterPty + Send>,
    writer: Option<Box<dyn Write + Send>>,
    output: Arc<Mutex<Vec<u8>>>,
    reader: Option<thread::JoinHandle<()>>,
    reader_done: Arc<AtomicBool>,
}

trait TestChildControl {
    fn try_wait_status(&mut self) -> io::Result<Option<String>>;
    fn terminate_and_reap(&mut self) -> String;
    fn captured_output(&mut self) -> String;
}

impl TestChildControl for Child {
    fn try_wait_status(&mut self) -> io::Result<Option<String>> {
        self.try_wait()
            .map(|status| status.map(|status| status.to_string()))
    }

    fn terminate_and_reap(&mut self) -> String {
        terminate_and_reap_child(self)
    }

    fn captured_output(&mut self) -> String {
        let (stdout, stderr) = collect_child_output(self);
        format!("stdout={stdout:?} stderr={stderr:?}")
    }
}

impl TestChildControl for Box<dyn portable_pty::Child + Send + Sync> {
    fn try_wait_status(&mut self) -> io::Result<Option<String>> {
        self.try_wait()
            .map(|status| status.map(|status| format!("{status:?}")))
    }

    fn terminate_and_reap(&mut self) -> String {
        terminate_and_reap_pty_child(self.as_mut())
    }

    fn captured_output(&mut self) -> String {
        String::new()
    }
}

fn wait_for_child_condition_with_budget(
    child: &mut impl TestChildControl,
    description: &str,
    budget: Duration,
    mut condition_met: impl FnMut() -> bool,
) -> Result<(), String> {
    let started_at = Instant::now();
    let deadline = started_at + budget;
    while Instant::now() < deadline {
        if condition_met() {
            return Ok(());
        }
        match child.try_wait_status() {
            Ok(Some(status)) => {
                let output = child.captured_output();
                if condition_met() {
                    return Ok(());
                }
                return Err(format!(
                    "{description}: child exited before condition after {:?} (backstop {budget:?}); child_status={status}; {output}",
                    started_at.elapsed()
                ));
            }
            Ok(None) => {}
            Err(error) => {
                let child_status = child.terminate_and_reap();
                let output = child.captured_output();
                return Err(format!(
                    "{description}: failed to poll child after {:?} (backstop {budget:?}): {error}; child_status={child_status}; {output}",
                    started_at.elapsed()
                ));
            }
        }
        thread::sleep(Duration::from_millis(20));
    }

    let child_status = child.terminate_and_reap();
    let output = child.captured_output();
    Err(format!(
        "{description}: condition not met after {:?} (backstop {budget:?}); child_status={child_status}; {output}",
        started_at.elapsed()
    ))
}

fn terminate_and_reap_pty_child(child: &mut dyn portable_pty::Child) -> String {
    match child.try_wait() {
        Ok(Some(status)) => return format!("{status:?}"),
        Ok(None) => {}
        Err(error) => return format!("poll_error={error}"),
    }

    if let Some(pid) = child.process_id()
        && pid > 1
        && pid != std::process::id()
    {
        unsafe {
            libc::killpg(pid as libc::pid_t, libc::SIGTERM);
        }
        for _ in 0..25 {
            match child.try_wait() {
                Ok(Some(status)) => return format!("{status:?}"),
                Ok(None) => thread::sleep(Duration::from_millis(20)),
                Err(error) => return format!("poll_error={error}"),
            }
        }
        unsafe {
            libc::killpg(pid as libc::pid_t, libc::SIGKILL);
        }
    }
    let _ = child.kill();
    child
        .wait()
        .map(|status| format!("{status:?}"))
        .unwrap_or_else(|error| format!("wait_error={error}"))
}

struct OwnedOperatorConsoleDaemon {
    data_dir: PathBuf,
    owned_pids: Vec<u32>,
    armed: bool,
}

impl OwnedOperatorConsoleDaemon {
    fn new(data_dir: &Path) -> Self {
        let status = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
            .arg("status")
            .arg("--data-dir")
            .arg(data_dir)
            .output()
            .expect("probe operator console daemon before spawn");
        assert!(
            !status.status.success(),
            "operator console test data directory already has a running daemon: {}",
            command_output_text(&status)
        );
        Self {
            data_dir: data_dir.to_path_buf(),
            owned_pids: Vec::new(),
            armed: true,
        }
    }

    fn wait_until_daemon_ready(&mut self, console: &mut OperatorConsolePty) {
        self.try_wait_until_daemon_ready(console)
            .unwrap_or_else(|error| panic!("{error}"));
    }

    fn try_wait_until_daemon_ready(
        &mut self,
        console: &mut OperatorConsolePty,
    ) -> Result<(), String> {
        self.try_wait_until_daemon_ready_with_backstop(
            console,
            OPERATOR_CONSOLE_READINESS_LIVENESS_BACKSTOP,
        )
    }

    fn try_wait_until_daemon_ready_with_backstop(
        &mut self,
        console: &mut OperatorConsolePty,
        liveness_backstop: Duration,
    ) -> Result<(), String> {
        let config = explicit_config(&self.data_dir);
        let output = Arc::clone(&console.output);
        let mut last_status = "status probe not attempted".to_string();
        let result = wait_for_child_condition_with_budget(
            &mut console.child,
            "waiting for typed operator-console daemon readiness",
            liveness_backstop,
            || {
                self.capture_owned_pid();
                match botster_hub::daemon_transport_request(
                    &config,
                    botster_hub::DaemonRequest::Status,
                ) {
                    Ok(_) => true,
                    Err(error) => {
                        last_status = error.to_string();
                        false
                    }
                }
            },
        );
        self.capture_owned_pid();
        if let Err(error) = result {
            let reader_status = console
                .finish_reader_after_exit(OPERATOR_CONSOLE_READER_DRAIN_BACKSTOP)
                .map_or_else(|error| error, |()| "reader reached EOF".to_string());
            self.capture_owned_pid();
            let metadata_path = self.data_dir.join(".botster-hub-runtime-daemon.json");
            let socket_path = config
                .transports
                .local_socket
                .as_ref()
                .expect("operator console local socket binding")
                .path
                .clone();
            return Err(format!(
                "{error}; last_status={last_status:?}; owned_daemon_pids={:?}; metadata_exists={}; socket_exists={}; reader_status={reader_status:?}; operator_console_output={}",
                self.owned_pids,
                metadata_path.exists(),
                socket_path.exists(),
                String::from_utf8_lossy(
                    &output
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                )
            ));
        }
        Ok(())
    }

    fn capture_owned_pid(&mut self) {
        let Some(pid) = self.validated_metadata_pid() else {
            return;
        };
        if !self.owned_pids.contains(&pid) {
            self.owned_pids.push(pid);
        }
    }

    fn validated_metadata_pid(&self) -> Option<u32> {
        let metadata_path = self.data_dir.join(".botster-hub-runtime-daemon.json");
        let bytes = fs::read(metadata_path).ok()?;
        let metadata = serde_json::from_slice::<serde_json::Value>(&bytes).ok()?;
        let pid = metadata["pid"]
            .as_u64()
            .and_then(|pid| u32::try_from(pid).ok())?;
        let expected_socket = explicit_config(&self.data_dir)
            .transports
            .local_socket
            .as_ref()
            .expect("operator console local socket binding")
            .path
            .display()
            .to_string();
        let metadata_matches = metadata["data_directory"].as_str()
            == Some(stable_path_string(&self.data_dir).as_str())
            && metadata["socket_path"].as_str() == Some(expected_socket.as_str())
            && metadata["hub_bin"]
                .as_str()
                .is_some_and(|path| Path::new(path).file_name() == Some("botster-hub".as_ref()));
        metadata_matches.then_some(pid)
    }

    fn assert_cleaned(&mut self) {
        self.cleanup()
            .unwrap_or_else(|error| panic!("operator console daemon cleanup failed: {error}"));
        self.armed = false;
    }

    fn owned_pids(&self) -> &[u32] {
        &self.owned_pids
    }

    fn record_owned_pid(&mut self, pid: u32) {
        if !self.owned_pids.contains(&pid) {
            self.owned_pids.push(pid);
        }
    }

    fn cleanup(&mut self) -> Result<(), String> {
        self.capture_owned_pid();
        let _ = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
            .arg("shutdown")
            .arg("--data-dir")
            .arg(&self.data_dir)
            .output();

        for pid in self.owned_pids.iter().copied() {
            wait_for_owned_pid_exit(pid, Duration::from_secs(2));
            if process_exists(pid) && self.process_identity_matches(pid) {
                unsafe {
                    libc::kill(pid as libc::pid_t, libc::SIGTERM);
                }
                wait_for_owned_pid_exit(pid, Duration::from_secs(2));
            }
            if process_exists(pid) && self.process_identity_matches(pid) {
                unsafe {
                    libc::kill(pid as libc::pid_t, libc::SIGKILL);
                }
                wait_for_owned_pid_exit(pid, Duration::from_secs(2));
            }
        }

        let alive = self
            .owned_pids
            .iter()
            .copied()
            .filter(|pid| process_exists(*pid) && self.process_identity_matches(*pid))
            .collect::<Vec<_>>();
        if !alive.is_empty() {
            return Err(format!("owned daemon pids still alive: {alive:?}"));
        }

        let metadata_path = self.data_dir.join(".botster-hub-runtime-daemon.json");
        let socket_path = explicit_config(&self.data_dir)
            .transports
            .local_socket
            .as_ref()
            .expect("operator console local socket binding")
            .path
            .clone();
        let socket_deadline = Instant::now() + Duration::from_secs(2);
        while Instant::now() < socket_deadline && socket_path.exists() {
            thread::sleep(Duration::from_millis(20));
        }
        let status = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
            .arg("status")
            .arg("--data-dir")
            .arg(&self.data_dir)
            .output()
            .map_err(|error| format!("probe stopped daemon: {error}"))?;
        if status.status.success() {
            return Err(format!(
                "typed daemon status still reports running: {}",
                command_output_text(&status)
            ));
        }
        if socket_path.exists() {
            return Err(format!(
                "owned daemon socket remains after stopped status: {}",
                socket_path.display()
            ));
        }
        if metadata_path.exists() {
            let Some(metadata_pid) = self.validated_metadata_pid() else {
                return Err(format!(
                    "unverified runtime metadata remains: {}",
                    metadata_path.display()
                ));
            };
            if !self.owned_pids.contains(&metadata_pid) {
                return Err(format!(
                    "runtime metadata pid {metadata_pid} was not captured as owned"
                ));
            }
            fs::remove_file(&metadata_path)
                .map_err(|error| format!("remove verified owned runtime metadata: {error}"))?;
        }
        Ok(())
    }

    fn process_identity_matches(&self, pid: u32) -> bool {
        let output = Command::new("ps")
            .arg("-p")
            .arg(pid.to_string())
            .arg("-o")
            .arg("command=")
            .output();
        let Ok(output) = output else {
            return false;
        };
        let command = String::from_utf8_lossy(&output.stdout);
        command.contains("botster-hub")
            && command.contains(" start ")
            && command.contains("--data-dir")
            && (command.contains(&self.data_dir.display().to_string())
                || command.contains(&stable_path_string(&self.data_dir)))
    }
}

impl Drop for OwnedOperatorConsoleDaemon {
    fn drop(&mut self) {
        if self.armed {
            let _ = self.cleanup();
        }
    }
}

fn wait_for_owned_pid_exit(pid: u32, budget: Duration) {
    let deadline = Instant::now() + budget;
    while Instant::now() < deadline && process_exists(pid) {
        thread::sleep(Duration::from_millis(20));
    }
}

fn assert_detached_daemon_stdin(pid: u32) {
    #[cfg(target_os = "linux")]
    {
        let stdin = fs::read_link(format!("/proc/{pid}/fd/0"))
            .unwrap_or_else(|error| panic!("read detached daemon pid {pid} stdin: {error}"));
        assert_eq!(
            stdin,
            Path::new("/dev/null"),
            "detached daemon pid {pid} retained operator-console stdin"
        );
    }

    #[cfg(target_os = "macos")]
    {
        let output = Command::new("lsof")
            .args(["-a", "-p", &pid.to_string(), "-d", "0", "-Fn"])
            .output()
            .unwrap_or_else(|error| panic!("inspect detached daemon pid {pid} stdin: {error}"));
        assert!(
            output.status.success(),
            "inspect detached daemon pid {pid} stdin: {}",
            command_output_text(&output)
        );
        assert!(
            String::from_utf8_lossy(&output.stdout)
                .lines()
                .any(|line| line == "n/dev/null"),
            "detached daemon pid {pid} retained operator-console stdin: {}",
            command_output_text(&output)
        );
    }

    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    panic!("detached daemon stdin assertion is unsupported on this Unix target for pid {pid}");
}

struct SessionCleanupGuard {
    data_dir: PathBuf,
    session_id: &'static str,
    armed: bool,
}

impl SessionCleanupGuard {
    fn new(data_dir: &Path, session_id: &'static str) -> Self {
        Self {
            data_dir: data_dir.to_path_buf(),
            session_id,
            armed: true,
        }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for SessionCleanupGuard {
    fn drop(&mut self) {
        if self.armed {
            let _ = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
                .arg("sessions")
                .arg("shutdown")
                .arg("--data-dir")
                .arg(&self.data_dir)
                .arg(self.session_id)
                .output();
        }
    }
}

impl OperatorConsolePty {
    fn spawn(data_dir: &Path) -> Self {
        Self::spawn_binary(Path::new(env!("CARGO_BIN_EXE_botster-hub")), data_dir)
    }

    fn spawn_binary(binary: &Path, data_dir: &Path) -> Self {
        Self::spawn_binary_with_env(binary, data_dir, &[])
    }

    fn spawn_with_env(data_dir: &Path, environment: &[(&str, &str)]) -> Self {
        Self::spawn_binary_with_env(
            Path::new(env!("CARGO_BIN_EXE_botster-hub")),
            data_dir,
            environment,
        )
    }

    fn spawn_binary_with_env(binary: &Path, data_dir: &Path, environment: &[(&str, &str)]) -> Self {
        let pty = native_pty_system()
            .openpty(PtySize {
                rows: 30,
                cols: 100,
                pixel_width: 0,
                pixel_height: 0,
            })
            .expect("open operator console PTY");
        let mut command = CommandBuilder::new(binary);
        command.env("BOTSTER_HUB_DATA_DIR", data_dir);
        for (name, value) in environment {
            command.env(name, value);
        }
        let child = pty
            .slave
            .spawn_command(command)
            .expect("spawn operator console");
        let mut reader = pty
            .master
            .try_clone_reader()
            .expect("clone operator console PTY reader");
        let writer = pty
            .master
            .take_writer()
            .expect("take operator console PTY writer");
        drop(pty.slave);
        let output = Arc::new(Mutex::new(Vec::new()));
        let reader_output = Arc::clone(&output);
        let reader_done = Arc::new(AtomicBool::new(false));
        let reader_done_signal = Arc::clone(&reader_done);
        let reader = thread::spawn(move || {
            let mut buffer = [0_u8; 1024];
            while let Ok(count) = reader.read(&mut buffer) {
                if count == 0 {
                    break;
                }
                reader_output
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .extend_from_slice(&buffer[..count]);
            }
            reader_done_signal.store(true, Ordering::Release);
        });
        Self {
            child,
            master: pty.master,
            writer: Some(writer),
            output,
            reader: Some(reader),
            reader_done,
        }
    }

    fn send(&mut self, bytes: &[u8]) {
        self.writer
            .as_mut()
            .expect("operator console writer is open")
            .write_all(bytes)
            .expect("write operator console input");
        self.writer
            .as_mut()
            .expect("operator console writer is open")
            .flush()
            .expect("flush operator console input");
    }

    fn send_and_wait_for_prompt(&mut self, bytes: &[u8]) {
        let expected = self.prompt_count() + 1;
        self.send(bytes);
        self.wait_for_occurrences("botster-hub> ", expected);
    }

    fn prompt_count(&self) -> usize {
        self.text().matches("botster-hub> ").count()
    }

    fn wait_for(&mut self, needle: &str) {
        self.wait_for_occurrences(needle, 1);
    }

    fn wait_for_occurrences(&mut self, needle: &str, expected: usize) {
        self.try_wait_for_occurrences(needle, expected)
            .unwrap_or_else(|error| panic!("{error}"));
    }

    fn output_checkpoint(&self) -> usize {
        self.output
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .len()
    }

    fn wait_for_output_after(&mut self, checkpoint: usize, needle: &str) {
        self.try_wait_for_output_after(
            checkpoint,
            needle,
            OPERATOR_CONSOLE_OUTPUT_PROGRESS_BACKSTOP,
        )
        .unwrap_or_else(|error| panic!("{error}"));
    }

    fn try_wait_for_output_after(
        &mut self,
        checkpoint: usize,
        needle: &str,
        budget: Duration,
    ) -> Result<(), String> {
        let started_at = Instant::now();
        let deadline = started_at + budget;
        while Instant::now() < deadline {
            if self.output_contains_after(checkpoint, needle.as_bytes()) {
                return Ok(());
            }
            match self.child.try_wait() {
                Ok(Some(status)) => {
                    self.finish_reader_after_exit(OPERATOR_CONSOLE_READER_DRAIN_BACKSTOP)?;
                    if self.output_contains_after(checkpoint, needle.as_bytes()) {
                        return Ok(());
                    }
                    return Err(format!(
                        "waiting for operator console output {needle:?} after byte checkpoint {checkpoint}: console exited after {:?}; console_status={status:?}; {}",
                        started_at.elapsed(),
                        self.output_progress_context(checkpoint)
                    ));
                }
                Ok(None) => {}
                Err(error) => {
                    return Err(format!(
                        "waiting for operator console output {needle:?} after byte checkpoint {checkpoint}: failed to poll console after {:?}: {error}; {}",
                        started_at.elapsed(),
                        self.output_progress_context(checkpoint)
                    ));
                }
            }
            thread::sleep(Duration::from_millis(20));
        }

        let diagnostics = self.foreground_diagnostics();
        self.writer.take();
        let console_status = terminate_and_reap_pty_child(self.child.as_mut());
        let reader_status = self
            .finish_reader_after_exit(OPERATOR_CONSOLE_READER_DRAIN_BACKSTOP)
            .err()
            .unwrap_or_else(|| "reader reached EOF".to_string());
        Err(format!(
            "waiting for operator console output {needle:?} after byte checkpoint {checkpoint}: no post-action progress after {:?} (backstop {budget:?}); console_status={console_status}; reader_status={reader_status:?}; {diagnostics}; {}",
            started_at.elapsed(),
            self.output_progress_context(checkpoint)
        ))
    }

    fn output_contains_after(&self, checkpoint: usize, needle: &[u8]) -> bool {
        let output = self
            .output
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        output
            .get(checkpoint..)
            .is_some_and(|suffix| suffix.windows(needle.len()).any(|window| window == needle))
    }

    fn output_progress_context(&self, checkpoint: usize) -> String {
        let output = self
            .output
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let suffix = output.get(checkpoint..).unwrap_or_default();
        format!(
            "post_checkpoint_output={:?}; operator_console_output={:?}",
            String::from_utf8_lossy(suffix),
            String::from_utf8_lossy(&output)
        )
    }

    fn foreground_diagnostics(&mut self) -> String {
        let console_status = match self.child.try_wait() {
            Ok(Some(status)) => format!("{status:?}"),
            Ok(None) => "running".to_string(),
            Err(error) => format!("inspection_error={error}"),
        };
        let raw_fd = self.master.as_raw_fd();
        let foreground_pgid = self.master.process_group_leader();
        let termios = raw_fd.map(|fd| {
            let mut attributes = std::mem::MaybeUninit::<libc::termios>::uninit();
            let result = unsafe { libc::tcgetattr(fd, attributes.as_mut_ptr()) };
            if result == 0 {
                let attributes = unsafe { attributes.assume_init() };
                format!(
                    "isig={}",
                    attributes.c_lflag & libc::ISIG as libc::tcflag_t != 0
                )
            } else {
                format!("inspection_error={}", io::Error::last_os_error())
            }
        });
        let group_probe = foreground_pgid.map(process_group_probe);
        let leader_probe = foreground_pgid.map(process_probe);
        let census = foreground_pgid.map(process_group_census);
        let inspection_consistency = match (&group_probe, &census) {
            (Some(Ok(true)), Some(Ok(rows))) if rows.is_empty() => {
                "inspection_error=live killpg probe disagrees with empty ps census".to_string()
            }
            (Some(Ok(false)), Some(Ok(rows))) if !rows.is_empty() => {
                "inspection_error=dead killpg probe disagrees with nonempty ps census".to_string()
            }
            (Some(Err(error)), _) => format!("inspection_error={error}"),
            (_, Some(Err(error))) => format!("inspection_error={error}"),
            _ => "inspection_consistent=true".to_string(),
        };
        format!(
            "foreground_diagnostics console_status={console_status}; raw_fd={raw_fd:?}; termios={termios:?}; foreground_pgid={foreground_pgid:?}; killpg_probe={group_probe:?}; leader_pid_probe={leader_probe:?}; group_census={census:?}; {inspection_consistency}"
        )
    }

    fn try_wait_for_occurrences(&mut self, needle: &str, expected: usize) -> Result<(), String> {
        let output = Arc::clone(&self.output);
        let result = wait_for_child_condition_with_budget(
            &mut self.child,
            &format!("waiting for {expected} occurrences of operator console output {needle:?}"),
            LOCAL_RUNTIME_DAEMON_READINESS_BUDGET,
            || {
                String::from_utf8_lossy(
                    &output
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner),
                )
                .matches(needle)
                .count()
                    >= expected
            },
        );
        if let Err(error) = result {
            if error.contains("child exited before condition") {
                self.finish_reader_after_exit(LOCAL_RUNTIME_DAEMON_READINESS_BUDGET)?;
                if self.text().matches(needle).count() >= expected {
                    return Ok(());
                }
            }
            return Err(format!("{error}; operator_console_output={}", self.text()));
        }
        Ok(())
    }

    fn text(&self) -> String {
        String::from_utf8_lossy(
            &self
                .output
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        )
        .into_owned()
    }

    fn wait_for_exit(&mut self) {
        let deadline = Instant::now() + Duration::from_secs(30);
        while Instant::now() < deadline {
            if self
                .child
                .try_wait()
                .expect("poll operator console")
                .is_some()
            {
                self.finish_reader_after_exit(OPERATOR_CONSOLE_READER_DRAIN_BACKSTOP)
                    .unwrap_or_else(|error| panic!("{error}"));
                return;
            }
            thread::sleep(Duration::from_millis(20));
        }
        self.writer.take();
        let child_status = terminate_and_reap_pty_child(self.child.as_mut());
        let reader_status = self
            .finish_reader_after_exit(OPERATOR_CONSOLE_READER_DRAIN_BACKSTOP)
            .err()
            .unwrap_or_else(|| "reader reached EOF".to_string());
        panic!(
            "operator console did not exit; child_status={child_status}; reader_status={reader_status}; output={}",
            self.text()
        );
    }

    fn finish_reader_after_exit(&mut self, budget: Duration) -> Result<(), String> {
        self.writer.take();
        let deadline = Instant::now() + budget;
        while Instant::now() < deadline && !self.reader_done.load(Ordering::Acquire) {
            thread::sleep(Duration::from_millis(20));
        }
        if !self.reader_done.load(Ordering::Acquire) {
            return Err(format!(
                "operator console PTY reader did not reach EOF within {budget:?}; output={}",
                self.text()
            ));
        }
        if let Some(reader) = self.reader.take() {
            reader
                .join()
                .map_err(|_| "operator console PTY reader panicked".to_string())?;
        }
        Ok(())
    }
}

fn process_probe(pid: libc::pid_t) -> Result<bool, String> {
    if unsafe { libc::kill(pid, 0) } == 0 {
        return Ok(true);
    }
    let error = io::Error::last_os_error();
    if error.raw_os_error() == Some(libc::ESRCH) {
        Ok(false)
    } else if error.raw_os_error() == Some(libc::EPERM) {
        Ok(true)
    } else {
        Err(format!("kill({pid}, 0) failed: {error}"))
    }
}

fn process_group_probe(pgid: libc::pid_t) -> Result<bool, String> {
    if unsafe { libc::killpg(pgid, 0) } == 0 {
        return Ok(true);
    }
    let error = io::Error::last_os_error();
    if error.raw_os_error() == Some(libc::ESRCH) {
        Ok(false)
    } else if error.raw_os_error() == Some(libc::EPERM) {
        Ok(true)
    } else {
        Err(format!("killpg({pgid}, 0) failed: {error}"))
    }
}

fn process_group_census(pgid: libc::pid_t) -> Result<Vec<String>, String> {
    let output = Command::new("ps")
        .args(["-axo", "pid=,ppid=,pgid=,stat=,command="])
        .output()
        .map_err(|error| format!("run portable process census: {error}"))?;
    if !output.status.success() {
        return Err(format!(
            "portable process census exited with {}: stderr={:?}",
            output.status,
            String::from_utf8_lossy(&output.stderr)
        ));
    }
    let rows = String::from_utf8_lossy(&output.stdout)
        .lines()
        .filter(|line| {
            line.split_whitespace()
                .nth(2)
                .and_then(|value| value.parse::<libc::pid_t>().ok())
                == Some(pgid)
        })
        .map(str::trim)
        .map(str::to_string)
        .collect();
    Ok(rows)
}

impl Drop for OperatorConsolePty {
    fn drop(&mut self) {
        self.writer.take();
        let _ = terminate_and_reap_pty_child(self.child.as_mut());
        let _ = self.finish_reader_after_exit(OPERATOR_CONSOLE_READER_DRAIN_BACKSTOP);
    }
}

fn daemon_test_lock() -> &'static Mutex<()> {
    REAL_DAEMON_TEST_LOCK.get_or_init(|| Mutex::new(()))
}

fn daemon_test_guard() -> std::sync::MutexGuard<'static, ()> {
    recovering_mutex_guard(daemon_test_lock())
}

#[test]
fn daemon_test_guard_recovers_poison_without_losing_mutual_exclusion() {
    static PROBE_LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    let lock = PROBE_LOCK.get_or_init(|| Mutex::new(()));
    let poisoner = thread::spawn(move || {
        let _guard = recovering_mutex_guard(lock);
        panic!("poison daemon test lock intentionally");
    });
    assert!(poisoner.join().is_err());

    let guard = recovering_mutex_guard(lock);
    let (acquired_tx, acquired_rx) = mpsc::channel();
    let waiter = thread::spawn(move || {
        let _guard = recovering_mutex_guard(lock);
        acquired_tx.send(()).expect("report lock acquisition");
    });

    assert!(
        acquired_rx
            .recv_timeout(Duration::from_millis(100))
            .is_err()
    );
    drop(guard);
    acquired_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("waiting thread acquires recovered lock after guard drops");
    waiter.join().expect("lock waiter exits cleanly");
}

fn start_cli_daemon(data_dir: &Path) -> Child {
    ensure_session_worker_binary();
    let mut command = Command::new(env!("CARGO_BIN_EXE_botster-hub"));
    command
        .arg("start")
        .arg("--data-dir")
        .arg(data_dir)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    configure_test_process_group(&mut command);
    let mut child = command.spawn().expect("spawn botster-hub start");

    wait_for_status(data_dir, &mut child);
    child
}

fn start_owned_incompatible_local_runtime_daemon(data_dir: &Path) -> Child {
    ensure_session_worker_binary();
    fs::create_dir_all(data_dir).expect("create data dir");
    let mut command = Command::new(env!("CARGO_BIN_EXE_botster-hub"));
    command
        .arg("start")
        .arg("--data-dir")
        .arg(data_dir)
        .arg("--session-worker-bin")
        .arg(session_worker_binary_path())
        .env("BOTSTER_HUB_TEST_INCOMPATIBLE_DAEMON", "1")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    configure_test_process_group(&mut command);
    let mut child = command
        .spawn()
        .expect("spawn incompatible botster-hub start");
    wait_for_incompatible_status(data_dir, &mut child);
    write_local_runtime_daemon_metadata(data_dir, child.id());
    child
}

fn wait_for_incompatible_status(data_dir: &Path, child: &mut Child) {
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    let mut last_output = String::new();
    while std::time::Instant::now() < deadline {
        if let Some(status) = child.try_wait().expect("check incompatible daemon child") {
            let mut stdout = String::new();
            let mut stderr = String::new();
            if let Some(mut pipe) = child.stdout.take() {
                let _ = pipe.read_to_string(&mut stdout);
            }
            if let Some(mut pipe) = child.stderr.take() {
                let _ = pipe.read_to_string(&mut stderr);
            }
            panic!(
                "incompatible daemon exited before ready with {status}: stdout={stdout:?} stderr={stderr:?}"
            );
        }
        let output = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
            .arg("status")
            .arg("--data-dir")
            .arg(data_dir)
            .output()
            .expect("run botster-hub status against incompatible daemon");
        last_output = command_output_text(&output);
        if !output.status.success()
            && (last_output.contains("running daemon is incompatible or stale")
                || last_output.contains("hub predates compatibility handshake"))
        {
            return;
        }
        thread::sleep(Duration::from_millis(50));
    }
    panic!("incompatible daemon did not become ready; last status output: {last_output}");
}

fn write_local_runtime_daemon_metadata(data_dir: &Path, pid: u32) {
    let config = explicit_config(data_dir);
    let socket_path = config
        .transports
        .local_socket
        .as_ref()
        .expect("local socket binding")
        .path
        .clone();
    let metadata = serde_json::json!({
        "pid": pid,
        "data_directory": stable_path_string(data_dir),
        "data_directory_arg": data_dir.display().to_string(),
        "socket_path": socket_path.display().to_string(),
        "hub_bin": stable_path_string(Path::new(env!("CARGO_BIN_EXE_botster-hub"))),
    });
    let metadata_path = data_dir.join(".botster-hub-runtime-daemon.json");
    fs::write(
        &metadata_path,
        serde_json::to_string_pretty(&metadata).expect("serialize daemon metadata"),
    )
    .expect("write daemon metadata");
    assert!(metadata_path.exists(), "daemon metadata should exist");
}

fn stable_path_string(path: &Path) -> String {
    fs::canonicalize(path)
        .unwrap_or_else(|_| path.to_path_buf())
        .display()
        .to_string()
}

fn start_cli_daemon_with_session_worker(data_dir: &Path, session_worker_bin: &Path) -> Child {
    let mut command = Command::new(env!("CARGO_BIN_EXE_botster-hub"));
    command
        .arg("start")
        .arg("--data-dir")
        .arg(data_dir)
        .arg("--session-worker-bin")
        .arg(session_worker_bin)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    configure_test_process_group(&mut command);
    let mut child = command.spawn().expect("spawn botster-hub start");

    wait_for_status(data_dir, &mut child);
    child
}

fn wait_for_status(data_dir: &Path, child: &mut Child) {
    wait_for_status_with_budget(data_dir, child, LOCAL_RUNTIME_DAEMON_READINESS_BUDGET)
        .unwrap_or_else(|error| panic!("{error}"));
}

fn wait_for_status_with_budget(
    data_dir: &Path,
    child: &mut Child,
    readiness_budget: Duration,
) -> Result<(), String> {
    let mut last_status = "status probe not attempted".to_string();
    wait_for_child_condition_with_budget(
        child,
        "waiting for typed daemon status readiness",
        readiness_budget,
        || {
        let output = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
            .arg("status")
            .arg("--data-dir")
            .arg(data_dir)
            .output()
            .expect("run botster-hub status");
        last_status = command_output_text(&output);
            output.status.success()
        },
    )
    .map_err(|error| {
        format!(
            "daemon did not become ready (readiness budget {readiness_budget:?}); last status output={last_status:?}; {error}"
        )
    })
}

fn terminate_and_reap_child(child: &mut Child) -> String {
    if let Some(status) = child.try_wait().expect("check daemon child before cleanup") {
        return status.to_string();
    }
    let pid = child.id();
    signal_test_group_or_child(pid, libc::SIGTERM)
        .expect("signal daemon group after readiness failure");
    let deadline = Instant::now() + Duration::from_millis(500);
    while Instant::now() < deadline {
        if let Some(status) = child.try_wait().expect("poll daemon child during cleanup") {
            return status.to_string();
        }
        thread::sleep(Duration::from_millis(20));
    }
    signal_test_group_or_child(pid, libc::SIGKILL)
        .expect("kill daemon group after readiness failure");
    let deadline = Instant::now() + Duration::from_secs(2);
    while Instant::now() < deadline {
        if let Some(status) = child.try_wait().expect("poll killed daemon child") {
            return status.to_string();
        }
        thread::sleep(Duration::from_millis(20));
    }
    panic!("daemon child {pid} did not exit within bounded cleanup");
}

fn configure_test_process_group(command: &mut Command) {
    unsafe {
        command.pre_exec(|| {
            if libc::setpgid(0, 0) == -1 {
                return Err(io::Error::last_os_error());
            }
            Ok(())
        });
    }
}

fn signal_test_group_or_child(pid: u32, signal: libc::c_int) -> io::Result<()> {
    if unsafe { libc::killpg(pid as libc::pid_t, signal) } == 0 {
        return Ok(());
    }
    let group_error = io::Error::last_os_error();
    if group_error.raw_os_error() != Some(libc::ESRCH) {
        return Err(group_error);
    }
    if unsafe { libc::kill(pid as libc::pid_t, signal) } == 0 {
        return Ok(());
    }
    let child_error = io::Error::last_os_error();
    if child_error.raw_os_error() == Some(libc::ESRCH) {
        Ok(())
    } else {
        Err(child_error)
    }
}

fn collect_child_output(child: &mut Child) -> (String, String) {
    let mut stdout = String::new();
    let mut stderr = String::new();
    if let Some(mut pipe) = child.stdout.take() {
        let _ = pipe.read_to_string(&mut stdout);
    }
    if let Some(mut pipe) = child.stderr.take() {
        let _ = pipe.read_to_string(&mut stderr);
    }
    (stdout, stderr)
}

#[test]
fn process_ownership_wait_for_status_timeout_reports_diagnostics_and_reaps_owned_child() {
    let data_dir = unique_test_dir("wait-for-status-timeout");
    let mut child = Command::new("sh")
        .arg("-c")
        .arg(
            "printf 'daemon stdout marker\\n'; printf 'daemon stderr marker\\n' >&2; exec sleep 60",
        )
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn never-ready daemon fixture");

    let error = wait_for_status_with_budget(&data_dir, &mut child, Duration::from_millis(100))
        .expect_err("never-ready child should time out");

    assert!(error.contains("readiness budget 100ms"), "{error}");
    assert!(error.contains("last status output="), "{error}");
    assert!(error.contains("daemon stdout marker"), "{error}");
    assert!(error.contains("daemon stderr marker"), "{error}");
    assert!(error.contains("child_status="), "{error}");
    assert!(
        child
            .try_wait()
            .expect("confirm child was reaped")
            .is_some(),
        "owned child should be reaped after readiness timeout"
    );

    let status = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("status")
        .arg("--data-dir")
        .arg(&data_dir)
        .output()
        .expect("probe after readiness timeout");
    assert!(
        !status.status.success(),
        "timed-out fixture must not answer status: {}",
        command_output_text(&status)
    );
}

#[test]
fn operator_console_output_wait_reports_early_child_exit() {
    let fixture_dir = unique_short_test_dir("console-child-exit");
    fs::create_dir_all(&fixture_dir).expect("create early-exit console fixture directory");
    let fixture = fixture_dir.join("early-exit-console");
    fs::write(
        &fixture,
        "#!/bin/sh\nprintf 'console-started\\n'\nexit 23\n",
    )
    .expect("write early-exit console fixture");
    let mut permissions = fs::metadata(&fixture)
        .expect("read early-exit console fixture metadata")
        .permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(&fixture, permissions).expect("make early-exit console fixture executable");

    let mut console = OperatorConsolePty::spawn_binary(&fixture, &fixture_dir);
    console.wait_for("console-started");
    let error = console
        .try_wait_for_occurrences("output-that-will-never-arrive", 1)
        .expect_err("exited console should fail an unrelated output wait");
    assert!(error.contains("child exited before condition"), "{error}");
    assert!(error.contains("code: 23"), "{error}");
    assert!(
        !error.contains("condition not met after"),
        "child-exit detection must precede the hang backstop: {error}"
    );
    console.wait_for_exit();
    fs::remove_dir_all(&fixture_dir).expect("remove early-exit console fixture directory");
}

#[test]
fn operator_console_output_checkpoint_reports_early_child_exit() {
    let fixture_dir = unique_short_test_dir("console-checkpoint-child-exit");
    fs::create_dir_all(&fixture_dir)
        .expect("create checkpoint early-exit console fixture directory");
    let fixture = fixture_dir.join("checkpoint-early-exit-console");
    fs::write(
        &fixture,
        "#!/bin/sh\nprintf 'console-started\\n'\nexit 23\n",
    )
    .expect("write checkpoint early-exit console fixture");
    let mut permissions = fs::metadata(&fixture)
        .expect("read checkpoint early-exit console fixture metadata")
        .permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(&fixture, permissions)
        .expect("make checkpoint early-exit console fixture executable");

    let mut console = OperatorConsolePty::spawn_binary(&fixture, &fixture_dir);
    console.wait_for("console-started");
    let checkpoint = console.output_checkpoint();
    let error = console
        .try_wait_for_output_after(
            checkpoint,
            "output-that-will-never-arrive",
            OPERATOR_CONSOLE_OUTPUT_PROGRESS_BACKSTOP,
        )
        .expect_err("exited console should fail a post-checkpoint output wait");
    assert!(error.contains("console exited after"), "{error}");
    assert!(error.contains("code: 23"), "{error}");
    assert!(
        !error.contains("no post-action progress"),
        "child-exit detection must precede the post-action backstop: {error}"
    );
    console.wait_for_exit();
    fs::remove_dir_all(&fixture_dir)
        .expect("remove checkpoint early-exit console fixture directory");
}

#[test]
fn operator_console_output_checkpoint_rejects_stale_identical_output() {
    let fixture_dir = unique_short_test_dir("console-output-checkpoint");
    fs::create_dir_all(&fixture_dir).expect("create output-checkpoint fixture directory");
    let fixture = fixture_dir.join("checkpoint-console");
    fs::write(
        &fixture,
        "#!/bin/sh\nprintf 'repeated-output\\n'; sleep 60\n",
    )
    .expect("write output-checkpoint console fixture");
    let mut permissions = fs::metadata(&fixture)
        .expect("read output-checkpoint console fixture metadata")
        .permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(&fixture, permissions)
        .expect("make output-checkpoint console fixture executable");

    let mut console = OperatorConsolePty::spawn_binary(&fixture, &fixture_dir);
    console.wait_for("repeated-output");
    let checkpoint = console.output_checkpoint();
    assert!(
        !console.output_contains_after(checkpoint, b"repeated-output"),
        "output from before the checkpoint satisfied a post-action observation"
    );
    let error = console
        .try_wait_for_output_after(checkpoint, "repeated-output", Duration::from_millis(100))
        .expect_err("stale identical output must not satisfy a post-checkpoint wait");
    assert!(error.contains("no post-action progress"), "{error}");
    assert!(
        !console.output_contains_after(checkpoint, b"repeated-output"),
        "output from before the checkpoint appeared in the post-checkpoint suffix: {error}"
    );
    console.wait_for_exit();
    fs::remove_dir_all(&fixture_dir).expect("remove output-checkpoint fixture directory");
}

#[test]
fn wait_for_child_condition_rechecks_after_exit_drain() {
    struct ExitDrainChild {
        drained: Arc<AtomicBool>,
    }

    impl TestChildControl for ExitDrainChild {
        fn try_wait_status(&mut self) -> io::Result<Option<String>> {
            Ok(Some("exit 0".to_string()))
        }

        fn terminate_and_reap(&mut self) -> String {
            "exit 0".to_string()
        }

        fn captured_output(&mut self) -> String {
            self.drained.store(true, Ordering::Release);
            "stdout=\"final output\" stderr=\"\"".to_string()
        }
    }

    let drained = Arc::new(AtomicBool::new(false));
    let mut child = ExitDrainChild {
        drained: Arc::clone(&drained),
    };
    wait_for_child_condition_with_budget(
        &mut child,
        "waiting for final drained output",
        Duration::from_secs(1),
        || drained.load(Ordering::Acquire),
    )
    .expect("condition should be rechecked after the exited child's output drain");
}

#[test]
fn owned_operator_console_cleanup_checks_pid_identity_and_runtime_artifacts() {
    let reused_pid_data_dir = unique_short_test_dir("console-reused-pid");
    let mut reused_pid_cleanup = OwnedOperatorConsoleDaemon::new(&reused_pid_data_dir);
    reused_pid_cleanup.record_owned_pid(std::process::id());
    reused_pid_cleanup.assert_cleaned();

    let stale_artifact_data_dir = unique_short_test_dir("console-stale-artifact");
    let mut stale_artifact_cleanup = OwnedOperatorConsoleDaemon::new(&stale_artifact_data_dir);
    fs::create_dir_all(&stale_artifact_data_dir).expect("create stale-artifact data directory");
    let metadata_path = stale_artifact_data_dir.join(".botster-hub-runtime-daemon.json");
    fs::write(&metadata_path, b"not owned daemon metadata")
        .expect("write unverified daemon metadata");
    let error = stale_artifact_cleanup
        .cleanup()
        .expect_err("cleanup must not unlink unverified runtime artifacts");
    assert!(
        error.contains("unverified runtime metadata remains"),
        "{error}"
    );
    assert!(
        metadata_path.exists(),
        "cleanup oracle removed the artifact it was supposed to verify"
    );
    stale_artifact_cleanup.armed = false;
    fs::remove_dir_all(&stale_artifact_data_dir).expect("remove stale-artifact data directory");
}

#[test]
fn operator_console_readiness_backstop_outlives_policy_and_reports_context() {
    assert!(
        OPERATOR_CONSOLE_READINESS_LIVENESS_BACKSTOP > LOCAL_RUNTIME_DAEMON_READINESS_BUDGET,
        "the harness liveness backstop must not preempt production readiness policy"
    );

    let fixture_dir = unique_short_test_dir("console-readiness-backstop");
    fs::create_dir_all(&fixture_dir).expect("create readiness-backstop fixture directory");
    let fixture = fixture_dir.join("wedged-console");
    fs::write(&fixture, "#!/bin/sh\nexec sleep 60\n").expect("write readiness-backstop fixture");
    let mut permissions = fs::metadata(&fixture)
        .expect("read readiness-backstop fixture metadata")
        .permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(&fixture, permissions).expect("make readiness-backstop fixture executable");

    let mut daemon_cleanup = OwnedOperatorConsoleDaemon::new(&fixture_dir);
    let diagnostic_pid = std::process::id();
    daemon_cleanup.record_owned_pid(diagnostic_pid);
    let mut console = OperatorConsolePty::spawn_binary(&fixture, &fixture_dir);
    let error = daemon_cleanup
        .try_wait_until_daemon_ready_with_backstop(&mut console, Duration::from_millis(100))
        .expect_err("wedged console should hit the harness liveness backstop");
    assert!(error.contains("condition not met after"), "{error}");
    assert!(error.contains("last_status="), "{error}");
    assert!(
        error.contains(&format!("owned_daemon_pids=[{diagnostic_pid}]")),
        "{error}"
    );
    assert!(error.contains("metadata_exists=false"), "{error}");
    assert!(error.contains("socket_exists=false"), "{error}");
    assert!(
        error.contains("reader_status=\"reader reached EOF\""),
        "{error}"
    );
    console.wait_for_exit();
    daemon_cleanup.assert_cleaned();
    fs::remove_dir_all(&fixture_dir).expect("remove readiness-backstop fixture directory");
}

#[test]
fn operator_console_detach_releases_reader_while_daemon_stays_running() {
    let _guard = daemon_test_guard();
    ensure_session_worker_binary();
    let data_dir = unique_short_test_dir("console-detach-reader");
    let mut daemon_cleanup = OwnedOperatorConsoleDaemon::new(&data_dir);
    let mut console = OperatorConsolePty::spawn(&data_dir);
    daemon_cleanup.wait_until_daemon_ready(&mut console);
    let daemon_pid = *daemon_cleanup
        .owned_pids()
        .first()
        .expect("capture detached daemon pid");
    assert_detached_daemon_stdin(daemon_pid);
    console.wait_for("botster-hub> ");
    console.send(&[4]);
    console.wait_for("detached=daemon_running");
    console.wait_for_exit();
    assert!(
        console.reader.is_none(),
        "console exit did not join its PTY reader"
    );

    let status = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("status")
        .arg("--data-dir")
        .arg(&data_dir)
        .output()
        .expect("probe daemon after detached console reader EOF");
    assert!(
        status.status.success(),
        "daemon did not remain running after console reader EOF: {}",
        command_output_text(&status)
    );
    daemon_cleanup.assert_cleaned();
    fs::remove_dir_all(&data_dir).expect("remove detached-reader console data directory");
}

#[test]
fn operator_console_ctrl_c_reaches_foreground_app_process_group_and_returns_prompt() {
    let _guard = daemon_test_guard();
    ensure_session_worker_binary();
    let data_dir = unique_short_test_dir("console-foreground-interrupt");
    let package_dir =
        unique_short_test_dir("console-foreground-interrupt-package").join("package with spaces");
    write_botster_tui_package_with_script(&package_dir, DETERMINISTIC_FOREGROUND_INTERRUPT_SCRIPT);

    let mut daemon_cleanup = OwnedOperatorConsoleDaemon::new(&data_dir);
    let mut console = OperatorConsolePty::spawn(&data_dir);
    daemon_cleanup.wait_until_daemon_ready(&mut console);
    console.wait_for("botster-hub> ");
    console.send_and_wait_for_prompt(
        format!(
            "packages install --path {}\n",
            shell_words::quote(&package_dir.to_string_lossy())
        )
        .as_bytes(),
    );
    console.send_and_wait_for_prompt(b"packages enable botster-tui\n");

    let prompt_after_interrupt = console.prompt_count() + 1;
    console.send(b"apps open botster-tui\n");
    console.wait_for("foreground-forward-ready");
    let foreground_interrupt_checkpoint = console.output_checkpoint();
    console.send(&[3]);
    console.wait_for_output_after(
        foreground_interrupt_checkpoint,
        "foreground app exited with code 130",
    );
    console.wait_for_output_after(foreground_interrupt_checkpoint, "botster-hub> ");
    assert_eq!(
        console.prompt_count(),
        prompt_after_interrupt,
        "foreground interrupt printed an unexpected number of prompts: {}",
        console.text()
    );
    assert!(
        !console
            .text()
            .contains("interrupt requested; finishing safely"),
        "foreground Ctrl-C was handled as inline console work: {}",
        console.text()
    );

    console.send(b"shutdown\n");
    console.wait_for_exit();
    daemon_cleanup.assert_cleaned();
    fs::remove_dir_all(&data_dir).expect("remove foreground-interrupt data directory");
    fs::remove_dir_all(
        package_dir
            .parent()
            .expect("foreground-interrupt package parent"),
    )
    .expect("remove foreground-interrupt package directory");
}

#[test]
fn process_ownership_operator_console_readiness_failure_reaps_console_and_owned_daemon() {
    let _guard = daemon_test_guard();
    ensure_session_worker_binary();
    let data_dir = unique_short_test_dir("console-readiness-failure");
    let mut daemon_cleanup = OwnedOperatorConsoleDaemon::new(&data_dir);
    let mut console = OperatorConsolePty::spawn_with_env(
        &data_dir,
        &[(TEST_LOCAL_RUNTIME_READINESS_BUDGET_MS_ENV, "1")],
    );
    let error = console
        .try_wait_for_occurrences("botster-hub> ", 1)
        .expect_err("injected daemon readiness failure should stop console startup");
    console.wait_for_exit();
    let output = console.text();
    let daemon_pid = output
        .split("terminated owned child_pid=")
        .nth(1)
        .and_then(|tail| {
            tail.chars()
                .take_while(char::is_ascii_digit)
                .collect::<String>()
                .parse::<u32>()
                .ok()
        })
        .expect("production diagnostic includes the terminated owned daemon pid");
    daemon_cleanup.record_owned_pid(daemon_pid);

    assert!(error.contains("child exited before condition"), "{error}");
    assert!(
        output.contains("timed out waiting for local runtime daemon readiness"),
        "{output}"
    );
    assert!(
        output.contains("(budget 1ms)"),
        "the injected production readiness budget was not observed: {output}"
    );
    assert!(
        output.contains("terminated owned child_pid="),
        "production failure diagnostic omitted terminated daemon evidence: {output}"
    );
    daemon_cleanup.assert_cleaned();
    assert!(
        !process_exists(daemon_pid),
        "induced readiness failure left exact daemon pid {daemon_pid} alive"
    );
    fs::remove_dir_all(&data_dir).expect("remove readiness-failure console data directory");
}

#[test]
fn operator_console_panic_reaps_console_and_owned_daemon() {
    let _guard = daemon_test_guard();
    ensure_session_worker_binary();
    let data_dir = unique_short_test_dir("console-panic-cleanup");
    let observed_pids = Arc::new(Mutex::new((None, None)));
    let unwind_pids = Arc::clone(&observed_pids);
    let unwind_data_dir = data_dir.clone();

    let unwind = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
        let mut daemon_cleanup = OwnedOperatorConsoleDaemon::new(&unwind_data_dir);
        let mut console = OperatorConsolePty::spawn(&unwind_data_dir);
        daemon_cleanup.wait_until_daemon_ready(&mut console);
        let console_pid = console
            .child
            .process_id()
            .expect("operator console fixture exposes a process id");
        let daemon_pid = *daemon_cleanup
            .owned_pids()
            .first()
            .expect("capture panic-test owned daemon pid");
        *unwind_pids
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) =
            (Some(console_pid), Some(daemon_pid));
        panic!("induced operator console panic");
    }));
    assert!(unwind.is_err(), "panic fixture should unwind");

    let (console_pid, daemon_pid) = *observed_pids
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let console_pid = console_pid.expect("recorded panic-test console pid");
    let daemon_pid = daemon_pid.expect("recorded panic-test daemon pid");
    wait_for_owned_pid_exit(console_pid, Duration::from_secs(2));
    wait_for_owned_pid_exit(daemon_pid, Duration::from_secs(2));
    assert!(
        !process_exists(console_pid),
        "panic left operator console pid {console_pid} alive"
    );
    assert!(
        !process_exists(daemon_pid),
        "panic left owned daemon pid {daemon_pid} alive"
    );
    let status = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("status")
        .arg("--data-dir")
        .arg(&data_dir)
        .output()
        .expect("probe panic-cleaned daemon");
    assert!(
        !status.status.success(),
        "panic cleanup left typed daemon status running: {}",
        command_output_text(&status)
    );
    assert!(
        !data_dir.join(".botster-hub-runtime-daemon.json").exists(),
        "panic cleanup left daemon metadata"
    );
    assert!(
        !explicit_config(&data_dir)
            .transports
            .local_socket
            .as_ref()
            .expect("panic-test local socket binding")
            .path
            .exists(),
        "panic cleanup left daemon socket"
    );
    fs::remove_dir_all(&data_dir).expect("remove panic-cleanup console data directory");
}

fn shutdown_cli_daemon(data_dir: &Path, child: Child) -> Output {
    let shutdown = request_cli_daemon_shutdown(data_dir).expect("run botster-hub shutdown");
    wait_for_cli_daemon_shutdown(&shutdown, child)
}

fn request_cli_daemon_shutdown(data_dir: &Path) -> io::Result<Output> {
    Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("shutdown")
        .arg("--data-dir")
        .arg(data_dir)
        .output()
}

fn local_webrtc_sender_failure(stderr: &[u8]) -> Option<&str> {
    std::str::from_utf8(stderr)
        .ok()?
        .lines()
        .rev()
        .find(|line| line.starts_with("local WebRTC response delivery failed:"))
}

fn local_webrtc_grant_id(output: &Output) -> Option<String> {
    command_output_text(output)
        .lines()
        .find_map(|line| line.strip_prefix("local_webrtc_grant_id="))
        .filter(|grant_id| !grant_id.is_empty() && grant_id.len() <= 128)
        .map(str::to_string)
}

fn local_webrtc_sender_terminal_record(
    data_dir: &Path,
    expected_grant_id: &str,
) -> serde_json::Value {
    let path = data_dir.join(LOCAL_WEBRTC_SENDER_TERMINAL_RECORD_FILE);
    assert!(
        !path.with_extension("json.tmp").exists(),
        "same-directory replacement must not leave a temporary sender record"
    );
    let bytes = fs::read(&path).expect("read persisted local WebRTC sender terminal record");
    assert!(
        bytes.len() <= LOCAL_WEBRTC_SENDER_TERMINAL_RECORD_MAX_BYTES,
        "sender terminal record exceeded fixed size bound"
    );
    let record: serde_json::Value = serde_json::from_slice(&bytes)
        .expect("parse persisted local WebRTC sender terminal record");
    let object = record
        .as_object()
        .expect("sender terminal record has a fixed JSON object schema");
    let mut actual_fields = object.keys().map(String::as_str).collect::<Vec<_>>();
    actual_fields.sort_unstable();
    let mut expected_fields = vec![
        "schema_version",
        "grant_id",
        "request_operation",
        "message_id",
        "next_chunk_index",
        "last_sent_chunk_index",
        "total_chunks",
        "pressured",
        "peer_connection_state",
        "channel_terminal_signal",
        "cause",
        "cleanup_disposition",
    ];
    expected_fields.sort_unstable();
    assert_eq!(actual_fields, expected_fields);
    assert_eq!(record["schema_version"], 1);
    assert_eq!(record["grant_id"], expected_grant_id);
    assert!(
        matches!(
            record["request_operation"].as_str(),
            Some(
                "status"
                    | "spawn"
                    | "attach"
                    | "send_input"
                    | "drain"
                    | "shutdown_session"
                    | "request_queue_overflow"
                    | "none"
                    | "other"
            )
        ),
        "sender record has a typed request operation: {record}"
    );
    assert!(record["message_id"].is_null() || record["message_id"].is_string());
    assert!(record["next_chunk_index"].is_u64());
    assert!(record["last_sent_chunk_index"].is_null() || record["last_sent_chunk_index"].is_u64());
    assert!(record["total_chunks"].is_u64());
    assert!(record["pressured"].is_boolean());
    assert!(
        matches!(
            record["peer_connection_state"].as_str(),
            Some(
                "unspecified"
                    | "new"
                    | "connecting"
                    | "connected"
                    | "disconnected"
                    | "failed"
                    | "closed"
            )
        ),
        "sender record has a typed peer state: {record}"
    );
    assert!(
        matches!(
            record["channel_terminal_signal"].as_str(),
            Some("none" | "on_close" | "on_error" | "poll_ended")
        ),
        "sender record has a typed channel signal: {record}"
    );
    assert!(
        matches!(
            record["cause"].as_str(),
            Some(
                "send_text"
                    | "channel_closed"
                    | "channel_error"
                    | "poll_ended"
                    | "invalid_request"
                    | "request_queue_overflow"
                    | "invalid_encrypted_request"
                    | "runtime_queue_closed"
                    | "response_framing"
                    | "low_water_threshold_setup"
                    | "high_water_threshold_setup"
                    | "peer_disconnected"
                    | "peer_failed"
                    | "peer_closed"
            )
        ),
        "sender record has a typed terminal cause: {record}"
    );
    assert_eq!(record["cleanup_disposition"], "newly_sent");
    let text = String::from_utf8(bytes).expect("sender terminal record is UTF-8 JSON");
    for forbidden in [
        "grant_secret",
        "payload",
        "request_body",
        "response_body",
        env!("CARGO_MANIFEST_DIR"),
    ] {
        assert!(
            !text.contains(forbidden),
            "sender terminal record contains forbidden data {forbidden:?}: {text}"
        );
    }
    assert!(
        !text.contains(&data_dir.display().to_string()),
        "sender terminal record contains its data-directory path"
    );
    record
}

fn local_webrtc_smoke_failure_evidence(output: &Output, data_dir: &Path) -> String {
    let text = command_output_text(output);
    let Some(grant_id) = local_webrtc_grant_id(output) else {
        return format!("smoke failed before local WebRTC bootstrap: {text}");
    };
    let record_path = data_dir.join(LOCAL_WEBRTC_SENDER_TERMINAL_RECORD_FILE);
    if !record_path.is_file() {
        return format!(
            "smoke failed: {text}; sender_record=missing file={LOCAL_WEBRTC_SENDER_TERMINAL_RECORD_FILE}"
        );
    }
    let terminal_record = local_webrtc_sender_terminal_record(data_dir, &grant_id);
    format!("smoke failed: {text}; sender_record={terminal_record}")
}

#[test]
fn local_webrtc_sender_terminal_record_rejects_stale_malformed_and_oversized_evidence() {
    let data_dir = unique_test_dir("local-webrtc-terminal-record-validation");
    fs::create_dir_all(&data_dir).expect("create terminal record validation directory");
    let path = data_dir.join(LOCAL_WEBRTC_SENDER_TERMINAL_RECORD_FILE);
    let valid_record = serde_json::json!({
        "schema_version": 1,
        "grant_id": "grant-current",
        "request_operation": "status",
        "message_id": null,
        "next_chunk_index": 0,
        "last_sent_chunk_index": null,
        "total_chunks": 0,
        "pressured": false,
        "peer_connection_state": "closed",
        "channel_terminal_signal": "on_close",
        "cause": "channel_closed",
        "cleanup_disposition": "newly_sent",
    });

    fs::write(
        &path,
        serde_json::to_vec(&valid_record).expect("serialize validation fixture"),
    )
    .expect("write stale validation fixture");
    assert!(
        std::panic::catch_unwind(|| {
            local_webrtc_sender_terminal_record(&data_dir, "grant-other")
        })
        .is_err(),
        "a record for another grant must not satisfy the evidence gate"
    );

    fs::write(&path, b"{\"schema_version\":1").expect("write truncated validation fixture");
    assert!(
        std::panic::catch_unwind(|| {
            local_webrtc_sender_terminal_record(&data_dir, "grant-current")
        })
        .is_err(),
        "a truncated record must not satisfy the evidence gate"
    );

    fs::write(
        &path,
        vec![b'x'; LOCAL_WEBRTC_SENDER_TERMINAL_RECORD_MAX_BYTES + 1],
    )
    .expect("write oversized validation fixture");
    assert!(
        std::panic::catch_unwind(|| {
            local_webrtc_sender_terminal_record(&data_dir, "grant-current")
        })
        .is_err(),
        "an oversized record must not satisfy the evidence gate"
    );
}

fn local_webrtc_bounded_stderr_tail(stderr: &[u8], data_dir: &Path) -> String {
    const MAX_LINES: usize = 20;
    const MAX_CHARS_PER_LINE: usize = 512;

    let stderr = String::from_utf8_lossy(stderr);
    let mut lines = stderr.lines().rev().take(MAX_LINES).collect::<Vec<_>>();
    lines.reverse();
    let mut tail = lines
        .into_iter()
        .map(|line| {
            let mut bounded = line.chars().take(MAX_CHARS_PER_LINE).collect::<String>();
            if line.chars().count() > MAX_CHARS_PER_LINE {
                bounded.push_str("<truncated>");
            }
            bounded
        })
        .collect::<Vec<_>>()
        .join("\n");

    for (path, replacement) in [
        (Some(data_dir.to_path_buf()), "<data-dir>"),
        (
            Some(PathBuf::from(env!("CARGO_MANIFEST_DIR"))),
            "<workspace>",
        ),
        (std::env::var_os("HOME").map(PathBuf::from), "<home>"),
        (Some(std::env::temp_dir()), "<temp>"),
    ] {
        if let Some(path) = path.and_then(|path| path.to_str().map(str::to_owned))
            && !path.is_empty()
        {
            tail = tail.replace(&path, replacement);
        }
    }

    if tail.is_empty() {
        "<empty>".to_string()
    } else {
        tail
    }
}

#[test]
fn local_webrtc_diagnostic_stderr_tail_is_bounded_and_redacts_paths() {
    let data_dir = std::env::temp_dir().join("local-webrtc-diagnostic-data");
    let mut lines = (0..25)
        .map(|index| format!("diagnostic line {index}"))
        .collect::<Vec<_>>();
    lines[23] = "x".repeat(600);
    lines[24] = format!(
        "data={} workspace={} home={} temp={}",
        data_dir.display(),
        env!("CARGO_MANIFEST_DIR"),
        std::env::var("HOME").unwrap_or_default(),
        std::env::temp_dir().display()
    );

    let tail = local_webrtc_bounded_stderr_tail(lines.join("\n").as_bytes(), &data_dir);

    assert!(!tail.contains("diagnostic line 4"));
    assert!(tail.contains("diagnostic line 5"));
    assert!(tail.contains("<truncated>"));
    assert!(tail.contains("<data-dir>"));
    assert!(tail.contains("<workspace>"));
    assert!(tail.contains("<home>"));
    assert!(tail.contains("<temp>"));
    assert!(!tail.contains(&data_dir.display().to_string()));
    assert!(!tail.contains(env!("CARGO_MANIFEST_DIR")));
}

struct PanicSafeCliDaemon {
    data_dir: PathBuf,
    child: Option<Child>,
    panic_context: &'static str,
    inspect_local_webrtc_sender: bool,
}

impl PanicSafeCliDaemon {
    fn start(data_dir: &Path, panic_context: &'static str) -> Self {
        Self {
            data_dir: data_dir.to_path_buf(),
            child: Some(start_cli_daemon(data_dir)),
            panic_context,
            inspect_local_webrtc_sender: false,
        }
    }

    fn start_with_local_webrtc_diagnostics(data_dir: &Path) -> Self {
        Self {
            data_dir: data_dir.to_path_buf(),
            child: Some(start_cli_daemon(data_dir)),
            panic_context: "local WebRTC target sender evidence",
            inspect_local_webrtc_sender: true,
        }
    }

    fn shutdown(mut self) {
        let child = self.child.take().expect("panic-safe daemon child");
        shutdown_cli_daemon(&self.data_dir, child);
    }
}

impl Drop for PanicSafeCliDaemon {
    fn drop(&mut self) {
        let Some(mut child) = self.child.take() else {
            return;
        };

        if !std::thread::panicking() {
            shutdown_cli_daemon(&self.data_dir, child);
            return;
        }

        let shutdown = request_cli_daemon_shutdown(&self.data_dir);
        let shutdown_failed = shutdown.as_ref().map_or(true, |output| {
            !output.status.success()
                && String::from_utf8_lossy(&output.stderr).trim()
                    != "botster-hub shutdown error: client disconnected"
        });
        if shutdown_failed && child.try_wait().ok().flatten().is_none() {
            let _ = child.kill();
        }

        match child.wait_with_output() {
            Ok(daemon) => {
                if self.inspect_local_webrtc_sender
                    && let Some(failure) = local_webrtc_sender_failure(&daemon.stderr)
                {
                    eprintln!("{}: {failure}", self.panic_context);
                    return;
                }
                eprintln!(
                    "{}: unavailable; daemon_status={}; daemon_stderr_tail={:?}",
                    self.panic_context,
                    daemon.status,
                    local_webrtc_bounded_stderr_tail(&daemon.stderr, &self.data_dir)
                );
            }
            Err(error) => eprintln!(
                "{}: unavailable; daemon_status=unavailable; daemon_wait_error_kind={:?}",
                self.panic_context,
                error.kind()
            ),
        }
    }
}

#[test]
fn cli_daemon_shutdown_rejects_exact_disconnect_after_clean_exit() {
    let shutdown =
        shell_output("printf 'botster-hub shutdown error: client disconnected\\n' >&2; exit 1");
    let daemon = shell_output("exit 0");

    let error = validate_cli_daemon_shutdown(&shutdown, &daemon)
        .expect_err("shutdown disconnect must remain visible after a clean daemon exit");

    assert!(error.contains("shutdown failed"));
    assert!(error.contains("client disconnected"));
}

#[test]
fn cli_daemon_shutdown_rejects_unrelated_command_error_after_clean_exit() {
    let shutdown =
        shell_output("printf 'botster-hub shutdown error: permission denied\\n' >&2; exit 1");
    let daemon = shell_output("exit 0");

    let error = validate_cli_daemon_shutdown(&shutdown, &daemon)
        .expect_err("unrelated shutdown error must be rejected");

    assert!(error.contains("shutdown failed"));
    assert!(error.contains("permission denied"));
}

#[test]
fn cli_daemon_shutdown_rejects_unclean_exit_with_disconnect_diagnostics() {
    let shutdown =
        shell_output("printf 'botster-hub shutdown error: client disconnected\\n' >&2; exit 1");
    let daemon = shell_output("printf 'daemon crash\\n' >&2; exit 42");

    let error = validate_cli_daemon_shutdown(&shutdown, &daemon)
        .expect_err("unclean daemon exit must be rejected");

    assert!(error.contains("daemon failed"));
    assert!(error.contains("daemon crash"));
    assert!(error.contains("client disconnected"));
}

fn shell_output(script: &str) -> Output {
    Command::new("sh")
        .arg("-c")
        .arg(script)
        .output()
        .expect("run shell fixture")
}

fn run_local_runtime_up(
    data_dir: &Path,
    _project_pipelines_package_path: &Path,
    web_package_path: &Path,
    tui_package_path: &Path,
    _workspaces_package_path: &Path,
    _web_port: u16,
) -> Output {
    ensure_runtime_packages(data_dir, web_package_path, tui_package_path);
    Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("up")
        .arg("--data-dir")
        .arg(data_dir)
        .arg("--session-worker-bin")
        .arg(session_worker_binary_path())
        .output()
        .expect("run botster-hub up")
}

fn run_local_runtime_smoke(
    data_dir: &Path,
    _project_pipelines_package_path: &Path,
    web_package_path: &Path,
    tui_package_path: &Path,
    _workspaces_package_path: &Path,
    _web_port: u16,
) -> Output {
    run_local_runtime_smoke_with_fault(
        data_dir,
        _project_pipelines_package_path,
        web_package_path,
        tui_package_path,
        _workspaces_package_path,
        _web_port,
        None,
    )
}

fn run_local_runtime_smoke_with_fault(
    data_dir: &Path,
    _project_pipelines_package_path: &Path,
    web_package_path: &Path,
    tui_package_path: &Path,
    _workspaces_package_path: &Path,
    _web_port: u16,
    close_operation: Option<&str>,
) -> Output {
    ensure_runtime_packages(data_dir, web_package_path, tui_package_path);
    let mut command = Command::new(env!("CARGO_BIN_EXE_botster-hub"));
    command
        .arg("smoke")
        .arg("--data-dir")
        .arg(data_dir)
        .arg("--session-worker-bin")
        .arg(session_worker_binary_path());
    if let Some(operation) = close_operation {
        command.env(TEST_CLOSE_LOCAL_WEBRTC_OPERATION_ENV, operation);
    }
    command.output().expect("run botster-hub smoke")
}

fn ensure_runtime_packages(data_dir: &Path, web_package_path: &Path, tui_package_path: &Path) {
    ensure_session_worker_binary();
    let config = explicit_config(data_dir);
    let mut setup_daemon =
        match botster_hub::daemon_transport_request(&config, botster_hub::DaemonRequest::Status) {
            Ok(_) => None,
            Err(_) => Some(start_cli_daemon_with_session_worker(
                data_dir,
                &session_worker_binary_path(),
            )),
        };
    let installed =
        botster_hub::daemon_transport_request(&config, botster_hub::DaemonRequest::ListPackages)
            .expect("list installed runtime packages")
            .packages;
    for (name, path) in [
        ("botster-web", web_package_path),
        ("botster-tui", tui_package_path),
    ] {
        if !installed.iter().any(|package| package.package_name == name) {
            let install = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
                .arg("packages")
                .arg("install")
                .arg("--data-dir")
                .arg(data_dir)
                .arg("--path")
                .arg(path)
                .output()
                .expect("install runtime package");
            assert!(
                install.status.success(),
                "install {name} failed: {}",
                command_output_text(&install)
            );
        }
        let enable = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
            .arg("packages")
            .arg("enable")
            .arg("--data-dir")
            .arg(data_dir)
            .arg(name)
            .output()
            .expect("enable runtime package");
        assert!(
            enable.status.success(),
            "enable {name} failed: {}",
            command_output_text(&enable)
        );
    }
    if let Some(child) = setup_daemon.as_mut() {
        shutdown_local_runtime_daemon(data_dir);
        let status = child.wait().expect("wait for setup daemon");
        assert!(status.success(), "setup daemon exited with {status}");
    }
}

fn assert_smoke_owned_daemon_gone(data_dir: &Path) {
    let status = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("status")
        .arg("--data-dir")
        .arg(data_dir)
        .output()
        .expect("run botster-hub status after smoke-owned daemon cleanup");
    assert!(
        !status.status.success(),
        "smoke should stop the daemon it started: {}",
        command_output_text(&status)
    );
}

fn shutdown_local_runtime_daemon(data_dir: &Path) {
    let output = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("shutdown")
        .arg("--data-dir")
        .arg(data_dir)
        .output()
        .expect("run local-runtime shutdown");
    assert!(
        output.status.success(),
        "local-runtime shutdown failed: {}",
        command_output_text(&output)
    );
}

fn has_diagnostic_kind(
    diagnostics: &[botster_hub_client::DaemonDiagnostic],
    kind: botster_hub_client::DaemonDiagnosticKind,
) -> bool {
    diagnostics.iter().any(|diagnostic| diagnostic.kind == kind)
}

fn has_failure_diagnostic(diagnostics: &[botster_hub_client::DaemonDiagnostic]) -> bool {
    diagnostics.iter().any(|diagnostic| {
        matches!(
            diagnostic.kind,
            botster_hub_client::DaemonDiagnosticKind::CompatibilityMismatch
                | botster_hub_client::DaemonDiagnosticKind::UnsupportedFeature
                | botster_hub_client::DaemonDiagnosticKind::TerminalStreamUnavailable
                | botster_hub_client::DaemonDiagnosticKind::ActionFailure
                | botster_hub_client::DaemonDiagnosticKind::DaemonStartupFailure
        )
    })
}

fn session_worker_binary_path() -> PathBuf {
    ensure_session_worker_binary();
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("target")
        .join("debug")
        .join("botster-session-worker")
}

struct TimedCommandOutput {
    output: Output,
    elapsed: Duration,
}

impl TimedCommandOutput {
    fn diagnostics(&self) -> String {
        format!(
            "elapsed={:?} status={} stdout={:?} stderr={:?}",
            self.elapsed,
            self.output.status,
            String::from_utf8_lossy(&self.output.stdout),
            String::from_utf8_lossy(&self.output.stderr),
        )
    }
}

fn child_state_diagnostics(child: &mut Child) -> String {
    match child.try_wait().expect("poll child for diagnostics") {
        None => "running".to_string(),
        Some(status) => {
            let (stdout, stderr) = collect_child_output(child);
            format!("exited status={status} stdout={stdout:?} stderr={stderr:?}")
        }
    }
}

fn run_command_with_timeout_diagnostics(
    stage: &str,
    mut command: Command,
    timeout: Duration,
) -> TimedCommandOutput {
    let started_at = std::time::Instant::now();
    let mut child = command
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn timed command");
    let deadline = started_at + timeout;

    while std::time::Instant::now() < deadline {
        if child.try_wait().expect("poll timed command").is_some() {
            return TimedCommandOutput {
                output: child.wait_with_output().expect("collect timed command"),
                elapsed: started_at.elapsed(),
            };
        }
        thread::sleep(Duration::from_millis(20));
    }

    let _ = child.kill();
    let output = child.wait_with_output().expect("collect timed out command");
    panic!(
        "{stage} timed out after {:?} (budget {timeout:?}): status={} stdout={:?} stderr={:?}",
        started_at.elapsed(),
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
}

#[derive(Debug)]
struct BufferedStdoutObservation {
    available_bytes: usize,
    elapsed: Duration,
    recent_samples: VecDeque<(Duration, usize)>,
}

fn wait_for_buffered_child_stdout(
    child: &mut Child,
    minimum_bytes: usize,
    stable_samples_required: usize,
    timeout: Duration,
) -> Result<BufferedStdoutObservation, String> {
    let started_at = std::time::Instant::now();
    let deadline = started_at + timeout;
    let mut previous_bytes = None;
    let mut stable_samples = 0;
    let mut last_available_bytes = 0;
    let mut recent_samples = VecDeque::with_capacity(25);

    while std::time::Instant::now() < deadline {
        if let Some(status) = child.try_wait().expect("poll buffered stdout child") {
            let (stdout, stderr) = collect_child_output(child);
            return Err(format!(
                "child exited with {status} before stdout backpressure after {:?}: available_bytes={last_available_bytes} recent_samples={recent_samples:?} stdout={stdout:?} stderr={stderr:?}",
                started_at.elapsed()
            ));
        }

        let stdout = child.stdout.as_ref().expect("buffered stdout child pipe");
        last_available_bytes = pipe_bytes_available(stdout)
            .map_err(|error| format!("inspect buffered child stdout: {error}"))?;
        if recent_samples.len() == recent_samples.capacity() {
            recent_samples.pop_front();
        }
        recent_samples.push_back((started_at.elapsed(), last_available_bytes));
        if last_available_bytes >= minimum_bytes {
            if previous_bytes == Some(last_available_bytes) {
                stable_samples += 1;
            } else {
                stable_samples = 0;
            }
            if stable_samples >= stable_samples_required {
                return Ok(BufferedStdoutObservation {
                    available_bytes: last_available_bytes,
                    elapsed: started_at.elapsed(),
                    recent_samples,
                });
            }
        } else {
            stable_samples = 0;
        }
        previous_bytes = Some(last_available_bytes);
        thread::sleep(Duration::from_millis(20));
    }

    let child_status = terminate_and_reap_child(child);
    let (stdout, stderr) = collect_child_output(child);
    Err(format!(
        "stdout did not reach stable backpressure within {timeout:?}: minimum_bytes={minimum_bytes} stable_samples_required={stable_samples_required} last_available_bytes={last_available_bytes} recent_samples={recent_samples:?} child_status={child_status} stdout={stdout:?} stderr={stderr:?}"
    ))
}

fn pipe_bytes_available(pipe: &impl AsRawFd) -> io::Result<usize> {
    let mut available: libc::c_int = 0;
    let result = unsafe { libc::ioctl(pipe.as_raw_fd(), libc::FIONREAD, &mut available) };
    if result == -1 {
        Err(io::Error::last_os_error())
    } else {
        Ok(available.max(0) as usize)
    }
}

#[test]
fn buffered_child_stdout_wait_observes_backpressure_condition() {
    let mut child = Command::new("sh")
        .arg("-c")
        .arg("exec yes buffered-output")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn buffered stdout fixture");

    let observation = wait_for_buffered_child_stdout(
        &mut child,
        STALLED_ATTACH_MIN_BUFFERED_STDOUT_BYTES,
        STALLED_ATTACH_STABLE_SAMPLES,
        Duration::from_secs(5),
    )
    .expect("observe child stdout backpressure");

    terminate_and_reap_child(&mut child);
    let _ = collect_child_output(&mut child);
    assert!(
        observation.available_bytes >= STALLED_ATTACH_MIN_BUFFERED_STDOUT_BYTES,
        "stdout backpressure should retain at least {} bytes, got {} after {:?}; recent_samples={:?}",
        STALLED_ATTACH_MIN_BUFFERED_STDOUT_BYTES,
        observation.available_bytes,
        observation.elapsed,
        observation.recent_samples,
    );
}

#[test]
fn daemon_package_dtos_expose_declared_surfaces_and_validate_surface_operations() {
    let _guard = daemon_test_guard();
    let data_dir = unique_short_test_dir("package-surfaces");
    let surface_package_dir = unique_test_dir("daemon-declared-surface-package");
    let legacy_package_dir = unique_test_dir("daemon-legacy-surface-package");
    let workspaces_package_dir = unique_test_dir("daemon-workspaces-surface-package");
    let iframe_package_dir = unique_test_dir("daemon-iframe-surface-package");
    write_declared_surface_plugin_package(&surface_package_dir);
    write_local_plugin_package(&legacy_package_dir);
    write_botster_workspaces_local_package(&workspaces_package_dir, "botster-workspaces");
    write_iframe_surface_local_plugin_package(&iframe_package_dir);
    let config = explicit_config(&data_dir);
    let socket_path = config
        .transports
        .local_socket
        .as_ref()
        .expect("test config has local socket")
        .path
        .clone();
    let endpoint = botster_hub_client::DaemonEndpoint::new(socket_path);
    let child = start_cli_daemon(&data_dir);
    let mut connection =
        botster_hub_client::DaemonConnection::connect(&endpoint).expect("external connect");

    let install_surface = connection
        .request(
            &botster_hub_client::DaemonRequest::InstallPackageLocalPath {
                path: surface_package_dir.clone(),
            },
        )
        .expect("install package with declared surfaces");
    assert_eq!(
        install_surface.kind,
        botster_hub_client::DaemonResponseKind::PackageDecision
    );
    let install_legacy = connection
        .request(
            &botster_hub_client::DaemonRequest::InstallPackageLocalPath {
                path: legacy_package_dir,
            },
        )
        .expect("install legacy package without declared surfaces");
    assert_eq!(
        install_legacy.kind,
        botster_hub_client::DaemonResponseKind::PackageDecision
    );
    let enable_workspaces = connection
        .request(&botster_hub_client::DaemonRequest::EnablePackageLocalPath {
            path: workspaces_package_dir,
        })
        .expect("enable workspaces package with declared surface");
    assert_eq!(
        enable_workspaces.kind,
        botster_hub_client::DaemonResponseKind::PackageDecision
    );
    let enable_iframe = connection
        .request(&botster_hub_client::DaemonRequest::EnablePackageLocalPath {
            path: iframe_package_dir,
        })
        .expect("enable iframe package with declared surface");
    assert_eq!(
        enable_iframe.kind,
        botster_hub_client::DaemonResponseKind::PackageDecision
    );

    let packages = connection
        .request(&botster_hub_client::DaemonRequest::ListPackages)
        .expect("list packages with declared surfaces");
    let surface_package = packages
        .packages
        .iter()
        .find(|package| package.package_name == "runtime.surface-plugin")
        .expect("surface package listed");
    assert_eq!(surface_package.surfaces.len(), 2);
    let surface = &surface_package.surfaces[0];
    assert_eq!(surface.id, "runtime.surface.home");
    assert_eq!(surface.kind, botster_ui_contract::PackageSurfaceKind::App);
    assert_eq!(surface.title, "Runtime Surface");
    assert_eq!(
        surface.description.as_deref(),
        Some("Surface descriptor fixture")
    );
    assert_eq!(surface.icon.as_deref(), Some("workflow"));
    assert_eq!(surface.order, Some(20));
    assert_eq!(surface.category.as_deref(), Some("runtime"));
    assert_eq!(
        surface.supports,
        [
            botster_ui_contract::PackageSurfaceOperation::Render,
            botster_ui_contract::PackageSurfaceOperation::Action
        ]
    );

    let show = connection
        .request(&botster_hub_client::DaemonRequest::ShowPackage {
            package_name: "runtime.surface-plugin".to_string(),
        })
        .expect("show package with declared surfaces");
    assert_eq!(show.packages.len(), 1);
    assert_eq!(show.packages[0].surfaces, surface_package.surfaces);

    let workspaces = connection
        .request(&botster_hub_client::DaemonRequest::PluginSurfaceRender {
            package_name: "botster-workspaces".to_string(),
            surface_id: "workspaces".to_string(),
            payload: serde_json::json!({}),
        })
        .expect("workspaces surface render returns plugin surface envelope");
    assert_eq!(
        workspaces.kind,
        botster_hub_client::DaemonResponseKind::PluginSurface
    );
    let plugin_surface = workspaces
        .plugin_surface
        .expect("workspaces render includes plugin surface");
    let plugin_surface_body =
        serde_json::to_value(&plugin_surface.body).expect("serialize typed workspaces surface");
    assert_eq!(plugin_surface.package_name, "botster-workspaces");
    assert_eq!(plugin_surface.surface_id, "workspaces");
    assert_eq!(plugin_surface_body["type"], "panel");
    assert_eq!(plugin_surface_body["id"], "botster-workspaces-panel");
    let snapshot = plugin_surface
        .ui_tree_snapshot
        .as_ref()
        .expect("workspaces render includes validated ui tree snapshot");
    assert_eq!(snapshot.package_name, "botster-workspaces");
    assert_eq!(snapshot.surface_id, "workspaces");
    let snapshot_body =
        serde_json::to_value(&snapshot.body).expect("serialize typed workspaces snapshot");
    assert_eq!(snapshot_body["id"], "botster-workspaces-panel");

    let iframe = connection
        .request(&botster_hub_client::DaemonRequest::PluginSurfaceRender {
            package_name: "iframe.plugin".to_string(),
            surface_id: "preview".to_string(),
            payload: serde_json::json!({}),
        })
        .expect("iframe surface render returns plugin surface envelope");
    assert_eq!(
        iframe.kind,
        botster_hub_client::DaemonResponseKind::PluginSurface
    );
    let iframe_surface = iframe
        .plugin_surface
        .expect("iframe render includes plugin surface");
    let iframe_surface_body =
        serde_json::to_value(&iframe_surface.body).expect("serialize typed iframe surface");
    assert_eq!(iframe_surface_body["type"], "iframe");
    assert_eq!(iframe_surface_body["id"], "preview-frame");
    assert_eq!(
        iframe_surface_body["props"]["src"],
        "/packages/iframe.plugin/assets/preview.html"
    );
    assert_eq!(iframe_surface_body["props"]["title"], "Preview");
    let iframe_snapshot = iframe_surface
        .ui_tree_snapshot
        .as_ref()
        .expect("iframe render includes validated ui tree snapshot");
    assert_eq!(iframe_snapshot.body, iframe_surface.body);
    assert_no_raw_html_ui_fields(&iframe_surface_body);

    let undeclared = connection
        .request(&botster_hub_client::DaemonRequest::PluginSurfaceRender {
            package_name: "runtime.surface-plugin".to_string(),
            surface_id: "runtime.surface.missing".to_string(),
            payload: serde_json::json!({}),
        })
        .expect("undeclared surface render returns operator frame");
    assert_eq!(
        undeclared.kind,
        botster_hub_client::DaemonResponseKind::OperatorError
    );
    let error = undeclared.error.as_ref().expect("operator error body");
    assert_eq!(error.code, "undeclared_plugin_surface");
    assert_eq!(error.operation, "plugin_surface_render");
    assert!(undeclared.diagnostics.iter().any(|diagnostic| {
        diagnostic.kind == botster_hub_client::DaemonDiagnosticKind::UnsupportedFeature
            && diagnostic.operation.as_deref() == Some("plugin_surface_render")
            && diagnostic.feature.as_deref()
                == Some(botster_hub_client::FEATURE_PLUGIN_SURFACE_RENDER)
    }));

    let undeclared_empty_manifest = connection
        .request(&botster_hub_client::DaemonRequest::PluginSurfaceRender {
            package_name: "runtime.plugin".to_string(),
            surface_id: "legacy.dynamic.surface".to_string(),
            payload: serde_json::json!({}),
        })
        .expect("package without descriptors returns operator error");
    assert_eq!(
        undeclared_empty_manifest.kind,
        botster_hub_client::DaemonResponseKind::OperatorError
    );
    assert_eq!(
        undeclared_empty_manifest
            .error
            .as_ref()
            .expect("undeclared operator error")
            .code,
        "undeclared_plugin_surface"
    );

    let unsupported_action = connection
        .request(&botster_hub_client::DaemonRequest::PluginSurfaceAction {
            package_name: "runtime.surface-plugin".to_string(),
            request: botster_ui_contract::UiActionRequest {
                request_id: botster_ui_contract::UiActionRequestId(
                    "unsupported-action".to_string(),
                ),
                surface_id: botster_ui_contract::UiSurfaceId(
                    "runtime.surface.settings".to_string(),
                ),
                action_id: botster_ui_contract::UiActionId("settings.save".to_string()),
                node_id: None,
                kind: botster_ui_contract::UiActionKind::Submit,
                values: None,
                payload: None,
            },
        })
        .expect("unsupported surface operation returns operator error");
    assert_eq!(
        unsupported_action.kind,
        botster_hub_client::DaemonResponseKind::OperatorError
    );
    let unsupported_error = unsupported_action
        .error
        .as_ref()
        .expect("unsupported operation error");
    assert_eq!(
        unsupported_error.code,
        "unsupported_plugin_surface_operation"
    );
    assert_eq!(unsupported_error.operation, "plugin_surface_action");
    assert!(unsupported_action.diagnostics.iter().any(|diagnostic| {
        diagnostic.kind == botster_hub_client::DaemonDiagnosticKind::UnsupportedFeature
            && diagnostic.operation.as_deref() == Some("plugin_surface_action")
            && diagnostic.feature.as_deref()
                == Some(botster_hub_client::FEATURE_PLUGIN_SURFACE_ACTION)
    }));

    let status = connection
        .request(&botster_hub_client::DaemonRequest::Status)
        .expect("daemon remains responsive after surface validation");
    assert_eq!(status.kind, botster_hub_client::DaemonResponseKind::Status);
    shutdown_cli_daemon(&data_dir, child);
}

#[test]
fn daemon_plugin_contract_matrix_fixture_exercises_public_package_contracts() {
    let _guard = daemon_test_guard();
    let fixture_dir = botster_hub_test_support::copy_plugin_contract_matrix_fixture(
        unique_test_dir("daemon-plugin-contract-matrix-fixture"),
    )
    .expect("copy published plugin contract matrix fixture");
    let hub = botster_hub_test_support::IsolatedHubBuilder::new()
        .hub_bin(env!("CARGO_BIN_EXE_botster-hub"))
        .session_worker_bin(session_worker_binary_path())
        .root(PathBuf::from("/tmp/bh-plugin-contract-matrix"))
        .name("plugin-contract-matrix")
        .start()
        .expect("start isolated hub through public test-support harness");

    let report =
        botster_hub_test_support::run_plugin_contract_matrix_conformance(&hub, fixture_dir)
            .expect("run plugin contract matrix conformance");
    let lifecycle = botster_hub_client::request(
        hub.endpoint(),
        botster_hub_client::DaemonRequest::PluginLifecycleStatus,
    )
    .expect("request live plugin worker counters");
    let counters = lifecycle
        .plugin_worker_counters
        .expect("plugin lifecycle response carries worker counters");
    let expected = CoreEngineOptions::default();
    assert_eq!(
        counters.configured_queue_capacity,
        expected.plugin_worker_queue_capacity
    );
    assert_eq!(
        counters.configured_executor_concurrency,
        expected.plugin_worker_executor_concurrency
    );
    assert!(counters.live_plugin_executors >= 1);
    assert!(counters.live_executor_workers >= 1);
    assert_eq!(counters.queued_jobs, 0);
    assert_eq!(counters.in_flight_jobs, 0);
    assert_eq!(report.package_name, "botster.plugin-contract-matrix");
    assert_eq!(report.installed_state, "installed");
    assert_eq!(report.enabled_state, "enabled");
    assert_eq!(
        report.surface_ids,
        vec![
            "contract.app",
            "contract.empty",
            "contract.sessions",
            "contract.blocked",
            "contract.invalid_body",
            "contract.settings",
        ]
    );
    assert_eq!(report.app_route_target_kind, "plugin_surface");
    assert_eq!(report.app_route_surface_id, "contract.app");
    assert!(report.app_route_blocked_after_install);
    assert_eq!(
        report.invalid_configuration_diagnostic_kind,
        "action_failure"
    );
    assert_eq!(
        report.invalid_configuration_diagnostic_operation,
        "configure"
    );
    assert!(report.invalid_configuration_diagnostic_mentions_rejected_value);
    assert_eq!(report.valid_configuration_mode, "write");
    assert_eq!(report.valid_configuration_secret_state, "redacted");
    assert!(report.list_surfaces_match_enabled);
    assert!(report.show_routes_match_list);
    assert_eq!(report.app_surface_node_id, "contract-app-panel");
    assert_eq!(
        report.app_surface_node_kinds,
        vec![
            "button",
            "button",
            "button",
            "dialog",
            "empty_state",
            "empty_state",
            "form",
            "metric",
            "metric_grid",
            "panel",
            "section",
            "status_badge",
            "table",
            "text",
            "text",
            "text",
            "text_input",
            "toolbar",
        ]
    );
    assert_eq!(
        report.app_surface_snapshot_package_name,
        "botster.plugin-contract-matrix"
    );
    assert_eq!(report.app_surface_snapshot_id, "contract.app");
    assert_eq!(report.app_surface_snapshot_node_id, "contract-app-panel");
    assert_eq!(
        report.app_surface_snapshot_node_kinds,
        report.app_surface_node_kinds
    );
    assert_eq!(report.session_surface_id, "contract.sessions");
    assert_eq!(
        report.session_surface_node_id,
        "contract-session-lifecycle-panel"
    );
    assert_eq!(report.session_surface_binding_family, "/session");
    assert!(report.session_surface_matches_fixture);
    assert_eq!(report.session_surface_references.len(), 5);
    assert_eq!(
        report
            .session_materialized_rows
            .iter()
            .map(|row| row.node_id.as_str())
            .collect::<Vec<_>>(),
        vec!["session-transition", "session-stable-current"]
    );
    assert_eq!(report.session_action_node_id, "session-stable-current");
    assert_eq!(
        report.session_action_payload,
        serde_json::json!({
            "operation": "select_session",
            "session_uuid": "session-stable-current"
        })
    );
    assert_eq!(report.session_action_state, "accepted");
    assert_eq!(
        report.session_action_result_node_id,
        "session-stable-current"
    );
    assert_eq!(
        report.session_action_result_payload,
        report.session_action_payload
    );
    assert_eq!(report.dialog_presence_key, "contract-dialog");
    assert_eq!(report.selected_workspace_equality_key, "selected-workspace");
    assert_eq!(report.selected_workspace_equality_value, "workspace-alpha");
    assert_eq!(report.open_action_id, "contract.action");
    assert_eq!(report.open_action_node_id, "contract-app-open");
    assert_eq!(
        report.open_action_payload,
        serde_json::json!({ "operation": "open" })
    );
    assert_eq!(
        report.open_set_values,
        std::collections::BTreeMap::from([
            ("contract-dialog".to_string(), serde_json::json!(true)),
            (
                "selected-workspace".to_string(),
                serde_json::json!("workspace-alpha"),
            ),
        ])
    );
    let matrix = botster_hub_test_support::first_party_client_support_matrix();
    assert_eq!(
        matrix.plugin_surfaces.dialog_presence_key,
        report.dialog_presence_key
    );
    assert_eq!(
        matrix.plugin_surfaces.selected_workspace_equality_key,
        report.selected_workspace_equality_key
    );
    assert_eq!(
        matrix.plugin_surfaces.selected_workspace_equality_value,
        report.selected_workspace_equality_value
    );
    assert_eq!(
        matrix.plugin_surfaces.authored_set_values,
        report.open_set_values
    );
    assert!(report.dialog_visible_after_open);
    assert!(report.selected_workspace_visible_after_open);
    assert!(!report.form_reachable_before_open);
    assert_eq!(report.dialog_form_node_id, "contract-app-form");
    assert_eq!(report.dialog_input_node_id, "contract-app-message");
    assert_eq!(report.submit_action_node_id, "contract-app-form");
    assert!(!report.actionable_sibling_form_during_dialog);
    assert_eq!(
        report.invalid_submit_values,
        serde_json::json!({ "message": "   " })
    );
    assert_eq!(
        report.valid_submit_values,
        serde_json::json!({ "message": "hello" })
    );
    assert!(report.rejected_state_retained);
    assert!(report.rejected_tree_retained);
    assert!(report.rejected_dialog_retained);
    assert!(report.rejected_form_retained);
    assert_eq!(report.rejected_field_error_node_id, "contract-app-message");
    assert_eq!(
        report.accepted_normalized_values,
        serde_json::json!({ "message": "hello" })
    );
    assert!(report.accepted_replacement_applied);
    assert!(report.dialog_state_cleared);
    assert!(!report.dialog_visible_after_valid_submit);
    assert_eq!(report.toggle_action_id, "contract.action");
    assert_eq!(report.toggle_action_node_id, "contract-app-toggle");
    assert_eq!(
        report.toggle_action_payload,
        serde_json::json!({ "operation": "toggle" })
    );
    assert_eq!(report.toggle_key, "contract-toggle");
    assert_eq!(report.toggle_visible_states, vec![false, true, false]);
    assert_eq!(report.empty_surface_child_id, "contract-empty-message");
    assert_eq!(report.blocked_render_operation, "plugin_surface_render");
    assert!(report.blocked_render_message_contains_failure);
    assert_eq!(report.invalid_body_error_code, "invalid_surface");
    assert_eq!(report.invalid_body_operation, "plugin_surface_render");
    assert_eq!(report.invalid_body_diagnostic_kind, "action_failure");
    assert_eq!(
        report.invalid_body_diagnostic_operation,
        "plugin_surface_render"
    );
    assert_eq!(report.settings_surface_node_id, "contract-settings-panel");
    assert!(report.settings_text_contains_endpoint);
    assert!(report.settings_text_contains_mode);
    assert!(report.settings_text_contains_redacted_secret);
    assert_eq!(report.action_success_state, "accepted");
    assert_eq!(report.action_success_message, "hello");
    assert_eq!(
        report.action_success_presentation_clear_key,
        "contract-dialog"
    );
    assert_eq!(
        report.action_success_replacement_node_id,
        "contract-action-replacement"
    );
    assert_eq!(report.submit_action_id, "contract.action");
    assert_eq!(report.action_error_state, "error");
    assert_eq!(report.action_error_diagnostic_kind, "action_failure");
    assert_eq!(
        report.action_error_diagnostic_operation,
        "plugin_surface_action"
    );
    assert_eq!(report.action_field_error_state, "rejected");
    assert_eq!(
        report.action_field_error_request_id,
        "contract-action-field-error"
    );
    assert_eq!(report.action_field_error_diagnostic_kind, "action_failure");
    assert_eq!(
        report.action_field_error_diagnostic_operation,
        "plugin_surface_action"
    );
    assert_eq!(report.action_field_error_message, "Message is required");
    assert_eq!(report.identity_mismatch_error_code, "invalid_action_result");
    assert_eq!(
        report.identity_mismatch_error_operation,
        "plugin_surface_action"
    );
    assert_eq!(
        report.invalid_replacement_error_code,
        "invalid_action_result"
    );
    assert_eq!(
        report.invalid_replacement_error_operation,
        "plugin_surface_action"
    );
    assert_eq!(
        report.client_render_check.class,
        botster_hub_test_support::ConformanceFailureClass::ClientRendering
    );
    assert_eq!(
        report.failure_classes.producer_contract,
        botster_hub_test_support::ConformanceFailureClass::ProducerContract
    );
    assert_eq!(
        report.failure_classes.environment_setup,
        botster_hub_test_support::ConformanceFailureClass::EnvironmentSetup
    );

    hub.shutdown().expect("shutdown isolated hub");
}

#[test]
fn daemon_project_pipelines_example_exercises_published_surface_conformance() {
    let _guard = daemon_test_guard();
    let hub = botster_hub_test_support::IsolatedHubBuilder::new()
        .hub_bin(env!("CARGO_BIN_EXE_botster-hub"))
        .session_worker_bin(session_worker_binary_path())
        .root(PathBuf::from("/tmp/bh-project-pipelines-conformance"))
        .name("project-pipelines")
        .start()
        .expect("start isolated hub through public test-support harness");
    let package_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("examples")
        .join("project-pipelines");

    let report = botster_hub_test_support::run_project_pipelines_conformance(&hub, package_path)
        .expect("run published Project Pipelines conformance through daemon socket");
    assert_eq!(report.package_state, "enabled");
    assert_eq!(
        report.rendered_surface_id,
        "project-pipelines.create-ticket"
    );
    assert_eq!(report.form_action_id, "project_pipelines.create_ticket");
    assert_eq!(report.invalid_title_error, "Title is required");

    hub.shutdown()
        .expect("shutdown Project Pipelines conformance hub");
}

#[test]
fn cli_doctor_reports_stopped_runtime_with_remediation() {
    let _guard = daemon_test_guard();
    let data_dir = unique_short_test_dir("cli-doctor-stopped");

    let output = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("doctor")
        .arg("--data-dir")
        .arg(&data_dir)
        .output()
        .expect("run botster-hub doctor against stopped runtime");
    assert!(
        !output.status.success(),
        "doctor should fail for stopped runtime: {}",
        command_output_text(&output)
    );
    let text = command_output_text(&output);
    assert!(text.contains("doctor=local_runtime"));
    assert!(text.contains("check name=daemon_running status=fail"));
    assert!(text.contains(&format!(
        "remediation=botster-hub up --data-dir {}",
        data_dir.display()
    )));
}

#[test]
fn cli_local_runtime_up_starts_reuses_and_down_stops_runtime() {
    let _guard = daemon_test_guard();
    let data_dir = unique_short_test_dir("cli-local-runtime-up");
    let project_pipelines_package_dir = unique_test_dir("cli-up-project-pipelines");
    let web_package_dir = unique_test_dir("cli-up-web");
    let tui_package_dir = unique_test_dir("cli-up-tui");
    let workspaces_package_dir = unique_test_dir("cli-up-workspaces");
    write_project_pipelines_availability_package(&project_pipelines_package_dir);
    write_botster_web_package(&web_package_dir);
    write_botster_tui_package(&tui_package_dir);
    write_botster_workspaces_local_package(&workspaces_package_dir, "botster-workspaces");

    let web_listener_port = unused_loopback_port();
    let first = run_local_runtime_up(
        &data_dir,
        &project_pipelines_package_dir,
        &web_package_dir,
        &tui_package_dir,
        &workspaces_package_dir,
        web_listener_port,
    );
    assert!(
        first.status.success(),
        "first up failed: {}",
        command_output_text(&first)
    );
    let first_text = command_output_text(&first);
    assert!(first_text.contains("runtime=ready"));
    assert!(first_text.contains("daemon=started"));
    assert!(first_text.contains("protocol=botster-hub-daemon-v1"));
    assert!(first_text.contains(&format!(
        "protocol_version={}",
        botster_hub_client::PROTOCOL_VERSION
    )));
    assert!(first_text.contains("conformance_fixture_revision="));
    assert!(first_text.contains("package_count=2"));
    assert!(first_text.contains("enabled_package_count=2"));
    assert!(first_text.contains("app_count="));
    assert!(first_text.contains("app package=botster-web app_id=web-client"));
    assert!(first_text.contains("web=http://127.0.0.1:"));
    assert!(!first_text.contains('?'));
    assert!(first_text.contains(&format!(
        "down=botster-hub down --data-dir {}",
        data_dir.display()
    )));
    for package_dir in [
        &project_pipelines_package_dir,
        &web_package_dir,
        &tui_package_dir,
        &workspaces_package_dir,
    ] {
        assert!(
            !first_text.contains(package_dir.to_string_lossy().as_ref()),
            "up output should not leak package source path {package_dir:?}: {first_text}"
        );
    }

    let unchanged = run_local_runtime_up(
        &data_dir,
        &project_pipelines_package_dir,
        &web_package_dir,
        &tui_package_dir,
        &workspaces_package_dir,
        web_listener_port,
    );
    assert!(
        unchanged.status.success(),
        "unchanged up failed: {}",
        command_output_text(&unchanged)
    );
    let unchanged_text = command_output_text(&unchanged);
    assert!(unchanged_text.contains("daemon=reused"));
    let first_web_url = first_text
        .lines()
        .find_map(|line| line.strip_prefix("web="))
        .expect("first up output includes Web URL");
    let unchanged_web_url = unchanged_text
        .lines()
        .find_map(|line| line.strip_prefix("web="))
        .expect("unchanged up output includes Web URL");
    assert_eq!(
        unchanged_web_url, first_web_url,
        "unchanged up should preserve the running Web entrypoint and structured URL"
    );

    rewrite_botster_web_entrypoint(
        &web_package_dir,
        "1.1.0",
        "local-package-server.mjs",
        "reused-up.marker",
    );
    let second = run_local_runtime_up(
        &data_dir,
        &project_pipelines_package_dir,
        &web_package_dir,
        &tui_package_dir,
        &workspaces_package_dir,
        web_listener_port,
    );
    assert!(
        second.status.success(),
        "second up failed: {}",
        command_output_text(&second)
    );
    assert!(command_output_text(&second).contains("daemon=reused"));
    assert!(
        web_package_dir.join("reused-up.marker").is_file(),
        "reused-daemon up should launch the refreshed package entrypoint"
    );
    let config = explicit_config(&data_dir);
    let packages =
        botster_hub::daemon_transport_request(&config, botster_hub::DaemonRequest::ListPackages)
            .expect("list packages after reused up refresh");
    assert_eq!(
        packages
            .packages
            .iter()
            .find(|package| package.package_name == "botster-web")
            .expect("botster-web package after reused up")
            .version,
        "1.1.0"
    );

    let live_idle_connection =
        botster_hub_client::DaemonConnection::connect(&botster_hub_client::DaemonEndpoint::new(
            config
                .transports
                .local_socket
                .as_ref()
                .expect("local runtime socket binding")
                .path
                .clone(),
        ))
        .expect("hold idle connection across down");
    let mut live_entity_subscription = botster_hub_client::subscribe_session_entities(
        &botster_hub_client::DaemonEndpoint::new(
            config
                .transports
                .local_socket
                .as_ref()
                .expect("local runtime socket binding")
                .path
                .clone(),
        ),
        "down-live-entity",
    )
    .expect("hold entity subscription across down");
    assert!(matches!(
        live_entity_subscription
            .next_frame()
            .expect("live entity initial snapshot"),
        botster_hub_client::DaemonEntityFrame::Snapshot { .. }
    ));
    let down = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("down")
        .arg("--data-dir")
        .arg(&data_dir)
        .output()
        .expect("run botster-hub down");
    assert!(
        down.status.success(),
        "down failed: {}",
        command_output_text(&down)
    );
    let down_text = command_output_text(&down);
    assert!(down_text.contains("response=shutdown"));
    drop(live_entity_subscription);
    drop(live_idle_connection);

    rewrite_botster_web_entrypoint(
        &web_package_dir,
        "1.2.0",
        "startup-local-package-server.mjs",
        "startup-up.marker",
    );
    let restarted = run_local_runtime_up(
        &data_dir,
        &project_pipelines_package_dir,
        &web_package_dir,
        &tui_package_dir,
        &workspaces_package_dir,
        unused_loopback_port(),
    );
    assert!(
        restarted.status.success(),
        "immediate up after down failed: {}",
        command_output_text(&restarted)
    );
    let restarted_text = command_output_text(&restarted);
    assert!(restarted_text.contains("runtime=ready"));
    assert!(restarted_text.contains("daemon=started"));
    assert!(
        web_package_dir.join("startup-up.marker").is_file(),
        "fresh-daemon up should launch the refreshed package entrypoint"
    );
    let packages =
        botster_hub::daemon_transport_request(&config, botster_hub::DaemonRequest::ListPackages)
            .expect("list packages after startup up refresh");
    assert_eq!(
        packages
            .packages
            .iter()
            .find(|package| package.package_name == "botster-web")
            .expect("botster-web package after startup up")
            .version,
        "1.2.0"
    );

    shutdown_local_runtime_daemon(&data_dir);

    let status = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("status")
        .arg("--data-dir")
        .arg(&data_dir)
        .output()
        .expect("run botster-hub status after daemon shutdown");
    assert!(
        !status.status.success(),
        "status should fail after daemon shutdown: {}",
        command_output_text(&status)
    );
}

#[test]
fn cli_shutdown_waits_for_metadata_owned_runtime_daemon_cleanup() {
    let _guard = daemon_test_guard();
    ensure_session_worker_binary();
    let data_dir = unique_short_test_dir("cli-shutdown-owned-runtime");
    let web_package_dir = unique_test_dir("cli-shutdown-owned-web");
    let tui_package_dir = unique_test_dir("cli-shutdown-owned-tui");
    write_botster_web_package(&web_package_dir);
    write_botster_tui_package(&tui_package_dir);
    ensure_runtime_packages(&data_dir, &web_package_dir, &tui_package_dir);

    let up = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("up")
        .arg("--data-dir")
        .arg(&data_dir)
        .arg("--session-worker-bin")
        .arg(session_worker_binary_path())
        .output()
        .expect("start metadata-owned runtime daemon");
    assert!(
        up.status.success(),
        "metadata-owned runtime startup failed: {}",
        command_output_text(&up)
    );

    let metadata_path = data_dir.join(".botster-hub-runtime-daemon.json");
    let metadata: serde_json::Value = serde_json::from_slice(
        &fs::read(&metadata_path).expect("read metadata-owned runtime daemon metadata"),
    )
    .expect("parse metadata-owned runtime daemon metadata");
    let daemon_pid = metadata["pid"].as_u64().expect("metadata-owned daemon pid") as u32;
    let socket_path = PathBuf::from(
        metadata["socket_path"]
            .as_str()
            .expect("metadata-owned daemon socket path"),
    );

    let shutdown = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("shutdown")
        .arg("--data-dir")
        .arg(&data_dir)
        .output()
        .expect("shutdown metadata-owned runtime daemon");
    assert!(
        shutdown.status.success(),
        "metadata-owned runtime shutdown failed: {}",
        command_output_text(&shutdown)
    );
    assert!(
        !process_exists(daemon_pid),
        "shutdown returned before metadata-owned daemon pid {daemon_pid} exited"
    );
    assert!(
        !metadata_path.exists(),
        "shutdown returned before owned runtime metadata was removed"
    );
    assert!(
        !socket_path.exists(),
        "shutdown returned before owned runtime socket was removed"
    );
}

#[test]
fn cli_local_runtime_up_reports_missing_installed_checkout_before_launch() {
    let _guard = daemon_test_guard();
    let data_dir = unique_short_test_dir("cli-up-missing-checkout");
    let project_pipelines_package_dir = unique_test_dir("cli-up-missing-project-pipelines");
    let web_package_dir = unique_test_dir("cli-up-missing-web");
    let tui_package_dir = unique_test_dir("cli-up-missing-tui");
    let workspaces_package_dir = unique_test_dir("cli-up-missing-workspaces");
    write_project_pipelines_availability_package(&project_pipelines_package_dir);
    write_botster_web_package(&web_package_dir);
    write_botster_tui_package(&tui_package_dir);
    write_botster_workspaces_local_package(&workspaces_package_dir, "botster-workspaces");

    let first = run_local_runtime_up(
        &data_dir,
        &project_pipelines_package_dir,
        &web_package_dir,
        &tui_package_dir,
        &workspaces_package_dir,
        unused_loopback_port(),
    );
    assert!(
        first.status.success(),
        "initial up failed: {}",
        command_output_text(&first)
    );
    shutdown_local_runtime_daemon(&data_dir);
    let socket_path = data_dir.join("botster-hub.sock");
    for _ in 0..100 {
        if !socket_path.exists() {
            break;
        }
        thread::sleep(Duration::from_millis(20));
    }
    assert!(
        !socket_path.exists(),
        "initial daemon socket should be gone before failed-start cleanup proof"
    );
    let failed_data_dir = unique_short_test_dir("cli-up-failed-cleanup");
    fs::create_dir_all(&failed_data_dir).expect("create failed-start data directory");
    fs::copy(
        data_dir.join("hub-state.json"),
        failed_data_dir.join("hub-state.json"),
    )
    .expect("copy installed package state into fresh failed-start directory");
    fs::remove_dir_all(&web_package_dir).expect("remove installed web checkout");

    let failed = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("up")
        .arg("--data-dir")
        .arg(&failed_data_dir)
        .arg("--session-worker-bin")
        .arg(session_worker_binary_path())
        .output()
        .expect("run up with missing installed checkout");
    assert!(
        !failed.status.success(),
        "up should fail for missing installed checkout"
    );
    let text = command_output_text(&failed);
    assert!(text.contains("botster-web"), "{text}");
    assert!(
        text.contains(web_package_dir.to_string_lossy().as_ref()),
        "{text}"
    );
    let config = explicit_config(failed_data_dir.clone());
    let status = botster_hub::daemon_transport_request(&config, botster_hub::DaemonRequest::Status);
    assert!(
        matches!(
            status,
            Err(botster_hub::DaemonTransportError::NotRunning)
                | Err(botster_hub::DaemonTransportError::ClientDisconnected)
        ),
        "failed startup should stop the daemon it started: {status:?}"
    );
    let failed_socket_path = failed_data_dir.join("botster-hub.sock");
    for _ in 0..100 {
        if !failed_socket_path.exists() {
            break;
        }
        thread::sleep(Duration::from_millis(20));
    }
    assert!(
        !failed_socket_path.exists(),
        "failed startup left its owned socket: {text}"
    );
}

#[test]
fn process_ownership_cli_local_runtime_up_failure_stops_started_daemon() {
    let _guard = daemon_test_guard();
    let data_dir = unique_short_test_dir("cli-up-post-ready-cleanup");
    let web_package_dir = unique_test_dir("cli-up-post-ready-web");
    let tui_package_dir = unique_test_dir("cli-up-post-ready-tui");
    write_botster_web_package(&web_package_dir);
    write_botster_tui_package(&tui_package_dir);
    let web_manifest_path = web_package_dir.join("botster-package.json");
    let mut web_manifest: serde_json::Value =
        serde_json::from_slice(&fs::read(&web_manifest_path).expect("read Web manifest"))
            .expect("parse Web manifest");
    web_manifest["runnable_entrypoints"][0]["environment"][0]["default"] =
        serde_json::Value::String("not-a-port".to_string());
    fs::write(
        &web_manifest_path,
        serde_json::to_string_pretty(&web_manifest).expect("serialize invalid-port Web manifest"),
    )
    .expect("write invalid-port Web manifest");
    ensure_session_worker_binary();
    ensure_runtime_packages(&data_dir, &web_package_dir, &tui_package_dir);

    let metadata_path = data_dir.join(".botster-hub-runtime-daemon.json");
    let mut up = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("up")
        .arg("--data-dir")
        .arg(&data_dir)
        .arg("--session-worker-bin")
        .arg(session_worker_binary_path())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn up with invalid Web package port");

    for _ in 0..500 {
        if metadata_path.exists() {
            break;
        }
        assert!(
            up.try_wait().expect("poll invalid-port up").is_none(),
            "invalid-port up exited before publishing owned daemon metadata"
        );
        thread::sleep(Duration::from_millis(10));
    }
    assert!(
        metadata_path.exists(),
        "invalid-port up should publish owned daemon metadata before Web launch fails"
    );
    let metadata: serde_json::Value = serde_json::from_slice(
        &fs::read(&metadata_path).expect("read invalid-port daemon metadata"),
    )
    .expect("parse invalid-port daemon metadata");
    let daemon_pid = metadata["pid"].as_u64().expect("metadata pid") as u32;
    let socket_path = PathBuf::from(
        metadata["socket_path"]
            .as_str()
            .expect("metadata socket path"),
    );

    let failed = up.wait_with_output().expect("wait for invalid-port up");
    assert!(
        !failed.status.success(),
        "up should fail for invalid Web package port"
    );
    let text = command_output_text(&failed);
    assert!(text.contains("botster-web"), "{text}");
    wait_for_process_exit(daemon_pid);
    assert!(
        !socket_path.exists(),
        "failed up left its configured owned socket: {socket_path:?}"
    );
    assert!(
        !metadata_path.exists(),
        "failed up left its owned daemon metadata"
    );
}

#[test]
fn process_ownership_metadata_write_failure_reaps_started_daemon_group() {
    let _guard = daemon_test_guard();
    ensure_session_worker_binary();
    let data_dir = unique_short_test_dir("cli-up-metadata-write-failure");
    fs::create_dir_all(&data_dir).expect("create metadata failure data directory");
    let metadata_path = data_dir.join(".botster-hub-runtime-daemon.json");
    fs::create_dir(&metadata_path).expect("block metadata file creation with a directory");
    let socket_path = explicit_config(&data_dir)
        .transports
        .local_socket
        .expect("local socket binding")
        .path;

    let failed = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("up")
        .arg("--data-dir")
        .arg(&data_dir)
        .arg("--session-worker-bin")
        .arg(session_worker_binary_path())
        .output()
        .expect("run up with blocked metadata path");

    assert!(
        !failed.status.success(),
        "metadata write failure must fail local runtime startup"
    );
    let text = command_output_text(&failed);
    assert!(
        text.contains("write local runtime daemon metadata"),
        "{text}"
    );
    let deadline = Instant::now() + Duration::from_secs(2);
    let data_dir_text = data_dir.to_string_lossy();
    loop {
        let process_rows = Command::new("ps")
            .args(["-axo", "command="])
            .output()
            .expect("inspect metadata-failure process rows");
        let process_rows_text = String::from_utf8_lossy(&process_rows.stdout);
        let attributable_rows = process_rows_text
            .lines()
            .filter(|row| row.contains(data_dir_text.as_ref()) && row.contains("botster-hub"))
            .collect::<Vec<_>>();
        if attributable_rows.is_empty() {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "metadata write failure left attributable daemon rows: {attributable_rows:?}"
        );
        thread::sleep(Duration::from_millis(20));
    }
    assert!(
        !socket_path.exists(),
        "metadata write failure left the owned daemon socket"
    );
    fs::remove_dir(&metadata_path).expect("remove metadata blocker");
    fs::remove_dir_all(&data_dir).expect("remove metadata failure data directory");
}

#[test]
fn cli_daily_commands_share_canonical_default_data_directory() {
    let _guard = daemon_test_guard();
    let checkout = unique_short_test_dir("daily");
    let other_checkout = unique_short_test_dir("daily-other-cwd");
    let home = unique_short_test_dir("daily-home");
    let xdg = unique_short_test_dir("daily-xdg");
    let data_dir = home.join(".botster/hub");
    let web_package_dir = unique_short_test_dir("cli-daily-default-web");
    let tui_package_dir = unique_short_test_dir("cli-daily-default-tui");
    fs::create_dir_all(&checkout).expect("create daily command checkout");
    fs::create_dir_all(&other_checkout).expect("create second daily command cwd");
    fs::create_dir_all(&home).expect("create daily command home");
    fs::create_dir_all(&xdg).expect("create ignored XDG root");
    for sibling in [
        "plugins",
        "agents",
        "lua",
        "profiles",
        "shared",
        "workspaces",
    ] {
        let sibling = home.join(".botster").join(sibling);
        fs::create_dir_all(&sibling).expect("create protected Botster sibling");
        fs::write(
            sibling.join("sentinel"),
            sibling.to_string_lossy().as_bytes(),
        )
        .expect("write protected Botster sibling sentinel");
    }
    write_botster_web_package(&web_package_dir);
    write_botster_tui_package(&tui_package_dir);
    ensure_session_worker_binary();
    ensure_runtime_packages(&data_dir, &web_package_dir, &tui_package_dir);

    let mut up_command = Command::new(env!("CARGO_BIN_EXE_botster-hub"));
    up_command
        .current_dir(&checkout)
        .env("HOME", &home)
        .env("XDG_DATA_HOME", &xdg)
        .env_remove("BOTSTER_HUB_DATA_DIR")
        .arg("up")
        .arg("--session-worker-bin")
        .arg(session_worker_binary_path());
    let up = up_command.output().expect("run default botster-hub up");
    assert!(
        up.status.success(),
        "default up failed: {}",
        command_output_text(&up)
    );
    assert!(
        command_output_text(&up).contains(&format!("data_dir=resolved:{}", data_dir.display()))
    );

    let run_daily = |command: &str, args: &[&str]| {
        let mut process = Command::new(env!("CARGO_BIN_EXE_botster-hub"));
        process
            .current_dir(&other_checkout)
            .env("HOME", &home)
            .env("XDG_DATA_HOME", &xdg)
            .env_remove("BOTSTER_HUB_DATA_DIR")
            .arg(command)
            .args(args);
        process.output().expect("run daily command")
    };

    let status = run_daily("status", &[]);
    assert!(
        status.status.success(),
        "default status failed: {}",
        command_output_text(&status)
    );
    assert!(command_output_text(&status).contains("lifecycle_state=running"));

    for (command, args, marker) in [
        ("packages", &["list"][..], "response=packages"),
        ("apps", &["list"][..], "response=apps"),
        ("sessions", &["list"][..], "response=sessions"),
        (
            "session-templates",
            &["list"][..],
            "response=session_templates",
        ),
        ("spawn-targets", &["list"][..], "response=spawn_targets"),
    ] {
        let output = run_daily(command, args);
        assert!(
            output.status.success(),
            "{command} without --data-dir failed: {}",
            command_output_text(&output)
        );
        assert!(
            command_output_text(&output).contains(marker),
            "{command} did not reach the shared daemon: {}",
            command_output_text(&output)
        );
    }
    for (command, args, usage) in [
        (
            "packages",
            &["list", "--registry", "/tmp/ignored"][..],
            "packages list",
        ),
        ("providers", &["list", "extra"][..], "providers list"),
        ("apps", &["list", "extra"][..], "apps list"),
        ("sessions", &["list", "extra"][..], "sessions list"),
        (
            "session-templates",
            &["list", "extra"][..],
            "session-templates list",
        ),
        (
            "spawn-targets",
            &["list", "extra"][..],
            "spawn-targets list",
        ),
        ("shutdown", &["extra"][..], "shutdown"),
        ("mcp-serve", &["extra"][..], "mcp-serve"),
    ] {
        let output = run_daily(command, args);
        assert!(
            !output.status.success(),
            "{command} silently accepted extra operands: {}",
            command_output_text(&output)
        );
        assert!(
            command_output_text(&output).contains(&format!("usage: botster-hub {usage}")),
            "{command} did not report its usage: {}",
            command_output_text(&output)
        );
    }
    let mut mcp_child = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .current_dir(&other_checkout)
        .env("HOME", &home)
        .env("XDG_DATA_HOME", &xdg)
        .env_remove("BOTSTER_HUB_DATA_DIR")
        .arg("mcp-serve")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn default mcp-serve");
    mcp_child
        .stdin
        .as_mut()
        .expect("mcp stdin")
        .write_all(
            br#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18"}}"#,
        )
        .expect("write MCP initialize");
    mcp_child
        .stdin
        .take()
        .expect("close mcp stdin after initialize");
    let mcp = mcp_child.wait_with_output().expect("wait for mcp-serve");
    assert!(
        mcp.status.success(),
        "mcp-serve without --data-dir failed: {}",
        command_output_text(&mcp)
    );
    let mcp_stdout = String::from_utf8(mcp.stdout).expect("MCP output is UTF-8");
    assert!(
        mcp_stdout.contains(r#""protocolVersion":"2025-06-18""#),
        "mcp-serve did not answer initialize through the shared daemon root: {mcp_stdout}"
    );

    let doctor = run_daily("doctor", &[]);
    assert!(
        doctor.status.success(),
        "default doctor failed: {}",
        command_output_text(&doctor)
    );
    let doctor_text = command_output_text(&doctor);
    assert!(doctor_text.contains(&format!("data_dir=resolved:{}", data_dir.display())));
    assert!(doctor_text.contains("check name=daemon_running status=pass"));

    let open_web = run_daily("open", &["web"]);
    assert!(
        open_web.status.success(),
        "default open web failed: {}",
        command_output_text(&open_web)
    );
    assert!(command_output_text(&open_web).contains("app_url=http://127.0.0.1:"));

    let open_tui = run_daily("open", &["tui"]);
    assert!(
        open_tui.status.success(),
        "default open tui failed: {}",
        command_output_text(&open_tui)
    );
    assert!(command_output_text(&open_tui).contains("botster-tui-fixture"));

    let mut smoke_command = Command::new(env!("CARGO_BIN_EXE_botster-hub"));
    smoke_command
        .current_dir(&checkout)
        .env("HOME", &home)
        .env("XDG_DATA_HOME", &xdg)
        .env_remove("BOTSTER_HUB_DATA_DIR")
        .arg("smoke")
        .arg("--session-worker-bin")
        .arg(session_worker_binary_path());
    let smoke = smoke_command
        .output()
        .expect("run default botster-hub smoke");
    if !smoke.status.success() {
        panic!("{}", local_webrtc_smoke_failure_evidence(&smoke, &data_dir));
    }
    let smoke_text = command_output_text(&smoke);
    assert!(smoke_text.contains(&format!("data_dir=resolved:{}", data_dir.display())));
    assert!(
        smoke_text.contains("check name=daemon status=pass message=daemon reused"),
        "smoke must reuse the daemon started by up: {smoke_text}"
    );
    assert!(smoke_text.contains("smoke_result=pass"));

    let status_after_smoke = run_daily("status", &[]);
    assert!(
        status_after_smoke.status.success(),
        "reused smoke must leave the default daemon running: {}",
        command_output_text(&status_after_smoke)
    );

    let down = run_daily("down", &[]);
    assert!(
        down.status.success(),
        "default down failed: {}",
        command_output_text(&down)
    );
    assert!(command_output_text(&down).contains("response=shutdown"));

    let stopped = run_daily("status", &[]);
    assert!(
        !stopped.status.success(),
        "default status should fail after down: {}",
        command_output_text(&stopped)
    );
    assert!(
        !xdg.join("botster-hub").exists(),
        "XDG_DATA_HOME must not select or create Hub state"
    );
    assert!(
        !checkout.join("target/botster-hub-runtime-data").exists(),
        "cwd-relative legacy default must not be recreated"
    );
    assert!(
        !other_checkout
            .join("target/botster-hub-runtime-data")
            .exists(),
        "second cwd must not receive legacy runtime state"
    );
    for sibling in [
        "plugins",
        "agents",
        "lua",
        "profiles",
        "shared",
        "workspaces",
    ] {
        assert!(
            home.join(".botster")
                .join(sibling)
                .join("sentinel")
                .exists(),
            "protected Botster sibling {sibling} was mutated"
        );
    }
}

#[test]
fn cli_doctor_reports_healthy_runtime_checks() {
    let _guard = daemon_test_guard();
    let data_dir = unique_short_test_dir("cli-doctor-healthy");
    let project_pipelines_package_dir = unique_test_dir("cli-doctor-project-pipelines");
    let web_package_dir = unique_test_dir("cli-doctor-web");
    let tui_package_dir = unique_test_dir("cli-doctor-tui");
    let workspaces_package_dir = unique_test_dir("cli-doctor-workspaces");
    write_project_pipelines_availability_package(&project_pipelines_package_dir);
    write_botster_web_package(&web_package_dir);
    write_botster_tui_package(&tui_package_dir);
    write_botster_workspaces_local_package(&workspaces_package_dir, "botster-workspaces");

    let up = run_local_runtime_up(
        &data_dir,
        &project_pipelines_package_dir,
        &web_package_dir,
        &tui_package_dir,
        &workspaces_package_dir,
        unused_loopback_port(),
    );
    assert!(
        up.status.success(),
        "up failed: {}",
        command_output_text(&up)
    );

    let doctor = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("doctor")
        .arg("--data-dir")
        .arg(&data_dir)
        .output()
        .expect("run botster-hub doctor against healthy runtime");
    assert!(
        doctor.status.success(),
        "doctor failed: {}",
        command_output_text(&doctor)
    );
    let text = command_output_text(&doctor);
    assert!(text.contains(&format!("data_dir=resolved:{}", data_dir.display())));
    assert!(text.contains("check name=daemon_running status=pass"));
    assert!(text.contains("check name=daemon_compatible status=pass"));
    assert!(text.contains("conformance_fixture_revision="));
    assert!(text.contains("check name=core_initialized status=pass"));
    assert!(text.contains("check name=package_registry status=pass"));
    assert!(text.contains("check name=botster_web_app status=pass"));
    for package_dir in [
        &project_pipelines_package_dir,
        &web_package_dir,
        &tui_package_dir,
        &workspaces_package_dir,
    ] {
        assert!(
            !text.contains(package_dir.to_string_lossy().as_ref()),
            "doctor output should not leak package source path {package_dir:?}: {text}"
        );
    }

    shutdown_local_runtime_daemon(&data_dir);
}

#[test]
fn cli_home_runtime_up_recovers_owned_incompatible_daemon() {
    let _guard = daemon_test_guard();
    let home = unique_short_test_dir("cli-home-owned-incompat");
    let data_dir = home.join(".botster/hub");
    let project_pipelines_package_dir = unique_test_dir("cli-up-owned-project-pipelines");
    let web_package_dir = unique_test_dir("cli-up-owned-web");
    let tui_package_dir = unique_test_dir("cli-up-owned-tui");
    let workspaces_package_dir = unique_test_dir("cli-up-owned-workspaces");
    write_project_pipelines_availability_package(&project_pipelines_package_dir);
    write_botster_web_package(&web_package_dir);
    write_botster_tui_package(&tui_package_dir);
    write_botster_workspaces_local_package(&workspaces_package_dir, "botster-workspaces");
    ensure_runtime_packages(&data_dir, &web_package_dir, &tui_package_dir);
    let mut stale_child = start_owned_incompatible_local_runtime_daemon(&data_dir);
    let stale_pid = stale_child.id();

    let output = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .env("HOME", &home)
        .env_remove("BOTSTER_HUB_DATA_DIR")
        .env_remove("XDG_DATA_HOME")
        .arg("up")
        .arg("--session-worker-bin")
        .arg(session_worker_binary_path())
        .output()
        .expect("run bare up after incompatible daemon");
    assert!(
        output.status.success(),
        "up failed after stale daemon recovery: {}",
        command_output_text(&output)
    );
    let text = command_output_text(&output);
    assert!(text.contains("runtime=ready"));
    assert!(text.contains("daemon=started"));
    let web_origin = text
        .lines()
        .find_map(|line| line.strip_prefix("web="))
        .expect("runtime output includes web URL")
        .trim_end_matches('/')
        .to_string();
    let health = read_json_health(&web_origin);
    assert_eq!(
        health["ok"], true,
        "replacement Web package server health: {health}"
    );
    assert_eq!(health["daemonReady"], true);
    let status = botster_hub::daemon_transport_request(
        &explicit_config(&data_dir),
        botster_hub::DaemonRequest::Status,
    )
    .expect("replacement daemon answers status");
    assert_eq!(status.kind, botster_hub_client::DaemonResponseKind::Status);
    assert_eq!(
        status.status.expect("runtime status body").lifecycle_state,
        "running"
    );
    let _ = stale_child.wait().expect("reap stale daemon");
    assert!(
        !process_exists(stale_pid),
        "stale incompatible daemon should be stopped"
    );
    assert!(
        explicit_config(&data_dir)
            .transports
            .local_socket
            .as_ref()
            .expect("replacement socket binding")
            .path
            .exists(),
        "replacement socket should remain after stale child exit"
    );

    shutdown_local_runtime_daemon(&data_dir);
}

#[test]
fn cli_home_runtime_start_does_not_reuse_dead_pid_metadata_and_rebinds_leftover_socket() {
    let _guard = daemon_test_guard();
    let home = unique_short_test_dir("cli-home-dead-metadata");
    let data_dir = home.join(".botster/hub");
    fs::create_dir_all(&data_dir).expect("create home runtime data directory");
    let socket_path = explicit_config(&data_dir)
        .transports
        .local_socket
        .as_ref()
        .expect("home runtime socket binding")
        .path
        .clone();
    let stale_listener = UnixListener::bind(&socket_path).expect("bind leftover socket fixture");
    drop(stale_listener);

    let mut exited = Command::new("true")
        .spawn()
        .expect("spawn dead pid fixture");
    let dead_pid = exited.id();
    assert!(exited.wait().expect("wait for dead pid fixture").success());
    assert!(!process_exists(dead_pid), "fixture pid must be dead");
    write_local_runtime_daemon_metadata(&data_dir, dead_pid);

    ensure_session_worker_binary();
    let mut daemon = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .env("HOME", &home)
        .env_remove("BOTSTER_HUB_DATA_DIR")
        .env_remove("XDG_DATA_HOME")
        .arg("start")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("start home runtime with stale dead-pid metadata");
    wait_for_status(&data_dir, &mut daemon);

    let stale_metadata: serde_json::Value = serde_json::from_slice(
        &fs::read(data_dir.join(".botster-hub-runtime-daemon.json"))
            .expect("read stale daemon metadata"),
    )
    .expect("parse stale daemon metadata");
    assert_eq!(stale_metadata["pid"].as_u64(), Some(dead_pid as u64));
    assert_ne!(daemon.id(), dead_pid, "start must not reuse the dead pid");
    assert!(
        process_exists(daemon.id()),
        "replacement daemon must remain alive after readiness"
    );
    assert!(
        socket_path.exists(),
        "replacement daemon must own the canonical home socket"
    );

    let shutdown = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .env("HOME", &home)
        .env_remove("BOTSTER_HUB_DATA_DIR")
        .env_remove("XDG_DATA_HOME")
        .arg("shutdown")
        .output()
        .expect("shut down replacement home runtime");
    assert!(
        shutdown.status.success(),
        "replacement shutdown failed: {}",
        command_output_text(&shutdown)
    );
    assert!(
        daemon.wait().expect("reap replacement daemon").success(),
        "replacement daemon should exit cleanly"
    );
}

#[test]
fn cli_local_runtime_down_recovers_owned_incompatible_daemon() {
    let _guard = daemon_test_guard();
    let data_dir = unique_short_test_dir("cli-down-owned-incompat");
    let stale_child = start_owned_incompatible_local_runtime_daemon(&data_dir);
    let stale_pid = stale_child.id();
    let socket_path = explicit_config(&data_dir)
        .transports
        .local_socket
        .as_ref()
        .expect("local socket binding")
        .path
        .clone();

    let output = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("down")
        .arg("--data-dir")
        .arg(&data_dir)
        .output()
        .expect("run botster-hub down against owned incompatible daemon");
    assert!(
        output.status.success(),
        "down failed after stale daemon recovery: {}",
        command_output_text(&output)
    );
    assert!(command_output_text(&output).contains("daemon=recovered_stale"));
    let _ = stale_child.wait_with_output().expect("reap stale daemon");
    assert!(
        !process_exists(stale_pid),
        "stale incompatible daemon should be stopped"
    );
    assert!(
        !socket_path.exists(),
        "down recovery should remove the selected data dir socket"
    );
}

#[test]
fn cli_local_runtime_recovery_removes_only_selected_data_dir_socket() {
    let _guard = daemon_test_guard();
    let data_dir = unique_short_test_dir("cli-scoped-owned-incompat");
    let other_data_dir = unique_short_test_dir("cli-scoped-other-incompat");
    let stale_child = start_owned_incompatible_local_runtime_daemon(&data_dir);
    let selected_socket_path = explicit_config(&data_dir)
        .transports
        .local_socket
        .as_ref()
        .expect("selected local socket binding")
        .path
        .clone();
    let other_socket_path = explicit_config(&other_data_dir)
        .transports
        .local_socket
        .as_ref()
        .expect("other local socket binding")
        .path
        .clone();
    fs::create_dir_all(other_socket_path.parent().expect("other socket parent"))
        .expect("create other socket parent");
    let _other_listener = UnixListener::bind(&other_socket_path).expect("bind other socket");

    let output = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("down")
        .arg("--data-dir")
        .arg(&data_dir)
        .output()
        .expect("run botster-hub down for selected data dir");
    assert!(
        output.status.success(),
        "down failed after stale daemon recovery: {}",
        command_output_text(&output)
    );
    let _ = stale_child.wait_with_output().expect("reap stale daemon");
    assert!(
        !selected_socket_path.exists(),
        "selected data dir socket should be removed"
    );
    assert!(
        other_socket_path.exists(),
        "recovery must not remove sockets for other data dirs"
    );
    let _ = fs::remove_file(other_socket_path);
}

#[test]
fn cli_local_runtime_up_refuses_unowned_incompatible_daemon() {
    let _guard = daemon_test_guard();
    let data_dir = unique_short_test_dir("cli-up-incompat");
    let config = explicit_config(&data_dir);
    let socket_path = config
        .transports
        .local_socket
        .as_ref()
        .expect("local socket binding")
        .path
        .clone();
    fs::create_dir_all(socket_path.parent().expect("socket parent")).expect("create socket parent");
    let listener = UnixListener::bind(&socket_path).expect("bind fake incompatible daemon");
    let (ready_tx, ready_rx) = mpsc::channel();
    let handle = thread::spawn(move || {
        ready_tx.send(()).expect("send listener ready");
        for _ in 0..2 {
            let Ok((mut stream, _addr)) = listener.accept() else {
                break;
            };
            let mut reader = BufReader::new(stream.try_clone().expect("clone fake stream"));
            let mut hello = String::new();
            let _ = reader.read_line(&mut hello);
            let _ = stream.write_all(b"{\"protocol\":\"botster-hub-daemon-v1\"}\n");
        }
    });
    ready_rx.recv().expect("fake listener ready");

    let output = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("up")
        .arg("--data-dir")
        .arg(&data_dir)
        .output()
        .expect("run botster-hub up against incompatible daemon");
    assert!(
        !output.status.success(),
        "up unexpectedly succeeded: {}",
        command_output_text(&output)
    );
    let text = command_output_text(&output);
    assert!(text.contains("running daemon is incompatible or stale"));
    assert!(text.contains("botster-hub down"));
    assert!(text.contains("may fail against this daemon"));
    assert!(text.contains("Stop the running botster-hub process directly"));
    assert!(text.contains("remove the stale local socket"));
    assert!(text.contains("botster-hub up [--data-dir <path>]"));
    assert!(
        socket_path.exists(),
        "up must not delete a connectable socket on compatibility failure"
    );

    let down = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("down")
        .arg("--data-dir")
        .arg(&data_dir)
        .output()
        .expect("run botster-hub down against incompatible daemon");
    assert!(
        !down.status.success(),
        "down unexpectedly succeeded: {}",
        command_output_text(&down)
    );
    let down_text = command_output_text(&down);
    assert!(down_text.contains("running daemon is incompatible or stale"));
    assert!(down_text.contains("Stop the running botster-hub process directly"));
    assert!(down_text.contains("remove the stale local socket"));

    handle.join().expect("fake incompatible daemon thread");
    let _ = fs::remove_file(socket_path);
}

#[test]
fn cli_local_runtime_refuses_forged_metadata_for_live_non_botster_pid() {
    let _guard = daemon_test_guard();
    let home = unique_short_test_dir("cli-home-forged-pid-incompat");
    let data_dir = home.join(".botster/hub");
    let config = explicit_config(&data_dir);
    let socket_path = config
        .transports
        .local_socket
        .as_ref()
        .expect("local socket binding")
        .path
        .clone();
    fs::create_dir_all(socket_path.parent().expect("socket parent")).expect("create socket parent");
    let listener = UnixListener::bind(&socket_path).expect("bind fake incompatible daemon");
    let (ready_tx, ready_rx) = mpsc::channel();
    let handle = thread::spawn(move || {
        ready_tx.send(()).expect("send listener ready");
        for _ in 0..2 {
            let Ok((mut stream, _addr)) = listener.accept() else {
                break;
            };
            let mut reader = BufReader::new(stream.try_clone().expect("clone fake stream"));
            let mut hello = String::new();
            let _ = reader.read_line(&mut hello);
            let _ = stream.write_all(b"{\"protocol\":\"botster-hub-daemon-v1\"}\n");
        }
    });
    ready_rx.recv().expect("fake listener ready");

    let mut decoy = ChildCleanup::spawn_non_botster_decoy();
    write_local_runtime_daemon_metadata(&data_dir, decoy.id());

    let output = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .env("HOME", &home)
        .env_remove("BOTSTER_HUB_DATA_DIR")
        .env_remove("XDG_DATA_HOME")
        .arg("up")
        .output()
        .expect("run botster-hub up against forged daemon metadata");
    assert!(
        !output.status.success(),
        "up unexpectedly recovered forged metadata: {}",
        command_output_text(&output)
    );
    let text = command_output_text(&output);
    assert!(text.contains("running daemon is incompatible or stale"));
    assert!(text.contains("Stop the running botster-hub process directly"));
    decoy.assert_alive();
    assert!(
        socket_path.exists(),
        "up must not delete a connectable socket when metadata pid is not botster-owned"
    );

    let down = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .env("HOME", &home)
        .env_remove("BOTSTER_HUB_DATA_DIR")
        .env_remove("XDG_DATA_HOME")
        .arg("down")
        .output()
        .expect("run botster-hub down against forged daemon metadata");
    assert!(
        !down.status.success(),
        "down unexpectedly recovered forged metadata: {}",
        command_output_text(&down)
    );
    let down_text = command_output_text(&down);
    assert!(down_text.contains("running daemon is incompatible or stale"));
    assert!(down_text.contains("Stop the running botster-hub process directly"));
    decoy.assert_alive();
    assert!(
        socket_path.exists(),
        "down must not delete a connectable socket when metadata pid is not botster-owned"
    );

    handle.join().expect("fake incompatible daemon thread");
    let _ = fs::remove_file(socket_path);
}

#[test]
fn cli_doctor_reports_incompatible_stale_daemon_without_deleting_socket() {
    let _guard = daemon_test_guard();
    let data_dir = unique_short_test_dir("cli-doctor-incompat");
    let config = explicit_config(&data_dir);
    let socket_path = config
        .transports
        .local_socket
        .as_ref()
        .expect("local socket binding")
        .path
        .clone();
    fs::create_dir_all(socket_path.parent().expect("socket parent")).expect("create socket parent");
    let listener = UnixListener::bind(&socket_path).expect("bind fake incompatible daemon");
    let (ready_tx, ready_rx) = mpsc::channel();
    let handle = thread::spawn(move || {
        ready_tx.send(()).expect("send listener ready");
        let Ok((mut stream, _addr)) = listener.accept() else {
            return;
        };
        let mut reader = BufReader::new(stream.try_clone().expect("clone fake stream"));
        let mut hello = String::new();
        let _ = reader.read_line(&mut hello);
        let _ = stream.write_all(b"{\"protocol\":\"botster-hub-daemon-v1\"}\n");
    });
    ready_rx.recv().expect("fake listener ready");

    let output = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("doctor")
        .arg("--data-dir")
        .arg(&data_dir)
        .output()
        .expect("run botster-hub doctor against incompatible daemon");
    assert!(
        !output.status.success(),
        "doctor unexpectedly succeeded: {}",
        command_output_text(&output)
    );
    let text = command_output_text(&output);
    assert!(text.contains("check name=daemon_compatible status=fail"));
    assert!(text.contains("running daemon is incompatible or stale"));
    assert!(text.contains("stop the stale botster-hub process"));
    assert!(
        socket_path.exists(),
        "doctor must not delete a connectable socket on compatibility failure"
    );

    handle.join().expect("fake incompatible daemon thread");
    let _ = fs::remove_file(socket_path);
}

#[test]
fn cli_smoke_proves_local_runtime_daemon_package_app_session_and_webrtc() {
    let _guard = daemon_test_guard();
    let data_dir = unique_short_test_dir("cli-smoke-success");
    let project_pipelines_package_dir = unique_test_dir("cli-smoke-project-pipelines");
    let web_package_dir = unique_test_dir("cli-smoke-web");
    let tui_package_dir = unique_test_dir("cli-smoke-tui");
    let workspaces_package_dir = unique_test_dir("cli-smoke-workspaces");
    write_project_pipelines_availability_package(&project_pipelines_package_dir);
    write_botster_web_package(&web_package_dir);
    write_botster_tui_package(&tui_package_dir);
    write_botster_workspaces_local_package(&workspaces_package_dir, "botster-workspaces");

    let output = run_local_runtime_smoke(
        &data_dir,
        &project_pipelines_package_dir,
        &web_package_dir,
        &tui_package_dir,
        &workspaces_package_dir,
        unused_loopback_port(),
    );
    let text = command_output_text(&output);
    assert_smoke_owned_daemon_gone(&data_dir);
    if !output.status.success() {
        panic!(
            "{}",
            local_webrtc_smoke_failure_evidence(&output, &data_dir)
        );
    }
    assert!(text.contains("smoke=local_runtime"));
    assert!(text.contains(&format!("data_dir=resolved:{}", data_dir.display())));
    assert!(text.contains("check name=daemon status=pass"));
    assert!(text.contains("check name=core status=pass"));
    assert!(text.contains("check name=packages status=pass"));
    assert!(text.contains("check name=apps status=pass"));
    assert!(text.contains("check name=session_terminal status=pass"));
    assert!(text.contains("check name=webrtc status=pass"));
    assert!(text.contains("smoke_result=pass"));
}

#[test]
fn cli_smoke_persists_matching_sender_record_when_webrtc_response_closes() {
    let _guard = daemon_test_guard();
    let data_dir = unique_short_test_dir("cli-smoke-webrtc-close");
    let project_pipelines_package_dir = unique_test_dir("cli-smoke-close-project-pipelines");
    let web_package_dir = unique_test_dir("cli-smoke-close-web");
    let tui_package_dir = unique_test_dir("cli-smoke-close-tui");
    let workspaces_package_dir = unique_test_dir("cli-smoke-close-workspaces");
    write_project_pipelines_availability_package(&project_pipelines_package_dir);
    write_botster_web_package(&web_package_dir);
    write_botster_tui_package(&tui_package_dir);
    write_botster_workspaces_local_package(&workspaces_package_dir, "botster-workspaces");

    let output = run_local_runtime_smoke_with_fault(
        &data_dir,
        &project_pipelines_package_dir,
        &web_package_dir,
        &tui_package_dir,
        &workspaces_package_dir,
        unused_loopback_port(),
        Some("status"),
    );
    let text = command_output_text(&output);
    assert!(
        !output.status.success(),
        "faulted smoke unexpectedly passed: {text}"
    );
    assert!(text.contains(
        "local_webrtc=local WebRTC response incomplete: operation=status cause=channel_closed message_id=pending next_chunk=0 expected_chunks=pending"
    ));
    let grant_id =
        local_webrtc_grant_id(&output).expect("faulted smoke reached local WebRTC bootstrap");
    let terminal_record = local_webrtc_sender_terminal_record(&data_dir, &grant_id);
    assert_eq!(terminal_record["request_operation"], "status");
    assert_eq!(terminal_record["next_chunk_index"], 0);
    assert_eq!(terminal_record["total_chunks"], 0);
    assert!(
        matches!(
            terminal_record["cause"].as_str(),
            Some(
                "channel_closed"
                    | "poll_ended"
                    | "peer_disconnected"
                    | "peer_failed"
                    | "peer_closed"
            )
        ),
        "faulted smoke must retain a usable sender terminal cause: {terminal_record}"
    );
    assert_smoke_owned_daemon_gone(&data_dir);
}

#[test]
fn fast_exit_attach_diagnostic_records_subscription_event_order() {
    let _guard = daemon_test_guard();
    let data_dir = unique_test_dir("fast-exit-attach-diagnostic");
    let config = explicit_config(&data_dir);
    let socket_path = config
        .transports
        .local_socket
        .as_ref()
        .expect("test config has local socket")
        .path
        .clone();
    let endpoint = botster_hub_client::DaemonEndpoint::new(socket_path);
    let child = start_cli_daemon(&data_dir);
    let session_id = format!(
        "smoke-{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time after epoch")
            .as_nanos()
    );
    let subscription_id = format!("{session_id}-subscription");
    let marker = "botster-smoke-terminal-ok";
    let expected = format!("smoke:{marker}");

    let spawn = botster_hub_client::request(
        &endpoint,
        botster_hub_client::DaemonRequest::Spawn {
            session_id: session_id.clone(),
            command: format!("printf 'smoke:{marker}\\n'"),
        },
    )
    .expect("spawn immediate-output diagnostic session");
    assert_eq!(spawn.kind, botster_hub_client::DaemonResponseKind::Spawned);

    // Mirrors stream_attach_connected in crates/botster-hub-client/src/lib.rs:123-172.
    // Any production boundary change there must update this diagnostic mirror.
    let mut connection = botster_hub_client::DaemonConnection::connect(&endpoint)
        .expect("connect diagnostic client");
    let mut response = connection
        .request(&botster_hub_client::DaemonRequest::Attach {
            session_id: session_id.clone(),
            subscription_id: subscription_id.clone(),
        })
        .expect("attach diagnostic subscription");
    let started_at = Instant::now();
    let mut observed = String::new();
    let mut matching_observed = String::new();
    let mut mismatched_marker = false;
    let mut opaque_history_bytes = 0;
    let mut saw_process_exit = false;
    let mut ordered_observations = Vec::new();
    let mut response_index = 0;
    let mut request_kind = "attach";
    let mut idle_drains = 0;

    let boundary_reason = loop {
        let mut response_observations = Vec::new();
        let mut response_renderable_bytes = 0;
        for event in &response.events {
            let observation = match event {
                botster_hub_client::DaemonEvent::SessionLifecycle {
                    session_id: event_session_id,
                    state,
                } => format!("session_lifecycle:session={event_session_id}:state={state}"),
                botster_hub_client::DaemonEvent::TerminalOutput {
                    session_id: event_session_id,
                    subscription_id: event_subscription_id,
                    data,
                } => {
                    response_renderable_bytes += data.len();
                    observed.push_str(data);
                    if event_session_id == &session_id && event_subscription_id == &subscription_id
                    {
                        matching_observed.push_str(data);
                    } else if data.contains(&expected) {
                        mismatched_marker = true;
                    }
                    format!(
                        "terminal_output:session={event_session_id}:subscription={event_subscription_id}:bytes={}",
                        data.len()
                    )
                }
                botster_hub_client::DaemonEvent::Snapshot {
                    session_id: event_session_id,
                    subscription_id: event_subscription_id,
                    history,
                } => {
                    opaque_history_bytes += history.bytes;
                    format!(
                        "snapshot:session={event_session_id}:subscription={event_subscription_id}:bytes={}",
                        history.bytes
                    )
                }
                botster_hub_client::DaemonEvent::Scrollback {
                    session_id: event_session_id,
                    subscription_id: event_subscription_id,
                    history,
                } => {
                    opaque_history_bytes += history.bytes;
                    format!(
                        "scrollback:session={event_session_id}:subscription={event_subscription_id}:bytes={}",
                        history.bytes
                    )
                }
                botster_hub_client::DaemonEvent::ProcessExit {
                    session_id: event_session_id,
                    subscription_id: event_subscription_id,
                    code,
                } => {
                    saw_process_exit = true;
                    format!(
                        "process_exit:session={event_session_id}:subscription={event_subscription_id}:code={code:?}"
                    )
                }
                botster_hub_client::DaemonEvent::AttachState {
                    session_id: event_session_id,
                    subscription_id: event_subscription_id,
                    state,
                } => {
                    format!(
                        "attach_state:session={event_session_id}:subscription={event_subscription_id}:state={state}"
                    )
                }
                botster_hub_client::DaemonEvent::RuntimeObservation { kind } => {
                    format!("runtime_observation:{kind}")
                }
                botster_hub_client::DaemonEvent::WorktreeLifecycle { .. } => {
                    "worktree_lifecycle".to_string()
                }
            };
            response_observations.push(observation.clone());
            ordered_observations.push(format!(
                "elapsed_us={}:response={response_index}:request={request_kind}:event={}:{}",
                started_at.elapsed().as_micros(),
                response_observations.len() - 1,
                observation
            ));
        }
        println!(
            "fast_exit_attach_diagnostic elapsed_us={} response={response_index} request={request_kind} events=[{}] renderable_bytes={response_renderable_bytes} cumulative_renderable_bytes={}",
            started_at.elapsed().as_micros(),
            response_observations.join(","),
            observed.len()
        );

        if saw_process_exit {
            break "process_exit";
        }
        if request_kind == "drain" {
            if response.events.is_empty() {
                idle_drains += 1;
            } else {
                idle_drains = 0;
            }
            if idle_drains >= 20 {
                break "idle_quiescence";
            }
        }

        thread::sleep(Duration::from_millis(25));
        response = connection
            .request(&botster_hub_client::DaemonRequest::Drain {
                session_id: session_id.clone(),
            })
            .expect("drain diagnostic subscription before production boundary");
        response_index += 1;
        request_kind = "drain";
    };

    let renderable_bytes_at_boundary = observed.len();
    let matching_bytes_at_boundary = matching_observed.len();
    let marker_at_boundary = observed.contains(&expected);
    println!(
        "fast_exit_attach_diagnostic boundary elapsed_us={} reason={boundary_reason} response={response_index} input_bytes=0 renderable_bytes_at_boundary={renderable_bytes_at_boundary} matching_bytes_at_boundary={matching_bytes_at_boundary} marker_at_boundary={marker_at_boundary} idle_drains={idle_drains}",
        started_at.elapsed().as_micros()
    );

    let mut tail_matching_observed = String::new();
    let mut tail_error = None;
    if !marker_at_boundary {
        let mut tail_idle_drains = 0;
        while tail_idle_drains < 20 {
            thread::sleep(Duration::from_millis(25));
            let tail_response =
                match connection.request(&botster_hub_client::DaemonRequest::Drain {
                    session_id: session_id.clone(),
                }) {
                    Ok(response) => response,
                    Err(error) => {
                        tail_error = Some(error.to_string());
                        break;
                    }
                };
            response_index += 1;
            if tail_response.events.is_empty() {
                tail_idle_drains += 1;
            } else {
                tail_idle_drains = 0;
            }
            for (event_index, event) in tail_response.events.iter().enumerate() {
                let elapsed_us = started_at.elapsed().as_micros();
                match event {
                    botster_hub_client::DaemonEvent::TerminalOutput {
                        session_id: event_session_id,
                        subscription_id: event_subscription_id,
                        data,
                    } => {
                        if event_session_id == &session_id
                            && event_subscription_id == &subscription_id
                        {
                            tail_matching_observed.push_str(data);
                        } else if data.contains(&expected) {
                            mismatched_marker = true;
                        }
                        println!(
                            "fast_exit_attach_tail_event elapsed_us={elapsed_us} response={response_index} event={event_index} type=terminal_output session={event_session_id} subscription={event_subscription_id} bytes={}",
                            data.len()
                        );
                    }
                    botster_hub_client::DaemonEvent::Snapshot {
                        session_id: event_session_id,
                        subscription_id: event_subscription_id,
                        history,
                    } => {
                        opaque_history_bytes += history.bytes;
                        println!(
                            "fast_exit_attach_tail_event elapsed_us={elapsed_us} response={response_index} event={event_index} type=snapshot session={event_session_id} subscription={event_subscription_id} bytes={}",
                            history.bytes
                        );
                    }
                    botster_hub_client::DaemonEvent::Scrollback {
                        session_id: event_session_id,
                        subscription_id: event_subscription_id,
                        history,
                    } => {
                        opaque_history_bytes += history.bytes;
                        println!(
                            "fast_exit_attach_tail_event elapsed_us={elapsed_us} response={response_index} event={event_index} type=scrollback session={event_session_id} subscription={event_subscription_id} bytes={}",
                            history.bytes
                        );
                    }
                    botster_hub_client::DaemonEvent::ProcessExit {
                        session_id: event_session_id,
                        subscription_id: event_subscription_id,
                        code,
                    } => println!(
                        "fast_exit_attach_tail_event elapsed_us={elapsed_us} response={response_index} event={event_index} type=process_exit session={event_session_id} subscription={event_subscription_id} code={code:?} bytes=0"
                    ),
                    botster_hub_client::DaemonEvent::AttachState {
                        session_id: event_session_id,
                        subscription_id: event_subscription_id,
                        state,
                    } => println!(
                        "fast_exit_attach_tail_event elapsed_us={elapsed_us} response={response_index} event={event_index} type=attach_state session={event_session_id} subscription={event_subscription_id} state={state} bytes=0"
                    ),
                    botster_hub_client::DaemonEvent::SessionLifecycle {
                        session_id: event_session_id,
                        state,
                    } => println!(
                        "fast_exit_attach_tail_event elapsed_us={elapsed_us} response={response_index} event={event_index} type=session_lifecycle session={event_session_id} subscription=none state={state} bytes=0"
                    ),
                    botster_hub_client::DaemonEvent::RuntimeObservation { kind } => println!(
                        "fast_exit_attach_tail_event elapsed_us={elapsed_us} response={response_index} event={event_index} type=runtime_observation session=none subscription=none kind={kind} bytes=0"
                    ),
                    botster_hub_client::DaemonEvent::WorktreeLifecycle { .. } => println!(
                        "fast_exit_attach_tail_event elapsed_us={elapsed_us} response={response_index} event={event_index} type=worktree_lifecycle session=none subscription=none bytes=0"
                    ),
                }
            }
        }
    }

    let (read_screen_bytes, read_screen_marker, read_screen_error) = if marker_at_boundary {
        (0, false, None)
    } else {
        match connection.request(&botster_hub_client::DaemonRequest::ReadScreen {
            session_id: session_id.clone(),
        }) {
            Ok(response) => match response.read_screen {
                Some(screen) => (screen.text.len(), screen.text.contains(&expected), None),
                None => (0, false, Some("missing_read_screen_body".to_string())),
            },
            Err(error) => (0, false, Some(error.to_string())),
        }
    };

    let status = connection.request(&botster_hub_client::DaemonRequest::Status);
    let daemon_lifecycle = status
        .as_ref()
        .ok()
        .and_then(|response| response.status.as_ref())
        .map(|status| status.lifecycle_state.as_str())
        .unwrap_or("missing");

    let sessions = connection
        .request(&botster_hub_client::DaemonRequest::ListSessions)
        .expect("list sessions after frozen production boundary");
    let session_lifecycle = sessions
        .sessions
        .iter()
        .find(|session| session.session_id == session_id)
        .map(|session| session.lifecycle.as_str())
        .unwrap_or("missing");
    let tail_matching_marker = tail_matching_observed.contains(&expected);
    println!(
        "fast_exit_attach_diagnostic state elapsed_us={} daemon_lifecycle={daemon_lifecycle} session_lifecycle={session_lifecycle} process_exit={saw_process_exit} renderable_bytes_at_boundary={renderable_bytes_at_boundary} matching_bytes_at_boundary={matching_bytes_at_boundary} tail_matching_bytes={} tail_matching_marker={tail_matching_marker} mismatched_marker={mismatched_marker} opaque_history_bytes={opaque_history_bytes} read_screen_bytes={read_screen_bytes} read_screen_marker={read_screen_marker} read_screen_error={read_screen_error:?} tail_error={tail_error:?} event_order=[{}]",
        started_at.elapsed().as_micros(),
        tail_matching_observed.len(),
        ordered_observations.join(",")
    );

    let detach = connection.request(&botster_hub_client::DaemonRequest::Detach {
        session_id: session_id.clone(),
        subscription_id: subscription_id.clone(),
    });
    println!(
        "fast_exit_attach_diagnostic cleanup elapsed_us={} detach_response={detach:?}",
        started_at.elapsed().as_micros()
    );
    let _ = connection.request(&botster_hub_client::DaemonRequest::ShutdownSession {
        session_id: session_id.clone(),
    });

    let shutdown = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("shutdown")
        .arg("--data-dir")
        .arg(&data_dir)
        .output()
        .expect("run botster-hub shutdown after fast-exit diagnostic");
    let daemon = child
        .wait_with_output()
        .expect("wait for diagnostic daemon child");
    let shutdown_validation = validate_cli_daemon_shutdown(&shutdown, &daemon);

    if !marker_at_boundary {
        let classification = if mismatched_marker && tail_matching_marker {
            "ambiguous_subscription_mismatch_and_output_queued_after_harness_stop"
        } else if mismatched_marker {
            "subscription_mismatch"
        } else if tail_matching_marker {
            "output_queued_after_harness_stop"
        } else if read_screen_marker {
            "output_produced_not_routed"
        } else if read_screen_error.is_some() {
            "retained_history_or_readback_failure"
        } else if tail_error.is_some() {
            "unclassified_diagnostic_transport_failure"
        } else {
            "output_never_produced"
        };
        println!(
            "fast_exit_attach_failure classification={classification} boundary_reason={boundary_reason} input_bytes=0 renderable_bytes_at_boundary={renderable_bytes_at_boundary} matching_bytes_at_boundary={matching_bytes_at_boundary} opaque_history_bytes={opaque_history_bytes} daemon_exit_status={} shutdown_status={} daemon_stdout_begin\n{}\ndaemon_stdout_end daemon_stderr_begin\n{}\ndaemon_stderr_end test_stdout_stderr=run_log",
            daemon.status,
            shutdown.status,
            String::from_utf8_lossy(&daemon.stdout),
            String::from_utf8_lossy(&daemon.stderr)
        );
    }

    assert!(
        shutdown_validation.is_ok(),
        "diagnostic daemon cleanup failed: {shutdown_validation:?}"
    );
    assert!(
        marker_at_boundary,
        "fast-exit marker missing at production boundary; renderable_bytes_at_boundary={renderable_bytes_at_boundary} matching_bytes_at_boundary={matching_bytes_at_boundary} tail_matching_marker={tail_matching_marker} mismatched_marker={mismatched_marker} read_screen_marker={read_screen_marker} read_screen_error={read_screen_error:?} tail_error={tail_error:?}"
    );
}

#[test]
fn cli_smoke_reports_missing_first_party_prerequisites() {
    let _guard = daemon_test_guard();
    let data_dir = unique_short_test_dir("cli-smoke-missing");

    let output = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("smoke")
        .arg("--data-dir")
        .arg(&data_dir)
        .output()
        .expect("run botster-hub smoke without package prerequisites");
    assert!(
        !output.status.success(),
        "smoke unexpectedly succeeded: {}",
        command_output_text(&output)
    );
    let text = command_output_text(&output);
    assert!(text.contains("smoke=local_runtime"));
    assert!(text.contains("missing_prerequisite=botster-web"));
    let failure = local_webrtc_smoke_failure_evidence(&output, &data_dir);
    assert!(failure.contains("smoke failed before local WebRTC bootstrap"));
    assert!(failure.contains("missing_prerequisite=botster-web"));
}

#[test]
fn daemon_starts_empty_state_reports_status_uses_core_and_stops_idempotently() {
    let config = explicit_config(unique_test_dir("empty"));
    let store = FileHubStateStore::for_data_directory(&config.data_directory);
    let mut daemon = HubDaemon::start(config.clone()).expect("start daemon from empty state");

    let status = daemon.status();
    assert_eq!(status.lifecycle_state, HubDaemonState::Running);
    assert_eq!(status.state_source, HubStateLoadSource::Initialized);
    assert_eq!(status.host_id, "hub-daemon-test");
    assert_eq!(status.host_display_name, "Hub Daemon Test");
    assert_eq!(status.schema_version, 2);
    assert!(status.data_dir_configured);
    assert!(status.core_initialized);
    assert_eq!(status.package_count, 0);
    assert_eq!(status.provider_count, 0);
    assert!(store.path().exists());

    let runtime = daemon.runtime_mut().expect("runtime initialized");
    let request = spawn_request(runtime.config());
    let session_id = request.session_id.clone();
    runtime
        .spawn_session(request, CoreSessionMetadata::new(), 1)
        .expect("spawn through core daemon runtime");
    assert_eq!(runtime.list_sessions().expect("daemon list").len(), 1);
    runtime
        .shutdown_session(session_id, 2)
        .expect("shutdown through core daemon runtime");

    let stopped = daemon.stop();
    assert_eq!(stopped.lifecycle_state, HubDaemonState::Stopped);
    assert!(!stopped.core_initialized);
    let stopped_again = daemon.stop();
    assert_eq!(stopped_again, stopped);

    let reopened = store
        .load_or_initialize(&config)
        .expect("reload committed daemon state");
    assert_eq!(reopened.schema_version, 2);
    assert_eq!(reopened.host.id, "hub-daemon-test");
}

#[test]
fn daemon_restart_preserves_split_plugin_worker_configuration() {
    let mut config = explicit_config(unique_test_dir("plugin-worker-config-restart"));
    config.core_engine.plugin_worker_queue_capacity = 9;
    config.core_engine.plugin_worker_executor_concurrency = 3;

    let mut daemon = HubDaemon::start(config.clone()).expect("start configured daemon");
    let initial = daemon
        .runtime()
        .expect("runtime initialized")
        .plugin_worker_debug_snapshot();
    assert_eq!(initial.configured_queue_capacity, 9);
    assert_eq!(initial.configured_executor_concurrency, 3);
    daemon.stop();

    let mut restarted = HubDaemon::start(config).expect("restart configured daemon");
    let reopened = restarted
        .runtime()
        .expect("runtime initialized")
        .plugin_worker_debug_snapshot();
    assert_eq!(reopened.configured_queue_capacity, 9);
    assert_eq!(reopened.configured_executor_concurrency, 3);
    restarted.stop();
}

#[test]
fn daemon_restart_reconnects_worker_backed_session_through_client_api() {
    let config = explicit_config(unique_test_dir("restart-reconnect"));
    let packages = empty_registry();
    let api = HubClientApi::local_operator("hub-daemon-restart-client");
    let session_id = SessionId("hub-daemon-restart-session".to_string());
    let subscription_id = SubscriptionId("hub-daemon-restart-subscription".to_string());
    let mut logical_clock = 10;

    let mut daemon = HubDaemon::start(config.clone()).expect("start first hub daemon");
    api.handle_request(
        daemon.runtime_mut().expect("runtime initialized"),
        &packages,
        HubClientRequest::Spawn {
            request_id: RequestId("hub-daemon-restart-spawn".to_string()),
            session_id: session_id.clone(),
            command: "printf 'restart-ready\\n'; while IFS= read -r line; do printf 'echo:%s\\n' \"$line\"; done".to_string(),
            now_seconds: logical_clock,
        },
    )
    .expect("spawn through hub client api");
    logical_clock += 1;
    api.handle_request(
        daemon.runtime_mut().expect("runtime initialized"),
        &packages,
        HubClientRequest::Attach {
            request_id: RequestId("hub-daemon-restart-attach".to_string()),
            session_id: session_id.clone(),
            subscription_id: subscription_id.clone(),
            now_seconds: logical_clock,
        },
    )
    .expect("attach before restart through client api");
    logical_clock += 1;
    drain_until_client_output(
        &api,
        daemon.runtime_mut().expect("runtime initialized"),
        &packages,
        &session_id,
        b"restart-ready",
        &mut logical_clock,
    );
    daemon.stop();

    let mut restarted = HubDaemon::start(config).expect("restart hub daemon");
    assert!(
        restarted
            .runtime()
            .expect("runtime initialized")
            .reconciliation()
            .recovered_sessions
            .contains(&session_id),
        "restart should recover the live worker-backed session"
    );
    let listed = api
        .handle_request(
            restarted.runtime_mut().expect("runtime initialized"),
            &packages,
            HubClientRequest::ListSessions {
                request_id: RequestId("hub-daemon-restart-list".to_string()),
            },
        )
        .expect("list after restart through client api");
    assert!(
        matches!(listed.body, HubClientResponseBody::Sessions(sessions) if sessions.iter().any(|session| session.session_id == session_id))
    );

    api.handle_request(
        restarted.runtime_mut().expect("runtime initialized"),
        &packages,
        HubClientRequest::Attach {
            request_id: RequestId("hub-daemon-restart-reattach".to_string()),
            session_id: session_id.clone(),
            subscription_id: subscription_id.clone(),
            now_seconds: logical_clock,
        },
    )
    .expect("reattach after restart through client api");
    logical_clock += 1;
    api.handle_request(
        restarted.runtime_mut().expect("runtime initialized"),
        &packages,
        HubClientRequest::Input {
            request_id: RequestId("hub-daemon-restart-input".to_string()),
            session_id: session_id.clone(),
            data: b"after-restart\n".to_vec(),
            now_seconds: logical_clock,
        },
    )
    .expect("input after restart through client api");
    logical_clock += 1;
    drain_until_client_output(
        &api,
        restarted.runtime_mut().expect("runtime initialized"),
        &packages,
        &session_id,
        b"echo:after-restart",
        &mut logical_clock,
    );
    api.handle_request(
        restarted.runtime_mut().expect("runtime initialized"),
        &packages,
        HubClientRequest::Shutdown {
            request_id: RequestId("hub-daemon-restart-shutdown".to_string()),
            session_id,
            now_seconds: logical_clock,
        },
    )
    .expect("shutdown after restart through client api");
}

#[test]
fn daemon_startup_reconciliation_marks_stale_and_recovers_missing_live_sessions() {
    let stale_config = explicit_config(unique_test_dir("stale-reconcile"));
    let stale_session_id = SessionId("hub-daemon-stale-session".to_string());
    let registry = SessionRegistry::new(stale_config.data_directory.clone());
    let mut stale_record = RegistryRecord::running(
        stale_session_id.clone(),
        Some(ProcessIdentity {
            pid: Some(42),
            runtime_id: Some("stale-runtime".to_string()),
        }),
        ResizePayload { rows: 24, cols: 80 },
        "sh".to_string(),
        1,
    );
    stale_record.observe_restart_contract(serde_json::json!({"session": "hub-daemon-stale"}), 2);
    registry
        .save(&stale_record)
        .expect("stale registry fixture should save");

    let stale_daemon = HubDaemon::start(stale_config).expect("start daemon with stale registry");
    assert!(
        stale_daemon
            .runtime()
            .expect("runtime initialized")
            .reconciliation()
            .stale_sessions
            .contains(&stale_session_id),
        "registry record without a live worker should become stale deterministically"
    );

    let recovered_config = explicit_config(unique_test_dir("recovered-reconcile"));
    let packages = empty_registry();
    let api = HubClientApi::local_operator("hub-daemon-recovered-client");
    let recovered_session_id = SessionId("hub-daemon-recovered-session".to_string());
    let mut first = HubDaemon::start(recovered_config.clone()).expect("start first daemon");
    api.handle_request(
        first.runtime_mut().expect("runtime initialized"),
        &packages,
        HubClientRequest::Spawn {
            request_id: RequestId("hub-daemon-recovered-spawn".to_string()),
            session_id: recovered_session_id.clone(),
            command: "printf 'recovered-ready\\n'; while IFS= read -r line; do printf 'echo:%s\\n' \"$line\"; done".to_string(),
            now_seconds: 1,
        },
    )
    .expect("spawn recovered session through client api");
    first.stop();

    let recovered =
        HubDaemon::start(recovered_config).expect("restart daemon with live core registry record");
    assert!(
        recovered
            .runtime()
            .expect("runtime initialized")
            .reconciliation()
            .recovered_sessions
            .contains(&recovered_session_id),
        "core-live worker-backed session absent from hub state should be recovered"
    );
}

#[test]
fn daemon_startup_reconciliation_marks_stale_adoption_socket_and_continues() {
    let (tx, rx) = mpsc::channel();
    thread::spawn(move || {
        let result = std::panic::catch_unwind(|| {
            let config = explicit_config(unique_test_dir("stale-adoption-socket"));
            let session_id = SessionId("hub-daemon-stale-adoption-socket".to_string());
            let stale_socket = PathBuf::from(format!(
                "/tmp/bh-stale-{}.sock",
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .expect("system time after epoch")
                    .as_nanos()
            ));
            let registry = SessionRegistry::new(config.data_directory.clone());
            let mut record = RegistryRecord::running(
                session_id.clone(),
                Some(ProcessIdentity {
                    pid: Some(42),
                    runtime_id: Some("stale-adoption-runtime".to_string()),
                }),
                ResizePayload { rows: 24, cols: 80 },
                "sh".to_string(),
                1,
            );
            record.observe_restart_contract(
                serde_json::json!({
                    "worker_control_socket": stale_socket,
                    "mode": "worker_process"
                }),
                2,
            );
            registry
                .save(&record)
                .expect("stale adoption registry fixture should save");

            let mut daemon =
                HubDaemon::start(config).expect("start daemon with stale worker control socket");
            let status = daemon.status();
            assert!(
                status.stale_sessions.contains(&session_id),
                "stale worker control socket should be surfaced in daemon status"
            );

            let packages = empty_registry();
            let api = HubClientApi::local_operator("hub-daemon-stale-adoption-client");
            let fresh_session_id = SessionId("hub-daemon-fresh-after-stale".to_string());
            api.handle_request(
                daemon.runtime_mut().expect("runtime initialized"),
                &packages,
                HubClientRequest::Spawn {
                    request_id: RequestId("hub-daemon-fresh-after-stale-spawn".to_string()),
                    session_id: fresh_session_id.clone(),
                    command: "printf 'fresh-after-stale-ready\\n'; sleep 1".to_string(),
                    now_seconds: 3,
                },
            )
            .expect("fresh session should spawn after stale adoption reconciliation");
            assert!(
                daemon
                    .runtime()
                    .expect("runtime initialized")
                    .list_sessions()
                    .expect("list sessions after fresh spawn")
                    .iter()
                    .any(|session| session.session_id == fresh_session_id),
                "fresh session should be visible after stale adoption reconciliation"
            );
        });
        let _ = tx.send(result);
    });

    match rx.recv_timeout(Duration::from_secs(15)) {
        Ok(Ok(())) => {}
        Ok(Err(payload)) => std::panic::resume_unwind(payload),
        Err(mpsc::RecvTimeoutError::Timeout) => {
            panic!("stale adoption socket startup reconciliation deadlocked")
        }
        Err(mpsc::RecvTimeoutError::Disconnected) => {
            panic!("stale adoption socket startup reconciliation worker exited unexpectedly")
        }
    }
}

#[test]
fn daemon_restores_existing_provider_policy_records_through_snapshot_admission() {
    let config = explicit_config(unique_test_dir("existing"));
    let store = FileHubStateStore::for_data_directory(&config.data_directory);
    let mut policy = PackageAdmissionPolicy::from_host_profile();
    policy
        .install(
            provider_manifest(),
            package_provenance(),
            "install provider policy record",
        )
        .expect("install provider");
    policy
        .enable("daemon.provider", "enable provider policy record")
        .expect("enable provider through admission");

    store
        .update(&config, |state| {
            state.package_registry = policy.registry().snapshot();
        })
        .expect("seed existing state through store");

    let mut daemon = HubDaemon::start(config.clone()).expect("start daemon from existing state");
    let status = daemon.status();

    assert_eq!(status.lifecycle_state, HubDaemonState::Running);
    assert_eq!(status.state_source, HubStateLoadSource::Loaded);
    assert!(status.core_initialized);
    assert_eq!(status.package_count, 1);
    assert_eq!(status.enabled_package_count, 1);
    assert_eq!(status.provider_count, 1);
    assert_eq!(status.enabled_provider_count, 1);
    assert_eq!(status.schema_version, 2);

    daemon.stop();
    let reopened = store
        .load_or_initialize(&config)
        .expect("reload existing state after stop");
    assert_eq!(reopened.package_registry.records.len(), 1);
    assert!(reopened.package_registry.records[0].is_enabled());
}

#[test]
fn cli_start_and_status_print_scrubbed_lifecycle_status() {
    let _guard = daemon_test_guard();
    let data_dir = unique_test_dir("cli-start");
    let child = start_cli_daemon(&data_dir);
    let output = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("status")
        .arg("--data-dir")
        .arg(&data_dir)
        .output()
        .expect("run botster-hub status");

    assert!(
        output.status.success(),
        "status failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).expect("stdout is utf8");
    assert!(stdout.contains("event=status"));
    assert!(stdout.contains("lifecycle_state=running"));
    assert!(stdout.contains("schema_version=2"));
    assert!(stdout.contains("core_initialized=true"));
    assert!(stdout.contains("state_source=initialized"));
    assert!(!stdout.contains(data_dir.to_string_lossy().as_ref()));
    assert!(!stdout.contains(concat!("/", "Users", "/")));
    assert!(!stdout.contains("/home/"));
    assert!(data_dir.join("hub-state.json").exists());

    let output = shutdown_cli_daemon(&data_dir, child);
    let stdout = String::from_utf8(output.stdout).expect("daemon stdout is utf8");
    assert!(stdout.contains("event=stopped"));
    assert!(stdout.contains("lifecycle_state=stopped"));
}

#[test]
fn cli_status_uses_daemon_status_path_without_local_paths() {
    let data_dir = unique_test_dir("cli-status");
    let output = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("status")
        .arg("--data-dir")
        .arg(&data_dir)
        .output()
        .expect("run botster-hub status");

    assert!(
        !output.status.success(),
        "status unexpectedly succeeded: {}",
        String::from_utf8_lossy(&output.stdout)
    );
    let stderr = String::from_utf8(output.stderr).expect("stderr is utf8");
    assert!(stderr.contains("daemon not running"));
    assert!(!stderr.contains(data_dir.to_string_lossy().as_ref()));
}

#[test]
fn cli_sessions_spawn_and_list_route_through_client_api() {
    let _guard = daemon_test_guard();
    let data_dir = unique_test_dir("cli-sessions");
    let child = start_cli_daemon(&data_dir);
    let spawn = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("sessions")
        .arg("spawn")
        .arg("--data-dir")
        .arg(&data_dir)
        .arg("--session-id")
        .arg("runtime-session")
        .arg("--")
        .arg("printf 'runtime-ok\\n'; IFS= read -r line; printf 'runtime:%s\\n' \"$line\"")
        .output()
        .expect("run botster-hub sessions spawn");

    assert!(
        spawn.status.success(),
        "spawn failed: {}",
        String::from_utf8_lossy(&spawn.stderr)
    );
    let stdout = String::from_utf8(spawn.stdout).expect("stdout is utf8");
    assert!(stdout.contains("response=spawned"));
    assert!(stdout.contains("session_id=runtime-session"));
    assert!(stdout.contains("lifecycle=running"));
    assert!(stdout.contains("event_count=0"));
    assert!(!stdout.contains(data_dir.to_string_lossy().as_ref()));

    let list = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("sessions")
        .arg("list")
        .arg("--data-dir")
        .arg(&data_dir)
        .output()
        .expect("run botster-hub sessions list");

    assert!(
        list.status.success(),
        "list failed: {}",
        String::from_utf8_lossy(&list.stderr)
    );
    let stdout = String::from_utf8(list.stdout).expect("stdout is utf8");
    assert!(stdout.contains("response=sessions"));
    assert!(stdout.contains("session_count=1"));
    assert!(stdout.contains("session id=runtime-session lifecycle=running"));
    assert!(!stdout.contains(data_dir.to_string_lossy().as_ref()));

    let resize = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("sessions")
        .arg("resize")
        .arg("--data-dir")
        .arg(&data_dir)
        .arg("runtime-session")
        .arg("30")
        .arg("100")
        .output()
        .expect("run botster-hub sessions resize");
    assert!(
        resize.status.success(),
        "resize failed: {}",
        String::from_utf8_lossy(&resize.stderr)
    );

    let attach = botster_hub::daemon_transport_request(
        &explicit_config(&data_dir),
        botster_hub::DaemonRequest::Attach {
            session_id: "runtime-session".to_string(),
            subscription_id: "botster-hub-cli-subscription".to_string(),
        },
    )
    .expect("attach before explicit detach");
    assert_eq!(attach.kind, botster_hub::DaemonResponseKind::Events);

    let detach = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("sessions")
        .arg("detach")
        .arg("--data-dir")
        .arg(&data_dir)
        .arg("runtime-session")
        .output()
        .expect("run botster-hub sessions detach");
    assert!(
        detach.status.success(),
        "detach failed: {}",
        String::from_utf8_lossy(&detach.stderr)
    );

    let mut attach_child = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("sessions")
        .arg("attach")
        .arg("--data-dir")
        .arg(&data_dir)
        .arg("runtime-session")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn botster-hub sessions attach");
    let mut attach_stdout = BufReader::new(
        attach_child
            .stdout
            .take()
            .expect("attach child stdout is piped"),
    );
    let mut stdout = String::new();
    attach_stdout
        .read_line(&mut stdout)
        .expect("read initial attach output");
    assert!(stdout.contains("runtime-ok"));

    let send = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("sessions")
        .arg("send-input")
        .arg("--data-dir")
        .arg(&data_dir)
        .arg("runtime-session")
        .arg("--")
        .arg("from-cli\r")
        .output()
        .expect("run botster-hub sessions send-input");
    assert!(
        send.status.success(),
        "send-input failed: {}",
        String::from_utf8_lossy(&send.stderr)
    );

    let attach_status = attach_child.wait().expect("wait for attach child");
    attach_stdout
        .read_to_string(&mut stdout)
        .expect("read remaining attach output");
    let mut stderr = String::new();
    attach_child
        .stderr
        .take()
        .expect("attach child stderr is piped")
        .read_to_string(&mut stderr)
        .expect("read attach stderr");
    assert!(attach_status.success(), "attach failed: {}", stderr);
    assert!(stdout.contains("runtime-ok"));
    assert!(stdout.contains("runtime:from-cli"));

    shutdown_cli_daemon(&data_dir, child);
}

#[test]
fn cli_short_lived_session_shutdown_returns_structured_cleanup() {
    let _guard = daemon_test_guard();
    let data_dir = unique_test_dir("cli-short-lived-shutdown");
    let child = start_cli_daemon(&data_dir);

    let spawn = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("sessions")
        .arg("spawn")
        .arg("--data-dir")
        .arg(&data_dir)
        .arg("--session-id")
        .arg("runtime-session")
        .arg("--")
        .arg("printf 'runtime-ok\\n'; IFS= read -r line; printf 'runtime:%s\\n' \"$line\"")
        .output()
        .expect("run botster-hub sessions spawn");
    assert!(
        spawn.status.success(),
        "spawn failed: {}",
        String::from_utf8_lossy(&spawn.stderr)
    );

    let attach_child = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("sessions")
        .arg("attach")
        .arg("--data-dir")
        .arg(&data_dir)
        .arg("runtime-session")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("run botster-hub sessions attach");

    thread::sleep(Duration::from_millis(150));
    let send = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("sessions")
        .arg("send-input")
        .arg("--data-dir")
        .arg(&data_dir)
        .arg("runtime-session")
        .arg("--")
        .arg("done\r")
        .output()
        .expect("run botster-hub sessions send-input");
    assert!(
        send.status.success(),
        "send-input failed: {}",
        String::from_utf8_lossy(&send.stderr)
    );

    let attach = attach_child
        .wait_with_output()
        .expect("wait for attach child");
    assert!(
        attach.status.success(),
        "attach failed: {}",
        String::from_utf8_lossy(&attach.stderr)
    );
    let attach_stdout = String::from_utf8(attach.stdout).expect("attach stdout is utf8");
    assert!(attach_stdout.contains("runtime-ok"));
    assert!(attach_stdout.contains("runtime:done"));

    let shutdown = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("sessions")
        .arg("shutdown")
        .arg("--data-dir")
        .arg(&data_dir)
        .arg("runtime-session")
        .output()
        .expect("run botster-hub sessions shutdown");
    assert!(
        shutdown.status.success(),
        "shutdown failed: {}",
        String::from_utf8_lossy(&shutdown.stderr)
    );
    let stdout = String::from_utf8(shutdown.stdout).expect("shutdown stdout is utf8");
    let stderr = String::from_utf8(shutdown.stderr).expect("shutdown stderr is utf8");
    assert!(stdout.contains("response=session_cleanup"));
    assert!(stdout.contains("session_id=runtime-session"));
    assert!(stdout.contains("outcome=already_exited"));
    assert!(!stdout.contains("client disconnected"));
    assert!(!stderr.contains("client disconnected"));
    assert!(!stdout.contains(data_dir.to_string_lossy().as_ref()));
    assert!(!stderr.contains(data_dir.to_string_lossy().as_ref()));

    shutdown_cli_daemon(&data_dir, child);
}

#[test]
fn cli_request_level_runtime_error_returns_operator_frame_and_keeps_daemon_responsive() {
    let _guard = daemon_test_guard();
    let data_dir = unique_test_dir("cli-operator-error");
    let child = start_cli_daemon(&data_dir);

    let send = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("sessions")
        .arg("send-input")
        .arg("--data-dir")
        .arg(&data_dir)
        .arg("missing-session")
        .arg("--")
        .arg("input\r")
        .output()
        .expect("run botster-hub sessions send-input");
    assert!(
        !send.status.success(),
        "missing-session send-input should fail with operator frame"
    );
    let stdout = String::from_utf8(send.stdout).expect("send stdout is utf8");
    let stderr = String::from_utf8(send.stderr).expect("send stderr is utf8");
    assert!(stdout.contains("response=operator_error"));
    assert!(stdout.contains("error_code=unknown_session"));
    assert!(stdout.contains("operation=input"));
    assert!(stderr.contains("operator error: unknown_session"));
    assert!(!stdout.contains("client disconnected"));
    assert!(!stderr.contains("client disconnected"));
    assert!(!stdout.contains(data_dir.to_string_lossy().as_ref()));
    assert!(!stderr.contains(data_dir.to_string_lossy().as_ref()));

    let status = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("status")
        .arg("--data-dir")
        .arg(&data_dir)
        .output()
        .expect("run botster-hub status after operator error");
    assert!(
        status.status.success(),
        "status failed after operator error: {}",
        String::from_utf8_lossy(&status.stderr)
    );
    let stdout = String::from_utf8(status.stdout).expect("status stdout is utf8");
    assert!(stdout.contains("event=status"));
    assert!(stdout.contains("lifecycle_state=running"));
    assert!(!stdout.contains(data_dir.to_string_lossy().as_ref()));

    shutdown_cli_daemon(&data_dir, child);
}

#[test]
fn process_ownership_daemon_restart_adopts_then_shuts_down_worker_session() {
    let _guard = daemon_test_guard();
    let data_dir = unique_test_dir("cli-restart-recover");
    let config = explicit_config(&data_dir);
    let session_id = format!("cli-restart-session-{}", std::process::id());

    let child = start_cli_daemon(&data_dir);
    let spawn = botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::Spawn {
            session_id: session_id.to_string(),
            command: "printf 'restart-ready\\n'; while IFS= read -r line; do printf 'echo:%s\\n' \"$line\"; if [ \"$line\" = after-restart ]; then exit 0; fi; done".to_string(),
        },
    )
    .expect("spawn restart recovery session through daemon transport");
    assert_eq!(
        spawn.kind,
        botster_hub::DaemonResponseKind::Spawned,
        "spawn failed: {:?}",
        spawn.error
    );
    assert!(
        spawn
            .sessions
            .iter()
            .any(|session| session.session_id == session_id && session.lifecycle == "running")
    );

    shutdown_cli_daemon(&data_dir, child);
    let restarted_child = start_cli_daemon(&data_dir);

    let status = botster_hub::daemon_transport_request(&config, botster_hub::DaemonRequest::Status)
        .expect("status after daemon restart");
    let status = status.status.expect("status response body");
    assert_eq!(status.lifecycle_state, "running");
    assert!(status.core_initialized);
    assert!(
        status
            .recovered_sessions
            .iter()
            .any(|recovered| recovered == &session_id),
        "restarted daemon should report startup recovery for the live worker-backed session"
    );
    assert!(
        !status
            .stale_sessions
            .iter()
            .any(|stale| stale == &session_id),
        "worker-backed session with protocol evidence should not be marked stale"
    );

    let list =
        botster_hub::daemon_transport_request(&config, botster_hub::DaemonRequest::ListSessions)
            .expect("list recovered session through daemon transport");
    assert!(
        list.sessions
            .iter()
            .any(|session| session.session_id == session_id && session.lifecycle == "running")
    );

    let resize = botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::Resize {
            session_id: session_id.to_string(),
            rows: 30,
            cols: 100,
        },
    )
    .expect("resize after daemon restart");
    assert_eq!(resize.kind, botster_hub::DaemonResponseKind::Events);
    let attach_config = config.clone();
    let attach_session_id = SessionId(session_id.to_string());
    let attach_handle = thread::spawn(move || {
        let mut output = Vec::new();
        botster_hub::stream_attach(
            &attach_config,
            attach_session_id,
            SubscriptionId("cli-restart-subscription-after".to_string()),
            &mut output,
        )
        .expect("stream attach after daemon restart");
        output
    });
    thread::sleep(Duration::from_millis(100));
    let send = botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::SendInput {
            session_id: session_id.to_string(),
            data: "after-restart\n".to_string(),
        },
    )
    .expect("send input after daemon restart");
    assert_eq!(send.kind, botster_hub::DaemonResponseKind::Events);

    let attached_output = attach_handle
        .join()
        .expect("stream attach thread should complete");
    let attached_output = String::from_utf8_lossy(&attached_output);
    assert!(
        attached_output.contains("echo:after-restart"),
        "stream attach should observe post-restart echo, got {attached_output:?}"
    );
    shutdown_cli_daemon(&data_dir, restarted_child);
}

#[test]
fn external_hub_client_read_mode_flags_drives_real_daemon_socket_protocol() {
    let _guard = daemon_test_guard();
    let data_dir = unique_test_dir("external-hub-client");
    let config = explicit_config(&data_dir);
    let socket_path = config
        .transports
        .local_socket
        .as_ref()
        .expect("test config has local socket")
        .path
        .clone();
    let endpoint = botster_hub_client::DaemonEndpoint::new(socket_path);
    let child = start_cli_daemon(&data_dir);

    let status = botster_hub_client::request(&endpoint, botster_hub_client::DaemonRequest::Status)
        .expect("external client status request");
    assert_eq!(status.kind, botster_hub_client::DaemonResponseKind::Status);
    assert!(status.diagnostics.iter().any(|diagnostic| {
        diagnostic.kind == botster_hub_client::DaemonDiagnosticKind::Connected
            && diagnostic.operation.as_deref() == Some("status")
    }));
    assert!(!has_failure_diagnostic(&status.diagnostics));
    assert_eq!(
        status
            .status
            .as_ref()
            .expect("status response body")
            .lifecycle_state,
        "running"
    );

    let list =
        botster_hub_client::request(&endpoint, botster_hub_client::DaemonRequest::ListSessions)
            .expect("external client list sessions request");
    assert_eq!(list.kind, botster_hub_client::DaemonResponseKind::Sessions);

    let spawn = botster_hub_client::request(
        &endpoint,
        botster_hub_client::DaemonRequest::Spawn {
            session_id: "external-client-session".to_string(),
            command:
                "printf '\\033[?1000h\\033[?1006h'; printf 'external-ready\\n'; while IFS= read -r line; do printf 'echo:%s\\n' \"$line\"; done"
                    .to_string(),
        },
    )
    .expect("external client spawn request");
    assert_eq!(spawn.kind, botster_hub_client::DaemonResponseKind::Spawned);
    assert!(
        spawn
            .sessions
            .iter()
            .any(|session| session.session_id == "external-client-session"
                && session.lifecycle == "running")
    );

    let mut connection =
        botster_hub_client::DaemonConnection::connect(&endpoint).expect("external connect");
    let attach = connection
        .request(&botster_hub_client::DaemonRequest::Attach {
            session_id: "external-client-session".to_string(),
            subscription_id: "external-client-subscription".to_string(),
        })
        .expect("external attach request");
    assert_eq!(attach.kind, botster_hub_client::DaemonResponseKind::Events);

    let resize = connection
        .request(&botster_hub_client::DaemonRequest::Resize {
            session_id: "external-client-session".to_string(),
            rows: 31,
            cols: 101,
        })
        .expect("external resize request");
    assert_eq!(resize.kind, botster_hub_client::DaemonResponseKind::Events);

    let send = connection
        .request(&botster_hub_client::DaemonRequest::SendInput {
            session_id: "external-client-session".to_string(),
            data: "external-input\n".to_string(),
        })
        .expect("external send input request");
    assert_eq!(send.kind, botster_hub_client::DaemonResponseKind::Events);

    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    let mut observed = String::new();
    while std::time::Instant::now() < deadline {
        let drain = connection
            .request(&botster_hub_client::DaemonRequest::Drain {
                session_id: "external-client-session".to_string(),
            })
            .expect("external drain request");
        for event in drain.events {
            if let botster_hub_client::DaemonEvent::TerminalOutput { data, .. } = event {
                observed.push_str(&data);
            }
        }
        if observed.contains("echo:external-input") {
            break;
        }
        thread::sleep(Duration::from_millis(30));
    }
    assert!(
        observed.contains("echo:external-input"),
        "external client should drain terminal output through the hub protocol, got {observed:?}"
    );

    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    let mode_flags = loop {
        let response = connection
            .request(&botster_hub_client::DaemonRequest::ReadModeFlags {
                session_id: "external-client-session".to_string(),
            })
            .expect("external read_mode_flags request");
        assert_eq!(
            response.kind,
            botster_hub_client::DaemonResponseKind::ReadModeFlags
        );
        let mode_flags = response.mode_flags.expect("read_mode_flags response body");
        if mode_flags.mouse_mode == 9 {
            break mode_flags;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "timed out waiting for exact combined mouse mode, last value {}",
            mode_flags.mouse_mode
        );
        thread::sleep(Duration::from_millis(20));
    };
    assert_eq!(mode_flags.session_id, "external-client-session");
    assert_eq!(mode_flags.mouse_mode, 9);

    let detach = connection
        .request(&botster_hub_client::DaemonRequest::Detach {
            session_id: "external-client-session".to_string(),
            subscription_id: "external-client-subscription".to_string(),
        })
        .expect("external detach request");
    assert_eq!(detach.kind, botster_hub_client::DaemonResponseKind::Events);

    let terminal_unavailable = connection
        .request(&botster_hub_client::DaemonRequest::Drain {
            session_id: "missing-external-client-session".to_string(),
        })
        .expect("missing terminal drain returns operator response");
    assert_eq!(
        terminal_unavailable.kind,
        botster_hub_client::DaemonResponseKind::OperatorError
    );
    assert!(terminal_unavailable.diagnostics.iter().any(|diagnostic| {
        diagnostic.kind == botster_hub_client::DaemonDiagnosticKind::TerminalStreamUnavailable
            && diagnostic.operation.as_deref() == Some("drain_runtime")
            && diagnostic.feature.as_deref() == Some(botster_hub_client::FEATURE_TERMINAL_STREAMING)
    }));
    assert!(!has_diagnostic_kind(
        &terminal_unavailable.diagnostics,
        botster_hub_client::DaemonDiagnosticKind::Connected
    ));
    let terminal_debug = format!("{:?}", terminal_unavailable.diagnostics);
    assert!(!terminal_debug.contains(&data_dir.to_string_lossy().to_string()));
    assert!(!terminal_debug.contains(concat!("/", "Users", "/")));
    assert!(!terminal_debug.contains("/home/"));

    let missing_read_screen = connection
        .request(&botster_hub_client::DaemonRequest::ReadScreen {
            session_id: "missing-external-client-session".to_string(),
        })
        .expect("missing read_screen returns operator response");
    assert_eq!(
        missing_read_screen.kind,
        botster_hub_client::DaemonResponseKind::OperatorError
    );
    let error = missing_read_screen.error.expect("read_screen error frame");
    assert_eq!(error.code, "unknown_session");
    assert_eq!(error.operation, "read_screen");

    let status_after_read_error = connection
        .request(&botster_hub_client::DaemonRequest::Status)
        .expect("connection stays usable after read_screen error");
    assert_eq!(
        status_after_read_error.kind,
        botster_hub_client::DaemonResponseKind::Status
    );

    let missing_mode_flags = connection
        .request(&botster_hub_client::DaemonRequest::ReadModeFlags {
            session_id: "missing-external-client-session".to_string(),
        })
        .expect("missing read_mode_flags returns operator response");
    assert_eq!(
        missing_mode_flags.kind,
        botster_hub_client::DaemonResponseKind::OperatorError
    );
    assert!(
        missing_mode_flags.mode_flags.is_none(),
        "unknown session must not fabricate a successful mouse-off body"
    );
    let error = missing_mode_flags
        .error
        .expect("read_mode_flags error frame");
    assert_eq!(error.code, "unknown_session");
    assert_eq!(error.operation, "read_mode_flags");

    let status_after_mode_error = connection
        .request(&botster_hub_client::DaemonRequest::Status)
        .expect("connection stays usable after read_mode_flags error");
    assert_eq!(
        status_after_mode_error.kind,
        botster_hub_client::DaemonResponseKind::Status
    );

    let missing_snapshot = connection
        .request(&botster_hub_client::DaemonRequest::CaptureSnapshot {
            session_id: "missing-external-client-session".to_string(),
        })
        .expect("missing capture_snapshot returns operator response");
    assert_eq!(
        missing_snapshot.kind,
        botster_hub_client::DaemonResponseKind::OperatorError
    );
    let error = missing_snapshot
        .error
        .expect("capture_snapshot error frame");
    assert_eq!(error.code, "unknown_session");
    assert_eq!(error.operation, "capture_snapshot");

    let status_after_snapshot_error = connection
        .request(&botster_hub_client::DaemonRequest::Status)
        .expect("connection stays usable after capture_snapshot error");
    assert_eq!(
        status_after_snapshot_error.kind,
        botster_hub_client::DaemonResponseKind::Status
    );

    drop(connection);

    let reconnect =
        botster_hub_client::DaemonConnection::connect(&endpoint).expect("external reconnect");
    drop(reconnect);

    let shutdown_session = botster_hub_client::request(
        &endpoint,
        botster_hub_client::DaemonRequest::ShutdownSession {
            session_id: "external-client-session".to_string(),
        },
    )
    .expect("external shutdown session request");
    assert_eq!(
        shutdown_session.kind,
        botster_hub_client::DaemonResponseKind::Events
    );
    shutdown_cli_daemon(&data_dir, child);
}

#[test]
fn session_entity_subscription_pushes_snapshot_ordered_deltas_and_fresh_reconnect() {
    let _guard = daemon_test_guard();
    let conformance_hub = botster_hub_test_support::IsolatedHubBuilder::new()
        .hub_bin(env!("CARGO_BIN_EXE_botster-hub"))
        .session_worker_bin(session_worker_binary_path())
        .root(PathBuf::from("/tmp/bh-slc"))
        .name("published-runner")
        .start()
        .expect("start isolated hub for published session lifecycle conformance");
    let conformance_report =
        botster_hub_test_support::run_session_lifecycle_subscription_conformance(&conformance_hub)
            .expect("run published session lifecycle conformance against real topology");
    assert!(conformance_report.initial_snapshot_authoritative);
    assert!(conformance_report.concurrent_subscribers_consistent);
    assert!(conformance_report.spawn_upsert_observed);
    assert!(conformance_report.lifecycle_patch_observed);
    assert!(conformance_report.natural_exit_patch_observed);
    assert!(conformance_report.remove_observed);
    assert!(conformance_report.sequences_strictly_increasing);
    assert!(conformance_report.disconnect_cleanup_released_subscription);
    assert!(conformance_report.fresh_subscription_snapshot_authoritative);
    assert_eq!(
        conformance_report.overflow_resync_reason,
        "subscriber_overflow"
    );
    assert!(conformance_report.failed_snapshot_delivery_closes_subscription);
    conformance_hub
        .shutdown()
        .expect("shutdown published session lifecycle conformance hub");

    let data_dir = unique_test_dir("session-entity-subscription");
    let config = explicit_config(&data_dir);
    let endpoint = botster_hub_client::DaemonEndpoint::new(
        config
            .transports
            .local_socket
            .as_ref()
            .expect("test config has local socket")
            .path
            .clone(),
    );
    let child = PanicSafeCliDaemon::start(&data_dir, "session entity daemon evidence");

    let mut first = botster_hub_client::subscribe_session_entities(&endpoint, "entities-first")
        .expect("subscribe first session entity stream");
    first
        .set_read_timeout(Some(Duration::from_secs(5)))
        .expect("bound first entity reads");
    let initial = first.next_frame().expect("initial authoritative snapshot");
    assert!(matches!(
        initial,
        botster_hub_client::DaemonEntityFrame::Snapshot {
            snapshot_seq: 0,
            ref items,
            resync_reason: None,
            ..
        } if items.is_empty()
    ));

    let mut second = botster_hub_client::subscribe_session_entities(&endpoint, "entities-second")
        .expect("subscribe independent session entity stream");
    second
        .set_read_timeout(Some(Duration::from_secs(5)))
        .expect("bound second entity reads");
    assert!(matches!(
        second.next_frame().expect("second authoritative snapshot"),
        botster_hub_client::DaemonEntityFrame::Snapshot { .. }
    ));

    let spawn = botster_hub_client::request(
        &endpoint,
        botster_hub_client::DaemonRequest::Spawn {
            session_id: "entity-session".to_string(),
            command: "printf 'entity-before\\nentity-ready\\n'; IFS= read -r release; \
                      printf 'entity-after:%s\\n' \"$release\""
                .to_string(),
        },
    )
    .expect("spawn entity session");
    assert_eq!(spawn.kind, botster_hub_client::DaemonResponseKind::Spawned);
    let mut session_cleanup = SessionCleanupGuard::new(&data_dir, "entity-session");

    let first_upsert = first.next_frame().expect("first subscriber upsert");
    let second_upsert = second.next_frame().expect("second subscriber upsert");
    let upsert_sequence = match first_upsert {
        botster_hub_client::DaemonEntityFrame::Upsert {
            snapshot_seq,
            ref id,
            ref entity,
            ..
        } if id == "entity-session" => {
            assert_eq!(entity.lifecycle_class, "current");
            snapshot_seq
        }
        other => panic!("expected first upsert, got {other:?}"),
    };
    assert!(matches!(
        second_upsert,
        botster_hub_client::DaemonEntityFrame::Upsert {
            snapshot_seq,
            ref id,
            ..
        } if id == "entity-session" && snapshot_seq == upsert_sequence
    ));

    let mut terminal =
        botster_hub_client::DaemonConnection::connect(&endpoint).expect("terminal connection");
    terminal
        .request(&botster_hub_client::DaemonRequest::Attach {
            session_id: "entity-session".to_string(),
            subscription_id: "terminal-alongside-entities".to_string(),
        })
        .expect("attach while entity pump is active");
    let terminal_deadline = Instant::now() + Duration::from_secs(5);
    let mut terminal_output = String::new();
    while Instant::now() < terminal_deadline && !terminal_output.contains("entity-ready") {
        let drain = terminal
            .request(&botster_hub_client::DaemonRequest::Drain {
                session_id: "entity-session".to_string(),
            })
            .expect("drain terminal output alongside entity pump");
        for event in drain.events {
            if let botster_hub_client::DaemonEvent::TerminalOutput { data, .. } = event {
                terminal_output.push_str(&data);
            }
        }
        thread::sleep(Duration::from_millis(20));
    }
    assert!(
        terminal_output.contains("entity-before") && terminal_output.contains("entity-ready"),
        "entity fixture must publish semantic readiness through retained terminal egress, \
         got {terminal_output:?}"
    );

    let resize = botster_hub_client::request(
        &endpoint,
        botster_hub_client::DaemonRequest::Resize {
            session_id: "entity-session".to_string(),
            rows: 31,
            cols: 101,
        },
    )
    .expect("resize entity session");
    assert_eq!(
        resize.kind,
        botster_hub_client::DaemonResponseKind::Events,
        "live semantic barrier must keep resize accepted: {resize:?}"
    );
    let first_resize = first
        .next_frame()
        .expect("first subscriber resize transition");
    let second_resize = second
        .next_frame()
        .expect("second subscriber resize transition");
    let resize_sequence = match &first_resize {
        botster_hub_client::DaemonEntityFrame::Patch {
            snapshot_seq,
            id,
            patch,
            ..
        } if id == "entity-session"
            && patch.get("rows").and_then(serde_json::Value::as_u64) == Some(31)
            && patch.get("cols").and_then(serde_json::Value::as_u64) == Some(101) =>
        {
            *snapshot_seq
        }
        _ => panic!(
            "expected rows=31/cols=101 as the first post-resize frame for both subscribers; \
             resize={resize:?} first={first_resize:?} second={second_resize:?}"
        ),
    };
    assert!(resize_sequence > upsert_sequence);
    let second_resize_sequence = match &second_resize {
        botster_hub_client::DaemonEntityFrame::Patch {
            snapshot_seq,
            id,
            patch,
            ..
        } if id == "entity-session"
            && patch.get("rows").and_then(serde_json::Value::as_u64) == Some(31)
            && patch.get("cols").and_then(serde_json::Value::as_u64) == Some(101) =>
        {
            *snapshot_seq
        }
        _ => panic!(
            "expected rows=31/cols=101 as the first post-resize frame for both subscribers; \
             resize={resize:?} first={first_resize:?} second={second_resize:?}"
        ),
    };
    assert_eq!(
        second_resize_sequence, resize_sequence,
        "subscriber resize sequences diverged: first={first_resize:?} second={second_resize:?}"
    );

    let release = terminal
        .request(&botster_hub_client::DaemonRequest::SendInput {
            session_id: "entity-session".to_string(),
            data: "release\r".to_string(),
        })
        .expect("release entity fixture through terminal input");
    assert_eq!(release.kind, botster_hub_client::DaemonResponseKind::Events);
    for event in release.events {
        if let botster_hub_client::DaemonEvent::TerminalOutput { data, .. } = event {
            terminal_output.push_str(&data);
        }
    }
    let release_deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < release_deadline && !terminal_output.contains("entity-after:release") {
        let drain = terminal
            .request(&botster_hub_client::DaemonRequest::Drain {
                session_id: "entity-session".to_string(),
            })
            .expect("drain terminal output after releasing entity fixture");
        for event in drain.events {
            if let botster_hub_client::DaemonEvent::TerminalOutput { data, .. } = event {
                terminal_output.push_str(&data);
            }
        }
        thread::sleep(Duration::from_millis(20));
    }
    assert!(
        terminal_output.contains("entity-after:release"),
        "entity lifecycle pumping must retain terminal egress through natural exit, \
         got {terminal_output:?}"
    );

    let first_exit = first.next_frame().expect("first subscriber natural exit");
    let second_exit = second.next_frame().expect("second subscriber natural exit");
    let exit_sequence = match &first_exit {
        botster_hub_client::DaemonEntityFrame::Patch {
            snapshot_seq,
            id,
            patch,
            ..
        } if id == "entity-session"
            && patch.get("lifecycle").and_then(serde_json::Value::as_str) == Some("exited")
            && patch
                .get("lifecycle_class")
                .and_then(serde_json::Value::as_str)
                == Some("ended")
            && patch.get("exit_code").and_then(serde_json::Value::as_i64) == Some(0) =>
        {
            *snapshot_seq
        }
        _ => panic!(
            "expected natural exit_code=0 as the first post-release frame for both subscribers; \
             first={first_exit:?} second={second_exit:?}"
        ),
    };
    let second_exit_sequence = match &second_exit {
        botster_hub_client::DaemonEntityFrame::Patch {
            snapshot_seq,
            id,
            patch,
            ..
        } if id == "entity-session"
            && patch.get("lifecycle").and_then(serde_json::Value::as_str) == Some("exited")
            && patch
                .get("lifecycle_class")
                .and_then(serde_json::Value::as_str)
                == Some("ended")
            && patch.get("exit_code").and_then(serde_json::Value::as_i64) == Some(0) =>
        {
            *snapshot_seq
        }
        _ => panic!(
            "expected natural exit_code=0 as the first post-release frame for both subscribers; \
             first={first_exit:?} second={second_exit:?}"
        ),
    };
    assert!(exit_sequence > resize_sequence);
    assert_eq!(
        second_exit_sequence, exit_sequence,
        "subscriber exit sequences diverged: first={first_exit:?} second={second_exit:?}"
    );

    let removed = botster_hub_client::request(
        &endpoint,
        botster_hub_client::DaemonRequest::RemoveSession {
            session_id: "entity-session".to_string(),
        },
    )
    .expect("remove terminal entity session");
    assert_eq!(
        removed.kind,
        botster_hub_client::DaemonResponseKind::SessionRemoved
    );
    session_cleanup.disarm();
    assert!(matches!(
        first.next_frame().expect("remove delta"),
        botster_hub_client::DaemonEntityFrame::Remove {
            snapshot_seq,
            ref id,
            ..
        } if id == "entity-session" && snapshot_seq > exit_sequence
    ));

    drop(first);
    let cleanup_deadline = Instant::now() + Duration::from_secs(2);
    let cleanup_probe = loop {
        match botster_hub_client::subscribe_session_entities(&endpoint, "entities-first") {
            Ok(subscription) => break subscription,
            Err(_) if Instant::now() < cleanup_deadline => {
                thread::sleep(Duration::from_millis(20));
            }
            Err(error) => panic!("socket EOF should release the old subscription: {error}"),
        }
    };
    cleanup_probe
        .unsubscribe()
        .expect("unsubscribe cleanup probe stream");
    let mut reconnected =
        botster_hub_client::subscribe_session_entities(&endpoint, "entities-reconnected")
            .expect("fresh reconnect subscription");
    reconnected
        .set_read_timeout(Some(Duration::from_secs(5)))
        .expect("bound reconnect entity reads");
    assert!(matches!(
        reconnected.next_frame().expect("fresh reconnect snapshot"),
        botster_hub_client::DaemonEntityFrame::Snapshot {
            ref subscription_id,
            ref items,
            ..
        } if subscription_id == "entities-reconnected" && items.is_empty()
    ));
    reconnected
        .unsubscribe()
        .expect("unsubscribe reconnect stream");
    second.unsubscribe().expect("unsubscribe second stream");
    child.shutdown();
}

#[test]
fn session_entity_subscription_projects_stale_row_as_indeterminate() {
    let _guard = daemon_test_guard();
    let data_dir = unique_test_dir("session-entity-stale");
    let config = explicit_config(&data_dir);
    let session_id = SessionId("session-entity-stale".to_string());
    let registry = SessionRegistry::new(config.data_directory.clone());
    let mut stale_record = RegistryRecord::running(
        session_id.clone(),
        Some(ProcessIdentity {
            pid: Some(42),
            runtime_id: Some("stale-runtime".to_string()),
        }),
        ResizePayload { rows: 24, cols: 80 },
        "sh".to_string(),
        1,
    );
    stale_record.observe_restart_contract(serde_json::json!({"session": "stale"}), 2);
    registry
        .save(&stale_record)
        .expect("save stale registry fixture");

    let endpoint = botster_hub_client::DaemonEndpoint::new(
        config
            .transports
            .local_socket
            .as_ref()
            .expect("test config has local socket")
            .path
            .clone(),
    );
    let child = start_cli_daemon(&data_dir);
    let mut subscription =
        botster_hub_client::subscribe_session_entities(&endpoint, "stale-session-entities")
            .expect("subscribe to stale session projection");
    subscription
        .set_read_timeout(Some(Duration::from_secs(5)))
        .expect("bound stale projection read");
    let snapshot = subscription
        .next_frame()
        .expect("authoritative stale snapshot");
    assert!(matches!(
        snapshot,
        botster_hub_client::DaemonEntityFrame::Snapshot { ref items, .. }
            if items.iter().any(|entity| {
                entity.session_uuid == session_id.0.as_str()
                    && entity.registry_state == "stale"
                    && entity.lifecycle_class == "indeterminate"
            })
    ));
    subscription
        .unsubscribe()
        .expect("unsubscribe stale projection");
    shutdown_cli_daemon(&data_dir, child);
}

fn process_thread_count(pid: u32) -> Option<usize> {
    for field in ["thcount=", "nlwp="] {
        let output = Command::new("ps")
            .args(["-o", field, "-p", &pid.to_string()])
            .output()
            .ok()?;
        if output.status.success()
            && let Ok(count) = String::from_utf8_lossy(&output.stdout).trim().parse()
        {
            return Some(count);
        }
    }
    None
}

#[test]
fn focused_connection_lifecycle_is_bounded_event_driven_and_counter_visible() {
    let _guard = daemon_test_guard();
    let data_dir = unique_test_dir("focused-connection-lifecycle");
    let config = explicit_config(&data_dir);
    let endpoint = botster_hub_client::DaemonEndpoint::new(
        config
            .transports
            .local_socket
            .as_ref()
            .expect("test config has local socket")
            .path
            .clone(),
    );
    let child = start_cli_daemon(&data_dir);
    let daemon_pid = child.id();
    let startup_counters =
        botster_hub_client::request(&endpoint, botster_hub_client::DaemonRequest::Status)
            .expect("status before first entity subscription")
            .status
            .expect("startup status body")
            .lifecycle_counters;
    assert_eq!(startup_counters.lifecycle_baseline_reads, 1);
    let mut subscription =
        botster_hub_client::subscribe_session_entities(&endpoint, "focused-idle")
            .expect("subscribe focused idle entity stream");
    subscription
        .set_read_timeout(Some(Duration::from_secs(5)))
        .expect("bound initial snapshot read");
    assert!(matches!(
        subscription.next_frame().expect("initial focused snapshot"),
        botster_hub_client::DaemonEntityFrame::Snapshot { .. }
    ));
    let before_idle =
        botster_hub_client::request(&endpoint, botster_hub_client::DaemonRequest::Status)
            .expect("status before focused idle window")
            .status
            .expect("focused status body")
            .lifecycle_counters;
    assert_eq!(
        before_idle.lifecycle_baseline_reads, startup_counters.lifecycle_baseline_reads,
        "first live subscriber must consume the startup-seeded baseline without owner-path I/O"
    );
    thread::sleep(Duration::from_millis(1_100));
    let after_idle =
        botster_hub_client::request(&endpoint, botster_hub_client::DaemonRequest::Status)
            .expect("status after focused idle window")
            .status
            .expect("focused status body")
            .lifecycle_counters;
    assert_eq!(
        after_idle.lifecycle_baseline_reads, before_idle.lifecycle_baseline_reads,
        "steady-state idle reconciliation must not rescan the session registry"
    );
    assert_eq!(
        after_idle.entity_delivery_attempts, before_idle.entity_delivery_attempts,
        "an idle entity stream must not receive timer-driven frames"
    );
    assert!(
        after_idle
            .lifecycle_change_reads
            .saturating_sub(before_idle.lifecycle_change_reads)
            <= 4,
        "the one shared idle backstop must stay low-frequency"
    );

    const FLOOD_CONNECTIONS: usize = 32;
    const FLOOD_REQUESTS_PER_CONNECTION: usize = 512;
    let mut flood_writers = Vec::new();
    let mut flood_readers = Vec::new();
    for _ in 0..FLOOD_CONNECTIONS {
        let mut writer =
            UnixStream::connect(&endpoint.socket_path).expect("connect pipelined pressure fixture");
        botster_hub_client::write_frame(
            &mut writer,
            &botster_hub_client::DaemonHello {
                protocol: botster_hub_client::PROTOCOL.to_string(),
                compatibility: botster_hub_client::DaemonCompatibilityRequirement::current(),
            },
        )
        .expect("write pressure-fixture hello");
        let _: botster_hub_client::DaemonHelloAck =
            botster_hub_client::read_frame(&mut writer).expect("read pressure-fixture hello ack");
        let mut reader = BufReader::new(
            writer
                .try_clone()
                .expect("clone pressure-fixture response reader"),
        );
        flood_readers.push(thread::spawn(move || {
            for _ in 0..FLOOD_REQUESTS_PER_CONNECTION {
                let _: botster_hub_client::DaemonResponse =
                    botster_hub_client::read_frame_from_reader(&mut reader)
                        .expect("drain pipelined status response");
            }
        }));
        flood_writers.push(writer);
    }
    let flood_before =
        botster_hub_client::request(&endpoint, botster_hub_client::DaemonRequest::Status)
            .expect("status before sustained control pressure")
            .status
            .expect("sustained control pressure status body")
            .lifecycle_counters;
    for writer in &mut flood_writers {
        for _ in 0..FLOOD_REQUESTS_PER_CONNECTION {
            botster_hub_client::write_frame(writer, &botster_hub_client::DaemonRequest::Status)
                .expect("pipeline sustained status request");
        }
    }
    thread::sleep(Duration::from_millis(1_100));
    let flood_after =
        botster_hub_client::request(&endpoint, botster_hub_client::DaemonRequest::Status)
            .expect("status during sustained control pressure")
            .status
            .expect("post-pressure status body")
            .lifecycle_counters;
    drop(flood_writers);
    for flood_reader in flood_readers {
        flood_reader
            .join()
            .expect("join pipelined status response drain");
    }
    assert!(
        flood_after.reconciliation_wakes > flood_before.reconciliation_wakes,
        "a continuously busy control queue must not starve shared entity reconciliation"
    );

    for index in 0..8 {
        botster_hub_client::request(
            &endpoint,
            botster_hub_client::DaemonRequest::Spawn {
                session_id: format!("focused-idle-session-{index}"),
                command: "sleep 10".to_string(),
            },
        )
        .expect("spawn focused session-count fixture");
    }
    let mut upserts = BTreeMap::new();
    while upserts.len() < 8 {
        if let botster_hub_client::DaemonEntityFrame::Upsert { id, .. } = subscription
            .next_frame()
            .expect("session-count fixture upsert")
        {
            upserts.insert(id, ());
        }
    }
    let mut additional_subscriptions = Vec::new();
    for index in 1..8 {
        let mut additional = botster_hub_client::subscribe_session_entities(
            &endpoint,
            format!("focused-idle-{index}"),
        )
        .expect("subscribe additional idle entity stream");
        additional
            .set_read_timeout(Some(Duration::from_secs(5)))
            .expect("bound additional snapshot read");
        assert!(matches!(
            additional.next_frame().expect("additional idle snapshot"),
            botster_hub_client::DaemonEntityFrame::Snapshot { ref items, .. }
                if items.len() == 8
        ));
        additional_subscriptions.push(additional);
    }
    thread::sleep(Duration::from_millis(600));
    let many_before =
        botster_hub_client::request(&endpoint, botster_hub_client::DaemonRequest::Status)
            .expect("status before many-session idle window")
            .status
            .expect("many-session status body")
            .lifecycle_counters;
    thread::sleep(Duration::from_millis(1_100));
    let many_after =
        botster_hub_client::request(&endpoint, botster_hub_client::DaemonRequest::Status)
            .expect("status after many-session idle window")
            .status
            .expect("many-session status body")
            .lifecycle_counters;
    assert_eq!(
        many_after.lifecycle_baseline_reads, many_before.lifecycle_baseline_reads,
        "session count must not restore filesystem-backed baseline polling"
    );
    assert_eq!(
        many_after.entity_delivery_attempts, many_before.entity_delivery_attempts,
        "subscriber count must not create timer-driven entity delivery"
    );
    assert!(
        many_after
            .reconciliation_wakes
            .saturating_sub(many_before.reconciliation_wakes)
            <= 4,
        "shared wake count must stay independent of session count"
    );
    assert!(
        many_after.lifecycle_session_drains > many_before.lifecycle_session_drains,
        "live sessions must drive the published lifecycle_session_drains producer"
    );

    let mut attached = botster_hub_client::DaemonConnection::connect(&endpoint)
        .expect("connect persistent attach counter fixture");
    attached
        .request(&botster_hub_client::DaemonRequest::Attach {
            session_id: "focused-idle-session-0".to_string(),
            subscription_id: "focused-attach".to_string(),
        })
        .expect("attach persistent counter fixture");
    let attached_counters =
        botster_hub_client::request(&endpoint, botster_hub_client::DaemonRequest::Status)
            .expect("status with live attach")
            .status
            .expect("live attach status body")
            .lifecycle_counters;
    assert_eq!(attached_counters.live_attach_subscriptions, 1);
    assert!(attached_counters.high_water_attach_subscriptions >= 1);
    botster_hub_client::request(
        &endpoint,
        botster_hub_client::DaemonRequest::ShutdownSession {
            session_id: "focused-idle-session-0".to_string(),
        },
    )
    .expect("shutdown attached cleanup-failure fixture");
    botster_hub_client::request(
        &endpoint,
        botster_hub_client::DaemonRequest::RemoveSession {
            session_id: "focused-idle-session-0".to_string(),
        },
    )
    .expect("remove attached cleanup-failure fixture");
    drop(attached);

    let failed_cleanup_deadline = Instant::now() + Duration::from_secs(3);
    loop {
        let counters =
            botster_hub_client::request(&endpoint, botster_hub_client::DaemonRequest::Status)
                .expect("status while waiting for failed detach cleanup")
                .status
                .expect("failed cleanup status body")
                .lifecycle_counters;
        if counters.cleanup_failed > attached_counters.cleanup_failed {
            assert_eq!(counters.live_attach_subscriptions, 0);
            break;
        }
        assert!(
            Instant::now() < failed_cleanup_deadline,
            "cleanup failure producer did not settle: {counters:?}"
        );
        thread::sleep(Duration::from_millis(20));
    }

    drop(subscription);
    drop(additional_subscriptions);

    let cleanup_deadline = Instant::now() + Duration::from_secs(3);
    loop {
        let counters =
            botster_hub_client::request(&endpoint, botster_hub_client::DaemonRequest::Status)
                .expect("status while waiting for entity cleanup")
                .status
                .expect("cleanup status body")
                .lifecycle_counters;
        if counters.live_entity_subscriptions == 0 {
            break;
        }
        assert!(
            Instant::now() < cleanup_deadline,
            "dropped entity stream did not release its subscription"
        );
        thread::sleep(Duration::from_millis(20));
    }

    let churn_start =
        botster_hub_client::request(&endpoint, botster_hub_client::DaemonRequest::Status)
            .expect("status before rapid reconnect churn")
            .status
            .expect("rapid reconnect start status")
            .lifecycle_counters;
    for index in 0..16 {
        let mut churn = botster_hub_client::subscribe_session_entities(
            &endpoint,
            format!("focused-churn-{index}"),
        )
        .expect("register fresh-id reconnect generation");
        churn
            .set_read_timeout(Some(Duration::from_secs(5)))
            .expect("bound churn snapshot read");
        assert!(matches!(
            churn.next_frame().expect("fresh-id churn snapshot"),
            botster_hub_client::DaemonEntityFrame::Snapshot { .. }
        ));
        drop(churn);
        let generation_deadline = Instant::now() + Duration::from_secs(2);
        loop {
            let counters =
                botster_hub_client::request(&endpoint, botster_hub_client::DaemonRequest::Status)
                    .expect("status while releasing reconnect generation")
                    .status
                    .expect("reconnect generation status body")
                    .lifecycle_counters;
            if counters.live_entity_subscriptions == 0 {
                assert_eq!(counters.high_water_entity_subscriptions, 8);
                break;
            }
            assert!(
                Instant::now() < generation_deadline,
                "reconnect generation {index} did not release: {counters:?}"
            );
            thread::sleep(Duration::from_millis(10));
        }
    }
    let churn_end =
        botster_hub_client::request(&endpoint, botster_hub_client::DaemonRequest::Status)
            .expect("status after rapid reconnect churn")
            .status
            .expect("rapid reconnect end status")
            .lifecycle_counters;
    assert_eq!(
        churn_end
            .reconnect_registrations
            .saturating_sub(churn_start.reconnect_registrations),
        16,
        "every released generation followed by a fresh subscription id is a reconnect"
    );
    assert!(
        churn_end
            .cleanup_by_reason
            .get("eof")
            .copied()
            .unwrap_or_default()
            .saturating_sub(
                churn_start
                    .cleanup_by_reason
                    .get("eof")
                    .copied()
                    .unwrap_or_default()
            )
            >= 16
    );

    let connection_baseline_threads = process_thread_count(daemon_pid);
    let mut idle_connections = Vec::new();
    for _ in 0..64 {
        idle_connections.push(
            botster_hub_client::DaemonConnection::connect(&endpoint)
                .expect("admit bounded idle connection"),
        );
    }
    let saturated = idle_connections[0]
        .request(&botster_hub_client::DaemonRequest::Status)
        .expect("existing client stays responsive at admission bound")
        .status
        .expect("saturated status body")
        .lifecycle_counters;
    assert_eq!(saturated.live_connections, 64);
    assert_eq!(saturated.high_water_live_connections, 64);
    assert!(saturated.accepted_connections >= 64);
    if let (Some(baseline), Some(peak)) = (
        connection_baseline_threads,
        process_thread_count(daemon_pid),
    ) {
        assert!(
            peak.saturating_sub(baseline) <= 8,
            "64 idle connections created too many OS threads: baseline={baseline} peak={peak}"
        );
    }

    let stalled_rejection =
        UnixStream::connect(&endpoint.socket_path).expect("connect stalled over-cap peer");
    let rejection_started = Instant::now();
    let mut rejected = botster_hub_client::DaemonConnection::connect(&endpoint)
        .expect("over-cap client receives typed admission hello");
    assert!(
        rejection_started.elapsed() < Duration::from_secs(1),
        "one silent over-cap peer must not head-of-line block typed rejection"
    );
    assert!(
        rejected
            .request(&botster_hub_client::DaemonRequest::Status)
            .is_err(),
        "over-cap connection must not enter the runtime request path"
    );
    let rejection_counter_deadline = Instant::now() + Duration::from_secs(3);
    loop {
        let rejected_status = idle_connections[0]
            .request(&botster_hub_client::DaemonRequest::Status)
            .expect("admitted client remains healthy after rejection")
            .status
            .expect("post-rejection status body")
            .lifecycle_counters;
        if rejected_status.rejected_connections >= 1 {
            break;
        }
        assert!(
            Instant::now() < rejection_counter_deadline,
            "typed rejection counter did not converge"
        );
        thread::sleep(Duration::from_millis(20));
    }
    drop(stalled_rejection);
    drop(rejected);
    drop(idle_connections);
    let release_deadline = Instant::now() + Duration::from_secs(3);
    loop {
        let counters =
            match botster_hub_client::request(&endpoint, botster_hub_client::DaemonRequest::Status)
            {
                Ok(response) => {
                    response
                        .status
                        .expect("released-connection status body")
                        .lifecycle_counters
                }
                Err(_) if Instant::now() < release_deadline => {
                    thread::sleep(Duration::from_millis(20));
                    continue;
                }
                Err(error) => panic!("connection admission did not recover: {error}"),
            };
        if counters.live_connections <= 1 {
            break;
        }
        assert!(
            Instant::now() < release_deadline,
            "idle connection owners did not release: {counters:?}"
        );
        thread::sleep(Duration::from_millis(20));
    }

    let mut malformed =
        UnixStream::connect(&endpoint.socket_path).expect("connect malformed raw daemon client");
    botster_hub_client::write_frame(
        &mut malformed,
        &botster_hub_client::DaemonHello {
            protocol: botster_hub_client::PROTOCOL.to_string(),
            compatibility: botster_hub_client::DaemonCompatibilityRequirement::current(),
        },
    )
    .expect("write malformed-client hello");
    let _: botster_hub_client::DaemonHelloAck =
        botster_hub_client::read_frame(&mut malformed).expect("read malformed-client hello ack");
    malformed
        .write_all(b"{\"type\":}\n")
        .expect("write malformed complete frame");
    drop(malformed);

    let mut half_open =
        UnixStream::connect(&endpoint.socket_path).expect("connect half-open raw daemon client");
    half_open
        .set_read_timeout(Some(Duration::from_secs(4)))
        .expect("bound half-open handshake close observation");
    let mut closed = [0_u8; 1];
    assert!(
        half_open.read(&mut closed).is_ok_and(|count| count == 0),
        "half-open handshake deadline must close the connection"
    );
    drop(half_open);

    let mut incomplete =
        UnixStream::connect(&endpoint.socket_path).expect("connect incomplete raw daemon client");
    botster_hub_client::write_frame(
        &mut incomplete,
        &botster_hub_client::DaemonHello {
            protocol: botster_hub_client::PROTOCOL.to_string(),
            compatibility: botster_hub_client::DaemonCompatibilityRequirement::current(),
        },
    )
    .expect("write incomplete-client hello");
    let _: botster_hub_client::DaemonHelloAck =
        botster_hub_client::read_frame(&mut incomplete).expect("read incomplete-client hello ack");
    incomplete
        .write_all(b"{\"type\":\"status\"")
        .expect("write incomplete frame");
    incomplete
        .set_read_timeout(Some(Duration::from_secs(4)))
        .expect("bound incomplete-frame close observation");
    assert!(
        incomplete.read(&mut closed).is_ok_and(|count| count == 0),
        "incomplete frame deadline must close the connection"
    );
    drop(incomplete);

    let cleanup_deadline = Instant::now() + Duration::from_secs(3);
    loop {
        let counters =
            botster_hub_client::request(&endpoint, botster_hub_client::DaemonRequest::Status)
                .expect("status after malformed and incomplete clients")
                .status
                .expect("terminal cleanup status body")
                .lifecycle_counters;
        if counters
            .cleanup_by_reason
            .get("protocol")
            .copied()
            .unwrap_or_default()
            >= 3
            && counters.live_connections <= 1
        {
            assert!(counters.cleanup_completed >= 67);
            break;
        }
        assert!(
            Instant::now() < cleanup_deadline,
            "transport cleanup counters did not settle: {counters:?}"
        );
        thread::sleep(Duration::from_millis(20));
    }

    for index in 1..8 {
        let session_id = format!("focused-idle-session-{index}");
        botster_hub_client::request(
            &endpoint,
            botster_hub_client::DaemonRequest::ShutdownSession {
                session_id: session_id.clone(),
            },
        )
        .expect("shutdown focused session-count fixture");
        botster_hub_client::request(
            &endpoint,
            botster_hub_client::DaemonRequest::RemoveSession { session_id },
        )
        .expect("remove focused session-count fixture");
    }

    shutdown_cli_daemon(&data_dir, child);
}

#[test]
fn session_entity_subscription_observes_natural_exit_without_terminal_attach() {
    let _guard = daemon_test_guard();
    let data_dir = unique_test_dir("session-entity-no-terminal");
    let config = explicit_config(&data_dir);
    let endpoint = botster_hub_client::DaemonEndpoint::new(
        config
            .transports
            .local_socket
            .as_ref()
            .expect("test config has local socket")
            .path
            .clone(),
    );
    let child = start_cli_daemon(&data_dir);
    let mut subscription =
        botster_hub_client::subscribe_session_entities(&endpoint, "entities-no-terminal")
            .expect("subscribe without terminal attach");
    subscription
        .set_read_timeout(Some(Duration::from_secs(5)))
        .expect("bound entity read");
    let _ = subscription.next_frame().expect("initial snapshot");

    botster_hub_client::request(
        &endpoint,
        botster_hub_client::DaemonRequest::Spawn {
            session_id: "entity-no-terminal".to_string(),
            command: "sleep 0.1".to_string(),
        },
    )
    .expect("spawn session without terminal attach");
    assert!(matches!(
        subscription.next_frame().expect("spawn upsert"),
        botster_hub_client::DaemonEntityFrame::Upsert { ref id, .. }
            if id == "entity-no-terminal"
    ));
    loop {
        match subscription.next_frame().expect("natural exit delta") {
            botster_hub_client::DaemonEntityFrame::Patch { patch, .. }
                if patch.get("lifecycle").and_then(serde_json::Value::as_str) == Some("exited") =>
            {
                break;
            }
            _ => {}
        }
    }
    subscription
        .unsubscribe()
        .expect("unsubscribe entity stream");
    shutdown_cli_daemon(&data_dir, child);
}

#[test]
fn shutdown_from_another_connection_preserves_process_exit_for_attached_subscription() {
    let _guard = daemon_test_guard();
    let data_dir = unique_short_test_dir("cross-shutdown-egress");
    let config = explicit_config(&data_dir);
    let endpoint = botster_hub_client::DaemonEndpoint::new(
        config
            .transports
            .local_socket
            .as_ref()
            .expect("test config has local socket")
            .path
            .clone(),
    );
    let child = start_cli_daemon(&data_dir);
    let session_id = "cross-connection-shutdown";
    let subscription_id = "cross-connection-terminal";
    let marker_path = data_dir.join("natural-exit-marker");
    let release_path = data_dir.join("natural-exit-release");

    botster_hub_client::request(
        &endpoint,
        botster_hub_client::DaemonRequest::Spawn {
            session_id: session_id.to_string(),
            command: format!(
                "printf ready; IFS= read -r line; printf 'cross-connection-exiting:%s\\n' \"$line\"; \
                 printf observed > '{}'; while [ ! -e '{}' ]; do sleep 0.01; done; \
                 printf 'cross-connection-tail\\n'; exit 0",
                marker_path.display(),
                release_path.display()
            ),
        },
    )
    .expect("spawn cross-connection shutdown session");
    let mut attached =
        botster_hub_client::DaemonConnection::connect(&endpoint).expect("terminal connection");
    attached
        .request(&botster_hub_client::DaemonRequest::Attach {
            session_id: session_id.to_string(),
            subscription_id: subscription_id.to_string(),
        })
        .expect("attach terminal subscription");
    attached
        .request(&botster_hub_client::DaemonRequest::SendInput {
            session_id: session_id.to_string(),
            data: "finish\r".to_string(),
        })
        .expect("release terminal fixture to its exit marker");
    for _ in 0..500 {
        if marker_path.exists() {
            break;
        }
        thread::sleep(Duration::from_millis(10));
    }
    assert!(
        marker_path.exists(),
        "terminal fixture did not publish its marker"
    );

    let mut observed_marker = false;
    for _ in 0..100 {
        let marker_drain = attached
            .request(&botster_hub_client::DaemonRequest::Drain {
                session_id: session_id.to_string(),
            })
            .expect("drain terminal marker before natural exit");
        observed_marker |= marker_drain.events.iter().any(|event| {
            matches!(
                event,
                botster_hub_client::DaemonEvent::TerminalOutput { data, .. }
                    if data.contains("cross-connection-exiting")
            )
        });
        assert!(
            marker_drain
                .events
                .iter()
                .all(|event| !matches!(event, botster_hub_client::DaemonEvent::ProcessExit { .. })),
            "fixture must remain live until its explicit release: {:?}",
            marker_drain.events
        );
        if observed_marker {
            break;
        }
        thread::sleep(Duration::from_millis(10));
    }
    assert!(
        observed_marker,
        "attached subscription did not observe the exit marker"
    );

    let registry: serde_json::Value = serde_json::from_slice(
        &fs::read(data_dir.join("sessions").join(format!("{session_id}.json")))
            .expect("read worker session registry"),
    )
    .expect("parse worker session registry");
    let pty_child_pid = registry["process"]["pid"]
        .as_u64()
        .expect("registry PTY child pid") as u32;
    let worker_socket = PathBuf::from(
        registry["recovery_identity"]["worker_control_socket"]
            .as_str()
            .expect("registry worker control socket"),
    );
    fs::write(&release_path, b"release").expect("release controlled natural exit");
    for _ in 0..500 {
        if !process_exists(pty_child_pid) && UnixStream::connect(&worker_socket).is_err() {
            break;
        }
        thread::sleep(Duration::from_millis(10));
    }
    assert!(
        !process_exists(pty_child_pid) && UnixStream::connect(&worker_socket).is_err(),
        "worker process and control route did not complete before shutdown"
    );

    let shutdown = botster_hub_client::request(
        &endpoint,
        botster_hub_client::DaemonRequest::ShutdownSession {
            session_id: session_id.to_string(),
        },
    )
    .expect("shutdown session from a separate connection");
    assert!(
        shutdown.events.iter().all(|event| !matches!(
            event,
            botster_hub_client::DaemonEvent::ProcessExit {
                subscription_id: event_subscription_id,
                ..
            } if event_subscription_id == subscription_id
        )),
        "shutdown caller must not consume the attached subscription's process exit: {:?}",
        shutdown.events
    );

    let attached_drain = attached
        .request(&botster_hub_client::DaemonRequest::Drain {
            session_id: session_id.to_string(),
        })
        .expect("drain attached subscription after cross-connection shutdown");
    assert_eq!(
        attached_drain
            .events
            .iter()
            .filter(|event| matches!(
                event,
                botster_hub_client::DaemonEvent::ProcessExit {
                    session_id: event_session_id,
                    subscription_id: event_subscription_id,
                    ..
                } if event_session_id == session_id && event_subscription_id == subscription_id
            ))
            .count(),
        1,
        "attached subscription must receive one process exit: {:?}",
        attached_drain.events
    );
    assert!(
        attached_drain.events.iter().any(|event| matches!(
            event,
            botster_hub_client::DaemonEvent::TerminalOutput { data, .. }
                if data.contains("cross-connection-tail")
        )),
        "final terminal output must be preserved with the process exit: {:?}",
        attached_drain.events
    );
    let drained_again = attached
        .request(&botster_hub_client::DaemonRequest::Drain {
            session_id: session_id.to_string(),
        })
        .expect("repeat drain after cross-connection shutdown");
    assert!(
        drained_again.events.iter().all(|event| !matches!(
            event,
            botster_hub_client::DaemonEvent::ProcessExit {
                subscription_id: event_subscription_id,
                ..
            } if event_subscription_id == subscription_id
        )),
        "subscription-scoped process exit must be delivered once: {:?}",
        drained_again.events
    );

    shutdown_cli_daemon(&data_dir, child);
}

#[test]
fn session_entity_subscription_observes_attached_natural_exit_with_pending_egress() {
    let _guard = daemon_test_guard();
    let data_dir = unique_test_dir("session-entity-attached-exit");
    let config = explicit_config(&data_dir);
    let endpoint = botster_hub_client::DaemonEndpoint::new(
        config
            .transports
            .local_socket
            .as_ref()
            .expect("test config has local socket")
            .path
            .clone(),
    );
    let child = start_cli_daemon(&data_dir);
    let mut subscription =
        botster_hub_client::subscribe_session_entities(&endpoint, "entities-attached-exit")
            .expect("subscribe before attached natural exit");
    subscription
        .set_read_timeout(Some(Duration::from_secs(5)))
        .expect("bound entity reads");
    let _ = subscription.next_frame().expect("initial snapshot");

    botster_hub_client::request(
        &endpoint,
        botster_hub_client::DaemonRequest::Spawn {
            session_id: "entity-attached-exit".to_string(),
            command: "sleep 0.15; printf 'pending-first\\n'; sleep 0.15; printf 'pending-second\\n'; exit 7"
                .to_string(),
        },
    )
    .expect("spawn attached natural-exit session");
    assert!(matches!(
        subscription.next_frame().expect("spawn upsert"),
        botster_hub_client::DaemonEntityFrame::Upsert { ref id, .. }
            if id == "entity-attached-exit"
    ));

    let mut terminal =
        botster_hub_client::DaemonConnection::connect(&endpoint).expect("terminal connection");
    terminal
        .request(&botster_hub_client::DaemonRequest::Attach {
            session_id: "entity-attached-exit".to_string(),
            subscription_id: "terminal-attached-exit".to_string(),
        })
        .expect("attach before output becomes pending");

    let exit_sequence = loop {
        match subscription
            .next_frame()
            .expect("natural exit delta with pending terminal egress")
        {
            botster_hub_client::DaemonEntityFrame::Patch {
                snapshot_seq,
                id,
                patch,
                ..
            } if id == "entity-attached-exit"
                && patch.get("lifecycle").and_then(serde_json::Value::as_str) == Some("exited") =>
            {
                assert_eq!(
                    patch.get("exit_code").and_then(serde_json::Value::as_i64),
                    Some(7)
                );
                break snapshot_seq;
            }
            _ => {}
        }
    };
    assert!(exit_sequence > 0);

    let retained = terminal
        .request(&botster_hub_client::DaemonRequest::Drain {
            session_id: "entity-attached-exit".to_string(),
        })
        .expect("drain retained terminal output after exit patch");
    let retained_output = retained
        .events
        .iter()
        .filter_map(|event| match event {
            botster_hub_client::DaemonEvent::TerminalOutput { data, .. } => Some(data.as_str()),
            _ => None,
        })
        .collect::<String>();
    assert_eq!(retained_output.matches("pending-first").count(), 1);
    assert_eq!(retained_output.matches("pending-second").count(), 1);
    assert!(
        retained_output.find("pending-first") < retained_output.find("pending-second"),
        "retained terminal output must preserve production order, got {retained_output:?}"
    );
    assert_eq!(
        retained
            .events
            .iter()
            .filter(|event| matches!(
                event,
                botster_hub_client::DaemonEvent::ProcessExit { code: Some(7), .. }
            ))
            .count(),
        1,
        "retained terminal events must include the process exit exactly once"
    );

    let drained_again = terminal
        .request(&botster_hub_client::DaemonRequest::Drain {
            session_id: "entity-attached-exit".to_string(),
        })
        .expect("second terminal drain after retained batch");
    assert!(
        drained_again.events.iter().all(|event| {
            !matches!(
                event,
                botster_hub_client::DaemonEvent::TerminalOutput { data, .. }
                    if data.contains("pending-first") || data.contains("pending-second")
            ) && !matches!(
                event,
                botster_hub_client::DaemonEvent::ProcessExit { code: Some(7), .. }
            )
        }),
        "retained terminal events must only be delivered once, got {:?}",
        drained_again.events
    );

    subscription
        .unsubscribe()
        .expect("unsubscribe entity stream");
    shutdown_cli_daemon(&data_dir, child);
}

#[test]
fn session_entity_subscription_recovers_after_terminal_disconnect_with_pending_egress() {
    let _guard = daemon_test_guard();
    let data_dir = unique_test_dir("entity-drop");
    let config = explicit_config(&data_dir);
    let endpoint = botster_hub_client::DaemonEndpoint::new(
        config
            .transports
            .local_socket
            .as_ref()
            .expect("test config has local socket")
            .path
            .clone(),
    );
    let child = start_cli_daemon(&data_dir);
    let mut subscription =
        botster_hub_client::subscribe_session_entities(&endpoint, "entities-terminal-disconnect")
            .expect("subscribe before terminal disconnect");
    subscription
        .set_read_timeout(Some(Duration::from_secs(5)))
        .expect("bound entity reads");
    let _ = subscription.next_frame().expect("initial snapshot");

    botster_hub_client::request(
        &endpoint,
        botster_hub_client::DaemonRequest::Spawn {
            session_id: "entity-terminal-disconnect".to_string(),
            command: "sleep 0.3; printf 'orphaned-output\\n'; sleep 0.6; exit 7".to_string(),
        },
    )
    .expect("spawn terminal disconnect session");
    assert!(matches!(
        subscription.next_frame().expect("spawn upsert"),
        botster_hub_client::DaemonEntityFrame::Upsert { ref id, .. }
            if id == "entity-terminal-disconnect"
    ));

    let mut terminal =
        botster_hub_client::DaemonConnection::connect(&endpoint).expect("terminal connection");
    terminal
        .request(&botster_hub_client::DaemonRequest::Attach {
            session_id: "entity-terminal-disconnect".to_string(),
            subscription_id: "terminal-disconnect".to_string(),
        })
        .expect("attach terminal before ungraceful disconnect");
    thread::sleep(Duration::from_millis(500));
    drop(terminal);

    let exit_sequence = loop {
        match subscription
            .next_frame()
            .expect("exit delta after terminal disconnect")
        {
            botster_hub_client::DaemonEntityFrame::Patch {
                snapshot_seq,
                patch,
                ..
            } if patch.get("lifecycle").and_then(serde_json::Value::as_str) == Some("exited") => {
                assert_eq!(
                    patch.get("exit_code").and_then(serde_json::Value::as_i64),
                    Some(7)
                );
                break snapshot_seq;
            }
            _ => {}
        }
    };

    let removed = botster_hub_client::request(
        &endpoint,
        botster_hub_client::DaemonRequest::RemoveSession {
            session_id: "entity-terminal-disconnect".to_string(),
        },
    )
    .expect("remove session after disconnected terminal exit");
    assert_eq!(
        removed.kind,
        botster_hub_client::DaemonResponseKind::SessionRemoved
    );
    assert!(matches!(
        subscription.next_frame().expect("remove delta"),
        botster_hub_client::DaemonEntityFrame::Remove {
            snapshot_seq,
            ref id,
            ..
        } if id == "entity-terminal-disconnect" && snapshot_seq > exit_sequence
    ));

    subscription
        .unsubscribe()
        .expect("unsubscribe entity stream");
    shutdown_cli_daemon(&data_dir, child);
}

#[test]
fn occupied_generic_web_port_reports_structured_entrypoint_failure() {
    let _guard = daemon_test_guard();
    let data_dir = unique_short_test_dir("web-occupied-port");
    let package_dir = unique_test_dir("web-occupied-port-package");
    write_botster_web_package(&package_dir);
    let child = start_cli_daemon(&data_dir);
    enable_supervised_package(&data_dir, &package_dir);

    let occupied = TcpListener::bind(("127.0.0.1", 0)).expect("reserve generic Web port");
    let occupied_port = occupied.local_addr().expect("occupied port address").port();
    let response = botster_hub::daemon_transport_request(
        &explicit_config(&data_dir),
        botster_hub::DaemonRequest::StartPackageEntrypoint {
            package_name: "botster-web".to_string(),
            entrypoint_id: "web-client".to_string(),
            environment_overrides: BTreeMap::from([(
                "BOTSTER_WEB_PORT".to_string(),
                occupied_port.to_string(),
            )]),
        },
    )
    .expect("occupied port returns an operator response");

    assert_eq!(
        response.kind,
        botster_hub::DaemonResponseKind::OperatorError
    );
    let error = response.error.expect("structured entrypoint error");
    assert_eq!(error.code, "entrypoint_readiness_failed");
    assert!(error.message.contains("package botster-web"));
    assert!(error.message.contains("entrypoint web-client"));
    assert!(error.message.contains("exited"));
    assert!(error.message.contains("EADDRINUSE"), "{}", error.message);

    drop(occupied);
    shutdown_cli_daemon(&data_dir, child);
}

#[test]
fn local_webrtc_chunks_oversized_encrypted_daemon_response() {
    let _guard = daemon_test_guard();
    let data_dir = unique_short_test_dir("web-webrtc");
    let package_dir = unique_test_dir("web-webrtc-package");
    write_botster_web_package(&package_dir);
    let config = explicit_config(&data_dir);
    let socket_path = config
        .transports
        .local_socket
        .as_ref()
        .expect("test config has local socket")
        .path
        .clone();
    let endpoint = botster_hub_client::DaemonEndpoint::new(socket_path);
    let child = PanicSafeCliDaemon::start_with_local_webrtc_diagnostics(&data_dir);
    enable_supervised_package(&data_dir, &package_dir);

    let web_listener_port = unused_loopback_port();
    let start = botster_hub_client::request(
        &endpoint,
        botster_hub_client::DaemonRequest::StartPackageEntrypoint {
            package_name: "botster-web".to_string(),
            entrypoint_id: "web-client".to_string(),
            environment_overrides: BTreeMap::from([(
                "BOTSTER_WEB_PORT".to_string(),
                web_listener_port.to_string(),
            )]),
        },
    )
    .expect("start botster-web entrypoint");
    assert_eq!(start.kind, botster_hub_client::DaemonResponseKind::Packages);
    let bootstrap = botster_hub_client::request(
        &endpoint,
        botster_hub_client::DaemonRequest::IssueLocalWebrtcBootstrap {
            package_name: "botster-web".to_string(),
            entrypoint_id: "web-client".to_string(),
            origin: format!("http://127.0.0.1:{web_listener_port}"),
        },
    )
    .expect("issue local WebRTC bootstrap")
    .local_webrtc_bootstrap
    .expect("bootstrap response includes local WebRTC bootstrap");
    assert_eq!(bootstrap.package_name, "botster-web");
    assert_eq!(bootstrap.entrypoint_id, "web-client");
    assert_eq!(
        bootstrap.expected_origin,
        format!("http://127.0.0.1:{web_listener_port}")
    );
    assert_eq!(bootstrap.signaling_transport, "daemon_request");
    assert_eq!(bootstrap.data_plane, "webrtc_data_channel");
    assert!(bootstrap.ordered);
    assert_eq!(bootstrap.max_retransmits, None);
    assert_eq!(bootstrap.max_packet_lifetime_ms, None);

    let stream_key = local_webrtc_stream_key(&bootstrap.grant_secret);

    block_on(async {
        let (mut offer_peer, offer) = LocalWebrtcOfferPeer::create_offer()
            .await
            .expect("create WebRTC offer peer");

        let rejected_origin = botster_hub_client::request(
            &endpoint,
            botster_hub_client::DaemonRequest::LocalWebrtcSignal {
                grant_id: bootstrap.grant_id.clone(),
                grant_secret: bootstrap.grant_secret.clone(),
                origin: "http://127.0.0.1:1".to_string(),
                offer: serde_json::Value::Null,
            },
        )
        .expect("wrong-origin signal returns operator response");
        assert_eq!(
            rejected_origin.kind,
            botster_hub_client::DaemonResponseKind::OperatorError
        );
        assert_eq!(
            rejected_origin
                .error
                .as_ref()
                .map(|error| error.code.as_str()),
            Some("local_webrtc_origin_mismatch")
        );

        let rejected_secret = botster_hub_client::request(
            &endpoint,
            botster_hub_client::DaemonRequest::LocalWebrtcSignal {
                grant_id: bootstrap.grant_id.clone(),
                grant_secret: "wrong-secret".to_string(),
                origin: bootstrap.expected_origin.clone(),
                offer: serde_json::Value::Null,
            },
        )
        .expect("wrong-secret signal returns operator response");
        assert_eq!(
            rejected_secret.kind,
            botster_hub_client::DaemonResponseKind::OperatorError
        );
        assert_eq!(
            rejected_secret
                .error
                .as_ref()
                .map(|error| error.code.as_str()),
            Some("local_webrtc_secret_mismatch")
        );

        let signal = botster_hub_client::request(
            &endpoint,
            botster_hub_client::DaemonRequest::LocalWebrtcSignal {
                grant_id: bootstrap.grant_id.clone(),
                grant_secret: bootstrap.grant_secret.clone(),
                origin: bootstrap.expected_origin.clone(),
                offer,
            },
        )
        .expect("signal local WebRTC offer");
        assert_eq!(
            signal.kind,
            botster_hub_client::DaemonResponseKind::LocalWebrtcAnswer
        );
        let answer = signal
            .local_webrtc_answer
            .as_ref()
            .expect("signal response includes WebRTC answer")
            .answer
            .clone();

        offer_peer
            .accept_answer(answer)
            .await
            .expect("offer peer accepts answer and opens channel");

        let status = offer_peer
            .encrypted_request(&stream_key, &botster_hub_client::DaemonRequest::Status)
            .await
            .expect("status over encrypted WebRTC data channel");
        assert_eq!(status.kind, botster_hub_client::DaemonResponseKind::Status);

        let list = offer_peer
            .encrypted_request(
                &stream_key,
                &botster_hub_client::DaemonRequest::ListSessions,
            )
            .await
            .expect("list sessions over encrypted WebRTC data channel");
        assert_eq!(list.kind, botster_hub_client::DaemonResponseKind::Sessions);

        let subscribed = offer_peer
            .encrypted_request(
                &stream_key,
                &botster_hub_client::DaemonRequest::SubscribeEntities {
                    entity_type: "session".to_string(),
                    subscription_id: "local-webrtc-entities".to_string(),
                },
            )
            .await
            .expect("subscribe to session entities over encrypted WebRTC data channel");
        assert_eq!(
            subscribed.kind,
            botster_hub_client::DaemonResponseKind::EntitySubscribed
        );
        assert!(matches!(
            offer_peer
                .next_entity_frame(&stream_key)
                .await
                .expect("initial WebRTC entity snapshot"),
            botster_hub_client::DaemonEntityFrame::Snapshot {
                ref subscription_id,
                ref items,
                ..
            } if subscription_id == "local-webrtc-entities" && items.is_empty()
        ));

        let spawn = botster_hub_client::request(
            &endpoint,
            botster_hub_client::DaemonRequest::Spawn {
                session_id: "local-webrtc-session".to_string(),
                command: "printf 'local-webrtc-ready\\n'; while IFS= read -r line; do printf 'webrtc:%s\\n' \"$line\"; done".to_string(),
            },
        )
        .expect("external daemon client spawns a session visible over WebRTC");
        assert_eq!(spawn.kind, botster_hub_client::DaemonResponseKind::Spawned);
        assert!(matches!(
            offer_peer
                .next_entity_frame(&stream_key)
                .await
                .expect("spawn upsert over WebRTC entity delivery"),
            botster_hub_client::DaemonEntityFrame::Upsert { ref id, .. }
                if id == "local-webrtc-session"
        ));

        let attach = offer_peer
            .encrypted_request(
                &stream_key,
                &botster_hub_client::DaemonRequest::Attach {
                    session_id: "local-webrtc-session".to_string(),
                    subscription_id: "local-webrtc-subscription".to_string(),
                },
            )
            .await
            .expect("attach over encrypted WebRTC data channel");
        assert_eq!(attach.kind, botster_hub_client::DaemonResponseKind::Events);

        let resize = offer_peer
            .encrypted_request(
                &stream_key,
                &botster_hub_client::DaemonRequest::Resize {
                    session_id: "local-webrtc-session".to_string(),
                    rows: 33,
                    cols: 111,
                },
            )
            .await
            .expect("resize over encrypted WebRTC data channel");
        assert_eq!(resize.kind, botster_hub_client::DaemonResponseKind::Events);

        let send = offer_peer
            .encrypted_request(
                &stream_key,
                &botster_hub_client::DaemonRequest::SendInput {
                    session_id: "local-webrtc-session".to_string(),
                    data: "from-local-webrtc\n".to_string(),
                },
            )
            .await
            .expect("send input over encrypted WebRTC data channel");
        assert_eq!(send.kind, botster_hub_client::DaemonResponseKind::Events);

        let mut observed = String::new();
        for _ in 0..120 {
            let drain = offer_peer
                .encrypted_request(
                    &stream_key,
                    &botster_hub_client::DaemonRequest::Drain {
                        session_id: "local-webrtc-session".to_string(),
                    },
                )
                .await
                .expect("drain over encrypted WebRTC data channel");
            for event in drain.events {
                if let botster_hub_client::DaemonEvent::TerminalOutput { data, .. } = event {
                    observed.push_str(&data);
                }
            }
            if observed.contains("webrtc:from-local-webrtc") {
                break;
            }
            sleep(Duration::from_millis(30)).await;
        }
        assert!(
            observed.contains("webrtc:from-local-webrtc"),
            "encrypted WebRTC data channel should drain session output, got {observed:?}"
        );

        let created = botster_hub_client::request(
            &endpoint,
            botster_hub_client::DaemonRequest::CreateSpawnTarget {
                target_id: Some("local-webrtc-large-target".to_string()),
                label: Some("Local WebRTC oversized response".to_string()),
                root: data_dir.clone(),
                enabled: true,
                kind: Some("directory".to_string()),
                base_ref: None,
                metadata: BTreeMap::from([("synthetic".to_string(), "x".repeat(300_000))]),
            },
        )
        .expect("seed synthetic oversized response through daemon socket");
        assert_eq!(
            created.kind,
            botster_hub_client::DaemonResponseKind::SpawnTargets
        );
        let (large_response, metrics) = offer_peer
            .encrypted_request_with_metrics(
                &stream_key,
                &botster_hub_client::DaemonRequest::ListSpawnTargets,
            )
            .await
            .expect("list oversized spawn-target response over encrypted WebRTC");
        assert_eq!(
            large_response.kind,
            botster_hub_client::DaemonResponseKind::SpawnTargets
        );
        assert_eq!(
            large_response
                .spawn_targets
                .iter()
                .find(|target| target.target_id == "local-webrtc-large-target")
                .and_then(|target| target.metadata.get("synthetic"))
                .map(String::len),
            Some(300_000)
        );
        assert!(metrics.envelope_bytes > 256 * 1024);
        assert!(metrics.chunk_count > 1);
        assert!(metrics.maximum_frame_bytes < botster_hub_client::LOCAL_WEBRTC_MAX_FRAME_BYTES);

        let shutdown = offer_peer
            .encrypted_request(
                &stream_key,
                &botster_hub_client::DaemonRequest::ShutdownSession {
                    session_id: "local-webrtc-session".to_string(),
                },
            )
            .await
            .expect("shutdown over encrypted WebRTC data channel");
        assert_eq!(
            shutdown.kind,
            botster_hub_client::DaemonResponseKind::Events
        );
        loop {
            if matches!(
                offer_peer
                    .next_entity_frame(&stream_key)
                    .await
                    .expect("lifecycle patch over WebRTC entity delivery"),
                botster_hub_client::DaemonEntityFrame::Patch {
                    ref id,
                    ref patch,
                    ..
                } if id == "local-webrtc-session"
                    && patch.get("lifecycle").and_then(serde_json::Value::as_str)
                        == Some("exited")
            ) {
                break;
            }
        }
        let removed = offer_peer
            .encrypted_request(
                &stream_key,
                &botster_hub_client::DaemonRequest::RemoveSession {
                    session_id: "local-webrtc-session".to_string(),
                },
            )
            .await
            .expect("remove session while WebRTC entity subscription is active");
        assert_eq!(
            removed.kind,
            botster_hub_client::DaemonResponseKind::SessionRemoved
        );
        loop {
            if matches!(
                offer_peer
                    .next_entity_frame(&stream_key)
                    .await
                    .expect("remove frame over WebRTC entity delivery"),
                botster_hub_client::DaemonEntityFrame::Remove { ref id, .. }
                    if id == "local-webrtc-session"
            ) {
                break;
            }
        }
        offer_peer
            .data_channel
            .send_text("invalid-encrypted-request")
            .await
            .expect("send terminal invalid request to prove fail-closed cleanup");
        sleep(Duration::from_millis(100)).await;
        let _ = offer_peer.data_channel.close().await;
        offer_peer.peer.close().await.expect("close offer peer");
    });

    let cleanup_deadline = Instant::now() + Duration::from_secs(5);
    let cleanup_subscription = loop {
        match botster_hub_client::subscribe_session_entities(&endpoint, "local-webrtc-entities") {
            Ok(subscription) => break subscription,
            Err(error) if Instant::now() < cleanup_deadline => {
                let _ = error;
                thread::sleep(Duration::from_millis(25));
            }
            Err(error) => {
                panic!("WebRTC peer cleanup did not release entity subscription: {error}")
            }
        }
    };
    cleanup_subscription
        .unsubscribe()
        .expect("cleanup proof subscription unsubscribes");

    let reused = botster_hub_client::request(
        &endpoint,
        botster_hub_client::DaemonRequest::LocalWebrtcSignal {
            grant_id: bootstrap.grant_id.clone(),
            grant_secret: bootstrap.grant_secret.clone(),
            origin: bootstrap.expected_origin.clone(),
            offer: serde_json::Value::Null,
        },
    )
    .expect("reused grant returns operator response");
    assert_eq!(
        reused.kind,
        botster_hub_client::DaemonResponseKind::OperatorError
    );
    assert_eq!(
        reused.error.as_ref().map(|error| error.code.as_str()),
        Some("local_webrtc_redeemed_grant")
    );

    let persisted_state =
        fs::read_to_string(data_dir.join("hub-state.json")).expect("read hub state");
    assert!(!persisted_state.contains(&bootstrap.grant_id));
    assert!(!persisted_state.contains(&bootstrap.grant_secret));
    assert!(!persisted_state.contains("grant_secret"));
    child.shutdown();
}

#[test]
fn botster_web_same_url_reload_issues_fresh_local_webrtc_bootstrap() {
    let _guard = daemon_test_guard();
    let test_started = Instant::now();
    let data_dir = unique_short_test_dir("web-webrtc-reload");
    let package_dir = unique_test_dir("web-webrtc-reload-package");
    write_botster_web_package(&package_dir);
    log_botster_web_phase(test_started, "fixture_built");
    let config = explicit_config(&data_dir);
    let socket_path = config
        .transports
        .local_socket
        .as_ref()
        .expect("test config has local socket")
        .path
        .clone();
    let endpoint = botster_hub_client::DaemonEndpoint::new(socket_path);
    let child = start_cli_daemon(&data_dir);
    log_botster_web_phase(test_started, "daemon_started");
    enable_supervised_package(&data_dir, &package_dir);
    log_botster_web_phase(test_started, "package_enabled");

    let web_listener_port = unused_loopback_port();
    let start = botster_hub_client::request(
        &endpoint,
        botster_hub_client::DaemonRequest::StartPackageEntrypoint {
            package_name: "botster-web".to_string(),
            entrypoint_id: "web-client".to_string(),
            environment_overrides: BTreeMap::from([
                (
                    "BOTSTER_WEB_PORT".to_string(),
                    web_listener_port.to_string(),
                ),
                (
                    "BOTSTER_WEB_TEST_STARTUP_DELAY_MS".to_string(),
                    BOTSTER_WEB_READINESS_STARTUP_DELAY_MS.to_string(),
                ),
            ]),
        },
    )
    .expect("start botster-web entrypoint");
    assert_eq!(start.kind, botster_hub_client::DaemonResponseKind::Packages);
    log_botster_web_phase(test_started, "entrypoint_start_returned");
    let web_origin = format!("http://127.0.0.1:{web_listener_port}");
    let expected_local_url = format!("{web_origin}/");
    let apps =
        wait_for_botster_web_readiness(&endpoint, &web_origin, &expected_local_url, test_started);
    assert_eq!(
        app_row(&apps, "web-client")
            .launch_target
            .local_url
            .as_deref(),
        Some(expected_local_url.as_str())
    );

    let wrong_origin = botster_hub_client::request(
        &endpoint,
        botster_hub_client::DaemonRequest::IssueLocalWebrtcBootstrap {
            package_name: "botster-web".to_string(),
            entrypoint_id: "web-client".to_string(),
            origin: "http://127.0.0.1:1".to_string(),
        },
    )
    .expect("wrong-origin bootstrap issuance returns operator response");
    assert_eq!(
        wrong_origin.kind,
        botster_hub_client::DaemonResponseKind::OperatorError
    );
    assert_eq!(
        wrong_origin.error.as_ref().map(|error| error.code.as_str()),
        Some("local_webrtc_bootstrap_origin_mismatch")
    );

    let bootstrap_a = botster_web_page_bootstrap(&web_origin);
    let bootstrap_b = botster_web_page_bootstrap(&web_origin);
    let bootstrap_c = botster_web_page_bootstrap(&web_origin);
    assert_eq!(bootstrap_a.package_name, "botster-web");
    assert_eq!(bootstrap_a.entrypoint_id, "web-client");
    assert_eq!(bootstrap_a.expected_origin, web_origin);
    assert_eq!(bootstrap_b.expected_origin, bootstrap_a.expected_origin);
    assert_ne!(bootstrap_a.grant_id, bootstrap_b.grant_id);
    assert_ne!(bootstrap_a.grant_secret, bootstrap_b.grant_secret);
    assert_ne!(bootstrap_b.grant_id, bootstrap_c.grant_id);
    assert_ne!(bootstrap_b.grant_secret, bootstrap_c.grant_secret);

    block_on(async {
        let (mut first_peer, first_key) = open_local_webrtc_peer(&endpoint, &bootstrap_a).await;
        let status = first_peer
            .encrypted_request(&first_key, &botster_hub_client::DaemonRequest::Status)
            .await
            .expect("status over first encrypted WebRTC data channel");
        assert_eq!(status.kind, botster_hub_client::DaemonResponseKind::Status);
        let subscribed = first_peer
            .encrypted_request(
                &first_key,
                &botster_hub_client::DaemonRequest::SubscribeEntities {
                    entity_type: "session".to_string(),
                    subscription_id: "reload-entities-generation-1".to_string(),
                },
            )
            .await
            .expect("subscribe on first WebRTC generation");
        assert_eq!(
            subscribed.kind,
            botster_hub_client::DaemonResponseKind::EntitySubscribed
        );
        assert!(matches!(
            first_peer
                .next_entity_frame(&first_key)
                .await
                .expect("first generation snapshot"),
            botster_hub_client::DaemonEntityFrame::Snapshot { ref items, .. }
                if items.is_empty()
        ));
        let spawn = first_peer
            .encrypted_request(
                &first_key,
                &botster_hub_client::DaemonRequest::Spawn {
                    session_id: "local-webrtc-reload-session".to_string(),
                    command: "printf 'reload-ready\\n'; while IFS= read -r line; do printf 'reload:%s\\n' \"$line\"; done".to_string(),
                },
            )
            .await
            .expect("spawn over first encrypted WebRTC data channel");
        assert_eq!(spawn.kind, botster_hub_client::DaemonResponseKind::Spawned);
        first_peer
            .data_channel
            .close()
            .await
            .expect("close first generation data channel");
        first_peer.peer.close().await.expect("close first peer");

        let rejected_secret = botster_hub_client::request(
            &endpoint,
            botster_hub_client::DaemonRequest::LocalWebrtcSignal {
                grant_id: bootstrap_b.grant_id.clone(),
                grant_secret: "wrong-secret".to_string(),
                origin: bootstrap_b.expected_origin.clone(),
                offer: serde_json::Value::Null,
            },
        )
        .expect("wrong-secret reload signal returns operator response");
        assert_eq!(
            rejected_secret.kind,
            botster_hub_client::DaemonResponseKind::OperatorError
        );
        assert_eq!(
            rejected_secret
                .error
                .as_ref()
                .map(|error| error.code.as_str()),
            Some("local_webrtc_secret_mismatch")
        );

        let (mut reload_peer, reload_key) = open_local_webrtc_peer(&endpoint, &bootstrap_b).await;
        let status = reload_peer
            .encrypted_request(&reload_key, &botster_hub_client::DaemonRequest::Status)
            .await
            .expect("status over reload encrypted WebRTC data channel");
        assert_eq!(status.kind, botster_hub_client::DaemonResponseKind::Status);
        let subscribed = reload_peer
            .encrypted_request(
                &reload_key,
                &botster_hub_client::DaemonRequest::SubscribeEntities {
                    entity_type: "session".to_string(),
                    subscription_id: "reload-entities-generation-2".to_string(),
                },
            )
            .await
            .expect("subscribe on second WebRTC generation");
        assert_eq!(
            subscribed.kind,
            botster_hub_client::DaemonResponseKind::EntitySubscribed
        );
        assert!(matches!(
            reload_peer
                .next_entity_frame(&reload_key)
                .await
                .expect("second generation fresh snapshot"),
            botster_hub_client::DaemonEntityFrame::Snapshot { ref items, .. }
                if items.iter().any(|item| item.session_uuid == "local-webrtc-reload-session")
        ));
        let generation_two_shutdown = reload_peer
            .encrypted_request(
                &reload_key,
                &botster_hub_client::DaemonRequest::ShutdownSession {
                    session_id: "local-webrtc-reload-session".to_string(),
                },
            )
            .await
            .expect("emit a lifecycle delta on the second WebRTC generation");
        assert_eq!(
            generation_two_shutdown.kind,
            botster_hub_client::DaemonResponseKind::Events
        );
        loop {
            if matches!(
                reload_peer
                    .next_entity_frame(&reload_key)
                    .await
                    .expect("current second-generation lifecycle delta"),
                botster_hub_client::DaemonEntityFrame::Patch {
                    ref subscription_id,
                    ref id,
                    ref patch,
                    ..
                } if subscription_id == "reload-entities-generation-2"
                    && id == "local-webrtc-reload-session"
                    && patch.get("lifecycle").and_then(serde_json::Value::as_str)
                        == Some("exited")
            ) {
                break;
            }
        }
        let sessions = reload_peer
            .encrypted_request(
                &reload_key,
                &botster_hub_client::DaemonRequest::ListSessions,
            )
            .await
            .expect("list sessions over reload encrypted WebRTC data channel");
        assert_eq!(
            sessions.kind,
            botster_hub_client::DaemonResponseKind::Sessions
        );
        assert!(
            sessions
                .sessions
                .iter()
                .any(|session| session.session_id == "local-webrtc-reload-session"),
            "reload DataChannel should hydrate existing sessions"
        );
        reload_peer
            .data_channel
            .close()
            .await
            .expect("close second generation data channel");
        reload_peer.peer.close().await.expect("close reload peer");

        let (mut final_peer, final_key) = open_local_webrtc_peer(&endpoint, &bootstrap_c).await;
        let subscribed = final_peer
            .encrypted_request(
                &final_key,
                &botster_hub_client::DaemonRequest::SubscribeEntities {
                    entity_type: "session".to_string(),
                    subscription_id: "reload-entities-generation-3".to_string(),
                },
            )
            .await
            .expect("subscribe on third WebRTC generation");
        assert_eq!(
            subscribed.kind,
            botster_hub_client::DaemonResponseKind::EntitySubscribed
        );
        assert!(matches!(
            final_peer
                .next_entity_frame(&final_key)
                .await
                .expect("third generation fresh snapshot"),
            botster_hub_client::DaemonEntityFrame::Snapshot { ref items, .. }
                if items.iter().any(|item| item.session_uuid == "local-webrtc-reload-session")
        ));
        let current_generation_remove = final_peer
            .encrypted_request(
                &final_key,
                &botster_hub_client::DaemonRequest::RemoveSession {
                    session_id: "local-webrtc-reload-session".to_string(),
                },
            )
            .await
            .expect("emit a lifecycle delta on the third WebRTC generation");
        assert_eq!(
            current_generation_remove.kind,
            botster_hub_client::DaemonResponseKind::SessionRemoved
        );
        loop {
            if matches!(
                final_peer
                    .next_entity_frame(&final_key)
                    .await
                    .expect("current third-generation lifecycle delta"),
                botster_hub_client::DaemonEntityFrame::Remove {
                    ref subscription_id,
                    ref id,
                    ..
                } if subscription_id == "reload-entities-generation-3"
                    && id == "local-webrtc-reload-session"
            ) {
                break;
            }
        }
        let status = final_peer
            .encrypted_request(&final_key, &botster_hub_client::DaemonRequest::Status)
            .await
            .expect("ordinary request on third WebRTC generation");
        assert_eq!(status.kind, botster_hub_client::DaemonResponseKind::Status);
        final_peer
            .data_channel
            .close()
            .await
            .expect("close third generation data channel");
        final_peer.peer.close().await.expect("close final peer");
    });

    let reused = botster_hub_client::request(
        &endpoint,
        botster_hub_client::DaemonRequest::LocalWebrtcSignal {
            grant_id: bootstrap_a.grant_id.clone(),
            grant_secret: bootstrap_a.grant_secret.clone(),
            origin: bootstrap_a.expected_origin.clone(),
            offer: serde_json::Value::Null,
        },
    )
    .expect("reused first page-load grant returns operator response");
    assert_eq!(
        reused.kind,
        botster_hub_client::DaemonResponseKind::OperatorError
    );
    assert_eq!(
        reused.error.as_ref().map(|error| error.code.as_str()),
        Some("local_webrtc_redeemed_grant")
    );

    let persisted_state =
        fs::read_to_string(data_dir.join("hub-state.json")).expect("read hub state");
    for secret in [
        bootstrap_a.grant_id.as_str(),
        bootstrap_a.grant_secret.as_str(),
        bootstrap_b.grant_id.as_str(),
        bootstrap_b.grant_secret.as_str(),
        bootstrap_c.grant_id.as_str(),
        bootstrap_c.grant_secret.as_str(),
    ] {
        assert!(!persisted_state.contains(secret));
    }
    assert!(!persisted_state.contains("grant_secret"));

    shutdown_cli_daemon(&data_dir, child);
}

#[test]
fn local_webrtc_peer_close_detaches_terminal_subscriptions() {
    let _guard = daemon_test_guard();
    let data_dir = unique_short_test_dir("web-webrtc-close");
    let package_dir = unique_test_dir("web-webrtc-close-package");
    write_botster_web_package(&package_dir);
    let config = explicit_config(&data_dir);
    let socket_path = config
        .transports
        .local_socket
        .as_ref()
        .expect("test config has local socket")
        .path
        .clone();
    let endpoint = botster_hub_client::DaemonEndpoint::new(socket_path);
    let child = start_cli_daemon(&data_dir);
    enable_supervised_package(&data_dir, &package_dir);

    let web_listener_port = unused_loopback_port();
    let start = botster_hub_client::request(
        &endpoint,
        botster_hub_client::DaemonRequest::StartPackageEntrypoint {
            package_name: "botster-web".to_string(),
            entrypoint_id: "web-client".to_string(),
            environment_overrides: BTreeMap::from([(
                "BOTSTER_WEB_PORT".to_string(),
                web_listener_port.to_string(),
            )]),
        },
    )
    .expect("start botster-web entrypoint");
    assert_eq!(start.kind, botster_hub_client::DaemonResponseKind::Packages);
    let bootstrap = botster_hub_client::request(
        &endpoint,
        botster_hub_client::DaemonRequest::IssueLocalWebrtcBootstrap {
            package_name: "botster-web".to_string(),
            entrypoint_id: "web-client".to_string(),
            origin: format!("http://127.0.0.1:{web_listener_port}"),
        },
    )
    .expect("issue local WebRTC bootstrap")
    .local_webrtc_bootstrap
    .expect("bootstrap response includes local WebRTC bootstrap");
    let stream_key = local_webrtc_stream_key(&bootstrap.grant_secret);

    block_on(async {
        let (mut offer_peer, offer) = LocalWebrtcOfferPeer::create_offer()
            .await
            .expect("create WebRTC offer peer");
        let signal = botster_hub_client::request(
            &endpoint,
            botster_hub_client::DaemonRequest::LocalWebrtcSignal {
                grant_id: bootstrap.grant_id.clone(),
                grant_secret: bootstrap.grant_secret.clone(),
                origin: bootstrap.expected_origin.clone(),
                offer,
            },
        )
        .expect("signal local WebRTC offer");
        let answer = signal
            .local_webrtc_answer
            .as_ref()
            .expect("signal response includes WebRTC answer")
            .answer
            .clone();
        offer_peer
            .accept_answer(answer)
            .await
            .expect("offer peer accepts answer and opens channel");

        let spawn = offer_peer
            .encrypted_request(
                &stream_key,
                &botster_hub_client::DaemonRequest::Spawn {
                    session_id: "local-webrtc-drop-session".to_string(),
                    command: "printf 'local-webrtc-drop-ready\\n'; while IFS= read -r line; do printf 'drop:%s\\n' \"$line\"; done".to_string(),
                },
            )
            .await
            .expect("spawn over encrypted WebRTC data channel");
        assert_eq!(spawn.kind, botster_hub_client::DaemonResponseKind::Spawned);

        let attach = offer_peer
            .encrypted_request(
                &stream_key,
                &botster_hub_client::DaemonRequest::Attach {
                    session_id: "local-webrtc-drop-session".to_string(),
                    subscription_id: "local-webrtc-drop-subscription".to_string(),
                },
            )
            .await
            .expect("attach over encrypted WebRTC data channel");
        assert_eq!(attach.kind, botster_hub_client::DaemonResponseKind::Events);

        offer_peer.peer.close().await.expect("close offer peer");
    });

    thread::sleep(Duration::from_millis(300));

    let mut connection =
        botster_hub_client::DaemonConnection::connect(&endpoint).expect("external connect");
    let socket_attach = connection
        .request(&botster_hub_client::DaemonRequest::Attach {
            session_id: "local-webrtc-drop-session".to_string(),
            subscription_id: "socket-after-webrtc-close-subscription".to_string(),
        })
        .expect("attach socket client after WebRTC peer close");
    assert_eq!(
        socket_attach.kind,
        botster_hub_client::DaemonResponseKind::Events
    );
    let send = connection
        .request(&botster_hub_client::DaemonRequest::SendInput {
            session_id: "local-webrtc-drop-session".to_string(),
            data: "after-webrtc-close\n".to_string(),
        })
        .expect("send input after WebRTC peer close");
    assert_eq!(send.kind, botster_hub_client::DaemonResponseKind::Events);

    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    let mut observed = String::new();
    let mut events_after_close = Vec::new();
    while std::time::Instant::now() < deadline {
        let drain = connection
            .request(&botster_hub_client::DaemonRequest::Drain {
                session_id: "local-webrtc-drop-session".to_string(),
            })
            .expect("drain after WebRTC peer close");
        for event in drain.events {
            if let botster_hub_client::DaemonEvent::TerminalOutput {
                data,
                subscription_id,
                ..
            } = &event
                && subscription_id == "socket-after-webrtc-close-subscription"
            {
                observed.push_str(data);
            }
            events_after_close.push(event);
        }
        if observed.contains("drop:after-webrtc-close") {
            break;
        }
        thread::sleep(Duration::from_millis(30));
    }
    assert!(
        observed.contains("drop:after-webrtc-close"),
        "socket client should observe output after WebRTC close, got {observed:?}"
    );
    assert!(
        events_after_close.iter().all(|event| {
            !matches!(
                event,
                botster_hub_client::DaemonEvent::TerminalOutput {
                    subscription_id,
                    data,
                    ..
                } if subscription_id == "local-webrtc-drop-subscription"
                    && data.contains("drop:after-webrtc-close")
            )
        }),
        "closed WebRTC peer subscription must not receive later output: {events_after_close:?}"
    );

    let shutdown_session = connection
        .request(&botster_hub_client::DaemonRequest::ShutdownSession {
            session_id: "local-webrtc-drop-session".to_string(),
        })
        .expect("shutdown drop test session");
    assert_eq!(
        shutdown_session.kind,
        botster_hub_client::DaemonResponseKind::Events
    );
    shutdown_cli_daemon(&data_dir, child);
}

#[test]
fn external_hub_client_spawns_botster_web_runtime_session_request_shape() {
    let _guard = daemon_test_guard();
    let data_dir = unique_short_test_dir("web-spawn");
    let config = explicit_config(&data_dir);
    let socket_path = config
        .transports
        .local_socket
        .as_ref()
        .expect("test config has local socket")
        .path
        .clone();
    let endpoint = botster_hub_client::DaemonEndpoint::new(socket_path);
    let child = start_cli_daemon(&data_dir);

    let mut connection =
        botster_hub_client::DaemonConnection::connect(&endpoint).expect("external connect");
    let spawn = connection
        .request(&botster_hub_client::DaemonRequest::Spawn {
            session_id: "botster-web-runtime-session".to_string(),
            command:
                "printf 'botster-web-runtime-ready\\n'; while IFS= read -r line; do printf 'web:%s\\n' \"$line\"; done"
                    .to_string(),
        })
        .expect("botster-web runtime spawn request");
    assert_eq!(spawn.kind, botster_hub_client::DaemonResponseKind::Spawned);
    assert!(spawn.sessions.iter().any(|session| session.session_id
        == "botster-web-runtime-session"
        && session.lifecycle == "running"));

    let list = connection
        .request(&botster_hub_client::DaemonRequest::ListSessions)
        .expect("list sessions after botster-web runtime spawn");
    assert_eq!(list.kind, botster_hub_client::DaemonResponseKind::Sessions);
    assert!(list.sessions.iter().any(|session| session.session_id
        == "botster-web-runtime-session"
        && session.lifecycle == "running"));

    let packages = connection
        .request(&botster_hub_client::DaemonRequest::ListPackages)
        .expect("list packages remains observable after botster-web runtime spawn");
    assert_eq!(
        packages.kind,
        botster_hub_client::DaemonResponseKind::Packages
    );

    let attach = connection
        .request(&botster_hub_client::DaemonRequest::Attach {
            session_id: "botster-web-runtime-session".to_string(),
            subscription_id: "botster-web-runtime-subscription".to_string(),
        })
        .expect("attach botster-web runtime session");
    assert_eq!(attach.kind, botster_hub_client::DaemonResponseKind::Events);

    let send = connection
        .request(&botster_hub_client::DaemonRequest::SendInput {
            session_id: "botster-web-runtime-session".to_string(),
            data: "from-web-action\n".to_string(),
        })
        .expect("send input to botster-web runtime session");
    assert_eq!(send.kind, botster_hub_client::DaemonResponseKind::Events);

    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    let mut observed = String::new();
    while std::time::Instant::now() < deadline {
        let drain = connection
            .request(&botster_hub_client::DaemonRequest::Drain {
                session_id: "botster-web-runtime-session".to_string(),
            })
            .expect("drain botster-web runtime session");
        for event in drain.events {
            if let botster_hub_client::DaemonEvent::TerminalOutput { data, .. } = event {
                observed.push_str(&data);
            }
        }
        if observed.contains("web:from-web-action") {
            break;
        }
        thread::sleep(Duration::from_millis(30));
    }
    assert!(
        observed.contains("web:from-web-action"),
        "botster-web runtime request shape should attach and drain output, got {observed:?}"
    );

    let shutdown_session = connection
        .request(&botster_hub_client::DaemonRequest::ShutdownSession {
            session_id: "botster-web-runtime-session".to_string(),
        })
        .expect("shutdown botster-web runtime session");
    assert_eq!(
        shutdown_session.kind,
        botster_hub_client::DaemonResponseKind::Events
    );
    shutdown_cli_daemon(&data_dir, child);
}

#[test]
fn external_hub_client_duplicate_botster_web_runtime_spawn_is_rejected_without_cleanup() {
    let _guard = daemon_test_guard();
    let data_dir = unique_short_test_dir("web-duplicate");
    let config = explicit_config(&data_dir);
    let socket_path = config
        .transports
        .local_socket
        .as_ref()
        .expect("test config has local socket")
        .path
        .clone();
    let endpoint = botster_hub_client::DaemonEndpoint::new(socket_path);
    let child = start_cli_daemon(&data_dir);

    let mut connection =
        botster_hub_client::DaemonConnection::connect(&endpoint).expect("external connect");
    let first_spawn = connection
        .request(&botster_hub_client::DaemonRequest::Spawn {
            session_id: "botster-web-runtime-session".to_string(),
            command:
                "printf 'botster-web-runtime-ready\\n'; while IFS= read -r line; do printf 'web:%s\\n' \"$line\"; done"
                    .to_string(),
        })
        .expect("first botster-web runtime spawn request");
    assert_eq!(
        first_spawn.kind,
        botster_hub_client::DaemonResponseKind::Spawned
    );

    let duplicate = connection
        .request(&botster_hub_client::DaemonRequest::Spawn {
            session_id: "botster-web-runtime-session".to_string(),
            command: "printf 'replacement-should-not-start\\n'".to_string(),
        })
        .expect("duplicate botster-web runtime spawn should return operator frame");
    assert_eq!(
        duplicate.kind,
        botster_hub_client::DaemonResponseKind::OperatorError
    );
    let error = duplicate.error.as_ref().expect("operator error body");
    assert_eq!(
        error.code, "session_already_exists",
        "unexpected duplicate spawn operator error: {error:?} diagnostics={:?}",
        duplicate.diagnostics
    );
    assert_eq!(error.operation, "spawn");
    assert!(
        duplicate.diagnostics.iter().any(|diagnostic| {
            diagnostic.kind == botster_hub_client::DaemonDiagnosticKind::ActionFailure
                && diagnostic.operation.as_deref() == Some("spawn")
                && diagnostic
                    .message
                    .as_deref()
                    .is_some_and(|message| message.contains("already exists"))
        }),
        "duplicate spawn should carry a session_already_exists diagnostic row, got {:?}",
        duplicate.diagnostics
    );

    let attach = connection
        .request(&botster_hub_client::DaemonRequest::Attach {
            session_id: "botster-web-runtime-session".to_string(),
            subscription_id: "botster-web-runtime-duplicate-subscription".to_string(),
        })
        .expect("attach original botster-web runtime session after duplicate rejection");
    assert_eq!(attach.kind, botster_hub_client::DaemonResponseKind::Events);

    let send = connection
        .request(&botster_hub_client::DaemonRequest::SendInput {
            session_id: "botster-web-runtime-session".to_string(),
            data: "after-duplicate\n".to_string(),
        })
        .expect("existing session remains writable after duplicate rejection");
    assert_eq!(send.kind, botster_hub_client::DaemonResponseKind::Events);

    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    let mut observed = String::new();
    while std::time::Instant::now() < deadline {
        let drain = connection
            .request(&botster_hub_client::DaemonRequest::Drain {
                session_id: "botster-web-runtime-session".to_string(),
            })
            .expect("drain original botster-web runtime session after duplicate rejection");
        for event in drain.events {
            if let botster_hub_client::DaemonEvent::TerminalOutput { data, .. } = event {
                observed.push_str(&data);
            }
        }
        if observed.contains("web:after-duplicate") {
            break;
        }
        thread::sleep(Duration::from_millis(30));
    }
    assert!(
        observed.contains("web:after-duplicate"),
        "duplicate rejection must not clean up or replace the existing session, got {observed:?}"
    );
    assert!(
        !observed.contains("replacement-should-not-start"),
        "duplicate rejected spawn command must not start, got {observed:?}"
    );

    let debug = format!("{error:?} {:?}", duplicate.diagnostics);
    assert!(!debug.contains(&data_dir.to_string_lossy().to_string()));
    assert!(!debug.contains(concat!("/", "Users", "/")));
    assert!(!debug.contains("/home/"));

    let shutdown_session = connection
        .request(&botster_hub_client::DaemonRequest::ShutdownSession {
            session_id: "botster-web-runtime-session".to_string(),
        })
        .expect("shutdown botster-web runtime session");
    assert_eq!(
        shutdown_session.kind,
        botster_hub_client::DaemonResponseKind::Events
    );
    shutdown_cli_daemon(&data_dir, child);
}

#[test]
fn external_hub_client_spawn_failure_returns_actionable_diagnostics() {
    let _guard = daemon_test_guard();
    let data_dir = unique_short_test_dir("spawn-fail");
    let bad_worker = data_dir.join("missing-botster-session-worker");
    let config = explicit_config(&data_dir);
    let socket_path = config
        .transports
        .local_socket
        .as_ref()
        .expect("test config has local socket")
        .path
        .clone();
    let endpoint = botster_hub_client::DaemonEndpoint::new(socket_path);
    let child = start_cli_daemon_with_session_worker(&data_dir, &bad_worker);

    let mut connection =
        botster_hub_client::DaemonConnection::connect(&endpoint).expect("external connect");
    let spawn = connection
        .request(&botster_hub_client::DaemonRequest::Spawn {
            session_id: "botster-web-runtime-session".to_string(),
            command: "printf 'should-not-start\\n'".to_string(),
        })
        .expect("spawn failure should return operator frame");
    assert_eq!(
        spawn.kind,
        botster_hub_client::DaemonResponseKind::OperatorError
    );
    let error = spawn.error.as_ref().expect("operator error body");
    assert_eq!(
        error.code, "spawn_failed",
        "unexpected spawn operator error: {error:?} diagnostics={:?}",
        spawn.diagnostics
    );
    assert_eq!(error.operation, "spawn");
    assert!(
        spawn.diagnostics.iter().any(|diagnostic| {
            diagnostic.kind == botster_hub_client::DaemonDiagnosticKind::ActionFailure
                && diagnostic.operation.as_deref() == Some("spawn")
                && diagnostic
                    .message
                    .as_deref()
                    .is_some_and(|message| message.contains("session worker"))
        }),
        "spawn failure should carry an actionable diagnostic row, got {:?}",
        spawn.diagnostics
    );
    assert!(!has_diagnostic_kind(
        &spawn.diagnostics,
        botster_hub_client::DaemonDiagnosticKind::Connected
    ));
    let debug = format!("{error:?} {:?}", spawn.diagnostics);
    assert!(!debug.contains(&data_dir.to_string_lossy().to_string()));
    assert!(!debug.contains(&bad_worker.to_string_lossy().to_string()));
    assert!(!debug.contains(concat!("/", "Users", "/")));
    assert!(!debug.contains("/home/"));

    let status = connection
        .request(&botster_hub_client::DaemonRequest::Status)
        .expect("daemon remains responsive after spawn failure");
    assert_eq!(status.kind, botster_hub_client::DaemonResponseKind::Status);
    shutdown_cli_daemon(&data_dir, child);
}

#[test]
fn external_hub_client_reports_compatibility_descriptor_and_mismatch_diagnostics() {
    let _guard = daemon_test_guard();
    let data_dir = unique_test_dir("compat");
    let config = explicit_config(&data_dir);
    let socket_path = config
        .transports
        .local_socket
        .as_ref()
        .expect("test config has local socket")
        .path
        .clone();
    let endpoint = botster_hub_client::DaemonEndpoint::new(socket_path.clone());
    let child = start_cli_daemon(&data_dir);

    let mut stream = UnixStream::connect(&socket_path).expect("connect raw compatibility socket");
    botster_hub_client::write_frame(
        &mut stream,
        &botster_hub_client::DaemonHello {
            protocol: botster_hub_client::PROTOCOL.to_string(),
            compatibility: botster_hub_client::DaemonCompatibilityRequirement::current(),
        },
    )
    .expect("write hello");
    let ack: botster_hub_client::DaemonHelloAck =
        botster_hub_client::read_frame(&mut stream).expect("read hello ack");
    assert_eq!(ack.protocol, botster_hub_client::PROTOCOL);
    assert!(ack.diagnostics.iter().any(|diagnostic| {
        diagnostic.kind == botster_hub_client::DaemonDiagnosticKind::Connected
            && diagnostic.operation.as_deref() == Some("hello")
    }));
    assert!(!has_failure_diagnostic(&ack.diagnostics));
    assert_eq!(ack.compatibility.protocol, botster_hub_client::PROTOCOL);
    assert_eq!(
        ack.compatibility.protocol_version,
        botster_hub_client::PROTOCOL_VERSION
    );
    assert!(
        ack.compatibility
            .supports_feature(botster_hub_client::FEATURE_SESSIONS)
    );
    assert!(
        ack.compatibility
            .supports_feature(botster_hub_client::FEATURE_TERMINAL_STREAMING)
    );
    assert!(
        ack.compatibility
            .supports_feature(botster_hub_client::FEATURE_RESIZE)
    );
    assert!(
        ack.compatibility
            .supports_feature(botster_hub_client::FEATURE_PLUGIN_SURFACE_RENDER)
    );
    assert!(
        ack.compatibility
            .supports_feature(botster_hub_client::FEATURE_PLUGIN_SURFACE_ACTION)
    );
    assert_eq!(
        ack.compatibility.conformance_fixture_revision,
        botster_hub_client::CONFORMANCE_FIXTURE_REVISION
    );

    let status = botster_hub_client::request(&endpoint, botster_hub_client::DaemonRequest::Status)
        .expect("external client status request");
    assert!(status.diagnostics.iter().any(|diagnostic| {
        diagnostic.kind == botster_hub_client::DaemonDiagnosticKind::Connected
            && diagnostic.operation.as_deref() == Some("status")
    }));
    assert!(!has_failure_diagnostic(&status.diagnostics));
    let status = status.status.expect("status response body");
    assert_eq!(status.compatibility, ack.compatibility);
    assert!(status.diagnostics.is_empty());

    let mut version_requirement = botster_hub_client::DaemonCompatibilityRequirement::current();
    version_requirement.client_name = "future-version-client".to_string();
    version_requirement.minimum_protocol_version = botster_hub_client::PROTOCOL_VERSION + 1;
    let version_error =
        botster_hub_client::connect_and_hello_with_requirement(&endpoint, &version_requirement)
            .expect_err("future protocol version should fail compatibility");
    let version_message = version_error.to_string();
    assert!(version_message.contains("future-version-client"));
    assert!(version_message.contains("unsupported protocol version"));
    assert!(!version_message.contains(&data_dir.to_string_lossy().to_string()));
    let botster_hub_client::DaemonTransportError::Compatibility(version_error) = version_error
    else {
        panic!("version mismatch should be a compatibility error");
    };
    assert!(version_error.diagnostics.iter().any(|diagnostic| {
        diagnostic.kind == botster_hub_client::DaemonDiagnosticKind::CompatibilityMismatch
            && diagnostic
                .message
                .as_deref()
                .is_some_and(|message| message.contains("unsupported protocol version"))
    }));
    assert!(!has_diagnostic_kind(
        &version_error.diagnostics,
        botster_hub_client::DaemonDiagnosticKind::Connected
    ));
    assert!(!has_diagnostic_kind(
        &version_error.diagnostics,
        botster_hub_client::DaemonDiagnosticKind::ActionFailure
    ));

    let mut feature_requirement = botster_hub_client::DaemonCompatibilityRequirement::current();
    feature_requirement.client_name = "future-feature-client".to_string();
    feature_requirement
        .required_features
        .push("future_feature".to_string());
    let feature_error =
        botster_hub_client::connect_and_hello_with_requirement(&endpoint, &feature_requirement)
            .expect_err("future feature should fail compatibility");
    let feature_message = feature_error.to_string();
    assert!(feature_message.contains("future-feature-client"));
    assert!(feature_message.contains("missing required feature(s): future_feature"));
    assert!(!feature_message.contains(&data_dir.to_string_lossy().to_string()));
    let botster_hub_client::DaemonTransportError::Compatibility(feature_error) = feature_error
    else {
        panic!("feature mismatch should be a compatibility error");
    };
    assert!(feature_error.diagnostics.iter().any(|diagnostic| {
        diagnostic.kind == botster_hub_client::DaemonDiagnosticKind::UnsupportedFeature
            && diagnostic.feature.as_deref() == Some("future_feature")
    }));
    assert!(!has_diagnostic_kind(
        &feature_error.diagnostics,
        botster_hub_client::DaemonDiagnosticKind::Connected
    ));
    assert!(!has_diagnostic_kind(
        &feature_error.diagnostics,
        botster_hub_client::DaemonDiagnosticKind::ActionFailure
    ));

    shutdown_cli_daemon(&data_dir, child);
}

#[test]
fn process_ownership_external_hub_test_support_cleans_up_isolated_daemon() {
    let _guard = daemon_test_guard();
    let first = botster_hub_test_support::IsolatedHubBuilder::new()
        .hub_bin(env!("CARGO_BIN_EXE_botster-hub"))
        .session_worker_bin(session_worker_binary_path())
        .root(PathBuf::from("/tmp/bh-test-support"))
        .name("downstream-shape")
        .start()
        .expect("start isolated hub through public test-support harness");
    assert!(first.data_dir().starts_with("/tmp/bh-test-support"));
    assert!(first.endpoint().socket_path.starts_with(first.data_dir()));
    let support_matrix = botster_hub_test_support::first_party_client_support_matrix();
    let first_report =
        botster_hub_test_support::run_client_conformance(&first).expect("run client conformance");
    assert_eq!(first_report.lifecycle_state, "running");
    assert_eq!(first_report.initial_session_count, 0);
    assert_eq!(first_report.spawned_lifecycle, "running");
    assert_eq!(
        support_matrix.session_actions,
        vec![
            "status",
            "list_sessions",
            "subscribe_entities",
            "unsubscribe_entities",
            "remove_session",
            "spawn",
            "attach",
            "drain",
            "send_input",
            "resize",
            "shutdown_session",
        ]
    );
    assert!(first_report.stream_contains_ready);
    assert!(first_report.stream_contains_echo);
    assert!(first_report.stream_contains_resize);
    assert_eq!(first_report.compatibility_protocol, support_matrix.protocol);
    assert_eq!(
        first_report.compatibility_protocol_version,
        support_matrix.protocol_version
    );
    assert_eq!(
        first_report.compatibility_features,
        support_matrix.supported_features
    );
    assert_eq!(
        first_report.compatibility_conformance_fixture_revision,
        support_matrix.conformance_fixture_revision
    );
    assert_eq!(first_report.connected_diagnostic_operation, "status");
    assert_eq!(first_report.validation_error_operation, "drain_runtime");
    assert_eq!(
        first_report.validation_diagnostic_kind,
        support_matrix
            .terminal_streaming
            .missing_session_diagnostic_kind
    );
    assert!(support_matrix.terminal_streaming.supported);
    assert!(support_matrix.terminal_streaming.held_open_stream);
    assert_eq!(
        support_matrix.terminal_streaming.conformance_ready_output,
        "conformance-ready"
    );
    assert_eq!(
        support_matrix.terminal_streaming.conformance_echo_output,
        "echo:from-conformance"
    );
    assert!(support_matrix.resize.supported);
    assert_eq!(support_matrix.resize.action, "resize");
    assert_eq!(support_matrix.resize.conformance_output_prefix, "winsize:");

    let plugin_report = botster_hub_test_support::run_plugin_contract_matrix_conformance(
        &first,
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("fixtures")
            .join("plugins")
            .join("plugin-contract-matrix"),
    )
    .expect("run plugin contract matrix conformance");
    assert_eq!(plugin_report.enabled_state, "enabled");
    assert!(support_matrix.plugin_surfaces.render_supported);
    assert!(support_matrix.plugin_surfaces.action_supported);
    assert_eq!(
        plugin_report.app_surface_kind,
        support_matrix.plugin_surfaces.rendered_surface_kind
    );
    assert_eq!(
        plugin_report.app_surface_node_id,
        support_matrix.plugin_surfaces.rendered_surface_node_id
    );
    assert_eq!(
        plugin_report.session_surface_id,
        support_matrix.session_entities.plugin_surface_id
    );
    assert_eq!(
        plugin_report.session_surface_binding_family,
        support_matrix.session_entities.binding_family
    );
    assert!(plugin_report.session_surface_matches_fixture);
    assert_eq!(plugin_report.action_error_state, "error");
    assert_eq!(
        plugin_report.action_error_diagnostic_kind,
        support_matrix
            .plugin_surfaces
            .invalid_action_diagnostic_kind
    );
    assert_eq!(
        plugin_report.client_render_check.class,
        botster_hub_test_support::ConformanceFailureClass::ClientRendering
    );

    let terminal_app_report =
        botster_hub_test_support::run_foreground_terminal_app_open_conformance(&first)
            .expect("run foreground terminal app open conformance");
    assert_eq!(terminal_app_report.package_state, "enabled");
    assert_eq!(
        terminal_app_report.package_name,
        "first-party.terminal-client"
    );
    assert_eq!(terminal_app_report.app_id, "tui");
    assert_eq!(terminal_app_report.entrypoint_id, "tui");
    assert_eq!(terminal_app_report.app_kind, "terminal_app");
    assert_eq!(terminal_app_report.launch_mode, "foreground_stdio");
    assert!(terminal_app_report.hub_connection_env_present);
    assert_eq!(terminal_app_report.hub_connection_transport, "unix_socket");
    assert!(terminal_app_report.hub_connection_socket_path_absolute);
    assert!(terminal_app_report.hub_data_dir_env_present);
    assert!(terminal_app_report.hub_data_dir_env_absolute);
    assert!(terminal_app_report.launch_working_directory_is_package_root);
    assert!(terminal_app_report.launch_working_directory_differs_from_daemon_cwd);
    assert_eq!(terminal_app_report.real_hub_action_operation, "status");
    assert_eq!(terminal_app_report.real_hub_action_result, "running");
    assert_eq!(terminal_app_report.exit_code, Some(0));
    assert!(
        terminal_app_report
            .stdout
            .contains("hub_connection_present=true")
    );
    assert!(
        terminal_app_report
            .stdout
            .contains("hub_connection_transport=unix_socket")
    );
    assert!(
        terminal_app_report
            .stdout
            .contains("hub_connection_socket_absolute=true")
    );
    assert!(
        terminal_app_report
            .stdout
            .contains("hub_data_dir_present=true")
    );
    assert!(terminal_app_report.stderr.is_empty());
    first.shutdown().expect("shutdown first isolated hub");

    let second = botster_hub_test_support::IsolatedHubBuilder::new()
        .hub_bin(env!("CARGO_BIN_EXE_botster-hub"))
        .session_worker_bin(session_worker_binary_path())
        .root(PathBuf::from("/tmp/bh-test-support"))
        .name("downstream-shape-determinism")
        .start()
        .expect("start second isolated hub through public test-support harness");
    let second_report =
        botster_hub_test_support::run_client_conformance(&second).expect("rerun conformance");
    assert_eq!(second_report, first_report);
    second.shutdown().expect("shutdown second isolated hub");
}

#[test]
fn foreground_terminal_app_open_absolutizes_relative_runtime_paths() {
    let _guard = daemon_test_guard();
    let daemon_working_directory = PathBuf::from("/tmp");
    let hub = botster_hub_test_support::IsolatedHubBuilder::new()
        .hub_bin(env!("CARGO_BIN_EXE_botster-hub"))
        .session_worker_bin(session_worker_binary_path())
        .root(PathBuf::from("bh-relative-runtime"))
        .working_directory(&daemon_working_directory)
        .name("package-cwd")
        .start()
        .expect("start isolated hub with relative runtime root");
    assert!(
        hub.data_dir()
            .starts_with(daemon_working_directory.join("bh-relative-runtime"))
    );

    let report = botster_hub_test_support::run_foreground_terminal_app_open_conformance(&hub)
        .expect("launch package-root child through daemon-resolved foreground contract");
    assert!(report.hub_connection_socket_path_absolute);
    assert!(report.hub_data_dir_env_absolute);
    assert!(report.launch_working_directory_is_package_root);
    assert!(report.launch_working_directory_differs_from_daemon_cwd);
    assert_eq!(report.real_hub_action_operation, "status");
    assert_eq!(report.real_hub_action_result, "running");
    assert_eq!(report.exit_code, Some(0));

    hub.shutdown().expect("shutdown relative-root isolated hub");
}

#[test]
fn external_hub_client_many_pty_adversarial_conformance_ci() {
    let _guard = daemon_test_guard();
    let hub = botster_hub_test_support::IsolatedHubBuilder::new()
        .hub_bin(env!("CARGO_BIN_EXE_botster-hub"))
        .session_worker_bin(session_worker_binary_path())
        .root(PathBuf::from("/tmp/bh-test-support"))
        .name("many-pty-client-attach-ci")
        .start()
        .expect("start isolated hub for CI-safe many-PTY proof");

    let report = botster_hub_test_support::run_many_pty_client_attach_conformance(&hub, 8)
        .expect("run CI-safe many-PTY client attach proof");
    // Ok(report) is the behavioral oracle; stage-labeled errors identify which
    // required observation failed. These assertions pin scenario and cleanup size.
    assert_eq!(report.total_sessions, 8);
    assert_eq!(report.quiet_sessions, 7);
    assert_eq!(report.cleaned_sessions, 8);

    hub.shutdown().expect("shutdown CI-safe many-PTY hub");
}

#[test]
#[ignore = "larger local adversarial proof; run explicitly with the documented command"]
fn external_hub_client_many_pty_adversarial_conformance_local() {
    let _guard = daemon_test_guard();
    let hub = botster_hub_test_support::IsolatedHubBuilder::new()
        .hub_bin(env!("CARGO_BIN_EXE_botster-hub"))
        .session_worker_bin(session_worker_binary_path())
        .root(PathBuf::from("/tmp/bh-test-support"))
        .name("many-pty-client-attach-local")
        .start()
        .expect("start isolated hub for larger local many-PTY proof");

    let report = botster_hub_test_support::run_many_pty_client_attach_conformance(&hub, 32)
        .expect("run larger local many-PTY client attach proof");
    // Ok(report) is the behavioral oracle; stage-labeled errors identify which
    // required observation failed. These assertions pin scenario and cleanup size.
    assert_eq!(report.total_sessions, 32);
    assert_eq!(report.quiet_sessions, 31);
    assert_eq!(report.cleaned_sessions, 32);

    hub.shutdown().expect("shutdown larger local many-PTY hub");
}

#[test]
fn external_daemon_same_session_reattach_replays_opaque_history_before_live_output() {
    let _guard = daemon_test_guard();
    let data_dir = unique_short_test_dir("late-history");
    let config = explicit_config(&data_dir);
    let child = start_cli_daemon(&data_dir);
    let mut connection =
        botster_hub::DaemonConnection::connect(&config).expect("connect daemon socket");

    let spawn = connection
        .request(&botster_hub::DaemonRequest::Spawn {
            session_id: "late-history-session".to_string(),
            command: "printf 'retained-before-attach\\n'; while IFS= read -r line; do printf 'after:%s\\n' \"$line\"; done".to_string(),
        })
        .expect("spawn late-history session");
    assert_eq!(spawn.kind, botster_hub::DaemonResponseKind::Spawned);

    let first_attach = connection
        .request(&botster_hub::DaemonRequest::Attach {
            session_id: "late-history-session".to_string(),
            subscription_id: "late-history-first-subscription".to_string(),
        })
        .expect("attach first subscription");
    assert_eq!(first_attach.kind, botster_hub::DaemonResponseKind::Events);

    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    let mut first_observed = String::new();
    while std::time::Instant::now() < deadline {
        let drain = connection
            .request(&botster_hub::DaemonRequest::Drain {
                session_id: "late-history-session".to_string(),
            })
            .expect("drain first subscription output");
        for event in drain.events {
            if let botster_hub::DaemonEvent::TerminalOutput {
                subscription_id,
                data,
                ..
            } = event
                && subscription_id == "late-history-first-subscription"
            {
                first_observed.push_str(&data);
            }
        }
        if first_observed.contains("retained-before-attach") {
            break;
        }
        thread::sleep(Duration::from_millis(30));
    }
    assert!(
        first_observed.contains("retained-before-attach"),
        "first subscription should observe initial output before late attach, got {first_observed:?}"
    );

    let retained_after_attach = connection
        .request(&botster_hub::DaemonRequest::SendInput {
            session_id: "late-history-session".to_string(),
            data: "retained-after-attach\n".to_string(),
        })
        .expect("send second retained marker before socket loss");
    assert_eq!(
        retained_after_attach.kind,
        botster_hub::DaemonResponseKind::Events
    );
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while std::time::Instant::now() < deadline {
        let drain = connection
            .request(&botster_hub::DaemonRequest::Drain {
                session_id: "late-history-session".to_string(),
            })
            .expect("drain second retained marker on first subscription");
        for event in drain.events {
            if let botster_hub::DaemonEvent::TerminalOutput {
                subscription_id,
                data,
                ..
            } = event
                && subscription_id == "late-history-first-subscription"
            {
                first_observed.push_str(&data);
            }
        }
        if first_observed.contains("after:retained-after-attach") {
            break;
        }
        thread::sleep(Duration::from_millis(30));
    }
    assert!(
        first_observed.contains("after:retained-after-attach"),
        "first subscription should observe the second marker before socket loss, got {first_observed:?}"
    );

    drop(connection);
    let mut connection =
        botster_hub::DaemonConnection::connect(&config).expect("reconnect daemon socket");
    let late_attach = connection
        .request(&botster_hub::DaemonRequest::Attach {
            session_id: "late-history-session".to_string(),
            subscription_id: "late-history-reattach-subscription".to_string(),
        })
        .expect("reattach same session with a fresh subscription id");
    assert_eq!(late_attach.kind, botster_hub::DaemonResponseKind::Events);

    let read_screen = connection
        .request(&botster_hub::DaemonRequest::ReadScreen {
            session_id: "late-history-session".to_string(),
        })
        .expect("read retained screen before later live output");
    assert_eq!(
        read_screen.kind,
        botster_hub::DaemonResponseKind::ReadScreen
    );
    let screen_text = read_screen
        .read_screen
        .expect("retained screen response body")
        .text;
    for marker in ["retained-before-attach", "after:retained-after-attach"] {
        assert_eq!(
            screen_text.matches(marker).count(),
            1,
            "ReadScreen should contain {marker:?} exactly once, got {screen_text:?}"
        );
    }
    assert!(
        screen_text.find("retained-before-attach")
            < screen_text.find("after:retained-after-attach"),
        "ReadScreen should preserve retained marker order, got {screen_text:?}"
    );

    let send = connection
        .request(&botster_hub::DaemonRequest::SendInput {
            session_id: "late-history-session".to_string(),
            data: "live-after-late\n".to_string(),
        })
        .expect("send later live output");
    assert_eq!(send.kind, botster_hub::DaemonResponseKind::Events);

    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    let mut observed_events = Vec::new();
    while std::time::Instant::now() < deadline {
        let drain = connection
            .request(&botster_hub::DaemonRequest::Drain {
                session_id: "late-history-session".to_string(),
            })
            .expect("drain late subscription output");
        observed_events.extend(drain.events);
        let saw_live = observed_events.iter().any(|event| {
            matches!(
                event,
                botster_hub::DaemonEvent::TerminalOutput {
                    subscription_id,
                    data,
                    ..
                } if subscription_id == "late-history-reattach-subscription"
                    && data.contains("after:live-after-late")
            )
        });
        if saw_live {
            break;
        }
        thread::sleep(Duration::from_millis(30));
    }

    let attaching_index = observed_events
        .iter()
        .position(|event| {
            matches!(
                event,
                botster_hub::DaemonEvent::AttachState {
                    subscription_id,
                    state,
                    ..
                } if subscription_id == "late-history-reattach-subscription" && state == "attaching"
            )
        })
        .expect("late subscription should enter attaching state on daemon socket");
    let history_events = observed_events
        .iter()
        .enumerate()
        .filter_map(|(index, event)| match event {
            botster_hub::DaemonEvent::Snapshot {
                subscription_id,
                history,
                ..
            }
            | botster_hub::DaemonEvent::Scrollback {
                subscription_id,
                history,
                ..
            } if subscription_id == "late-history-reattach-subscription" => Some((index, history)),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert!(
        !history_events.is_empty(),
        "reattach should receive opaque history"
    );
    for (_, history) in &history_events {
        let payload = history
            .decoded_bytes()
            .expect("real daemon opaque history payload decodes");
        assert_eq!(
            history.payload_encoding,
            botster_hub_client::DaemonHistoryEncoding::Base64
        );
        assert_eq!(history.bytes, payload.len());
        assert!(
            !payload.is_empty(),
            "opaque history payload must not be empty"
        );
    }
    let history_index = history_events
        .first()
        .map(|(index, _)| *index)
        .unwrap_or_else(|| {
            panic!("reattach should receive retained history, got {observed_events:?}")
        });
    let last_history_index = history_events
        .last()
        .map(|(index, _)| *index)
        .expect("reattach history should have a last event");
    let live_index = observed_events
        .iter()
        .position(|event| {
            matches!(
                event,
                botster_hub::DaemonEvent::TerminalOutput {
                    subscription_id,
                    data,
                    ..
                } if subscription_id == "late-history-reattach-subscription"
                    && data.contains("after:live-after-late")
            )
        })
        .expect("late subscription should receive later live output");
    let attached_index = observed_events
        .iter()
        .position(|event| {
            matches!(
                event,
                botster_hub::DaemonEvent::AttachState {
                    subscription_id,
                    state,
                    ..
                } if subscription_id == "late-history-reattach-subscription" && state == "attached"
            )
        })
        .expect("late subscription should become attached after history on daemon socket");
    let first_terminal_output_index = observed_events
        .iter()
        .position(|event| {
            matches!(
                event,
                botster_hub::DaemonEvent::TerminalOutput {
                    subscription_id,
                    ..
                } if subscription_id == "late-history-reattach-subscription"
            )
        })
        .expect("late subscription should receive terminal output on daemon socket");
    assert!(
        attaching_index < history_index
            && last_history_index < attached_index
            && attached_index < first_terminal_output_index
            && attached_index < live_index,
        "fresh reattach subscription should observe attaching < history < attached < live, got {observed_events:?}"
    );
    assert!(
        !observed_events.iter().any(|event| {
            matches!(
                event,
                botster_hub::DaemonEvent::TerminalOutput {
                    subscription_id,
                    data,
                    ..
                } if subscription_id == "late-history-first-subscription"
                    && data.contains("after:live-after-late")
            )
        }),
        "socket cleanup should detach the old subscription before later live output, got {observed_events:?}"
    );

    let no_history_spawn = connection
        .request(&botster_hub::DaemonRequest::Spawn {
            session_id: "no-history-session".to_string(),
            command: "while IFS= read -r line; do printf 'after:%s\\n' \"$line\"; done".to_string(),
        })
        .expect("spawn no-history session");
    assert_eq!(
        no_history_spawn.kind,
        botster_hub::DaemonResponseKind::Spawned
    );

    let first_no_history_attach = connection
        .request(&botster_hub::DaemonRequest::Attach {
            session_id: "no-history-session".to_string(),
            subscription_id: "no-history-first-subscription".to_string(),
        })
        .expect("attach first no-history subscription");
    assert_eq!(
        first_no_history_attach.kind,
        botster_hub::DaemonResponseKind::Events
    );

    drop(connection);
    let mut connection =
        botster_hub::DaemonConnection::connect(&config).expect("reconnect idle daemon socket");
    let late_no_history_attach = connection
        .request(&botster_hub::DaemonRequest::Attach {
            session_id: "no-history-session".to_string(),
            subscription_id: "no-history-reattach-subscription".to_string(),
        })
        .expect("reattach idle session with a fresh subscription id");
    assert_eq!(
        late_no_history_attach.kind,
        botster_hub::DaemonResponseKind::Events
    );

    let no_history_read_screen = connection
        .request(&botster_hub::DaemonRequest::ReadScreen {
            session_id: "no-history-session".to_string(),
        })
        .expect("read blank screen before sending live output");
    assert_eq!(
        no_history_read_screen.kind,
        botster_hub::DaemonResponseKind::ReadScreen
    );
    let no_history_screen = no_history_read_screen
        .read_screen
        .expect("blank read screen response body");
    assert!(
        no_history_screen.text.is_empty(),
        "idle session should have no prior renderable output, got {:?}",
        no_history_screen.text
    );

    let no_history_send = connection
        .request(&botster_hub::DaemonRequest::SendInput {
            session_id: "no-history-session".to_string(),
            data: "live-only\n".to_string(),
        })
        .expect("send no-history live output");
    assert_eq!(
        no_history_send.kind,
        botster_hub::DaemonResponseKind::Events
    );

    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    let mut no_history_events = Vec::new();
    while std::time::Instant::now() < deadline {
        let drain = connection
            .request(&botster_hub::DaemonRequest::Drain {
                session_id: "no-history-session".to_string(),
            })
            .expect("drain no-history live output");
        no_history_events.extend(drain.events);
        let saw_live = no_history_events.iter().any(|event| {
            matches!(
                event,
                botster_hub::DaemonEvent::TerminalOutput {
                    subscription_id,
                    data,
                    ..
                } if subscription_id == "no-history-reattach-subscription"
                    && data.contains("after:live-only")
            )
        });
        if saw_live {
            break;
        }
        thread::sleep(Duration::from_millis(30));
    }

    assert!(
        !no_history_events.iter().any(|event| {
            matches!(
                event,
                botster_hub::DaemonEvent::Scrollback {
                    subscription_id,
                    ..
                } if subscription_id == "no-history-reattach-subscription"
            )
        }),
        "idle subscription should not receive fabricated scrollback, got {no_history_events:?}"
    );
    let no_history_attaching_index = no_history_events
        .iter()
        .position(|event| {
            matches!(
                event,
                botster_hub::DaemonEvent::AttachState {
                    subscription_id,
                    state,
                    ..
                } if subscription_id == "no-history-reattach-subscription" && state == "attaching"
            )
        })
        .expect("late no-history subscription should enter attaching state");
    let no_history_attached_index = no_history_events
        .iter()
        .position(|event| {
            matches!(
                event,
                botster_hub::DaemonEvent::AttachState {
                    subscription_id,
                    state,
                    ..
                } if subscription_id == "no-history-reattach-subscription" && state == "attached"
            )
        })
        .expect("late no-history subscription should become attached");
    let no_history_live_index = no_history_events
        .iter()
        .position(|event| {
            matches!(
                event,
                botster_hub::DaemonEvent::TerminalOutput {
                    subscription_id,
                    data,
                    ..
                } if subscription_id == "no-history-reattach-subscription"
                    && data.contains("after:live-only")
            )
        })
        .expect("late no-history subscription should receive live output");
    let no_history_last_initial_state_index = no_history_events.iter().rposition(|event| {
        matches!(
            event,
            botster_hub::DaemonEvent::Snapshot {
                subscription_id,
                ..
            } | botster_hub::DaemonEvent::Scrollback {
                subscription_id,
                ..
            } if subscription_id == "no-history-reattach-subscription"
        )
    });
    let no_history_first_terminal_output_index = no_history_events
        .iter()
        .position(|event| {
            matches!(
                event,
                botster_hub::DaemonEvent::TerminalOutput {
                    subscription_id,
                    ..
                } if subscription_id == "no-history-reattach-subscription"
            )
        })
        .expect("late no-history subscription should receive terminal output");
    assert!(
        no_history_attaching_index < no_history_attached_index
            && no_history_last_initial_state_index
                .is_none_or(|index| index < no_history_attached_index)
            && no_history_attached_index < no_history_first_terminal_output_index
            && no_history_attached_index < no_history_live_index,
        "idle subscription should observe attaching < optional initial state < attached < live, got {no_history_events:?}"
    );

    let shutdown_session = connection
        .request(&botster_hub::DaemonRequest::ShutdownSession {
            session_id: "late-history-session".to_string(),
        })
        .expect("shutdown late-history session");
    assert_eq!(
        shutdown_session.kind,
        botster_hub::DaemonResponseKind::Events
    );
    let shutdown_no_history_session = connection
        .request(&botster_hub::DaemonRequest::ShutdownSession {
            session_id: "no-history-session".to_string(),
        })
        .expect("shutdown no-history session");
    assert_eq!(
        shutdown_no_history_session.kind,
        botster_hub::DaemonResponseKind::Events
    );
    shutdown_cli_daemon(&data_dir, child);
}

#[test]
fn daemon_detaches_subscription_when_attach_connection_drops() {
    let _guard = daemon_test_guard();
    let data_dir = unique_test_dir("cli-attach-eof");
    let config = explicit_config(&data_dir);
    let child = start_cli_daemon(&data_dir);

    let spawn = botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::Spawn {
            session_id: "eof-session".to_string(),
            command:
                "printf 'ready\\n'; while IFS= read -r line; do printf 'echo:%s\\n' \"$line\"; done"
                    .to_string(),
        },
    )
    .expect("spawn eof test session");
    assert_eq!(spawn.kind, botster_hub::DaemonResponseKind::Spawned);

    let attach = botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::Attach {
            session_id: "eof-session".to_string(),
            subscription_id: "dropped-subscription".to_string(),
        },
    )
    .expect("attach dropped subscription");
    assert_eq!(attach.kind, botster_hub::DaemonResponseKind::Events);

    thread::sleep(Duration::from_millis(150));

    botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::SendInput {
            session_id: "eof-session".to_string(),
            data: "after-eof\r".to_string(),
        },
    )
    .expect("send input after dropped attach");

    let deadline = std::time::Instant::now() + Duration::from_secs(1);
    let mut observed_events = Vec::new();
    while std::time::Instant::now() < deadline {
        let drain = botster_hub::daemon_transport_request(
            &config,
            botster_hub::DaemonRequest::Drain {
                session_id: "eof-session".to_string(),
            },
        )
        .expect("drain after dropped attach");
        observed_events.extend(drain.events);
        thread::sleep(Duration::from_millis(30));
    }

    assert!(
        observed_events.iter().all(|event| {
            !matches!(
                event,
                botster_hub::DaemonEvent::TerminalOutput {
                    subscription_id,
                    data,
                    ..
                } if subscription_id == "dropped-subscription" && data.contains("after-eof")
            )
        }),
        "dropped attach subscription received later terminal output: {observed_events:?}"
    );

    let shutdown_session = botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::ShutdownSession {
            session_id: "eof-session".to_string(),
        },
    )
    .expect("shutdown eof test session");
    assert_eq!(
        shutdown_session.kind,
        botster_hub::DaemonResponseKind::Events
    );
    let sessions_after_shutdown =
        botster_hub::daemon_transport_request(&config, botster_hub::DaemonRequest::ListSessions)
            .expect("list sessions after eof test session shutdown");
    assert!(
        sessions_after_shutdown
            .sessions
            .iter()
            .any(|session| session.session_id == "eof-session" && session.lifecycle == "exited"),
        "eof-session should be exited after shutdown: {:?}",
        sessions_after_shutdown.sessions
    );
    shutdown_cli_daemon(&data_dir, child);
}

#[test]
fn daemon_notify_session_defers_without_observed_readiness_over_socket() {
    let _guard = daemon_test_guard();
    let data_dir = unique_test_dir("daemon-notify-session");
    let config = explicit_config(&data_dir);
    let child = start_cli_daemon(&data_dir);

    let spawn = botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::Spawn {
            session_id: "notify-socket-session".to_string(),
            command:
                "printf 'ready\\n'; while IFS= read -r line; do printf 'echo:%s\\n' \"$line\"; done"
                    .to_string(),
        },
    )
    .expect("spawn guarded socket session");
    assert_eq!(spawn.kind, botster_hub::DaemonResponseKind::Spawned);

    let mut connection =
        botster_hub::DaemonConnection::connect(&config).expect("connect TUI-grade socket");
    connection
        .request(&botster_hub::DaemonRequest::Attach {
            session_id: "notify-socket-session".to_string(),
            subscription_id: "notify-socket-subscription".to_string(),
        })
        .expect("attach persistent socket subscription");

    let write = connection
        .request(&botster_hub::DaemonRequest::NotifySession {
            session_id: "notify-socket-session".to_string(),
            data: "notify-socket\n".to_string(),
        })
        .expect("notify session over daemon socket");
    assert_eq!(write.kind, botster_hub::DaemonResponseKind::SessionNotified);
    let notify = write
        .coordination
        .and_then(|coordination| coordination.notify)
        .expect("notify response body");
    assert!(notify.decision.starts_with("Defer"));
    assert_eq!(notify.states, vec!["accepted", "deferred"]);

    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    let mut observed = String::new();
    while std::time::Instant::now() < deadline {
        let drain = connection
            .request(&botster_hub::DaemonRequest::Drain {
                session_id: "notify-socket-session".to_string(),
            })
            .expect("drain guarded socket session");
        for event in drain.events {
            if let botster_hub::DaemonEvent::TerminalOutput { data, .. } = event {
                observed.push_str(&data);
            }
        }
        if observed.contains("echo:notify-socket") {
            break;
        }
        thread::sleep(Duration::from_millis(30));
    }
    assert!(
        !observed.contains("echo:notify-socket"),
        "notify session without observed readiness should not reach PTY input path, got {observed:?}"
    );

    let shutdown_session = connection
        .request(&botster_hub::DaemonRequest::ShutdownSession {
            session_id: "notify-socket-session".to_string(),
        })
        .expect("shutdown guarded socket session");
    assert_eq!(
        shutdown_session.kind,
        botster_hub::DaemonResponseKind::Events
    );
    let sessions_after_shutdown = connection
        .request(&botster_hub::DaemonRequest::ListSessions)
        .expect("list sessions after guarded socket session shutdown");
    assert!(
        sessions_after_shutdown.sessions.iter().any(|session| {
            session.session_id == "notify-socket-session" && session.lifecycle == "exited"
        }),
        "notify-socket-session should be exited after shutdown: {:?}",
        sessions_after_shutdown.sessions
    );
    shutdown_cli_daemon(&data_dir, child);
}

#[test]
fn stalled_attach_stdout_does_not_block_other_daemon_commands() {
    let _guard = daemon_test_guard();
    let data_dir = unique_test_dir("cli-stalled-attach");
    let child = start_cli_daemon(&data_dir);

    let mut spawn_command = Command::new(env!("CARGO_BIN_EXE_botster-hub"));
    spawn_command
        .arg("sessions")
        .arg("spawn")
        .arg("--data-dir")
        .arg(&data_dir)
        .arg("--session-id")
        .arg("slow-consumer")
        .arg("--")
        .arg(
            "i=0; while [ \"$i\" -lt 50000 ]; do printf 'flood-line-%05d\\n' \"$i\"; i=$((i + 1)); done; while IFS= read -r line; do printf 'echo:%s\\n' \"$line\"; done",
        );
    let spawn = run_command_with_timeout_diagnostics(
        "spawn",
        spawn_command,
        LOCAL_RUNTIME_DAEMON_READINESS_BUDGET,
    );
    assert!(
        spawn.output.status.success(),
        "spawn failed: {}",
        spawn.diagnostics(),
    );

    let mut attach_child = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("sessions")
        .arg("attach")
        .arg("--data-dir")
        .arg(&data_dir)
        .arg("slow-consumer")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn stalled attach");
    let buffered_attach_stdout = wait_for_buffered_child_stdout(
        &mut attach_child,
        STALLED_ATTACH_MIN_BUFFERED_STDOUT_BYTES,
        STALLED_ATTACH_STABLE_SAMPLES,
        LOCAL_RUNTIME_DAEMON_READINESS_BUDGET,
    )
    .unwrap_or_else(|error| panic!("stalled attach did not reach stdout backpressure: {error}"));
    assert!(
        buffered_attach_stdout.available_bytes >= STALLED_ATTACH_MIN_BUFFERED_STDOUT_BYTES,
        "stalled attach should retain at least {} unread stdout bytes, got {} after {:?}; recent_samples={:?}",
        STALLED_ATTACH_MIN_BUFFERED_STDOUT_BYTES,
        buffered_attach_stdout.available_bytes,
        buffered_attach_stdout.elapsed,
        buffered_attach_stdout.recent_samples,
    );
    if let Some(status) = attach_child.try_wait().expect("poll stalled attach") {
        let (stdout, stderr) = collect_child_output(&mut attach_child);
        panic!(
            "attach exited before the slow-consumer check after {:?}: status={status} backpressure={buffered_attach_stdout:?} stdout={stdout:?} stderr={stderr:?}",
            buffered_attach_stdout.elapsed,
        );
    }

    let mut list_command = Command::new(env!("CARGO_BIN_EXE_botster-hub"));
    list_command
        .arg("sessions")
        .arg("list")
        .arg("--data-dir")
        .arg(&data_dir);
    let list = run_command_with_timeout_diagnostics(
        "list",
        list_command,
        LOCAL_RUNTIME_DAEMON_READINESS_BUDGET,
    );
    assert!(
        list.output.status.success(),
        "list failed while attach stdout was blocked: {}; attach_child={}",
        list.diagnostics(),
        child_state_diagnostics(&mut attach_child),
    );

    let mut send_command = Command::new(env!("CARGO_BIN_EXE_botster-hub"));
    send_command
        .arg("sessions")
        .arg("send-input")
        .arg("--data-dir")
        .arg(&data_dir)
        .arg("slow-consumer")
        .arg("--")
        .arg("still-responsive\r");
    let send = run_command_with_timeout_diagnostics(
        "send-input",
        send_command,
        LOCAL_RUNTIME_DAEMON_READINESS_BUDGET,
    );
    assert!(
        send.output.status.success(),
        "send-input failed while attach stdout was blocked: {}; attach_child={}",
        send.diagnostics(),
        child_state_diagnostics(&mut attach_child),
    );

    let mut resize_command = Command::new(env!("CARGO_BIN_EXE_botster-hub"));
    resize_command
        .arg("sessions")
        .arg("resize")
        .arg("--data-dir")
        .arg(&data_dir)
        .arg("slow-consumer")
        .arg("32")
        .arg("120");
    let resize = run_command_with_timeout_diagnostics(
        "resize",
        resize_command,
        LOCAL_RUNTIME_DAEMON_READINESS_BUDGET,
    );
    assert!(
        resize.output.status.success(),
        "resize failed while attach stdout was blocked: {}; attach_child={}",
        resize.diagnostics(),
        child_state_diagnostics(&mut attach_child),
    );

    let mut shutdown_command = Command::new(env!("CARGO_BIN_EXE_botster-hub"));
    shutdown_command
        .arg("shutdown")
        .arg("--data-dir")
        .arg(&data_dir);
    let shutdown = run_command_with_timeout_diagnostics(
        "shutdown",
        shutdown_command,
        LOCAL_RUNTIME_DAEMON_READINESS_BUDGET,
    );
    assert!(
        shutdown.output.status.success(),
        "shutdown failed while attach stdout was blocked: {}; attach_child={}",
        shutdown.diagnostics(),
        child_state_diagnostics(&mut attach_child),
    );

    let _ = attach_child.kill();
    let _ = attach_child.wait_with_output();
    let output = child.wait_with_output().expect("wait for daemon child");
    assert!(
        output.status.success(),
        "daemon failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let cleanup_child = start_cli_daemon(&data_dir);
    let shutdown_session = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("sessions")
        .arg("shutdown")
        .arg("--data-dir")
        .arg(&data_dir)
        .arg("slow-consumer")
        .output()
        .expect("shut down recovered slow-consumer session");
    assert!(
        shutdown_session.status.success(),
        "recovered session shutdown failed: {}",
        String::from_utf8_lossy(&shutdown_session.stderr)
    );
    let shutdown_session_stdout =
        String::from_utf8(shutdown_session.stdout).expect("session shutdown stdout is utf8");
    let returned_shutdown_events = shutdown_session_stdout.contains("response=events");
    let returned_terminal_cleanup = shutdown_session_stdout.contains("response=session_cleanup")
        && shutdown_session_stdout.contains("session_id=slow-consumer")
        && shutdown_session_stdout.contains("outcome=already_exited");
    assert!(
        returned_shutdown_events || returned_terminal_cleanup,
        "recovered session shutdown should return events or terminal cleanup: {shutdown_session_stdout:?}"
    );

    let sessions_after_shutdown = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("sessions")
        .arg("list")
        .arg("--data-dir")
        .arg(&data_dir)
        .output()
        .expect("list sessions after recovered slow-consumer shutdown");
    assert!(
        sessions_after_shutdown.status.success(),
        "list failed after recovered slow-consumer shutdown: {}",
        String::from_utf8_lossy(&sessions_after_shutdown.stderr)
    );
    let sessions_after_shutdown_stdout = String::from_utf8(sessions_after_shutdown.stdout)
        .expect("sessions after shutdown stdout is utf8");
    assert!(
        !sessions_after_shutdown_stdout.contains("session_id=slow-consumer"),
        "slow-consumer should be absent after recovered session shutdown: {sessions_after_shutdown_stdout:?}"
    );

    shutdown_cli_daemon(&data_dir, cleanup_child);
}

#[test]
fn cli_inspect_reports_not_found_for_fresh_in_process_daemon() {
    let data_dir = unique_test_dir("cli-inspect");
    let output = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("inspect")
        .arg("--data-dir")
        .arg(&data_dir)
        .arg("runtime-session")
        .output()
        .expect("run botster-hub inspect");

    assert!(
        output.status.success(),
        "inspect failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).expect("stdout is utf8");
    assert!(stdout.contains("inspect=session"));
    assert!(stdout.contains("session_id=runtime-session"));
    assert!(stdout.contains("found=false"));
    assert!(!stdout.contains(data_dir.to_string_lossy().as_ref()));
}

#[test]
fn cli_packages_enable_local_path_routes_through_running_daemon_and_persists() {
    let _guard = daemon_test_guard();
    let data_dir = unique_test_dir("cli-packages");
    let package_dir = unique_test_dir("local-package");
    write_local_plugin_package(&package_dir);
    let child = start_cli_daemon(&data_dir);

    let enable = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("packages")
        .arg("enable")
        .arg("--data-dir")
        .arg(&data_dir)
        .arg("--path")
        .arg(&package_dir)
        .output()
        .expect("run botster-hub packages enable");

    assert!(
        enable.status.success(),
        "enable failed: {}",
        String::from_utf8_lossy(&enable.stderr)
    );
    let stdout = String::from_utf8(enable.stdout).expect("stdout is utf8");
    assert!(stdout.contains("decision=package"));
    assert!(stdout.contains("package_name=runtime.plugin"));
    assert!(stdout.contains("action=enable"));
    assert!(stdout.contains("response=packages"));
    assert!(stdout.contains("package name=runtime.plugin"));
    assert!(stdout.contains("state=enabled"));
    assert!(stdout.contains("runnable_entrypoints=1"));
    assert!(stdout.contains("package_entrypoint package=runtime.plugin id=web kind=web_app launch_mode=background command=bin/botster-web args=2 working_directory=package_root environment=1 capabilities=1 may_supervise=true process_state=not_started"));
    assert!(!stdout.contains(package_dir.to_string_lossy().as_ref()));

    let status = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("status")
        .arg("--data-dir")
        .arg(&data_dir)
        .output()
        .expect("run botster-hub status after package enable");
    assert!(
        status.status.success(),
        "status failed: {}",
        String::from_utf8_lossy(&status.stderr)
    );
    let stdout = String::from_utf8(status.stdout).expect("stdout is utf8");
    assert!(stdout.contains("package_count=1"));
    assert!(stdout.contains("enabled_package_count=1"));

    let lifecycle = botster_hub::daemon_transport_request(
        &explicit_config(&data_dir),
        botster_hub::DaemonRequest::PluginLifecycleStatus,
    )
    .expect("daemon plugin lifecycle status");
    assert_eq!(
        lifecycle.kind,
        botster_hub::DaemonResponseKind::PluginLifecycle
    );
    assert!(
        lifecycle.lifecycle.iter().any(|plugin| {
            plugin.package_name == "runtime.plugin" && plugin.state == "enabled" && plugin.loaded
        }),
        "enabled package should load into daemon lifecycle without restart"
    );

    let list = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("packages")
        .arg("list")
        .arg("--data-dir")
        .arg(&data_dir)
        .output()
        .expect("run botster-hub packages list");

    assert!(
        list.status.success(),
        "packages list failed: {}",
        String::from_utf8_lossy(&list.stderr)
    );
    let stdout = String::from_utf8(list.stdout).expect("stdout is utf8");
    assert!(stdout.contains("response=packages"));
    assert!(stdout.contains("package_count=1"));
    assert!(stdout.contains("package name=runtime.plugin"));
    assert!(stdout.contains("state=enabled"));
    assert!(stdout.contains("runnable_entrypoints=1"));
    assert!(stdout.contains("process_state=not_started"));
    assert!(!stdout.contains(package_dir.to_string_lossy().as_ref()));

    let providers = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("providers")
        .arg("list")
        .arg("--data-dir")
        .arg(&data_dir)
        .output()
        .expect("run botster-hub providers list");
    assert!(
        providers.status.success(),
        "providers list failed: {}",
        String::from_utf8_lossy(&providers.stderr)
    );
    let stdout = String::from_utf8(providers.stdout).expect("stdout is utf8");
    assert!(stdout.contains("response=providers"));
    assert!(stdout.contains("package_count=0"));
    assert!(!stdout.contains(package_dir.to_string_lossy().as_ref()));

    shutdown_cli_daemon(&data_dir, child);

    let restarted = start_cli_daemon(&data_dir);
    let list_after_restart = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("packages")
        .arg("list")
        .arg("--data-dir")
        .arg(&data_dir)
        .output()
        .expect("run botster-hub packages list after restart");
    assert!(
        list_after_restart.status.success(),
        "packages list after restart failed: {}",
        String::from_utf8_lossy(&list_after_restart.stderr)
    );
    let stdout = String::from_utf8(list_after_restart.stdout).expect("stdout is utf8");
    assert!(stdout.contains("package_count=1"));
    assert!(stdout.contains("package name=runtime.plugin"));
    assert!(stdout.contains("state=enabled"));
    assert!(stdout.contains("runnable_entrypoints=1"));
    assert!(stdout.contains("package_entrypoint package=runtime.plugin id=web kind=web_app launch_mode=background command=bin/botster-web args=2 working_directory=package_root environment=1 capabilities=1 may_supervise=true process_state=not_started"));
    assert!(!stdout.contains(package_dir.to_string_lossy().as_ref()));

    shutdown_cli_daemon(&data_dir, restarted);
}

#[test]
fn package_entrypoint_supervision_starts_and_reports_running() {
    let _guard = daemon_test_guard();
    let data_dir = unique_test_dir("entrypoint-start");
    let package_dir = unique_test_dir("entrypoint-start-package");
    write_supervised_package(
        &package_dir,
        "runtime.supervised",
        "sh",
        &[
            "-c",
            "printf 'entrypoint-ready\\n'; while true; do sleep 1; done",
        ],
    );
    let child = start_cli_daemon(&data_dir);
    enable_supervised_package(&data_dir, &package_dir);

    let start = botster_hub::daemon_transport_request(
        &explicit_config(&data_dir),
        botster_hub::DaemonRequest::StartPackageEntrypoint {
            package_name: "runtime.supervised".to_string(),
            entrypoint_id: "web".to_string(),
            environment_overrides: BTreeMap::new(),
        },
    )
    .expect("start supervised entrypoint");
    let entrypoint = package_entrypoint(&start, "runtime.supervised");
    assert_eq!(entrypoint.process.state, "running");
    assert!(entrypoint.process.pid.is_some());
    assert!(entrypoint.process.started_at.is_some());

    let list = botster_hub::daemon_transport_request(
        &explicit_config(&data_dir),
        botster_hub::DaemonRequest::ListPackages,
    )
    .expect("list packages after supervised start");
    let entrypoint = package_entrypoint(&list, "runtime.supervised");
    assert_eq!(entrypoint.process.state, "running");
    assert!(entrypoint.process.pid.is_some());
    assert_eq!(
        package_action(&entrypoint.actions, "start_package_entrypoint").status,
        botster_hub::DaemonPackageActionStatus::Unavailable
    );
    let stop_action = package_action(&entrypoint.actions, "stop_package_entrypoint");
    assert_eq!(
        stop_action.status,
        botster_hub::DaemonPackageActionStatus::Available
    );
    assert_eq!(
        stop_action
            .request
            .as_ref()
            .expect("stop entrypoint request")
            .entrypoint_id
            .as_deref(),
        Some("web")
    );
    assert_eq!(
        package_action(&entrypoint.actions, "restart_package_entrypoint")
            .request
            .as_ref()
            .expect("restart entrypoint request")
            .request_type,
        "restart_package_entrypoint"
    );

    let cli_status = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("packages")
        .arg("entrypoint-status")
        .arg("--data-dir")
        .arg(&data_dir)
        .arg("runtime.supervised")
        .arg("web")
        .output()
        .expect("run botster-hub packages entrypoint-status");
    assert!(
        cli_status.status.success(),
        "entrypoint-status failed: {}",
        String::from_utf8_lossy(&cli_status.stderr)
    );
    let stdout = String::from_utf8(cli_status.stdout).expect("stdout is utf8");
    assert!(stdout.contains("response=packages"));
    assert!(stdout.contains("process_state=running"));
    assert!(stdout.contains("package_entrypoint_process package=runtime.supervised id=web"));

    shutdown_cli_daemon(&data_dir, child);
}

#[test]
fn daemon_list_apps_projects_installed_package_entrypoints() {
    let _guard = daemon_test_guard();
    let data_dir = unique_test_dir("list-apps");
    let package_dir = unique_test_dir("list-apps-package");
    write_app_registry_package(&package_dir);
    let child = start_cli_daemon(&data_dir);
    enable_supervised_package(&data_dir, &package_dir);

    let before_start = botster_hub::daemon_transport_request(
        &explicit_config(&data_dir),
        botster_hub::DaemonRequest::ListApps,
    )
    .expect("list apps before start");
    assert_eq!(before_start.kind, botster_hub::DaemonResponseKind::Apps);
    assert_eq!(before_start.apps.len(), 2);
    let web = app_row(&before_start, "web");
    assert_eq!(web.package_name, "runtime.apps");
    assert_eq!(web.app_id, "web");
    assert_eq!(web.entrypoint_id, "web");
    assert_eq!(web.kind, "web_app");
    assert_eq!(web.launch_mode, "background");
    assert_eq!(web.lifecycle_state, "not_started");
    assert_eq!(web.launch_target.kind, "web_app");
    assert_eq!(web.launch_target.local_url, None);

    let terminal = app_row(&before_start, "terminal");
    assert_eq!(terminal.kind, "terminal_app");
    assert_eq!(terminal.launch_mode, "foreground_stdio");
    assert_eq!(terminal.launch_target.kind, "terminal_app");
    assert_eq!(terminal.launch_target.local_url, None);
    assert!(terminal.blocked_reasons.is_empty());
    assert!(terminal.actions.is_empty());

    botster_hub::daemon_transport_request(
        &explicit_config(&data_dir),
        botster_hub::DaemonRequest::StartPackageEntrypoint {
            package_name: "runtime.apps".to_string(),
            entrypoint_id: "web".to_string(),
            environment_overrides: BTreeMap::new(),
        },
    )
    .expect("start web app entrypoint");

    let after_start = wait_for_app_local_url(&data_dir, "web", "http://127.0.0.1:49152");
    let web = app_row(&after_start, "web");
    assert_eq!(web.lifecycle_state, "running");
    assert_eq!(web.launch_target.kind, "web_app");
    assert_eq!(
        web.launch_target.local_url.as_deref(),
        Some("http://127.0.0.1:49152")
    );
    assert_eq!(
        package_action(&web.actions, "start_package_entrypoint").status,
        botster_hub::DaemonPackageActionStatus::Unavailable
    );
    assert_eq!(
        package_action(&web.actions, "stop_package_entrypoint").status,
        botster_hub::DaemonPackageActionStatus::Available
    );

    shutdown_cli_daemon(&data_dir, child);
}

#[test]
fn daemon_spawns_session_template_and_script_reads_botster_context() {
    let _guard = daemon_test_guard();
    let data_dir = unique_short_test_dir("session-template-context");
    let package_root = unique_test_dir("session-template-context-package");
    write_session_template_context_package(&package_root);
    let config = explicit_config(&data_dir);
    let child = start_cli_daemon(&data_dir);

    let enable = botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::EnablePackageLocalPath {
            path: package_root.clone(),
        },
    )
    .expect("enable session template package");
    assert_eq!(
        enable.kind,
        botster_hub::DaemonResponseKind::PackageDecision
    );

    let templates = botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::ListSessionTemplates,
    )
    .expect("list session templates");
    assert_eq!(
        templates.session_templates[0].template_id,
        "runtime.session-template/init"
    );

    let rejected = botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::ResolveSessionTemplate {
            template_id: "init".to_string(),
            request: botster_hub::DaemonSessionTemplateRequest {
                cwd: Some("/tmp".to_string()),
                ..botster_hub::DaemonSessionTemplateRequest::default()
            },
        },
    )
    .expect("unauthorized cwd response");
    assert_eq!(
        rejected.kind,
        botster_hub::DaemonResponseKind::OperatorError
    );
    assert_eq!(
        rejected.error.as_ref().map(|error| error.code.as_str()),
        Some("cwd_not_admitted")
    );

    let spawn = botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::SpawnSessionTemplate {
            template_id: "init".to_string(),
            session_id: "session-template-context".to_string(),
            request: botster_hub::DaemonSessionTemplateRequest {
                context: botster_hub::DaemonSessionTemplateContextInput {
                    prompt: Some("pipeline prompt".to_string()),
                    ticket_id: Some("ticket-123".to_string()),
                    ..botster_hub::DaemonSessionTemplateContextInput::default()
                },
                ..botster_hub::DaemonSessionTemplateRequest::default()
            },
        },
    )
    .expect("spawn session template");
    assert_eq!(spawn.kind, botster_hub::DaemonResponseKind::Spawned);

    let deadline = std::time::Instant::now() + Duration::from_secs(3);
    let context_output = package_root.join("context-output.json");
    let mut output = String::new();
    while std::time::Instant::now() < deadline {
        if let Ok(contents) = fs::read_to_string(&context_output) {
            output = contents;
            if output.contains("pipeline prompt") {
                break;
            }
        }
        thread::sleep(Duration::from_millis(30));
    }
    assert!(
        package_root.join("context-started.txt").exists(),
        "template script should have started"
    );
    assert!(
        output.contains("\"prompt\":\"pipeline prompt\""),
        "template script should read botster context through CLI, context_output={output:?}, context_error={:?}",
        fs::read_to_string(package_root.join("context-error.txt")).unwrap_or_default()
    );

    shutdown_cli_daemon(&data_dir, child);
}

#[test]
fn daemon_spawn_target_crud_persists_plain_non_git_directory_and_cli_lists_it() {
    let _guard = daemon_test_guard();
    let data_dir = unique_short_test_dir("spawn-target-crud");
    let target_root = unique_short_test_dir("plain-target");
    fs::create_dir_all(&target_root).expect("create plain target root");
    assert!(
        !target_root.join(".git").exists(),
        "test target intentionally has no git metadata"
    );
    let config = explicit_config(&data_dir);
    let child = start_cli_daemon(&data_dir);

    let created = botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::CreateSpawnTarget {
            target_id: Some("tgt_plain_directory".to_string()),
            label: Some("Plain Directory".to_string()),
            root: target_root.clone(),
            enabled: true,
            kind: Some("directory".to_string()),
            base_ref: None,
            metadata: BTreeMap::new(),
        },
    )
    .expect("create spawn target through daemon");
    assert_eq!(created.kind, botster_hub::DaemonResponseKind::SpawnTargets);
    assert_eq!(created.spawn_targets[0].target_id, "tgt_plain_directory");
    assert!(created.spawn_targets[0].enabled);

    let listed = botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::ListSpawnTargets,
    )
    .expect("list spawn targets through daemon");
    assert_eq!(listed.spawn_targets.len(), 1);
    assert_eq!(
        listed.spawn_targets[0].root,
        fs::canonicalize(&target_root).expect("canonical target root")
    );

    let cli_list = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("spawn-targets")
        .arg("list")
        .arg("--data-dir")
        .arg(&data_dir)
        .output()
        .expect("run spawn-targets list cli");
    assert!(
        cli_list.status.success(),
        "spawn-targets list failed: {}",
        String::from_utf8_lossy(&cli_list.stderr)
    );
    let stdout = String::from_utf8_lossy(&cli_list.stdout);
    assert!(stdout.contains("response=spawn_targets"));
    assert!(stdout.contains("id=tgt_plain_directory"));

    let validation = botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::ValidateSpawnTarget {
            target_id: "tgt_plain_directory".to_string(),
        },
    )
    .expect("validate enabled target")
    .spawn_target_validation
    .expect("validation response");
    assert!(validation.ok);
    assert_eq!(validation.status, "ok");

    let disabled = botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::UpdateSpawnTarget {
            target_id: "tgt_plain_directory".to_string(),
            label: Some("Plain Directory Disabled".to_string()),
            root: None,
            enabled: Some(false),
            kind: None,
            base_ref: None,
            metadata: None,
        },
    )
    .expect("disable target");
    assert!(!disabled.spawn_targets[0].enabled);
    let validation = botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::ValidateSpawnTarget {
            target_id: "tgt_plain_directory".to_string(),
        },
    )
    .expect("validate disabled target")
    .spawn_target_validation
    .expect("validation response");
    assert!(!validation.ok);
    assert_eq!(validation.status, "disabled");

    let enabled = botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::UpdateSpawnTarget {
            target_id: "tgt_plain_directory".to_string(),
            label: None,
            root: None,
            enabled: Some(true),
            kind: None,
            base_ref: None,
            metadata: None,
        },
    )
    .expect("re-enable target");
    assert!(enabled.spawn_targets[0].enabled);

    shutdown_cli_daemon(&data_dir, child);
    let restarted = start_cli_daemon(&data_dir);
    let reloaded = botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::ShowSpawnTarget {
            target_id: "tgt_plain_directory".to_string(),
        },
    )
    .expect("show reloaded target");
    assert_eq!(reloaded.spawn_targets.len(), 1);
    assert_eq!(reloaded.spawn_targets[0].label, "Plain Directory Disabled");

    let deleted = botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::DeleteSpawnTarget {
            target_id: "tgt_plain_directory".to_string(),
        },
    )
    .expect("delete target");
    assert_eq!(deleted.spawn_targets[0].target_id, "tgt_plain_directory");
    let validation = botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::ValidateSpawnTarget {
            target_id: "tgt_plain_directory".to_string(),
        },
    )
    .expect("validate deleted target")
    .spawn_target_validation
    .expect("validation response");
    assert!(!validation.ok);
    assert_eq!(validation.status, "not_found");
    shutdown_cli_daemon(&data_dir, restarted);
}

#[test]
fn daemon_worktree_crud_scopes_paths_to_spawn_targets_without_requiring_git() {
    let _guard = daemon_test_guard();
    let data_dir = unique_short_test_dir("worktree-crud");
    let target_root = unique_short_test_dir("worktree-target");
    let plain_worktree = target_root.join("plain");
    let git_worktree = target_root.join("gitish");
    let outside_dir = unique_short_test_dir("worktree-outside");
    fs::create_dir_all(&plain_worktree).expect("create plain worktree");
    fs::create_dir_all(git_worktree.join(".git")).expect("create git metadata dir");
    fs::write(git_worktree.join(".git/HEAD"), "ref: refs/heads/main\n").expect("write HEAD");
    fs::create_dir_all(&outside_dir).expect("create outside dir");
    let escape_link = target_root.join("escape-link");
    std::os::unix::fs::symlink(&outside_dir, &escape_link).expect("create symlink escape");
    assert!(
        !plain_worktree.join(".git").exists(),
        "plain worktree intentionally has no git metadata"
    );
    let config = explicit_config(&data_dir);
    let child = start_cli_daemon(&data_dir);

    botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::CreateSpawnTarget {
            target_id: Some("tgt_worktrees".to_string()),
            label: Some("Worktree Target".to_string()),
            root: target_root.clone(),
            enabled: true,
            kind: Some("directory".to_string()),
            base_ref: None,
            metadata: BTreeMap::new(),
        },
    )
    .expect("create spawn target for worktrees");

    let created = botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::CreateWorktree {
            worktree_id: Some("wt_plain".to_string()),
            target_id: "tgt_worktrees".to_string(),
            label: Some("Plain Worktree".to_string()),
            path: plain_worktree.clone(),
            metadata: BTreeMap::new(),
        },
    )
    .expect("create plain worktree through daemon");
    assert_eq!(created.kind, botster_hub::DaemonResponseKind::Worktrees);
    assert_eq!(created.worktrees[0].worktree_id, "wt_plain");
    assert_eq!(created.worktrees[0].target_id, "tgt_worktrees");
    assert_eq!(created.worktrees[0].status, "present");
    let created_event = created
        .events
        .iter()
        .find_map(|event| match event {
            botster_hub::DaemonEvent::WorktreeLifecycle { event } => Some(event),
            _ => None,
        })
        .expect("create response should include worktree lifecycle event");
    assert_eq!(created_event.event, "worktree_created");
    assert_eq!(created_event.worktree_id.as_deref(), Some("wt_plain"));
    assert_eq!(created_event.target_id.as_deref(), Some("tgt_worktrees"));
    assert_eq!(created_event.status.as_deref(), Some("present"));
    assert_eq!(created_event.display_path.as_deref(), Some("plain"));
    let created_events_json =
        serde_json::to_string(&created.events).expect("serialize created worktree events");
    assert!(
        !created_events_json.contains(target_root.to_string_lossy().as_ref()),
        "worktree lifecycle events must not expose raw spawn target paths: {created_events_json}"
    );
    assert!(
        created.worktrees[0].git.is_none(),
        "git metadata must be optional for plain directories"
    );

    let listed =
        botster_hub::daemon_transport_request(&config, botster_hub::DaemonRequest::ListWorktrees)
            .expect("list worktrees through daemon");
    assert_eq!(listed.worktrees.len(), 1);
    assert_eq!(listed.worktrees[0].worktree_id, "wt_plain");

    let shown = botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::ShowWorktree {
            worktree_id: "wt_plain".to_string(),
        },
    )
    .expect("show worktree through daemon");
    assert_eq!(
        shown.worktrees[0].path,
        fs::canonicalize(&plain_worktree).expect("canonical plain worktree")
    );

    let deleted = botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::DeleteWorktree {
            worktree_id: "wt_plain".to_string(),
        },
    )
    .expect("delete worktree record through daemon");
    assert_eq!(deleted.worktrees[0].worktree_id, "wt_plain");
    let deleted_event = deleted
        .events
        .iter()
        .find_map(|event| match event {
            botster_hub::DaemonEvent::WorktreeLifecycle { event } => Some(event),
            _ => None,
        })
        .expect("delete response should include worktree lifecycle event");
    assert_eq!(deleted_event.event, "worktree_deleted");
    assert_eq!(deleted_event.worktree_id.as_deref(), Some("wt_plain"));
    assert!(
        plain_worktree.exists(),
        "worktree record deletion must not delete filesystem contents"
    );
    let delete_missing = botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::DeleteWorktree {
            worktree_id: "wt_plain".to_string(),
        },
    )
    .expect("delete missing worktree response");
    assert_eq!(
        delete_missing.kind,
        botster_hub::DaemonResponseKind::OperatorError
    );
    let delete_failed_event = delete_missing
        .events
        .iter()
        .find_map(|event| match event {
            botster_hub::DaemonEvent::WorktreeLifecycle { event } => Some(event),
            _ => None,
        })
        .expect("delete failure response should include worktree lifecycle event");
    assert_eq!(delete_failed_event.event, "worktree_delete_failed");
    assert_eq!(delete_failed_event.worktree_id.as_deref(), Some("wt_plain"));
    assert_eq!(
        delete_failed_event.failure_kind.as_deref(),
        Some("not_found")
    );

    let git_created = botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::CreateWorktree {
            worktree_id: Some("wt_gitish".to_string()),
            target_id: "tgt_worktrees".to_string(),
            label: Some("Git Metadata Worktree".to_string()),
            path: git_worktree.clone(),
            metadata: BTreeMap::new(),
        },
    )
    .expect("create git metadata worktree through daemon");
    assert_eq!(
        git_created.worktrees[0]
            .git
            .as_ref()
            .and_then(|git| git.branch.as_deref()),
        Some("main")
    );

    let traversal = botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::CreateWorktree {
            worktree_id: Some("wt_escape_parent".to_string()),
            target_id: "tgt_worktrees".to_string(),
            label: None,
            path: target_root.join(".."),
            metadata: BTreeMap::new(),
        },
    )
    .expect("traversal rejection response");
    assert_eq!(
        traversal.kind,
        botster_hub::DaemonResponseKind::OperatorError
    );
    assert_eq!(
        traversal.error.as_ref().map(|error| error.code.as_str()),
        Some("path_outside_target")
    );
    let create_failed_event = traversal
        .events
        .iter()
        .find_map(|event| match event {
            botster_hub::DaemonEvent::WorktreeLifecycle { event } => Some(event),
            _ => None,
        })
        .expect("create failure response should include worktree lifecycle event");
    assert_eq!(create_failed_event.event, "worktree_create_failed");
    assert_eq!(
        create_failed_event.worktree_id.as_deref(),
        Some("wt_escape_parent")
    );
    assert_eq!(
        create_failed_event.target_id.as_deref(),
        Some("tgt_worktrees")
    );
    assert_eq!(
        create_failed_event.failure_kind.as_deref(),
        Some("path_outside_target")
    );
    let failure_events_json =
        serde_json::to_string(&traversal.events).expect("serialize failure events");
    assert!(
        !failure_events_json.contains(target_root.to_string_lossy().as_ref())
            && !failure_events_json.contains("/Users/"),
        "failure lifecycle events must not expose raw local paths: {failure_events_json}"
    );

    let symlink_escape = botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::CreateWorktree {
            worktree_id: Some("wt_symlink_escape".to_string()),
            target_id: "tgt_worktrees".to_string(),
            label: None,
            path: escape_link,
            metadata: BTreeMap::new(),
        },
    )
    .expect("symlink escape rejection response");
    assert_eq!(
        symlink_escape
            .error
            .as_ref()
            .map(|error| error.code.as_str()),
        Some("path_outside_target")
    );

    shutdown_cli_daemon(&data_dir, child);
    let restarted = start_cli_daemon(&data_dir);
    let reloaded = botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::ShowWorktree {
            worktree_id: "wt_gitish".to_string(),
        },
    )
    .expect("show persisted worktree after restart");
    assert_eq!(reloaded.worktrees[0].status, "present");
    fs::remove_dir_all(&git_worktree).expect("remove persisted worktree path");
    shutdown_cli_daemon(&data_dir, restarted);
    let restarted_missing = start_cli_daemon(&data_dir);
    let missing = botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::ShowWorktree {
            worktree_id: "wt_gitish".to_string(),
        },
    )
    .expect("show missing worktree after restart");
    assert_eq!(missing.worktrees[0].status, "missing");

    shutdown_cli_daemon(&data_dir, restarted_missing);
}

#[test]
fn daemon_spawns_repo_local_session_template_after_state_reload() {
    let _guard = daemon_test_guard();
    let data_dir = unique_short_test_dir("repo-session-template");
    let package_root = unique_test_dir("repo-session-template-package");
    let repo_root = std::env::current_dir()
        .expect("current dir")
        .join(unique_test_dir("repo-session-template-repo"));
    write_session_template_context_package(&package_root);
    fs::create_dir_all(repo_root.join(".botster")).expect("create repo .botster dir");
    fs::create_dir_all(repo_root.join("bin")).expect("create repo bin dir");
    let script = repo_root.join("bin/repo-template.sh");
    fs::write(
        &script,
        "#!/bin/sh\nprintf 'repo:%s\\n' \"$BOTSTER_MODE\" > repo-template-output.txt\nsleep 1\n",
    )
    .expect("write repo template script");
    let mut permissions = fs::metadata(&script)
        .expect("script metadata")
        .permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(&script, permissions).expect("chmod repo script");
    fs::write(
        repo_root.join(".botster/session-templates.json"),
        serde_json::to_string_pretty(&serde_json::json!({
            "session_templates": [{
                "id": "init",
                "command": "bin/repo-template.sh",
                "environment": { "BOTSTER_MODE": "repo" },
                "allowed_environment_overrides": ["BOTSTER_MODE"]
            }]
        }))
        .expect("serialize repo templates"),
    )
    .expect("write repo templates");

    let config = explicit_config(&data_dir);
    let store = FileHubStateStore::for_data_directory(&config.data_directory);
    store
        .update(&config, |state| {
            state.spawn_targets = vec![SpawnTarget {
                target_id: "repo:runtime".to_string(),
                label: "Repo Runtime".to_string(),
                root: repo_root.clone(),
                enabled: true,
                kind: "directory".to_string(),
                base_ref: None,
                metadata: BTreeMap::new(),
            }];
        })
        .expect("persist admitted repo target before daemon start");
    let child = start_cli_daemon(&data_dir);

    let enable = botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::EnablePackageLocalPath {
            path: package_root.clone(),
        },
    )
    .expect("enable package session template baseline");
    assert_eq!(
        enable.kind,
        botster_hub::DaemonResponseKind::PackageDecision
    );

    let templates = botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::ListSessionTemplates,
    )
    .expect("list session templates");
    assert_eq!(templates.session_templates.len(), 1);
    assert_eq!(templates.session_templates[0].source, "repo");
    assert_eq!(templates.session_templates[0].target_id, "repo:runtime");

    let spawn = botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::SpawnSessionTemplate {
            template_id: "init".to_string(),
            session_id: "repo-session-template".to_string(),
            request: botster_hub::DaemonSessionTemplateRequest {
                environment: BTreeMap::from([("BOTSTER_MODE".to_string(), "explicit".to_string())]),
                ..botster_hub::DaemonSessionTemplateRequest::default()
            },
        },
    )
    .expect("spawn repo session template");
    assert_eq!(
        spawn.kind,
        botster_hub::DaemonResponseKind::Spawned,
        "spawn response error={:?}",
        spawn.error
    );

    let deadline = std::time::Instant::now() + Duration::from_secs(3);
    let output_path = repo_root.join("repo-template-output.txt");
    let mut output = String::new();
    while std::time::Instant::now() < deadline {
        if let Ok(contents) = fs::read_to_string(&output_path) {
            output = contents;
            if output.contains("repo:explicit") {
                break;
            }
        }
        thread::sleep(Duration::from_millis(30));
    }
    assert_eq!(output.trim(), "repo:explicit");

    shutdown_cli_daemon(&data_dir, child);
}

#[test]
fn daemon_resolves_terminal_app_foreground_launch_contract() {
    let _guard = daemon_test_guard();
    let data_dir = unique_test_dir("resolve-terminal-app");
    let package_dir = unique_test_dir("resolve-terminal-app-package");
    write_botster_tui_package(&package_dir);
    let child = start_cli_daemon(&data_dir);
    enable_supervised_package(&data_dir, &package_dir);

    let response = botster_hub::daemon_transport_request(
        &explicit_config(&data_dir),
        botster_hub::DaemonRequest::ResolveAppLaunch {
            package_name: "botster-tui".to_string(),
            entrypoint_id: "botster-tui".to_string(),
        },
    )
    .expect("resolve terminal app launch");
    assert_eq!(
        response.kind,
        botster_hub::DaemonResponseKind::ResolvedAppLaunch
    );
    let launch = response
        .resolved_app_launch
        .expect("resolved foreground launch");
    assert_eq!(launch.package_name, "botster-tui");
    assert_eq!(launch.kind, "terminal_app");
    assert_eq!(launch.launch_mode, "foreground_stdio");
    assert_eq!(launch.command, "sh");
    let connection: serde_json::Value = serde_json::from_str(
        launch
            .environment
            .get("BOTSTER_HUB_CONNECTION")
            .expect("Hub connection injection"),
    )
    .expect("decode Hub connection injection");
    assert_eq!(
        connection["transport"]["type"],
        serde_json::Value::String("unix_socket".to_string())
    );
    assert!(
        connection["transport"]["path"]
            .as_str()
            .expect("Hub connection path")
            .starts_with('/')
    );
    assert!(launch.environment.contains_key("BOTSTER_HUB_DATA_DIR"));
    assert_eq!(
        launch
            .environment
            .get("BOTSTER_TUI_MODE")
            .map(String::as_str),
        Some("headless")
    );

    shutdown_cli_daemon(&data_dir, child);

    let restarted = start_cli_daemon(&data_dir);
    let apps = botster_hub::daemon_transport_request(
        &explicit_config(&data_dir),
        botster_hub::DaemonRequest::ListApps,
    )
    .expect("list apps after daemon restart");
    let app = app_row(&apps, "botster-tui");
    assert_eq!(app.package_name, "botster-tui");
    assert_eq!(app.entrypoint_id, "botster-tui");
    assert_eq!(app.kind, "terminal_app");
    let app_route = app.route.as_ref().expect("app route descriptor");
    assert_eq!(app_route.route_id, "app:botster-tui");
    assert_eq!(
        app_route.route_path,
        "/packages/botster-tui/apps/botster-tui"
    );
    assert_eq!(app_route.target.kind, "app_entrypoint");
    assert_eq!(
        app_route.target.entrypoint_id.as_deref(),
        Some("botster-tui")
    );
    assert_eq!(app_route.layout_mode, "app_entrypoint");
    assert!(app_route.enabled);
    assert!(!app_route.blocked);

    let reloaded = botster_hub::daemon_transport_request(
        &explicit_config(&data_dir),
        botster_hub::DaemonRequest::ResolveAppLaunch {
            package_name: "botster-tui".to_string(),
            entrypoint_id: "botster-tui".to_string(),
        },
    )
    .expect("resolve terminal app launch after daemon restart");
    assert_eq!(
        reloaded.kind,
        botster_hub::DaemonResponseKind::ResolvedAppLaunch
    );
    assert_eq!(
        reloaded
            .resolved_app_launch
            .expect("resolved foreground launch after restart")
            .command,
        "sh"
    );
    let resolved_route = botster_hub::daemon_transport_request(
        &explicit_config(&data_dir),
        botster_hub::DaemonRequest::ResolvePackageRoute {
            package_name: "botster-tui".to_string(),
            route_id: "app:botster-tui".to_string(),
        },
    )
    .expect("resolve terminal app route after daemon restart");
    assert_eq!(
        resolved_route.kind,
        botster_hub::DaemonResponseKind::ResolvedPackageRoute
    );
    assert_eq!(
        resolved_route
            .resolved_package_route
            .expect("resolved app route")
            .route_path,
        "/packages/botster-tui/apps/botster-tui"
    );

    shutdown_cli_daemon(&data_dir, restarted);
}

#[test]
fn cli_apps_list_show_and_open_web_use_structured_app_url() {
    let _guard = daemon_test_guard();
    let data_dir = unique_test_dir("cli-apps-web");
    let package_dir = unique_test_dir("cli-apps-web-package");
    write_app_registry_package(&package_dir);
    let child = start_cli_daemon(&data_dir);
    enable_supervised_package(&data_dir, &package_dir);

    let list = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("apps")
        .arg("list")
        .arg("--data-dir")
        .arg(&data_dir)
        .output()
        .expect("run apps list");
    assert!(
        list.status.success(),
        "apps list failed: {}",
        command_output_text(&list)
    );
    let list_text = command_output_text(&list);
    assert!(list_text.contains("response=apps"));
    assert!(list_text.contains("app package=runtime.apps app_id=web"));
    assert!(list_text.contains("app package=runtime.apps app_id=terminal"));

    let show = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("apps")
        .arg("show")
        .arg("--data-dir")
        .arg(&data_dir)
        .arg("runtime.apps/web")
        .output()
        .expect("run apps show");
    assert!(
        show.status.success(),
        "apps show failed: {}",
        command_output_text(&show)
    );
    let show_text = command_output_text(&show);
    assert!(show_text.contains("response=app"));
    assert!(show_text.contains("package=runtime.apps"));
    assert!(show_text.contains("app_id=web"));

    let open = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("apps")
        .arg("open")
        .arg("--data-dir")
        .arg(&data_dir)
        .arg("web")
        .output()
        .expect("run apps open web");
    assert!(
        open.status.success(),
        "apps open web failed: {}",
        command_output_text(&open)
    );
    let open_text = command_output_text(&open);
    assert!(open_text.contains("app_url=http://127.0.0.1:49152"));
    assert!(!open_text.contains("http://127.0.0.1:59999"));
    let apps = botster_hub::daemon_transport_request(
        &explicit_config(&data_dir),
        botster_hub::DaemonRequest::ListApps,
    )
    .expect("list apps after cli open");
    assert_eq!(
        app_row(&apps, "web").launch_target.local_url.as_deref(),
        Some("http://127.0.0.1:49152")
    );

    shutdown_cli_daemon(&data_dir, child);
}

#[test]
fn cli_apps_open_web_injects_hub_connection_environment() {
    let _guard = daemon_test_guard();
    let data_dir = unique_test_dir("cli-apps-web-hub-env");
    let package_dir = unique_test_dir("cli-apps-web-hub-env-package");
    write_hub_env_web_app_package(&package_dir);
    let child = start_cli_daemon(&data_dir);
    enable_supervised_package(&data_dir, &package_dir);

    let open = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("apps")
        .arg("open")
        .arg("--data-dir")
        .arg(&data_dir)
        .arg("runtime.hub-env/web")
        .output()
        .expect("run apps open web with hub env fixture");
    assert!(
        open.status.success(),
        "apps open web failed: {}",
        command_output_text(&open)
    );
    let open_text = command_output_text(&open);
    assert!(open_text.contains("app_url=http://127.0.0.1:49153"));
    assert!(!open_text.contains("BOTSTER_HUB_CONNECTION must"));

    let status = botster_hub::daemon_transport_request(
        &explicit_config(&data_dir),
        botster_hub::DaemonRequest::PackageEntrypointStatus {
            package_name: "runtime.hub-env".to_string(),
            entrypoint_id: "web".to_string(),
        },
    )
    .expect("inspect web app entrypoint status");
    let entrypoint = package_entrypoint(&status, "runtime.hub-env");
    assert_eq!(entrypoint.process.state, "running");
    assert!(
        entrypoint
            .process
            .diagnostics
            .iter()
            .all(|diagnostic| { !diagnostic.message.contains("BOTSTER_HUB_CONNECTION must") })
    );

    shutdown_cli_daemon(&data_dir, child);
}

#[test]
fn cli_apps_open_terminal_uses_foreground_launch_contract() {
    let _guard = daemon_test_guard();
    let data_dir = unique_test_dir("cli-apps-terminal");
    let package_dir = unique_test_dir("cli-apps-terminal-package");
    write_botster_tui_package(&package_dir);
    let child = start_cli_daemon(&data_dir);
    enable_supervised_package(&data_dir, &package_dir);

    let open = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("apps")
        .arg("open")
        .arg("--data-dir")
        .arg(&data_dir)
        .arg("botster-tui")
        .output()
        .expect("run apps open terminal");
    assert!(
        open.status.success(),
        "apps open terminal failed: {}",
        command_output_text(&open)
    );
    assert!(command_output_text(&open).contains("botster-tui-fixture"));

    let removed_alias = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("tui")
        .arg("--data-dir")
        .arg(&data_dir)
        .output()
        .expect("run removed tui alias");
    assert!(
        !removed_alias.status.success(),
        "removed tui alias should fail: {}",
        command_output_text(&removed_alias)
    );
    let removed_alias_text = command_output_text(&removed_alias);
    assert!(removed_alias_text.contains("unknown command"));
    assert!(removed_alias_text.contains("usage: botster-hub <"));
    assert!(!removed_alias_text.contains("botster-tui-fixture"));
    assert!(!removed_alias_text.contains("first-party host profile ready"));

    shutdown_cli_daemon(&data_dir, child);
}

#[test]
fn cli_no_arg_non_tty_rejects_before_creating_runtime_state() {
    let data_dir = unique_short_test_dir("no-tty");
    let output = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .env("BOTSTER_HUB_DATA_DIR", &data_dir)
        .output()
        .expect("run no-arg hub without a TTY");
    assert!(
        !output.status.success(),
        "no-arg non-TTY invocation should fail: {}",
        command_output_text(&output)
    );
    let text = command_output_text(&output);
    assert!(text.contains("requires terminal stdin and stdout"));
    assert!(text.contains("scripts must use an explicit subcommand"));
    assert!(
        !data_dir.exists(),
        "non-TTY invocation created runtime state"
    );
}

#[test]
fn cli_operator_console_starts_reuses_detaches_handles_ctrl_c_and_stops() {
    let _guard = daemon_test_guard();
    ensure_session_worker_binary();
    let data_dir = unique_short_test_dir("console");
    let package_dir = unique_short_test_dir("console-package").join("package with spaces");
    let web_package_dir = unique_short_test_dir("console-web-package").join("web package");
    write_botster_tui_package_with_script(
        &package_dir,
        "stty raw -echo; printf 'console-terminal-failure\\r\\n'; exit 7",
    );
    write_botster_web_package(&web_package_dir);
    let web_manifest_path = web_package_dir.join("botster-package.json");
    let mut web_manifest: serde_json::Value = serde_json::from_slice(
        &fs::read(&web_manifest_path).expect("read console botster-web manifest"),
    )
    .expect("parse console botster-web manifest");
    let delay = web_manifest["runnable_entrypoints"][0]["environment"]
        .as_array_mut()
        .expect("botster-web environment array")
        .iter_mut()
        .find(|value| {
            value.get("name").and_then(serde_json::Value::as_str)
                == Some("BOTSTER_WEB_TEST_STARTUP_DELAY_MS")
        })
        .expect("botster-web startup delay environment declaration");
    delay["default"] = serde_json::Value::String("1500".to_string());
    fs::write(
        &web_manifest_path,
        serde_json::to_vec_pretty(&web_manifest).expect("serialize delayed botster-web manifest"),
    )
    .expect("write delayed botster-web manifest");

    let mut daemon_cleanup = OwnedOperatorConsoleDaemon::new(&data_dir);
    let mut first = OperatorConsolePty::spawn(&data_dir);
    daemon_cleanup.wait_until_daemon_ready(&mut first);
    first.wait_for("daemon=started");
    first.wait_for("prerequisite botster-web=missing");
    first.wait_for("botster-hub> ");
    first.send_and_wait_for_prompt(b"open tui\n");
    first.wait_for("botster-hub open error: app botster-tui is not installed or enabled");
    first.send_and_wait_for_prompt(b"open web\n");
    first
        .wait_for("botster-hub open error: app botster-web/web-client is not installed or enabled");
    first.send_and_wait_for_prompt(
        format!(
            "packages install --path {}\n",
            shell_words::quote(&package_dir.to_string_lossy())
        )
        .as_bytes(),
    );
    first.wait_for("decision=package");
    first.send_and_wait_for_prompt(b"packages enable botster-tui\n");
    first.wait_for("state=enabled");
    first.send_and_wait_for_prompt(b"packages list\n");
    first.wait_for("response=packages");
    first.send_and_wait_for_prompt(b"packages show botster-tui\n");
    first.wait_for("package_name=botster-tui");
    first.send_and_wait_for_prompt(b"sessions spawn --session-id console-sentinel -- sleep 300\n");
    first.wait_for("session_id=console-sentinel");
    let mut sentinel_cleanup = SessionCleanupGuard::new(&data_dir, "console-sentinel");
    first.send_and_wait_for_prompt(b"sessions list\n");
    first.wait_for("session id=console-sentinel lifecycle=running");
    first.send_and_wait_for_prompt(b"apps list\n");
    first.wait_for("response=apps");
    first.wait_for("kind=terminal_app");
    first.send_and_wait_for_prompt(b"open tui\n");
    first.wait_for("console-terminal-failure");
    first.wait_for("foreground app exited with code 7");
    first.send_and_wait_for_prompt(b"status\r");
    first.wait_for("event=status");
    let explicit_open = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("apps")
        .arg("open")
        .arg("--data-dir")
        .arg(&data_dir)
        .arg("botster-tui")
        .output()
        .expect("run explicit foreground app after console handoff");
    assert_eq!(
        explicit_open.status.code(),
        Some(7),
        "explicit CLI did not preserve foreground app exit code: {}",
        command_output_text(&explicit_open)
    );
    write_botster_tui_package_with_script(
        &package_dir,
        "stty raw -echo; printf 'foreground-clean\\r\\n'; exit 0",
    );
    first.send_and_wait_for_prompt(b"packages reload botster-tui\n");
    first.wait_for("action=reload");
    first.send_and_wait_for_prompt(b"apps open botster-tui\n");
    first.wait_for("foreground-clean");
    first.send_and_wait_for_prompt(b"status\r");
    first.wait_for("event=status");
    write_botster_tui_package_with_script(&package_dir, DETERMINISTIC_FOREGROUND_INTERRUPT_SCRIPT);
    first.send_and_wait_for_prompt(b"packages reload botster-tui\n");
    first.wait_for("action=reload");
    let prompt_after_foreground_interrupt = first.prompt_count() + 1;
    first.send(b"apps open botster-tui\n");
    first.wait_for("foreground-forward-ready");
    let foreground_interrupt_checkpoint = first.output_checkpoint();
    first.send(&[3]);
    first.wait_for_output_after(
        foreground_interrupt_checkpoint,
        "foreground app exited with code 130",
    );
    first.wait_for_output_after(foreground_interrupt_checkpoint, "botster-hub> ");
    assert_eq!(
        first.prompt_count(),
        prompt_after_foreground_interrupt,
        "foreground interrupt printed an unexpected number of prompts: {}",
        first.text()
    );
    assert!(
        !first
            .text()
            .contains("interrupt requested; finishing safely"),
        "foreground Ctrl-C was handled as inline console work: {}",
        first.text()
    );
    first.send_and_wait_for_prompt(b"sessions list\r");
    first.wait_for("session id=console-sentinel lifecycle=running");
    first.send_and_wait_for_prompt(
        format!(
            "packages install --path {}\n",
            shell_words::quote(&web_package_dir.to_string_lossy())
        )
        .as_bytes(),
    );
    first.wait_for_occurrences("package_name=botster-web", 1);
    first.send_and_wait_for_prompt(b"packages enable botster-web\n");
    first.wait_for_occurrences("package_name=botster-web", 2);
    let prompt_after_inline_interrupt = first.prompt_count() + 1;
    first.send(b"up\n");
    thread::sleep(Duration::from_millis(100));
    first.send(&[3]);
    first.wait_for("interrupt requested; finishing safely");
    first.wait_for("runtime=ready");
    first.wait_for_occurrences("botster-hub> ", prompt_after_inline_interrupt);
    first.send_and_wait_for_prompt(b"open web\n");
    first.wait_for("app_url=http://");
    first.send_and_wait_for_prompt(b"sessions list\n");
    first.wait_for("session id=console-sentinel lifecycle=running");
    let prompt_after_idle_interrupt = first.prompt_count() + 1;
    first.send(b"partial input");
    first.send(&[3]);
    first.wait_for("^C");
    first.wait_for_occurrences("botster-hub> ", prompt_after_idle_interrupt);
    first.send_and_wait_for_prompt(b"sessions list\n");
    first.wait_for("session id=console-sentinel lifecycle=running");
    first.send_and_wait_for_prompt(b"botster-hub status\n");
    first.wait_for("omit the repeated `botster-hub` prefix");
    first.send_and_wait_for_prompt(b"packages list \"unterminated\n");
    first.wait_for("console parse error");
    first.send_and_wait_for_prompt(b"status --data-dir /tmp/not-this-console\n");
    first.wait_for("this console is pinned to");
    first.send_and_wait_for_prompt(b"not-a-command\n");
    first.wait_for(
        format!(
            "run `botster-hub not-a-command --data-dir {}` outside the console",
            data_dir.display()
        )
        .as_str(),
    );
    first.send_and_wait_for_prompt(b"status\n");
    first.wait_for("event=status");
    first.send_and_wait_for_prompt(b"sessions shutdown console-sentinel\n");
    first.wait_for("response=events");
    first.send_and_wait_for_prompt(b"sessions list\n");
    first.wait_for("session id=console-sentinel lifecycle=exited");
    sentinel_cleanup.disarm();
    first.send(&[4]);
    first.wait_for("detached=daemon_running");
    first.wait_for_exit();

    let status = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("status")
        .arg("--data-dir")
        .arg(&data_dir)
        .output()
        .expect("query daemon after console detach");
    assert!(
        status.status.success(),
        "detached console stopped daemon: {}",
        command_output_text(&status)
    );

    let mut exit_console = OperatorConsolePty::spawn(&data_dir);
    daemon_cleanup.wait_until_daemon_ready(&mut exit_console);
    exit_console.wait_for("daemon=reused");
    exit_console.wait_for("botster-hub> ");
    exit_console.send(b"exit\n");
    exit_console.wait_for("detached=daemon_running");
    exit_console.wait_for_exit();

    let mut second = OperatorConsolePty::spawn(&data_dir);
    daemon_cleanup.wait_until_daemon_ready(&mut second);
    let shutdown_daemon_pid = *daemon_cleanup
        .owned_pids()
        .last()
        .expect("capture daemon generation before console shutdown");
    second.wait_for("daemon=reused");
    second.wait_for("botster-hub> ");
    second.send(b"shutdown\n");
    second.wait_for("response=shutdown");
    second.wait_for_exit();
    assert!(
        !process_exists(shutdown_daemon_pid),
        "console shutdown returned before owned daemon pid {shutdown_daemon_pid} exited"
    );
    assert!(
        !data_dir.join(".botster-hub-runtime-daemon.json").exists(),
        "console shutdown left owned daemon metadata"
    );
    assert!(
        !explicit_config(&data_dir)
            .transports
            .local_socket
            .as_ref()
            .expect("operator console local socket binding")
            .path
            .exists(),
        "console shutdown left the owned daemon socket"
    );

    let stopped = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("status")
        .arg("--data-dir")
        .arg(&data_dir)
        .output()
        .expect("query daemon after console down");
    assert!(
        !stopped.status.success(),
        "console shutdown left daemon running: {}",
        command_output_text(&stopped)
    );

    let mut third = OperatorConsolePty::spawn(&data_dir);
    daemon_cleanup.wait_until_daemon_ready(&mut third);
    third.wait_for("daemon=started");
    third.wait_for("botster-hub> ");
    third.send(b"down\n");
    third.wait_for("response=shutdown");
    third.wait_for_exit();
    let stopped = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("status")
        .arg("--data-dir")
        .arg(&data_dir)
        .output()
        .expect("query daemon after console down");
    assert!(
        !stopped.status.success(),
        "console down left daemon running: {}",
        command_output_text(&stopped)
    );
    daemon_cleanup.assert_cleaned();
    fs::remove_dir_all(&data_dir).expect("remove isolated operator console data directory");
    fs::remove_dir_all(
        package_dir
            .parent()
            .expect("operator console package has a parent"),
    )
    .expect("remove isolated operator console package directory");
    fs::remove_dir_all(
        web_package_dir
            .parent()
            .expect("operator console web package has a parent"),
    )
    .expect("remove isolated operator console web package directory");
}

#[test]
fn cli_operator_console_reuses_before_worker_lookup_and_reports_missing_worker() {
    let _guard = daemon_test_guard();
    ensure_session_worker_binary();
    let data_dir = unique_short_test_dir("console-worker-reuse");
    let child = start_cli_daemon(&data_dir);
    let isolated_bin_dir = unique_short_test_dir("console-bin");
    fs::create_dir_all(&isolated_bin_dir).expect("create isolated console binary directory");
    let isolated_hub = isolated_bin_dir.join("botster-hub");
    fs::copy(env!("CARGO_BIN_EXE_botster-hub"), &isolated_hub)
        .expect("copy hub without its worker sibling");

    let mut reused = OperatorConsolePty::spawn_binary(&isolated_hub, &data_dir);
    reused.wait_for("daemon=reused");
    assert!(
        !reused.text().contains("missing botster-session-worker"),
        "reused daemon unexpectedly required a local worker: {}",
        reused.text()
    );
    reused.send(b"exit\n");
    reused.wait_for("detached=daemon_running");
    reused.wait_for_exit();
    shutdown_cli_daemon(&data_dir, child);

    let fresh_data_dir = unique_short_test_dir("console-worker-missing");
    let mut missing = OperatorConsolePty::spawn_binary(&isolated_hub, &fresh_data_dir);
    missing.wait_for("missing botster-session-worker binary");
    missing.wait_for("Install the complete Botster distribution");
    missing.wait_for("cargo build --locked -p botster-core --bin botster-session-worker");
    missing.wait_for_exit();
    assert!(
        !fresh_data_dir
            .join(".botster-hub-runtime-daemon.json")
            .exists(),
        "missing-worker startup wrote runtime metadata"
    );
    let status = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("status")
        .arg("--data-dir")
        .arg(&fresh_data_dir)
        .output()
        .expect("probe missing-worker console runtime");
    assert!(
        !status.status.success(),
        "missing-worker console started a daemon: {}",
        command_output_text(&status)
    );

    fs::remove_dir_all(&data_dir).expect("remove reused console runtime directory");
    fs::remove_dir_all(&fresh_data_dir).expect("remove missing-worker console runtime directory");
    fs::remove_dir_all(&isolated_bin_dir).expect("remove isolated console binary directory");
}

#[test]
fn cli_help_like_args_print_command_guidance_without_daemon() {
    for arg in ["help", "--help"] {
        let help = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
            .arg(arg)
            .output()
            .expect("run help-like hub command");
        assert!(
            help.status.success(),
            "help command failed: {}",
            command_output_text(&help)
        );
        let text = command_output_text(&help);
        assert!(text.contains("Daily runtime commands:"));
        assert!(text.contains("botster-hub up [--data-dir <path>]"));
        assert!(text.contains("botster-hub down [--data-dir <path>]"));
        assert!(text.contains("botster-hub status [--data-dir <path>]"));
        assert!(text.contains("botster-hub doctor [--data-dir <path>]"));
        assert!(text.contains("botster-hub smoke [--data-dir <path>]"));
        assert!(text.contains("botster-hub open web [--data-dir <path>]"));
        assert!(text.contains("botster-hub open tui [--data-dir <path>]"));
        assert!(text.contains("botster-hub mcp-serve [--data-dir <path>]"));
        assert!(text.contains("botster-hub apps open [--data-dir <path>] <app|package/app>"));
        assert!(text.contains(
            "botster-hub packages config set [--data-dir <path>] <name> '<json-object>'"
        ));
        assert!(text.contains(
            "botster-hub packages apply-update [--data-dir <path>] <name> --revision <revision>"
        ));
        assert!(!text.contains("first-party host profile ready"));
        assert!(!text.contains("unknown command"));
    }
}

#[test]
fn package_entrypoint_supervision_passes_environment_overrides() {
    let _guard = daemon_test_guard();
    let data_dir = unique_test_dir("entrypoint-env");
    let package_dir = unique_test_dir("entrypoint-env-package");
    let output_path = std::env::current_dir()
        .expect("current dir")
        .join(data_dir.join("entrypoint-env.txt"));
    write_supervised_package(
        &package_dir,
        "runtime.env",
        "sh",
        &[
            "-c",
            &format!(
                "printf '%s' \"$BOTSTER_TEST_ENV_OVERRIDE\" > {}; while true; do sleep 1; done",
                output_path.display()
            ),
        ],
    );
    let child = start_cli_daemon(&data_dir);
    enable_supervised_package(&data_dir, &package_dir);

    let start = botster_hub::daemon_transport_request(
        &explicit_config(&data_dir),
        botster_hub::DaemonRequest::StartPackageEntrypoint {
            package_name: "runtime.env".to_string(),
            entrypoint_id: "web".to_string(),
            environment_overrides: BTreeMap::from([(
                "BOTSTER_TEST_ENV_OVERRIDE".to_string(),
                "override-reached-child".to_string(),
            )]),
        },
    )
    .expect("start supervised entrypoint with env");
    let entrypoint = package_entrypoint(&start, "runtime.env");
    assert_eq!(entrypoint.process.state, "running");

    for _ in 0..100 {
        if output_path.exists() {
            break;
        }
        thread::sleep(Duration::from_millis(20));
    }
    assert_eq!(
        fs::read_to_string(&output_path).expect("read env output"),
        "override-reached-child"
    );

    shutdown_cli_daemon(&data_dir, child);
}

#[test]
fn package_entrypoint_supervision_reports_missing_command() {
    let _guard = daemon_test_guard();
    let data_dir = unique_test_dir("entrypoint-missing-command");
    let package_dir = unique_test_dir("entrypoint-missing-command-package");
    write_supervised_package(
        &package_dir,
        "runtime.missing-command",
        "definitely-missing-botster-command",
        &[],
    );
    let child = start_cli_daemon(&data_dir);
    enable_supervised_package(&data_dir, &package_dir);

    let start = botster_hub::daemon_transport_request(
        &explicit_config(&data_dir),
        botster_hub::DaemonRequest::StartPackageEntrypoint {
            package_name: "runtime.missing-command".to_string(),
            entrypoint_id: "web".to_string(),
            environment_overrides: BTreeMap::new(),
        },
    )
    .expect("start missing supervised entrypoint");
    let entrypoint = package_entrypoint(&start, "runtime.missing-command");
    assert_eq!(entrypoint.process.state, "failed");
    assert!(entrypoint.process.pid.is_none());
    assert!(
        entrypoint
            .process
            .diagnostics
            .iter()
            .any(|diagnostic| diagnostic.kind == "spawn_error")
    );
    assert!(!format!("{start:?}").contains(package_dir.to_string_lossy().as_ref()));

    shutdown_cli_daemon(&data_dir, child);
}

#[test]
fn package_entrypoint_supervision_reports_failed_command() {
    let _guard = daemon_test_guard();
    let data_dir = unique_test_dir("entrypoint-failed-command");
    let package_dir = unique_test_dir("entrypoint-failed-command-package");
    write_supervised_package(
        &package_dir,
        "runtime.failed-command",
        "sh",
        &["-c", "printf 'fixture failure\\n' >&2; exit 42"],
    );
    let child = start_cli_daemon(&data_dir);
    enable_supervised_package(&data_dir, &package_dir);

    let _ = botster_hub::daemon_transport_request(
        &explicit_config(&data_dir),
        botster_hub::DaemonRequest::StartPackageEntrypoint {
            package_name: "runtime.failed-command".to_string(),
            entrypoint_id: "web".to_string(),
            environment_overrides: BTreeMap::new(),
        },
    )
    .expect("start failing supervised entrypoint");
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    let status = loop {
        let status = botster_hub::daemon_transport_request(
            &explicit_config(&data_dir),
            botster_hub::DaemonRequest::PackageEntrypointStatus {
                package_name: "runtime.failed-command".to_string(),
                entrypoint_id: "web".to_string(),
            },
        )
        .expect("status failing supervised entrypoint");
        if package_entrypoint(&status, "runtime.failed-command")
            .process
            .state
            != "running"
        {
            break status;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "failing supervised entrypoint did not reach a terminal state"
        );
        thread::sleep(Duration::from_millis(20));
    };
    let entrypoint = package_entrypoint(&status, "runtime.failed-command");
    assert_eq!(entrypoint.process.state, "failed");
    assert_eq!(entrypoint.process.exit_status.as_deref(), Some("exit:42"));
    assert!(
        entrypoint
            .process
            .diagnostics
            .iter()
            .any(|diagnostic| diagnostic.kind == "stderr"
                && diagnostic.message.contains("fixture failure"))
    );

    shutdown_cli_daemon(&data_dir, child);
}

#[test]
fn package_entrypoint_supervision_stops_and_restarts() {
    let _guard = daemon_test_guard();
    let data_dir = unique_test_dir("entrypoint-restart");
    let package_dir = unique_test_dir("entrypoint-restart-package");
    write_supervised_package(
        &package_dir,
        "runtime.restart",
        "sh",
        &["-c", "while true; do sleep 1; done"],
    );
    let child = start_cli_daemon(&data_dir);
    enable_supervised_package(&data_dir, &package_dir);

    let start = botster_hub::daemon_transport_request(
        &explicit_config(&data_dir),
        botster_hub::DaemonRequest::StartPackageEntrypoint {
            package_name: "runtime.restart".to_string(),
            entrypoint_id: "web".to_string(),
            environment_overrides: BTreeMap::new(),
        },
    )
    .expect("start restart fixture");
    let first_pid = package_entrypoint(&start, "runtime.restart")
        .process
        .pid
        .expect("first pid");

    let stop = botster_hub::daemon_transport_request(
        &explicit_config(&data_dir),
        botster_hub::DaemonRequest::StopPackageEntrypoint {
            package_name: "runtime.restart".to_string(),
            entrypoint_id: "web".to_string(),
        },
    )
    .expect("stop restart fixture");
    assert_eq!(
        package_entrypoint(&stop, "runtime.restart").process.state,
        "stopped"
    );
    wait_for_process_exit(first_pid);
    // The deterministic pending-reader regression guard lives in
    // stop_preserves_pending_terminal_launch_result_state; this exercises the app projection.
    let stopped_apps = botster_hub::daemon_transport_request(
        &explicit_config(&data_dir),
        botster_hub::DaemonRequest::ListApps,
    )
    .expect("list apps after stopping restart fixture");
    let stopped_app = app_row(&stopped_apps, "web");
    assert_ne!(stopped_app.lifecycle_state, "running");
    assert_eq!(
        package_action(&stopped_app.actions, "start_package_entrypoint").status,
        botster_hub::DaemonPackageActionStatus::Available
    );

    let restart = botster_hub::daemon_transport_request(
        &explicit_config(&data_dir),
        botster_hub::DaemonRequest::RestartPackageEntrypoint {
            package_name: "runtime.restart".to_string(),
            entrypoint_id: "web".to_string(),
        },
    )
    .expect("restart fixture");
    let second_pid = package_entrypoint(&restart, "runtime.restart")
        .process
        .pid
        .expect("second pid");
    assert_ne!(first_pid, second_pid);

    shutdown_cli_daemon(&data_dir, child);
}

#[test]
fn process_ownership_package_entrypoint_cleanup_covers_disable_remove_and_shutdown() {
    let _guard = daemon_test_guard();
    let data_dir = unique_test_dir("entrypoint-cleanup");
    let package_dir = unique_test_dir("entrypoint-cleanup-package");
    write_supervised_package(
        &package_dir,
        "runtime.cleanup",
        "sh",
        &["-c", "while true; do sleep 1; done"],
    );
    let child = start_cli_daemon(&data_dir);
    enable_supervised_package(&data_dir, &package_dir);

    let start = botster_hub::daemon_transport_request(
        &explicit_config(&data_dir),
        botster_hub::DaemonRequest::StartPackageEntrypoint {
            package_name: "runtime.cleanup".to_string(),
            entrypoint_id: "web".to_string(),
            environment_overrides: BTreeMap::new(),
        },
    )
    .expect("start cleanup fixture");
    let disable_pid = package_entrypoint(&start, "runtime.cleanup")
        .process
        .pid
        .expect("disable pid");
    let _ = botster_hub::daemon_transport_request(
        &explicit_config(&data_dir),
        botster_hub::DaemonRequest::DisablePackage {
            package_name: "runtime.cleanup".to_string(),
        },
    )
    .expect("disable cleanup package");
    wait_for_process_exit(disable_pid);

    let _ = botster_hub::daemon_transport_request(
        &explicit_config(&data_dir),
        botster_hub::DaemonRequest::EnablePackage {
            package_name: "runtime.cleanup".to_string(),
        },
    )
    .expect("re-enable cleanup package");
    let restart = botster_hub::daemon_transport_request(
        &explicit_config(&data_dir),
        botster_hub::DaemonRequest::StartPackageEntrypoint {
            package_name: "runtime.cleanup".to_string(),
            entrypoint_id: "web".to_string(),
            environment_overrides: BTreeMap::new(),
        },
    )
    .expect("restart cleanup fixture");
    let shutdown_pid = package_entrypoint(&restart, "runtime.cleanup")
        .process
        .pid
        .expect("shutdown pid");

    shutdown_cli_daemon(&data_dir, child);
    wait_for_process_exit(shutdown_pid);
}

#[test]
fn package_entrypoint_supervision_cleans_up_on_daemon_signal() {
    let _guard = daemon_test_guard();
    let data_dir = unique_test_dir("entrypoint-signal");
    let package_dir = unique_test_dir("entrypoint-signal-package");
    write_supervised_package(
        &package_dir,
        "runtime.signal",
        "sh",
        &["-c", "while true; do sleep 1; done"],
    );
    let child = start_cli_daemon(&data_dir);
    enable_supervised_package(&data_dir, &package_dir);

    let start = botster_hub::daemon_transport_request(
        &explicit_config(&data_dir),
        botster_hub::DaemonRequest::StartPackageEntrypoint {
            package_name: "runtime.signal".to_string(),
            entrypoint_id: "web".to_string(),
            environment_overrides: BTreeMap::new(),
        },
    )
    .expect("start signal fixture");
    let pid = package_entrypoint(&start, "runtime.signal")
        .process
        .pid
        .expect("signal pid");

    unsafe {
        libc::kill(child.id() as libc::pid_t, libc::SIGTERM);
    }
    let output = child.wait_with_output().expect("wait for signaled daemon");
    assert!(
        output.status.success(),
        "daemon signal shutdown failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    wait_for_process_exit(pid);
}

#[test]
fn cli_packages_local_path_install_enable_disable_remove_flow() {
    let _guard = daemon_test_guard();
    let data_dir = unique_test_dir("cli-packages-flow");
    let package_dir = unique_test_dir("local-package-flow");
    write_local_plugin_package(&package_dir);
    let child = start_cli_daemon(&data_dir);

    let install = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("packages")
        .arg("install")
        .arg("--data-dir")
        .arg(&data_dir)
        .arg("--path")
        .arg(&package_dir)
        .output()
        .expect("run botster-hub packages install");
    assert!(
        install.status.success(),
        "install failed: {}",
        String::from_utf8_lossy(&install.stderr)
    );
    let stdout = String::from_utf8(install.stdout).expect("stdout is utf8");
    assert!(stdout.contains("decision=package"));
    assert!(stdout.contains("package_name=runtime.plugin"));
    assert!(stdout.contains("action=install"));
    assert!(stdout.contains("package_count=1"));
    assert!(stdout.contains("state=installed"));
    assert!(!stdout.contains(package_dir.to_string_lossy().as_ref()));
    assert!(!stdout.contains(data_dir.to_string_lossy().as_ref()));

    let show = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("packages")
        .arg("show")
        .arg("--data-dir")
        .arg(&data_dir)
        .arg("runtime.plugin")
        .output()
        .expect("run botster-hub packages show");
    assert!(
        show.status.success(),
        "show failed: {}",
        String::from_utf8_lossy(&show.stderr)
    );
    let stdout = String::from_utf8(show.stdout).expect("stdout is utf8");
    assert!(stdout.contains("package_count=1"));
    assert!(stdout.contains("package name=runtime.plugin"));
    assert!(stdout.contains("state=installed"));
    assert!(!stdout.contains(package_dir.to_string_lossy().as_ref()));
    assert!(!stdout.contains(data_dir.to_string_lossy().as_ref()));

    let enable = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("packages")
        .arg("enable")
        .arg("--data-dir")
        .arg(&data_dir)
        .arg("runtime.plugin")
        .output()
        .expect("run botster-hub packages enable");
    assert!(
        enable.status.success(),
        "enable failed: {}",
        String::from_utf8_lossy(&enable.stderr)
    );
    let stdout = String::from_utf8(enable.stdout).expect("stdout is utf8");
    assert!(stdout.contains("action=enable"));
    assert!(stdout.contains("state=enabled"));
    assert!(!stdout.contains(package_dir.to_string_lossy().as_ref()));
    assert!(!stdout.contains(data_dir.to_string_lossy().as_ref()));

    let lifecycle = botster_hub::daemon_transport_request(
        &explicit_config(&data_dir),
        botster_hub::DaemonRequest::PluginLifecycleStatus,
    )
    .expect("daemon plugin lifecycle status after enable");
    assert!(lifecycle.lifecycle.iter().any(|plugin| {
        plugin.package_name == "runtime.plugin" && plugin.state == "enabled" && plugin.loaded
    }));

    let disable = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("packages")
        .arg("disable")
        .arg("--data-dir")
        .arg(&data_dir)
        .arg("runtime.plugin")
        .output()
        .expect("run botster-hub packages disable");
    assert!(
        disable.status.success(),
        "disable failed: {}",
        String::from_utf8_lossy(&disable.stderr)
    );
    let stdout = String::from_utf8(disable.stdout).expect("stdout is utf8");
    assert!(stdout.contains("action=disable"));
    assert!(stdout.contains("state=disabled"));

    let lifecycle = botster_hub::daemon_transport_request(
        &explicit_config(&data_dir),
        botster_hub::DaemonRequest::PluginLifecycleStatus,
    )
    .expect("daemon plugin lifecycle status after disable");
    assert!(lifecycle.lifecycle.iter().any(|plugin| {
        plugin.package_name == "runtime.plugin" && plugin.state == "disabled" && !plugin.loaded
    }));

    let remove = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("packages")
        .arg("remove")
        .arg("--data-dir")
        .arg(&data_dir)
        .arg("runtime.plugin")
        .output()
        .expect("run botster-hub packages remove");
    assert!(
        remove.status.success(),
        "remove failed: {}",
        String::from_utf8_lossy(&remove.stderr)
    );
    let stdout = String::from_utf8(remove.stdout).expect("stdout is utf8");
    assert!(stdout.contains("action=remove"));
    assert!(stdout.contains("package_count=0"));
    assert!(!stdout.contains(package_dir.to_string_lossy().as_ref()));
    assert!(!stdout.contains(data_dir.to_string_lossy().as_ref()));

    shutdown_cli_daemon(&data_dir, child);

    let restarted = start_cli_daemon(&data_dir);
    let list_after_restart = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("packages")
        .arg("list")
        .arg("--data-dir")
        .arg(&data_dir)
        .output()
        .expect("run botster-hub packages list after remove restart");
    assert!(
        list_after_restart.status.success(),
        "packages list after remove restart failed: {}",
        String::from_utf8_lossy(&list_after_restart.stderr)
    );
    let stdout = String::from_utf8(list_after_restart.stdout).expect("stdout is utf8");
    assert!(stdout.contains("package_count=0"));

    shutdown_cli_daemon(&data_dir, restarted);
}

#[test]
fn daemon_packages_registry_fixture_preview_and_install_flow() {
    let _guard = daemon_test_guard();
    let data_dir = unique_test_dir("daemon-registry-flow");
    let registry_dir = unique_test_dir("daemon-package-registry");
    let package_dir = registry_dir.join("packages").join("local");
    write_local_plugin_package(&package_dir);
    fs::write(
        registry_dir.join(botster_hub::LOCAL_PACKAGE_REGISTRY_FILE),
        r#"{
  "source": { "id": "daemon-fixture", "kind": "local_path", "label": "Daemon Fixture" },
  "entries": [
    {
      "id": "runtime-local",
      "first_party": true,
      "source": { "type": "local_path", "path": "packages/local" }
    },
    {
      "id": "runtime-git",
      "first_party": true,
      "source": {
        "type": "git",
        "repo": "https://example.invalid/botster/runtime.git",
        "branch": "main",
        "tag": "v1.0.0",
        "rev": "abc123"
      },
      "manifest": {
        "name": "runtime.git",
        "version": "1.0.0",
        "kind": "plugin",
        "botster": ">=0.1.0",
        "capabilities": [
          { "surface": "surfaces" }
        ],
        "entrypoints": [
          { "runtime": "lua", "path": "plugin.lua", "bootstrap": false }
        ]
      }
    }
  ]
}
"#,
    )
    .expect("write package registry fixture");
    let child = start_cli_daemon(&data_dir);
    let config = explicit_config(&data_dir);

    let available = botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::ListAvailablePackages {
            registry_path: registry_dir.clone(),
        },
    )
    .expect("list available packages through daemon");
    assert_eq!(
        available.kind,
        botster_hub::DaemonResponseKind::AvailablePackages
    );
    assert_eq!(available.available_packages.len(), 2);
    assert!(available.available_packages.iter().all(|package| {
        !package
            .source_label
            .contains(data_dir.to_string_lossy().as_ref())
            && !package
                .source_label
                .contains(registry_dir.to_string_lossy().as_ref())
    }));
    let local_available = available
        .available_packages
        .iter()
        .find(|package| package.entry_id == "runtime-local")
        .expect("local available entry");
    let install_action = package_action(&local_available.actions, "install_package_registry_entry");
    assert_eq!(
        install_action.status,
        botster_hub::DaemonPackageActionStatus::Available
    );
    let install_request = install_action
        .request
        .as_ref()
        .expect("install request mapping");
    assert_eq!(
        install_request.request_type,
        "install_package_registry_entry"
    );
    assert_eq!(install_request.entry_id.as_deref(), Some("runtime-local"));
    assert_eq!(
        install_request.registry_path.as_deref(),
        Some(registry_dir.to_string_lossy().as_ref())
    );
    assert_eq!(
        package_action(&local_available.actions, "enable_package").status,
        botster_hub::DaemonPackageActionStatus::Unavailable
    );

    let inspect = botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::InspectAvailablePackage {
            registry_path: registry_dir.clone(),
            entry_id: "runtime-git".to_string(),
        },
    )
    .expect("inspect git-shaped entry through daemon");
    let git_entry = inspect
        .available_packages
        .first()
        .expect("inspected git entry");
    assert_eq!(git_entry.source_kind, "git");
    assert_eq!(
        git_entry.pin.as_ref().expect("git pin").rev.as_deref(),
        Some("abc123")
    );

    let preview = botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::PreviewPackageInstall {
            registry_path: registry_dir.clone(),
            entry_id: "runtime-local".to_string(),
        },
    )
    .expect("preview install through daemon");
    let plan = preview.install_plan.expect("install plan");
    assert!(!plan.mutates_registry);
    assert!(!plan.starts_entrypoints);
    assert!(
        plan.effects
            .iter()
            .any(|effect| effect.kind == "no_entrypoint_start")
    );
    let list_after_preview =
        botster_hub::daemon_transport_request(&config, botster_hub::DaemonRequest::ListPackages)
            .expect("list after preview");
    assert!(list_after_preview.packages.is_empty());

    let install = botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::InstallPackageRegistryEntry {
            registry_path: registry_dir.clone(),
            entry_id: "runtime-git".to_string(),
        },
    )
    .expect("install git-shaped entry through daemon");
    assert_eq!(
        install.package_decision.expect("install decision").action,
        "install"
    );
    let installed = install
        .packages
        .iter()
        .find(|package| package.package_name == "runtime.git")
        .expect("installed package row");
    assert_eq!(installed.state, "installed");
    let enable_action = package_action(&installed.actions, "enable_package");
    assert_eq!(
        enable_action.status,
        botster_hub::DaemonPackageActionStatus::Blocked
    );
    let remove_action = package_action(&installed.actions, "remove_package");
    assert_eq!(
        remove_action.status,
        botster_hub::DaemonPackageActionStatus::Available
    );
    assert_eq!(
        remove_action
            .request
            .as_ref()
            .expect("remove request mapping")
            .request_type,
        "remove_package"
    );
    let reload_action = package_action(&installed.actions, "reload_package");
    assert_eq!(
        reload_action.status,
        botster_hub::DaemonPackageActionStatus::Unavailable
    );

    shutdown_cli_daemon(&data_dir, child);
    let state = FileHubStateStore::for_data_directory(&data_dir)
        .load_or_initialize(&explicit_config(&data_dir))
        .expect("load persisted hub state after registry install");
    let restored = PackageRegistry::from_snapshot(state.package_registry)
        .expect("restore package registry snapshot");
    let record = restored.package("runtime.git").expect("restored package");
    assert_eq!(record.state, botster_hub::PackageState::Installed);
    assert_eq!(
        record
            .source_metadata
            .as_ref()
            .expect("source metadata")
            .entry_id,
        "runtime-git"
    );
    assert_eq!(
        record.pin.as_ref().expect("pin").rev.as_deref(),
        Some("abc123")
    );
}

#[test]
fn live_hub_managed_git_spawn_reconciles_and_reuses_after_restart() {
    let _guard = daemon_test_guard();
    let data_dir = unique_short_test_dir("managed-live");
    let competing_data_dir = unique_short_test_dir("managed-competing");
    let package_dir = unique_short_test_dir("managed-package");
    let repository = unique_short_test_dir("managed-repo");
    fs::create_dir_all(&repository).expect("create managed live repository");
    run_fixture_git(None, &["init", "-b", "main", path_str(&repository)]);
    run_fixture_git(
        Some(&repository),
        &["config", "user.email", "botster@example.invalid"],
    );
    run_fixture_git(
        Some(&repository),
        &["config", "user.name", "Botster Live Test"],
    );
    fs::write(repository.join("README.md"), "managed live\n").expect("write repository fixture");
    run_fixture_git(Some(&repository), &["add", "README.md"]);
    run_fixture_git(Some(&repository), &["commit", "-m", "managed live fixture"]);
    write_managed_git_session_package(&package_dir);

    let first_child = start_cli_daemon(&data_dir);
    let enabled = botster_hub::daemon_transport_request(
        &explicit_config(&data_dir),
        botster_hub::DaemonRequest::EnablePackageLocalPath {
            path: package_dir.clone(),
        },
    )
    .expect("install and enable live managed Git package");
    assert_eq!(
        enabled.kind,
        botster_hub::DaemonResponseKind::PackageDecision
    );
    let created_target = botster_hub::daemon_transport_request(
        &explicit_config(&data_dir),
        botster_hub::DaemonRequest::CreateSpawnTarget {
            target_id: Some("tgt_live_managed".to_string()),
            label: Some("Live Managed".to_string()),
            root: repository.clone(),
            enabled: true,
            kind: Some("git".to_string()),
            base_ref: Some("main".to_string()),
            metadata: BTreeMap::new(),
        },
    )
    .expect("create live Git spawn target");
    assert_eq!(
        created_target.spawn_targets[0].base_ref.as_deref(),
        Some("main")
    );

    let call = |call_data_dir: &Path| {
        botster_hub::daemon_transport_request(
            &explicit_config(call_data_dir),
            botster_hub::DaemonRequest::PluginMcpCallTool {
                name: "managed_git.live_spawn".to_string(),
                arguments: serde_json::json!({
                    "target_id": "tgt_live_managed",
                    "branch": "feature/live-restart",
                    "template_id": "managed-git.live-plugin/init"
                }),
            },
        )
        .expect("call live managed Git tool")
    };
    let first = call(&data_dir);
    assert_eq!(
        first.kind,
        botster_hub::DaemonResponseKind::PluginMcpToolResult
    );
    assert_eq!(first.plugin_tool_result["ok"], true);
    assert_eq!(first.plugin_tool_result["result"]["created_worktree"], true);
    let first_session_id = first.plugin_tool_result["result"]["session_id"]
        .as_str()
        .expect("first live session UUID")
        .to_string();
    assert_eq!(first_session_id.len(), 36);
    let first_worktree_path = PathBuf::from(
        first.plugin_tool_result["result"]["worktree_path"]
            .as_str()
            .expect("first live worktree path"),
    );
    let first_marker = first_worktree_path.join("live-managed.txt");
    for _ in 0..100 {
        if first_marker.exists() {
            break;
        }
        thread::sleep(Duration::from_millis(10));
    }
    assert_eq!(
        fs::read_to_string(first_marker).expect("live managed cwd marker"),
        "live-managed\n"
    );

    let competing_child = start_cli_daemon(&competing_data_dir);
    botster_hub::daemon_transport_request(
        &explicit_config(&competing_data_dir),
        botster_hub::DaemonRequest::EnablePackageLocalPath {
            path: package_dir.clone(),
        },
    )
    .expect("enable package in competing Hub");
    botster_hub::daemon_transport_request(
        &explicit_config(&competing_data_dir),
        botster_hub::DaemonRequest::CreateSpawnTarget {
            target_id: Some("tgt_live_managed".to_string()),
            label: Some("Competing Managed".to_string()),
            root: repository.clone(),
            enabled: true,
            kind: Some("git".to_string()),
            base_ref: Some("main".to_string()),
            metadata: BTreeMap::new(),
        },
    )
    .expect("create competing Git spawn target");
    let competing = call(&competing_data_dir);
    assert_eq!(competing.plugin_tool_result["ok"], false);
    assert_eq!(
        competing.plugin_tool_result["error"]["kind"],
        "branch_in_use"
    );
    assert!(
        first_worktree_path.exists(),
        "competing Hub must not remove the winning worktree"
    );
    let competing_shutdown = shutdown_cli_daemon(&competing_data_dir, competing_child);
    assert!(
        competing_shutdown.status.success(),
        "competing daemon shutdown failed: {}",
        command_output_text(&competing_shutdown)
    );

    let first_shutdown = shutdown_cli_daemon(&data_dir, first_child);
    assert!(
        first_shutdown.status.success(),
        "first live daemon shutdown failed: {}",
        command_output_text(&first_shutdown)
    );

    let second_child = start_cli_daemon(&data_dir);
    let listed = botster_hub::daemon_transport_request(
        &explicit_config(&data_dir),
        botster_hub::DaemonRequest::ListWorktrees,
    )
    .expect("list reconciled managed worktree after restart");
    let managed = listed
        .worktrees
        .iter()
        .find(|worktree| worktree.target_id == "tgt_live_managed")
        .expect("managed row after restart");
    assert_eq!(managed.management, "hub_managed_git");
    assert_eq!(managed.status, "present");
    assert_eq!(managed.path, first_worktree_path);
    let persisted_target = botster_hub::daemon_transport_request(
        &explicit_config(&data_dir),
        botster_hub::DaemonRequest::ShowSpawnTarget {
            target_id: "tgt_live_managed".to_string(),
        },
    )
    .expect("show persisted Git target after restart");
    assert_eq!(
        persisted_target.spawn_targets[0].base_ref.as_deref(),
        Some("main")
    );
    let downgrade = botster_hub::daemon_transport_request(
        &explicit_config(&data_dir),
        botster_hub::DaemonRequest::UpdateSpawnTarget {
            target_id: "tgt_live_managed".to_string(),
            label: None,
            root: None,
            enabled: None,
            kind: Some("directory".to_string()),
            base_ref: None,
            metadata: None,
        },
    )
    .expect("return operator error for managed target downgrade");
    assert_eq!(
        downgrade.kind,
        botster_hub::DaemonResponseKind::OperatorError
    );
    assert_eq!(
        downgrade.error.as_ref().map(|error| error.code.as_str()),
        Some("managed_worktrees_exist")
    );
    let delete_target = botster_hub::daemon_transport_request(
        &explicit_config(&data_dir),
        botster_hub::DaemonRequest::DeleteSpawnTarget {
            target_id: "tgt_live_managed".to_string(),
        },
    )
    .expect("return operator error for managed target deletion");
    assert_eq!(
        delete_target.kind,
        botster_hub::DaemonResponseKind::OperatorError
    );
    assert_eq!(
        delete_target
            .error
            .as_ref()
            .map(|error| error.code.as_str()),
        Some("managed_worktrees_exist")
    );
    let delete_worktree = botster_hub::daemon_transport_request(
        &explicit_config(&data_dir),
        botster_hub::DaemonRequest::DeleteWorktree {
            worktree_id: managed.worktree_id.clone(),
        },
    )
    .expect("return operator error for record-only managed worktree deletion");
    assert_eq!(
        delete_worktree.kind,
        botster_hub::DaemonResponseKind::OperatorError
    );
    assert_eq!(
        delete_worktree
            .error
            .as_ref()
            .map(|error| error.code.as_str()),
        Some("managed_worktree_requires_reclaim")
    );

    let second = call(&data_dir);
    assert_eq!(second.plugin_tool_result["ok"], true);
    assert_eq!(second.plugin_tool_result["result"]["reused_worktree"], true);
    assert_eq!(
        second.plugin_tool_result["result"]["worktree_path"],
        first.plugin_tool_result["result"]["worktree_path"]
    );
    let second_session_id = second.plugin_tool_result["result"]["session_id"]
        .as_str()
        .expect("second live session UUID")
        .to_string();
    assert_ne!(second_session_id, first_session_id);

    for session_id in [first_session_id, second_session_id] {
        botster_hub::daemon_transport_request(
            &explicit_config(&data_dir),
            botster_hub::DaemonRequest::ShutdownSession { session_id },
        )
        .expect("shut down live managed session");
    }
    let second_shutdown = shutdown_cli_daemon(&data_dir, second_child);
    assert!(
        second_shutdown.status.success(),
        "second live daemon shutdown failed: {}",
        command_output_text(&second_shutdown)
    );
}

fn path_str(path: &Path) -> &str {
    path.to_str().expect("test path is UTF-8")
}

fn run_fixture_git(root: Option<&Path>, args: &[&str]) {
    let mut command = Command::new("git");
    if let Some(root) = root {
        command.arg("-C").arg(root);
    }
    assert!(
        command
            .args(args)
            .status()
            .expect("run fixture Git")
            .success(),
        "fixture Git command failed: {args:?}"
    );
}

#[test]
fn cli_packages_local_path_diagnostics_are_actionable() {
    let _guard = daemon_test_guard();
    let data_dir = unique_test_dir("cli-packages-diagnostics");
    let invalid_dir = unique_test_dir("local-package-invalid");
    let incompatible_dir = unique_test_dir("local-package-incompatible");
    let duplicate_dir = unique_test_dir("local-package-duplicate");
    let denied_dir = unique_test_dir("local-package-denied");
    write_invalid_local_package(&invalid_dir);
    write_incompatible_local_package(&incompatible_dir);
    write_local_plugin_package(&duplicate_dir);
    write_denied_capability_local_package(&denied_dir);
    let child = start_cli_daemon(&data_dir);

    let invalid = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("packages")
        .arg("install")
        .arg("--data-dir")
        .arg(&data_dir)
        .arg("--path")
        .arg(&invalid_dir)
        .output()
        .expect("run invalid package install");
    assert!(!invalid.status.success());
    let text = command_output_text(&invalid);
    assert!(text.contains("response=operator_error"));
    assert!(text.contains("operation=install"));
    assert!(text.contains("InvalidLocalManifest"));
    assert!(!text.contains(invalid_dir.to_string_lossy().as_ref()));
    assert!(!text.contains(data_dir.to_string_lossy().as_ref()));

    let incompatible = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("packages")
        .arg("install")
        .arg("--data-dir")
        .arg(&data_dir)
        .arg("--path")
        .arg(&incompatible_dir)
        .output()
        .expect("run incompatible package install");
    assert!(!incompatible.status.success());
    let text = command_output_text(&incompatible);
    assert!(text.contains("response=operator_error"));
    assert!(text.contains("operation=install"));
    assert!(text.contains("BotsterCompatibility"));
    assert!(!text.contains(incompatible_dir.to_string_lossy().as_ref()));
    assert!(!text.contains(data_dir.to_string_lossy().as_ref()));

    let first_install = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("packages")
        .arg("install")
        .arg("--data-dir")
        .arg(&data_dir)
        .arg("--path")
        .arg(&duplicate_dir)
        .output()
        .expect("run first duplicate package install");
    assert!(
        first_install.status.success(),
        "first duplicate install failed: {}",
        String::from_utf8_lossy(&first_install.stderr)
    );
    let duplicate = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("packages")
        .arg("install")
        .arg("--data-dir")
        .arg(&data_dir)
        .arg("--path")
        .arg(&duplicate_dir)
        .output()
        .expect("run duplicate package install");
    assert!(!duplicate.status.success());
    let text = command_output_text(&duplicate);
    assert!(text.contains("response=operator_error"));
    assert!(text.contains("operation=install"));
    assert!(text.contains("AlreadyInstalled"));
    assert!(!text.contains(duplicate_dir.to_string_lossy().as_ref()));
    assert!(!text.contains(data_dir.to_string_lossy().as_ref()));

    let denied_install = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("packages")
        .arg("install")
        .arg("--data-dir")
        .arg(&data_dir)
        .arg("--path")
        .arg(&denied_dir)
        .output()
        .expect("run denied package install");
    assert!(
        denied_install.status.success(),
        "denied package install failed before enable: {}",
        String::from_utf8_lossy(&denied_install.stderr)
    );
    let denied_enable = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("packages")
        .arg("enable")
        .arg("--data-dir")
        .arg(&data_dir)
        .arg("runtime.denied-plugin")
        .output()
        .expect("run denied package enable");
    assert!(!denied_enable.status.success());
    let text = command_output_text(&denied_enable);
    assert!(text.contains("response=operator_error"));
    assert!(text.contains("operation=enable"));
    assert!(text.contains("UngrantedCapability"));

    let missing_show = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("packages")
        .arg("show")
        .arg("--data-dir")
        .arg(&data_dir)
        .arg("runtime.missing-plugin")
        .output()
        .expect("run missing package show");
    assert!(!missing_show.status.success());
    let text = command_output_text(&missing_show);
    assert!(text.contains("response=operator_error"));
    assert!(text.contains("operation=show"));
    assert!(text.contains("PackageNotInstalled"));
    assert!(text.contains("runtime.missing-plugin"));
    assert!(!text.contains(data_dir.to_string_lossy().as_ref()));

    let missing_remove = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("packages")
        .arg("remove")
        .arg("--data-dir")
        .arg(&data_dir)
        .arg("runtime.missing-plugin")
        .output()
        .expect("run missing package remove");
    assert!(!missing_remove.status.success());
    let text = command_output_text(&missing_remove);
    assert!(text.contains("response=operator_error"));
    assert!(text.contains("operation=remove"));
    assert!(text.contains("PackageNotInstalled"));
    assert!(text.contains("runtime.missing-plugin"));
    assert!(!text.contains(data_dir.to_string_lossy().as_ref()));

    shutdown_cli_daemon(&data_dir, child);
}

#[test]
fn cli_packages_enable_botster_workspaces_first_party_plugin_db_namespace() {
    let _guard = daemon_test_guard();
    let data_dir = unique_short_test_dir("cli-pkg-ws");
    let package_dir = unique_test_dir("botster-workspaces-package");
    write_botster_workspaces_local_package(&package_dir, "botster-workspaces");
    let child = start_cli_daemon(&data_dir);

    let install = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("packages")
        .arg("install")
        .arg("--data-dir")
        .arg(&data_dir)
        .arg("--path")
        .arg(&package_dir)
        .output()
        .expect("run botster-workspaces package install");
    assert!(
        install.status.success(),
        "botster-workspaces install failed: {}",
        command_output_text(&install)
    );
    let text = command_output_text(&install);
    assert!(text.contains("package name=botster-workspaces"));
    assert!(text.contains("state=installed"));
    assert!(!text.contains(package_dir.to_string_lossy().as_ref()));
    assert!(!text.contains(data_dir.to_string_lossy().as_ref()));

    let show_installed = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("packages")
        .arg("show")
        .arg("--data-dir")
        .arg(&data_dir)
        .arg("botster-workspaces")
        .output()
        .expect("run botster-workspaces package show after install");
    assert!(
        show_installed.status.success(),
        "botster-workspaces show failed: {}",
        command_output_text(&show_installed)
    );
    let text = command_output_text(&show_installed);
    assert!(text.contains("package name=botster-workspaces"));
    assert!(text.contains("state=installed"));
    assert!(text.contains("capabilities=4"));

    let enable = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("packages")
        .arg("enable")
        .arg("--data-dir")
        .arg(&data_dir)
        .arg("botster-workspaces")
        .output()
        .expect("run botster-workspaces package enable");
    assert!(
        enable.status.success(),
        "botster-workspaces enable failed: {}",
        command_output_text(&enable)
    );
    let text = command_output_text(&enable);
    assert!(text.contains("package name=botster-workspaces"));
    assert!(text.contains("state=enabled"));

    let list = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("packages")
        .arg("list")
        .arg("--data-dir")
        .arg(&data_dir)
        .output()
        .expect("run botster-workspaces package list");
    assert!(
        list.status.success(),
        "botster-workspaces list failed: {}",
        command_output_text(&list)
    );
    let text = command_output_text(&list);
    assert!(text.contains("package name=botster-workspaces"));
    assert!(text.contains("state=enabled"));
    assert!(!text.contains(package_dir.to_string_lossy().as_ref()));
    assert!(!text.contains(data_dir.to_string_lossy().as_ref()));

    shutdown_cli_daemon(&data_dir, child);
}

#[test]
fn cli_packages_deny_botster_workspaces_mismatched_plugin_db_namespace() {
    let _guard = daemon_test_guard();
    let data_dir = unique_short_test_dir("cli-pkg-ws-denied");
    let package_dir = unique_test_dir("botster-workspaces-denied-package");
    write_botster_workspaces_local_package(&package_dir, "other-plugin");
    let child = start_cli_daemon(&data_dir);

    let install = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("packages")
        .arg("install")
        .arg("--data-dir")
        .arg(&data_dir)
        .arg("--path")
        .arg(&package_dir)
        .output()
        .expect("run mismatched botster-workspaces package install");
    assert!(
        install.status.success(),
        "mismatched botster-workspaces install failed before enable: {}",
        command_output_text(&install)
    );

    let enable = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("packages")
        .arg("enable")
        .arg("--data-dir")
        .arg(&data_dir)
        .arg("botster-workspaces")
        .output()
        .expect("run mismatched botster-workspaces package enable");
    assert!(!enable.status.success());
    let text = command_output_text(&enable);
    assert!(text.contains("response=operator_error"));
    assert!(text.contains("operation=enable"));
    assert!(text.contains("UngrantedCapability"));
    assert!(text.contains("other-plugin"));
    assert!(!text.contains(package_dir.to_string_lossy().as_ref()));
    assert!(!text.contains(data_dir.to_string_lossy().as_ref()));

    shutdown_cli_daemon(&data_dir, child);
}

#[test]
fn package_configuration_daemon_set_show_list_reload_and_cli_are_redacted() {
    let _guard = daemon_test_guard();
    let data_dir = unique_test_dir("package-configuration-daemon");
    let package_dir = unique_test_dir("configurable-package");
    write_configurable_local_plugin_package(&package_dir);
    let config = explicit_config(&data_dir);
    let child = start_cli_daemon(&data_dir);

    let install = botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::InstallPackageLocalPath {
            path: package_dir.clone(),
        },
    )
    .expect("install configurable package");
    assert_eq!(
        install.kind,
        botster_hub::DaemonResponseKind::PackageDecision
    );
    let installed = install
        .packages
        .iter()
        .find(|package| package.package_name == "configurable.plugin")
        .expect("installed configurable package");
    assert_eq!(
        installed.configuration.missing_required,
        vec!["endpoint".to_string(), "api_token".to_string()]
    );
    let enable_action = package_action(&installed.actions, "enable_package");
    assert_eq!(
        enable_action.status,
        botster_hub::DaemonPackageActionStatus::Blocked
    );
    assert!(
        enable_action
            .required_references
            .iter()
            .any(|reference| { reference.kind == "config" && reference.key == "endpoint" })
    );
    assert!(
        enable_action
            .required_references
            .iter()
            .any(|reference| { reference.kind == "config" && reference.key == "api_token" })
    );
    let configure_action = package_action(&installed.actions, "set_package_configuration");
    assert_eq!(
        configure_action.status,
        botster_hub::DaemonPackageActionStatus::Available
    );
    assert_eq!(
        configure_action
            .request
            .as_ref()
            .expect("configure request mapping")
            .request_type,
        "set_package_configuration"
    );

    let missing_enable = botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::EnablePackage {
            package_name: "configurable.plugin".to_string(),
        },
    )
    .expect("enable missing config returns operator error");
    assert_eq!(
        missing_enable.kind,
        botster_hub::DaemonResponseKind::OperatorError
    );
    assert!(
        missing_enable
            .error
            .as_ref()
            .expect("operator error")
            .message
            .contains("MissingRequiredConfiguration")
    );

    let bad_config = botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::SetPackageConfiguration {
            package_name: "configurable.plugin".to_string(),
            values: BTreeMap::from([(
                "unknown".to_string(),
                serde_json::json!({"type":"string","value":"nope"}),
            )]),
        },
    )
    .expect("bad config returns operator error");
    assert_eq!(
        bad_config.kind,
        botster_hub::DaemonResponseKind::OperatorError
    );

    let configured = botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::SetPackageConfiguration {
            package_name: "configurable.plugin".to_string(),
            values: BTreeMap::from([
                (
                    "endpoint".to_string(),
                    serde_json::json!({"type":"url","value":"https://example.invalid/hook"}),
                ),
                (
                    "api_token".to_string(),
                    serde_json::json!({"type":"secret","state":"write_only"}),
                ),
            ]),
        },
    )
    .expect("set config through daemon");
    let configured_package = configured
        .packages
        .iter()
        .find(|package| package.package_name == "configurable.plugin")
        .expect("configured package");
    assert!(configured_package.configuration.missing_required.is_empty());
    assert_eq!(
        configured_package.configuration.effective_values["api_token"],
        serde_json::json!({"type":"secret","state":"redacted"})
    );
    assert_eq!(
        configured_package.configuration.effective_values["mode"],
        serde_json::json!({"type":"select","value":"read"})
    );

    let list =
        botster_hub::daemon_transport_request(&config, botster_hub::DaemonRequest::ListPackages)
            .expect("list after config mutation");
    let listed = list
        .packages
        .iter()
        .find(|package| package.package_name == "configurable.plugin")
        .expect("listed configurable package");
    assert!(listed.configuration.missing_required.is_empty());
    assert_eq!(
        listed.configuration.effective_values["api_token"],
        serde_json::json!({"type":"secret","state":"redacted"})
    );

    let state_json =
        fs::read_to_string(data_dir.join("hub-state.json")).expect("read hub state json");
    assert!(state_json.contains("\"state\": \"redacted\""));
    assert!(!state_json.contains("write_only"));
    assert!(!state_json.contains("super-secret-token"));

    let cli = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("packages")
        .arg("config")
        .arg("--data-dir")
        .arg(&data_dir)
        .arg("configurable.plugin")
        .output()
        .expect("run packages config");
    assert!(
        cli.status.success(),
        "packages config failed: {}",
        String::from_utf8_lossy(&cli.stderr)
    );
    let stdout = String::from_utf8(cli.stdout).expect("stdout is utf8");
    assert!(stdout.contains("package_config package=configurable.plugin schema_present=true"));
    assert!(stdout.contains("\"state\":\"redacted\""));
    assert!(!stdout.contains("write_only"));
    assert!(!stdout.contains("super-secret-token"));

    shutdown_cli_daemon(&data_dir, child);

    let restarted = start_cli_daemon(&data_dir);
    let reloaded =
        botster_hub::daemon_transport_request(&config, botster_hub::DaemonRequest::ListPackages)
            .expect("list after restart");
    let package = reloaded
        .packages
        .iter()
        .find(|package| package.package_name == "configurable.plugin")
        .expect("reloaded package");
    assert_eq!(
        package.configuration.effective_values["api_token"],
        serde_json::json!({"type":"secret","state":"redacted"})
    );
    shutdown_cli_daemon(&data_dir, restarted);
}

#[test]
fn local_package_reload_rereads_manifest_restarts_running_app_and_cli_open_uses_refreshed_state() {
    let _guard = daemon_test_guard();
    let data_dir = unique_test_dir("local-package-reload");
    let package_dir = unique_test_dir("reloadable-app-package");
    write_reloadable_app_package(&package_dir, "1.0.0", "http://127.0.0.1:49160");
    let config = explicit_config(&data_dir);
    let child = start_cli_daemon(&data_dir);

    let enable = botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::EnablePackageLocalPath {
            path: package_dir.clone(),
        },
    )
    .expect("enable reloadable local app package");
    assert_eq!(
        enable.package_decision.expect("enable decision").action,
        "enable"
    );
    let enabled_package = enable
        .packages
        .iter()
        .find(|package| package.package_name == "runtime.reloadable")
        .expect("enabled package row");
    assert_eq!(enabled_package.source_kind, "path");
    let reload_action = package_action(&enabled_package.actions, "reload_package");
    assert_eq!(
        reload_action.status,
        botster_hub::DaemonPackageActionStatus::Available
    );
    assert_eq!(
        reload_action
            .request
            .as_ref()
            .expect("reload request")
            .request_type,
        "reload_package"
    );

    botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::StartPackageEntrypoint {
            package_name: "runtime.reloadable".to_string(),
            entrypoint_id: "web".to_string(),
            environment_overrides: BTreeMap::new(),
        },
    )
    .expect("start reloadable app");
    wait_for_app_local_url(&data_dir, "web", "http://127.0.0.1:49160");

    write_reloadable_app_package(&package_dir, "1.1.0", "http://127.0.0.1:49161");
    let reload = botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::ReloadPackage {
            package_name: "runtime.reloadable".to_string(),
        },
    )
    .expect("reload local package");
    assert_eq!(
        reload.package_decision.expect("reload decision").action,
        "reload"
    );
    let reloaded_package = reload
        .packages
        .iter()
        .find(|package| package.package_name == "runtime.reloadable")
        .expect("reloaded package row");
    assert_eq!(reloaded_package.version, "1.1.0");

    let apps = wait_for_app_local_url(&data_dir, "web", "http://127.0.0.1:49161");
    let app = app_row(&apps, "web");
    assert_eq!(app.package_name, "runtime.reloadable");
    assert_eq!(
        app.launch_target.local_url.as_deref(),
        Some("http://127.0.0.1:49161")
    );

    let open = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("apps")
        .arg("open")
        .arg("--data-dir")
        .arg(&data_dir)
        .arg("runtime.reloadable/web")
        .output()
        .expect("open refreshed web app");
    assert!(
        open.status.success(),
        "apps open failed after reload: {}",
        command_output_text(&open)
    );
    let open_text = command_output_text(&open);
    assert!(open_text.contains("app_url=http://127.0.0.1:49161"));
    assert!(!open_text.contains(package_dir.to_string_lossy().as_ref()));
    assert!(!open_text.contains(data_dir.to_string_lossy().as_ref()));

    let cli = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("packages")
        .arg("reload")
        .arg("--data-dir")
        .arg(&data_dir)
        .arg("runtime.reloadable")
        .output()
        .expect("run package reload CLI");
    assert!(
        cli.status.success(),
        "packages reload failed: {}",
        command_output_text(&cli)
    );
    let cli_text = command_output_text(&cli);
    assert!(cli_text.contains("decision=package"));
    assert!(cli_text.contains("package_name=runtime.reloadable"));
    assert!(cli_text.contains("action=reload"));
    assert!(cli_text.contains("version=1.1.0"));
    assert!(!cli_text.contains(package_dir.to_string_lossy().as_ref()));
    assert!(!cli_text.contains(data_dir.to_string_lossy().as_ref()));

    let alias = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("reload")
        .arg("runtime.reloadable")
        .arg("--data-dir")
        .arg(&data_dir)
        .output()
        .expect("run package reload alias CLI");
    assert!(
        alias.status.success(),
        "reload alias failed: {}",
        command_output_text(&alias)
    );
    let alias_text = command_output_text(&alias);
    assert!(alias_text.contains("decision=package"));
    assert!(alias_text.contains("package_name=runtime.reloadable"));
    assert!(alias_text.contains("action=reload"));
    assert!(alias_text.contains("version=1.1.0"));
    assert!(!alias_text.contains(package_dir.to_string_lossy().as_ref()));
    assert!(!alias_text.contains(data_dir.to_string_lossy().as_ref()));

    shutdown_cli_daemon(&data_dir, child);
}

#[test]
fn daemon_batch_local_refresh_rejects_mixed_registration_set_on_validation_failure() {
    let _guard = daemon_test_guard();
    let data_dir = unique_test_dir("batch-local-refresh-atomic");
    let alpha_dir = unique_test_dir("batch-local-refresh-alpha");
    let beta_dir = unique_test_dir("batch-local-refresh-beta");
    write_reloadable_app_package_named(
        &alpha_dir,
        "refresh.alpha",
        "1.0.0",
        "http://127.0.0.1:49164",
    );
    write_reloadable_app_package_named(
        &beta_dir,
        "refresh.beta",
        "1.0.0",
        "http://127.0.0.1:49165",
    );
    let config = explicit_config(&data_dir);
    let child = start_cli_daemon(&data_dir);

    for package_dir in [&alpha_dir, &beta_dir] {
        botster_hub::daemon_transport_request(
            &config,
            botster_hub::DaemonRequest::InstallPackageLocalPath {
                path: package_dir.clone(),
            },
        )
        .expect("install local package");
    }

    write_reloadable_app_package_named(
        &alpha_dir,
        "refresh.alpha",
        "2.0.0",
        "http://127.0.0.1:49166",
    );
    fs::remove_file(beta_dir.join("plugin.lua")).expect("remove beta entrypoint");

    let refresh = botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::RefreshLocalPackages,
    )
    .expect("failed refresh should return an operator frame");
    assert_eq!(refresh.kind, botster_hub::DaemonResponseKind::OperatorError);
    let error = refresh.error.expect("refresh operator error");
    assert!(error.message.contains("refresh.beta"));
    assert!(error.message.contains(beta_dir.to_string_lossy().as_ref()));

    let packages =
        botster_hub::daemon_transport_request(&config, botster_hub::DaemonRequest::ListPackages)
            .expect("list packages after failed refresh");
    for package_name in ["refresh.alpha", "refresh.beta"] {
        assert_eq!(
            packages
                .packages
                .iter()
                .find(|package| package.package_name == package_name)
                .expect("installed package")
                .version,
            "1.0.0"
        );
    }
    let state_json =
        fs::read_to_string(data_dir.join("hub-state.json")).expect("read durable hub state");
    assert!(!state_json.contains("\"version\": \"2.0.0\""));

    shutdown_cli_daemon(&data_dir, child);
}

#[test]
fn daemon_exposes_and_resolves_plugin_surface_and_settings_routes() {
    let _guard = daemon_test_guard();
    let data_dir = unique_test_dir("package-route-descriptors");
    let package_dir = unique_test_dir("package-route-descriptors-package");
    write_configurable_local_plugin_package(&package_dir);
    let config = explicit_config(&data_dir);
    let child = start_cli_daemon(&data_dir);

    let install = botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::InstallPackageLocalPath {
            path: package_dir.clone(),
        },
    )
    .expect("install configurable package");
    assert_eq!(
        install.kind,
        botster_hub::DaemonResponseKind::PackageDecision
    );
    let installed = install
        .packages
        .iter()
        .find(|package| package.package_name == "configurable.plugin")
        .expect("installed configurable package");
    let surface_route = package_route(&installed.routes, "surface:config.home");
    assert_eq!(
        surface_route.route_path,
        "/packages/configurable.plugin/surfaces/config.home"
    );
    assert_eq!(surface_route.target.kind, "plugin_surface");
    assert_eq!(
        surface_route.target.surface_id.as_deref(),
        Some("config.home")
    );
    assert_eq!(surface_route.app_id.as_deref(), Some("config.home"));
    assert_eq!(surface_route.surface_id.as_deref(), Some("config.home"));
    assert_eq!(surface_route.title, "Config Home");
    assert_eq!(surface_route.icon.as_deref(), Some("settings"));
    assert_eq!(surface_route.category.as_deref(), Some("configuration"));
    assert_eq!(surface_route.layout_mode, "plugin_surface");
    assert!(surface_route.supports_settings);
    assert!(!surface_route.enabled);
    assert!(surface_route.blocked);
    assert!(
        surface_route
            .required_capabilities
            .iter()
            .any(|capability| capability.surface.eq_ignore_ascii_case("surfaces"))
    );
    assert!(
        surface_route
            .diagnostics
            .iter()
            .any(|diagnostic| diagnostic.kind == "package_not_enabled")
    );

    let settings_route = package_route(&installed.routes, "settings");
    assert_eq!(
        settings_route.route_path,
        "/packages/configurable.plugin/settings"
    );
    assert_eq!(settings_route.target.kind, "package_settings");
    assert_eq!(settings_route.layout_mode, "settings_form");
    assert!(settings_route.supports_settings);
    assert!(settings_route.enabled);
    assert!(!settings_route.blocked);
    assert!(settings_route.required_capabilities.is_empty());
    assert!(
        settings_route
            .diagnostics
            .iter()
            .any(
                |diagnostic| diagnostic.kind == "missing_required_configuration"
                    && diagnostic.message.contains("endpoint")
            )
    );

    let resolved_surface = botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::ResolvePackageRoute {
            package_name: "configurable.plugin".to_string(),
            route_id: "surface:config.home".to_string(),
        },
    )
    .expect("resolve plugin surface route");
    assert_eq!(
        resolved_surface.kind,
        botster_hub::DaemonResponseKind::ResolvedPackageRoute
    );
    assert_eq!(
        resolved_surface
            .resolved_package_route
            .as_ref()
            .expect("resolved route")
            .route_path,
        surface_route.route_path
    );

    let resolved_settings = botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::ResolvePackageRoute {
            package_name: "configurable.plugin".to_string(),
            route_id: "settings".to_string(),
        },
    )
    .expect("resolve settings route");
    assert_eq!(
        resolved_settings.kind,
        botster_hub::DaemonResponseKind::ResolvedPackageRoute
    );
    assert_eq!(
        resolved_settings
            .resolved_package_route
            .as_ref()
            .expect("resolved settings route")
            .target
            .kind,
        "package_settings"
    );

    let missing_route = botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::ResolvePackageRoute {
            package_name: "configurable.plugin".to_string(),
            route_id: "surface:missing".to_string(),
        },
    )
    .expect("missing route returns operator error");
    assert_eq!(
        missing_route.kind,
        botster_hub::DaemonResponseKind::OperatorError
    );
    assert_eq!(
        missing_route.error.as_ref().expect("operator error").code,
        "route_not_found"
    );
    assert!(missing_route.diagnostics.iter().any(|diagnostic| {
        diagnostic
            .message
            .as_deref()
            .is_some_and(|message| message.contains("route_not_found"))
    }));

    shutdown_cli_daemon(&data_dir, child);
}

#[test]
fn daemon_lists_admitted_package_navigation_with_default_app_surface_fallback() {
    let _guard = daemon_test_guard();
    let data_dir = unique_test_dir("package-navigation-registry");
    let default_package_dir = unique_test_dir("package-navigation-default-package");
    let explicit_package_dir = unique_test_dir("package-navigation-explicit-package");
    write_configurable_local_plugin_package(&default_package_dir);
    write_explicit_navigation_local_plugin_package(&explicit_package_dir);
    let config = explicit_config(&data_dir);
    let child = start_cli_daemon(&data_dir);

    botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::InstallPackageLocalPath {
            path: default_package_dir,
        },
    )
    .expect("install default navigation package");
    botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::InstallPackageLocalPath {
            path: explicit_package_dir,
        },
    )
    .expect("install explicit navigation package");

    let blocked = botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::ListPackageNavigation,
    )
    .expect("list package navigation");
    assert_eq!(
        blocked.kind,
        botster_hub::DaemonResponseKind::PackageNavigation
    );
    let default_nav = package_navigation(
        &blocked.package_navigation,
        "configurable.plugin",
        "config.home",
    );
    assert_eq!(default_nav.label, "Config Home");
    assert_eq!(default_nav.route_id, "surface:config.home");
    assert_eq!(
        default_nav.route_path,
        "/packages/configurable.plugin/surfaces/config.home"
    );
    assert!(!default_nav.enabled);
    assert!(default_nav.blocked);
    assert!(
        default_nav
            .diagnostics
            .iter()
            .any(|diagnostic| { diagnostic.kind == "package_not_enabled" })
    );

    let explicit_blocked =
        package_navigation(&blocked.package_navigation, "navigation.plugin", "primary");
    assert_eq!(explicit_blocked.label, "Primary Workbench");
    assert_eq!(explicit_blocked.route_id, "surface:workbench");
    assert!(!explicit_blocked.enabled);
    assert!(explicit_blocked.blocked);
    let blocked_json =
        serde_json::to_string(&blocked.package_navigation).expect("serialize navigation rows");
    assert!(!blocked_json.contains("order"));
    assert!(!blocked_json.contains("priority"));

    botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::EnablePackage {
            package_name: "navigation.plugin".to_string(),
        },
    )
    .expect("enable explicit navigation package");

    let enabled = botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::ListPackageNavigation,
    )
    .expect("list enabled package navigation");
    let enabled_nav =
        package_navigation(&enabled.package_navigation, "navigation.plugin", "primary");
    assert!(enabled_nav.enabled);
    assert!(!enabled_nav.blocked);
    assert!(enabled_nav.diagnostics.is_empty());

    let resolved = botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::ResolvePackageRoute {
            package_name: "navigation.plugin".to_string(),
            route_id: enabled_nav.route_id.clone(),
        },
    )
    .expect("resolve explicit navigation route");
    let route = resolved
        .resolved_package_route
        .as_ref()
        .expect("resolved route");
    assert_eq!(enabled_nav.route_path, route.route_path);
    assert_eq!(enabled_nav.target, route.target);

    shutdown_cli_daemon(&data_dir, child);
}

#[test]
fn local_package_reload_name_mismatch_returns_path_free_operator_error() {
    let _guard = daemon_test_guard();
    let data_dir = unique_test_dir("reload-name-mismatch");
    let package_dir = unique_test_dir("reload-pkg-mismatch");
    write_reloadable_app_package(&package_dir, "1.0.0", "http://127.0.0.1:49162");
    let config = explicit_config(&data_dir);
    let child = start_cli_daemon(&data_dir);

    botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::EnablePackageLocalPath {
            path: package_dir.clone(),
        },
    )
    .expect("enable reloadable local app package");

    write_reloadable_app_package_named(
        &package_dir,
        "runtime.reloadable-renamed",
        "1.1.0",
        "http://127.0.0.1:49163",
    );
    let reload = botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::ReloadPackage {
            package_name: "runtime.reloadable".to_string(),
        },
    )
    .expect("reload renamed local package returns operator frame");

    assert_eq!(reload.kind, botster_hub::DaemonResponseKind::OperatorError);
    let error = reload.error.as_ref().expect("operator error");
    assert!(error.message.contains("InvalidLocalManifest"));
    assert!(error.message.contains("runtime.reloadable-renamed"));
    assert!(error.message.contains("runtime.reloadable"));
    assert!(
        !error
            .message
            .contains(package_dir.to_string_lossy().as_ref())
    );
    assert!(!error.message.contains(data_dir.to_string_lossy().as_ref()));

    shutdown_cli_daemon(&data_dir, child);
}

#[test]
fn daemon_package_list_exposes_dependency_and_feature_availability_matrix() {
    let _guard = daemon_test_guard();
    let data_dir = unique_test_dir("package-availability-daemon");
    let package_dir = unique_test_dir("project-pipelines-availability-package");
    let blocked_package_dir = unique_test_dir("required-dependency-package");
    write_project_pipelines_availability_package(&package_dir);
    write_required_dependency_package(&blocked_package_dir);
    let config = explicit_config(&data_dir);
    let child = start_cli_daemon(&data_dir);

    let enable = botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::EnablePackageLocalPath { path: package_dir },
    )
    .expect("enable project pipelines availability package");
    assert_eq!(
        enable.kind,
        botster_hub::DaemonResponseKind::PackageDecision
    );
    botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::InstallPackageLocalPath {
            path: blocked_package_dir,
        },
    )
    .expect("install required dependency package");

    let list =
        botster_hub::daemon_transport_request(&config, botster_hub::DaemonRequest::ListPackages)
            .expect("list packages with availability matrix");
    let package = list
        .packages
        .iter()
        .find(|package| package.package_name == "project-pipelines")
        .expect("project pipelines package row");

    assert_eq!(
        package.availability.state,
        botster_hub::DaemonPackageAvailabilityState::Available
    );
    let local_feature = package
        .feature_availability
        .iter()
        .find(|feature| feature.id == "local_pipelines")
        .expect("local pipelines feature row");
    assert_eq!(
        local_feature.state,
        botster_hub::DaemonPackageAvailabilityState::Available
    );
    let github_feature = package
        .feature_availability
        .iter()
        .find(|feature| feature.id == "github_pr_lifecycle")
        .expect("github feature row");
    assert_eq!(
        github_feature.state,
        botster_hub::DaemonPackageAvailabilityState::Blocked
    );
    assert!(github_feature.reasons.iter().any(|reason| {
        reason.reason == "missing_package"
            && reason.action == "install_package"
            && reason.package_name.as_deref() == Some("github-provider")
    }));
    assert!(github_feature.reasons.iter().any(|reason| {
        reason.reason == "missing_auth"
            && reason.action == "authenticate"
            && reason.requirement.as_deref() == Some("github_token")
    }));
    let blocked_package = list
        .packages
        .iter()
        .find(|package| package.package_name == "dependency-blocked.plugin")
        .expect("dependency blocked package row");
    assert_eq!(
        blocked_package.availability.state,
        botster_hub::DaemonPackageAvailabilityState::Blocked
    );
    let enable_action = package_action(&blocked_package.actions, "enable_package");
    assert_eq!(
        enable_action.status,
        botster_hub::DaemonPackageActionStatus::Blocked
    );
    assert!(
        enable_action.required_references.iter().any(|reference| {
            reference.kind == "dependency" && reference.key == "github-provider"
        })
    );

    let show = botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::ShowPackage {
            package_name: "project-pipelines".to_string(),
        },
    )
    .expect("show package with availability matrix");
    assert_eq!(
        show.packages[0].feature_availability,
        package.feature_availability
    );

    let dto_json = serde_json::to_string(package).expect("serialize daemon package");
    assert!(!dto_json.contains(&data_dir.display().to_string()));
    assert!(!dto_json.contains(&config.data_directory.display().to_string()));
    assert!(!dto_json.contains("token-value"));

    shutdown_cli_daemon(&data_dir, child);
}

#[test]
fn package_update_apply_preserves_configuration_and_pin_metadata() {
    let _guard = daemon_test_guard();
    let data_dir = unique_test_dir("package-update-apply");
    let package_dir = unique_test_dir("configurable-package-update");
    write_configurable_local_plugin_package(&package_dir);
    let child = start_cli_daemon(&data_dir);
    let config = explicit_config(&data_dir);

    botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::InstallPackageLocalPath {
            path: package_dir.clone(),
        },
    )
    .expect("install configurable package");
    botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::SetPackageConfiguration {
            package_name: "configurable.plugin".to_string(),
            values: BTreeMap::from([
                (
                    "endpoint".to_string(),
                    serde_json::json!({"type":"url","value":"https://example.invalid/hook"}),
                ),
                (
                    "api_token".to_string(),
                    serde_json::json!({"type":"secret","state":"write_only"}),
                ),
            ]),
        },
    )
    .expect("set config before update");

    let pin = botster_hub::DaemonPackagePin {
        revision: "v1.0.1".to_string(),
        branch: Some("main".to_string()),
        tag: Some("v1.0.1".to_string()),
        rev: Some("def456".to_string()),
        checksum: Some("sha256:update-test".to_string()),
        update_policy: "track_source".to_string(),
    };
    let preview = botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::PreviewPackageUpdate {
            package_name: "configurable.plugin".to_string(),
            pin: pin.clone(),
        },
    )
    .expect("preview update");
    assert_eq!(
        preview.kind,
        botster_hub::DaemonResponseKind::PackageUpdateStatus
    );
    assert!(!preview.install_plan.expect("preview plan").mutates_registry);

    let apply = botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::ApplyPackageUpdate {
            package_name: "configurable.plugin".to_string(),
            pin: pin.clone(),
        },
    )
    .expect("apply update");
    assert_eq!(
        apply.package_decision.expect("apply decision").action,
        "apply_update"
    );
    let updated = apply
        .packages
        .iter()
        .find(|package| package.package_name == "configurable.plugin")
        .expect("updated package row");
    assert_eq!(
        updated.configuration.effective_values["api_token"],
        serde_json::json!({"type":"secret","state":"redacted"})
    );

    shutdown_cli_daemon(&data_dir, child);
    let restarted = start_cli_daemon(&data_dir);
    let reloaded =
        botster_hub::daemon_transport_request(&config, botster_hub::DaemonRequest::ListPackages)
            .expect("list after restart");
    let package = reloaded
        .packages
        .iter()
        .find(|package| package.package_name == "configurable.plugin")
        .expect("reloaded package");
    assert_eq!(
        package.configuration.effective_values["endpoint"],
        serde_json::json!({"type":"url","value":"https://example.invalid/hook"})
    );

    shutdown_cli_daemon(&data_dir, restarted);
    let state = FileHubStateStore::for_data_directory(&data_dir)
        .load_or_initialize(&explicit_config(&data_dir))
        .expect("load persisted hub state after update");
    let restored =
        PackageRegistry::from_snapshot(state.package_registry).expect("restore package registry");
    let record = restored
        .package("configurable.plugin")
        .expect("restored configurable package");
    let restored_pin = record.pin.as_ref().expect("restored pin");
    assert_eq!(restored_pin.revision, "v1.0.1");
    assert_eq!(restored_pin.rev.as_deref(), Some("def456"));
    assert_eq!(
        restored_pin.update_policy,
        botster_hub::PackageUpdatePolicy::TrackSource
    );
    assert!(record.configuration.values.contains_key("api_token"));
}

#[test]
fn package_update_unsupported_cases_return_structured_diagnostics() {
    let _guard = daemon_test_guard();
    let data_dir = unique_test_dir("package-update-diagnostics");
    let package_dir = unique_test_dir("local-package-update-diagnostics");
    write_local_plugin_package(&package_dir);
    let child = start_cli_daemon(&data_dir);
    let config = explicit_config(&data_dir);

    botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::InstallPackageLocalPath {
            path: package_dir.clone(),
        },
    )
    .expect("install local package");

    let check = botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::CheckPackageUpdate {
            package_name: "runtime.plugin".to_string(),
        },
    )
    .expect("check update");
    let status = check.update_status.expect("update status");
    assert!(!status.update_available);
    assert!(status.diagnostics.iter().any(|diagnostic| {
        diagnostic.kind == "update_unavailable"
            && diagnostic
                .message
                .contains("without registry source metadata")
    }));
    assert!(
        status
            .diagnostics
            .iter()
            .any(|diagnostic| diagnostic.kind == "pin_required")
    );
    assert_eq!(
        package_action(&status.actions, "check_package_update").status,
        botster_hub::DaemonPackageActionStatus::Available
    );
    let preview_action = package_action(&status.actions, "preview_package_update");
    assert_eq!(
        preview_action.status,
        botster_hub::DaemonPackageActionStatus::Blocked
    );
    assert!(
        preview_action
            .required_references
            .iter()
            .any(|reference| { reference.kind == "pin" && reference.key == "package_update_pin" })
    );
    assert_eq!(
        package_action(&status.actions, "reload_package").status,
        botster_hub::DaemonPackageActionStatus::Available
    );

    botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::EnablePackage {
            package_name: "runtime.plugin".to_string(),
        },
    )
    .expect("enable local package");
    let enabled_check = botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::CheckPackageUpdate {
            package_name: "runtime.plugin".to_string(),
        },
    )
    .expect("check enabled update");
    let enabled_status = enabled_check.update_status.expect("enabled update status");
    assert!(enabled_status.reload_required);
    assert!(enabled_status.diagnostics.iter().any(|diagnostic| {
        diagnostic.kind == "reload_available" && diagnostic.message.contains("reload_package")
    }));

    let cli = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("packages")
        .arg("check-update")
        .arg("--data-dir")
        .arg(&data_dir)
        .arg("runtime.plugin")
        .output()
        .expect("run packages check-update");
    assert!(
        cli.status.success(),
        "packages check-update failed: {}",
        String::from_utf8_lossy(&cli.stderr)
    );
    let stdout = String::from_utf8(cli.stdout).expect("stdout is utf8");
    assert!(stdout.contains("package_update package=runtime.plugin"));
    assert!(stdout.contains("reload_required=true"));
    assert!(
        stdout.contains("package_update_diagnostic package=runtime.plugin kind=reload_available")
    );

    shutdown_cli_daemon(&data_dir, child);
}

#[test]
fn cli_packages_enable_local_process_package_does_not_attempt_lua_load() {
    let _guard = daemon_test_guard();
    let data_dir = unique_test_dir("cli-process-package");
    let package_dir = unique_test_dir("local-process-package");
    write_local_process_plugin_package(&package_dir);
    let child = start_cli_daemon(&data_dir);

    let enable = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("packages")
        .arg("enable")
        .arg("--data-dir")
        .arg(&data_dir)
        .arg("--path")
        .arg(&package_dir)
        .output()
        .expect("run botster-hub packages enable process package");

    assert!(
        enable.status.success(),
        "enable process package failed: {}",
        String::from_utf8_lossy(&enable.stderr)
    );
    let lifecycle = botster_hub::daemon_transport_request(
        &explicit_config(&data_dir),
        botster_hub::DaemonRequest::PluginLifecycleStatus,
    )
    .expect("daemon plugin lifecycle status");
    assert!(lifecycle.lifecycle.iter().any(|plugin| {
        plugin.package_name == "runtime.process-plugin"
            && plugin.state == "enabled"
            && !plugin.loaded
    }));

    shutdown_cli_daemon(&data_dir, child);
}

#[test]
fn cli_packages_enable_without_running_daemon_does_not_mutate_hub_state() {
    let data_dir = unique_test_dir("cli-packages-offline");
    let package_dir = unique_test_dir("local-package-offline");
    write_local_plugin_package(&package_dir);

    let enable = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("packages")
        .arg("enable")
        .arg("--data-dir")
        .arg(&data_dir)
        .arg("--path")
        .arg(&package_dir)
        .output()
        .expect("run botster-hub packages enable without daemon");

    assert!(
        !enable.status.success(),
        "offline enable unexpectedly succeeded: {}",
        String::from_utf8_lossy(&enable.stdout)
    );
    let stderr = String::from_utf8(enable.stderr).expect("stderr is utf8");
    assert!(stderr.contains("daemon not running"));
    assert!(
        !data_dir.join("hub-state.json").exists(),
        "offline package mutation should not create durable state"
    );
}

#[test]
fn no_arg_non_tty_does_not_create_home_or_xdg_state_file() {
    let home = unique_test_dir("home");
    let xdg = unique_test_dir("xdg");
    fs::create_dir_all(&home).expect("create home");
    fs::create_dir_all(&xdg).expect("create xdg");

    let output = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .env("HOME", &home)
        .env("XDG_DATA_HOME", &xdg)
        .output()
        .expect("run botster-hub without a TTY");

    assert!(
        !output.status.success(),
        "no-arg non-TTY unexpectedly succeeded: {}",
        command_output_text(&output)
    );
    assert!(
        command_output_text(&output).contains("scripts must use an explicit subcommand"),
        "{}",
        command_output_text(&output)
    );
    assert_no_state_file_under(&home);
    assert_no_state_file_under(&xdg);
}

fn assert_no_state_file_under(root: &Path) {
    let direct = root.join("hub-state.json");
    let botster = root.join("botster").join("hub-state.json");
    let botster_hub = root.join("botster-hub").join("hub-state.json");
    let canonical_hub = root.join(".botster").join("hub").join("hub-state.json");

    assert!(!direct.exists(), "unexpected state file at {direct:?}");
    assert!(!botster.exists(), "unexpected state file at {botster:?}");
    assert!(
        !botster_hub.exists(),
        "unexpected state file at {botster_hub:?}"
    );
    assert!(
        !canonical_hub.exists(),
        "unexpected state file at {canonical_hub:?}"
    );
}
