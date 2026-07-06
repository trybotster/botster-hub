#![cfg(unix)]

use std::collections::BTreeMap;
use std::fs;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::os::unix::fs::PermissionsExt;
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::sync::{Arc, Mutex, OnceLock, mpsc};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use botster_core::{
    AesGcmEnvelope, AesGcmKey, Capability, CapabilitySurface, CoreSessionMetadata,
    ExtensionEntrypoint, ExtensionKind, ExtensionRuntime, HostProfileMetadata,
    HostProfilePolicySection, PackageManifest, PackageSource, ProcessIdentity, RequestId,
    ResizePayload, SessionId, SessionSpawnRequest, SpawnEnvironment, SpawnWorkingDirectory,
    SubscriptionId, decrypt_aes_gcm, encrypt_aes_gcm,
};
use botster_core_daemon::{RegistryRecord, SessionRegistry};
use botster_hub::{
    AdmittedSessionTemplateTarget, DataDirectoryOption, FileHubStateStore, HostIdentityOptions,
    HubClientApi, HubClientEvent, HubClientRequest, HubClientResponseBody, HubDaemon,
    HubDaemonState, HubStartupOptions, HubStateLoadSource, HubStateStore, PackageAdmissionPolicy,
    PackageProvenance, PackageRegistry, RuntimeEnvironment, SessionDefaults, TransportBindings,
};
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
use support::ensure_session_worker_binary;

static REAL_DAEMON_TEST_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

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
    .build_config_for_environment(&RuntimeEnvironment::from_values(None, None, None))
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
        let plaintext = serde_json::to_vec(request)?;
        let envelope = encrypt_aes_gcm(key, &plaintext, 1)?;
        self.data_channel
            .send_text(&serde_json::to_string(&envelope)?)
            .await?;
        let response = timeout(Duration::from_secs(10), self.data_channel_message_rx.recv())
            .await
            .map_err(|_| std::io::Error::other("timed out waiting for data channel response"))?
            .ok_or_else(|| {
                std::io::Error::other("local WebRTC data channel closed before response")
            })?;
        let envelope = serde_json::from_str::<AesGcmEnvelope>(&response)?;
        let plaintext = decrypt_aes_gcm(key, &envelope)?;
        Ok(serde_json::from_slice(&plaintext)?)
    }
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

fn provider_manifest() -> PackageManifest {
    let capabilities = vec![Capability {
        surface: CapabilitySurface::Surfaces,
        scope: None,
    }];

    PackageManifest {
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
    }
}

fn write_local_plugin_package(root: &Path) {
    fs::create_dir_all(root).expect("create local package root");
    fs::write(root.join("plugin.lua"), "return botster.register({})\n")
        .expect("write plugin entrypoint");
    fs::write(
        root.join("botster-package.json"),
        r#"{
  "name": "dogfood.plugin",
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
        "name": "dogfood.session-template",
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
        "name": "dogfood.apps",
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
    write_reloadable_app_package_named(root, "dogfood.reloadable", version, local_url);
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
    let manifest = serde_json::json!({
        "name": "dogfood.hub-env",
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
            "command": "sh",
            "args": [
                "-c",
                "if [ -z \"$BOTSTER_HUB_SOCKET\" ] || [ -z \"$BOTSTER_HUB_DATA_DIR\" ]; then echo 'BOTSTER_HUB_BIN must point to a botster-hub binary' >&2; exit 42; fi; test -S \"$BOTSTER_HUB_SOCKET\" || exit 43; test -d \"$BOTSTER_HUB_DATA_DIR\" || exit 44; test \"$BOTSTER_WEB_MODE\" = daemon-default || exit 45; printf '%s\n' '{\"entrypoint_id\":\"web\",\"process_state\":\"running\",\"local_url\":\"http://127.0.0.1:49153\"}' > \"$BOTSTER_ENTRYPOINT_LAUNCH_RESULT\"; while true; do sleep 1; done"
            ],
            "working_directory": { "policy": "package_root" },
            "environment": [
                { "name": "BOTSTER_HUB_SOCKET", "required": false },
                { "name": "BOTSTER_HUB_DATA_DIR", "required": false },
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
            "args": ["-c", "test -n \"$BOTSTER_HUB_SOCKET\" && test -n \"$BOTSTER_HUB_DATA_DIR\" && printf 'botster-tui-fixture\\n'"],
            "working_directory": { "policy": "package_root" },
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
        root.join("scripts").join("real-hub-dogfood-bridge.mjs"),
        r#"
import fs from 'fs';
import http from 'http';
import net from 'net';

const port = Number(process.env.BOTSTER_WEB_DOGFOOD_BRIDGE_PORT || '41739');
const socket = process.env.BOTSTER_HUB_SOCKET;
const dataDir = process.env.BOTSTER_HUB_DATA_DIR;
const launchResult = process.env.BOTSTER_ENTRYPOINT_LAUNCH_RESULT;
const mixedOwnership = Boolean(process.env.BOTSTER_WEB_DOGFOOD_DATA_DIR && (socket || dataDir));
const source = socket ? 'socket' : (dataDir ? 'data_dir' : 'spawned');
const mode = socket || dataDir ? 'existing_hub' : 'spawned_hub';
const socketExists = socket ? fs.existsSync(socket) : false;
const localWebrtcGrantPresent = Boolean(process.env.BOTSTER_LOCAL_WEBRTC_GRANT_ID);
const localWebrtcSecretPresent = Boolean(process.env.BOTSTER_LOCAL_WEBRTC_GRANT_SECRET);
const localWebrtcOriginPresent = Boolean(process.env.BOTSTER_LOCAL_WEBRTC_EXPECTED_ORIGIN);
const connections = new Map();

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
    client_name: 'botster-web-dogfood-bridge-fixture',
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
    throw new Error('BOTSTER_HUB_SOCKET is not set');
  }
  const stream = net.createConnection(socket);
  const connection = { stream, buffer: '' };
  await new Promise((resolve, reject) => {
    stream.once('connect', resolve);
    stream.once('error', reject);
  });
  stream.write(JSON.stringify({
    protocol: 'botster-hub-daemon-v1',
    compatibility: currentRequirement(),
  }) + '\n');
  await readLine(connection);
  return connection;
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

function readBody(request) {
  return new Promise((resolve, reject) => {
    let body = '';
    request.on('data', (chunk) => {
      body += chunk.toString('utf8');
    });
    request.on('end', () => resolve(body));
    request.on('error', reject);
  });
}

http.createServer(async (request, response) => {
  if (request.url === '/?dogfood=real-hub') {
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
  if (request.url === '/bridge' && request.method === 'POST') {
    try {
      const payload = JSON.parse(await readBody(request));
      const daemonResponse = await daemonRequest(payload);
      response.writeHead(200, { 'content-type': 'application/json' });
      response.end(JSON.stringify({
        ok: true,
        mode,
        source,
        dataDir,
        socketExists,
        response: daemonResponse,
      }));
    } catch (error) {
      response.writeHead(502, { 'content-type': 'application/json' });
      response.end(JSON.stringify({
        ok: false,
        mode,
        source,
        dataDir,
        socketExists,
        error: String(error && error.message ? error.message : error),
      }));
    }
    return;
  }
  if (request.url !== '/health') {
    response.writeHead(404);
    response.end('not found');
    return;
  }
  response.writeHead(200, { 'content-type': 'application/json' });
    response.end(JSON.stringify({
    ok: mode === 'existing_hub' && source === 'socket' && socketExists && !mixedOwnership,
    mode,
    source,
    port,
    socketExists,
    mixedOwnership,
  }));
}).listen(port, '127.0.0.1', () => {
  if (launchResult) {
    fs.writeFileSync(launchResult, JSON.stringify({
      entrypoint_id: 'web-client',
      process_state: 'running',
      local_url: `http://127.0.0.1:${port}/?dogfood=real-hub`,
      local_webrtc_grant_present: localWebrtcGrantPresent,
      local_webrtc_secret_present: localWebrtcSecretPresent,
      local_webrtc_origin_present: localWebrtcOriginPresent,
    }));
  }
});
"#,
    )
    .expect("write botster-web bridge script");
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
            "args": ["scripts/real-hub-dogfood-bridge.mjs"],
            "working_directory": { "policy": "package_root" },
            "environment": [
                { "name": "BOTSTER_HUB_SOCKET", "required": false },
                { "name": "BOTSTER_HUB_DATA_DIR", "required": false },
                { "name": "BOTSTER_WEB_DOGFOOD_BRIDGE_PORT", "required": false, "default": "41739" },
                { "name": "BOTSTER_LOCAL_WEBRTC_GRANT_ID", "required": false },
                { "name": "BOTSTER_LOCAL_WEBRTC_GRANT_SECRET", "required": false },
                { "name": "BOTSTER_LOCAL_WEBRTC_SIGNALING_TRANSPORT", "required": false },
                { "name": "BOTSTER_LOCAL_WEBRTC_EXPECTED_ORIGIN", "required": false }
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

fn write_health_only_botster_web_package(root: &Path) {
    fs::create_dir_all(root.join("scripts")).expect("create health-only botster-web package root");
    fs::write(root.join("plugin.lua"), "return botster.register({})\n")
        .expect("write health-only botster-web core entrypoint");
    fs::write(
        root.join("scripts").join("real-hub-dogfood-bridge.mjs"),
        r#"
import fs from 'fs';
import http from 'http';

const port = Number(process.env.BOTSTER_WEB_DOGFOOD_BRIDGE_PORT || '41739');
const socket = process.env.BOTSTER_HUB_SOCKET;
const dataDir = process.env.BOTSTER_HUB_DATA_DIR;
const source = socket ? 'socket' : (dataDir ? 'data_dir' : 'spawned');
const mode = socket || dataDir ? 'existing_hub' : 'spawned_hub';
const socketExists = socket ? fs.existsSync(socket) : false;

http.createServer((request, response) => {
  if (request.url !== '/health') {
    response.writeHead(404, { 'content-type': 'application/json' });
    response.end(JSON.stringify({ error: 'not_found' }));
    return;
  }
  response.writeHead(200, { 'content-type': 'application/json' });
  response.end(JSON.stringify({
    ok: mode === 'existing_hub' && source === 'socket' && socketExists,
    mode,
    source,
    port,
    socketExists,
  }));
}).listen(port, '127.0.0.1');
"#,
    )
    .expect("write health-only botster-web bridge script");
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
            "args": ["scripts/real-hub-dogfood-bridge.mjs"],
            "working_directory": { "policy": "package_root" },
            "environment": [
                { "name": "BOTSTER_HUB_SOCKET", "required": false },
                { "name": "BOTSTER_HUB_DATA_DIR", "required": false },
                { "name": "BOTSTER_WEB_DOGFOOD_BRIDGE_PORT", "required": false, "default": "41739" }
            ],
            "launch_mode": "background",
            "capabilities": [{ "surface": "network", "scope": "localhost" }],
            "may_supervise": true
        }]
    });
    fs::write(
        root.join("botster-package.json"),
        serde_json::to_string_pretty(&manifest)
            .expect("serialize health-only botster-web manifest"),
    )
    .expect("write health-only botster-web manifest");
}

fn write_failing_botster_web_package(root: &Path) {
    fs::create_dir_all(root.join("scripts")).expect("create failing botster-web package root");
    fs::write(root.join("plugin.lua"), "return botster.register({})\n")
        .expect("write failing botster-web core entrypoint");
    fs::write(
        root.join("scripts").join("real-hub-dogfood-bridge.mjs"),
        "console.error('bridge bind failed: fixture'); process.exit(42);\n",
    )
    .expect("write failing botster-web bridge script");
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
            "args": ["scripts/real-hub-dogfood-bridge.mjs"],
            "working_directory": { "policy": "package_root" },
            "environment": [
                { "name": "BOTSTER_HUB_SOCKET", "required": false },
                { "name": "BOTSTER_HUB_DATA_DIR", "required": false },
                { "name": "BOTSTER_WEB_DOGFOOD_BRIDGE_PORT", "required": false, "default": "41739" }
            ],
            "launch_mode": "background",
            "capabilities": [{ "surface": "network", "scope": "localhost" }],
            "may_supervise": true
        }]
    });
    fs::write(
        root.join("botster-package.json"),
        serde_json::to_string_pretty(&manifest).expect("serialize failing botster-web manifest"),
    )
    .expect("write failing botster-web manifest");
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

fn read_web_html(url: &str) -> String {
    let bridge_url = url
        .strip_suffix("/?dogfood=real-hub")
        .expect("web URL path");
    let (headers, body) = read_http_path(bridge_url, "/?dogfood=real-hub");
    assert!(
        headers.starts_with("HTTP/1.1 200") || headers.starts_with("HTTP/1.0 200"),
        "web URL returned non-200: {headers}"
    );
    assert!(
        headers
            .to_ascii_lowercase()
            .contains("content-type: text/html"),
        "web URL did not return HTML: {headers}"
    );
    assert!(body.contains("<!doctype html>") || body.contains("<html"));
    assert!(!body.contains(r#""error":"not_found""#));
    assert_ne!(body.trim(), "not found");
    body
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

fn post_http_json(url: &str, path: &str, body: &str) -> (String, String) {
    let port = url
        .strip_prefix("http://127.0.0.1:")
        .expect("local HTTP URL")
        .parse::<u16>()
        .expect("HTTP port");
    let mut stream = TcpStream::connect(("127.0.0.1", port)).expect("connect HTTP endpoint");
    let request = format!(
        "POST {path} HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
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

fn dogfood_bridge_request(
    bridge_url: &str,
    connection_id: Option<&str>,
    request: &botster_hub_client::DaemonRequest,
) -> botster_hub_client::DaemonResponse {
    let mut payload = serde_json::json!({ "request": request });
    if let Some(connection_id) = connection_id {
        payload["connection_id"] = serde_json::Value::String(connection_id.to_string());
    }
    let body = serde_json::to_string(&payload).expect("serialize bridge request");
    let (headers, body) = post_http_json(bridge_url, "/bridge", &body);
    assert!(
        headers.starts_with("HTTP/1.1 200") || headers.starts_with("HTTP/1.0 200"),
        "bridge request returned non-200: {headers} body={body}"
    );
    let envelope: serde_json::Value = serde_json::from_str(body.trim()).expect("bridge JSON");
    assert_eq!(envelope["ok"], true, "bridge envelope: {envelope}");
    assert_eq!(envelope["mode"], "existing_hub");
    assert_eq!(envelope["source"], "socket");
    assert_eq!(envelope["socketExists"], true);
    serde_json::from_value(envelope["response"].clone()).expect("daemon response JSON")
}

fn botster_web_page_bootstrap(bridge_url: &str) -> botster_hub_client::DaemonLocalWebrtcBootstrap {
    let (headers, body) = read_http_path(bridge_url, "/?dogfood=real-hub");
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

fn wait_for_botster_web_health(bridge_url: &str) {
    let port = bridge_url
        .strip_prefix("http://127.0.0.1:")
        .expect("local HTTP URL")
        .parse::<u16>()
        .expect("HTTP port");
    for _ in 0..100 {
        if TcpStream::connect(("127.0.0.1", port)).is_ok() {
            let health = read_json_health(bridge_url);
            if health["ok"] == true {
                return;
            }
        }
        thread::sleep(Duration::from_millis(20));
    }
    panic!("botster-web bridge health did not become ready");
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
  "name": "dogfood.process-plugin",
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
  "name": "dogfood.surface-plugin",
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
      "id": "dogfood.surface.home",
      "kind": "app",
      "title": "Dogfood Surface",
      "description": "Surface descriptor fixture",
      "icon": "workflow",
      "order": 20,
      "category": "dogfood",
      "supports": ["render", "action"]
    },
    {
      "id": "dogfood.surface.settings",
      "kind": "settings",
      "title": "Dogfood Settings",
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
  "name": "dogfood.incompatible-plugin",
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
  "name": "dogfood.denied-plugin",
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
  local workspace = {
    id = workspace_id(arguments),
    name = arguments.name or "Local Workspace",
    status = "created",
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
  workspace.status = "used"
  botster.capabilities.plugin_db.set({
    key = "workspace/" .. workspace.id,
    schema_version = 1,
    payload = workspace,
  })
  return { ok = true, workspace = workspace }
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
        },
        additionalProperties = false,
      },
      handler = "use",
      call = use_workspace,
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

fn daemon_test_lock() -> &'static Mutex<()> {
    REAL_DAEMON_TEST_LOCK.get_or_init(|| Mutex::new(()))
}

fn start_cli_daemon(data_dir: &Path) -> Child {
    ensure_session_worker_binary();
    let mut child = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("start")
        .arg("--data-dir")
        .arg(data_dir)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn botster-hub start");

    wait_for_status(data_dir, &mut child);
    child
}

fn start_owned_incompatible_dev_stack_daemon(data_dir: &Path) -> Child {
    ensure_session_worker_binary();
    fs::create_dir_all(data_dir).expect("create data dir");
    let mut child = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("start")
        .arg("--data-dir")
        .arg(data_dir)
        .arg("--session-worker-bin")
        .arg(session_worker_binary_path())
        .env("BOTSTER_HUB_TEST_INCOMPATIBLE_DAEMON", "1")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn incompatible botster-hub start");
    wait_for_incompatible_status(data_dir, &mut child);
    write_dev_stack_daemon_metadata(data_dir, child.id());
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

fn write_dev_stack_daemon_metadata(data_dir: &Path, pid: u32) {
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
    let metadata_path = data_dir.join(".botster-hub-dev-stack-daemon.json");
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
    let mut child = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("start")
        .arg("--data-dir")
        .arg(data_dir)
        .arg("--session-worker-bin")
        .arg(session_worker_bin)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn botster-hub start");

    wait_for_status(data_dir, &mut child);
    child
}

fn wait_for_status(data_dir: &Path, child: &mut Child) {
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while std::time::Instant::now() < deadline {
        if let Some(status) = child.try_wait().expect("check daemon child") {
            let mut stdout = String::new();
            let mut stderr = String::new();
            if let Some(mut pipe) = child.stdout.take() {
                let _ = pipe.read_to_string(&mut stdout);
            }
            if let Some(mut pipe) = child.stderr.take() {
                let _ = pipe.read_to_string(&mut stderr);
            }
            panic!("daemon exited before ready with {status}: stdout={stdout:?} stderr={stderr:?}");
        }
        let output = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
            .arg("status")
            .arg("--data-dir")
            .arg(data_dir)
            .output()
            .expect("run botster-hub status");
        if output.status.success() {
            return;
        }
        thread::sleep(Duration::from_millis(50));
    }
    panic!("daemon did not become ready");
}

fn shutdown_cli_daemon(data_dir: &Path, child: Child) -> Output {
    let shutdown = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("shutdown")
        .arg("--data-dir")
        .arg(data_dir)
        .output()
        .expect("run botster-hub shutdown");
    assert!(
        shutdown.status.success(),
        "shutdown failed: {}",
        String::from_utf8_lossy(&shutdown.stderr)
    );
    let output = child.wait_with_output().expect("wait for daemon child");
    assert!(
        output.status.success(),
        "daemon failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    output
}

fn run_dev_stack_bootstrap(
    data_dir: &Path,
    project_pipelines_package_path: &Path,
    web_package_path: &Path,
    tui_package_path: &Path,
    workspaces_package_path: &Path,
    web_bridge_port: u16,
) -> Output {
    ensure_session_worker_binary();
    Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("dev-stack")
        .arg("bootstrap")
        .arg("--data-dir")
        .arg(data_dir)
        .arg("--session-worker-bin")
        .arg(session_worker_binary_path())
        .arg("--project-pipelines-package-path")
        .arg(project_pipelines_package_path)
        .arg("--web-package-path")
        .arg(web_package_path)
        .arg("--tui-package-path")
        .arg(tui_package_path)
        .arg("--workspaces-package-path")
        .arg(workspaces_package_path)
        .arg("--web-bridge-port")
        .arg(web_bridge_port.to_string())
        .output()
        .expect("run dev-stack bootstrap")
}

fn run_local_runtime_up(
    data_dir: &Path,
    project_pipelines_package_path: &Path,
    web_package_path: &Path,
    tui_package_path: &Path,
    workspaces_package_path: &Path,
    web_bridge_port: u16,
) -> Output {
    ensure_session_worker_binary();
    Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("up")
        .arg("--data-dir")
        .arg(data_dir)
        .arg("--session-worker-bin")
        .arg(session_worker_binary_path())
        .arg("--project-pipelines-package-path")
        .arg(project_pipelines_package_path)
        .arg("--web-package-path")
        .arg(web_package_path)
        .arg("--tui-package-path")
        .arg(tui_package_path)
        .arg("--workspaces-package-path")
        .arg(workspaces_package_path)
        .arg("--web-bridge-port")
        .arg(web_bridge_port.to_string())
        .output()
        .expect("run botster-hub up")
}

fn run_local_runtime_smoke(
    data_dir: &Path,
    project_pipelines_package_path: &Path,
    web_package_path: &Path,
    tui_package_path: &Path,
    workspaces_package_path: &Path,
    web_bridge_port: u16,
) -> Output {
    ensure_session_worker_binary();
    Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("smoke")
        .arg("--data-dir")
        .arg(data_dir)
        .arg("--session-worker-bin")
        .arg(session_worker_binary_path())
        .arg("--project-pipelines-package-path")
        .arg(project_pipelines_package_path)
        .arg("--web-package-path")
        .arg(web_package_path)
        .arg("--tui-package-path")
        .arg(tui_package_path)
        .arg("--workspaces-package-path")
        .arg(workspaces_package_path)
        .arg("--web-bridge-port")
        .arg(web_bridge_port.to_string())
        .output()
        .expect("run botster-hub smoke")
}

fn shutdown_dev_stack_daemon(data_dir: &Path) {
    let output = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("shutdown")
        .arg("--data-dir")
        .arg(data_dir)
        .output()
        .expect("run dev-stack shutdown");
    assert!(
        output.status.success(),
        "dev-stack shutdown failed: {}",
        command_output_text(&output)
    );
}

fn enabled_package_names(data_dir: &Path) -> Vec<String> {
    let response = botster_hub::daemon_transport_request(
        &explicit_config(data_dir),
        botster_hub::DaemonRequest::ListPackages,
    )
    .expect("list dev-stack packages");
    let mut names = response
        .packages
        .into_iter()
        .filter(|package| package.state == "enabled")
        .map(|package| package.package_name)
        .collect::<Vec<_>>();
    names.sort();
    names
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

fn run_command_with_timeout(mut command: Command, timeout: Duration) -> Output {
    let mut child = command
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn timed command");
    let deadline = std::time::Instant::now() + timeout;

    while std::time::Instant::now() < deadline {
        if child.try_wait().expect("poll timed command").is_some() {
            return child.wait_with_output().expect("collect timed command");
        }
        thread::sleep(Duration::from_millis(20));
    }

    let _ = child.kill();
    let output = child.wait_with_output().expect("collect timed out command");
    panic!(
        "command timed out after {timeout:?}: stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

fn spawn_dogfood_launcher(
    data_dir: Option<&Path>,
    web_package_path: &Path,
    web_bridge_port: Option<u16>,
) -> (Child, mpsc::Receiver<String>) {
    spawn_dogfood_launcher_with_tui(data_dir, web_package_path, None, web_bridge_port)
}

fn spawn_dogfood_launcher_with_tui(
    data_dir: Option<&Path>,
    web_package_path: &Path,
    tui_package_path: Option<&Path>,
    web_bridge_port: Option<u16>,
) -> (Child, mpsc::Receiver<String>) {
    ensure_session_worker_binary();
    let mut command = Command::new(env!("CARGO_BIN_EXE_botster-hub"));
    command.arg("dogfood");
    if let Some(data_dir) = data_dir {
        command.arg("--data-dir").arg(data_dir);
    }
    command.arg("--web-package-path").arg(web_package_path);
    if let Some(tui_package_path) = tui_package_path {
        command.arg("--tui-package-path").arg(tui_package_path);
    }
    if let Some(web_bridge_port) = web_bridge_port {
        command
            .arg("--web-bridge-port")
            .arg(web_bridge_port.to_string());
    }
    let mut child = command
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn botster-hub dogfood");

    let stdout = child.stdout.take().expect("dogfood stdout pipe");
    let (tx, rx) = mpsc::channel();
    thread::spawn(move || {
        for line in BufReader::new(stdout).lines() {
            let Ok(line) = line else {
                break;
            };
            if tx.send(line).is_err() {
                break;
            }
        }
    });

    (child, rx)
}

fn collect_dogfood_ready_output(child: &mut Child, rx: &mpsc::Receiver<String>) -> Vec<String> {
    let mut lines = wait_for_dogfood_output(child, rx, "dogfood=ready");
    lines.extend(wait_for_dogfood_output(
        child,
        rx,
        "bridge=http://127.0.0.1:",
    ));
    lines.extend(wait_for_dogfood_output(child, rx, "web=http://127.0.0.1:"));
    lines.extend(wait_for_dogfood_output(child, rx, "shutdown="));
    lines
}

fn dogfood_output_value(lines: &[String], prefix: &str) -> String {
    lines
        .iter()
        .find_map(|line| line.strip_prefix(prefix).map(str::to_string))
        .unwrap_or_else(|| panic!("missing dogfood output line with prefix {prefix:?}: {lines:?}"))
}

fn dogfood_bridge_port(lines: &[String]) -> u16 {
    dogfood_output_value(lines, "bridge=http://127.0.0.1:")
        .parse()
        .expect("dogfood bridge port is numeric")
}

fn dogfood_web_url(lines: &[String]) -> String {
    dogfood_output_value(lines, "web=")
}

fn dogfood_data_dir(lines: &[String]) -> PathBuf {
    let value = dogfood_output_value(lines, "data_dir=");
    PathBuf::from(value.strip_prefix("isolated:").unwrap_or(&value))
}

fn wait_for_dogfood_output(
    child: &mut Child,
    rx: &mpsc::Receiver<String>,
    needle: &str,
) -> Vec<String> {
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    let mut lines = Vec::new();
    while std::time::Instant::now() < deadline {
        if let Some(status) = child.try_wait().expect("poll dogfood launcher") {
            let mut stderr = String::new();
            if let Some(mut pipe) = child.stderr.take() {
                let _ = pipe.read_to_string(&mut stderr);
            }
            panic!(
                "dogfood exited before {needle:?} with {status}: lines={lines:?} stderr={stderr:?}"
            );
        }

        match rx.recv_timeout(Duration::from_millis(50)) {
            Ok(line) => {
                let found = line.contains(needle);
                lines.push(line);
                if found {
                    return lines;
                }
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {}
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                let status = child
                    .try_wait()
                    .expect("poll dogfood launcher after stdout close");
                let mut stderr = String::new();
                if let Some(mut pipe) = child.stderr.take() {
                    let _ = pipe.read_to_string(&mut stderr);
                }
                panic!(
                    "dogfood stdout closed before {needle:?}: status={status:?} lines={lines:?} stderr={stderr:?}"
                );
            }
        }
    }

    panic!("timed out waiting for {needle:?}: lines={lines:?}");
}

#[test]
fn daemon_package_dtos_expose_declared_surfaces_and_validate_surface_ids() {
    let _guard = daemon_test_lock()
        .lock()
        .expect("serialize real daemon test");
    let data_dir = unique_short_test_dir("package-surfaces");
    let surface_package_dir = unique_test_dir("daemon-declared-surface-package");
    let legacy_package_dir = unique_test_dir("daemon-legacy-surface-package");
    let workspaces_package_dir = unique_test_dir("daemon-workspaces-surface-package");
    write_declared_surface_plugin_package(&surface_package_dir);
    write_local_plugin_package(&legacy_package_dir);
    write_botster_workspaces_local_package(&workspaces_package_dir, "botster-workspaces");
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

    let packages = connection
        .request(&botster_hub_client::DaemonRequest::ListPackages)
        .expect("list packages with declared surfaces");
    let surface_package = packages
        .packages
        .iter()
        .find(|package| package.package_name == "dogfood.surface-plugin")
        .expect("surface package listed");
    assert_eq!(surface_package.surfaces.len(), 2);
    let surface = &surface_package.surfaces[0];
    assert_eq!(surface.id, "dogfood.surface.home");
    assert_eq!(surface.kind, "app");
    assert_eq!(surface.title, "Dogfood Surface");
    assert_eq!(
        surface.description.as_deref(),
        Some("Surface descriptor fixture")
    );
    assert_eq!(surface.icon.as_deref(), Some("workflow"));
    assert_eq!(surface.order, Some(20));
    assert_eq!(surface.category.as_deref(), Some("dogfood"));
    assert_eq!(surface.supports, ["render", "action"]);

    let show = connection
        .request(&botster_hub_client::DaemonRequest::ShowPackage {
            package_name: "dogfood.surface-plugin".to_string(),
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
    assert_eq!(plugin_surface.package_name, "botster-workspaces");
    assert_eq!(plugin_surface.surface_id, "workspaces");
    assert_eq!(plugin_surface.body["type"], "panel");
    assert_eq!(plugin_surface.body["id"], "botster-workspaces-panel");

    let undeclared = connection
        .request(&botster_hub_client::DaemonRequest::PluginSurfaceRender {
            package_name: "dogfood.surface-plugin".to_string(),
            surface_id: "dogfood.surface.missing".to_string(),
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

    let legacy_passthrough = connection
        .request(&botster_hub_client::DaemonRequest::PluginSurfaceRender {
            package_name: "dogfood.plugin".to_string(),
            surface_id: "legacy.dynamic.surface".to_string(),
            payload: serde_json::json!({}),
        })
        .expect("legacy package render passes beyond descriptor guard");
    assert_eq!(
        legacy_passthrough.kind,
        botster_hub_client::DaemonResponseKind::OperatorError
    );
    assert_ne!(
        legacy_passthrough
            .error
            .as_ref()
            .expect("legacy operator error")
            .code,
        "undeclared_plugin_surface"
    );

    let status = connection
        .request(&botster_hub_client::DaemonRequest::Status)
        .expect("daemon remains responsive after surface validation");
    assert_eq!(status.kind, botster_hub_client::DaemonResponseKind::Status);
    shutdown_cli_daemon(&data_dir, child);
}

#[test]
fn daemon_plugin_contract_matrix_fixture_exercises_public_package_contracts() {
    let _guard = daemon_test_lock()
        .lock()
        .expect("serialize real daemon test");
    let fixture_dir = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("fixtures")
        .join("plugins")
        .join("plugin-contract-matrix");
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
    assert_eq!(report.package_name, "botster.plugin-contract-matrix");
    assert_eq!(report.installed_state, "installed");
    assert_eq!(report.enabled_state, "enabled");
    assert_eq!(
        report.surface_ids,
        vec![
            "contract.app",
            "contract.empty",
            "contract.blocked",
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
    assert_eq!(report.empty_surface_child_id, "contract-empty-message");
    assert_eq!(report.blocked_render_operation, "plugin_surface_render");
    assert!(report.blocked_render_message_contains_failure);
    assert_eq!(report.settings_surface_node_id, "contract-settings-panel");
    assert!(report.settings_text_contains_endpoint);
    assert!(report.settings_text_contains_mode);
    assert!(report.settings_text_contains_redacted_secret);
    assert_eq!(report.action_success_state, "accepted");
    assert_eq!(report.action_success_message, "hello");
    assert_eq!(report.action_error_state, "error");
    assert_eq!(report.action_error_diagnostic_kind, "action_failure");
    assert_eq!(
        report.action_error_diagnostic_operation,
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
fn cli_dev_stack_first_party_plugin_dogfood_smoke_runs_contract_matrix_then_real_packages() {
    let _guard = daemon_test_lock()
        .lock()
        .expect("serialize real daemon test");
    let fixture_dir = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("fixtures")
        .join("plugins")
        .join("plugin-contract-matrix");
    let project_pipelines_package_dir = std::env::current_dir()
        .expect("current dir")
        .join("examples")
        .join("project-pipelines");
    assert!(
        project_pipelines_package_dir
            .join("botster-package.json")
            .is_file(),
        "failure_class=local_package_path missing project-pipelines package at {project_pipelines_package_dir:?}"
    );
    let workspaces_package_dir = unique_test_dir("first-party-dogfood-workspaces-package");
    write_botster_workspaces_local_package(&workspaces_package_dir, "botster-workspaces");

    let hub = botster_hub_test_support::IsolatedHubBuilder::new()
        .hub_bin(env!("CARGO_BIN_EXE_botster-hub"))
        .session_worker_bin(session_worker_binary_path())
        .root(PathBuf::from("/tmp/bh-first-party-plugin-dogfood"))
        .name("first-party-plugin-dogfood")
        .start()
        .expect("failure_class=environment start isolated hub through public harness");

    let contract_report =
        botster_hub_test_support::run_plugin_contract_matrix_conformance(&hub, fixture_dir)
            .expect("failure_class=hub_contract run generic plugin contract matrix first");
    assert_eq!(
        contract_report.package_name,
        "botster.plugin-contract-matrix"
    );
    assert_eq!(
        contract_report.app_surface_package_name,
        "botster.plugin-contract-matrix"
    );
    assert_eq!(contract_report.app_surface_id, "contract.app");

    let workspaces_install = botster_hub_client::request(
        hub.endpoint(),
        botster_hub_client::DaemonRequest::InstallPackageLocalPath {
            path: workspaces_package_dir.clone(),
        },
    )
    .expect("failure_class=local_package_path install helper-written botster-workspaces package");
    assert_eq!(
        workspaces_install.kind,
        botster_hub_client::DaemonResponseKind::PackageDecision
    );
    let installed_workspaces = workspaces_install
        .packages
        .iter()
        .find(|package| package.package_name == "botster-workspaces")
        .expect("failure_class=plugin_producer install response includes botster-workspaces");
    assert_eq!(installed_workspaces.state, "installed");
    let installed_workspaces_route =
        package_route(&installed_workspaces.routes, "surface:workspaces");
    assert_eq!(
        installed_workspaces_route.route_path,
        "/packages/botster-workspaces/surfaces/workspaces"
    );
    assert_eq!(installed_workspaces_route.target.kind, "plugin_surface");
    assert_eq!(
        installed_workspaces_route.target.surface_id.as_deref(),
        Some("workspaces"),
        "failure_class=plugin_producer missing package route target surface metadata"
    );
    assert_eq!(
        installed_workspaces_route.surface_id.as_deref(),
        Some("workspaces"),
        "failure_class=web_tui_consumer missing package route surface metadata"
    );
    assert!(installed_workspaces_route.blocked);

    let workspaces_enable = botster_hub_client::request(
        hub.endpoint(),
        botster_hub_client::DaemonRequest::EnablePackage {
            package_name: "botster-workspaces".to_string(),
        },
    )
    .expect("failure_class=hub_contract enable botster-workspaces package");
    assert_eq!(
        workspaces_enable.kind,
        botster_hub_client::DaemonResponseKind::PackageDecision
    );
    let enabled_workspaces = workspaces_enable
        .packages
        .iter()
        .find(|package| package.package_name == "botster-workspaces")
        .expect("failure_class=plugin_producer enable response includes botster-workspaces");
    assert_eq!(enabled_workspaces.state, "enabled");
    let enabled_workspaces_route = package_route(&enabled_workspaces.routes, "surface:workspaces");
    assert!(!enabled_workspaces_route.blocked);
    assert!(enabled_workspaces_route.enabled);

    let workspaces_reload = botster_hub_client::request(
        hub.endpoint(),
        botster_hub_client::DaemonRequest::ReloadPackage {
            package_name: "botster-workspaces".to_string(),
        },
    )
    .expect("failure_class=stale_build reload helper-written botster-workspaces package");
    assert_eq!(
        workspaces_reload.kind,
        botster_hub_client::DaemonResponseKind::PackageDecision
    );
    assert_eq!(
        workspaces_reload
            .package_decision
            .as_ref()
            .expect("failure_class=stale_build reload decision")
            .action,
        "reload"
    );

    let workspaces_show = botster_hub_client::request(
        hub.endpoint(),
        botster_hub_client::DaemonRequest::ShowPackage {
            package_name: "botster-workspaces".to_string(),
        },
    )
    .expect("failure_class=hub_contract show botster-workspaces package");
    assert_eq!(
        workspaces_show.kind,
        botster_hub_client::DaemonResponseKind::Packages
    );
    let shown_workspaces = workspaces_show
        .packages
        .iter()
        .find(|package| package.package_name == "botster-workspaces")
        .expect("failure_class=plugin_producer show response includes botster-workspaces");
    assert_eq!(
        shown_workspaces.routes, enabled_workspaces.routes,
        "failure_class=web_tui_consumer ListPackages/ShowPackage route descriptors diverged"
    );

    let workspaces_surface = botster_hub_client::request(
        hub.endpoint(),
        botster_hub_client::DaemonRequest::PluginSurfaceRender {
            package_name: "botster-workspaces".to_string(),
            surface_id: "workspaces".to_string(),
            payload: serde_json::json!({}),
        },
    )
    .expect("failure_class=plugin_producer render botster-workspaces surface");
    assert_eq!(
        workspaces_surface.kind,
        botster_hub_client::DaemonResponseKind::PluginSurface
    );
    let workspaces_surface = workspaces_surface
        .plugin_surface
        .expect("failure_class=web_tui_consumer missing wrapped plugin_surface payload metadata");
    assert_eq!(workspaces_surface.package_name, "botster-workspaces");
    assert_eq!(workspaces_surface.surface_id, "workspaces");
    assert_eq!(workspaces_surface.body["type"], "panel");
    assert_eq!(workspaces_surface.body["id"], "botster-workspaces-panel");

    let project_report = botster_hub_test_support::run_project_pipelines_conformance(
        &hub,
        project_pipelines_package_dir,
    )
    .expect("failure_class=plugin_producer run real Project Pipelines surface/action conformance");
    assert_eq!(project_report.package_state, "enabled");
    assert_eq!(project_report.surface_kind, "panel");
    assert_eq!(project_report.surface_id, "project-pipelines-create-panel");
    assert_eq!(project_report.invalid_action_status, "failure");
    assert_eq!(
        project_report.invalid_action_diagnostic_kind,
        "action_failure"
    );
    assert_eq!(project_report.invalid_title_error, "Title is required");

    let project_show = botster_hub_client::request(
        hub.endpoint(),
        botster_hub_client::DaemonRequest::ShowPackage {
            package_name: "project-pipelines".to_string(),
        },
    )
    .expect("failure_class=hub_contract show project-pipelines package");
    assert_eq!(
        project_show.kind,
        botster_hub_client::DaemonResponseKind::Packages
    );
    let project_package = project_show
        .packages
        .iter()
        .find(|package| package.package_name == "project-pipelines")
        .expect("failure_class=plugin_producer show response includes project-pipelines");
    let settings_route = package_route(&project_package.routes, "settings");
    assert_eq!(
        settings_route.route_path,
        "/packages/project-pipelines/settings"
    );
    assert_eq!(settings_route.target.kind, "package_settings");
    assert_eq!(settings_route.layout_mode, "settings_form");
    assert!(settings_route.supports_settings);
    assert!(
        settings_route.enabled || settings_route.blocked,
        "failure_class=web_tui_consumer settings route should open or report precise blocked state"
    );
    assert!(
        settings_route.enabled
            || settings_route
                .diagnostics
                .iter()
                .any(|diagnostic| diagnostic.kind.contains("configuration")
                    || diagnostic.kind.contains("provider")),
        "failure_class=plugin_producer settings/config route blocked without precise provider/config diagnostic: {:?}",
        settings_route.diagnostics
    );

    let project_reload = botster_hub_client::request(
        hub.endpoint(),
        botster_hub_client::DaemonRequest::ReloadPackage {
            package_name: "project-pipelines".to_string(),
        },
    )
    .expect("failure_class=stale_build reload project-pipelines package");
    assert_eq!(
        project_reload.kind,
        botster_hub_client::DaemonResponseKind::PackageDecision
    );
    assert_eq!(
        project_reload
            .package_decision
            .as_ref()
            .expect("failure_class=stale_build project-pipelines reload decision")
            .action,
        "reload"
    );

    let status = botster_hub_client::request(
        hub.endpoint(),
        botster_hub_client::DaemonRequest::Status,
    )
    .expect("failure_class=environment daemon remains responsive after first-party dogfood smoke");
    assert_eq!(status.kind, botster_hub_client::DaemonResponseKind::Status);

    hub.shutdown()
        .expect("failure_class=environment shutdown isolated first-party dogfood hub");
}

#[test]
fn cli_dogfood_launcher_starts_botster_web_in_existing_hub_mode_and_shuts_down() {
    let _guard = daemon_test_lock()
        .lock()
        .expect("serialize real daemon test");
    let data_dir = unique_test_dir("cli-dogfood-launcher");
    let web_package_dir = unique_test_dir("cli-dogfood-botster-web-package");
    write_botster_web_package(&web_package_dir);
    let web_bridge_port = unused_loopback_port();
    let (mut child, rx) =
        spawn_dogfood_launcher(Some(&data_dir), &web_package_dir, Some(web_bridge_port));
    let mut lines = collect_dogfood_ready_output(&mut child, &rx);
    lines.sort();
    let text = lines.join("\n");
    let web_url = dogfood_web_url(&lines);

    assert!(text.contains("dogfood=ready"));
    assert!(text.contains("package name=project-pipelines state=enabled"));
    assert!(text.contains("package name=botster-web state=enabled"));
    assert!(text.contains(&format!("bridge=http://127.0.0.1:{web_bridge_port}")));
    assert_eq!(
        web_url,
        format!("http://127.0.0.1:{web_bridge_port}/?dogfood=real-hub")
    );
    assert!(text.contains(&format!(
        "tui=botster-hub apps open --data-dir {} botster-tui",
        data_dir.display()
    )));
    assert!(text.contains("mcp=botster-hub mcp-serve --data-dir"));
    assert!(text.contains("status=botster-hub status --data-dir"));
    assert!(text.contains("shutdown=run botster-hub shutdown --data-dir"));
    assert!(text.contains("Ctrl-C hard-stops the foreground launcher"));
    assert!(!text.contains("local web entrypoint unavailable"));
    assert!(
        !text.contains("examples/project-pipelines"),
        "launcher output should not leak the local package source path"
    );
    assert!(
        !text.contains(web_package_dir.to_string_lossy().as_ref()),
        "launcher output should not leak the botster-web package source path"
    );

    let health = read_json_health(&format!("http://127.0.0.1:{web_bridge_port}"));
    assert_eq!(health["ok"], true);
    assert_eq!(health["mode"], "existing_hub");
    assert_eq!(health["source"], "socket");
    assert_eq!(health["socketExists"], true);
    assert_eq!(health["mixedOwnership"], false);
    let html = read_web_html(&web_url);
    assert!(html.contains("botster-web packaged UI"));

    let status = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("status")
        .arg("--data-dir")
        .arg(&data_dir)
        .output()
        .expect("run botster-hub status after dogfood readiness");
    assert!(
        status.status.success(),
        "status failed: {}",
        String::from_utf8_lossy(&status.stderr)
    );
    let stdout = String::from_utf8(status.stdout).expect("stdout is utf8");
    assert!(stdout.contains("lifecycle_state=running"));
    assert!(stdout.contains("enabled_package_count=2"));

    let packages = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("packages")
        .arg("list")
        .arg("--data-dir")
        .arg(&data_dir)
        .output()
        .expect("run botster-hub packages list after dogfood readiness");
    assert!(
        packages.status.success(),
        "packages list failed: {}",
        String::from_utf8_lossy(&packages.stderr)
    );
    let stdout = String::from_utf8(packages.stdout).expect("stdout is utf8");
    assert!(stdout.contains("package name=project-pipelines"));
    assert!(stdout.contains("package name=botster-web"));
    assert!(stdout.contains("state=enabled"));
    assert!(stdout.contains(
        "package_entrypoint package=botster-web id=web-client kind=web_app launch_mode=background command=node"
    ));
    assert!(stdout.contains("process_state=running"));
    assert!(!stdout.contains("examples/project-pipelines"));
    assert!(!stdout.contains(web_package_dir.to_string_lossy().as_ref()));

    let lifecycle = botster_hub::daemon_transport_request(
        &explicit_config(&data_dir),
        botster_hub::DaemonRequest::PluginLifecycleStatus,
    )
    .expect("daemon plugin lifecycle status after dogfood enable");
    assert!(lifecycle.lifecycle.iter().any(|plugin| {
        plugin.package_name == "project-pipelines" && plugin.state == "enabled" && plugin.loaded
    }));
    assert!(lifecycle.lifecycle.iter().any(|plugin| {
        plugin.package_name == "botster-web" && plugin.state == "enabled" && plugin.loaded
    }));

    let list = botster_hub::daemon_transport_request(
        &explicit_config(&data_dir),
        botster_hub::DaemonRequest::ListPackages,
    )
    .expect("list packages after botster-web entrypoint start");
    let web_entrypoint = list
        .packages
        .iter()
        .find(|package| package.package_name == "botster-web")
        .and_then(|package| {
            package
                .runnable_entrypoints
                .iter()
                .find(|entrypoint| entrypoint.id == "web-client")
        })
        .expect("botster-web web-client entrypoint");
    assert_eq!(web_entrypoint.process.state, "running");
    let web_pid = web_entrypoint.process.pid.expect("botster-web pid");

    shutdown_cli_daemon(&data_dir, child);
    wait_for_process_exit(web_pid);
}

#[test]
fn cli_dogfood_launcher_bridge_request_endpoint_uses_same_daemon_state() {
    let _guard = daemon_test_lock()
        .lock()
        .expect("serialize real daemon test");
    let data_dir = unique_short_test_dir("dogfood-bridge");
    let web_package_dir = unique_test_dir("cli-dogfood-bridge-botster-web-package");
    write_botster_web_package(&web_package_dir);
    let web_bridge_port = unused_loopback_port();
    let (mut child, rx) =
        spawn_dogfood_launcher(Some(&data_dir), &web_package_dir, Some(web_bridge_port));
    let mut lines = collect_dogfood_ready_output(&mut child, &rx);
    lines.sort();
    let text = lines.join("\n");
    let bridge_url = format!("http://127.0.0.1:{web_bridge_port}");
    let connection_id = "dogfood-bridge-consistency";

    assert!(text.contains("dogfood=ready"));
    assert!(text.contains("package name=project-pipelines state=enabled"));
    assert!(text.contains("package name=botster-web state=enabled"));
    assert_eq!(dogfood_data_dir(&lines), data_dir);
    assert_eq!(
        dogfood_web_url(&lines),
        format!("{bridge_url}/?dogfood=real-hub")
    );

    let status = dogfood_bridge_request(
        &bridge_url,
        Some(connection_id),
        &botster_hub_client::DaemonRequest::Status,
    );
    assert_eq!(status.kind, botster_hub_client::DaemonResponseKind::Status);
    let status_body = status.status.expect("bridge status body");
    assert_eq!(status_body.lifecycle_state, "running");
    assert!(status_body.core_initialized);
    assert_eq!(status_body.enabled_package_count, 2);

    let packages = dogfood_bridge_request(
        &bridge_url,
        Some(connection_id),
        &botster_hub_client::DaemonRequest::ListPackages,
    );
    assert_eq!(
        packages.kind,
        botster_hub_client::DaemonResponseKind::Packages
    );
    assert!(packages.packages.iter().any(|package| {
        package.package_name == "project-pipelines" && package.state == "enabled"
    }));
    let web_package = packages
        .packages
        .iter()
        .find(|package| package.package_name == "botster-web" && package.state == "enabled")
        .expect("bridge lists enabled botster-web package");
    let web_entrypoint = web_package
        .runnable_entrypoints
        .iter()
        .find(|entrypoint| entrypoint.id == "web-client")
        .expect("bridge lists botster-web web-client entrypoint");
    assert_eq!(web_entrypoint.process.state, "running");

    let empty_sessions = dogfood_bridge_request(
        &bridge_url,
        Some(connection_id),
        &botster_hub_client::DaemonRequest::ListSessions,
    );
    assert_eq!(
        empty_sessions.kind,
        botster_hub_client::DaemonResponseKind::Sessions
    );

    let spawn = dogfood_bridge_request(
        &bridge_url,
        Some(connection_id),
        &botster_hub_client::DaemonRequest::Spawn {
            session_id: "bridge-dogfood-session".to_string(),
            command: "printf 'bridge-ready\\n'; while IFS= read -r line; do printf 'bridge:%s\\n' \"$line\"; done".to_string(),
        },
    );
    assert_eq!(spawn.kind, botster_hub_client::DaemonResponseKind::Spawned);
    assert!(spawn.sessions.iter().any(|session| {
        session.session_id == "bridge-dogfood-session" && session.lifecycle == "running"
    }));

    let bridge_sessions = dogfood_bridge_request(
        &bridge_url,
        Some(connection_id),
        &botster_hub_client::DaemonRequest::ListSessions,
    );
    assert!(bridge_sessions.sessions.iter().any(|session| {
        session.session_id == "bridge-dogfood-session" && session.lifecycle == "running"
    }));
    let daemon_sessions = botster_hub::daemon_transport_request(
        &explicit_config(&data_dir),
        botster_hub::DaemonRequest::ListSessions,
    )
    .expect("direct daemon list sessions after bridge spawn");
    assert!(daemon_sessions.sessions.iter().any(|session| {
        session.session_id == "bridge-dogfood-session" && session.lifecycle == "running"
    }));

    let attach = dogfood_bridge_request(
        &bridge_url,
        Some(connection_id),
        &botster_hub_client::DaemonRequest::Attach {
            session_id: "bridge-dogfood-session".to_string(),
            subscription_id: "bridge-dogfood-subscription".to_string(),
        },
    );
    assert_eq!(attach.kind, botster_hub_client::DaemonResponseKind::Events);

    let resize = dogfood_bridge_request(
        &bridge_url,
        Some(connection_id),
        &botster_hub_client::DaemonRequest::Resize {
            session_id: "bridge-dogfood-session".to_string(),
            rows: 33,
            cols: 111,
        },
    );
    assert_eq!(resize.kind, botster_hub_client::DaemonResponseKind::Events);

    let send = dogfood_bridge_request(
        &bridge_url,
        Some(connection_id),
        &botster_hub_client::DaemonRequest::SendInput {
            session_id: "bridge-dogfood-session".to_string(),
            data: "from-bridge\n".to_string(),
        },
    );
    assert_eq!(send.kind, botster_hub_client::DaemonResponseKind::Events);

    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    let mut observed = String::new();
    while std::time::Instant::now() < deadline {
        let drain = dogfood_bridge_request(
            &bridge_url,
            Some(connection_id),
            &botster_hub_client::DaemonRequest::Drain {
                session_id: "bridge-dogfood-session".to_string(),
            },
        );
        for event in drain.events {
            if let botster_hub_client::DaemonEvent::TerminalOutput { data, .. } = event {
                observed.push_str(&data);
            }
        }
        if observed.contains("bridge:from-bridge") {
            break;
        }
        thread::sleep(Duration::from_millis(30));
    }
    assert!(
        observed.contains("bridge:from-bridge"),
        "bridge should attach and drain terminal output through the dogfood daemon, got {observed:?}"
    );

    let shutdown_session = dogfood_bridge_request(
        &bridge_url,
        Some(connection_id),
        &botster_hub_client::DaemonRequest::ShutdownSession {
            session_id: "bridge-dogfood-session".to_string(),
        },
    );
    assert_eq!(
        shutdown_session.kind,
        botster_hub_client::DaemonResponseKind::Events
    );

    let missing_session = dogfood_bridge_request(
        &bridge_url,
        Some(connection_id),
        &botster_hub_client::DaemonRequest::Drain {
            session_id: "missing-bridge-dogfood-session".to_string(),
        },
    );
    assert_eq!(
        missing_session.kind,
        botster_hub_client::DaemonResponseKind::OperatorError
    );
    let error = missing_session.error.expect("operator error body");
    assert_eq!(error.code, "unknown_session");
    assert_eq!(error.operation, "drain_runtime");
    assert!(error.message.contains("UnknownSession"), "{error:?}");
    assert!(error.diagnostics.iter().any(|diagnostic| {
        diagnostic.kind == botster_hub_client::DaemonDiagnosticKind::TerminalStreamUnavailable
            && diagnostic.operation.as_deref() == Some("drain_runtime")
            && diagnostic.feature.as_deref() == Some(botster_hub_client::FEATURE_TERMINAL_STREAMING)
            && diagnostic
                .message
                .as_deref()
                .is_some_and(|message| message.contains("UnknownSession"))
    }));
    for diagnostic in &error.diagnostics {
        let diagnostic = serde_json::to_value(diagnostic).expect("serialize diagnostic");
        assert_ne!(
            diagnostic["kind"], "runtime",
            "diagnostic kind should use bounded DaemonDiagnosticKind variants"
        );
    }

    let after_shutdown = dogfood_bridge_request(
        &bridge_url,
        Some(connection_id),
        &botster_hub_client::DaemonRequest::ListSessions,
    );
    assert!(after_shutdown.sessions.iter().any(|session| {
        session.session_id == "bridge-dogfood-session" && session.lifecycle == "exited"
    }));
    let direct_after_shutdown = botster_hub::daemon_transport_request(
        &explicit_config(&data_dir),
        botster_hub::DaemonRequest::ListSessions,
    )
    .expect("direct daemon list sessions after bridge shutdown");
    assert!(direct_after_shutdown.sessions.iter().any(|session| {
        session.session_id == "bridge-dogfood-session" && session.lifecycle == "exited"
    }));

    shutdown_cli_daemon(&data_dir, child);
}

#[test]
fn cli_dogfood_launcher_uses_generated_data_dir_and_dynamic_bridge_port() {
    let _guard = daemon_test_lock()
        .lock()
        .expect("serialize real daemon test");
    let web_package_dir = unique_test_dir("cli-dogfood-default-botster-web-package");
    write_botster_web_package(&web_package_dir);
    let (mut child, rx) = spawn_dogfood_launcher(None, &web_package_dir, None);
    let lines = collect_dogfood_ready_output(&mut child, &rx);
    let text = lines.join("\n");
    let data_dir = dogfood_data_dir(&lines);
    let web_bridge_port = dogfood_bridge_port(&lines);
    let web_url = dogfood_web_url(&lines);

    assert!(text.contains("dogfood=ready"));
    assert!(text.contains("data_dir=isolated:"));
    assert!(data_dir.is_absolute());
    assert!(data_dir.starts_with(Path::new("/tmp").join("botster-hub-dogfood")));
    assert_ne!(web_bridge_port, 0);
    assert_eq!(
        web_url,
        format!("http://127.0.0.1:{web_bridge_port}/?dogfood=real-hub")
    );

    let health = read_json_health(&format!("http://127.0.0.1:{web_bridge_port}"));
    assert_eq!(health["ok"], true);
    assert_eq!(health["mode"], "existing_hub");
    assert_eq!(health["source"], "socket");
    assert_eq!(health["port"], web_bridge_port);
    let html = read_web_html(&web_url);
    assert!(html.contains("botster-web packaged UI"));

    shutdown_cli_daemon(&data_dir, child);
}

#[test]
fn cli_dogfood_launcher_enables_local_tui_package_for_apps_open() {
    let _guard = daemon_test_lock()
        .lock()
        .expect("serialize real daemon test");
    let data_dir = unique_test_dir("cli-dogfood-tui");
    let web_package_dir = unique_test_dir("cli-dogfood-tui-botster-web-package");
    let tui_package_dir = unique_test_dir("cli-dogfood-tui-package");
    write_botster_web_package(&web_package_dir);
    write_botster_tui_package(&tui_package_dir);
    let web_bridge_port = unused_loopback_port();
    let (mut child, rx) = spawn_dogfood_launcher_with_tui(
        Some(&data_dir),
        &web_package_dir,
        Some(&tui_package_dir),
        Some(web_bridge_port),
    );
    let lines = collect_dogfood_ready_output(&mut child, &rx);
    let text = lines.join("\n");
    assert!(text.contains("dogfood=ready"));
    assert!(text.contains("package name=botster-web state=enabled"));
    assert_eq!(
        dogfood_web_url(&lines),
        format!("http://127.0.0.1:{web_bridge_port}/?dogfood=real-hub")
    );
    assert!(text.contains(&format!(
        "tui=botster-hub apps open --data-dir {} botster-tui",
        data_dir.display()
    )));
    assert!(
        !text.contains(web_package_dir.to_string_lossy().as_ref()),
        "launcher output should not leak the botster-web package source path"
    );
    assert!(
        !text.contains(tui_package_dir.to_string_lossy().as_ref()),
        "launcher output should not leak the botster-tui package source path"
    );

    let list_apps = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("apps")
        .arg("list")
        .arg("--data-dir")
        .arg(&data_dir)
        .output()
        .expect("list dogfood apps from stable data dir");
    assert!(
        list_apps.status.success(),
        "dogfood apps list failed: {}",
        command_output_text(&list_apps)
    );
    let list_apps_text = command_output_text(&list_apps);
    assert!(list_apps_text.contains("response=apps"));
    assert!(list_apps_text.contains("app package=botster-web app_id=web-client"));
    assert!(list_apps_text.contains("app package=botster-tui app_id=botster-tui"));
    assert!(list_apps_text.contains("kind=web_app"));
    assert!(list_apps_text.contains("kind=terminal_app"));
    assert!(list_apps_text.contains(&format!(
        "local_url=http://127.0.0.1:{web_bridge_port}/?dogfood=real-hub"
    )));
    assert!(!list_apps_text.contains(web_package_dir.to_string_lossy().as_ref()));
    assert!(!list_apps_text.contains(tui_package_dir.to_string_lossy().as_ref()));

    let show_web = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("apps")
        .arg("show")
        .arg("--data-dir")
        .arg(&data_dir)
        .arg("botster-web/web-client")
        .output()
        .expect("show dogfood botster-web app");
    assert!(
        show_web.status.success(),
        "dogfood apps show botster-web failed: {}",
        command_output_text(&show_web)
    );
    let show_web_text = command_output_text(&show_web);
    assert!(show_web_text.contains("response=app"));
    assert!(show_web_text.contains("package=botster-web"));
    assert!(show_web_text.contains("app_id=web-client"));
    assert!(show_web_text.contains(&format!(
        "local_url=http://127.0.0.1:{web_bridge_port}/?dogfood=real-hub"
    )));

    let open_web = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("apps")
        .arg("open")
        .arg("--data-dir")
        .arg(&data_dir)
        .arg("botster-web/web-client")
        .output()
        .expect("open dogfood botster-web app");
    assert!(
        open_web.status.success(),
        "dogfood apps open botster-web failed: {}",
        command_output_text(&open_web)
    );
    assert!(command_output_text(&open_web).contains(&format!(
        "app_url=http://127.0.0.1:{web_bridge_port}/?dogfood=real-hub"
    )));

    let open_web_alias = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("open")
        .arg("web")
        .arg("--data-dir")
        .arg(&data_dir)
        .output()
        .expect("open dogfood botster-web app through alias");
    assert!(
        open_web_alias.status.success(),
        "dogfood open web alias failed: {}",
        command_output_text(&open_web_alias)
    );
    assert!(command_output_text(&open_web_alias).contains(&format!(
        "app_url=http://127.0.0.1:{web_bridge_port}/?dogfood=real-hub"
    )));

    let open_tui = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("apps")
        .arg("open")
        .arg("--data-dir")
        .arg(&data_dir)
        .arg("botster-tui")
        .output()
        .expect("open dogfood botster-tui app");
    assert!(
        open_tui.status.success(),
        "dogfood apps open botster-tui failed: {}",
        command_output_text(&open_tui)
    );
    assert!(command_output_text(&open_tui).contains("botster-tui-fixture"));

    let open_tui_alias = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("open")
        .arg("tui")
        .arg("--data-dir")
        .arg(&data_dir)
        .output()
        .expect("open dogfood botster-tui app through alias");
    assert!(
        open_tui_alias.status.success(),
        "dogfood open tui alias failed: {}",
        command_output_text(&open_tui_alias)
    );
    assert!(command_output_text(&open_tui_alias).contains("botster-tui-fixture"));

    let removed_alias = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("tui")
        .arg("--data-dir")
        .arg(&data_dir)
        .output()
        .expect("run removed dogfood tui alias");
    assert!(
        !removed_alias.status.success(),
        "removed dogfood tui alias should fail: {}",
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
fn cli_dev_stack_bootstrap_starts_daemon_enables_first_party_packages_and_prints_apps() {
    let _guard = daemon_test_lock()
        .lock()
        .expect("serialize real daemon test");
    let data_dir = unique_test_dir("cli-dev-stack-bootstrap");
    let project_pipelines_package_dir = unique_test_dir("cli-dev-stack-project-pipelines-package");
    let web_package_dir = unique_test_dir("cli-dev-stack-web-package");
    let tui_package_dir = unique_test_dir("cli-dev-stack-tui-package");
    let workspaces_package_dir = unique_test_dir("cli-dev-stack-workspaces-package");
    write_project_pipelines_availability_package(&project_pipelines_package_dir);
    write_botster_web_package(&web_package_dir);
    write_botster_tui_package(&tui_package_dir);
    write_botster_workspaces_local_package(&workspaces_package_dir, "botster-workspaces");
    let web_bridge_port = unused_loopback_port();

    let output = run_dev_stack_bootstrap(
        &data_dir,
        &project_pipelines_package_dir,
        &web_package_dir,
        &tui_package_dir,
        &workspaces_package_dir,
        web_bridge_port,
    );
    assert!(
        output.status.success(),
        "dev-stack bootstrap failed: {}",
        command_output_text(&output)
    );
    let text = command_output_text(&output);
    assert!(text.contains("dev_stack=ready"));
    assert!(text.contains("daemon=started"));
    assert!(text.contains("package name=project-pipelines state=enabled"));
    assert!(text.contains("package name=botster-web state=enabled"));
    assert!(text.contains("package name=botster-tui state=enabled"));
    assert!(text.contains("package name=botster-workspaces state=enabled"));
    assert!(text.contains(&format!(
        "web=http://127.0.0.1:{web_bridge_port}/?dogfood=real-hub"
    )));
    assert!(text.contains(&format!(
        "tui=botster-hub apps open --data-dir {} botster-tui",
        data_dir.display()
    )));
    assert!(text.contains(&format!(
        "apps=botster-hub apps list --data-dir {}",
        data_dir.display()
    )));
    for package_dir in [
        &project_pipelines_package_dir,
        &web_package_dir,
        &tui_package_dir,
        &workspaces_package_dir,
    ] {
        assert!(
            !text.contains(package_dir.to_string_lossy().as_ref()),
            "dev-stack output should not leak package source path {package_dir:?}: {text}"
        );
    }

    let apps = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("apps")
        .arg("list")
        .arg("--data-dir")
        .arg(&data_dir)
        .output()
        .expect("list dev-stack apps");
    assert!(
        apps.status.success(),
        "apps list failed: {}",
        command_output_text(&apps)
    );
    let apps_text = command_output_text(&apps);
    assert!(apps_text.contains("app package=botster-web app_id=web-client"));
    assert!(apps_text.contains("app package=botster-tui app_id=botster-tui"));
    assert!(apps_text.contains(&format!(
        "local_url=http://127.0.0.1:{web_bridge_port}/?dogfood=real-hub"
    )));

    shutdown_dev_stack_daemon(&data_dir);
}

#[test]
fn cli_dev_stack_project_pipelines_configuration_schema_acceptance_target() {
    let _guard = daemon_test_lock()
        .lock()
        .expect("serialize real daemon test");
    let data_dir = unique_short_test_dir("cli-dev-stack-project-pipelines-config");
    let project_pipelines_package_dir = std::env::current_dir()
        .expect("current dir")
        .join("examples")
        .join("project-pipelines");
    let web_package_dir = unique_test_dir("cli-dev-stack-config-web");
    let tui_package_dir = unique_test_dir("cli-dev-stack-config-tui");
    let workspaces_package_dir = unique_test_dir("cli-dev-stack-config-workspaces");
    write_botster_web_package(&web_package_dir);
    write_botster_tui_package(&tui_package_dir);
    write_botster_workspaces_local_package(&workspaces_package_dir, "botster-workspaces");

    let output = run_dev_stack_bootstrap(
        &data_dir,
        &project_pipelines_package_dir,
        &web_package_dir,
        &tui_package_dir,
        &workspaces_package_dir,
        unused_loopback_port(),
    );
    assert!(
        output.status.success(),
        "dev-stack bootstrap failed: {}",
        command_output_text(&output)
    );
    let config = explicit_config(&data_dir);
    let list =
        botster_hub::daemon_transport_request(&config, botster_hub::DaemonRequest::ListPackages)
            .expect("list dev-stack packages");
    let project_pipelines = list
        .packages
        .iter()
        .find(|package| package.package_name == "project-pipelines")
        .expect("project-pipelines package row");
    assert_eq!(project_pipelines.state, "enabled");
    let schema = project_pipelines
        .configuration
        .schema
        .as_ref()
        .expect("project-pipelines configuration schema");
    let fields = schema["fields"].as_array().expect("schema fields");
    assert!(fields.iter().any(|field| {
        field["key"] == "operator_endpoint"
            && field["type"] == "url"
            && field["default"]["value"] == "https://example.invalid/project-pipelines"
    }));
    assert!(fields.iter().any(|field| {
        field["key"] == "pipeline_mode"
            && field["type"] == "select"
            && field["default"]["value"] == "local"
    }));
    assert_eq!(
        project_pipelines.configuration.effective_values["operator_endpoint"],
        serde_json::json!({"type":"url","value":"https://example.invalid/project-pipelines"})
    );
    assert_eq!(
        project_pipelines.configuration.effective_values["pipeline_mode"],
        serde_json::json!({"type":"select","value":"local"})
    );
    assert!(project_pipelines.configuration.missing_required.is_empty());

    let invalid = botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::SetPackageConfiguration {
            package_name: "project-pipelines".to_string(),
            values: BTreeMap::from([(
                "pipeline_mode".to_string(),
                serde_json::json!({"type":"select","value":"sideways"}),
            )]),
        },
    )
    .expect("invalid project-pipelines config returns operator error");
    assert_eq!(invalid.kind, botster_hub::DaemonResponseKind::OperatorError);
    assert!(
        invalid.diagnostics.iter().any(|diagnostic| {
            diagnostic.kind == botster_hub_client::DaemonDiagnosticKind::ActionFailure
                && diagnostic.operation.as_deref() == Some("configure")
                && diagnostic.feature.as_deref() == Some("package_registry")
                && diagnostic
                    .message
                    .as_deref()
                    .is_some_and(|message| message.contains("sideways"))
        }),
        "expected package configuration diagnostic, got {:?}",
        invalid.diagnostics
    );

    let saved = botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::SetPackageConfiguration {
            package_name: "project-pipelines".to_string(),
            values: BTreeMap::from([
                (
                    "operator_endpoint".to_string(),
                    serde_json::json!({"type":"url","value":"https://example.invalid/project-pipelines/acceptance"}),
                ),
                (
                    "pipeline_mode".to_string(),
                    serde_json::json!({"type":"select","value":"github"}),
                ),
            ]),
        },
    )
    .expect("set project-pipelines config through daemon");
    let saved_package = saved
        .packages
        .iter()
        .find(|package| package.package_name == "project-pipelines")
        .expect("saved project-pipelines package row");
    assert_eq!(
        saved_package.configuration.effective_values["operator_endpoint"],
        serde_json::json!({"type":"url","value":"https://example.invalid/project-pipelines/acceptance"})
    );
    assert_eq!(
        saved_package.configuration.effective_values["pipeline_mode"],
        serde_json::json!({"type":"select","value":"github"})
    );

    shutdown_dev_stack_daemon(&data_dir);

    let restarted = start_cli_daemon(&data_dir);
    let reloaded =
        botster_hub::daemon_transport_request(&config, botster_hub::DaemonRequest::ListPackages)
            .expect("list project-pipelines after restart");
    let reloaded_package = reloaded
        .packages
        .iter()
        .find(|package| package.package_name == "project-pipelines")
        .expect("reloaded project-pipelines package row");
    assert_eq!(
        reloaded_package.configuration.effective_values["operator_endpoint"],
        serde_json::json!({"type":"url","value":"https://example.invalid/project-pipelines/acceptance"})
    );
    assert_eq!(
        reloaded_package.configuration.effective_values["pipeline_mode"],
        serde_json::json!({"type":"select","value":"github"})
    );
    shutdown_cli_daemon(&data_dir, restarted);
}

#[test]
fn cli_dev_stack_bootstrap_reuses_live_daemon_and_preserves_state_after_restart() {
    let _guard = daemon_test_lock()
        .lock()
        .expect("serialize real daemon test");
    let data_dir = unique_test_dir("cli-dev-stack-rerun");
    let project_pipelines_package_dir = unique_test_dir("cli-dev-stack-rerun-project-pipelines");
    let web_package_dir = unique_test_dir("cli-dev-stack-rerun-web");
    let tui_package_dir = unique_test_dir("cli-dev-stack-rerun-tui");
    let workspaces_package_dir = unique_test_dir("cli-dev-stack-rerun-workspaces");
    write_project_pipelines_availability_package(&project_pipelines_package_dir);
    write_botster_web_package(&web_package_dir);
    write_botster_tui_package(&tui_package_dir);
    write_botster_workspaces_local_package(&workspaces_package_dir, "botster-workspaces");

    let first_port = unused_loopback_port();
    let first = run_dev_stack_bootstrap(
        &data_dir,
        &project_pipelines_package_dir,
        &web_package_dir,
        &tui_package_dir,
        &workspaces_package_dir,
        first_port,
    );
    assert!(
        first.status.success(),
        "first dev-stack bootstrap failed: {}",
        command_output_text(&first)
    );

    let second = run_dev_stack_bootstrap(
        &data_dir,
        &project_pipelines_package_dir,
        &web_package_dir,
        &tui_package_dir,
        &workspaces_package_dir,
        first_port,
    );
    assert!(
        second.status.success(),
        "second live dev-stack bootstrap failed: {}",
        command_output_text(&second)
    );
    let second_text = command_output_text(&second);
    assert!(second_text.contains("daemon=reused"));
    assert_eq!(
        enabled_package_names(&data_dir),
        vec![
            "botster-tui".to_string(),
            "botster-web".to_string(),
            "botster-workspaces".to_string(),
            "project-pipelines".to_string(),
        ]
    );

    shutdown_dev_stack_daemon(&data_dir);

    let third_port = unused_loopback_port();
    let third = run_dev_stack_bootstrap(
        &data_dir,
        &project_pipelines_package_dir,
        &web_package_dir,
        &tui_package_dir,
        &workspaces_package_dir,
        third_port,
    );
    assert!(
        third.status.success(),
        "post-shutdown dev-stack bootstrap failed: {}",
        command_output_text(&third)
    );
    let third_text = command_output_text(&third);
    assert!(third_text.contains("daemon=started"));
    assert!(third_text.contains(&format!(
        "web=http://127.0.0.1:{third_port}/?dogfood=real-hub"
    )));
    assert_eq!(
        enabled_package_names(&data_dir),
        vec![
            "botster-tui".to_string(),
            "botster-web".to_string(),
            "botster-workspaces".to_string(),
            "project-pipelines".to_string(),
        ]
    );

    shutdown_dev_stack_daemon(&data_dir);
}

#[test]
fn cli_doctor_reports_stopped_runtime_with_remediation() {
    let _guard = daemon_test_lock()
        .lock()
        .expect("serialize real daemon test");
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
    let _guard = daemon_test_lock()
        .lock()
        .expect("serialize real daemon test");
    let data_dir = unique_test_dir("cli-local-runtime-up");
    let project_pipelines_package_dir = unique_test_dir("cli-up-project-pipelines");
    let web_package_dir = unique_test_dir("cli-up-web");
    let tui_package_dir = unique_test_dir("cli-up-tui");
    let workspaces_package_dir = unique_test_dir("cli-up-workspaces");
    write_project_pipelines_availability_package(&project_pipelines_package_dir);
    write_botster_web_package(&web_package_dir);
    write_botster_tui_package(&tui_package_dir);
    write_botster_workspaces_local_package(&workspaces_package_dir, "botster-workspaces");

    let web_bridge_port = unused_loopback_port();
    let first = run_local_runtime_up(
        &data_dir,
        &project_pipelines_package_dir,
        &web_package_dir,
        &tui_package_dir,
        &workspaces_package_dir,
        web_bridge_port,
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
    assert!(first_text.contains("protocol_version=1"));
    assert!(first_text.contains("conformance_fixture_revision="));
    assert!(first_text.contains("package_count=4"));
    assert!(first_text.contains("enabled_package_count=4"));
    assert!(first_text.contains("app_count="));
    assert!(first_text.contains("app package=botster-web app_id=web-client"));
    assert!(first_text.contains(&format!(
        "web=http://127.0.0.1:{web_bridge_port}/?dogfood=real-hub"
    )));
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

    let second = run_local_runtime_up(
        &data_dir,
        &project_pipelines_package_dir,
        &web_package_dir,
        &tui_package_dir,
        &workspaces_package_dir,
        web_bridge_port,
    );
    assert!(
        second.status.success(),
        "second up failed: {}",
        command_output_text(&second)
    );
    assert!(command_output_text(&second).contains("daemon=reused"));

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

    let status = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("status")
        .arg("--data-dir")
        .arg(&data_dir)
        .output()
        .expect("run botster-hub status after down");
    assert!(
        !status.status.success(),
        "status should fail after down: {}",
        command_output_text(&status)
    );
}

#[test]
fn cli_doctor_reports_healthy_runtime_checks() {
    let _guard = daemon_test_lock()
        .lock()
        .expect("serialize real daemon test");
    let data_dir = unique_test_dir("cli-doctor-healthy");
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

    shutdown_dev_stack_daemon(&data_dir);
}

#[test]
fn cli_local_runtime_up_recovers_owned_incompatible_daemon() {
    let _guard = daemon_test_lock()
        .lock()
        .expect("serialize real daemon test");
    let data_dir = unique_test_dir("cli-up-owned-incompat");
    let project_pipelines_package_dir = unique_test_dir("cli-up-owned-project-pipelines");
    let web_package_dir = unique_test_dir("cli-up-owned-web");
    let tui_package_dir = unique_test_dir("cli-up-owned-tui");
    let workspaces_package_dir = unique_test_dir("cli-up-owned-workspaces");
    write_project_pipelines_availability_package(&project_pipelines_package_dir);
    write_botster_web_package(&web_package_dir);
    write_botster_tui_package(&tui_package_dir);
    write_botster_workspaces_local_package(&workspaces_package_dir, "botster-workspaces");
    let stale_child = start_owned_incompatible_dev_stack_daemon(&data_dir);
    let stale_pid = stale_child.id();

    let output = run_local_runtime_up(
        &data_dir,
        &project_pipelines_package_dir,
        &web_package_dir,
        &tui_package_dir,
        &workspaces_package_dir,
        unused_loopback_port(),
    );
    assert!(
        output.status.success(),
        "up failed after stale daemon recovery: {}",
        command_output_text(&output)
    );
    let text = command_output_text(&output);
    assert!(text.contains("runtime=ready"));
    assert!(text.contains("daemon=started"));
    let _ = stale_child.wait_with_output().expect("reap stale daemon");
    assert!(
        !process_exists(stale_pid),
        "stale incompatible daemon should be stopped"
    );

    shutdown_dev_stack_daemon(&data_dir);
}

#[test]
fn cli_local_runtime_down_recovers_owned_incompatible_daemon() {
    let _guard = daemon_test_lock()
        .lock()
        .expect("serialize real daemon test");
    let data_dir = unique_short_test_dir("cli-down-owned-incompat");
    let stale_child = start_owned_incompatible_dev_stack_daemon(&data_dir);
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
    let _guard = daemon_test_lock()
        .lock()
        .expect("serialize real daemon test");
    let data_dir = unique_short_test_dir("cli-scoped-owned-incompat");
    let other_data_dir = unique_short_test_dir("cli-scoped-other-incompat");
    let stale_child = start_owned_incompatible_dev_stack_daemon(&data_dir);
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
    let _guard = daemon_test_lock()
        .lock()
        .expect("serialize real daemon test");
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
    assert!(text.contains("botster-hub up --data-dir <path>"));
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
    let _guard = daemon_test_lock()
        .lock()
        .expect("serialize real daemon test");
    let data_dir = unique_short_test_dir("cli-forged-pid-incompat");
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
    write_dev_stack_daemon_metadata(&data_dir, decoy.id());

    let output = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("up")
        .arg("--data-dir")
        .arg(&data_dir)
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
        .arg("down")
        .arg("--data-dir")
        .arg(&data_dir)
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
    let _guard = daemon_test_lock()
        .lock()
        .expect("serialize real daemon test");
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
    let _guard = daemon_test_lock()
        .lock()
        .expect("serialize real daemon test");
    let data_dir = unique_test_dir("cli-smoke-success");
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
    assert!(
        output.status.success(),
        "smoke failed: {}",
        command_output_text(&output)
    );
    let text = command_output_text(&output);
    assert!(text.contains("smoke=local_runtime"));
    assert!(text.contains("check name=daemon status=pass"));
    assert!(text.contains("check name=core status=pass"));
    assert!(text.contains("check name=packages status=pass"));
    assert!(text.contains("check name=apps status=pass"));
    assert!(text.contains("check name=session_terminal status=pass"));
    assert!(text.contains("check name=webrtc status=pass"));
    assert!(text.contains("smoke_result=pass"));

    let status = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("status")
        .arg("--data-dir")
        .arg(&data_dir)
        .output()
        .expect("run botster-hub status after smoke-owned daemon cleanup");
    assert!(
        !status.status.success(),
        "smoke should stop the daemon it started: {}",
        command_output_text(&status)
    );
}

#[test]
fn cli_smoke_reports_missing_first_party_prerequisites() {
    let _guard = daemon_test_lock()
        .lock()
        .expect("serialize real daemon test");
    let data_dir = unique_short_test_dir("cli-smoke-missing");

    let output = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("smoke")
        .arg("--data-dir")
        .arg(&data_dir)
        .arg("--project-pipelines-package-path")
        .arg(data_dir.join("missing-project-pipelines"))
        .output()
        .expect("run botster-hub smoke without package prerequisites");
    assert!(
        !output.status.success(),
        "smoke unexpectedly succeeded: {}",
        command_output_text(&output)
    );
    let text = command_output_text(&output);
    assert!(text.contains("smoke=local_runtime"));
    assert!(text.contains("missing_prerequisite=project-pipelines"));
}

#[test]
fn cli_dev_stack_acceptance_smoke_exercises_first_party_plugins_project_pipelines_session_templates_reload_and_shutdown()
 {
    let _guard = daemon_test_lock()
        .lock()
        .expect("serialize real daemon test");
    let data_dir = unique_test_dir("cli-dev-stack-acceptance");
    let project_pipelines_package_dir = std::env::current_dir()
        .expect("current dir")
        .join("examples/project-pipelines");
    let web_package_dir = unique_test_dir("cli-dev-stack-acceptance-web");
    let tui_package_dir = unique_test_dir("cli-dev-stack-acceptance-tui");
    let workspaces_package_dir = unique_test_dir("cli-dev-stack-acceptance-workspaces");
    write_botster_web_package(&web_package_dir);
    write_botster_tui_package(&tui_package_dir);
    write_botster_workspaces_local_package(&workspaces_package_dir, "botster-workspaces");
    let web_bridge_port = unused_loopback_port();

    let bootstrap = run_dev_stack_bootstrap(
        &data_dir,
        &project_pipelines_package_dir,
        &web_package_dir,
        &tui_package_dir,
        &workspaces_package_dir,
        web_bridge_port,
    );
    assert!(
        bootstrap.status.success(),
        "dev-stack bootstrap failed: {}",
        command_output_text(&bootstrap)
    );
    let bootstrap_text = command_output_text(&bootstrap);
    assert!(bootstrap_text.contains("dev_stack=ready"));
    assert!(bootstrap_text.contains("package name=project-pipelines state=enabled"));
    assert!(bootstrap_text.contains("package name=botster-web state=enabled"));
    assert!(bootstrap_text.contains("package name=botster-tui state=enabled"));
    assert!(bootstrap_text.contains("package name=botster-workspaces state=enabled"));

    let config = explicit_config(&data_dir);
    let apps = botster_hub::daemon_transport_request(&config, botster_hub::DaemonRequest::ListApps)
        .expect("list first-party dev-stack apps");
    let web_app = app_row(&apps, "web-client");
    assert_eq!(web_app.package_name, "botster-web");
    assert_eq!(
        web_app.launch_target.local_url.as_deref(),
        Some(format!("http://127.0.0.1:{web_bridge_port}/?dogfood=real-hub").as_str())
    );
    let tui_app = app_row(&apps, "botster-tui");
    assert_eq!(tui_app.package_name, "botster-tui");
    assert_eq!(tui_app.kind, "terminal_app");

    let web_open = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("apps")
        .arg("open")
        .arg("--data-dir")
        .arg(&data_dir)
        .arg("botster-web/web-client")
        .output()
        .expect("open botster-web app descriptor");
    assert!(
        web_open.status.success(),
        "apps open botster-web failed: {}",
        command_output_text(&web_open)
    );
    assert!(command_output_text(&web_open).contains(&format!(
        "app_url=http://127.0.0.1:{web_bridge_port}/?dogfood=real-hub"
    )));
    read_web_html(&format!(
        "http://127.0.0.1:{web_bridge_port}/?dogfood=real-hub"
    ));

    let tui_open = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("apps")
        .arg("open")
        .arg("--data-dir")
        .arg(&data_dir)
        .arg("botster-tui")
        .output()
        .expect("open botster-tui launch descriptor");
    assert!(
        tui_open.status.success(),
        "apps open botster-tui failed: {}",
        command_output_text(&tui_open)
    );
    assert!(command_output_text(&tui_open).contains("botster-tui-fixture"));

    let workspace_create = botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::PluginMcpCallTool {
            name: "botster_workspaces.create".to_string(),
            arguments: serde_json::json!({
                "workspace_id": "workspace-acceptance-1",
                "name": "Acceptance Workspace"
            }),
        },
    )
    .expect("create workspace through botster-workspaces MCP tool");
    assert_eq!(
        workspace_create.kind,
        botster_hub::DaemonResponseKind::PluginMcpToolResult
    );
    assert_eq!(
        workspace_create.plugin_tool_result["workspace"]["status"],
        "created"
    );
    let workspace_use = botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::PluginMcpCallTool {
            name: "botster_workspaces.use".to_string(),
            arguments: serde_json::json!({ "workspace_id": "workspace-acceptance-1" }),
        },
    )
    .expect("use workspace through botster-workspaces MCP tool");
    assert_eq!(
        workspace_use.plugin_tool_result["workspace"]["status"],
        "used"
    );

    let tools = botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::PluginMcpListTools,
    )
    .expect("list plugin MCP tools");
    let tool_names = tools
        .plugin_tools
        .iter()
        .filter_map(|tool| tool.get("name").and_then(serde_json::Value::as_str))
        .collect::<Vec<_>>();
    assert!(tool_names.contains(&"project_pipelines.create"));
    assert!(tool_names.contains(&"project_pipelines.start"));
    assert!(tool_names.contains(&"botster_workspaces.create"));

    let created_ticket = botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::PluginMcpCallTool {
            name: "project_pipelines.create".to_string(),
            arguments: serde_json::json!({
                "title": "Acceptance smoke",
                "pipeline_id": "local_pipeline"
            }),
        },
    )
    .expect("create project-pipelines ticket through live plugin MCP");
    assert_eq!(created_ticket.plugin_tool_result["ok"], true);
    let ticket_id = created_ticket.plugin_tool_result["ticket"]["id"]
        .as_str()
        .expect("created ticket id")
        .to_string();
    let start_run = botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::PluginMcpCallTool {
            name: "project_pipelines.start".to_string(),
            arguments: serde_json::json!({
                "ticket_id": ticket_id,
                "target_id": "target-acceptance",
                "worktree": "acceptance-worktree",
                "agent_name": "codex"
            }),
        },
    )
    .expect("start project-pipelines run through live plugin MCP");
    assert_eq!(start_run.plugin_tool_result["ok"], true);
    let run = &start_run.plugin_tool_result["run"];
    assert_eq!(run["coordination"]["target_id"], "target-acceptance");
    assert_eq!(
        run["coordination"]["assigned_worktree"],
        "acceptance-worktree"
    );
    assert_eq!(
        run["coordination"]["session_template_id"],
        "project-pipelines/agent-step"
    );
    assert_eq!(run["coordination"]["session_lifecycle"], "running");
    let session_id = run["coordination"]["session_uuid"]
        .as_str()
        .expect("project-pipelines session id")
        .to_string();

    let mut connection =
        botster_hub::DaemonConnection::connect(&config).expect("connect daemon socket");
    let attach = connection
        .request(&botster_hub::DaemonRequest::Attach {
            session_id: session_id.clone(),
            subscription_id: "project-pipelines-acceptance-subscription".to_string(),
        })
        .expect("attach project-pipelines spawned session");
    assert_eq!(attach.kind, botster_hub::DaemonResponseKind::Events);
    let send = connection
        .request(&botster_hub::DaemonRequest::SendInput {
            session_id: session_id.clone(),
            data: "acceptance-input\n".to_string(),
        })
        .expect("send input to project-pipelines spawned session");
    assert_eq!(send.kind, botster_hub::DaemonResponseKind::Events);
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    let mut observed = String::new();
    while std::time::Instant::now() < deadline {
        let drain = connection
            .request(&botster_hub::DaemonRequest::Drain {
                session_id: session_id.clone(),
            })
            .expect("drain project-pipelines spawned session");
        for event in drain.events {
            match event {
                botster_hub::DaemonEvent::TerminalOutput { data, .. }
                | botster_hub::DaemonEvent::Snapshot { data, .. }
                | botster_hub::DaemonEvent::Scrollback { data, .. } => observed.push_str(&data),
                _ => {}
            }
        }
        if observed.contains("project-pipelines-step:acceptance-input") {
            break;
        }
        thread::sleep(Duration::from_millis(30));
    }
    assert!(
        observed.contains("project-pipelines-step:acceptance-input"),
        "project-pipelines spawned PTY should echo input through attach/drain, got {observed:?}"
    );

    let mut web_manifest: serde_json::Value = serde_json::from_str(
        &fs::read_to_string(web_package_dir.join("botster-package.json"))
            .expect("read botster-web manifest"),
    )
    .expect("parse botster-web manifest");
    web_manifest["version"] = serde_json::Value::String("1.1.0".to_string());
    fs::write(
        web_package_dir.join("botster-package.json"),
        serde_json::to_string_pretty(&web_manifest)
            .expect("serialize updated botster-web manifest"),
    )
    .expect("write updated botster-web manifest");
    let reload = botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::ReloadPackage {
            package_name: "botster-web".to_string(),
        },
    )
    .expect("reload updated botster-web package");
    assert_eq!(
        reload.package_decision.expect("reload decision").action,
        "reload"
    );
    let reloaded_web = reload
        .packages
        .iter()
        .find(|package| package.package_name == "botster-web")
        .expect("reloaded botster-web package row");
    assert_eq!(reloaded_web.version, "1.1.0");

    let shutdown_session = connection
        .request(&botster_hub::DaemonRequest::ShutdownSession { session_id })
        .expect("shutdown project-pipelines spawned session");
    assert!(
        matches!(
            shutdown_session.kind,
            botster_hub::DaemonResponseKind::Events
                | botster_hub::DaemonResponseKind::SessionCleanup
        ),
        "unexpected session shutdown response: {:?}",
        shutdown_session.kind
    );
    drop(connection);
    shutdown_dev_stack_daemon(&data_dir);
    let status = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("status")
        .arg("--data-dir")
        .arg(&data_dir)
        .output()
        .expect("check dev-stack status after shutdown");
    assert!(
        !status.status.success(),
        "dev-stack daemon should be stopped after shutdown: {}",
        command_output_text(&status)
    );
}

#[test]
fn cli_dogfood_launcher_reruns_against_existing_explicit_data_dir() {
    let _guard = daemon_test_lock()
        .lock()
        .expect("serialize real daemon test");
    let data_dir = unique_test_dir("cli-dogfood-rerun");
    let web_package_dir = unique_test_dir("cli-dogfood-rerun-botster-web-package");
    write_botster_web_package(&web_package_dir);

    let first_port = unused_loopback_port();
    let (mut first_child, first_rx) =
        spawn_dogfood_launcher(Some(&data_dir), &web_package_dir, Some(first_port));
    let first_lines = collect_dogfood_ready_output(&mut first_child, &first_rx);
    assert_eq!(dogfood_bridge_port(&first_lines), first_port);
    assert_eq!(
        dogfood_web_url(&first_lines),
        format!("http://127.0.0.1:{first_port}/?dogfood=real-hub")
    );
    shutdown_cli_daemon(&data_dir, first_child);

    let second_port = unused_loopback_port();
    let (mut second_child, second_rx) =
        spawn_dogfood_launcher(Some(&data_dir), &web_package_dir, Some(second_port));
    let second_lines = collect_dogfood_ready_output(&mut second_child, &second_rx);
    let second_text = second_lines.join("\n");

    assert_eq!(dogfood_bridge_port(&second_lines), second_port);
    assert_eq!(
        dogfood_web_url(&second_lines),
        format!("http://127.0.0.1:{second_port}/?dogfood=real-hub")
    );
    assert!(second_text.contains("package name=project-pipelines state=enabled"));
    assert!(second_text.contains("package name=botster-web state=enabled"));
    let health = read_json_health(&format!("http://127.0.0.1:{second_port}"));
    assert_eq!(health["ok"], true);
    assert_eq!(health["source"], "socket");
    let html = read_web_html(&dogfood_web_url(&second_lines));
    assert!(html.contains("botster-web packaged UI"));

    shutdown_cli_daemon(&data_dir, second_child);
}

#[test]
fn cli_dogfood_launcher_reports_failed_web_entrypoint_diagnostics() {
    let _guard = daemon_test_lock()
        .lock()
        .expect("serialize real daemon test");
    let data_dir = unique_test_dir("dogfood-web-fail");
    let web_package_dir = unique_test_dir("dogfood-web-fail-package");
    write_failing_botster_web_package(&web_package_dir);
    let web_bridge_port = unused_loopback_port();

    let mut command = Command::new(env!("CARGO_BIN_EXE_botster-hub"));
    command
        .arg("dogfood")
        .arg("--data-dir")
        .arg(&data_dir)
        .arg("--web-package-path")
        .arg(&web_package_dir)
        .arg("--web-bridge-port")
        .arg(web_bridge_port.to_string());
    let output = run_command_with_timeout(command, Duration::from_secs(30));

    assert!(!output.status.success());
    let text = command_output_text(&output);
    assert!(
        text.contains("start botster-web web-client entrypoint"),
        "{text}"
    );
    assert!(text.contains("process state failed"), "{text}");
    assert!(
        text.contains("stderr: bridge bind failed: fixture"),
        "{text}"
    );
    assert!(
        !text.contains("verify botster-web existing-hub health"),
        "{text}"
    );
    assert!(
        !text.contains(
            std::env::current_dir()
                .expect("current dir")
                .to_string_lossy()
                .as_ref()
        )
    );
    assert!(!text.contains(web_package_dir.to_string_lossy().as_ref()));
}

#[test]
fn cli_dogfood_launcher_rejects_health_only_web_entrypoint() {
    let _guard = daemon_test_lock()
        .lock()
        .expect("serialize real daemon test");
    let data_dir = unique_test_dir("dogfood-web-health-only");
    let web_package_dir = unique_test_dir("dogfood-web-health-only-package");
    write_health_only_botster_web_package(&web_package_dir);
    let web_bridge_port = unused_loopback_port();

    let mut command = Command::new(env!("CARGO_BIN_EXE_botster-hub"));
    command
        .arg("dogfood")
        .arg("--data-dir")
        .arg(&data_dir)
        .arg("--web-package-path")
        .arg(&web_package_dir)
        .arg("--web-bridge-port")
        .arg(web_bridge_port.to_string());
    let output = run_command_with_timeout(command, Duration::from_secs(30));

    assert!(!output.status.success());
    let text = command_output_text(&output);
    assert!(text.contains("verify botster-web packaged UI"), "{text}");
    assert!(
        text.contains("botster-web UI returned non-200 status"),
        "{text}"
    );
    assert!(
        !text.contains("verify botster-web existing-hub health"),
        "{text}"
    );
    assert!(!text.contains("dogfood=ready"), "{text}");
    assert!(
        !text.contains(
            std::env::current_dir()
                .expect("current dir")
                .to_string_lossy()
                .as_ref()
        )
    );
    assert!(!text.contains(web_package_dir.to_string_lossy().as_ref()));
}

#[test]
fn cli_dogfood_launcher_reports_missing_session_worker_without_mutating_state() {
    let data_dir = unique_test_dir("cli-dogfood-missing-worker");
    let missing_worker = data_dir.join("missing-botster-session-worker");

    let output = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("dogfood")
        .arg("--data-dir")
        .arg(&data_dir)
        .arg("--session-worker-bin")
        .arg(&missing_worker)
        .output()
        .expect("run botster-hub dogfood with missing worker");

    assert!(!output.status.success());
    let text = command_output_text(&output);
    assert!(text.contains("missing botster-session-worker binary"));
    assert!(text.contains("--session-worker-bin <path>"));
    assert!(
        !data_dir.join("hub-state.json").exists(),
        "missing worker should fail before package or hub state mutation"
    );
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
    assert_eq!(status.schema_version, 1);
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
    assert_eq!(reopened.schema_version, 1);
    assert_eq!(reopened.host.id, "hub-daemon-test");
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
    assert_eq!(status.schema_version, 1);

    daemon.stop();
    let reopened = store
        .load_or_initialize(&config)
        .expect("reload existing state after stop");
    assert_eq!(reopened.package_registry.records.len(), 1);
    assert!(reopened.package_registry.records[0].is_enabled());
}

#[test]
fn cli_start_requires_explicit_data_dir_and_prints_scrubbed_lifecycle_status() {
    let _guard = daemon_test_lock()
        .lock()
        .expect("serialize real daemon test");
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
    assert!(stdout.contains("schema_version=1"));
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
    let _guard = daemon_test_lock()
        .lock()
        .expect("serialize real daemon test");
    let data_dir = unique_test_dir("cli-sessions");
    let child = start_cli_daemon(&data_dir);
    let spawn = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("sessions")
        .arg("spawn")
        .arg("--data-dir")
        .arg(&data_dir)
        .arg("--session-id")
        .arg("dogfood-session")
        .arg("--")
        .arg("printf 'dogfood-ok\\n'; IFS= read -r line; printf 'dogfood:%s\\n' \"$line\"")
        .output()
        .expect("run botster-hub sessions spawn");

    assert!(
        spawn.status.success(),
        "spawn failed: {}",
        String::from_utf8_lossy(&spawn.stderr)
    );
    let stdout = String::from_utf8(spawn.stdout).expect("stdout is utf8");
    assert!(stdout.contains("response=spawned"));
    assert!(stdout.contains("session_id=dogfood-session"));
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
    assert!(stdout.contains("session id=dogfood-session lifecycle=running"));
    assert!(!stdout.contains(data_dir.to_string_lossy().as_ref()));

    let resize = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("sessions")
        .arg("resize")
        .arg("--data-dir")
        .arg(&data_dir)
        .arg("dogfood-session")
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
            session_id: "dogfood-session".to_string(),
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
        .arg("dogfood-session")
        .output()
        .expect("run botster-hub sessions detach");
    assert!(
        detach.status.success(),
        "detach failed: {}",
        String::from_utf8_lossy(&detach.stderr)
    );

    let send = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("sessions")
        .arg("send-input")
        .arg("--data-dir")
        .arg(&data_dir)
        .arg("dogfood-session")
        .arg("--")
        .arg("from-cli\r")
        .output()
        .expect("run botster-hub sessions send-input");
    assert!(
        send.status.success(),
        "send-input failed: {}",
        String::from_utf8_lossy(&send.stderr)
    );

    let attach = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("sessions")
        .arg("attach")
        .arg("--data-dir")
        .arg(&data_dir)
        .arg("dogfood-session")
        .output()
        .expect("run botster-hub sessions attach");
    assert!(
        attach.status.success(),
        "attach failed: {}",
        String::from_utf8_lossy(&attach.stderr)
    );
    let stdout = String::from_utf8(attach.stdout).expect("attach stdout is utf8");
    assert!(stdout.contains("dogfood-ok"));

    shutdown_cli_daemon(&data_dir, child);
}

#[test]
fn cli_short_lived_session_shutdown_returns_structured_cleanup() {
    let _guard = daemon_test_lock()
        .lock()
        .expect("serialize real daemon test");
    let data_dir = unique_test_dir("cli-short-lived-shutdown");
    let child = start_cli_daemon(&data_dir);

    let spawn = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("sessions")
        .arg("spawn")
        .arg("--data-dir")
        .arg(&data_dir)
        .arg("--session-id")
        .arg("dogfood-session")
        .arg("--")
        .arg("printf 'dogfood-ok\\n'; IFS= read -r line; printf 'dogfood:%s\\n' \"$line\"")
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
        .arg("dogfood-session")
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
        .arg("dogfood-session")
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
    assert!(attach_stdout.contains("dogfood-ok"));
    assert!(attach_stdout.contains("dogfood:done"));

    let shutdown = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("sessions")
        .arg("shutdown")
        .arg("--data-dir")
        .arg(&data_dir)
        .arg("dogfood-session")
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
    assert!(stdout.contains("session_id=dogfood-session"));
    assert!(stdout.contains("outcome=already_exited"));
    assert!(!stdout.contains("client disconnected"));
    assert!(!stderr.contains("client disconnected"));
    assert!(!stdout.contains(data_dir.to_string_lossy().as_ref()));
    assert!(!stderr.contains(data_dir.to_string_lossy().as_ref()));

    shutdown_cli_daemon(&data_dir, child);
}

#[test]
fn cli_request_level_runtime_error_returns_operator_frame_and_keeps_daemon_responsive() {
    let _guard = daemon_test_lock()
        .lock()
        .expect("serialize real daemon test");
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
fn cli_daemon_restart_recovers_worker_backed_session_through_transport() {
    let _guard = daemon_test_lock()
        .lock()
        .expect("serialize real daemon test");
    let data_dir = unique_test_dir("cli-restart-recover");
    let config = explicit_config(&data_dir);
    let session_id = "cli-restart-session";

    let child = start_cli_daemon(&data_dir);
    let spawn = botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::Spawn {
            session_id: session_id.to_string(),
            command: "printf 'restart-ready\\n'; while IFS= read -r line; do printf 'echo:%s\\n' \"$line\"; done".to_string(),
        },
    )
    .expect("spawn restart recovery session through daemon transport");
    assert_eq!(spawn.kind, botster_hub::DaemonResponseKind::Spawned);
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
            .any(|recovered| recovered == session_id),
        "restarted daemon should report startup recovery for the live worker-backed session"
    );
    assert!(
        !status
            .stale_sessions
            .iter()
            .any(|stale| stale == session_id),
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

    let shutdown_session = botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::ShutdownSession {
            session_id: session_id.to_string(),
        },
    )
    .expect("shutdown recovered session through daemon transport");
    assert_eq!(
        shutdown_session.kind,
        botster_hub::DaemonResponseKind::Events
    );
    shutdown_cli_daemon(&data_dir, restarted_child);
}

#[test]
fn external_hub_client_crate_drives_real_daemon_socket_protocol() {
    let _guard = daemon_test_lock()
        .lock()
        .expect("serialize real daemon test");
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
                "printf 'external-ready\\n'; while IFS= read -r line; do printf 'echo:%s\\n' \"$line\"; done"
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
fn installed_botster_web_launch_issues_local_webrtc_grant_and_data_channel_adapter() {
    let _guard = daemon_test_lock()
        .lock()
        .expect("serialize real daemon test");
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
    let child = start_cli_daemon(&data_dir);
    enable_supervised_package(&data_dir, &package_dir);

    let web_bridge_port = unused_loopback_port();
    let start = botster_hub_client::request(
        &endpoint,
        botster_hub_client::DaemonRequest::StartPackageEntrypoint {
            package_name: "botster-web".to_string(),
            entrypoint_id: "web-client".to_string(),
            environment_overrides: BTreeMap::from([(
                "BOTSTER_WEB_DOGFOOD_BRIDGE_PORT".to_string(),
                web_bridge_port.to_string(),
            )]),
        },
    )
    .expect("start botster-web entrypoint");
    assert_eq!(
        start.kind,
        botster_hub_client::DaemonResponseKind::LocalWebrtcBootstrap
    );
    let bootstrap = start
        .local_webrtc_bootstrap
        .expect("start response includes local WebRTC bootstrap");
    assert_eq!(bootstrap.package_name, "botster-web");
    assert_eq!(bootstrap.entrypoint_id, "web-client");
    assert_eq!(
        bootstrap.expected_origin,
        format!("http://127.0.0.1:{web_bridge_port}")
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

        let spawn = offer_peer
            .encrypted_request(
                &stream_key,
                &botster_hub_client::DaemonRequest::Spawn {
                    session_id: "local-webrtc-session".to_string(),
                    command: "printf 'local-webrtc-ready\\n'; while IFS= read -r line; do printf 'webrtc:%s\\n' \"$line\"; done".to_string(),
                },
            )
            .await
            .expect("spawn over encrypted WebRTC data channel");
        assert_eq!(spawn.kind, botster_hub_client::DaemonResponseKind::Spawned);

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
        offer_peer.peer.close().await.expect("close offer peer");
    });

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

    shutdown_cli_daemon(&data_dir, child);
}

#[test]
fn botster_web_same_url_reload_issues_fresh_local_webrtc_bootstrap() {
    let _guard = daemon_test_lock()
        .lock()
        .expect("serialize real daemon test");
    let data_dir = unique_short_test_dir("web-webrtc-reload");
    let package_dir = unique_test_dir("web-webrtc-reload-package");
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

    let web_bridge_port = unused_loopback_port();
    let start = botster_hub_client::request(
        &endpoint,
        botster_hub_client::DaemonRequest::StartPackageEntrypoint {
            package_name: "botster-web".to_string(),
            entrypoint_id: "web-client".to_string(),
            environment_overrides: BTreeMap::from([(
                "BOTSTER_WEB_DOGFOOD_BRIDGE_PORT".to_string(),
                web_bridge_port.to_string(),
            )]),
        },
    )
    .expect("start botster-web entrypoint");
    assert_eq!(
        start.kind,
        botster_hub_client::DaemonResponseKind::LocalWebrtcBootstrap
    );
    let bridge_url = format!("http://127.0.0.1:{web_bridge_port}");
    wait_for_botster_web_health(&bridge_url);

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

    let bootstrap_a = botster_web_page_bootstrap(&bridge_url);
    let bootstrap_b = botster_web_page_bootstrap(&bridge_url);
    assert_eq!(bootstrap_a.package_name, "botster-web");
    assert_eq!(bootstrap_a.entrypoint_id, "web-client");
    assert_eq!(bootstrap_a.expected_origin, bridge_url);
    assert_eq!(bootstrap_b.expected_origin, bootstrap_a.expected_origin);
    assert_ne!(bootstrap_a.grant_id, bootstrap_b.grant_id);
    assert_ne!(bootstrap_a.grant_secret, bootstrap_b.grant_secret);

    block_on(async {
        let (mut first_peer, first_key) = open_local_webrtc_peer(&endpoint, &bootstrap_a).await;
        let status = first_peer
            .encrypted_request(&first_key, &botster_hub_client::DaemonRequest::Status)
            .await
            .expect("status over first encrypted WebRTC data channel");
        assert_eq!(status.kind, botster_hub_client::DaemonResponseKind::Status);
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
        let attach = reload_peer
            .encrypted_request(
                &reload_key,
                &botster_hub_client::DaemonRequest::Attach {
                    session_id: "local-webrtc-reload-session".to_string(),
                    subscription_id: "local-webrtc-reload-subscription".to_string(),
                },
            )
            .await
            .expect("attach over reload encrypted WebRTC data channel");
        assert_eq!(attach.kind, botster_hub_client::DaemonResponseKind::Events);
        let send = reload_peer
            .encrypted_request(
                &reload_key,
                &botster_hub_client::DaemonRequest::SendInput {
                    session_id: "local-webrtc-reload-session".to_string(),
                    data: "after-reload\n".to_string(),
                },
            )
            .await
            .expect("send input over reload encrypted WebRTC data channel");
        assert_eq!(send.kind, botster_hub_client::DaemonResponseKind::Events);
        let mut observed = String::new();
        for _ in 0..120 {
            let drain = reload_peer
                .encrypted_request(
                    &reload_key,
                    &botster_hub_client::DaemonRequest::Drain {
                        session_id: "local-webrtc-reload-session".to_string(),
                    },
                )
                .await
                .expect("drain over reload encrypted WebRTC data channel");
            for event in drain.events {
                if let botster_hub_client::DaemonEvent::TerminalOutput { data, .. } = event {
                    observed.push_str(&data);
                }
            }
            if observed.contains("reload:after-reload") {
                break;
            }
            sleep(Duration::from_millis(30)).await;
        }
        assert!(
            observed.contains("reload:after-reload"),
            "reload encrypted WebRTC data channel should drain session output, got {observed:?}"
        );

        let shutdown = reload_peer
            .encrypted_request(
                &reload_key,
                &botster_hub_client::DaemonRequest::ShutdownSession {
                    session_id: "local-webrtc-reload-session".to_string(),
                },
            )
            .await
            .expect("shutdown over reload encrypted WebRTC data channel");
        assert_eq!(
            shutdown.kind,
            botster_hub_client::DaemonResponseKind::Events
        );
        reload_peer.peer.close().await.expect("close reload peer");
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
    ] {
        assert!(!persisted_state.contains(secret));
    }
    assert!(!persisted_state.contains("grant_secret"));

    shutdown_cli_daemon(&data_dir, child);
}

#[test]
fn local_webrtc_peer_close_detaches_terminal_subscriptions() {
    let _guard = daemon_test_lock()
        .lock()
        .expect("serialize real daemon test");
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

    let web_bridge_port = unused_loopback_port();
    let start = botster_hub_client::request(
        &endpoint,
        botster_hub_client::DaemonRequest::StartPackageEntrypoint {
            package_name: "botster-web".to_string(),
            entrypoint_id: "web-client".to_string(),
            environment_overrides: BTreeMap::from([(
                "BOTSTER_WEB_DOGFOOD_BRIDGE_PORT".to_string(),
                web_bridge_port.to_string(),
            )]),
        },
    )
    .expect("start botster-web entrypoint");
    let bootstrap = start
        .local_webrtc_bootstrap
        .expect("start response includes local WebRTC bootstrap");
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
fn external_hub_client_spawns_botster_web_dogfood_session_request_shape() {
    let _guard = daemon_test_lock()
        .lock()
        .expect("serialize real daemon test");
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
            session_id: "botster-web-dogfood-session".to_string(),
            command:
                "printf 'botster-web-dogfood-ready\\n'; while IFS= read -r line; do printf 'web:%s\\n' \"$line\"; done"
                    .to_string(),
        })
        .expect("botster-web dogfood spawn request");
    assert_eq!(spawn.kind, botster_hub_client::DaemonResponseKind::Spawned);
    assert!(spawn.sessions.iter().any(|session| session.session_id
        == "botster-web-dogfood-session"
        && session.lifecycle == "running"));

    let list = connection
        .request(&botster_hub_client::DaemonRequest::ListSessions)
        .expect("list sessions after botster-web dogfood spawn");
    assert_eq!(list.kind, botster_hub_client::DaemonResponseKind::Sessions);
    assert!(list.sessions.iter().any(|session| session.session_id
        == "botster-web-dogfood-session"
        && session.lifecycle == "running"));

    let packages = connection
        .request(&botster_hub_client::DaemonRequest::ListPackages)
        .expect("list packages remains observable after botster-web dogfood spawn");
    assert_eq!(
        packages.kind,
        botster_hub_client::DaemonResponseKind::Packages
    );

    let attach = connection
        .request(&botster_hub_client::DaemonRequest::Attach {
            session_id: "botster-web-dogfood-session".to_string(),
            subscription_id: "botster-web-dogfood-subscription".to_string(),
        })
        .expect("attach botster-web dogfood session");
    assert_eq!(attach.kind, botster_hub_client::DaemonResponseKind::Events);

    let send = connection
        .request(&botster_hub_client::DaemonRequest::SendInput {
            session_id: "botster-web-dogfood-session".to_string(),
            data: "from-web-action\n".to_string(),
        })
        .expect("send input to botster-web dogfood session");
    assert_eq!(send.kind, botster_hub_client::DaemonResponseKind::Events);

    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    let mut observed = String::new();
    while std::time::Instant::now() < deadline {
        let drain = connection
            .request(&botster_hub_client::DaemonRequest::Drain {
                session_id: "botster-web-dogfood-session".to_string(),
            })
            .expect("drain botster-web dogfood session");
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
        "botster-web dogfood request shape should attach and drain output, got {observed:?}"
    );

    let shutdown_session = connection
        .request(&botster_hub_client::DaemonRequest::ShutdownSession {
            session_id: "botster-web-dogfood-session".to_string(),
        })
        .expect("shutdown botster-web dogfood session");
    assert_eq!(
        shutdown_session.kind,
        botster_hub_client::DaemonResponseKind::Events
    );
    shutdown_cli_daemon(&data_dir, child);
}

#[test]
fn external_hub_client_duplicate_botster_web_dogfood_spawn_is_rejected_without_cleanup() {
    let _guard = daemon_test_lock()
        .lock()
        .expect("serialize real daemon test");
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
            session_id: "botster-web-dogfood-session".to_string(),
            command:
                "printf 'botster-web-dogfood-ready\\n'; while IFS= read -r line; do printf 'web:%s\\n' \"$line\"; done"
                    .to_string(),
        })
        .expect("first botster-web dogfood spawn request");
    assert_eq!(
        first_spawn.kind,
        botster_hub_client::DaemonResponseKind::Spawned
    );

    let duplicate = connection
        .request(&botster_hub_client::DaemonRequest::Spawn {
            session_id: "botster-web-dogfood-session".to_string(),
            command: "printf 'replacement-should-not-start\\n'".to_string(),
        })
        .expect("duplicate botster-web dogfood spawn should return operator frame");
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
            session_id: "botster-web-dogfood-session".to_string(),
            subscription_id: "botster-web-dogfood-duplicate-subscription".to_string(),
        })
        .expect("attach original botster-web dogfood session after duplicate rejection");
    assert_eq!(attach.kind, botster_hub_client::DaemonResponseKind::Events);

    let send = connection
        .request(&botster_hub_client::DaemonRequest::SendInput {
            session_id: "botster-web-dogfood-session".to_string(),
            data: "after-duplicate\n".to_string(),
        })
        .expect("existing session remains writable after duplicate rejection");
    assert_eq!(send.kind, botster_hub_client::DaemonResponseKind::Events);

    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    let mut observed = String::new();
    while std::time::Instant::now() < deadline {
        let drain = connection
            .request(&botster_hub_client::DaemonRequest::Drain {
                session_id: "botster-web-dogfood-session".to_string(),
            })
            .expect("drain original botster-web dogfood session after duplicate rejection");
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
            session_id: "botster-web-dogfood-session".to_string(),
        })
        .expect("shutdown botster-web dogfood session");
    assert_eq!(
        shutdown_session.kind,
        botster_hub_client::DaemonResponseKind::Events
    );
    shutdown_cli_daemon(&data_dir, child);
}

#[test]
fn external_hub_client_spawn_failure_returns_actionable_diagnostics() {
    let _guard = daemon_test_lock()
        .lock()
        .expect("serialize real daemon test");
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
            session_id: "botster-web-dogfood-session".to_string(),
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
    let _guard = daemon_test_lock()
        .lock()
        .expect("serialize real daemon test");
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
fn external_hub_test_support_drives_isolated_daemon_socket_protocol() {
    let _guard = daemon_test_lock()
        .lock()
        .expect("serialize real daemon test");
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
    assert!(terminal_app_report.hub_socket_env_present);
    assert!(terminal_app_report.hub_data_dir_env_present);
    assert_eq!(terminal_app_report.real_hub_action_operation, "status");
    assert_eq!(terminal_app_report.real_hub_action_result, "running");
    assert_eq!(terminal_app_report.exit_code, Some(0));
    assert!(
        terminal_app_report
            .stdout
            .contains("hub_socket_present=true")
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
fn external_daemon_attach_replays_prior_history_with_renderable_byte_count() {
    let _guard = daemon_test_lock()
        .lock()
        .expect("serialize real daemon test");
    let data_dir = unique_short_test_dir("late-history");
    let config = explicit_config(&data_dir);
    let child = start_cli_daemon(&data_dir);
    let mut connection =
        botster_hub::DaemonConnection::connect(&config).expect("connect daemon socket");

    let spawn = connection
        .request(&botster_hub::DaemonRequest::Spawn {
            session_id: "late-history-session".to_string(),
            command: "printf 'before-late\\n'; while IFS= read -r line; do printf 'after:%s\\n' \"$line\"; done".to_string(),
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
        if first_observed.contains("before-late") {
            break;
        }
        thread::sleep(Duration::from_millis(30));
    }
    assert!(
        first_observed.contains("before-late"),
        "first subscription should observe initial output before late attach, got {first_observed:?}"
    );

    let late_attach = connection
        .request(&botster_hub::DaemonRequest::Attach {
            session_id: "late-history-session".to_string(),
            subscription_id: "late-history-late-subscription".to_string(),
        })
        .expect("attach late subscription");
    assert_eq!(late_attach.kind, botster_hub::DaemonResponseKind::Events);

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
                } if subscription_id == "late-history-late-subscription"
                    && data.contains("after:live-after-late")
            )
        });
        if saw_live {
            break;
        }
        thread::sleep(Duration::from_millis(30));
    }

    let history_index = observed_events
        .iter()
        .position(|event| {
            matches!(
                event,
                botster_hub::DaemonEvent::Snapshot {
                    subscription_id,
                    data,
                    bytes,
                    ..
                }
                | botster_hub::DaemonEvent::Scrollback {
                    subscription_id,
                    data,
                    bytes,
                    ..
                } if subscription_id == "late-history-late-subscription"
                    && data.contains("before-late")
                    && *bytes == data.len()
            )
        })
        .expect("late subscription should receive prior output history with bytes == data.len()");
    let live_index = observed_events
        .iter()
        .position(|event| {
            matches!(
                event,
                botster_hub::DaemonEvent::TerminalOutput {
                    subscription_id,
                    data,
                    ..
                } if subscription_id == "late-history-late-subscription"
                    && data.contains("after:live-after-late")
            )
        })
        .expect("late subscription should receive later live output");
    assert!(
        history_index < live_index,
        "late history should precede later live output, got {observed_events:?}"
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

    let late_no_history_attach = connection
        .request(&botster_hub::DaemonRequest::Attach {
            session_id: "no-history-session".to_string(),
            subscription_id: "no-history-late-subscription".to_string(),
        })
        .expect("attach late no-history subscription");
    assert_eq!(
        late_no_history_attach.kind,
        botster_hub::DaemonResponseKind::Events
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
                } if subscription_id == "no-history-late-subscription"
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
                botster_hub::DaemonEvent::Snapshot {
                    subscription_id,
                    data,
                    ..
                }
                | botster_hub::DaemonEvent::Scrollback {
                    subscription_id,
                    data,
                    ..
                } if subscription_id == "no-history-late-subscription" && !data.is_empty()
            )
        }),
        "late no-history subscription should not receive fabricated history, got {no_history_events:?}"
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
    let _guard = daemon_test_lock()
        .lock()
        .expect("serialize real daemon test");
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

    shutdown_cli_daemon(&data_dir, child);
}

#[test]
fn daemon_notify_session_defers_without_observed_readiness_over_socket() {
    let _guard = daemon_test_lock()
        .lock()
        .expect("serialize real daemon test");
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

    shutdown_cli_daemon(&data_dir, child);
}

#[test]
fn stalled_attach_stdout_does_not_block_other_daemon_commands() {
    let _guard = daemon_test_lock()
        .lock()
        .expect("serialize real daemon test");
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
    let spawn = run_command_with_timeout(spawn_command, Duration::from_secs(3));
    assert!(
        spawn.status.success(),
        "spawn failed: {}",
        String::from_utf8_lossy(&spawn.stderr)
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
    thread::sleep(Duration::from_millis(500));
    assert!(
        attach_child
            .try_wait()
            .expect("poll stalled attach")
            .is_none(),
        "attach exited before the slow-consumer check"
    );

    let mut list_command = Command::new(env!("CARGO_BIN_EXE_botster-hub"));
    list_command
        .arg("sessions")
        .arg("list")
        .arg("--data-dir")
        .arg(&data_dir);
    let list = run_command_with_timeout(list_command, Duration::from_secs(2));
    assert!(
        list.status.success(),
        "list failed while attach stdout was blocked: {}",
        String::from_utf8_lossy(&list.stderr)
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
    let send = run_command_with_timeout(send_command, Duration::from_secs(2));
    assert!(
        send.status.success(),
        "send-input failed while attach stdout was blocked: {}",
        String::from_utf8_lossy(&send.stderr)
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
    let resize = run_command_with_timeout(resize_command, Duration::from_secs(2));
    assert!(
        resize.status.success(),
        "resize failed while attach stdout was blocked: {}",
        String::from_utf8_lossy(&resize.stderr)
    );

    let mut shutdown_command = Command::new(env!("CARGO_BIN_EXE_botster-hub"));
    shutdown_command
        .arg("shutdown")
        .arg("--data-dir")
        .arg(&data_dir);
    let shutdown = run_command_with_timeout(shutdown_command, Duration::from_secs(2));
    assert!(
        shutdown.status.success(),
        "shutdown failed while attach stdout was blocked: {}",
        String::from_utf8_lossy(&shutdown.stderr)
    );

    let _ = attach_child.kill();
    let _ = attach_child.wait_with_output();
    let output = child.wait_with_output().expect("wait for daemon child");
    assert!(
        output.status.success(),
        "daemon failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn cli_inspect_reports_not_found_for_fresh_in_process_daemon() {
    let data_dir = unique_test_dir("cli-inspect");
    let output = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("inspect")
        .arg("--data-dir")
        .arg(&data_dir)
        .arg("dogfood-session")
        .output()
        .expect("run botster-hub inspect");

    assert!(
        output.status.success(),
        "inspect failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).expect("stdout is utf8");
    assert!(stdout.contains("inspect=session"));
    assert!(stdout.contains("session_id=dogfood-session"));
    assert!(stdout.contains("found=false"));
    assert!(!stdout.contains(data_dir.to_string_lossy().as_ref()));
}

#[test]
fn cli_packages_enable_local_path_routes_through_running_daemon_and_persists() {
    let _guard = daemon_test_lock()
        .lock()
        .expect("serialize real daemon test");
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
    assert!(stdout.contains("package_name=dogfood.plugin"));
    assert!(stdout.contains("action=enable"));
    assert!(stdout.contains("response=packages"));
    assert!(stdout.contains("package name=dogfood.plugin"));
    assert!(stdout.contains("state=enabled"));
    assert!(stdout.contains("runnable_entrypoints=1"));
    assert!(stdout.contains("package_entrypoint package=dogfood.plugin id=web kind=web_app launch_mode=background command=bin/botster-web args=2 working_directory=package_root environment=1 capabilities=1 may_supervise=true process_state=not_started"));
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
            plugin.package_name == "dogfood.plugin" && plugin.state == "enabled" && plugin.loaded
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
    assert!(stdout.contains("package name=dogfood.plugin"));
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
    assert!(stdout.contains("package name=dogfood.plugin"));
    assert!(stdout.contains("state=enabled"));
    assert!(stdout.contains("runnable_entrypoints=1"));
    assert!(stdout.contains("package_entrypoint package=dogfood.plugin id=web kind=web_app launch_mode=background command=bin/botster-web args=2 working_directory=package_root environment=1 capabilities=1 may_supervise=true process_state=not_started"));
    assert!(!stdout.contains(package_dir.to_string_lossy().as_ref()));

    shutdown_cli_daemon(&data_dir, restarted);
}

#[test]
fn package_entrypoint_supervision_starts_and_reports_running() {
    let _guard = daemon_test_lock()
        .lock()
        .expect("serialize real daemon test");
    let data_dir = unique_test_dir("entrypoint-start");
    let package_dir = unique_test_dir("entrypoint-start-package");
    write_supervised_package(
        &package_dir,
        "dogfood.supervised",
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
            package_name: "dogfood.supervised".to_string(),
            entrypoint_id: "web".to_string(),
            environment_overrides: BTreeMap::new(),
        },
    )
    .expect("start supervised entrypoint");
    let entrypoint = package_entrypoint(&start, "dogfood.supervised");
    assert_eq!(entrypoint.process.state, "running");
    assert!(entrypoint.process.pid.is_some());
    assert!(entrypoint.process.started_at.is_some());

    let list = botster_hub::daemon_transport_request(
        &explicit_config(&data_dir),
        botster_hub::DaemonRequest::ListPackages,
    )
    .expect("list packages after supervised start");
    let entrypoint = package_entrypoint(&list, "dogfood.supervised");
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
        .arg("dogfood.supervised")
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
    assert!(stdout.contains("package_entrypoint_process package=dogfood.supervised id=web"));

    shutdown_cli_daemon(&data_dir, child);
}

#[test]
fn daemon_list_apps_projects_installed_package_entrypoints() {
    let _guard = daemon_test_lock()
        .lock()
        .expect("serialize real daemon test");
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
    assert_eq!(web.package_name, "dogfood.apps");
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
            package_name: "dogfood.apps".to_string(),
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
    let _guard = daemon_test_lock()
        .lock()
        .expect("serialize real daemon test");
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
        "dogfood.session-template/init"
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
fn daemon_spawns_repo_local_session_template_after_state_reload() {
    let _guard = daemon_test_lock()
        .lock()
        .expect("serialize real daemon test");
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
            state.admitted_session_template_targets = vec![AdmittedSessionTemplateTarget {
                target_id: "repo:dogfood".to_string(),
                root: repo_root.clone(),
                enabled: true,
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
    assert_eq!(templates.session_templates[0].target_id, "repo:dogfood");

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
    let _guard = daemon_test_lock()
        .lock()
        .expect("serialize real daemon test");
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
    assert!(launch.environment.contains_key("BOTSTER_HUB_SOCKET"));
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
    let _guard = daemon_test_lock()
        .lock()
        .expect("serialize real daemon test");
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
    assert!(list_text.contains("app package=dogfood.apps app_id=web"));
    assert!(list_text.contains("app package=dogfood.apps app_id=terminal"));

    let show = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("apps")
        .arg("show")
        .arg("--data-dir")
        .arg(&data_dir)
        .arg("dogfood.apps/web")
        .output()
        .expect("run apps show");
    assert!(
        show.status.success(),
        "apps show failed: {}",
        command_output_text(&show)
    );
    let show_text = command_output_text(&show);
    assert!(show_text.contains("response=app"));
    assert!(show_text.contains("package=dogfood.apps"));
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
    let _guard = daemon_test_lock()
        .lock()
        .expect("serialize real daemon test");
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
        .arg("dogfood.hub-env/web")
        .output()
        .expect("run apps open web with hub env fixture");
    assert!(
        open.status.success(),
        "apps open web failed: {}",
        command_output_text(&open)
    );
    let open_text = command_output_text(&open);
    assert!(open_text.contains("app_url=http://127.0.0.1:49153"));
    assert!(!open_text.contains("BOTSTER_HUB_BIN must point to a botster-hub binary"));

    let status = botster_hub::daemon_transport_request(
        &explicit_config(&data_dir),
        botster_hub::DaemonRequest::PackageEntrypointStatus {
            package_name: "dogfood.hub-env".to_string(),
            entrypoint_id: "web".to_string(),
        },
    )
    .expect("inspect web app entrypoint status");
    let entrypoint = package_entrypoint(&status, "dogfood.hub-env");
    assert_eq!(entrypoint.process.state, "running");
    assert!(entrypoint.process.diagnostics.iter().all(|diagnostic| {
        !diagnostic
            .message
            .contains("BOTSTER_HUB_BIN must point to a botster-hub binary")
    }));

    shutdown_cli_daemon(&data_dir, child);
}

#[test]
fn cli_apps_open_terminal_uses_foreground_launch_contract() {
    let _guard = daemon_test_lock()
        .lock()
        .expect("serialize real daemon test");
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
fn cli_no_arg_prints_host_profile_boot_summary() {
    let summary = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .output()
        .expect("run no-arg hub summary");
    assert!(
        summary.status.success(),
        "no-arg hub summary failed: {}",
        command_output_text(&summary)
    );
    let text = command_output_text(&summary);
    assert!(text.contains("first-party host profile ready"));
    assert!(text.contains("Daily runtime commands:"));
    assert!(text.contains("botster-hub open web --data-dir <path>"));
    assert!(text.contains(
        "botster-hub packages available --data-dir <path> --registry <registry-dir-or-file>"
    ));
    assert!(text.contains("botster-hub packages reload --data-dir <path> <name>"));
    assert!(!text.contains("unknown command"));
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
        assert!(text.contains("botster-hub apps open --data-dir <path> <app|package/app>"));
        assert!(
            text.contains(
                "botster-hub packages config set --data-dir <path> <name> '<json-object>'"
            )
        );
        assert!(text.contains(
            "botster-hub packages apply-update --data-dir <path> <name> --revision <revision>"
        ));
        assert!(!text.contains("first-party host profile ready"));
        assert!(!text.contains("unknown command"));
    }
}

#[test]
fn package_entrypoint_supervision_passes_environment_overrides() {
    let _guard = daemon_test_lock()
        .lock()
        .expect("serialize real daemon test");
    let data_dir = unique_test_dir("entrypoint-env");
    let package_dir = unique_test_dir("entrypoint-env-package");
    let output_path = std::env::current_dir()
        .expect("current dir")
        .join(data_dir.join("entrypoint-env.txt"));
    write_supervised_package(
        &package_dir,
        "dogfood.env",
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
            package_name: "dogfood.env".to_string(),
            entrypoint_id: "web".to_string(),
            environment_overrides: BTreeMap::from([(
                "BOTSTER_TEST_ENV_OVERRIDE".to_string(),
                "override-reached-child".to_string(),
            )]),
        },
    )
    .expect("start supervised entrypoint with env");
    let entrypoint = package_entrypoint(&start, "dogfood.env");
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
    let _guard = daemon_test_lock()
        .lock()
        .expect("serialize real daemon test");
    let data_dir = unique_test_dir("entrypoint-missing-command");
    let package_dir = unique_test_dir("entrypoint-missing-command-package");
    write_supervised_package(
        &package_dir,
        "dogfood.missing-command",
        "definitely-missing-botster-command",
        &[],
    );
    let child = start_cli_daemon(&data_dir);
    enable_supervised_package(&data_dir, &package_dir);

    let start = botster_hub::daemon_transport_request(
        &explicit_config(&data_dir),
        botster_hub::DaemonRequest::StartPackageEntrypoint {
            package_name: "dogfood.missing-command".to_string(),
            entrypoint_id: "web".to_string(),
            environment_overrides: BTreeMap::new(),
        },
    )
    .expect("start missing supervised entrypoint");
    let entrypoint = package_entrypoint(&start, "dogfood.missing-command");
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
    let _guard = daemon_test_lock()
        .lock()
        .expect("serialize real daemon test");
    let data_dir = unique_test_dir("entrypoint-failed-command");
    let package_dir = unique_test_dir("entrypoint-failed-command-package");
    write_supervised_package(
        &package_dir,
        "dogfood.failed-command",
        "sh",
        &["-c", "printf 'fixture failure\\n' >&2; exit 42"],
    );
    let child = start_cli_daemon(&data_dir);
    enable_supervised_package(&data_dir, &package_dir);

    let _ = botster_hub::daemon_transport_request(
        &explicit_config(&data_dir),
        botster_hub::DaemonRequest::StartPackageEntrypoint {
            package_name: "dogfood.failed-command".to_string(),
            entrypoint_id: "web".to_string(),
            environment_overrides: BTreeMap::new(),
        },
    )
    .expect("start failing supervised entrypoint");
    thread::sleep(Duration::from_millis(100));
    let status = botster_hub::daemon_transport_request(
        &explicit_config(&data_dir),
        botster_hub::DaemonRequest::PackageEntrypointStatus {
            package_name: "dogfood.failed-command".to_string(),
            entrypoint_id: "web".to_string(),
        },
    )
    .expect("status failing supervised entrypoint");
    let entrypoint = package_entrypoint(&status, "dogfood.failed-command");
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
    let _guard = daemon_test_lock()
        .lock()
        .expect("serialize real daemon test");
    let data_dir = unique_test_dir("entrypoint-restart");
    let package_dir = unique_test_dir("entrypoint-restart-package");
    write_supervised_package(
        &package_dir,
        "dogfood.restart",
        "sh",
        &[
            "-c",
            "test -n \"$BOTSTER_HUB_SOCKET\" && test -n \"$BOTSTER_HUB_DATA_DIR\" && while true; do sleep 1; done",
        ],
    );
    let child = start_cli_daemon(&data_dir);
    enable_supervised_package(&data_dir, &package_dir);

    let start = botster_hub::daemon_transport_request(
        &explicit_config(&data_dir),
        botster_hub::DaemonRequest::StartPackageEntrypoint {
            package_name: "dogfood.restart".to_string(),
            entrypoint_id: "web".to_string(),
            environment_overrides: BTreeMap::new(),
        },
    )
    .expect("start restart fixture");
    let first_pid = package_entrypoint(&start, "dogfood.restart")
        .process
        .pid
        .expect("first pid");

    let stop = botster_hub::daemon_transport_request(
        &explicit_config(&data_dir),
        botster_hub::DaemonRequest::StopPackageEntrypoint {
            package_name: "dogfood.restart".to_string(),
            entrypoint_id: "web".to_string(),
        },
    )
    .expect("stop restart fixture");
    assert_eq!(
        package_entrypoint(&stop, "dogfood.restart").process.state,
        "stopped"
    );
    wait_for_process_exit(first_pid);

    let restart = botster_hub::daemon_transport_request(
        &explicit_config(&data_dir),
        botster_hub::DaemonRequest::RestartPackageEntrypoint {
            package_name: "dogfood.restart".to_string(),
            entrypoint_id: "web".to_string(),
        },
    )
    .expect("restart fixture");
    let second_pid = package_entrypoint(&restart, "dogfood.restart")
        .process
        .pid
        .expect("second pid");
    assert_ne!(first_pid, second_pid);

    shutdown_cli_daemon(&data_dir, child);
}

#[test]
fn package_entrypoint_supervision_cleans_up_on_disable_remove_and_shutdown() {
    let _guard = daemon_test_lock()
        .lock()
        .expect("serialize real daemon test");
    let data_dir = unique_test_dir("entrypoint-cleanup");
    let package_dir = unique_test_dir("entrypoint-cleanup-package");
    write_supervised_package(
        &package_dir,
        "dogfood.cleanup",
        "sh",
        &["-c", "while true; do sleep 1; done"],
    );
    let child = start_cli_daemon(&data_dir);
    enable_supervised_package(&data_dir, &package_dir);

    let start = botster_hub::daemon_transport_request(
        &explicit_config(&data_dir),
        botster_hub::DaemonRequest::StartPackageEntrypoint {
            package_name: "dogfood.cleanup".to_string(),
            entrypoint_id: "web".to_string(),
            environment_overrides: BTreeMap::new(),
        },
    )
    .expect("start cleanup fixture");
    let disable_pid = package_entrypoint(&start, "dogfood.cleanup")
        .process
        .pid
        .expect("disable pid");
    let _ = botster_hub::daemon_transport_request(
        &explicit_config(&data_dir),
        botster_hub::DaemonRequest::DisablePackage {
            package_name: "dogfood.cleanup".to_string(),
        },
    )
    .expect("disable cleanup package");
    wait_for_process_exit(disable_pid);

    let _ = botster_hub::daemon_transport_request(
        &explicit_config(&data_dir),
        botster_hub::DaemonRequest::EnablePackage {
            package_name: "dogfood.cleanup".to_string(),
        },
    )
    .expect("re-enable cleanup package");
    let restart = botster_hub::daemon_transport_request(
        &explicit_config(&data_dir),
        botster_hub::DaemonRequest::StartPackageEntrypoint {
            package_name: "dogfood.cleanup".to_string(),
            entrypoint_id: "web".to_string(),
            environment_overrides: BTreeMap::new(),
        },
    )
    .expect("restart cleanup fixture");
    let shutdown_pid = package_entrypoint(&restart, "dogfood.cleanup")
        .process
        .pid
        .expect("shutdown pid");

    shutdown_cli_daemon(&data_dir, child);
    wait_for_process_exit(shutdown_pid);
}

#[test]
fn package_entrypoint_supervision_cleans_up_on_daemon_signal() {
    let _guard = daemon_test_lock()
        .lock()
        .expect("serialize real daemon test");
    let data_dir = unique_test_dir("entrypoint-signal");
    let package_dir = unique_test_dir("entrypoint-signal-package");
    write_supervised_package(
        &package_dir,
        "dogfood.signal",
        "sh",
        &["-c", "while true; do sleep 1; done"],
    );
    let child = start_cli_daemon(&data_dir);
    enable_supervised_package(&data_dir, &package_dir);

    let start = botster_hub::daemon_transport_request(
        &explicit_config(&data_dir),
        botster_hub::DaemonRequest::StartPackageEntrypoint {
            package_name: "dogfood.signal".to_string(),
            entrypoint_id: "web".to_string(),
            environment_overrides: BTreeMap::new(),
        },
    )
    .expect("start signal fixture");
    let pid = package_entrypoint(&start, "dogfood.signal")
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
    let _guard = daemon_test_lock()
        .lock()
        .expect("serialize real daemon test");
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
    assert!(stdout.contains("package_name=dogfood.plugin"));
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
        .arg("dogfood.plugin")
        .output()
        .expect("run botster-hub packages show");
    assert!(
        show.status.success(),
        "show failed: {}",
        String::from_utf8_lossy(&show.stderr)
    );
    let stdout = String::from_utf8(show.stdout).expect("stdout is utf8");
    assert!(stdout.contains("package_count=1"));
    assert!(stdout.contains("package name=dogfood.plugin"));
    assert!(stdout.contains("state=installed"));
    assert!(!stdout.contains(package_dir.to_string_lossy().as_ref()));
    assert!(!stdout.contains(data_dir.to_string_lossy().as_ref()));

    let enable = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("packages")
        .arg("enable")
        .arg("--data-dir")
        .arg(&data_dir)
        .arg("dogfood.plugin")
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
        plugin.package_name == "dogfood.plugin" && plugin.state == "enabled" && plugin.loaded
    }));

    let disable = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("packages")
        .arg("disable")
        .arg("--data-dir")
        .arg(&data_dir)
        .arg("dogfood.plugin")
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
        plugin.package_name == "dogfood.plugin" && plugin.state == "disabled" && !plugin.loaded
    }));

    let remove = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("packages")
        .arg("remove")
        .arg("--data-dir")
        .arg(&data_dir)
        .arg("dogfood.plugin")
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
    let _guard = daemon_test_lock()
        .lock()
        .expect("serialize real daemon test");
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
      "id": "dogfood-local",
      "first_party": true,
      "source": { "type": "local_path", "path": "packages/local" }
    },
    {
      "id": "dogfood-git",
      "first_party": true,
      "source": {
        "type": "git",
        "repo": "https://example.invalid/botster/dogfood.git",
        "branch": "main",
        "tag": "v1.0.0",
        "rev": "abc123"
      },
      "manifest": {
        "name": "dogfood.git",
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
        .find(|package| package.entry_id == "dogfood-local")
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
    assert_eq!(install_request.entry_id.as_deref(), Some("dogfood-local"));
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
            entry_id: "dogfood-git".to_string(),
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
            entry_id: "dogfood-local".to_string(),
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
            entry_id: "dogfood-git".to_string(),
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
        .find(|package| package.package_name == "dogfood.git")
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
    let record = restored.package("dogfood.git").expect("restored package");
    assert_eq!(record.state, botster_hub::PackageState::Installed);
    assert_eq!(
        record
            .source_metadata
            .as_ref()
            .expect("source metadata")
            .entry_id,
        "dogfood-git"
    );
    assert_eq!(
        record.pin.as_ref().expect("pin").rev.as_deref(),
        Some("abc123")
    );
}

#[test]
fn cli_packages_local_path_diagnostics_are_actionable() {
    let _guard = daemon_test_lock()
        .lock()
        .expect("serialize real daemon test");
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
        .arg("dogfood.denied-plugin")
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
        .arg("dogfood.missing-plugin")
        .output()
        .expect("run missing package show");
    assert!(!missing_show.status.success());
    let text = command_output_text(&missing_show);
    assert!(text.contains("response=operator_error"));
    assert!(text.contains("operation=show"));
    assert!(text.contains("PackageNotInstalled"));
    assert!(text.contains("dogfood.missing-plugin"));
    assert!(!text.contains(data_dir.to_string_lossy().as_ref()));

    let missing_remove = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("packages")
        .arg("remove")
        .arg("--data-dir")
        .arg(&data_dir)
        .arg("dogfood.missing-plugin")
        .output()
        .expect("run missing package remove");
    assert!(!missing_remove.status.success());
    let text = command_output_text(&missing_remove);
    assert!(text.contains("response=operator_error"));
    assert!(text.contains("operation=remove"));
    assert!(text.contains("PackageNotInstalled"));
    assert!(text.contains("dogfood.missing-plugin"));
    assert!(!text.contains(data_dir.to_string_lossy().as_ref()));

    shutdown_cli_daemon(&data_dir, child);
}

#[test]
fn cli_packages_enable_botster_workspaces_first_party_plugin_db_namespace() {
    let _guard = daemon_test_lock()
        .lock()
        .expect("serialize real daemon test");
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
    let _guard = daemon_test_lock()
        .lock()
        .expect("serialize real daemon test");
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
    let _guard = daemon_test_lock()
        .lock()
        .expect("serialize real daemon test");
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
    let _guard = daemon_test_lock()
        .lock()
        .expect("serialize real daemon test");
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
        .find(|package| package.package_name == "dogfood.reloadable")
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
            package_name: "dogfood.reloadable".to_string(),
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
            package_name: "dogfood.reloadable".to_string(),
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
        .find(|package| package.package_name == "dogfood.reloadable")
        .expect("reloaded package row");
    assert_eq!(reloaded_package.version, "1.1.0");

    let apps = wait_for_app_local_url(&data_dir, "web", "http://127.0.0.1:49161");
    let app = app_row(&apps, "web");
    assert_eq!(app.package_name, "dogfood.reloadable");
    assert_eq!(
        app.launch_target.local_url.as_deref(),
        Some("http://127.0.0.1:49161")
    );

    let open = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("apps")
        .arg("open")
        .arg("--data-dir")
        .arg(&data_dir)
        .arg("dogfood.reloadable/web")
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
        .arg("dogfood.reloadable")
        .output()
        .expect("run package reload CLI");
    assert!(
        cli.status.success(),
        "packages reload failed: {}",
        command_output_text(&cli)
    );
    let cli_text = command_output_text(&cli);
    assert!(cli_text.contains("decision=package"));
    assert!(cli_text.contains("package_name=dogfood.reloadable"));
    assert!(cli_text.contains("action=reload"));
    assert!(cli_text.contains("version=1.1.0"));
    assert!(!cli_text.contains(package_dir.to_string_lossy().as_ref()));
    assert!(!cli_text.contains(data_dir.to_string_lossy().as_ref()));

    let alias = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .arg("reload")
        .arg("dogfood.reloadable")
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
    assert!(alias_text.contains("package_name=dogfood.reloadable"));
    assert!(alias_text.contains("action=reload"));
    assert!(alias_text.contains("version=1.1.0"));
    assert!(!alias_text.contains(package_dir.to_string_lossy().as_ref()));
    assert!(!alias_text.contains(data_dir.to_string_lossy().as_ref()));

    shutdown_cli_daemon(&data_dir, child);
}

#[test]
fn daemon_exposes_and_resolves_plugin_surface_and_settings_routes() {
    let _guard = daemon_test_lock()
        .lock()
        .expect("serialize real daemon test");
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
fn local_package_reload_name_mismatch_returns_path_free_operator_error() {
    let _guard = daemon_test_lock()
        .lock()
        .expect("serialize real daemon test");
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
        "dogfood.reloadable-renamed",
        "1.1.0",
        "http://127.0.0.1:49163",
    );
    let reload = botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::ReloadPackage {
            package_name: "dogfood.reloadable".to_string(),
        },
    )
    .expect("reload renamed local package returns operator frame");

    assert_eq!(reload.kind, botster_hub::DaemonResponseKind::OperatorError);
    let error = reload.error.as_ref().expect("operator error");
    assert!(error.message.contains("InvalidLocalManifest"));
    assert!(error.message.contains("dogfood.reloadable-renamed"));
    assert!(error.message.contains("dogfood.reloadable"));
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
    let _guard = daemon_test_lock()
        .lock()
        .expect("serialize real daemon test");
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
    let _guard = daemon_test_lock()
        .lock()
        .expect("serialize real daemon test");
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
    let _guard = daemon_test_lock()
        .lock()
        .expect("serialize real daemon test");
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
            package_name: "dogfood.plugin".to_string(),
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
            package_name: "dogfood.plugin".to_string(),
        },
    )
    .expect("enable local package");
    let enabled_check = botster_hub::daemon_transport_request(
        &config,
        botster_hub::DaemonRequest::CheckPackageUpdate {
            package_name: "dogfood.plugin".to_string(),
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
        .arg("dogfood.plugin")
        .output()
        .expect("run packages check-update");
    assert!(
        cli.status.success(),
        "packages check-update failed: {}",
        String::from_utf8_lossy(&cli.stderr)
    );
    let stdout = String::from_utf8(cli.stdout).expect("stdout is utf8");
    assert!(stdout.contains("package_update package=dogfood.plugin"));
    assert!(stdout.contains("reload_required=true"));
    assert!(
        stdout.contains("package_update_diagnostic package=dogfood.plugin kind=reload_available")
    );

    shutdown_cli_daemon(&data_dir, child);
}

#[test]
fn cli_packages_enable_local_process_package_does_not_attempt_lua_load() {
    let _guard = daemon_test_lock()
        .lock()
        .expect("serialize real daemon test");
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
        plugin.package_name == "dogfood.process-plugin"
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
fn no_arg_boot_summary_does_not_create_home_or_xdg_state_file() {
    let home = unique_test_dir("home");
    let xdg = unique_test_dir("xdg");
    fs::create_dir_all(&home).expect("create home");
    fs::create_dir_all(&xdg).expect("create xdg");

    let output = Command::new(env!("CARGO_BIN_EXE_botster-hub"))
        .env("HOME", &home)
        .env("XDG_DATA_HOME", &xdg)
        .output()
        .expect("run botster-hub summary");

    assert!(
        output.status.success(),
        "summary failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_no_state_file_under(&home);
    assert_no_state_file_under(&xdg);
}

fn assert_no_state_file_under(root: &Path) {
    let direct = root.join("hub-state.json");
    let botster = root.join("botster").join("hub-state.json");
    let botster_hub = root.join("botster-hub").join("hub-state.json");

    assert!(!direct.exists(), "unexpected state file at {direct:?}");
    assert!(!botster.exists(), "unexpected state file at {botster:?}");
    assert!(
        !botster_hub.exists(),
        "unexpected state file at {botster_hub:?}"
    );
}
