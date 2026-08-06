//! Loopback release fixture and signing harness for installer transaction tests.

use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use ring::rand::SystemRandom;
use ring::signature::{Ed25519KeyPair, KeyPair};

static SEQUENCE: AtomicU64 = AtomicU64::new(1);

/// One response the loopback release fixture will serve.
#[derive(Clone)]
pub struct Route {
    pub status: u16,
    pub body: Vec<u8>,
    pub location: Option<String>,
}

impl Route {
    pub fn ok(body: Vec<u8>) -> Self {
        Self {
            status: 200,
            body,
            location: None,
        }
    }

    pub fn redirect(location: &str) -> Self {
        Self {
            status: 302,
            body: Vec::new(),
            location: Some(location.to_string()),
        }
    }
}

/// A loopback HTTP origin. Loopback plain HTTP is the only non-HTTPS coordinate
/// the installer accepts, and it exists for exactly this purpose.
pub struct ReleaseServer {
    pub base: String,
    routes: Arc<Mutex<HashMap<String, Route>>>,
    stopping: Arc<AtomicBool>,
    address: String,
    handle: Option<thread::JoinHandle<()>>,
}

impl ReleaseServer {
    pub fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind release fixture");
        let address = listener.local_addr().expect("release fixture address");
        let routes: Arc<Mutex<HashMap<String, Route>>> = Arc::new(Mutex::new(HashMap::new()));
        let stopping = Arc::new(AtomicBool::new(false));

        let served = Arc::clone(&routes);
        let halt = Arc::clone(&stopping);
        let handle = thread::spawn(move || {
            for stream in listener.incoming() {
                if halt.load(Ordering::Relaxed) {
                    break;
                }
                let Ok(mut stream) = stream else { continue };
                let _ = stream.set_read_timeout(Some(Duration::from_secs(5)));
                let mut buffer = [0_u8; 8192];
                let Ok(read) = stream.read(&mut buffer) else {
                    continue;
                };
                let request = String::from_utf8_lossy(&buffer[..read]);
                let path = request
                    .lines()
                    .next()
                    .and_then(|line| line.split_whitespace().nth(1))
                    .unwrap_or("/")
                    .to_string();
                let route = served.lock().expect("routes").get(&path).cloned();
                match route {
                    Some(route) => write_response(&mut stream, &route),
                    None => write_response(
                        &mut stream,
                        &Route {
                            status: 404,
                            body: Vec::new(),
                            location: None,
                        },
                    ),
                }
            }
        });

        Self {
            base: format!("http://{address}"),
            routes,
            stopping,
            address: address.to_string(),
            handle: Some(handle),
        }
    }

    pub fn serve(&self, path: &str, route: Route) {
        self.routes
            .lock()
            .expect("routes")
            .insert(path.to_string(), route);
    }

    pub fn url(&self, path: &str) -> String {
        format!("{}{path}", self.base)
    }
}

impl Drop for ReleaseServer {
    fn drop(&mut self) {
        self.stopping.store(true, Ordering::Relaxed);
        let _ = TcpStream::connect(&self.address);
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

fn write_response(stream: &mut TcpStream, route: &Route) {
    let reason = match route.status {
        200 => "OK",
        302 => "Found",
        404 => "Not Found",
        _ => "Unknown",
    };
    let mut headers = format!(
        "HTTP/1.1 {} {reason}\r\nContent-Length: {}\r\nConnection: close\r\n",
        route.status,
        route.body.len()
    );
    if let Some(location) = &route.location {
        headers.push_str(&format!("Location: {location}\r\n"));
    }
    headers.push_str("\r\n");
    let _ = stream.write_all(headers.as_bytes());
    let _ = stream.write_all(&route.body);
    let _ = stream.flush();
}

/// A synthetic release: two coupled artifacts plus the metadata describing them.
pub struct Release {
    pub version: String,
    pub hub_revision: String,
    pub core_revision: String,
    pub hub_bytes: Vec<u8>,
    pub worker_bytes: Vec<u8>,
}

impl Release {
    /// Build a synthetic revision-coupled pair.
    ///
    /// The Hub artifact answers `version` with `key=value` identity lines, and
    /// the worker artifact carries a marker naming the same release. That pairing
    /// is what makes a mixed Hub/worker pair *detectable* in a test rather than
    /// merely asserted to be impossible.
    pub fn new(version: &str, hub_nibble: char, core_nibble: char) -> Self {
        let hub_revision: String = std::iter::repeat_n(hub_nibble, 40).collect();
        let core_revision: String = std::iter::repeat_n(core_nibble, 40).collect();
        let hub_bytes = format!(
            "#!/bin/sh\n\
             if [ \"$1\" != version ]; then exit 64; fi\n\
             echo product_id=botster-hub\n\
             echo version={version}\n\
             echo build_revision={hub_revision}\n"
        )
        .into_bytes();
        let worker_bytes = format!("botster-session-worker for release {version}\n").into_bytes();
        Self {
            version: version.to_string(),
            hub_revision,
            core_revision,
            hub_bytes,
            worker_bytes,
        }
    }

    pub fn generation(&self) -> String {
        format!("{}-{}", self.hub_revision, self.core_revision)
    }

    pub fn manifest(&self, server: &ReleaseServer) -> serde_json::Value {
        serde_json::json!({
            "product_id": "botster-hub",
            "release_channel": "stable",
            "version": self.version,
            "build_revision": self.hub_revision,
            "source_revisions": {
                "botster_hub": self.hub_revision,
                "botster_core": self.core_revision,
            },
            "artifacts": [
                {
                    "name": "botster-hub",
                    "url": server.url(&format!("/{}/botster-hub", self.version)),
                    "size": self.hub_bytes.len(),
                    "sha256": sha256_hex(&self.hub_bytes),
                },
                {
                    "name": "botster-session-worker",
                    "url": server.url(&format!("/{}/botster-session-worker", self.version)),
                    "size": self.worker_bytes.len(),
                    "sha256": sha256_hex(&self.worker_bytes),
                },
            ],
        })
    }

    pub fn publish_artifacts(&self, server: &ReleaseServer) {
        server.serve(
            &format!("/{}/botster-hub", self.version),
            Route::ok(self.hub_bytes.clone()),
        );
        server.serve(
            &format!("/{}/botster-session-worker", self.version),
            Route::ok(self.worker_bytes.clone()),
        );
    }
}

pub fn sha256_hex(bytes: &[u8]) -> String {
    ring::digest::digest(&ring::digest::SHA256, bytes)
        .as_ref()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

/// The signing side of the harness.
pub struct SigningKey {
    pair: Ed25519KeyPair,
}

impl SigningKey {
    pub fn generate() -> Self {
        let random = SystemRandom::new();
        let document = Ed25519KeyPair::generate_pkcs8(&random).expect("generate signing key");
        Self {
            pair: Ed25519KeyPair::from_pkcs8(document.as_ref()).expect("load signing key"),
        }
    }

    pub fn public_base64(&self) -> String {
        BASE64.encode(self.pair.public_key().as_ref())
    }

    /// Sign the exact manifest bytes and wrap them in a release document.
    pub fn document(&self, manifest: &serde_json::Value) -> serde_json::Value {
        let bytes = serde_json::to_vec(manifest).expect("serialize manifest");
        self.document_from_bytes(&bytes, manifest)
    }

    pub fn document_from_bytes(
        &self,
        signed_bytes: &[u8],
        envelope_source: &serde_json::Value,
    ) -> serde_json::Value {
        let signature = self.pair.sign(signed_bytes);
        serde_json::json!({
            "schema_version": 2,
            "product_id": envelope_source["product_id"],
            "release_channel": envelope_source["release_channel"],
            "version": envelope_source["version"],
            "build_revision": envelope_source["build_revision"],
            "install_manifest": BASE64.encode(signed_bytes),
            "signature": {
                "algorithm": "ed25519",
                "key_id": "test-only-do-not-trust",
                "value": BASE64.encode(signature.as_ref()),
            },
        })
    }
}

/// A prefix, a home, a trust anchor, and a loopback origin.
pub struct Harness {
    pub root: PathBuf,
    pub prefix: PathBuf,
    pub home: PathBuf,
    pub anchor: PathBuf,
    pub key: SigningKey,
    pub server: ReleaseServer,
}

impl Harness {
    pub fn new(label: &str) -> Self {
        let root = std::env::temp_dir().join(format!(
            "botster-hub-installer-{label}-{}-{}",
            std::process::id(),
            SEQUENCE.fetch_add(1, Ordering::Relaxed)
        ));
        let prefix = root.join("prefix");
        let home = root.join("home");
        std::fs::create_dir_all(&home).expect("create home");
        let key = SigningKey::generate();
        let anchor = root.join("trust-anchor.pub");
        std::fs::write(&anchor, format!("{}\n", key.public_base64())).expect("write trust anchor");
        Self {
            root,
            prefix,
            home,
            anchor,
            key,
            server: ReleaseServer::start(),
        }
    }

    /// Publish a release and its artifacts at `/<version>/botster-hub.json`.
    pub fn publish(&self, release: &Release) -> String {
        let manifest = release.manifest(&self.server);
        let document = self.key.document(&manifest);
        self.publish_document(release, &document)
    }

    pub fn publish_document(&self, release: &Release, document: &serde_json::Value) -> String {
        release.publish_artifacts(&self.server);
        let path = format!("/{}/botster-hub.json", release.version);
        self.server.serve(
            &path,
            Route::ok(serde_json::to_vec(document).expect("serialize document")),
        );
        self.server.url(&path)
    }

    pub fn install(&self, source_url: &str) -> Output {
        self.install_with_injection(source_url, None)
    }

    pub fn install_with_injection(&self, source_url: &str, inject: Option<&str>) -> Output {
        self.installer_command(source_url, inject)
            .output()
            .expect("run installer")
    }

    pub fn installer_command(&self, source_url: &str, inject: Option<&str>) -> Command {
        let mut command = Command::new(env!("CARGO_BIN_EXE_botster-hub-installer"));
        command
            .arg("install")
            .arg("--prefix")
            .arg(&self.prefix)
            .arg("--source")
            .arg(source_url)
            .arg("--trust-anchor")
            .arg(&self.anchor)
            .env("HOME", &self.home)
            .env("BOTSTER_ENV", "test");
        match inject {
            Some(injection) => {
                command.env("BOTSTER_HUB_INSTALLER_TEST_INJECT", injection);
            }
            None => {
                command.env_remove("BOTSTER_HUB_INSTALLER_TEST_INJECT");
            }
        }
        command
    }

    /// Run the installer paused at `point`, run `during` while it is paused
    /// inside the mutation transaction, then release it.
    pub fn install_while_held<T>(
        &self,
        source_url: &str,
        point: &str,
        during: impl FnOnce() -> T,
    ) -> (T, Output) {
        let rendezvous = self.root.join(format!("hold-{point}"));
        std::fs::create_dir_all(&rendezvous).expect("create rendezvous");
        let mut command = self.installer_command(source_url, Some(&format!("hold:{point}")));
        command.env(botster_hub_installer::inject::HOLD_DIR_ENV, &rendezvous);
        let mut child = command
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .expect("spawn held installer");

        let reached = rendezvous.join("reached");
        let deadline = std::time::Instant::now() + Duration::from_secs(30);
        while !reached.exists() {
            assert!(
                std::time::Instant::now() < deadline,
                "the installer never reached the {point} hold point"
            );
            if let Some(status) = child.try_wait().expect("poll held installer") {
                panic!("the installer exited with {status} before reaching {point}");
            }
            thread::sleep(Duration::from_millis(10));
        }

        let observed = during();
        std::fs::write(rendezvous.join("release"), b"go").expect("release the installer");
        let output = child.wait_with_output().expect("wait for held installer");
        (observed, output)
    }

    pub fn receipt_path(&self) -> PathBuf {
        self.home.join(".botster/installations/botster-hub.json")
    }

    pub fn receipt(&self) -> Option<serde_json::Value> {
        let bytes = std::fs::read(self.receipt_path()).ok()?;
        serde_json::from_slice(&bytes).ok()
    }

    pub fn current_target(&self) -> Option<String> {
        std::fs::read_link(self.prefix.join("current"))
            .ok()
            .map(|target| target.to_string_lossy().into_owned())
    }

    /// The generation both live binaries actually come from.
    ///
    /// Reading the pair *through* `current` is the point: a mixed pair would
    /// show a Hub reporting one version beside a worker marked with another.
    pub fn live_pair(&self) -> Option<(String, String)> {
        let generation = self.current_target()?;
        let directory = self.prefix.join(&generation);
        let hub = std::fs::read_to_string(directory.join("botster-hub")).ok()?;
        let worker = std::fs::read_to_string(directory.join("botster-session-worker")).ok()?;
        let hub_version = hub
            .lines()
            .find_map(|line| line.strip_prefix("echo version="))?
            .to_string();
        let worker_version = worker.split_whitespace().last().map(str::to_string)?;
        Some((hub_version, worker_version))
    }

    pub fn generations(&self) -> Vec<String> {
        let Ok(entries) = std::fs::read_dir(self.prefix.join("generations")) else {
            return Vec::new();
        };
        let mut names: Vec<String> = entries
            .flatten()
            .map(|entry| entry.file_name().to_string_lossy().into_owned())
            .collect();
        names.sort();
        names
    }

    pub fn entrypoint(&self) -> PathBuf {
        self.prefix.join("bin/botster-hub")
    }

    /// Everything under the prefix, as a sorted (path, digest) list.
    ///
    /// Used to assert that a rejected release mutated *nothing*.
    pub fn prefix_fingerprint(&self) -> Vec<(String, String)> {
        let mut entries = Vec::new();
        collect(&self.prefix, &self.prefix, &mut entries);
        entries.sort();
        entries
    }
}

fn collect(root: &Path, directory: &Path, entries: &mut Vec<(String, String)>) {
    let Ok(children) = std::fs::read_dir(directory) else {
        return;
    };
    for child in children.flatten() {
        let path = child.path();
        let relative = path
            .strip_prefix(root)
            .unwrap_or(&path)
            .to_string_lossy()
            .into_owned();
        let Ok(metadata) = std::fs::symlink_metadata(&path) else {
            continue;
        };
        if metadata.file_type().is_symlink() {
            let target = std::fs::read_link(&path).unwrap_or_default();
            entries.push((relative, format!("symlink:{}", target.display())));
        } else if metadata.is_dir() {
            entries.push((relative, "dir".to_string()));
            collect(root, &path, entries);
        } else {
            let bytes = std::fs::read(&path).unwrap_or_default();
            entries.push((relative, sha256_hex(&bytes)));
        }
    }
}

impl Drop for Harness {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

pub fn output_text(output: &Output) -> String {
    format!(
        "status={} stdout={} stderr={}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    )
}
