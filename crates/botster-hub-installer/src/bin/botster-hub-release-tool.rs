//! `botster-hub-release-tool` — mint a signing key and sign a release manifest.
//!
//! Kept as a separate binary from the installer so the installer stays purely a
//! *verifier*: the component that writes executables to disk holds no signing
//! capability, and the release side holds no install capability. They share a
//! crate only because they share `ring`.
//!
//! No production key exists yet. This ticket ships the signing *procedure* and
//! an unmistakably-named test keypair; real key custody and a real HTTPS origin
//! are a follow-up release ticket.

use std::path::PathBuf;
use std::process;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use ring::rand::SystemRandom;
use ring::signature::{Ed25519KeyPair, KeyPair};

use botster_hub_installation::{ReleaseDocument, ReleaseManifest, ReleaseSignature};

const USAGE: &str = "usage:
  botster-hub-release-tool generate-key --out-dir <dir> --name <basename>
  botster-hub-release-tool sign \\
      --key <pkcs8-path> \\
      --key-id <id> \\
      --manifest <manifest.json> \\
      --out <release.json>

`sign` reads a manifest, base64-encodes its exact bytes, signs those exact bytes
with ed25519, and emits the schema-2 release document. The signature covers the
transported bytes verbatim, so signer and verifier cannot disagree about what was
signed — no canonical-JSON agreement is required.";

fn main() {
    let arguments: Vec<String> = std::env::args().skip(1).collect();
    match run(&arguments) {
        Ok(message) => println!("{message}"),
        Err(error) => {
            eprintln!("botster-hub-release-tool error: {error}");
            process::exit(1);
        }
    }
}

fn run(arguments: &[String]) -> Result<String, String> {
    match arguments.first().map(String::as_str) {
        Some("generate-key") => generate_key(&arguments[1..]),
        Some("sign") => sign(&arguments[1..]),
        Some("help" | "--help" | "-h") | None => Ok(USAGE.to_string()),
        Some(other) => Err(format!("unknown command {other:?}\n{USAGE}")),
    }
}

fn options(arguments: &[String]) -> Result<Vec<(String, String)>, String> {
    let mut parsed = Vec::new();
    let mut cursor = 0;
    while cursor < arguments.len() {
        let flag = arguments[cursor]
            .strip_prefix("--")
            .ok_or_else(|| format!("unexpected argument {:?}\n{USAGE}", arguments[cursor]))?;
        let value = arguments
            .get(cursor + 1)
            .ok_or_else(|| format!("--{flag} requires a value\n{USAGE}"))?;
        parsed.push((flag.to_string(), value.clone()));
        cursor += 2;
    }
    Ok(parsed)
}

fn option(parsed: &[(String, String)], name: &str) -> Result<String, String> {
    parsed
        .iter()
        .find(|(flag, _)| flag == name)
        .map(|(_, value)| value.clone())
        .ok_or_else(|| format!("--{name} is required\n{USAGE}"))
}

fn generate_key(arguments: &[String]) -> Result<String, String> {
    let parsed = options(arguments)?;
    let out_dir = PathBuf::from(option(&parsed, "out-dir")?);
    let name = option(&parsed, "name")?;

    let random = SystemRandom::new();
    let document = Ed25519KeyPair::generate_pkcs8(&random)
        .map_err(|error| format!("generate ed25519 key: {error}"))?;
    let pair = Ed25519KeyPair::from_pkcs8(document.as_ref())
        .map_err(|error| format!("load generated ed25519 key: {error}"))?;

    std::fs::create_dir_all(&out_dir).map_err(|error| format!("create {out_dir:?}: {error}"))?;
    let private = out_dir.join(format!("{name}.pkcs8"));
    let public = out_dir.join(format!("{name}.pub"));
    std::fs::write(&private, format!("{}\n", BASE64.encode(document.as_ref())))
        .map_err(|error| format!("write {private:?}: {error}"))?;
    std::fs::write(
        &public,
        format!("{}\n", BASE64.encode(pair.public_key().as_ref())),
    )
    .map_err(|error| format!("write {public:?}: {error}"))?;

    Ok(format!(
        "private_key={}\npublic_key={}",
        private.display(),
        public.display()
    ))
}

fn sign(arguments: &[String]) -> Result<String, String> {
    let parsed = options(arguments)?;
    let key_path = PathBuf::from(option(&parsed, "key")?);
    let key_id = option(&parsed, "key-id")?;
    let manifest_path = PathBuf::from(option(&parsed, "manifest")?);
    let out_path = PathBuf::from(option(&parsed, "out")?);

    let encoded = std::fs::read_to_string(&key_path)
        .map_err(|error| format!("read signing key {key_path:?}: {error}"))?;
    let pkcs8 = BASE64
        .decode(encoded.trim().as_bytes())
        .map_err(|error| format!("decode signing key: {error}"))?;
    let pair =
        Ed25519KeyPair::from_pkcs8(&pkcs8).map_err(|error| format!("load signing key: {error}"))?;

    // Sign the manifest bytes exactly as they will be transported: read once,
    // sign, and embed the same bytes. Re-serializing between signing and
    // embedding is the canonical-JSON bug class this design avoids.
    let manifest_bytes = std::fs::read(&manifest_path)
        .map_err(|error| format!("read manifest {manifest_path:?}: {error}"))?;
    let manifest: ReleaseManifest = serde_json::from_slice(&manifest_bytes)
        .map_err(|error| format!("parse manifest {manifest_path:?}: {error}"))?;
    let signature = pair.sign(&manifest_bytes);

    let document = ReleaseDocument {
        schema_version: botster_hub_installation::RELEASE_SCHEMA_VERSION,
        product_id: manifest.product_id.clone(),
        release_channel: manifest.release_channel.clone(),
        version: manifest.version.clone(),
        build_revision: manifest.build_revision.clone(),
        install_manifest: BASE64.encode(&manifest_bytes),
        signature: ReleaseSignature {
            algorithm: "ed25519".to_string(),
            key_id,
            value: BASE64.encode(signature.as_ref()),
        },
    };
    let serialized = serde_json::to_vec_pretty(&document)
        .map_err(|error| format!("serialize release document: {error}"))?;
    std::fs::write(&out_path, serialized)
        .map_err(|error| format!("write {out_path:?}: {error}"))?;

    Ok(format!("release_document={}", out_path.display()))
}
