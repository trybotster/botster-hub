//! `botster-hub-installer` — install a signed, revision-coupled botster-hub
//! release into a managed prefix and record the installation receipt.
//!
//! The trust anchor is **not** embedded. No production key exists yet, and
//! shipping a default test key would create exactly the "mistaken for production
//! material" hazard that keeping the anchor explicit avoids. `--trust-anchor` is
//! required and the installer refuses to run without one.

use std::path::PathBuf;
use std::process;

use botster_hub_installer::error::{InstallerError, InstallerResult};
use botster_hub_installer::install::{InstallRequest, install, worker_path};
use botster_hub_installer::verify::parse_trust_anchor;

const USAGE: &str = "usage: botster-hub-installer install \\
      --prefix <dir> \\
      --source <https-url> \\
      --trust-anchor <path> \\
      [--channel stable|beta|nightly]

Installs the revision-coupled botster-hub and botster-session-worker pair as one
generation, switches the installation pointer atomically, and writes the managed
installation receipt to $HOME/.botster/installations/botster-hub.json.

Upgrades are offline: the installer refuses to run while any managed Hub daemon
from the same installation holds the installation lease.";

fn main() {
    let arguments: Vec<String> = std::env::args().skip(1).collect();
    match run(&arguments) {
        Ok(message) => println!("{message}"),
        Err(error) => {
            eprintln!("botster-hub-installer error: {error}");
            process::exit(1);
        }
    }
}

fn run(arguments: &[String]) -> InstallerResult<String> {
    match arguments.first().map(String::as_str) {
        Some("install") => run_install(&arguments[1..]),
        Some("help" | "--help" | "-h") => Ok(USAGE.to_string()),
        Some(other) => Err(InstallerError::new(
            "unknown_command",
            format!("unknown command {other:?}\n{USAGE}"),
        )),
        None => Err(InstallerError::new(
            "usage",
            format!("a command is required\n{USAGE}"),
        )),
    }
}

fn run_install(arguments: &[String]) -> InstallerResult<String> {
    let mut prefix: Option<PathBuf> = None;
    let mut source: Option<String> = None;
    let mut trust_anchor: Option<PathBuf> = None;
    let mut channel = "stable".to_string();

    let mut cursor = 0;
    while cursor < arguments.len() {
        let take = |name: &str| -> InstallerResult<String> {
            arguments.get(cursor + 1).cloned().ok_or_else(|| {
                InstallerError::new("usage", format!("{name} requires a value\n{USAGE}"))
            })
        };
        match arguments[cursor].as_str() {
            "--prefix" => prefix = Some(PathBuf::from(take("--prefix")?)),
            "--source" => source = Some(take("--source")?),
            "--trust-anchor" => trust_anchor = Some(PathBuf::from(take("--trust-anchor")?)),
            "--channel" => channel = take("--channel")?,
            other => {
                return Err(InstallerError::new(
                    "usage",
                    format!("unexpected argument {other:?}\n{USAGE}"),
                ));
            }
        }
        cursor += 2;
    }

    let prefix = prefix.ok_or_else(|| required("--prefix"))?;
    let source_url = source.ok_or_else(|| required("--source"))?;
    let trust_anchor = trust_anchor.ok_or_else(|| required("--trust-anchor"))?;
    let home = std::env::var_os("HOME")
        .filter(|home| !home.is_empty())
        .map(PathBuf::from)
        .ok_or_else(|| {
            InstallerError::new(
                "home_unavailable",
                "HOME is required to write the installation receipt",
            )
        })?;

    let anchor_contents = std::fs::read_to_string(&trust_anchor).map_err(|error| {
        InstallerError::new(
            "invalid_trust_anchor",
            format!(
                "trust anchor {} could not be read: {error}",
                trust_anchor.display()
            ),
        )
    })?;
    let trust_anchor = parse_trust_anchor(&anchor_contents)?;

    let request = InstallRequest {
        prefix,
        home,
        source_url,
        release_channel: channel,
        trust_anchor,
    };
    let summary = install(&request)?;

    let mut report = String::new();
    report.push_str(&format!("installed_version={}\n", summary.version));
    report.push_str(&format!("build_revision={}\n", summary.build_revision));
    report.push_str(&format!("generation={}\n", summary.generation));
    report.push_str(&format!(
        "reused_generation={}\n",
        summary.reused_generation
    ));
    report.push_str(&format!(
        "previous_generation={}\n",
        summary.previous_generation.as_deref().unwrap_or("none")
    ));
    report.push_str(&format!(
        "entrypoint={}\n",
        request
            .prefix
            .join(botster_hub_installation::layout::BIN_DIRECTORY)
            .join(botster_hub_installation::layout::HUB_BINARY_NAME)
            .display()
    ));
    report.push_str(&format!(
        "session_worker={}",
        worker_path(&request.prefix, &summary.generation).display()
    ));
    Ok(report)
}

fn required(flag: &str) -> InstallerError {
    InstallerError::new("usage", format!("{flag} is required\n{USAGE}"))
}
