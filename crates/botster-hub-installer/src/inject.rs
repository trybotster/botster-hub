//! Test-only failure injection points.
//!
//! Ordering regressions are invisible without an injection point: a test that
//! only checks the happy path cannot distinguish "binary before receipt" from
//! "receipt before binary".
//!
//! Two mechanisms, because conflating them is exactly what makes an ordering
//! guarantee self-contradictory:
//!
//! * `fail:<point>` returns a **recoverable error**, so rollback code runs. This
//!   is what proves the rollback path.
//! * `abort:<point>` raises `SIGKILL` on this process, so **no** rollback code
//!   runs. This is what proves the crash window is bounded to safe states.
//!
//! Both are gated on `BOTSTER_ENV=test` *and* an explicit opt-in variable,
//! mirroring how the Hub gates its incompatible-daemon fixture.

use crate::error::{InstallerError, InstallerResult};

/// The environment variable that selects an injection.
pub const INJECT_ENV: &str = "BOTSTER_HUB_INSTALLER_TEST_INJECT";

/// A boundary at which an injection can fire.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Point {
    /// Between opening a staged artifact and finishing its write.
    ArtifactWrite,
    /// After a complete, verified staging directory, before it is renamed in.
    BeforeStagingRename,
    /// After the generation is published, immediately before the pointer moves.
    BeforeSwitch,
    /// Immediately after the pointer switch.
    AfterSwitch,
    /// First install only: immediately after `current` is created.
    AfterCurrent,
    /// First install only: immediately after `bin/botster-hub` is published.
    AfterBin,
    /// Post-switch identity verification.
    PostSwitchVerify,
    /// After verification, immediately before the receipt is committed.
    BeforeReceipt,
    /// Models a crash *during* the receipt write: a unique stale temporary is
    /// left in the installations directory and the process dies before the
    /// rename.
    ReceiptWrite,
}

impl Point {
    const fn token(self) -> &'static str {
        match self {
            Self::ArtifactWrite => "artifact_write",
            Self::BeforeStagingRename => "before_staging_rename",
            Self::BeforeSwitch => "before_switch",
            Self::AfterSwitch => "after_switch",
            Self::AfterCurrent => "after_current",
            Self::AfterBin => "after_bin",
            Self::PostSwitchVerify => "post_switch_verify",
            Self::BeforeReceipt => "before_receipt",
            Self::ReceiptWrite => "receipt_write",
        }
    }
}

/// The directory a `hold:` injection rendezvous through.
pub const HOLD_DIR_ENV: &str = "BOTSTER_HUB_INSTALLER_TEST_HOLD_DIR";
/// Bound on how long a `hold:` injection will wait to be released.
const HOLD_LIMIT: std::time::Duration = std::time::Duration::from_secs(60);

/// What an injection does when it fires.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    /// Abrupt termination: no rollback, no destructors, no cleanup.
    Abort,
    /// A returned error: the recoverable path, so rollback runs.
    Fail,
    /// Pause inside the mutation transaction until released.
    ///
    /// This is what makes "the lease is held *continuously*" testable rather
    /// than asserted. Without a way to observe the window between acquiring the
    /// lease and reaching a final state, a check-then-act implementation — which
    /// acquires, releases, then mutates — passes every other test.
    Hold,
}

/// The configured injection, if any.
#[must_use]
pub fn configured() -> Option<(Mode, String)> {
    if std::env::var("BOTSTER_ENV").as_deref() != Ok("test") {
        return None;
    }
    let raw = std::env::var(INJECT_ENV).ok()?;
    let (mode, point) = raw.split_once(':')?;
    let mode = match mode {
        "abort" => Mode::Abort,
        "fail" => Mode::Fail,
        "hold" => Mode::Hold,
        _ => return None,
    };
    Some((mode, point.to_string()))
}

/// Whether an injection is armed for `point`.
#[must_use]
pub fn armed(point: Point) -> Option<Mode> {
    configured().and_then(|(mode, token)| (token == point.token()).then_some(mode))
}

/// Fire the injection configured for `point`, if any.
pub fn check(point: Point) -> InstallerResult<()> {
    match armed(point) {
        None => Ok(()),
        Some(Mode::Fail) => Err(InstallerError::new(
            "injected_failure",
            format!("injected recoverable failure at {}", point.token()),
        )),
        Some(Mode::Abort) => {
            // SIGKILL rather than `process::abort`: no unwinding, no destructors,
            // no `Drop` on the lease — the closest available model of power loss.
            unsafe { libc::raise(libc::SIGKILL) };
            unreachable!("SIGKILL does not return");
        }
        Some(Mode::Hold) => {
            hold(point);
            Ok(())
        }
    }
}

/// Announce arrival at `point` and wait, bounded, to be released.
fn hold(point: Point) {
    let Ok(directory) = std::env::var(HOLD_DIR_ENV) else {
        return;
    };
    let directory = std::path::PathBuf::from(directory);
    let _ = std::fs::create_dir_all(&directory);
    let _ = std::fs::write(directory.join("reached"), point.token());
    let release = directory.join("release");
    let deadline = std::time::Instant::now() + HOLD_LIMIT;
    while std::time::Instant::now() < deadline {
        if release.exists() {
            return;
        }
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
}
