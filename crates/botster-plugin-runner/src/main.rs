//! The Lua plugin runner, started by Core as a service (Stage 2 brief, architecture fact 3). Not built yet: it reports
//! that and exits with status 2.

use std::process::ExitCode;

fn main() -> ExitCode {
    eprintln!("botster-plugin-runner: not built yet (botster-hub v1 bootstrap)");
    ExitCode::from(2)
}
