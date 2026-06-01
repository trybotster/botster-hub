use std::process;

use botster_hub::{RuntimeEnvironment, cli};

fn main() {
    let environment = RuntimeEnvironment::from_current_process();
    let output = cli::run(std::env::args_os().skip(1), &environment);

    print!("{}", output.stdout);
    eprint!("{}", output.stderr);

    if output.status != 0 {
        process::exit(output.status);
    }
}
