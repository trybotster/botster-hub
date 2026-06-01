use std::process;

use botster_hub::{RuntimeEnvironment, build_default_config_for_runtime};

fn main() {
    let environment = RuntimeEnvironment::from_current_process();

    match build_default_config_for_runtime(&environment) {
        Ok(_config) => println!("botster-hub config ready"),
        Err(error) => {
            eprintln!("botster-hub config error: {error}");
            process::exit(1);
        }
    }
}
