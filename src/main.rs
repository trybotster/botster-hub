use std::process;

use botster_hub::{RuntimeEnvironment, build_default_config_for_runtime};

fn main() {
    let environment = RuntimeEnvironment::from_current_process();

    match build_default_config_for_runtime(&environment) {
        Ok(_config) => {
            let summary = botster_hub::architecture_summary();
            println!(
                "botster-hub config ready: {} roles, {} provider capability contracts",
                summary.responsibilities().len(),
                summary.provider_capabilities().len()
            );
        }
        Err(error) => {
            eprintln!("botster-hub config error: {error}");
            process::exit(1);
        }
    }
}
