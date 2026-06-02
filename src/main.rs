use std::process;

use botster_hub::{HubRuntime, RuntimeEnvironment, build_default_config_for_runtime};

fn main() {
    let environment = RuntimeEnvironment::from_current_process();

    match build_default_config_for_runtime(&environment) {
        Ok(config) => {
            let runtime = HubRuntime::new(config);
            let summary = botster_hub::architecture_summary();
            println!(
                "botster-hub runtime ready for {}: {} responsibility roles over botster-core",
                runtime.config().host.id,
                summary.responsibilities().len()
            );
        }
        Err(error) => {
            eprintln!("botster-hub config error: {error}");
            process::exit(1);
        }
    }
}
