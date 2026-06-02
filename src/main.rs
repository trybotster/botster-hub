use std::process;

use botster_hub::{HubRuntime, RuntimeEnvironment, build_default_config_for_runtime, host_profile};

fn main() {
    let environment = RuntimeEnvironment::from_current_process();

    match build_default_config_for_runtime(&environment) {
        Ok(config) => {
            let runtime = HubRuntime::new(config);
            let profile = host_profile();
            println!(
                "{} first-party host profile ready for {}: {} roles, {} core capability surfaces",
                profile.id,
                runtime.config().host.id,
                profile.responsibilities().len(),
                profile.capability_surfaces().len()
            );
        }
        Err(error) => {
            eprintln!("botster-hub config error: {error}");
            process::exit(1);
        }
    }
}
