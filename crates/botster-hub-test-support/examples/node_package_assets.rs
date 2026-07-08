use std::env;
use std::error::Error;
use std::fs;
use std::path::PathBuf;

use botster_hub_test_support::{
    application_primitives_fixture_descriptor, copy_plugin_contract_matrix_fixture,
    daemon_protocol_typescript_artifact, first_party_client_support_matrix,
    plugin_contract_matrix_fixture_asset,
};
use serde_json::json;

fn ordered_unique(values: &[&'static str]) -> Vec<&'static str> {
    values
        .iter()
        .copied()
        .fold(Vec::new(), |mut unique, value| {
            if !unique.contains(&value) {
                unique.push(value);
            }
            unique
        })
}

fn main() -> Result<(), Box<dyn Error>> {
    let output_dir = env::args()
        .nth(1)
        .map(PathBuf::from)
        .ok_or("usage: node_package_assets <output-dir>")?;

    fs::create_dir_all(&output_dir)?;

    let protocol = daemon_protocol_typescript_artifact();
    fs::write(output_dir.join("daemon-protocol.ts"), protocol.contents)?;

    let fixture = plugin_contract_matrix_fixture_asset();
    copy_plugin_contract_matrix_fixture(&output_dir)?;
    let application_primitives = application_primitives_fixture_descriptor();

    let matrix = first_party_client_support_matrix();
    let metadata = json!({
        "protocol": matrix.protocol,
        "protocol_version": matrix.protocol_version,
        "conformance_fixture_revision": matrix.conformance_fixture_revision,
        "daemon_protocol_source_artifact": protocol.artifact_path,
        "plugin_contract_matrix": {
            "package_name": fixture.package_name,
            "artifact_path": fixture.artifact_path,
            "files": fixture
                .files
                .iter()
                .map(|file| file.relative_path)
                .collect::<Vec<_>>(),
        },
        "application_primitives": {
            "fixture_package_name": application_primitives.fixture_package_name,
            "artifact_path": application_primitives.artifact_path,
            "surface_id": application_primitives.surface_id,
            "route_id": application_primitives.route_id,
            "renderer_entrypoint": application_primitives.renderer_entrypoint,
            "primitive_kinds": ordered_unique(application_primitives.node_kinds),
        },
    });

    fs::write(
        output_dir.join("metadata-origin.json"),
        format!("{}\n", serde_json::to_string_pretty(&metadata)?),
    )?;

    Ok(())
}
