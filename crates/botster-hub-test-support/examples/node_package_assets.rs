use std::env;
use std::error::Error;
use std::fs;
use std::path::PathBuf;

use botster_hub_test_support::{
    application_primitives_fixture_descriptor, copy_plugin_contract_matrix_fixture,
    daemon_protocol_typescript_artifact, first_party_client_support_matrix,
    late_attach_history_conformance_fixture_json,
    local_webrtc_delivery_chunk_conformance_fixture_json, mode_flags_conformance_fixture_json,
    plugin_contract_matrix_fixture_asset, session_lifecycle_subscription_conformance_fixture_json,
    session_plugin_binding_conformance_fixture_json,
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
    fs::write(
        output_dir.join("first-party-client-support-matrix.json"),
        format!("{}\n", serde_json::to_string_pretty(&matrix)?),
    )?;
    fs::write(
        output_dir.join("session-lifecycle-subscription-conformance-fixture.json"),
        format!(
            "{}\n",
            serde_json::to_string_pretty(
                &session_lifecycle_subscription_conformance_fixture_json()
            )?
        ),
    )?;
    fs::write(
        output_dir.join("session-plugin-binding-conformance-fixture.json"),
        format!(
            "{}\n",
            serde_json::to_string_pretty(&session_plugin_binding_conformance_fixture_json())?
        ),
    )?;
    fs::write(
        output_dir.join("late-attach-history-conformance-fixture.json"),
        format!(
            "{}\n",
            serde_json::to_string_pretty(&late_attach_history_conformance_fixture_json())?
        ),
    )?;
    fs::write(
        output_dir.join("local-webrtc-delivery-chunk-conformance-fixture.json"),
        format!(
            "{}\n",
            serde_json::to_string_pretty(&local_webrtc_delivery_chunk_conformance_fixture_json())?
        ),
    )?;
    fs::write(
        output_dir.join("mode-flags-conformance-fixture.json"),
        format!(
            "{}\n",
            serde_json::to_string_pretty(&mode_flags_conformance_fixture_json())?
        ),
    )?;
    let metadata = json!({
        "protocol": matrix.protocol,
        "protocol_version": matrix.protocol_version,
        "conformance_fixture_revision": matrix.conformance_fixture_revision,
        "ui_contract": {
            "package_name": "@trybotster/ui-contract",
            "package_version": "0.3.3",
            "conformance_fixture_export": "@trybotster/ui-contract/conformance-fixtures",
        },
        "daemon_protocol_source_artifact": protocol.artifact_path,
        "first_party_client_support_matrix": {
            "artifact_path": "first-party-client-support-matrix.json",
            "source_artifact_path": "botster_hub_test_support::first_party_client_support_matrix()",
        },
        "session_lifecycle_subscription_conformance_fixture": {
            "artifact_path": "session-lifecycle-subscription-conformance-fixture.json",
            "source_artifact_path": "botster_hub_test_support::session_lifecycle_subscription_conformance_fixture_json()",
        },
        "session_plugin_binding_conformance_fixture": {
            "artifact_path": "session-plugin-binding-conformance-fixture.json",
            "source_artifact_path": "botster_hub_test_support::session_plugin_binding_conformance_fixture_json()",
        },
        "late_attach_history_conformance_fixture": {
            "artifact_path": "late-attach-history-conformance-fixture.json",
            "source_artifact_path": "botster_hub_test_support::late_attach_history_conformance_fixture_json()",
        },
        "local_webrtc_delivery_chunk_conformance_fixture": {
            "artifact_path": "local-webrtc-delivery-chunk-conformance-fixture.json",
            "source_artifact_path": "botster_hub_test_support::local_webrtc_delivery_chunk_conformance_fixture_json()",
        },
        "mode_flags_conformance_fixture": {
            "artifact_path": "mode-flags-conformance-fixture.json",
            "source_artifact_path": "botster_hub_test_support::mode_flags_conformance_fixture_json()",
        },
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
