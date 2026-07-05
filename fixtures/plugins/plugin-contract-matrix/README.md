# Plugin Contract Matrix Fixture

This is the canonical hub-owned fixture package for exercising public Botster package and plugin contracts. It is intentionally PII-free and uses only `example.invalid` fixture values.

Client repos can install this directory as a local package and use the real daemon protocol to verify conformance:

```text
botster-hub packages install fixtures/plugins/plugin-contract-matrix
botster-hub packages enable botster.plugin-contract-matrix
```

Rust client and plugin repos should prefer the hub-owned test-support helper
instead of inventing DTO fixtures:

```rust
let report = botster_hub_test_support::run_plugin_contract_matrix_conformance(
    &hub,
    "fixtures/plugins/plugin-contract-matrix",
)
.expect("plugin UI conformance");
assert_eq!(report.app_surface_node_id, "contract-app-panel");
assert_eq!(report.client_render_check.expected_redacted_secret_state, "redacted");
```

In this repository, the full isolated-hub proof is:

```bash
./test.sh --test hub_daemon_lifecycle_test daemon_plugin_contract_matrix_fixture_exercises_public_package_contracts
```

Harness failures are classified as producer contract failures, downstream
renderer comparisons, or environment/setup failures such as missing hub binary
paths.

## Matrix

- `contract.app`: app surface returning a concrete UiNode payload through `plugin_surface_render`.
- `contract.empty`: placeholder app surface returning a valid empty-state UiNode payload.
- `contract.blocked`: render handler that fails deliberately so clients can assert the daemon `operator_error` response and continued daemon responsiveness.
- `contract.settings`: settings surface returning sanitized effective configuration from `botster.capabilities.config.get()`.
- Configuration schema: `endpoint` URL default, `mode` select default and validation options, and redacted `api_token` secret.
- `contract.action`: `plugin_surface_action` handler with accepted and error states selected by the request payload.
- Package route descriptors: manifest `surfaces` should project to `surface:<id>` routes under `/packages/botster.plugin-contract-matrix/surfaces/<id>`.
- Package lifecycle compatibility: hub conformance should prove install, enable, list, show, route descriptors, and action-state projection through the daemon package DTOs. The installed `DaemonPackage` row currently does not expose a separate protocol compatibility descriptor; that remains covered by package admission and lifecycle state.
