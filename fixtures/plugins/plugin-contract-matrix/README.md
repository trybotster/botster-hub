# Plugin Contract Matrix Fixture

This is the canonical hub-owned fixture package for exercising public Botster package and plugin contracts. It is intentionally PII-free and uses only `example.invalid` fixture values.

Hub developers can install this source fixture directory as a local package and use the real daemon protocol to verify conformance:

```text
botster-hub packages install fixtures/plugins/plugin-contract-matrix
botster-hub packages enable botster.plugin-contract-matrix
```

Rust client and plugin repos should prefer the hub-owned test-support helper
instead of inventing DTO fixtures or relying on a sibling hub checkout:

```rust
let fixture_root = tempfile::tempdir().expect("fixture tempdir");
let fixture_path = botster_hub_test_support::copy_plugin_contract_matrix_fixture(
    fixture_root.path(),
)
.expect("copy plugin contract matrix fixture");

let report = botster_hub_test_support::run_plugin_contract_matrix_conformance(
    &hub,
    fixture_path,
)
.expect("plugin UI conformance");
assert_eq!(report.app_surface_node_id, "contract-app-panel");
assert!(report.dialog_visible_after_open);
assert_eq!(report.dialog_form_node_id, "contract-app-form");
assert!(!report.actionable_sibling_form_during_dialog);
assert!(report.selected_workspace_visible_after_open);
assert!(!report.dialog_visible_after_valid_submit);
assert_eq!(report.client_render_check.expected_redacted_secret_state, "redacted");
```

Client protocol drift checks can read the generated TypeScript protocol through
`botster_hub_test_support::daemon_protocol_typescript_artifact()`. The helper is
a convenience wrapper; `botster-hub-client` remains the protocol source of
truth.

Node client repos should consume the published package instead of relying on a
sibling hub checkout:

```js
import {
  materializePluginContractMatrixFixture,
  readDaemonProtocolTypescript,
} from "@trybotster/hub-test-support";

const protocolSource = readDaemonProtocolTypescript();
const fixturePath = materializePluginContractMatrixFixture(tempDirectory);
```

In this repository, the full isolated-hub proof is:

```bash
./test.sh --test hub_daemon_lifecycle_test daemon_plugin_contract_matrix_fixture_exercises_public_package_contracts
```

Harness failures are classified as producer contract failures, downstream
renderer comparisons, or environment/setup failures such as missing hub binary
paths.

## Matrix

- `contract.app`: app surface returning a concrete UiNode payload through `plugin_surface_render`; it exercises the hub-validated application primitives `panel`, `toolbar`, `metric_grid`, `table`, `empty_state`, `status_badge`, `section`, `form`, `text_input`, `button`, and `dialog`. The one canonical Form and its input/submit controls live inside the presence-bound Dialog rather than behind it as a panel sibling. A browser-shaped consumer reads Open from the initially visible tree, applies its accepted scoped `set` operations, and restricts submit discovery to the active Dialog subtree. Invalid submission rejects while retaining that reachable Dialog/Form and field association; valid submission returns normalized values plus a replacement and clears the dialog. A distinct rendered action toggles `contract-toggle`.
- `contract.empty`: placeholder app surface returning a valid empty-state UiNode payload.
- `contract.blocked`: render handler that fails deliberately so clients can assert the daemon `operator_error` response and continued daemon responsiveness.
- `contract.invalid_body`: declared render surface whose handler returns malformed UiNode data so clients can assert `invalid_surface` and a structured `plugin_surface_render` diagnostic from hub validation.
- `contract.settings`: settings surface returning sanitized effective configuration from `botster.capabilities.config.get()`.
- Configuration schema: `endpoint` URL default, `mode` select default and validation options, and redacted `api_token` secret.
- `contract.action`: `plugin_surface_action` handler consuming the canonical request envelope. Rendered payload metadata selects open, toggle, or submit behavior; negative conformance probes select a generic error, deliberately mismatched identity, or malformed replacement. Submitted form data comes from `values`. Every valid result echoes the request's complete identity, including the presence or absence of `node_id`. Field-error responses are keyed by the rendered `contract-app-message` input id.
- Package route descriptors: manifest `surfaces` should project to `surface:<id>` routes under `/packages/botster.plugin-contract-matrix/surfaces/<id>`.
- Package lifecycle compatibility: hub conformance should prove install, enable, list, show, route descriptors, and action-state projection through the daemon package DTOs. The installed `DaemonPackage` row currently does not expose a separate protocol compatibility descriptor; that remains covered by package admission and lifecycle state.

Successful render responses should expose the validated UI payload through
`plugin_surface.ui_tree_snapshot`. The compatibility `plugin_surface.body` field
is preserved, but browser and TUI clients should treat the hub-validated
snapshot as the blessed rendering contract.
