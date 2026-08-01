# Implementation report: accept bound sentinels for required bindable fields

## Target and assumptions

- Target repository: `trybotster/botster-hub`
- Target ID: `tgt_7e208a0c76a44980a83b63af976b1f22`
- Pipeline run: `run_1785617167_977535`
- Ticket: `ticket_1785617154_342333`
- Base Hub commit: `1955c9e0713281093f609d09f6597a1dcfaf07d3`
- Locked Core commit used by the downstream worker build: `5846fc776d31e2b6c98a8d932f50a31078743901`
- Assumption: `UiNode::validate()` remains the compatible authored-validation API; consumers that materialize bindings opt into the new strict `validate_realized()` API.
- Assumption: Hub owns authored surface admission and transport, while TUI/Web own production materialization after repinning the merged Hub contract.

## Guidance applied

Loaded in the required order:

1. `[[implementer-playbook]]`
2. `[[botster-implementer-playbook]]`
3. `[[botster-hub-playbook]]`
4. Targeted notes: `[[botster-architecture]]`, `[[cli-patterns]]`, `[[spa-patterns]]`, `[[botster package surface semantics live in ui contract while hub owns admission]]`, `[[plugin surface handlers must validate against hub locked uinode contract]]`, `[[plugin surfaces request model state through ui bindings not hub subscribe]]`, `[[plugin dynamic ui lists bind to plugin-owned entities]]`, `[[ui contract row ids can bind before template expansion]]`, `[[ui bind list typed templates are narrower than the runtime wire grammar]]`, `[[hub supervision admission changes require exact live hub launch proof]]`, `[[live hub proof records distinct hub and locked core binary provenance]]`, `[[hub test support npm releases need external consumer smoke]]`, `[[shared conformance fixtures that contradict the core contract teach clients the wrong state machine]]`, and `[[conformance fixture revisions must be unique per published content]]`.
5. `[[project-pipelines-playbook]]` for workflow/checklist/report/gate discipline only; no Project Pipelines source changed.

The implementation also followed the approved revision at `docs/plans/accept-bound-sentinels-for-required-bindable-fields.md` and human rulings `question_1785617615_993233` and `question_1785617674_572099`.

## Implementation and files changed

- Contract validation and generated authorities:
  - `crates/botster-ui-contract/Cargo.toml`
  - `crates/botster-ui-contract/src/lib.rs`
  - `crates/botster-ui-contract/src/assets.rs`
  - `crates/botster-ui-contract/tests/ui_contract_test.rs`
  - `crates/botster-ui-contract/tests/generated_assets_test.rs`
  - `packages/ui-contract/package.json`, `index.js`, `index.d.ts`, `schema.json`, `conformance-fixtures.json`, `README.md`, and `test.mjs`
- Hub admission and live downstream proof:
  - `src/runtime.rs`
  - `tests/hub_daemon_lifecycle_test.rs`
  - `crates/botster-hub-client/src/lib.rs`
- Source fixture, mirrors, and strict Rust/Node reference materializers:
  - `fixtures/plugins/plugin-contract-matrix/README.md` and `plugin.lua`
  - `crates/botster-hub-test-support/src/lib.rs`, `examples/node_package_assets.rs`, and mirrored fixture `README.md`/`plugin.lua`
  - `packages/hub-test-support/package.json`, `index.js`, `index.d.ts`, `metadata.json`, `test.mjs`, `README.md`, all generated conformance JSON files, and the mirrored fixture `README.md`/`plugin.lua`
- Release/docs/lockfile:
  - `Cargo.lock`
  - `README.md`
  - `docs/client-protocol.md`
  - `docs/plans/accept-bound-sentinels-for-required-bindable-fields.md`
  - this report

Authored validation now accepts a structurally valid `$bind` sentinel for the seven proven required-bindable fields. Class A keeps nonblank string-or-bind semantics for Button/IconButton/MenuItem `label`, Form `submit_label`, and Iframe `src`/`title`. Class B keeps Text `text` presence-only literal semantics, including empty string, number, and null, while accepting valid binds. Representative required non-bindable fields still reject sentinels. Strict realized validation recursively rejects unresolved binds, bound identity/list constructs, and sentinels nested in action payloads.

Prepared coordinated unpublished identities are `@trybotster/ui-contract@0.3.1`, `@trybotster/hub-test-support@0.1.20`, and conformance fixture revision `27`; publication remains a manual post-merge operator action.

## Ownership and cross-repository routing

The change stays within the Hub charter: renderer-neutral UI contract, Hub admission/transport, Hub-owned fixtures, generated package assets, and test-support reference materializers. It does not change Core, TUI, TUI Kit, Web, Workspaces, renderer/input behavior, or Project Pipelines source.

No new cross-repository prerequisite was discovered. The existing downstream TUI run owns the post-merge repin and production renderer proof; Web owns its eventual repin. The locked Core dependency was compiled without source changes. No compatibility branch or client workaround was added.

## Deviations from the approved plan

- No semantic deviation.
- Generated field-specific JSON Schema and TypeScript aliases were necessary to express the approved matrix in published artifacts; no shared Rust schema-metadata refactor was introduced.
- The live daemon assertion records the exact source value `current`; capitalization would be presentation behavior outside this contract.
- A clean npm consumer needed a task-local npm cache because the user cache was not writable in the sandbox. Both tarballs were then installed offline and tested together.

## Verification and downstream proof

- Red-before proof: `./test.sh -p botster-ui-contract authored_button_accepts_required_bound_label -- --exact` failed with `Button missing required label` before the validator change.
- Focused contract suites: `./test.sh -p botster-ui-contract` passed (3 generated-asset tests and 81 semantic tests).
- Test-support suites: `./test.sh -p botster-hub-test-support` passed (42 tests and 3 doctests).
- Hub runtime admission tests passed for a valid required bound label, malformed required label rejection, and action replacement binding admission.
- `./test.sh --test hub_client_api_test` passed (24 tests).
- `./test.sh --test hub_daemon_lifecycle_test daemon_plugin_contract_matrix_fixture_exercises_public_package_contracts` passed through a real isolated Hub and locked session worker, including the realized `current` Button label.
- `cargo build --locked -p botster-core --bin botster-session-worker` passed against Core `5846fc776d31e2b6c98a8d932f50a31078743901`.
- `cargo fmt --all -- --check` passed.
- `cargo clippy --workspace --all-targets --all-features -- -D warnings` passed.
- `./test.sh` passed: all repository tests green; one documented adversarial test remained ignored by design.
- UI package `run check` and `test` passed; Hub test-support `run check` and `test` passed.
- `npm pack --dry-run --json` passed for both packages and included expected declarations, schema, conformance fixtures, metadata, fixtures, and licenses.
- Actual `0.3.1` and `0.1.20` tarballs installed together in a clean temporary consumer. The smoke test verified exact versions/dependency/revision, generated schema and declarations, checksums, licenses, realized label `current`, and absence of unresolved `$bind` values.
- Live registry collision check found published UI versions through `0.2.0` and Hub test-support through `0.1.18`; the prepared identities do not collide.

## Residual risk and vault guidance

- Unverified by this repository: production TUI/Web renderer consumption after their normal-registry repins. That work remains explicitly downstream and separately routed.
- Packages were packed and consumed locally but were intentionally not published.
- The vault did not yet state the confirmed reusable rule that required UiNode bindability is field-explicit, literal semantics can differ by field, and authored versus realized validation differs only in whether unresolved sentinels are allowed. This is a durable capture candidate after merge; it was not written into the external vault from this restricted run worktree.
