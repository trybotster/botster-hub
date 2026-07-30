# Bindable BindList Row Identity Implementation Report

## Routing and scope

- Ticket: `ticket_1785436979_640117`
- Run: `run_1785436979_236604`
- Target repository: `trybotster/botster-hub`
- Target ID: `tgt_7e208a0c76a44980a83b63af976b1f22`
- Run worktree: `project-pipelines/ticket_1785436979_640117`
- Approved plan: `docs/plans/make-bind-list-row-identity-bindable-before-expansion.md`
- Approved plan tip after rebasing onto `origin/main`: `fd0a418b102ec520195ac78459d78a6ac78d82e2`

The implementation assumes that “bindable before expansion” means authored
`UiNode.id` may be an item-relative binding only on the direct
`BindList.item_template` root. Realized action request/result `node_id` values
remain strings. This is the approved plan's explicit interpretation; no
alternative static-root or recursive-template interpretation was selected.

## Guidance applied

- Role playbooks: `implementer-playbook`, `botster-implementer-playbook`
- Repository charter: `botster-hub-playbook`
- Surface overlays: `botster-hub-client-playbook`,
  `botster-package-reviewer-playbook`, `botster-package-verifier-playbook`
- Targeted notes: row identity, dynamic lists and exact `where` filters,
  empty-template behavior, typed-template binding gotchas, package-surface
  ownership, declared-operation admission, canonical fixture mirrors,
  conformance revision uniqueness, exact live proof/provenance, and the
  repository-owned test wrapper
- Personal conventions: `self/identity.md`, `self/goals.md`

`project-pipelines-playbook` was not loaded because no Project Pipelines
package/plugin source or workflow-policy implementation is in scope. Pipeline
checklist and gate APIs are workflow evidence, not source ownership.

## Implementation

The Rust authority now distinguishes authored node identity from realized
identity with `UiAuthoredNodeId`. Literal IDs continue to deserialize and
serialize unchanged. Bound IDs validate only in the direct BindList item-root
context, must use a valid item-relative `@/...` path, and are rejected at
detached roots, static descendants, empty templates, and action-result
replacement roots/static children. Action request/result identity remains
`UiNodeId`.

Generated schema, TypeScript, JavaScript metadata, fixtures, documentation, and
versions advance `@trybotster/ui-contract` to `0.2.0`. The Hub-owned canonical
plugin fixture adds a current-session BindList whose Button binds both its ID
and action payload to `@/session_uuid`. Its manifest admits render and action.
The fixture action handler echoes the selected session.

Rust and Node test support preserve existing lifecycle materializer signatures
and add strict row materializers. They reject missing, duplicate, mutated, and
extra oracle shapes; preserve producer order; reject blank/non-string/duplicate
realized IDs; and produce the bound action payload. Conformance revision 25
contains two initial current rows and one row after each later lifecycle stage.

The real conformance runner renders the Lua fixture, materializes the
producer-backed second row, dispatches `PluginSurfaceAction` through the
daemon/client path and `admit_plugin_surface_operation`, and records the
accepted result plus echoed identity and payload.

## Files changed

- Contract authority and generated assets:
  `crates/botster-ui-contract/**`, `packages/ui-contract/**`, `Cargo.lock`
- Hub admission and runtime proof:
  `src/runtime.rs`, `tests/hub_lua_runtime_test.rs`,
  `tests/hub_daemon_lifecycle_test.rs`
- Canonical producer fixture and mirrors:
  `fixtures/plugins/plugin-contract-matrix/**`,
  `crates/botster-hub-test-support/fixtures/plugin-contract-matrix/**`,
  `packages/hub-test-support/fixtures/plugin-contract-matrix/**`
- Rust/Node published support:
  `crates/botster-hub-test-support/**`,
  `packages/hub-test-support/**`,
  `crates/botster-hub-client/src/lib.rs`
- Documentation: `README.md`, `docs/client-protocol.md`, this report

## Ownership boundaries

Hub-owned contract, admission, fixture, client descriptor, and test-support
surfaces are the only source surfaces changed. No Botster Core, TUI, TUI Kit,
Web, Workspaces, terminal, or Project Pipelines source was edited. Core was
consumed at the lockfile pin
`5846fc776d31e2b6c98a8d932f50a31078743901`.

The separately routed downstream renderer work is
`ticket_1785438029_926883`. That ticket must repin the merged and publicly
available Hub packages and prove expansion, interaction, and duplicate-ID
rejection without a path override. Review proved that the public
`UiNode.id` type change is source-breaking for TUI Kit even though its
validation semantics remain compatible. The required kit repin and literal-id
call-site adaptation is `ticket_1785443243_233047`, and the TUI ticket depends
on it through `dependency_1785443246_930722`.

Direct item-template-root identity is the intentional ceiling of this contract
revision. Multi-control descendant identity is separately routed as Hub ticket
`ticket_1785443253_376782`.

## Verification

Passed:

- `./test.sh -p botster-ui-contract` — 77 tests
- `npm --prefix packages/ui-contract run check`
- `npm --prefix packages/ui-contract test`
- `npm --prefix packages/hub-test-support run check`
- `script/publish-npm-packages --dry-run` — installed the exact UI tarball
  into Hub test support, passed its Node test, and packed both artifacts
- `cargo build --locked -p botster-core --bin botster-session-worker`
- `./test.sh --test hub_client_api_test` — 24 tests
- `./test.sh --test hub_lua_runtime_test` — 21 tests
- `./test.sh --test hub_daemon_lifecycle_test daemon_plugin_contract_matrix_fixture_exercises_public_package_contracts -- --exact --nocapture`
- `cargo fmt --all -- --check`
- `cargo clippy --workspace --all-targets --all-features -- -D warnings`
- `./test.sh` — full Hub suite passed, with one documented ignored
  adversarial test
- `git diff --check`

`./test.sh -p botster-hub-test-support` passes all 40 contract/materializer and
asset tests but reports two unrelated lifecycle timing failures:
`start_reports_daemon_exit_before_readiness` and
`start_timeout_cleans_up_unready_child`. Both fail identically with the same
isolated commands in a clean archive of `origin/main`
`162e8eaf120642eca978971656c03f9b53274cbf`.

Runtime provenance:

- Hub source before the implementation commit:
  `fd0a418b102ec520195ac78459d78a6ac78d82e2`
- Core lock pin:
  `5846fc776d31e2b6c98a8d932f50a31078743901`
- Worker:
  `target/debug/botster-session-worker` in this run worktree
- Hub:
  `target/debug/botster-hub` in this run worktree

## Publication, deviations, and residual risk

Registry preflight found `@trybotster/ui-contract@0.1.1` and
`@trybotster/hub-test-support@0.1.16`; therefore the selected coordinates
`0.2.0` and `0.1.17` are unused.

The clean-tree release dry-run passed for both coordinates. Fresh independent
consumer tarballs had these SHA-256 checksums:

- `@trybotster/ui-contract@0.2.0`:
  `ccca09e17b8d43a2abb921a0d2a35472a1e69f2be03bcec50285745a23360138`
- `@trybotster/hub-test-support@0.1.17`:
  `0bd559e3c8d377be16a28ae072faf0c0bbf31859e522bba509c0b04e3893890c`

A fresh consumer installed exactly those two tarballs. Its runtime smoke loaded
contract version `0.2.0`, schema version `0.2.0`, fixture revision 25, and
materialized `session-transition` plus `session-stable-current` with matching
payloads. TypeScript `7.0.2` under strict NodeNext settings compiled a bound
`UiNode.id` and rejected `{ $bind: "@/session_uuid" }` for
`UiActionRequest.node_id` with `TS2322`.

There are no implementation-scope deviations from the approved plan. Review
corrected two plan/handoff assumptions: the contract type change does require a
separately routed TUI Kit repin, and the live downstream owner is
`ticket_1785438029_926883` rather than its closed predecessor. Public publication is
intentionally deferred until this implementation is reviewed and merged; the
verified operator command is `script/publish-npm-packages`, which publishes UI
contract before Hub test support. Until those packages are public and the
separate downstream ticket repins them, downstream renderer behavior remains
intentionally unverified.

No missing vault guidance was discovered. The existing durable row-identity
note should not be updated yet: its plan requires implementation, exact runtime
proof, tarball proof, and downstream repin before appending verification
evidence.

## Review corrections

Review findings were addressed without crossing the Hub charter:

- The published Rust row materializer now returns an error instead of
  panicking if the canonical lifecycle filter is absent.
- The generated schema and package README state that schema validation cannot
  enforce row context and that the Rust/Hub validator is authoritative.
- The direct item-template-root ceiling is explicit in validation diagnostics
  and public docs, with a negative descendant test.
- Package/root READMEs describe `0.1.17` as prepared but unpublished.
- Stale downstream references now point to live TUI ticket
  `ticket_1785438029_926883`.
- Review-proven TUI Kit source adaptation is separately routed to
  `ticket_1785443243_233047` and blocks the live TUI ticket through
  `dependency_1785443246_930722`.
- Multi-control descendant identity is separately routed to Hub ticket
  `ticket_1785443253_376782`.

After these corrections, the UI Rust/Node suites, targeted Rust support
materializer tests, generated-asset checks, strict workspace clippy, formatting,
and the full Hub suite passed again.
