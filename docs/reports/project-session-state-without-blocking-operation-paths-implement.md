# Implement report: Hub project session state without blocking operation paths

## Target

- Repository: `botster-hub`
- `target_id`: `tgt_7e208a0c76a44980a83b63af976b1f22`
- Ticket: `ticket_1786663582_169720`
- Run: `run_1786689005_381068`
- Implement step: `run_step_1786769047_341144`
- Review return: `review_1786769034_892803`
- Approved plan: `docs/plans/project-session-state-without-blocking-operation-paths.md` (`c4fad52`)
- `teardown_class_applies`: no
- Delivery: direct-merge, no pull request
- Implement commit: `522b30bd266707e87f48072b59434ea1700aac78`

## Playbooks and notes applied

- `implementer-playbook`
- `botster-implementer-playbook`
- `botster-hub-playbook`
- `Hub owner loop calls bounded Core lifecycle page APIs`
- `Hub session projection continues without subscribers or terminal Drain`
- `Hub owner loop wakes only for mutations and pending resync`
- `Hub synchronizes plugin workers with session lifecycle events and a baseline`
- `botster hub events use bounded priority lanes instead of unbounded queue fuses`
- `incremental attach snapshot frames require lossless streaming backpressure`
- `test script required for rust tests not cargo test`
- `implement gate must verify committed work and pr link before review`
- `implementation artifacts must match actual git state`
- `implementation steps must persist report artifacts for review`
- `rust repo strict lints must be verified before dismissing warnings`
- `pipeline vault checklists must cite exact resolvable note titles`

Ambient SessionStart mapped rails/general. This run used the ticket target `botster-hub`.

## Files changed

- `src/daemon_entity_subscriptions.rs`
- `src/daemon_maintenance.rs`
- `src/config.rs`
- `src/runtime.rs`
- `docs/client-protocol.md`
- `tests/external_core_engine_options_construct.rs`
- `tests/hub_plugin_lifecycle_test.rs`
- `tests/hub_daemon_lifecycle/plugin_bounds.rs`
- `docs/reports/project-session-state-without-blocking-operation-paths-implement.md`

## Ownership boundaries preserved

Hub owns owner-loop slices, the session projection, session subscriber delivery, and `/session` host-bridge admission. Core still owns page APIs. Terminal bytes, Workspaces/membership, and client DTO variants are unchanged.

## Cross-repo routing

No Web or TUI checkout. The first session snapshot is again one complete replace-all `entity_snapshot`. Existing clients that replace on that frame keep a complete baseline. Class knobs moved off `CoreEngineOptions` onto `HubStartupOptions` / `HubConfig`.

## Deviations from plan

1. `origin/main` pins Core `f4f6bf5` (13 Aug). That revision does not expose the sliced lifecycle APIs. This ticket keeps one Git-visible pin at `aef6516` (14 Aug).
2. Plugin-worker class knobs are not fields on `CoreEngineOptions`. They live on `HubStartupOptions` and `HubConfig` so the original five-field `CoreEngineOptions` literal still compiles.

## Tests and downstream proof

Ran from this checkout with one local target and no `CARGO_TARGET_DIR`:

1. `cargo fmt --all`
2. `cargo clippy --workspace --all-targets --locked --offline -- -D warnings`
3. `cargo build --locked --offline -p botster-core-daemon --bin botster-session-worker`
4. `./test.sh --locked --offline`

The first full-suite run failed `real_lua_plugin_atomically_ensures_managed_worktree_and_spawns_session`. The same command passed in isolation. A second `./test.sh --locked --offline` passed, including `hub_lua_runtime_test` 32/32. That path was not edited.

## Provenance

- Core lock: `aef6516d5809d563961ed7fdd07da29a7b4edddc`
- Merged main: `959c58f55726d098299cced8af151d8f496f41e3`
- Parent implement: `20d757ff53bee82f41139d3176fab620191ad42d`
- `target/debug/botster-hub`
- `target/debug/botster-session-worker`

## Unverified behavior or residual risk

- A 256-process live daemon was not started. Large-registry proofs remain unit tests.
- Web and TUI were not rebuilt. They already treat the first `entity_snapshot` as replace-all.
- The complete snapshot is assembled in pages and sent only when the last page arrives. A very large encoded snapshot still closes with `entity_provider_frame_too_large`.

## Missing vault guidance

None new.

## Review findings addressed

- `finding_1786769034_335845`: Assembly stays in `Assembling { source_seq }` until the complete snapshot is sent. A projection sequence change clears assembled rows and restarts. Test patches a prefix ID during catch-up.
- `finding_1786769034_813657`: HostBridge tracks visits, bytes, and elapsed time. Admission peeks a payload, charges bytes, then commits. A rejected byte budget leaves the pending queue unchanged.
- `finding_1786769034_381740`: The first client frame is again one complete replace-all snapshot. Protocol text matches that contract. Web and TUI already consume that frame.
- `finding_1786769034_209655`: `CoreEngineOptions` has the original five public fields. The external-crate test uses that exhaustive literal.
