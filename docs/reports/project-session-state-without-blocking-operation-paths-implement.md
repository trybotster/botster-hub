# Implement report: Hub project session state without blocking operation paths

## Target

- Repository: `botster-hub`
- `target_id`: `tgt_7e208a0c76a44980a83b63af976b1f22`
- Ticket: `ticket_1786663582_169720`
- Run: `run_1786689005_381068`
- Implement step: `run_step_1786772830_861058`
- Review return: `review_1786772819_486857`
- Approved plan: `docs/plans/project-session-state-without-blocking-operation-paths.md` (`c4fad52`)
- `teardown_class_applies`: no
- Delivery: direct-merge, no pull request

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
- `src/config.rs`
- `src/lib.rs`
- `tests/external_core_engine_options_construct.rs`
- `tests/hub_plugin_lifecycle_test.rs`
- `tests/hub_daemon_lifecycle/plugin_bounds.rs`
- `tests/hub_daemon_lifecycle/sessions.rs`
- `docs/reports/project-session-state-without-blocking-operation-paths-implement.md`

## Ownership boundaries preserved

Hub owns owner-loop slices, the session projection, session subscriber delivery, and `/session` host-bridge admission. Core still owns page APIs. Terminal bytes, Workspaces/membership, and client DTO variants are unchanged.

## Cross-repo routing

No Web or TUI checkout. The first session snapshot remains one complete replace-all `entity_snapshot`. Class knobs are Core defaults. `PluginWorkerClassOptions` is no longer a public type.

## Deviations from plan

1. `origin/main` pins Core `f4f6bf5` (13 Aug). That revision does not expose the sliced lifecycle APIs. This ticket keeps one Git-visible pin at `aef6516` (14 Aug).
2. Plugin-worker class knobs are not public configuration. Hub maps Core defaults. Queue capacity and executor concurrency remain on `CoreEngineOptions`.

## Tests and downstream proof

Ran from this checkout with one local target and no `CARGO_TARGET_DIR`:

1. `cargo fmt --all`
2. `cargo clippy --workspace --all-targets --locked --offline -- -D warnings`
3. `cargo build --locked --offline -p botster-core-daemon --bin botster-session-worker`
4. `./test.sh --locked --offline`

## Provenance

- Core lock: `aef6516d5809d563961ed7fdd07da29a7b4edddc`
- Merged main: `959c58f55726d098299cced8af151d8f496f41e3`
- Parent implement: `672a1a387b4c69b5f8f78d61e8a89d48a0dee439`
- `target/debug/botster-hub`
- `target/debug/botster-session-worker`

## Unverified behavior or residual risk

- A 256-process live daemon was not started. Near-limit assembly proofs are unit tests plus a 24-session production ready-spawn wait.
- Web and TUI were not rebuilt. They already treat the first `entity_snapshot` as replace-all.
- The last assemble page moves the incrementally built `Vec<Value>` onto the subscriber channel. Socket serialization stays on the connection task.

## Missing vault guidance

None new.

## Review findings addressed

- `finding_1786772819_298072`: Pages append already-encoded JSON values. The last page moves that vec into the snapshot frame. It does not walk `state.entities` or call `to_value` again. Near-limit pages stay inside `MAX_OWNER_TURN_MS` and `MAX_READY_OPERATION_WAIT_MS`. A production subscribe/spawn path measures ready-operation wait during assembly.
- `finding_1786772819_643241`: The size counter includes JSON array commas. A boundary test closes when item bytes plus the envelope fit but separator bytes do not.
- `finding_1786772819_378990`: Hub startup validates Core default reservations against `plugin_worker_executor_concurrency`. Concurrency `1` returns `InvalidPluginWorkerReservation`.
- `finding_1786772819_962504`: `PluginWorkerClassOptions` is removed from the public surface. Production maps `PluginWorkerEngineConfig::default()`.
