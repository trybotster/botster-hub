# Implement report: Hub project session state without blocking operation paths

## Target

- Repository: `botster-hub`
- `target_id`: `tgt_7e208a0c76a44980a83b63af976b1f22`
- Ticket: `ticket_1786663582_169720`
- Run: `run_1786689005_381068`
- Approved plan: `docs/plans/project-session-state-without-blocking-operation-paths.md` (`c4fad52`)
- `teardown_class_applies`: no
- Delivery: direct-merge, no pull request

## Playbooks and notes applied

- `implementer-playbook`
- `botster-implementer-playbook`
- `botster-hub-playbook`
- `Hub owner loop calls bounded Core lifecycle page APIs`
- `Hub session projection continues without subscribers or terminal Drain`
- `test script required for rust tests not cargo test`
- `implement gate must verify committed work and pr link before review`
- `plugin worker queue capacity and executor concurrency are independent host profile knobs`
- `worker isolation now has a Core try-admit non-blocking primitive`
- `live hub proof records distinct hub and locked core binary provenance`
- `botster session worker requires explicit build in dogfood launchers`
- `rust repo strict lints must be verified before dismissing warnings`
- `pipeline vault checklists must cite exact resolvable note titles`

Ambient SessionStart mapped rails/general. This run used the ticket target `botster-hub`, not those rails conventions.

## Files changed

- `Cargo.toml`, `Cargo.lock`, `crates/botster-hub-client/Cargo.toml`, `crates/botster-hub-test-support/Cargo.toml`
- `src/session_projection.rs` (new)
- `src/daemon_maintenance.rs` (new)
- `src/config.rs`, `src/runtime.rs`, `src/lifecycle.rs`, `src/lib.rs`
- `src/daemon_transport.rs`, `src/daemon_entity_subscriptions.rs`
- `tests/session_projection_owner_loop.rs` (new)
- `tests/hub_daemon_lifecycle/sessions.rs`, `tests/hub_daemon_lifecycle/plugin_bounds.rs`, `tests/hub_plugin_lifecycle_test.rs`
- `docs/client-protocol.md`, `docs/lua-plugin-abi.md`
- `docs/plans/project-session-state-without-blocking-operation-paths.md`

## Ownership boundaries preserved

Hub remains the first-party host profile over Core. This change owns host knobs, owner-loop slices, the in-memory session projection, and `/session` host-bridge admission. It does not own Core page APIs, terminal bytes, Workspaces/membership policy, or client DTO variants.

## Cross-repo routing

No new cross-repo work. Consumed closed Core dependencies at exact revision `aef6516d5809d563961ed7fdd07da29a7b4edddc`. Client frames stay `entity_snapshot` / upsert / patch / remove, so Web and TUI checkouts were not required.

## Deviations from plan

1. Complete observe slices do not `try_wake`. Incomplete observe still wakes to continue the pass.
2. Read-only control replies do not `try_wake`. Spawn/Resize/Shutdown/Remove still prefer journal. Plugin surface actions wake. Pending package-entity resync keeps the loop awake until degraded or converged.
3. Attached natural-exit Drain may surface `SessionLifecycle` exited instead of `ProcessExit`. The entity patch remains the lifecycle oracle. The committed plan item 12 records this.

## Tests and downstream proof

Ran from this checkout with one local target and no `CARGO_TARGET_DIR`:

1. `cargo fmt --all -- --check`
2. `cargo clippy --workspace --all-targets --locked -- -D warnings`
3. `cargo build --locked -p botster-core-daemon --bin botster-session-worker`
4. `./test.sh --locked` — `hub_daemon_lifecycle_test` 203 passed, 1 ignored
5. `cargo test --doc --workspace --locked`

Production path: `serve_daemon` → at most one ready control → one `run_one_owner_maintenance_slice` → `observe_lifecycle_slice` / `lifecycle_baseline_page`. Operation handlers no longer call `drive_entity_subscriptions`.

## Provenance

Recorded after the implement commit. Hub SHA and lockfile Core SHA are distinct. Both binaries resolve under this checkout target:

- Hub checkout: `3a1e5e01cb579c6f044ec4a653b3d24b368d2b2b`
- Core lock: `aef6516d5809d563961ed7fdd07da29a7b4edddc`
- `target/debug/botster-hub`
- `target/debug/botster-session-worker`

## Unverified behavior or residual risk

- Core observe of an attached worker session may omit `ProcessExit` on later unbound Drain. Hub does not invent that frame.
- Ready-operation wait was proven by `ready_spawn_stays_within_budget_when_live_sessions_exceed_one_observe_slice`. Live multi-hour soak was not run.
- Duplicate Implement checklist `checklist_1786752486_668953` was left unused. Evidence is on `checklist_1786754098_916716`.

## Missing vault guidance

No charter note said Status must not wake the owner loop. Captured as `hub-owner-loop-must-not-wake-on-read-only-control-requests`. No charter note said observe-first attached Drain may lack `ProcessExit`. Captured as `observe-first-attached-drain-may-surface-session-lifecycle-not-process-exit`.
