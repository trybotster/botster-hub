# Implement report: Hub project session state without blocking operation paths

## Target

- Repository: `botster-hub`
- `target_id`: `tgt_7e208a0c76a44980a83b63af976b1f22`
- Ticket: `ticket_1786663582_169720`
- Run: `run_1786689005_381068`
- Implement step: `run_step_1786759582_514565`
- Review return: `review_1786759568_783661`
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
- `test script required for rust tests not cargo test`
- `implement gate must verify committed work and pr link before review`
- `implementation artifacts must match actual git state`
- `implementation steps must persist report artifacts for review`
- `plugin worker queue capacity and executor concurrency are independent host profile knobs`
- `worker isolation now has a Core try-admit non-blocking primitive`
- `live hub proof records distinct hub and locked core binary provenance`
- `rust repo strict lints must be verified before dismissing warnings`
- `pipeline vault checklists must cite exact resolvable note titles`
- `incremental attach snapshot frames require lossless streaming backpressure`

Ambient SessionStart mapped rails/general. This run used the ticket target `botster-hub`, not those rails conventions.

## Files changed

This Review-return commit:

- `src/session_projection.rs`
- `src/daemon_maintenance.rs`
- `src/daemon_entity_subscriptions.rs`
- `src/daemon_attach_stream.rs`
- `src/daemon_transport.rs`
- `src/config.rs`
- `crates/botster-hub-test-support/src/lib.rs`
- `tests/hub_daemon_lifecycle/sessions.rs`
- `tests/hub_daemon_lifecycle/webrtc_proofs.rs`
- `docs/reports/project-session-state-without-blocking-operation-paths-implement.md`

## Ownership boundaries preserved

Hub remains the first-party host profile over Core. This change owns owner-loop slices, the in-memory session projection, session subscriber delivery, and `/session` host-bridge admission. It does not own Core page APIs, terminal bytes, Workspaces/membership policy, or client DTO variants.

## Cross-repo routing

No new cross-repo work. Consumed closed Core dependencies at exact revision `aef6516d5809d563961ed7fdd07da29a7b4edddc`. Client frames stay `entity_snapshot` / upsert / patch / remove. The first session snapshot is now empty. Later rows arrive as paged upserts. Web and TUI checkouts were not required.

## Deviations from plan

1. Complete observe slices do not `try_wake`. Incomplete observe still wakes to continue the pass.
2. Read-only control replies do not `try_wake`. Successful Spawn/Resize/Shutdown/Remove prefer journal.
3. Attached natural-exit Drain may surface `SessionLifecycle` exited instead of `ProcessExit`. The entity patch remains the lifecycle oracle.
4. First Review return: subscriber and plugin snapshot delivery are paged. Incomplete baselines are a gap. Session-family request ids are unique per frame. Failed mutations do not wake.
5. Second Review return: a complete baseline is ingested one Core page at a time. Subscribe and resync send an empty snapshot, then page upserts. Plugin delta queues have item and byte limits. Pressure marks a gap and restarts a snapshot. `snapshot_complete` is set only after `snapshot_end`. Subscriber sequences are monotonic per subscriber. `CoreEngineOptions` stays constructible with `..Default::default()`. `#[non_exhaustive]` was not used because external struct-update syntax cannot construct that type.

## Tests and downstream proof

Ran from this checkout with one local target and no `CARGO_TARGET_DIR`:

1. `cargo fmt --all`
2. `cargo clippy --workspace --all-targets --locked --offline -- -D warnings`
3. `cargo build --locked --offline -p botster-core-daemon --bin botster-session-worker`
4. `./test.sh --locked --offline` — `hub_daemon_lifecycle_test` 203 passed, 1 ignored
5. `cargo test --doc --workspace --locked --offline`

Production path: `serve_daemon` → at most one ready control → one `run_one_owner_maintenance_slice` → `observe_lifecycle_slice` / `lifecycle_baseline_page`. Operation handlers do not call `drive_entity_subscriptions`.

Representative external Hub consumer: `tests/hub_plugin_lifecycle_test.rs` constructs `CoreEngineOptions { ..CoreEngineOptions::default() }`.

## Provenance

Recorded after the implement commit. Hub SHA and lockfile Core SHA are distinct. Both binaries resolve under this checkout target:

- Hub checkout: `db8055b7c5781e0f3442f1730392c74e7f78a80e`
- Core lock: `aef6516d5809d563961ed7fdd07da29a7b4edddc`
- `target/debug/botster-hub`
- `target/debug/botster-session-worker`

## Unverified behavior or residual risk

- Ready-operation wait was proven by `ready_spawn_stays_within_budget_when_live_sessions_exceed_one_observe_slice` with 24 live sessions. A live daemon with hundreds of workers was not started.
- Large-registry delivery and ingest proofs are unit tests with 256 synthetic rows and two subscribers. They measure page size and hang bounds. They do not measure a 256-process production owner turn.
- Plugin-consumer refresh still clones the current handler list from `event_handlers_for`, then applies a cursor of eight handlers. A live many-plugin soak was not run.
- `event_handlers_for` itself still materializes the full handler vector. Paging that Core/Hub lifecycle lock was left out of this return.

## Missing vault guidance

No new missing charter note. The attach Drain batching failure showed that a projection-dirty wake with zero session subscribers can delay socket Drain. The existing note `incremental attach snapshot frames require lossless streaming backpressure` already covers that host-scheduling constraint.

## Review findings addressed

- `finding_1786759568_114473`: ingest each baseline page; empty first snapshot; paged resync and delivery; no attach cleanup on an incomplete baseline; paged plugin-consumer refresh.
- `finding_1786759568_131280`: `snapshot_complete` only after `FamilyFrameKind::End`. Production begin/chunk/end test.
- `finding_1786759568_317425`: remove phase then row phase; monotonic `next_seq`; skip and reverse-id tests.
- `finding_1786759568_777913`: per-consumer item and byte queue limits; pressure restarts a snapshot.
- `finding_1786759568_469692`: `..Default::default()` construction in crate tests and `hub_plugin_lifecycle_test`.
