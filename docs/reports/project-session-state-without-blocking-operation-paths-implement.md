# Implement report: Hub project session state without blocking operation paths

## Target

- Repository: `botster-hub`
- `target_id`: `tgt_7e208a0c76a44980a83b63af976b1f22`
- Ticket: `ticket_1786663582_169720`
- Run: `run_1786689005_381068`
- Implement step: `run_step_1786762655_499213`
- Review return: `review_1786762634_128047`
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

This Review-return plus main merge:

- `src/daemon_entity_subscriptions.rs`
- `src/daemon_maintenance.rs`
- `src/config.rs` (unchanged this pass; external construct test added)
- `docs/client-protocol.md`
- `tests/external_core_engine_options_construct.rs` (new)
- `tests/session_projection_owner_loop.rs`
- `crates/botster-hub-test-support/src/lib.rs`
- `crates/botster-hub-test-support/src/conformance_data.rs`
- `crates/botster-hub-test-support/build.rs`
- `crates/botster-hub-test-support/Cargo.toml`
- `Cargo.toml`, `crates/botster-hub-client/Cargo.toml`, `Cargo.lock`
- `docs/reports/project-session-state-without-blocking-operation-paths-implement.md`

Merged `origin/main` at `959c58f` (late-attach Core fixture consume). Merge commit `cd57148`.

## Ownership boundaries preserved

Hub owns owner-loop slices, the session projection, session subscriber delivery, and `/session` host-bridge admission. Core still owns page APIs. Terminal bytes, Workspaces/membership, and client DTO variants are unchanged.

## Cross-repo routing

No Web or TUI checkout. The first session snapshot is populated again, so existing clients that replace-all on `entity_snapshot` keep a stable-id-ordered baseline. Remaining rows after a full page arrive as upserts.

## Deviations from plan

1. `origin/main` pins Core `f4f6bf5` (13 Aug). That revision does not expose `observe_lifecycle_slice`, `lifecycle_baseline_page`, `lifecycle_changes_page`, or `take_journal_advanced_wake`. This ticket keeps one Git-visible pin at `aef6516` (14 Aug), which includes those APIs and the Core-owned late-attach fixture files that main now consumes through `build.rs`.
2. Supported `CoreEngineOptions` construction is `..CoreEngineOptions::default()`. Exhaustive external struct literals are not a supported seam after field additions. Proven by `tests/external_core_engine_options_construct.rs`.

## Tests and downstream proof

Ran from this checkout with one local target and no `CARGO_TARGET_DIR`:

1. `cargo fmt --all`
2. `cargo clippy --workspace --all-targets --offline -- -D warnings`
3. `cargo build --offline -p botster-core-daemon --bin botster-session-worker`
4. `./test.sh --offline` — `hub_daemon_lifecycle_test` 203 passed, 1 ignored
5. Doc tests ran as part of `./test.sh --offline`

## Provenance

- Core lock: `aef6516d5809d563961ed7fdd07da29a7b4edddc`
- Merged main: `959c58f55726d098299cced8af151d8f496f41e3`
- `target/debug/botster-hub`
- `target/debug/botster-session-worker`

## Unverified behavior or residual risk

- A 256-process live daemon was not started. Large-registry proofs remain unit tests.
- Plugin fanout pages eight consumers per turn and continues with a pending frame. A live many-plugin soak was not run.
- Web and TUI were not rebuilt. They already consume snapshot then upsert/patch/remove.

## Missing vault guidance

None new. Main's older Core pin lacks the sliced lifecycle APIs. This report records that integration decision.

## Review findings addressed

- `finding_1786762634_705484`: BTreeMap range cursors for rows and removes. Encoded-frame check against remaining byte budget. Plugin completion lookup uses a request-id index. Consumer fanout, refresh, and snapshot start use bounded key pages.
- `finding_1786762634_243665`: First snapshot is populated and stable-id-ordered. Protocol text updated. Remaining rows after the page arrive as upserts.
- `finding_1786762635_623875`: Overflow resync increments the per-connection sequence. It does not reset to the Core cursor.
- `finding_1786762635_184244`: Merged current `origin/main`. Kept `aef6516` because `f4f6bf5` lacks the sliced APIs.
- `finding_1786762635_361949`: External-crate construct test using `..Default::default()`.
