# Implement report: Hub project session state without blocking operation paths

## Target

- Repository: `botster-hub`
- `target_id`: `tgt_7e208a0c76a44980a83b63af976b1f22`
- Ticket: `ticket_1786663582_169720`
- Run: `run_1786689005_381068`
- Implement step: `run_step_1786764465_328930`
- Review return: `review_1786764453_301288`
- Approved plan: `docs/plans/project-session-state-without-blocking-operation-paths.md` (`c4fad52`)
- `teardown_class_applies`: no
- Delivery: direct-merge, no pull request
- Implement commit: `9f1f1c49d1cd0c38779df7d26e4e7422baf5edf8`

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

This Review-return:

- `src/daemon_maintenance.rs`
- `src/lifecycle.rs`
- `src/runtime.rs`
- `src/daemon_entity_subscriptions.rs`
- `src/config.rs`
- `docs/client-protocol.md`
- `tests/external_core_engine_options_construct.rs`
- `docs/reports/project-session-state-without-blocking-operation-paths-implement.md`

## Ownership boundaries preserved

Hub owns owner-loop slices, the session projection, session subscriber delivery, and `/session` host-bridge admission. Core still owns page APIs. Terminal bytes, Workspaces/membership, and client DTO variants are unchanged.

## Cross-repo routing

No Web or TUI checkout. The first session snapshot is one complete replace-all `entity_snapshot`. Existing clients that treat that frame as the sole baseline keep a stable-id-ordered set. If the encoded frame exceeds the daemon frame limit, Hub returns `entity_provider_frame_too_large`.

## Deviations from plan

1. `origin/main` pins Core `f4f6bf5` (13 Aug). That revision does not expose `observe_lifecycle_slice`, `lifecycle_baseline_page`, `lifecycle_changes_page`, or `take_journal_advanced_wake`. This ticket keeps one Git-visible pin at `aef6516` (14 Aug).
2. Supported `CoreEngineOptions` construction is `CoreEngineOptions::new(...)`. New worker-queue knobs use defaults. Exhaustive external struct literals are not a supported seam. Proven by `tests/external_core_engine_options_construct.rs`.

## Tests and downstream proof

Ran from this checkout with one local target and no `CARGO_TARGET_DIR`:

1. `cargo fmt --all`
2. `cargo clippy --workspace --all-targets --locked --offline -- -D warnings`
3. `cargo build --locked --offline -p botster-core-daemon --bin botster-session-worker`
4. `./test.sh --locked --offline`
5. Doc tests ran as part of `./test.sh --locked --offline`

The first full-suite run failed `webrtc_terminal_adapter_write_budget_emits_core_adapter_closed_while_peer_stays_readable`. The same command passed in isolation (`--test-threads=1`). A second `./test.sh --locked --offline` passed. This path was not edited.

## Provenance

- Core lock: `aef6516d5809d563961ed7fdd07da29a7b4edddc`
- Merged main: `959c58f55726d098299cced8af151d8f496f41e3`
- `target/debug/botster-hub`
- `target/debug/botster-session-worker`

## Unverified behavior or residual risk

- A 256-process live daemon was not started. Large-registry and many-plugin proofs remain unit tests.
- Web and TUI were not rebuilt. They already consume a complete first `entity_snapshot`.
- The WebRTC write-budget sibling-output check can fail under parallel suite load and then pass in isolation.

## Missing vault guidance

None new. Main's older Core pin lacks the sliced lifecycle APIs. This report records that integration decision.

## Review findings addressed

- `finding_1786764454_235298`: `needs_work` uses an O(1) busy count plus pending fanout/gap/snapshot flags. Handler refresh and admission use a paged API and a persistent cursor. A 64-plugin unit test stays inside `MAX_OWNER_TURN_MS`.
- `finding_1786764454_584341`: Each lifecycle frame is a `FanoutJob` with its own consumer cursor. HostBridge resumes gap, snapshot start, and fanout without waiting for another journal change. Tests cover 20 consumers, two ordered deltas, and a baseline restart.
- `finding_1786764454_439175`: The first snapshot is assembled off the control path and sent as one complete replace-all frame. Protocol text no longer describes a partial page plus upserts.
- `finding_1786764454_332872`: `CoreEngineOptions::new` is the supported external constructor. The external-crate test uses that path.
