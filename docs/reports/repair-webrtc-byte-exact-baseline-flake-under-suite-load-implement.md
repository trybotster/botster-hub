# Implement report: repair the WebRTC byte-exact baseline flake under suite load

| Field | Value |
| --- | --- |
| Ticket | `ticket_1787999248_674913` |
| Run | `run_1787999630_574649` |
| Step | `botster_stack_implement` |
| Target repository | `botster-hub` (`trybotster/botster-hub`) |
| `target_id` | `tgt_7e208a0c76a44980a83b63af976b1f22` |
| Authoritative path | run `target_id` plus Plan Review spawn-target mapping; the process working directory did not select it |
| Pipeline worktree | the ticket worktree on `project-pipelines/ticket_1787999248_674913` |
| Plan | `docs/plans/repair-webrtc-byte-exact-baseline-flake-under-suite-load.md` |
| Delivery | direct-merge; no pull request |
| Class | runtime-teardown does not apply |
| Locked Core | `Cargo.lock` pins `botster-core` / `botster-core-daemon` at `7eafa470a18025895995bbedc20d34b58106a03b` |
| Toolchain | `RUSTUP_TOOLCHAIN=1.97.0`; `rustc 1.97.0 (2d8144b78 2026-07-07)`; `CARGO_TARGET_DIR` unset |

Independent routing: `project_pipelines_current_context` maps `tgt_7e208a0c76a44980a83b63af976b1f22` to this Hub run. The approved plan uses the same target and repository. Work stayed in the ticket worktree.

## Repository playbook and other playbooks/notes applied

### Playbooks

- [[implementer-playbook]]
- [[botster-implementer-playbook]]
- [[botster-hub-playbook]]

### Targeted atomic notes

- [[botster runtime teardown lenses]]
- [[wall-clock ready-operation bounds through a daemon child are ambient-load-sensitive]]
- [[host exhaustion markers identify each failed test]]
- [[a suite-load oracle must not demand more than the host contract another test in the same file already codifies]]
- [[a regression test must be shown to go red with the fix reverted]]
- [[test names do not prove their bodies can fail on the named claim]]
- [[exact Rust test ablations require a one test baseline]]
- [[Hub official gates must not set CARGO TARGET DIR]]
- [[Hub suite runs prebuild the session worker before the locked test wrapper]]
- [[release file gated producers flush readiness before release]]
- [[implement gate must verify committed work and pr link before review]]
- [[implementation artifacts must match actual git state]]
- [[implementation steps must persist report artifacts for review]]
- [[pipeline vault checklists must cite exact resolvable note titles]]
- [[test script required for rust tests not cargo test]]
- [[botster Hub pipeline shells can override RUSTUP TOOLCHAIN below the CI pin]]

**Not loaded:** [[project-pipelines-playbook]] — Project Pipelines package/plugin paths and workflow-policy implementation are out of scope. Other repository charters were not loaded.

### Constraints applied before edits

- Work only in this `botster-hub` ticket worktree.
- Follow the approved plan. Change one test body. Change no production WebRTC, adapter, Core, client, or hub-test-support path.
- Runtime-teardown class does not apply. Do not implement the teardown lenses.
- Keep the test name and the byte-exactness assertion text unchanged.
- Emit `harness_budget_expired` only when zero post-release bytes arrived.
- Use `./test.sh --locked`. Do not use bare `cargo test`.
- Official gates use `RUSTUP_TOOLCHAIN=1.97.0` and leave `CARGO_TARGET_DIR` unset.
- Direct merge. Do not create a pull request.

## Files changed

Feature behavior:

- `tests/hub_daemon_lifecycle/subscription_ownership_baseline.rs` — `webrtc_terminal_output_is_byte_exact` now waits for Unix `producer-ready`, drains that marker off the WebRTC route, writes the release file, then completes on the expected byte window or on observed session exit plus eight quiet drain turns. An empty post-release window at the 30 s backstop panics through `format_harness_budget_expired`.

Handoff:

- `docs/plans/repair-webrtc-byte-exact-baseline-flake-under-suite-load.md` — Implement measurement binding.
- `docs/reports/repair-webrtc-byte-exact-baseline-flake-under-suite-load-implement.md` — this report.

Production source files: none.

## Ownership boundaries preserved

Hub owns this lifecycle test and the host-exhaustion marker format. The change stays inside Hub test code. Core still owns terminal bytes and session lifecycle. `botster-hub-client` still owns the DTOs the test decodes. No DTO, protocol, adapter, or production WebRTC path changed.

## Cross-repo routing

None. No dependency ticket is required. Hub decomposition 4a (`ticket_1787894421_128594`) remains a blocked consumer of a clean baseline; this run does not change that ticket.

## Deviations from plan

No scope change. Implement recorded a measurement binding in the committed plan:

1. Unix `wait_for_producer_ready` is not enough to keep `concatenated` empty. The isolated run showed 20 WebRTC bytes that included `producer-ready` before session exit. Implement drains until WebRTC shows `PRODUCER_READY_MARKER`, then clears `concatenated` before the release-file write so the starvation marker stays exclusive to an empty post-release window.
2. File-local outer backstop is 30 s, not the old eight-second product gate. The backstop only panics when post-release `concatenated` is empty.
3. Quiet drain after exit is eight turns. Isolation matched the expected window before exit (14.6 ms), so those turns are a load drain, not the success path.

No shared harness constant was raised. The byte-exactness assertion text and the test name are unchanged.

## Review return (`review_1788002895_413318`)

Review submitted `changes_required` with high finding `finding_1788002895_328395`: the readiness backstop could `break` into the release-file write when WebRTC had bytes but not the complete `PRODUCER_READY_MARKER`. That contaminates the post-release window and can fail the byte assertion instead of emitting `harness_budget_expired`.

Fix: the readiness loop leaves only when `concatenated` contains `PRODUCER_READY_MARKER`. Backstop expiry always panics through `format_harness_budget_expired`, including the non-empty incomplete-marker case. The post-release empty-only marker lane is unchanged.

Partial-marker ablation: replaced the readiness `contains(PRODUCER_READY_MARKER)` check with an impossible string and set the file-local backstop to 2 s. Result: exit 101. Panic: `harness_budget_expired test=webrtc_terminal_output_is_byte_exact ... timed out waiting for complete WebRTC producer-ready marker; concatenated=[112, 114, 111, 100, 117, 99, 101, 114, 45, 114, 101, 97, 100, 121, 13, 10]`. No byte assertion. Source restored.

## Tests and downstream proof run

Prebuild (same shell as the gates):

```text
unset CARGO_TARGET_DIR
RUSTUP_TOOLCHAIN=1.97.0
rustc 1.97.0 (2d8144b78 2026-07-07)
cargo build --locked -p botster-core-daemon --bin botster-session-worker
cargo build --locked --bin botster-hub
```

1. One-test baseline, then green focused rerun:
   `./test.sh --locked --test hub_daemon_lifecycle_test webrtc_terminal_output_is_byte_exact -- --exact --nocapture`
   Result: `ok. 1 passed; 0 failed; 0 ignored; 0 measured; 320 filtered out`.
2. Official locked suite on a normally loaded host:
   `./test.sh --locked`
   Result: exit 0. Lifecycle crate `ok. 319 passed; 0 failed; 2 ignored` in 266.77 s. `test webrtc_terminal_output_is_byte_exact ... ok`.
3. Product red-on-revert: producer wrote `&[0x00, 0x1b, 0xff, 0xc1]` while the assertion still expected `&[0x00, 0x1b, 0xff, 0xc0]`.
   Result: exit 101. Panic: `WebRTC adapter frames must preserve exact bytes, got [0, 27, 255, 193]`. No `harness_budget_expired`. Source restored.
4. Precondition arm: removed Unix `wait_for_producer_ready` and the WebRTC producer-ready drain, then wrote the release file immediately.
   Result: isolation still `ok. 1 passed`. The waits are a precondition, not the proof. Source restored.
5. Marker arm: skipped the release-file write.
   Result: exit 101. Panic: `harness_budget_expired test=webrtc_terminal_output_is_byte_exact kind=webrtc_byte_exact budget_ms=30000 resource=ETIMEDOUT probe=unconfirmed timed out waiting for WebRTC adapter frames after producer-ready release; concatenated is empty`. Source restored.
6. Strict gates: `cargo fmt --all -- --check` exit 0. `cargo clippy --workspace --all-targets --locked -- -D warnings` exit 0. `cargo clippy --locked --test hub_daemon_lifecycle_test -- -D warnings` exit 0.
7. Review-return focused rerun of the same exact filter: `ok. 1 passed; 320 filtered out`.
8. Partial-marker ablation for `finding_1788002895_328395`, described in the Review return section. Source restored.

Downstream proof: none required. Hub-internal test code.

Production-path note: this ticket is intentionally test-only. The production WebRTC delivery path is unchanged.

## Unverified behavior or residual risk

- Quiet drain after exit was not taken in isolation. The full suite passed without a marker, but Implement did not observe exit-before-last-frame under this host load.
- The 30 s backstop can still fire on a genuinely exhausted host. That outcome is acceptable only with the named marker.
- Sibling `Duration::from_secs(8)` wait sites remain out of scope.
- `AUTHORITATIVE_SESSION_EXIT_WAIT` did not expire here. A later suite-load expiry of that shared Unix wait would need its own ticket.
- The incomplete-readiness timeout path is proven by ablation, not by a permanent suite test. A permanent 30 s negative test would add suite time without strengthening the byte claim.

## Missing vault guidance discovered

Captured to vault inbox for later processing:

- `live-byte-delivery-proofs-need-producer-ready-and-a-completion-oracle.md`
- `webrtc-starvation-markers-must-drop-pre-release-producer-ready-bytes.md`

Existing wall-clock notes cover owner-turn budgets and ready-operation ordering, not live byte delivery through a PTY producer. The repository-wide eight-second wait constant remains a follow-up candidate, not a note written as if this run swept those sites.
