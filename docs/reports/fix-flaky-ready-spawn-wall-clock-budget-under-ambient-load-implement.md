# Implement report: fix flaky ready_spawn wall-clock MAX_READY_OPERATION_WAIT_MS budget tests

| Field | Value |
| --- | --- |
| Ticket | `ticket_1786938984_190098` |
| Run | `run_1786944939_873939` |
| Step | `botster_stack_implement` |
| Target repository | `botster-hub` (`trybotster/botster-hub`) |
| `target_id` | `tgt_7e208a0c76a44980a83b63af976b1f22` |
| Authoritative path | ticket `target_id` plus worktree `origin` remote `https://github.com/trybotster/botster-hub.git` |
| Pipeline worktree | the ticket worktree on `project-pipelines/ticket_1786938984_190098` |
| Original approved plan commit | `c0f5646` (v3 product repair) |
| First v3.1 plan revision | `c550f1c` (focused-gate amendment for acceptance check 4) |
| This follow-up | v3.3 live-frame oracle after `review_1787006443_517771` |
| Delivery | direct-merge; no pull request |
| Class | not runtime-teardown (`teardown_class_applies: false`) |
| Plan | `docs/plans/fix-flaky-ready-spawn-wall-clock-budget-under-ambient-load.md` revision v3.3 |
| Implement checklist | `checklist_1786943181_130677` (ticket-scoped; no duplicate created) |
| Run status | focused Implement after Review `finding_1787006443_430008` |

Independent routing: `project_pipelines_get_project` lists `tgt_7e208a0c76a44980a83b63af976b1f22` as a registered project target. The ticket worktree `origin` remote is `trybotster/botster-hub`. Approved plan v3 used the same `target_id` and repository.

Durable answer `question_1787003911_553236` chooses option 2 and supersedes the parking instruction in `question_1786977344_650479`. This ticket's Implement and Review gate is focused proof plus known-baseline exact-bytes evidence. `ticket_1786977409_499180` does not block this repair. Both tickets directly block final integration. Do not absorb exact-bytes. Do not start an unfiltered lifecycle suite now.

## Repository playbook and other playbooks/notes applied

### Playbooks

- [[implementer-playbook]]
- [[botster-implementer-playbook]]
- [[botster-hub-playbook]]

### Targeted atomic notes

- [[botster-architecture]]
- [[cli-patterns]]
- [[wall-clock MAX_OWNER_TURN_MS assertions flake under default-concurrency lib load]]
- [[conformance harnesses gate on deterministic invariants not timing]]
- [[Owner loop must not stack maintenance and pump ahead of queued control]]
- [[Hub background fairness must stay policy-neutral]]
- [[a regression test must be shown to go red with the fix reverted]]
- [[test script required for rust tests not cargo test]]
- [[hub daemon runtime stays on one owner thread while socket handlers submit requests]]
- [[implement gate must verify committed work and pr link before review]]
- [[implementation artifacts must match actual git state]]
- [[implementation steps must persist report artifacts for review]]
- [[pipeline artifacts should use path neutral worktree references]]
- [[pipeline vault checklists must cite exact resolvable note titles]]
- [[suite wide acceptance criteria make every observed test failure in scope]]
- [[pre existing failure waivers must isolate the first non cascade failure on base]]
- [[full suite hangs need source and behavior proof before unrelated waivers]]

**Not loaded:** [[project-pipelines-playbook]] as a repository charter overlay. Project Pipelines package and plugin paths are out of scope. Workflow MCP tools were used. [[botster runtime teardown lenses]] was not loaded. Other repository charters were not loaded.

### Constraints applied before edits

- Work only in this `botster-hub` ticket worktree.
- Follow approved plan v3, then the v3.3 oracle revision authorized by `review_1787006443_517771`.
- Extract the busy-path classification with identical arms and order.
- Do not change `MAX_READY_OPERATION_WAIT_MS`, `MAX_OWNER_TURN_MS`, `OBSERVE_SLICE_BUDGET`, owner-loop scheduling, or Pump/Maintenance fairness.
- Do not change public DTOs, `botster-hub-client`, hub-test-support, or downstream pins.
- Do not absorb `ticket_1786937228_425608` or `ticket_1786913892_208903`.
- Prefer repair over quarantine. Do not start from `#[ignore]`.
- Use `./test.sh` for Rust tests. Direct merge. Do not create a pull request.

## Files changed

Feature behavior:

- `src/daemon_transport.rs` — extract `classify_owner_poll` / `OwnerPollDecision` from the owner-loop busy-path `try_recv` match. Arms and order stay: queued control serves first; `slice_due` runs one maintenance slice; otherwise the loop blocks. Add unit test `queued_control_precedes_a_due_maintenance_slice`.
- `tests/hub_daemon_lifecycle/sessions.rs` — rename and repair the two flaky tests. Keep fixtures. Delete the wall-clock `MAX_READY_OPERATION_WAIT_MS` assertion. Keep `waited` as an `eprintln!` observation. Keep the functional Spawn, unsubscribe, and shutdown assertions. After `finding_1787006443_430008`, the snapshot-assembly test requires one live subscription frame and fails immediately on `DaemonEntityFrame::Error`. It does not wait for 24 projected identities.

Handoff:

- `docs/plans/fix-flaky-ready-spawn-wall-clock-budget-under-ambient-load.md` — v3.3 live-frame oracle after `review_1787006443_517771`. Focused-gate contract from `question_1787003911_553236` is unchanged.
- `docs/reports/fix-flaky-ready-spawn-wall-clock-budget-under-ambient-load-implement.md` — this report.

Merge/rebase cleanup: none.

### Old-to-new test-name mapping

| Old name | New name |
| --- | --- |
| `ready_spawn_stays_within_budget_when_live_sessions_exceed_one_observe_slice` | `ready_spawn_completes_when_live_sessions_exceed_one_observe_slice` |
| `ready_spawn_stays_within_budget_during_session_snapshot_assembly` | `ready_spawn_completes_during_session_snapshot_assembly` |

## Ownership boundaries preserved

Hub owns the daemon transport, the owner loop, and this lifecycle suite. The production diff is a structural extraction of the existing busy-path decision. `ServeControl` carries `Box<Option<ControlMessage>>` and `serve_daemon` moves that box into `OwnerEvent::Control`, so queued control still allocates one box. Scheduling behavior, budgets, and public contracts stay unchanged. Core, hub-client, Web, TUI, and package/plugin paths were not edited.

## Cross-repo routing

No cross-repository prerequisite and no PR.

Same-target siblings, not absorbed:

| Ticket | Owns | Relation |
| --- | --- | --- |
| `ticket_1786977409_499180` | `external_hub_webrtc_live_output_preserves_exact_bytes` suite-load OperatorError | Created and started this Implement visit. The later orchestrator flatten removed the serial edge onto this ticket. Known-baseline owner. Not absorbed. |
| `ticket_1786937228_425608` | `unix_adapter_unbound_printf_stream_attach_completes` flake | Passed in every binding suite run this visit. Not absorbed. |
| `ticket_1786913892_208903` | WebRTC write-budget sibling continuation | Different test. Not absorbed. |

## Deviations from plan

- `OwnerPollDecision::ServeControl` carries `Box<Option<ControlMessage>>`. Plan v3 sketched an unboxed `ControlMessage` variant. `clippy::large_enum_variant` rejected that form. Review `finding_1787003842_417998` then required the same single-allocation box as `OwnerEvent::Control`. Classification arms and order stay identical.
- Pre-change reproduction on this worktree was not run. The ticket already carries exact base-`547ca38` failure evidence. Plan v3 marked local reproduction as corroborating only.
- Plan v3.1, authorized by `question_1787003911_553236`, replaces acceptance check 4. This ticket no longer requires five consecutive unfiltered lifecycle suites. The Implement and Review gate is focused proof plus known-baseline exact-bytes evidence. Final integration remains the zero-failure convergence gate.
- Plan v3.2, authorized by `question_1787005268_112714` and Verify `review_1787005607_892358`, replaces the first-snapshot count assert. Direct-merge acceptance on `a1cb911` failed repetition 4 of `ready_spawn_completes` at `items.len() >= 24` while Spawn waited 17.794416ms. Production slice budgets stay unchanged.
- Plan v3.3, authorized by Review `review_1787006443_517771`, replaces both the 30-second quiet drain and the later Subscribe-turn drain. Review's focused gate at `b3432e4` failed repetition 2 with 17 of 24 identities after 30 seconds. A local Subscribe-turn draft then froze at 8 identities because Subscribe does not run Observe and extra Subscribe turns starve Observe. The repaired oracle keeps the 24-session load fixture, asserts ready Spawn plus one live frame, and fails immediately on `DaemonEntityFrame::Error`.

## Tests and downstream proof run

Tracked `.gitignore` is 53 bytes and matches `HEAD`. The ticket worktree path has no `:`. No `CARGO_TARGET_DIR` override.

Production entry point: `serve_daemon` in `src/daemon_transport.rs` now calls `classify_owner_poll(control_rx.try_recv(), slice_due)` at the busy-path decision. The helper is the single classification site. A queued control message becomes one `Box<Option<ControlMessage>>` that the loop moves into `OwnerEvent::Control`. Scheduling arms are the same as the pre-change `try_recv` match.

### Red-proof

Temporary Ablation A inverted helper precedence so `slice_due` won over a ready control message. Reverted after capture.

| Control | Command | Exit | First failure |
| --- | --- | --- | --- |
| A (precedence inversion) | `./test.sh --locked --lib queued_control_precedes_a_due_maintenance_slice` | 101 | `assertion failed: matches!(classify_owner_poll(Ok(ControlMessage::RejectedConnection), true), OwnerPollDecision::ServeControl(message) if matches!(*message, Some(ControlMessage::RejectedConnection)))` at `src/daemon_transport.rs:7860` |
| B (Error treated as live) | `./test.sh --locked --test hub_daemon_lifecycle_test assemble_subscription_rejects_an_error_frame` | 101 | `a typed Error frame must not count as a live subscription` after `assemble_subscription_frame_is_live` accepted `DaemonEntityFrame::Error` |
| C (Subscribe-turn completeness, discarded) | `./test.sh --locked --test hub_daemon_lifecycle_test ready_spawn_completes` | 101 | repetition 3: `assemble projection must include all 24 load-session identities after 8 Subscribe catch-up turns; missing=["assemble-session-08".."assemble-session-23"]; seen=8` |

No timers or trial counts. The v1 nine-second sabotage and the v2 `biased;`-removal ablation were not used.

### Acceptance tallies

| Command | Result |
| --- | --- |
| `cargo build --locked -p botster-core-daemon --bin botster-session-worker` | exit 0 |
| `./test.sh --locked --lib queued_control_precedes_a_due_maintenance_slice` × 20 on the final boxed helper | 20/20 PASS. Repeated 20/20 after the v3.3 live-frame oracle. |
| `./test.sh --locked --test hub_daemon_lifecycle_test ready_spawn_completes` × 20 | 20/20 PASS on the v3.3 live-frame oracle; each run `2 passed; 0 failed; 219 filtered out`. Defective completeness oracles already failed: first-snapshot count on `a1cb911` rep 4; 30-second drain at `b3432e4` Review rep 2 (17/24); Subscribe-turn draft local rep 3 (8/24). |
| Observation run with `--nocapture` | snapshot assembly 13.346375ms; observe-slice load 51.575042ms |
| `./test.sh --locked --test hub_daemon_lifecycle_test` × 5 | Historical record only. 4/5 PASS; run 1 FAIL on known-baseline exact-bytes owned by `ticket_1786977409_499180`. Not this ticket's gate after `question_1787003911_553236`. |
| `cargo fmt --all -- --check` | exit 0 after the v3.3 live-frame oracle |
| `cargo clippy --workspace --all-targets --locked -- -D warnings` | exit 0 after the v3.3 live-frame oracle |

Downstream proof: not required. No public surface, DTO, pin, or runtime behavior changes.

Non-binding loaded-daemon workflow dispatch was not run.

### Known-baseline suite record

| Run | Result | Detail |
| --- | --- | --- |
| 1 | FAIL exit 101 | 218 passed; 1 failed; 1 ignored; 341.37s. Failure: `external_hub_webrtc_live_output_preserves_exact_bytes` at `tests/hub_daemon_lifecycle/webrtc_proofs.rs:417`: `shutdown should complete the write(2) session, got OperatorError`. Both `ready_spawn_completes_*` tests passed. `unix_adapter_unbound_printf_stream_attach_completes` passed. |
| 2 | PASS | 219 passed; 0 failed; 1 ignored; 336.92s |
| 3 | PASS | 219 passed; 0 failed; 1 ignored; 323.33s |
| 4 | PASS | 219 passed; 0 failed; 1 ignored; 321.08s |
| 5 | PASS | 219 passed; 0 failed; 1 ignored; 320.97s |

Isolated exact-bytes command on this branch: `./test.sh --locked --test hub_daemon_lifecycle_test external_hub_webrtc_live_output_preserves_exact_bytes` => PASS exit 0 in 3.45s. The test comment says isolated green is not its suite-load proof.

Source proof: `tests/hub_daemon_lifecycle/webrtc_proofs.rs` is byte-identical to `origin/main`. Plan Review on this worktree at plan commit `c0f5646` ran the same suite command: 219 passed; 0 failed; 1 ignored.

The exact-bytes failure did not repeat in runs 2-5. It still owns `ticket_1786977409_499180`. This focused repair does not absorb it. Final integration cannot resume until that sibling and the other direct blockers close.

## Unverified behavior or residual risk

- The decision-level unit test proves the busy-path classification, not whole-loop wiring. A future pre-control drain that bypasses the helper would not fail it. The loop call site is one match for Review to check. The existing `due_reconciliation_precedes_an_already_ready_control_message` still pins the blocking path.
- The two integration tests no longer assert any latency bound. `waited` stays visible as an observation. `tests/session_projection_owner_loop.rs` still const-asserts the budget relations.
- The snapshot-assembly test no longer waits for 24 projected identities. That wait is a loop-tick window: Observe publishes eight live rows per slice, Subscribe does not run Observe, and extra Subscribe turns starve Observe. The test keeps the 24-session load fixture and asserts ready Spawn plus one live frame. A typed `DaemonEntityFrame::Error` fails immediately. The 5-second read timeout is single-frame hang protection, the same bound other entity-subscription tests in this file already use.
- Suite run 1 failed on exact-bytes. That failure is known-baseline evidence for `ticket_1786977409_499180`, not residual risk and not this ticket's repair. Review `finding_1787003842_944465` is resolved by durable answer `question_1787003911_553236` plus Plan v3.1.
- `tests/hub_daemon_lifecycle/package_event_plane.rs` still has a wall-clock ready-operation assertion. It did not fail these runs.

## Missing vault guidance discovered

Captured to the vault inbox:

- Wall-clock ready-operation bounds through a real daemon child are ambient-load-sensitive. Load-window liveness is not an ordering proof. The durable idiom is a decision-level unit oracle plus functional-under-load integration coverage. Remaining site: `tests/hub_daemon_lifecycle/package_event_plane.rs`.
- A pipeline run `target_id` can be a corrupted merge of two registered targets. Resolve routing from the ticket record first.
