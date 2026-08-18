# Implement report: Hub ShutdownSession natural-exit idempotency

| Field | Value |
| --- | --- |
| Ticket | `ticket_1786977409_499180` |
| Run | `run_1787012955_256937` |
| Step | `botster_stack_implement` |
| Target repository | `botster-hub` (`trybotster/botster-hub`) |
| `target_id` | `tgt_7e208a0c76a44980a83b63af976b1f22` |
| Authoritative path | spawn target `botster-hub` via `list_spawn_targets` |
| Plan | `docs/plans/hub-shutdown-session-idempotent-across-natural-exit-races.md` @ `075e9e6` |
| Decision gate | Rule B |
| Core dependency | `ticket_1787015956_494734` / `dependency_1787015963_708930` closed |
| Core pin | `d981bb03` (was `fc541a5`); recorded Rule B repin |
| Hub main integrated | `c1ce7e5` via merge commit `b117d0d` |
| Review requested | no; pin and focused proofs are in; wrapper W1/W2 still cannot spawn |

## Repository playbook and other playbooks/notes applied

- [[implementer-playbook]]
- [[botster-implementer-playbook]]
- [[botster-hub-playbook]]
- [[botster runtime teardown lenses]]
- [[host ShutdownSession classification must call the exact-session Core query]]
- [[observed-exit waits must issue a production exact-session observe turn]]
- [[a suite-load oracle must not demand more than the host contract another test in the same file already codifies]]
- [[flake oracles over typed response frames must print the full typed error body]]
- [[hub shutdown preserves durable session workers]]
- [[conformance harnesses gate on deterministic invariants not timing]]
- [[a regression test must be shown to go red with the fix reverted]]
- [[test script required for rust tests not cargo test]]
- [[implementation artifacts must match actual git state]]
- [[implement gate must verify committed work and pr link before review]]
- [[implementation steps must persist report artifacts for review]]
- [[pipeline vault checklists must cite exact resolvable note titles]]
- [[project-pipelines-playbook]] because Rule B changed workflow state
- [[dependency ticket creation must start its run or emit an operator action]]
- [[cross repo dependency registration must use dependency repo target]]

Convention conflicts: none.

## Constraints applied before edits

- Work only in this run worktree for `botster-hub`.
- Hub owns ShutdownSession classification and recover. Core owns worker exit-evidence mechanics.
- No production wall-clock, retry, or suite-load correctness mechanism.
- No Core edits in this worktree.
- No pull request; merge policy is direct.

## Phase 1 captures

### Hygiene

- Tracked `.gitignore` is present and has 5 lines.
- Worktree path has no colon.
- Worker prebuild: `cargo build --locked -p botster-core-daemon --bin botster-session-worker` finished in 0.27s.

### Wrapper diagnosis

A parent wrapper registered through production `core_engine.session_worker_path` cannot wrap the real worker as a child of Core's spawned process.

Core binds two identities to the spawned child:

1. Readiness line `botster-session-worker-ready <child_pid>`.
2. Welcome `recovery_identity.worker_pid` must equal that same child pid.

A naive parent that runs the real worker therefore fails spawn.

Verbatim Hub spawn failure on both transports, W1 and W2, after the parent-wrapper attempts:

```
code=spawn_failed
operation=spawn
message=spawn failed before the session started; verify the configured session worker and command
```

A later control-socket proxy that rewrites welcome `worker_pid` still failed spawn with the same typed body. Those four forced-window tests are present and ignored until the Core dependency lands. They are not this ticket's passing gate.

### Mechanism validation

The locked Core pin `fc541a5` still matches the plan citations:

- `WorkerProcessRuntime::drain_output` surfaces `ProcessExited` only when `reader_finished` and `child.try_wait()` reports `status.success()`. Adopted sessions (`child: None`) pass. A live wrapper or delayed reap yields `try_wait() == None`. A non-success worker exit suppresses the payload permanently.
- `observe_session_lifecycle` drains before the registry read, so classify `Err` never consults recorded `Exited`.
- Hub `recover_after_core_shutdown_error` previously propagated classify `Err` with `?`.

Sub-cases 4 and 5 remain Core-internal. Hub cannot distinguish "ProcessExited in flight" from "truly active or stuck" without a Core surface. That is the Rule B decision.

Rule A is rejected. The capture line that decided it is the spawn-time welcome identity plus the `try_wait().success()` gate: no current exact-session query exposes pending-exit payload presence.

## Files changed

- `src/daemon_transport.rs` -- recover fallback after classify `Err`; recorded-registry mapping; recover unit tests; comments on the Active OperatorError boundary.
- `tests/hub_daemon_lifecycle/session_fixtures.rs` -- wrapper fixture helpers and census-aware wrapper path.
- `tests/hub_daemon_lifecycle/session_worker_wrapper.py` -- test-only wrapper script for the later forced-window proofs.
- `tests/hub_daemon_lifecycle/webrtc_proofs.rs` -- ignored W1/W2 WebRTC forced-window tests.
- `tests/hub_daemon_lifecycle/unix_terminal_adapter.rs` -- full typed error body on the Unix strict assert; ignored W1/W2 Unix tests.
- `docs/reports/hub-shutdown-session-idempotent-across-natural-exit-races-implement.md` -- this report.
- Rule B Core pin: `Cargo.toml`, `Cargo.lock`, `crates/botster-hub-client/Cargo.toml`, `crates/botster-hub-test-support/{Cargo.toml,build.rs,src/conformance_data.rs,src/lib.rs}`, `tests/session_projection_owner_loop.rs`, `tests/hub_daemon_lifecycle/package_event_plane.rs`, `tests/hub_daemon_lifecycle/webrtc_terminal_adapter.rs`.

## Ownership boundaries preserved

Hub still owns classification, recover, and host response kinds. Core still owns payload delivery, shutdown deadline, managed rollback, and observe-before-registry-read. The wrapper is test-only. No hub-client DTO change. No Core edit in this worktree.

## Cross-repo dependencies or separately routed work

- Created `ticket_1787015956_494734` on Core target `tgt_1f7bce66eb304881980f9b4a2a5ae3fe`.
- Registered `dependency_1787015963_708930`: this ticket depends on that Core ticket (`depends_on_status=open`).
- Started Core run `run_1787015981_429380` (`botster_stack_delivery`, Plan queued). Automatic start succeeded, so no `ask_human` operator action was required.
- Downstream blocker `dependency_1787014444_456296` remains: `ticket_1786938984_190098` depends on this ticket.
- Core ticket `ticket_1787015956_494734` is closed. This run repinned Hub to merged Core main `d981bb03f91e2d13428000ac989c50d794f659b2`.
- Downstream blocker `dependency_1787014444_456296` remains: `ticket_1786938984_190098` depends on this ticket.

## Deviations from plan

- Phase 1 did not obtain live blind-ShutdownSession `OperatorError` bodies under a successful W1/W2 spawn. The wrapper could not become a legal Core child. The decision gate still fired from the Core source plus the spawn-identity captures.
- Forced-window tests exist but are `#[ignore]` until the Core dependency lands. They are not green gates on this commit.
- Exact-bytes blind restore, idempotency tightening, and red-on-revert of the W1 window are deferred to the post-repin resume. Recover-fallback unit tests are the Hub-leg red-on-revert surface that is available now.
- No full lifecycle suite ran, as required by acceptance check 10.

## Hub main `c1ce7e5` integrate

`ticket_1786937228_425608` is already an ancestor of `origin/main` (`0a22c36`). This branch merged `origin/main` at `c1ce7e5` (`b117d0d`). The merge was clean.

Overlap recheck:

- Incoming Unix work lives in `unix_adapter_unbound_printf_stream_attach_completes` and `unix_adapter_bound_printf_stream_attach_delivers_process_exit`. Those tests hold the child on a release file. They do not call `ShutdownSession` as observation.
- This ticket's Unix work lives in `unix_shutdown_session_from_another_connection_classifies_attached_exit` and the ignored W1/W2 wrappers. Those tests still exist after the merge.
- Incoming reports name this ticket as not absorbed. They do not change the recover path.
- Incoming write-budget edits are in `src/local_webrtc.rs` and WebRTC adapter files. They do not touch `recover_after_core_shutdown_error`.
- Production `src/daemon_transport.rs` did not change on main since the previous merge-base.

No full lifecycle suite ran after the integrate.

## Core pin `d981bb03`

Recorded Rule B repin after `ticket_1787015956_494734` merged to Core main.

Pin delta: `fc541a59338d0591ba4fb3fa522a030d212d26d0` -> `d981bb03f91e2d13428000ac989c50d794f659b2`.

Live pin sources updated together: workspace and member `Cargo.toml` files, `Cargo.lock`, test-support `build.rs` / late-attach provenance, and the Git-visible pin fixtures. Historical sibling reports were not rewritten.

Core `c23b833` delivers `ProcessExited` when the worker payload arrives. `drain_output` no longer gates that delivery on `try_wait().success()`. A delayed reap goes to a background reaper.

The Hub parent-wrapper still cannot spawn. After the pin, `unix_shutdown_session_after_w1_delayed_worker_reap_is_idempotent` still failed at `spawn_and_bind` with `kind=OperatorError` instead of `Spawned`. Those four tests stay ignored.

No full lifecycle suite ran after the pin.

## Tests and downstream proof run

All commands used `./test.sh` except the documented worker prebuild and the fmt/clippy gates.

| Check | Command / filter | Result |
| --- | --- | --- |
| Worker prebuild | `cargo build --locked -p botster-core-daemon --bin botster-session-worker` | pass |
| Recover fallback units | `./test.sh --locked --lib recover_recorded` | 4 passed |
| Recover fallback failure | `./test.sh --locked --lib recover_fallback` | 1 passed |
| Active OperatorError units | `./test.sh --locked --lib shutdown_active` | 2 passed |
| Remaining shutdown family | `shutdown_unknown_session`, `shutdown_exited_classification`, `shutdown_stopping_record`, `production_core_shutdown` | 5 passed |
| Fmt | `cargo fmt --all -- --check` | pass |
| Clippy | `cargo clippy --workspace --all-targets --locked -- -D warnings` | pass |
| Post-integrate recover units | `./test.sh --locked --lib recover_` then `shutdown_active` | 5 + 2 passed |
| Post-integrate focused Unix trio | `./test.sh --locked --test hub_daemon_lifecycle_test -- --exact unix_adapter_unbound_printf_stream_attach_completes unix_adapter_bound_printf_stream_attach_delivers_process_exit unix_shutdown_session_from_another_connection_classifies_attached_exit` | 3 passed in 6.03s |
| Post-pin worker | `cargo build --locked -p botster-core-daemon --bin botster-session-worker` | pass in 1m 18s |
| Post-pin fixture provenance | `./test.sh --locked --lib late_attach_goldens_have_distinct` | 1 passed |
| Post-pin Git-visible pin | `./test.sh --locked --test session_projection_owner_loop -- --exact git_visible_hub_members_share_one_exact_core_revision` | 1 passed |
| Post-pin recover units | `./test.sh --locked --lib recover_` | 5 passed |
| Post-pin focused natural-exit | `./test.sh --locked --test hub_daemon_lifecycle_test -- --exact unix_shutdown_session_from_another_connection_classifies_attached_exit external_hub_webrtc_live_output_preserves_exact_bytes` | 2 passed in 8.21s |
| Post-pin ignored W1 Unix | `./test.sh --locked --test hub_daemon_lifecycle_test -- --ignored --exact unix_shutdown_session_after_w1_delayed_worker_reap_is_idempotent` | fail: Spawn `OperatorError`, not `Spawned` |

Production entry point: Unix/WebRTC `ShutdownSession` still enters `src/daemon_transport.rs` at the `DaemonRequest::ShutdownSession` arm, then `classify_shutdown_session`, Core `shutdown_session`, and `recover_after_core_shutdown_error`. The new recover path is that production recover function. Classify `Ok(Active)` plus a real Core error is unchanged.

## Runtime-teardown lenses

Every lens from the approved plan remains in force. No lens was dropped to informal follow-up. Closed Core `ticket_1787015956_494734` owns the payload-delivery lens. Hub still owns classify, recover, and the live host-path proofs.

## Unverified behavior or residual risk

- Hub wrapper W1/W2 tests still cannot spawn. Core now owns those mechanism windows.
- Blind exact-bytes and tightened idempotency oracles are not restored on this pin commit.
- Recover fallback still covers only classify `Err` after a Core shutdown error.
- Ready-spawn suite co-flake stays owned by `ticket_1786938984_190098`.

## Missing vault guidance discovered

- The suite-load oracle note still documents the superseded legal-OperatorError contract. Capture after this ticket closes.
- [[host ShutdownSession classification must call the exact-session Core query]] still says it is not shipped. Hub main already ships the exact-session query.
- New capture candidates after Core lands: worker ProcessExited must not gate on reap timing or worker exit status; ShutdownSession strict natural-exit idempotency is Events-or-SessionCleanup on every transport; Core welcome `worker_pid` prevents a parent wrapper from standing in for the real worker.
