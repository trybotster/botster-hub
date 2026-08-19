# Implement report: suppress ShutdownSession close events before Core teardown

## Target repository and target_id

| Field | Value |
| --- | --- |
| Target repository | `botster-hub` (`trybotster/botster-hub`) |
| `target_id` | `tgt_7e208a0c76a44980a83b63af976b1f22` |
| Authoritative spawn target | `trybotster/botster-hub` from `botster context` (`BOTSTER_TARGET_REPO`) and ticket/run `target_id` |
| Pipeline worktree | this run worktree |
| Ticket | `ticket_1787143511_231816` |
| Run | `run_1787143511_194671` |
| Step | `botster_stack_implement` (`run_step_1787152504_592497`, return from `review_1787152492_111779`) |
| Approved plan | `docs/plans/suppress-shutdownsession-close-events-before-core-teardown.md` revision 2 |
| Merge policy | `direct`; no PR required |
| Integrated base | `origin/main` `0a3458a` |
| `teardown_class_applies` | yes |

Independent routing: ticket, run, `botster context`, and the approved plan all map `tgt_7e208a0c76a44980a83b63af976b1f22` to `trybotster/botster-hub`. Grok's Botster MCP handshake failed (`broken pipe` on initialize). Context was loaded from `botster context`, the Project Pipelines plugin database, and the committed plan. Work stayed in this run worktree.

## Repository playbook and other playbooks/notes applied

### Playbooks

- [[implementer-playbook]]
- [[botster-implementer-playbook]]
- [[botster-hub-playbook]]
- [[botster runtime teardown lenses]]

### Targeted atomic notes

- [[host ShutdownSession classification must call the exact-session Core query]]
- [[Unix mux host events are unsolicited control frames]]
- [[host reconciliation must not rewrite a completed Core adapter close reason]]
- [[WebRTC host events use unsolicited daemon-event delivery]]
- [[Core terminal subscription ownership is session, subscription, and generation]]
- [[pre READY attach failure creates no attach ownership]]
- [[test script required for rust tests not cargo test]]
- [[implement gate must verify committed work and pr link before review]]
- [[implementation artifacts must match actual git state]]
- [[implementation steps must persist report artifacts for review]]
- [[pipeline vault checklists must cite exact resolvable note titles]]
- [[a suite-load oracle must not demand more than the host contract another test in the same file already codifies]]
- [[flake oracles over typed response frames must print the full typed error body]]
- [[a public occupancy oracle must union Hub routes with Core inventory]]
- [[ShutdownSession suppresses exact route generations before Core teardown]]
- [[a regression test must be shown to go red with the fix reverted]]
- [[narrow ablation at the enforcement point is the cleanest regression negative control]]
- [[an ablation that reddens at the first assertion does not vouch for later ones]]

### Explicitly not loaded

- [[project-pipelines-playbook]] — no Project Pipelines package or plugin path is in scope
- Other repository charters

### Constraints applied before edits

- Work only in the Hub run worktree for `tgt_7e208a0c76a44980a83b63af976b1f22`
- Follow approved plan revision 2
- Keep host close-event policy inside Hub
- Do not change Core, client DTOs, or protocol
- Implement every runtime-teardown lens
- Prefer the smallest surgical change
- Use `./test.sh`, not bare `cargo test`

## Files changed

| Path | Change |
| --- | --- |
| `src/daemon_transport.rs` | Install exact-key suppression before the Core `Shutdown` request on the Active/Stopping/classify-error path. Remove post-request suppress calls. Helpers now call `suppress_session_route_generations`. After a ShutdownSession OperatorError, count live Hub routes whose adapters are no longer bound and increment `cleanup_by_reason["shutdown_error_host_close"]`. Stopping classify inject requires `BOTSTER_ENV=test` plus the exact session id. Source-order oracle and test-mode inject oracle. |
| `src/unix_terminal_adapter.rs` | Add `suppress_session_route_generations`. Remove session-wide `suppress_sessions` / `suppress_session` / `session_is_suppressed`. Mux unit proofs for Running silence, host-close silence, later-generation emit, and empty snapshot. |
| `src/webrtc_terminal_adapter.rs` | Same mux change, mirrored. |
| `tests/hub_daemon_lifecycle/sessions.rs` | Live Core-error path: exact OperatorError body (`runtime_error`, `daemon-sessions-shutdown`, `shutdown`, exact message, empty diagnostics), `shutdown_error_host_close` counter increment, occupancy still present, zero close events, sibling envelopes. |
| `tests/hub_daemon_lifecycle/unix_terminal_adapter.rs` | Active success path with generation and late Status. Missing, sibling, replacement-owner proofs. Attached Stopping path through `BOTSTER_HUB_TEST_FORCE_SHUTDOWN_CLASSIFY_STOPPING_FOR`. |
| `crates/botster-hub-test-support/src/isolated_hub.rs` | Clear the Stopping classify inject unless a test sets it. |
| `tests/hub_daemon_lifecycle/webrtc_terminal_adapter.rs` | Extend the protocol-7 WebRTC success path with Core generation and late Status. |
| `docs/reports/suppress-shutdownsession-close-events-before-core-teardown-implement.md` | This report. |

## Ownership boundaries preserved

Hub still owns host close-event policy, ShutdownSession control, and mux suppression. Core still owns subscription lifecycle, hard-stop adapter close, and generation identity. The data plane and `botster-hub-client` DTOs are unchanged.

Mux `register` runs from `bind_unix_adapter_after_attaching` / `bind_webrtc_adapter_after_attaching` on the same daemon control path as `ShutdownSession`. A route cannot appear between the snapshot and the Core call.

## Cross-repo dependencies or separately routed work

None. The required Core APIs already ship on `main`. Parent ticket `ticket_1786912572_610381` stays test-only.

## Deviations from plan

None that change scope.

Review findings from `review_1787148532_135255` (resolved on `95b15af`):

- `finding_1787148532_899522`: occupancy is not an adapter-close oracle. After a ShutdownSession OperatorError, Hub now counts live attach routes whose adapters are unbound and publishes `cleanup_by_reason["shutdown_error_host_close"]`. The live test also pins the exact OperatorError body.
- `finding_1787148532_501615`: a second ShutdownSession after Active success can be Cleanup. Core waits two seconds for exit and the worker SIGKILLs at 500 ms, so Stopping does not survive a completed Shutdown. The live test forces Stopping classification with `BOTSTER_HUB_TEST_FORCE_SHUTDOWN_CLASSIFY_STOPPING_FOR` and drives the production fall-through. The source-order unit test requires Stopping to sit before suppression.

Review findings from `review_1787152492_111779`:

- `finding_1787152492_152596`: the Stopping inject honored `BOTSTER_HUB_TEST_FORCE_SHUTDOWN_CLASSIFY_STOPPING_FOR` in any process. `classify_shutdown_session` now calls `forced_shutdown_classify_stopping`, which requires `BOTSTER_ENV=test` and an exact session-id match. `forced_stopping_classify_inject_requires_test_mode` proves production, unset, wrong-session, and missing inject values stay inert, and that classify uses the helper.
- `finding_1787152492_643596`: the prior report claimed that removing `close_adapters_for_session` reddens the host-close assertion, but it recorded no executed ablation. Narrow ablation at the Core-error handler only (`pending_runtime.close_adapters_for_session(&session_id)` removed from the `HubClientRequest::Shutdown` `Err` arm; Missing-path close left intact):

```text
./test.sh --locked --test hub_daemon_lifecycle_test shutdown_session
```

Result while ablated: cargo test failed. `test result: FAILED. 6 passed; 1 failed; 0 ignored; 0 measured; 249 filtered out`. The one failure is `external_hub_shutdown_session_failure_keeps_daemon_and_sibling_usable` at `tests/hub_daemon_lifecycle/sessions.rs:3759`: `failed ShutdownSession must host-close the bound victim adapter: before=0 after=0 occupancy=[sibling generation 1, victim generation 0]`. Green siblings: `shutdown_session_exact_keys_preserve_replacement_owner_and_siblings`, `shutdown_session_classifies_parked_exit_beyond_one_baseline_page`, `process_exit_and_shutdown_session_do_not_emit_terminal_subscription_closed`, `unix_shutdown_session_from_another_connection_classifies_attached_exit`, `unix_shutdown_session_stuck_stopping_without_exit_evidence_stays_operator_error`, `attached_stopping_shutdown_session_suppresses_exact_generation`. Restoration: the Core-error `close_adapters_for_session` call is back; `HEAD` stayed `95b15afc67ee2c3111cb0383df98bf63edc8fd71` until this visit's commit. The remaining worktree diff is the test-mode inject gate and this report.

## Runtime-teardown lenses implemented

| Lens | Implementation |
| --- | --- |
| Isolation | Suppression keys are the target session's exact mux routes at snapshot time. Sibling sessions and other connections keep their routes and close-event behavior. |
| Bounds | Two mutex inserts per route. No new wait. Core `Shutdown` keeps its existing deadline. Core-error recovery stays one reclassify plus typed result. |
| Late-message matrix | Attach remains control-loop serialized. Detach still uses exact `suppress_generation`. Missing installs no keys. Second ShutdownSession is idempotent. Replacement owners get a later Core generation. |
| Production-path proof | `ShutdownSession` → classify → suppress exact keys → Core `Shutdown` → CloseEvents marks suppressed closed routes reported. Live Unix success, live Unix Core-error, and live WebRTC proofs go through the real daemon. |
| Ownership identity | Keys are Core-issued `(session, subscription, generation)`. Replacement-owner live proof uses real occupancy generations. |
| Sibling / fail-closed | Sibling streams and occupancy survive success and Core-error paths. Failed shutdown keeps the typed OperatorError and does not sacrifice siblings. |

## Tests and downstream proof run

Commands:

```text
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo build --locked -p botster-core-daemon --bin botster-session-worker
./test.sh --locked
```

Results:

- rustfmt: passed
- clippy `-D warnings`: passed
- session-worker: already built
- `./test.sh --locked`: passed. `hub_daemon_lifecycle_test` 255 passed / 1 ignored. `botster-hub` lib unit tests 422 passed, including `forced_stopping_classify_inject_requires_test_mode`. Restored `shutdown_session` filter: 7 passed.

Focused production-path proofs:

- `shutdown_session_arm_installs_exact_suppression_before_core_request`
- `forced_stopping_classify_inject_requires_test_mode`
- mux `exact_generation_suppression_silences_running_close_and_preserves_later_generation` (Unix and WebRTC)
- `process_exit_and_shutdown_session_do_not_emit_terminal_subscription_closed`
- `shutdown_session_exact_keys_preserve_replacement_owner_and_siblings`
- `webrtc_terminal_adapter_detach_peer_death_process_exit_and_shutdown_do_not_emit_close_event`
- `external_hub_shutdown_session_failure_keeps_daemon_and_sibling_usable`
- `attached_stopping_shutdown_session_suppresses_exact_generation`

Production entry point: `handle_runtime_control_request` `DaemonRequest::ShutdownSession` in `src/daemon_transport.rs`.

## Unverified behavior or residual risk

- Mux routes marked reported are still not retired until connection death. Pre-existing. Not changed.
- `Absent` CloseEvents classification still returns `None` forever for orphan routes. Pre-existing.
- Error-path host-close still ends other clients' live subscriptions for a still-Active session. Shipped policy. Only suppression order changed.
- Grok Botster MCP was down for this session. Gate and artifact submission use `botster mcp-serve` stdio as a workaround.

## Missing vault guidance discovered

- [[host ShutdownSession classification must call the exact-session Core query]] still says "not shipped yet". Classification already ships on main.
- No prior note said ShutdownSession must suppress exact generations before Core teardown. Captured to vault inbox as `shutdownsession-suppresses-exact-route-generations-before-core-teardown.md`.
- Mux reported-route retirement and permanent `Absent` revisit work remain the plan's recorded gaps.
