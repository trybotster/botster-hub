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
| Step | `botster_stack_implement` (`run_step_1787145549_478989`) |
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
| `src/daemon_transport.rs` | Install exact-key suppression before the Core `Shutdown` request on the Active/Stopping/classify-error path. Remove post-request suppress calls. Helpers now call `suppress_session_route_generations`. Add source-order red-on-revert oracle and Missing recover unit test. |
| `src/unix_terminal_adapter.rs` | Add `suppress_session_route_generations`. Remove session-wide `suppress_sessions` / `suppress_session` / `session_is_suppressed`. Mux unit proofs for Running silence, host-close silence, later-generation emit, and empty snapshot. |
| `src/webrtc_terminal_adapter.rs` | Same mux change, mirrored. |
| `tests/hub_daemon_lifecycle/sessions.rs` | Extend the live Core-error production path: attach the victim, record the Core generation, keep reading, prove typed OperatorError, occupancy still Running, no close event, sibling envelopes still flow. |
| `tests/hub_daemon_lifecycle/unix_terminal_adapter.rs` | Extend the success path with generation, second shutdown, observe progress, and late Status. Add Missing, sibling, replacement-owner, and later-generation emit proof. |
| `tests/hub_daemon_lifecycle/webrtc_terminal_adapter.rs` | Extend the protocol-7 WebRTC success path with Core generation and late Status. |
| `docs/reports/suppress-shutdownsession-close-events-before-core-teardown-implement.md` | This report. |

## Ownership boundaries preserved

Hub still owns host close-event policy, ShutdownSession control, and mux suppression. Core still owns subscription lifecycle, hard-stop adapter close, and generation identity. The data plane and `botster-hub-client` DTOs are unchanged.

Mux `register` runs from `bind_unix_adapter_after_attaching` / `bind_webrtc_adapter_after_attaching` on the same daemon control path as `ShutdownSession`. A route cannot appear between the snapshot and the Core call.

## Cross-repo dependencies or separately routed work

None. The required Core APIs already ship on `main`. Parent ticket `ticket_1786912572_610381` stays test-only.

## Deviations from plan

None that change scope.

Clarification of acceptance check 3(b): after a failed Core `Shutdown` the occupancy union still lists the victim because the session stays Active and mux routes are not retired. That occupancy is the live Running classifier. The adapter is host-closed under suppression; keep-reading proves no `TerminalSubscriptionClosed` for that exact generation. The plan's "adapter closed" requirement is met by the production host-close plus silent CloseEvents, not by occupancy disappearance.

Red-on-revert uses the deterministic source-order unit test `shutdown_session_arm_installs_exact_suppression_before_core_request` plus mux exact-key tests. No ablation report was needed.

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
- `./test.sh --locked`: passed, including `hub_daemon_lifecycle_test` 254 passed / 1 ignored

Focused production-path proofs:

- `shutdown_session_arm_installs_exact_suppression_before_core_request`
- mux `exact_generation_suppression_silences_running_close_and_preserves_later_generation` (Unix and WebRTC)
- `process_exit_and_shutdown_session_do_not_emit_terminal_subscription_closed`
- `shutdown_session_exact_keys_preserve_replacement_owner_and_siblings`
- `webrtc_terminal_adapter_detach_peer_death_process_exit_and_shutdown_do_not_emit_close_event`
- `external_hub_shutdown_session_failure_keeps_daemon_and_sibling_usable`

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
