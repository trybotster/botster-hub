# Implementation report: WebRTC teardown follow-up

| Field | Value |
| --- | --- |
| Ticket | `ticket_1786385940_304814` |
| Run | `run_1786386094_979398` |
| Step | `botster_stack_implement` (review rework; sequence 9+) |
| Target repository | `botster-hub` (`trybotster/botster-hub`) |
| `target_id` | `tgt_7e208a0c76a44980a83b63af976b1f22` |
| Base | `main` @ `26f1673` (PR #200 merged) |
| Branch | `project-pipelines/ticket_1786385940_304814` |
| Plan | `docs/plans/webrtc-teardown-follow-up-late-attach-bounded-close.md` (sequence 5) |
| Class | runtime-teardown (`teardown_class_applies: yes`) |
| PR | https://github.com/trybotster/botster-hub/pull/201 |

## Review rework (findings resolved)

| Finding | Resolution |
| --- | --- |
| Stale PeerClosed attach snapshot can detach replacement owner | Snapshot/fail-closed attach candidates owner-checked against `attach_owner_grant_ids`; preserve foreign owners. Test: `local_webrtc_stale_peer_attach_snapshot_does_not_detach_replacement_owner` |
| Fail-closed teardown has no handler-wide close bound | `fail_closed_drop_dedicated_runtime`, `park_runtime_if_idle`, and `stop_all` drop runtime/peers immediately without sequential re-close waits |
| Hang oracle bypasses production peer.close timeout | Hang inject uses the **same** `timeout(BOUND, close_future)` path; hang = `pending()` before `peer.close()` so only the production bound cancels |
| Format/whitespace gates fail | `cargo fmt --all` + `git diff --check` clean |
| Fail-closed leaves primary `peer_state` | Failed primary grant passed into `fail_closed_drop_dedicated_runtime` so `take_remove_result` sweeps it; `peer_state_count()` oracle asserts 0 after hang + error fail-closed |
| Hang test lacks external hard-stop | Parent spawns child process; parent kills after `HANG_CLOSE_CHILD_DEADLINE` if child never exits — finite red if production timeout ablated |
| Hard-stop child can orphan session worker | Hang child uses entity subscriptions only (no Spawn/Attach); sibling attach fail-closed remains on forced-error test |
| Global worker-readiness race | `spawn_capture_lock` serializes Spawn→census; only adopt live new PIDs (prefer data-dir ownership when attributable); 3× default-concurrency `./test.sh local_webrtc` green |
| Report/plan whitespace + clippy | Plan trailing spaces stripped; `git diff main...HEAD --check` + workspace clippy `-D warnings` exit 0 |
| PR test evidence stale | PR body updated to 36 tests + stale attach snapshot proof |

## Repository playbook and notes applied

- [[implementer-playbook]]
- [[botster-implementer-playbook]]
- [[botster-hub-playbook]] (ownership charter)
- [[botster runtime teardown lenses]] (every lens implemented)
- [[terminal webrtc failure records do not prove peer runtime teardown]]
- [[webrtc peer cleanup removes every per peer owner together]]
- [[test script required for rust tests not cargo test]]
- [[a regression test must be shown to go red with the fix reverted]] (structure supports red-on-revert; hang/late paths named)
- Plan-cited Hub charter must-load notes (no package/plugin/Project Pipelines overlay)

**Not loaded:** [[project-pipelines-playbook]] (package/plugin/workflow paths out of scope).

## Files changed

| Path | Change |
| --- | --- |
| `src/daemon_transport.rs` | `ControlMessage::Request { grant_id }`; universal live-peer fail-closed for tagged Requests; owner-checked late Unsubscribe; attach owner index + PeerClosed residual attach sweep; socket constructors `grant_id: None` |
| `src/local_webrtc.rs` | Stamp grant on peer Requests; success-only peer-side attach bookkeeping; bounded `close_peer_on_runtime` (`LOCAL_WEBRTC_PEER_CLOSE_BOUND`); hang inject; production-path tests |
| `docs/plans/webrtc-teardown-follow-up-late-attach-bounded-close.md` | Approved plan artifact (sequence 5) |
| `docs/reports/webrtc-teardown-follow-up-implement-report.md` | This report |

## Ownership boundaries preserved

- Work stays in hub control plane + local WebRTC transport only.
- No SessionIo/ClientWorker byte-path edits, no hub-client DTO ownership changes, no core actor changes, no package/plugin/TUI/SPA work.
- Socket path remains untagged (`grant_id: None`) and unrestricted.

## Cross-repo dependencies / separately routed work

- None registered for this implementation.
- Parent dependency `ticket_1786327694_445993` / PR #200 is closed and on base.
- Live multi-hour CPU/no-spin sample remains Verify-stage preferred path (or same-repo dependency/waiver); not a cross-repo item.

## Runtime-teardown lenses (implemented)

| Lens | Implementation |
| --- | --- |
| Isolation | Successful close removes one grant; hang/timeout/Err → fail-closed drops shared dedicated runtime (siblings sacrificed, tested) |
| Bounds | `LOCAL_WEBRTC_PEER_CLOSE_BOUND` wraps `peer.close()` (timeout → `ClosePeerOutcome::Failed` → fail-closed); no retry on timeout |
| Late-message matrix | Subscribe (existing); Unsubscribe owner-checked; **all** tagged `Request` fail-closed when grant not live |
| Production-path proof | Tests drive `handle_control_message` / PeerClosed / remove_peer; hang inject + handler elapsed deadline; worker join after park/fail-closed |
| Ownership identity | Entity `owner_grant_id` retained; `pending_runtime.attach_owner_grant_ids` for WebRTC attaches; PeerClosed owner sweeps; late Unsubscribe never deletes replacement owner |
| Sibling fail-closed | Success isolation test remains; hang path + Err path fail-closed sibling cleanup tested |

## Deviations from plan

1. **Test-only close bound duration:** production uses 3s; under `cfg(test)` bound is 200ms (handler deadline 2s) so hang injection stays CI-cheap. Semantics (timeout → fail-closed) unchanged.
2. **Handler hard-stop:** hang body runs in a child process; parent kills after `HANG_CLOSE_CHILD_DEADLINE` if child never exits (finite red on timeout ablation). Child uses entity subscriptions only (no durable session workers).
3. **Fail-closed no longer best-effort re-closes siblings:** runtime drop alone is the hard stop (matches review: no N×bound sequential waits).
4. **Live CPU/no-spin sample:** still Verify-stage preferred path or same-repo waiver/dependency.

## Tests and downstream proof

```sh
cargo build --locked -p botster-core --bin botster-session-worker
cargo fmt --all -- --check                                          # exit 0
git diff main...HEAD --check                                        # exit 0 (branch-range)
cargo clippy --workspace --all-targets --all-features -- -D warnings  # exit 0
./test.sh local_webrtc                                              # 36 lib tests passed
```

Result: **36** `local_webrtc` lib tests passed (including late Attach/Spawn/Unsubscribe-reuse, attach owner empty-snapshot sweep, stale attach snapshot preserve, hang fail-closed subprocess hard-stop without durable workers + `peer_state_count==0`) plus existing #200 suite; integration/client WebRTC filters green.

Production entry points:

- WebRTC DataChannel → `ControlMessage::Request { grant_id: Some }` → live-peer gate before `handle_control_request`
- Terminal peer state → `LocalWebrtcPeerClosed` → `remove_peer` → bounded close → map remove / fail-closed drop → control-plane entity + attach owner sweeps

## Unverified behavior / residual risk

- Multi-hour battery / multi-core spin under real browser offerer not sampled at Implement (Verify or waiver).
- Close bound of 3s may false-fail-close under extreme load; sibling sacrifice is intentional and tested.
- Timeout ablation red is the hang child's parent kill after `HANG_CLOSE_CHILD_DEADLINE` (finite nonzero), not a permanent ablation patch kept in CI.

## Missing vault guidance discovered

Matches plan capture candidates (post-merge capture recommended):

1. Late Attach vs late Subscribe asymmetry after #200 (now fixed in hub).
2. Late Unsubscribe must be owner-checked (bug on main after #200; fixed).
3. Unbounded `block_on(peer.close())` hang distinct from close `Err` (bounded here).
4. No capture yet for rejected per-peer runtime rewrite (still non-scope).
