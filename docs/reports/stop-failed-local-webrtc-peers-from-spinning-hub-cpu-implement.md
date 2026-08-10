# Implement report: Stop failed local WebRTC peers from spinning botster-hub CPU

## Target repository and target_id

| Field | Value |
| --- | --- |
| Target repository | `botster-hub` (`trybotster/botster-hub`) |
| `target_id` | `tgt_7e208a0c76a44980a83b63af976b1f22` |
| Authoritative path | `/Users/jasonconigliari/Projects/botster-hub` |
| Pipeline worktree | this session worktree (`project-pipelines/ticket_1786327694_445993`) |
| Ticket | `ticket_1786327694_445993` |
| Run | `run_1786327694_835389` |
| Step | `bsg_implement` (`run_step_1786329302_759836`) |
| Approved plan | `docs/plans/stop-failed-local-webrtc-peers-from-spinning-hub-cpu.md` (sequence 7) |

Routing verified via `list_spawn_targets`: `tgt_7e208a0c76a44980a83b63af976b1f22` → `botster-hub` @ `/Users/jasonconigliari/Projects/botster-hub`. Plan artifact used the same `target_id`.

## Repository playbook and other playbooks/notes applied

### Playbooks

- [[implementer-playbook]]
- [[botster-implementer-playbook]]
- [[botster-hub-playbook]] — ownership charter for this run

### Targeted atomic notes

- [[terminal webrtc failure records do not prove peer runtime teardown]]
- [[webrtc peer cleanup removes every per peer owner together]]
- [[file descriptor exhaustion from stale webrtc connections]]
- [[graceful-termination-requires-explicit-cleanup-hooks]]
- [[cleanup_webrtc_channel double-fires from concurrent callers]]
- [[late webrtc messages after disconnect must not recreate clients]]
- [[test script required for rust tests not cargo test]]
- [[a regression test must be shown to go red with the fix reverted]]
- [[cli-patterns]]

### Explicitly not loaded

- [[project-pipelines-playbook]] — package/plugin workflow paths out of scope
- Other repository charters

### Constraints applied before edits

- Work only in the hub pipeline worktree for `tgt_7e208a0c76a44980a83b63af976b1f22`
- Surgical fix inside hub local WebRTC transport + control-plane forget call site
- Repo-owned `./test.sh` only
- No public API / DTO changes
- No cross-repo edits

## Files changed

| Path | Change |
| --- | --- |
| `src/local_webrtc.rs` | Strengthen `remove_peer`: `PeerConnection::close` via dedicated runtime, empty-map runtime drop; share close helper with `stop_all`; `#[cfg(test)]` close-completion evidence, live oracles, worker-thread counters, production-handler failure injection; H1–H3 tests |
| `src/daemon_transport.rs` | `handle_control_message` / `DaemonControlState` / `EntitySubscriptionState` `pub(crate)` for in-process production-path harness; sole `LocalWebrtcPeerClosed` arm still calls `remove_peer` (now closes) |
| `docs/plans/stop-failed-local-webrtc-peers-from-spinning-hub-cpu.md` | Approved sequence 7 plan (committed with implement) |
| `docs/reports/stop-failed-local-webrtc-peers-from-spinning-hub-cpu-implement.md` | This report |

## Ownership boundaries preserved

- **In scope (botster-hub):** local WebRTC peer map, dedicated `botster-local-webrtc` runtime, terminal cleanup / forget on `LocalWebrtcPeerClosed`
- **Out of scope:** botster-core, hub-client DTOs, web UI, session workers, Project Pipelines package, ICE/TURN redesign
- **External:** still only calls consumed `webrtc` 0.20.0-rc.1 `PeerConnection::close()` — no crate fork

## Cross-repo dependencies or separately routed work

None for the primary fix. Parallel comparison ticket `ticket_1786324642_480494` continues independently per human answer.

## Deviations from plan

None material.

- Close location: transport forget from sole `LocalWebrtcPeerClosed` arm (unchanged call site name `remove_peer`)
- Runtime lifecycle: immediate `runtime.take()` when peer map empty
- Close oracle: `#[cfg(test)]` close-completion evidence after real `peer.close()` (no `connection_state()`)
- H1–H3 exact test names used
- Optional lifecycle integration test not required (in-process production chain covers signal + entity subscribe + PeerClosed arm)

## Implementation summary

### Production path

```text
on_connection_state_change(Failed)
  → observe_peer_connection_state / cleanup_once(PeerFailed)
  → ControlMessage::LocalWebrtcPeerClosed
  → handle_control_message:
       persist local-webrtc-sender-terminal.json
       LocalWebrtcTransport::remove_peer:
         peers.remove → runtime.block_on(peer.close()) → if empty runtime.take()
       remove entity_subscription_ids
       detach attach subscriptions
```

### Before

`remove_peer` was map-only remove; terminal JSON could complete while `PeerConnectionDriver` timeout loops kept `botster-local-webrtc` threads hot.

### After

Same control-plane arm; forget now closes the live peer and parks the dedicated runtime when no peers remain. `stop_all` shares `close_peer_on_runtime`.

## Tests and downstream proof run

### Commands (green with fix)

```sh
./test.sh --lib local_webrtc_peer_failed_closes_live_peer_parks_runtime_and_clears_driver_threads
./test.sh --lib local_webrtc_single_peer_failed_cleanup_preserves_sibling_peer_and_runtime
./test.sh --lib local_webrtc_after_last_peer_cleanup_new_signal_recreates_runtime_and_succeeds
./test.sh --lib local_webrtc
```

Results:

- H1: **ok** (~0.8–0.9s)
- H2: **ok** (~1.3s)
- H3: **ok** (~1.3s)
- `--lib local_webrtc`: **25 passed**, 0 failed (includes H1–H3 + prior unit surface); hub-client local_webrtc filters also green (3 passed)

### H1 production chain exercised

1. Real `signal` / `answer_offer` → real `PeerConnection` + DataChannel open
2. Production `SubscribeEntities` over encrypted DataChannel → `ControlMessage::SubscribeEntities` → daemon `entity_subscriptions` register → `EntitySubscribed`
3. `inject_peer_connection_state_for_test(Failed)` → production `LocalWebrtcHandler::on_connection_state_change`
4. `cleanup_once(PeerFailed)` → `LocalWebrtcPeerClosed`
5. `handle_control_message` arm: terminal persist + strengthened `remove_peer` + entity detach

### H1 oracles

| Oracle | Result |
| --- | --- |
| Terminal file `cause == peer_failed`, `peer_connection_state == failed` | pass |
| Entity subscription removed / live count 0 | pass |
| `active_peer_count() == 0` | pass |
| `has_dedicated_runtime() == false` | pass |
| Close-completion evidence for grant | pass (count 1) |
| Dedicated worker threads join ≤ 2s | pass |

### Red proof (fix temporarily reverted)

| Axis | Mutation | H1 failure |
| --- | --- | --- |
| 1 | Map-only remove (historical bug) | Fails `!has_dedicated_runtime()` |
| 2 | Map remove + runtime drop **without** `peer.close()` | Fails close-completion count `0 != 1` (does not green-wash on runtime drop alone) |

Fix restored after red proof; suite re-run green.

## Unverified behavior or residual risk

1. Long multi-hour battery soak after live `peer_failed` is observational, not a CI gate. H1 proves close + map + runtime + thread join on the production path.
2. Assumes `PeerConnection::close()` stops that peer’s driver loops (same assumption as existing `stop_all`). If a crate-internal hold remains after close + runtime drop, escalate — do not paper over with sleeps.
3. Failure injection uses the production handler method with deterministic `Failed` state rather than flaky real ICE failure (plan-locked).
4. Parallel ticket may land an alternate patch; outcomes compare independently.

## Missing vault guidance discovered

None that blocked implementation. Plan-time capture decision stands: after ship, update [[terminal webrtc failure records do not prove peer runtime teardown]] with fix evidence rather than many new notes. No new vault note written during Implement (source capture + atomic diagnosis already exist).

## Botster layers changed

| Layer | Changed? |
| --- | --- |
| Hub local WebRTC transport / peer registry | Yes |
| Hub daemon control-plane `LocalWebrtcPeerClosed` | Call site unchanged; forget semantics fixed |
| Core / hub-client / web / packages | No |
