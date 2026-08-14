# Implement report: stale WebRTC peer attach snapshot must not detach replacement owner

| Field | Value |
| --- | --- |
| Ticket | `ticket_1786690597_154692` |
| Run | `run_1786690609_367424` |
| Step | `botster_stack_implement` |
| Target repository | `botster-hub` (`trybotster/botster-hub`) |
| `target_id` | `tgt_7e208a0c76a44980a83b63af976b1f22` |
| Authoritative path | spawn target `botster-hub` via `list_spawn_targets` |
| Pipeline worktree | ticket worktree on `project-pipelines/ticket_1786690597_154692` |
| Base | Hub `main` @ `173e528` |
| Locked Core | `033cd01` |
| Delivery | direct-merge; no pull request |
| Class | runtime-teardown (`teardown_class_applies: yes`) |
| Plan | `docs/plans/stale-webrtc-peer-attach-snapshot-must-not-detach-replacement-owner.md` |
| Checklist | `checklist_1786691630_333381` |

Routing verified independently: `list_spawn_targets` maps `tgt_7e208a0c76a44980a83b63af976b1f22` to `botster-hub` / `trybotster/botster-hub`. The approved plan used the same `target_id`.

## Repository playbook and other playbooks/notes applied

### Playbooks

- [[implementer-playbook]]
- [[botster-implementer-playbook]]
- [[botster-hub-playbook]] — ownership charter

### Class overlay

- [[botster runtime teardown lenses]] — every lens implemented below

### Targeted atomic notes

- [[webrtc peer cleanup removes every per peer owner together]]
- [[terminal webrtc failure records do not prove peer runtime teardown]]
- [[attach failed cleanup is route aware and idempotent]]
- [[pre READY attach failure creates no attach ownership]]
- [[late webrtc messages after disconnect must not recreate clients]]
- [[Hub route registry names describe ownership not attach queues]]
- [[test script required for rust tests not cargo test]]
- [[rust repo strict lints must be verified before dismissing warnings]]
- [[a regression test must be shown to go red with the fix reverted]]
- [[an ablation that reddens at the first assertion does not vouch for later ones]]
- [[implementation artifacts must match actual git state]]
- [[implement gate must verify committed work and pr link before review]]
- [[implementation steps must persist report artifacts for review]]
- [[pipeline vault checklists must cite exact resolvable note titles]]
- [[botster hub is a first party host profile over core]]
- [[botster data plane bypasses the hub through session and client actors]]
- [[file descriptor exhaustion from stale webrtc connections]]
- [[graceful-termination-requires-explicit-cleanup-hooks]]

**Not loaded:** [[project-pipelines-playbook]] — package/plugin/workflow paths are out of scope.

### Constraints applied before edits

- Work only in the ticket worktree for `tgt_7e208a0c76a44980a83b63af976b1f22`
- Surgical PeerClosed occupancy accounting only; keep owner-check and Core Detach
- Do not import Core generation into Hub attach identity
- Do not change session projection, hub-client DTOs, or data-plane byte paths
- Repo wrapper `./test.sh` for Rust tests; ticket clippy is `--locked -- -D warnings`

## Files changed

| Path | Change |
| --- | --- |
| `src/daemon_transport.rs` | `LocalWebrtcPeerClosed` releases attach occupancy through `record_attached_subscription_change(Detach)` and drops the independent `detach_list.len()` counter subtract / `released_attach_generations` add. Owner-check filter kept. `attach_owner_grant_ids.retain` runs after occupancy release. Core Detach still goes through `detach_local_webrtc_subscriptions`. |
| `src/daemon_attach_stream.rs` | `#[derive(Default)]` on `AttachStreamRegistry`; delete identical manual impl (`clippy::derivable_impls`). |
| `docs/plans/stale-webrtc-peer-attach-snapshot-must-not-detach-replacement-owner.md` | Approved plan (untracked at Implement start). |
| `docs/reports/stale-webrtc-peer-attach-snapshot-must-not-detach-replacement-owner-implement-report.md` | This report. |

## Ownership boundaries preserved

- Hub owns control-plane attach occupancy (`live_attach_routes` / `live_attach_subscriptions`) and PeerClosed forget.
- Core Detach continues through existing `handle_control_request(Detach)`. No Core identity change.
- No hub-client DTO, TypeScript, or hub-test-support package edits.
- No SessionIo / ClientWorker byte-path edits.
- No web / TUI / workspaces / Project Pipelines product work.
- Socket-path `handle_connection_cleanup` independent counter decrement left unchanged.

## Cross-repo dependencies or separately routed work

None. Sibling `ticket_1786663582_169720` (session projection) is not a dependency and was not implemented here.

## Deviations from plan

None.

Optional `daemon_transport` unit-test companion was not added. The existing production-path WebRTC test already drives `handle_control_message` for both the first PeerClosed and the delayed snapshot.

## Runtime-teardown lenses (implemented)

| Lens | Implementation |
| --- | --- |
| Isolation | Successful PeerClosed detaches only attaches whose current `attach_owner_grant_ids` owner is a removed grant or unowned residual. Replacement grant B keeps `(S,X)` and its live peer. |
| Bounds | No new `block_on(close)`. Existing `LOCAL_WEBRTC_PEER_CLOSE_BOUND` and fail-closed runtime drop unchanged. |
| Late-message matrix | Attach/Detach/other tagged Requests keep live-peer fail-closed. Subscribe/Unsubscribe entity owner-check unchanged. PeerClosed attach snapshot is owner-filtered; empty snapshot still sweeps same-grant residuals via the owner index. |
| Production-path proof | peer terminal → `LocalWebrtcPeerClosed` → `handle_control_message` → `remove_peer` → owner-checked sweep → `record_attached_subscription_change` + Core Detach. Oracle: `local_webrtc_stale_peer_attach_snapshot_does_not_detach_replacement_owner` drives the production handler. Live checks: `has_live_peer(B)`, `attach_owner_grant_ids[(S,X)] == B`, `active_subscriptions` contains X, `live_attach_subscriptions` unchanged by the delayed snapshot. |
| Ownership identity | Hub attach owner is `grant_id` keyed by `(session_id, subscription_id)`. Reused ids transfer on successful Attach. Delayed PeerClosed for A cannot delete B. No Core generation imported. |
| Sibling fail-closed | Successful close isolates. Ultimate close failure still drops the shared dedicated runtime (existing tests remain). |

## Tests and downstream proof

Production entry point: `ControlMessage::LocalWebrtcPeerClosed` in `handle_control_message`.

| Command | Result |
| --- | --- |
| `cargo build --locked -p botster-core-daemon --bin botster-session-worker` | exit 0 (already built) |
| `./test.sh local_webrtc_stale_peer_attach_snapshot_does_not_detach_replacement_owner` baseline | red at `src/local_webrtc.rs:5276` `live_attach_before >= 1` |
| same focused test after fix (two runs) | green, 1.60s then 1.40s |
| `./test.sh local_webrtc_attach_owner_sweep_on_empty_snapshot` | green |
| `./test.sh local_webrtc_stale_peer` | green (attach reuse + entity unsubscribe reuse) |
| Ablation 1: drop occupancy recording, restore independent `detach_list.len()` subtract | red at `src/local_webrtc.rs:5276` `live_attach_before >= 1` |
| Ablation 2: keep occupancy fix, owner-check always true | red at `src/local_webrtc.rs:5310` `delayed PeerClosed for A must not detach B's reused attach` |
| `cargo fmt --all -- --check` | exit 0 |
| `git diff --check` | exit 0 |
| `cargo clippy --workspace --all-targets --locked -- -D warnings` | exit 0 |
| `./test.sh --locked` | lib 233/233 green including focused test; then `hub_capability_runtime_test` 11/14. First root: `capability_operations_do_not_block_session_hot_path` timed out waiting for `"ready"` at `tests/hub_capability_runtime_test.rs:250` |
| Same isolated capability command on this branch | exit 101, same assertion |
| Same isolated capability command on base `173e528` | exit 101, same assertion |
| Isolated `cli_local_runtime_up_starts_reuses_and_down_stops_runtime -- --test-threads=1` on this branch | exit 0 |
| `./test.sh --locked --test hub_local_runtime_test` | 1/1 green |
| `./test.sh --locked --test hub_lua_runtime_test` | 32/32 green |
| `./test.sh --locked --test hub_mcp_test` | 7/7 green |
| `./test.sh --locked --test hub_plugin_lifecycle_test` | 7/7 green |
| Isolated `hub_runtime_routes_production_session_verbs_through_core_daemon` on this branch | exit 101, timed out waiting for `"ready"` at `tests/hub_runtime_test.rs:148` |
| Same isolated hub_runtime command on base `173e528` | exit 101, same assertion |
| Isolated `hub_late_attach_fixture_matches_core_snapshot_before_live_ordering` on this branch | exit 101, fixture sequence `[Attaching, History, History, History, Attached, Live]` vs `[Attaching, History, Attached, Live]` |
| Same isolated late-attach fixture command on base `173e528` | exit 101, same assertion |
| Parallel `hub_daemon_lifecycle_test` (169 tests) | hung on several `cli_local_runtime_*` smokes; killed after ~12m. Isolated smoke passes. Matches the lock's known lifecycle-smoke contention. Not introduced by this diff. |
| `cargo test --workspace --locked --exclude botster-hub` | member crates + their doctests green |
| `./test.sh --locked --test update_command_test` | 6 passed, 1 ignored |

## Unverified behavior or residual risk

- Socket-path `handle_connection_cleanup` still independently decrements `live_attach_subscriptions` after Detach. Out of scope; no existing test forced a change.
- `./test.sh --locked` fail-fast still stops on the pre-existing capability-runtime `"ready"` timeout. Isolated on branch and base with identical command and exit 101. The same session-worker `"ready"` banner timeout also fails `hub_runtime_routes_production_session_verbs_through_core_daemon` on branch and base.
- Parallel `hub_daemon_lifecycle_test` can hang under default concurrency on this lock. Isolated smoke passes on this branch.
- Fail-closed still sacrifices siblings on the shared dedicated runtime (existing policy, not retuned).
- No live browser / multi-hour CPU sample (not claimed by this ticket).

## Missing vault guidance discovered

Matches the plan's post-merge capture candidates (not captured here; capture after merge):

1. Hub PeerClosed attach accounting must go through the same `live_attach_routes` occupancy set as Attach/Detach responses. An independent `detach_list.len()` subtract leaves an occupied route and a zero counter, so replacement Attach cannot become live.
2. A red at `live_attach_before >= 1` does not prove the later stale-snapshot owner-check; those are two claims and need separate ablations.

No additional missing convention blocked implementation.
