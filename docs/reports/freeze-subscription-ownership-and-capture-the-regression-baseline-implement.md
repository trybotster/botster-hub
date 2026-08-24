# Implement report: Freeze subscription ownership and capture the regression baseline

## Target repository and target_id

| Field | Value |
| --- | --- |
| Target repository | `botster-hub` (`trybotster/botster-hub`) |
| `target_id` | `tgt_7e208a0c76a44980a83b63af976b1f22` |
| Authoritative spawn target | `botster-hub` at `/Users/jasonconigliari/Projects/botster-hub` |
| Pipeline worktree | this run's Hub worktree |
| Ticket | `ticket_1787600670_129312` |
| Run | `run_1787605830_934897` |
| Step | `botster_stack_implement` (`run_step_1787613456_330395`) |
| Approved plan | `docs/plans/freeze-subscription-ownership-and-capture-the-regression-baseline.md` |
| Plan commit | `dfbf934` |
| Implement commit | `ca77a33e5edb482078b61fe7f452fa8f0e8a9bdd` |
| Base commit | `85a0434` (`origin/main`) |
| Merge policy | direct (no PR required) |
| Locked Core SHA | `7eafa470a18025895995bbedc20d34b58106a03b` |

Routing was resolved from the run `target_id`, not from the process working directory. `list_spawn_targets` maps `tgt_7e208a0c76a44980a83b63af976b1f22` to `trybotster/botster-hub`. The approved plan §1 uses the same mapping. Implementation stayed in this run worktree.

## Repository playbook and other playbooks/notes applied

### Playbooks

- [[implementer-playbook]]
- [[botster-implementer-playbook]]
- [[botster-hub-playbook]] — ownership charter
- [[botster runtime teardown lenses]] — class applies; this ticket adds the §15 characterization tests named in §11.7

Required charter/context notes:

- [[botster-architecture]]
- [[cli-patterns]]
- [[spa-patterns]]

### Targeted atomic notes

- [[core owns duplex terminal transport while Hub stays content blind]]
- [[botster subscriptions use dedicated ordered DataChannels]]
- [[Hub extraction must reduce ownership rather than only split files]]
- [[botster hub gravity must be watched before it becomes the new monolith]]
- [[botster Hub Rust stays a trusted host kernel]]
- [[botster hub is a first party host profile over core]]
- [[botster data plane bypasses the hub through session and client actors]]
- [[lua plugins are the hub composition layer]]
- [[Hub ultimate WebRTC close failure sacrifices every peer on the dedicated runtime]]
- [[WebRTC DataChannel local close uses the peer close bound before cleanup]]
- [[a ready WebRTC send must win over a queued DataChannel close]]
- [[Client event holders are connection-scoped]]
- [[admitted event holders survive producer unload until Core completion]]
- [[terminal transport north star publishes behavioral oracles not numeric budgets]]
- [[Fair host-control writing selects already-admitted frames]]
- [[Fault-injected WebRTC close requires a daemon started with the inject env]]
- [[live hub proof records distinct hub and locked core binary provenance]]
- [[Hub suite runs prebuild the session worker before the locked test wrapper]]
- [[test script required for rust tests not cargo test]]
- [[implementation artifacts must match actual git state]]
- [[implement gate must verify committed work and pr link before review]]
- [[implementation steps must persist report artifacts for review]]
- [[pipeline vault checklists must cite exact resolvable note titles]]

### Explicitly not loaded

- [[project-pipelines-playbook]] — this ticket changes no Project Pipelines package or plugin path

### Constraints applied before edits

- Work only in the Hub run worktree for `tgt_7e208a0c76a44980a83b63af976b1f22`
- Follow the approved plan; do not change transport behavior
- Add the §15 characterization tests that pin current behavior
- Keep Hub charter ownership; do not implement Core ingress, Web, TUI, or Unix entity-stream moves
- Use the charter test sequence, not `./test.sh --workspace`
- Runtime-teardown lenses stay on current behavior; new Reserved-route surfaces stay with `ticket_1787600674_500120`

## Files changed

| Path | Change |
| --- | --- |
| `docs/plans/freeze-subscription-ownership-and-capture-the-regression-baseline.md` | Already committed on the Plan step (`dfbf934`). Unchanged in Implement. |
| `tests/hub_daemon_lifecycle/subscription_ownership_baseline.rs` | New §15 IsolatedHub and source characterization tests |
| `tests/hub_daemon_lifecycle_test.rs` | `include!` of the new file |
| `tests/hub_daemon_lifecycle/webrtc_fixtures.rs` | Extra DataChannel close oracle |
| `src/host_control_fair_write.rs` | `fair_write_class_coverage_per_transport` |
| `src/local_webrtc.rs` | `ultimate_close_failure_sacrifices_every_peer_and_sweeps_all_owners` |
| `docs/reports/freeze-subscription-ownership-and-capture-the-regression-baseline-implement.md` | This report |

## Ownership boundaries preserved

- Hub still admits, hosts, and bounds transport. It does not take Core terminal bytes, attach phases, or Ghostty semantics.
- No extraction, channel rewrite, or JSON-input deletion landed. Those remain with the assigned downstream tickets.
- Lua stays out of terminal input and output. The new `no_lua_dispatch_in_terminal_input_or_output` test pins that.
- Unix entity subscription (`handle_entity_subscription_async`) was not moved.
- `src/runtime.rs`, `src/packages.rs`, `src/main.rs`, `src/daemon_maintenance.rs`, and `src/package_event_router.rs` received no extraction assignment and no edits.

## Cross-repo dependencies or separately routed work

None in this commit. The plan's registered graph stays unchanged:

- `ticket_1787600674_500120` owns WebRTC per-subscription channels and §9 limits
- `ticket_1787603671_590198` depends on that merge, then owns Unix subscription channels
- Core ingress stays on `ticket_1787600672_342292`
- Web, TUI, Restty, and cutover tickets stay on their own targets

## Deviations from plan

No scope change. Implementation notes, not waived requirements:

1. `webrtc_ready_entity_frame_defers_terminal_output` pins the production gate text in `run_data_channel`. A race-free live deferral oracle would need a new flush seam; the plan forbids transport changes.
2. `attach_ready_precedes_history_finish` proves Hello advertisement, post-bind `SendInput`, and no host-plane `FINISH`. Bound adapters keep READY in the terminal plane, so host events do not carry `AttachState { attached }`.
3. `webrtc_peer_rejects_a_second_data_channel` proves the extra channel receives no terminal frames and that `claim_data_channel` plus `local_close` remain on the production path. Offerer `OnClose` is not a reliable webrtc-rs oracle.
4. `ultimate_close_failure_sacrifices_every_peer_and_sweeps_all_owners` drives the existing production handler (`inject_peer_connection_state_for_test(Failed)` plus `force_next_close_error_for_test`). That is the peer-close fail-closed seam. `BOTSTER_HUB_TEST_CLOSE_LOCAL_WEBRTC_OPERATION` injects DataChannel `local_close` after Status and does not exercise dedicated-runtime sacrifice.
5. `wait_for_webrtc_marker` uses a 20 s deadline so suite load cannot hide terminal bytes that already followed attach-state frames.

The committed plan's acceptance checks were not rewritten. These notes do not change §14 or §15 ownership.

## Tests and downstream proof run

Charter sequence:

```sh
cargo build --locked -p botster-core-daemon --bin botster-session-worker
cargo build --locked -p botster-hub
./test.sh --locked
```

Do not use `./test.sh --workspace`.

Named §15 tests run in isolation before the suite:

| Test | Binary | Result |
| --- | --- | --- |
| `webrtc_peer_rejects_a_second_data_channel` | `hub_daemon_lifecycle_test` | pass |
| `webrtc_shared_channel_carries_control_entity_event_and_terminal_frames` | `hub_daemon_lifecycle_test` | pass; first full-suite run failed on an 8 s printf wait after attach-state frames; deadline is now 20 s |
| `webrtc_ready_entity_frame_defers_terminal_output` | `hub_daemon_lifecycle_test` | pass |
| `fair_write_class_coverage_per_transport` | lib | pass |
| `terminal_input_travels_as_a_json_control_request` | `hub_daemon_lifecycle_test` | pass |
| `terminal_adapter_contract_is_egress_only_at_the_locked_core_pin` | `hub_daemon_lifecycle_test` | pass |
| `no_lua_dispatch_in_terminal_input_or_output` | `hub_daemon_lifecycle_test` | pass |
| `attach_ready_precedes_history_finish` | `hub_daemon_lifecycle_test` | pass |
| `shutdown_suppresses_exact_route_generations_before_core_teardown` | `hub_daemon_lifecycle_test` | pass |
| `webrtc_terminal_output_is_byte_exact` | `hub_daemon_lifecycle_test` | pass |
| `peer_close_leaves_sibling_peers_working` | `hub_daemon_lifecycle_test` | pass |
| `ultimate_close_failure_sacrifices_every_peer_and_sweeps_all_owners` | lib | pass |

Locked-suite evidence on this Implement tree:

| Run | Result |
| --- | --- |
| First `./test.sh --locked` (8 s marker wait) | Hub lib 488 passed. Lifecycle 315 passed, 1 failed: `webrtc_shared_channel_carries_control_entity_event_and_terminal_frames` missed `so-4cls-ready` after attach-state `attaching` then `attached`. `unix_eof_skip_core_detach_ablation_keeps_named_pair_on_status` passed. |
| Isolated `webrtc_shared_channel_...` after 20 s wait | pass in 3.14 s |
| Second `./test.sh --locked` (20 s wait) | Hub lib 488 passed. Lifecycle 315 passed, 1 failed: `unix_eof_skip_core_detach_ablation_keeps_named_pair_on_status`. All 12 named §15 tests passed. |
| Isolated `unix_eof_skip_core_detach_ablation_keeps_named_pair_on_status` | pass in 1.91 s. This is a documented workspace-load flake on `origin/main` (see `docs/reports/publish-package-owned-client-notice-reactions-implement.md`). This Implement change does not edit Unix EOF ablation. |
| Third `./test.sh --locked` | pass. Hub lib 488 passed. Lifecycle 316 passed, 2 ignored. Remaining workspace crates and doctests passed. Exit 0 in 392 s. |

Live Hub pin for this worktree:

- Hub binary: `env!("CARGO_BIN_EXE_botster-hub")` from the locked build
- Locked Core revision: `7eafa470a18025895995bbedc20d34b58106a03b`
- Session worker: `cargo build --locked -p botster-core-daemon --bin botster-session-worker`

No Web or TUI consumer artifact was required. This ticket does not change DTOs.

## Unverified behavior or residual risk

- Workspace-load flake `unix_eof_skip_core_detach_ablation_keeps_named_pair_on_status` failed once on the second locked suite. Isolation passed in 1.91 s. The first and third locked suites passed that test. Prior Hub implement reports already record this flake. This change does not edit Unix EOF ablation.
- Offerer-side `OnClose` after Hub rejects a second DataChannel is not a stable oracle.
- The entity-defer gate is pinned by source, not by a live flush-order race.
- New §8.2 Reserved-route late-`open` surfaces do not exist yet. Ticket `ticket_1787600674_500120` owns them.
- A27b Core `WRITE_ATTEMPT_BUDGET` hard-stop through Hub is owned by `ticket_1787600674_500120`.
- Web timing observations remain out of scope (`ticket_1787603669_760394`).
- Ghostty terminal semantics were not changed and were not re-measured.

## Missing vault guidance discovered

Same gaps as plan §18. This ticket did not prove the new channel contract, so it did not capture them:

1. Hub creates subscription DataChannels after admission.
2. A subscription channel label binds identity and generation.
3. Per-channel AES-GCM binds a frame to its subscription.
4. The fair-write scheduler is three-class on WebRTC and two-class on Unix. The new test pins the current call sites; capture still belongs with `ticket_1787600682_233928` when the file is deleted.
5. Hub content-blindness permits transport framing.

No new vault gap beyond those five.

## Runtime-teardown lenses

Class applies. This ticket implements the current-behavior characterization promised by plan §11.7. It does not drop a lens to informal follow-up.

| Lens | Implement evidence |
| --- | --- |
| Isolation | `peer_close_leaves_sibling_peers_working` keeps sibling terminal delivery after a successful peer close |
| Bounds | Existing `LOCAL_WEBRTC_PEER_CLOSE_BOUND` path unchanged; ultimate-close test drives the bound fail-closed handler |
| Late-message matrix | Current surfaces stay covered by existing PeerClosed tests; new Reserved `open` rows stay with ticket 674 |
| Production-path proof | Ultimate close uses `on_connection_state_change` → `cleanup_once`, not a helper-only close |
| Ownership identity | Existing generation-suppression tests remain; `shutdown_suppresses_exact_route_generations_before_core_teardown` re-checks the live suppress-before-Core order |
| Sibling fail-closed | `ultimate_close_failure_sacrifices_every_peer_and_sweeps_all_owners` |

## Assumptions

- The approved plan is the executable architecture contract. This ticket adds tests and does not change transport behavior.
- Merge policy is direct, so no PR link is required for review admission.
- Plan §18 capture remains deferred until a later ticket proves the new contract.
