# Implement report: Freeze subscription ownership and capture the regression baseline

## Target repository and target_id

| Field | Value |
| --- | --- |
| Target repository | `botster-hub` (`trybotster/botster-hub`) |
| `target_id` | `tgt_7e208a0c76a44980a83b63af976b1f22` |
| Authoritative spawn target | the authoritative botster-hub spawn target |
| Pipeline worktree | the pipeline-provided ticket worktree |
| Ticket | `ticket_1787600670_129312` |
| Run | `run_1787605830_934897` |
| Step | `botster_stack_implement` (`run_step_1787628425_109069`) |
| Approved plan | `docs/plans/freeze-subscription-ownership-and-capture-the-regression-baseline.md` |
| Plan commit | `dfbf934` |
| Implement commit | `ca77a33e5edb482078b61fe7f452fa8f0e8a9bdd` |
| Review-return Implement commit | `3f4e0e36f287312d8a111e34342c4a5ba8bd3461` |
| Second Review-return Implement commit | `1e588c38ed8e870c1510e5abfeadf9c8bb0b8beb` |
| Verify-return Implement commit | `d8aa2b96fb8fd1e056231b43c99d6d2d2c226219` |
| Third Review-return Implement commit | `a9863ade179944dd3df0c8f26cb3e292b1e6e829` |
| Fourth Review-return Implement commit | `f79b00b3ef6304bb0cfb2a6caf7b6e719a75c238` |
| Second Verify-return Implement commit | pending; recorded after the work commit |
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
- [[source regex guards can mask behavioral ablations]]
- [[test names do not prove their bodies can fail on the named claim]]
- [[pipeline artifacts should use path neutral worktree references]]

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
| `src/local_webrtc.rs` | Ultimate-close timeout plus Core inventory sweep; test-only extra-channel close marker |
| `src/daemon_transport.rs` | `live_attach_routes` visible for the occupancy-union oracle |
| `tests/hub_daemon_lifecycle/webrtc_fixtures.rs` | Extra DataChannel in the initial offer |
| `tests/hub_daemon_lifecycle/webrtc_terminal_adapter.rs` | IsolatedHub extra-env helper |
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
2. `attach_ready_precedes_history_finish` decodes Ghostty READY then FINISH from the Unix terminal stream, sends input after READY, and keeps host-plane `FINISH` absent.
3. IsolatedHub does not deliver a post-connect extra DataChannel, and a second `LocalWebrtcSignal` cannot renegotiate a redeemed grant. Question `question_1787624446_986511` chose option C: keep both channels in the initial offer and identify the survivor by encrypted Hello. The test does not assume callback order or which label loses. `on_data_channel` calls `claim_data_channel()` before any label await. The label is read only after a lost claim. Observation and close-marker instrumentation stay test-only and do not change claim order. The marker writes for any rejected label after `lost_claim` and `Ok(Ok(()))`. The one-shot negative control stays.
4. `ultimate_close_failure_sacrifices_every_peer_and_sweeps_all_owners` runs the bound-exceeded hang fail-closed path, then the attach fail-closed path, and asserts zero Core inventory rows and zero Hub attach routes before session shutdown.
5. `wait_for_webrtc_marker` uses a 45 s deadline so CPU-load IsolatedHub runs can still observe terminal bytes after attach-state frames. A 20 s bound missed `so-2ch-ready` once under 14 `yes` workers after only attach-state frames arrived.

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
| Review-return isolated `webrtc_peer_rejects_a_second_data_channel` | pass in 4.01 s after Hub close-marker oracle |
| Second Review-return isolated `webrtc_peer_rejects_a_second_data_channel` | pass in 4.18 s after production claim-lost plus successful close |
| Second Review-return isolated `webrtc_peer_rejects_a_second_data_channel_requires_one_shot_claim` | pass in 3.75 s; lost-claim oracle stays empty when one-shot is disabled |
| Second Review-return lib `reject_extra_data_channel_closes_the_unclaimed_channel` | pass |
| Review-return isolated `attach_ready_precedes_history_finish` | pass in 1.63 s after terminal-plane READY then FINISH |
| Review-return isolated `ultimate_close_failure_sacrifices_every_peer_and_sweeps_all_owners` | pass in 3.51 s after timeout hang plus Core inventory sweep |
| Review-return `cargo clippy --workspace --all-targets --locked -- -D warnings` | pass |
| Review-return `cargo fmt --all -- --check` | pass |
| Review-return `./test.sh --locked` | pass. Exit 0 in 394 s. |
| Second Review-return `cargo fmt --all -- --check` | pass |
| Second Review-return `cargo clippy --workspace --all-targets --locked -- -D warnings` | pass |
| Second Review-return first `./test.sh --locked` | Hub lib 489 passed. Lifecycle 314 passed, 3 failed: extra-channel source pin looked for `lost_claim && close_ok` after rustfmt split the condition; `peer_close_leaves_sibling_peers_working` missed `so-sib-a-ready` after attach-state frames. Isolation of the sibling test later passed in 3.37 s. |
| Second Review-return isolated extra-channel tests after source-pin comment | both passed |
| Second Review-return isolated `peer_close_leaves_sibling_peers_working` | pass in 3.37 s |
| Second Review-return `./test.sh --locked` | pass. Hub lib 489. Lifecycle 317 passed, 2 ignored. Exit 0 in 386 s. |
| Verify-return isolated extra-channel test | pass |
| Verify-return extra-channel test under 14 `yes` workers | 3 of 3 passed |
| Verify-return one-shot negative control | pass |
| Verify-return lib label control | pass |
| Verify-return first `./test.sh --locked` | Hub lib 490. Lifecycle 316 passed, 1 failed: `external_hub_live_output_preserves_split_utf8_frames` missed `exited` in 10 s. Isolation later passed in 2.07 s. That file is unchanged on this branch. |
| Verify-return `./test.sh --locked` | pass. Hub lib 490. Lifecycle 317 passed, 2 ignored. Exit 0 in 366 s. |
| Third Review-return isolated extra-channel test | pass in 4.11 s after dual-offer Hello admission |
| Third Review-return extra-channel test under 14 `yes` workers | 3 of 3 passed |
| Third Review-return one-shot negative control | pass in 2.86 s |
| Third Review-return lib marker control | pass |
| Third Review-return `cargo fmt --all -- --check` | pass |
| Third Review-return `cargo clippy --workspace --all-targets --locked -- -D warnings` | pass |
| Third Review-return `./test.sh --locked` | pass. Hub lib 490. Lifecycle 317 passed, 2 ignored. Exit 0 in 420 s. |
| Fourth Review-return isolated extra-channel test | pass in 3.62 s after claim-before-label |
| Fourth Review-return extra-channel test under 14 `yes` workers | 3 of 3 passed |
| Fourth Review-return one-shot negative control | pass in 2.79 s |
| Fourth Review-return lib marker control | pass |
| Fourth Review-return `cargo fmt --all -- --check` | pass |
| Fourth Review-return `cargo clippy --workspace --all-targets --locked -- -D warnings` | pass |
| Fourth Review-return `./test.sh --locked` | pass. Hub lib 490. Lifecycle 317 passed, 2 ignored. Exit 0 in 382 s. |
| Verify-return isolated extra-channel test after terminal-marker restore | pass in 4.28 s, then 3.42 s after the 45 s wait bound |
| Verify-return extra-channel test under 14 `yes` workers | first 45 s-bound campaign: 3 of 3 passed (3.71 s, 3.71 s, 3.35 s). One earlier 20 s-bound run missed `so-2ch-ready` after attach-state frames |
| Verify-return one-shot negative control | pass in 13.75 s (shared compile lock with Clippy) |
| Verify-return `cargo fmt --all -- --check` | pass |
| Verify-return `cargo clippy --workspace --all-targets --locked -- -D warnings` | pass |
| Verify-return `./test.sh --locked` | pass. Hub lib 490. Lifecycle 317 passed, 2 ignored. Exit 0 in 401 s. |

Live Hub pin for this worktree:

- Hub binary: `env!("CARGO_BIN_EXE_botster-hub")` from the locked build
- Locked Core revision: `7eafa470a18025895995bbedc20d34b58106a03b`
- Session worker: `cargo build --locked -p botster-core-daemon --bin botster-session-worker`

No Web or TUI consumer artifact was required. This ticket does not change DTOs.

## Unverified behavior or residual risk

- Workspace-load flake `unix_eof_skip_core_detach_ablation_keeps_named_pair_on_status` failed once on the second locked suite. Isolation passed in 1.91 s. The first and third locked suites passed that test. Prior Hub implement reports already record this flake. This change does not edit Unix EOF ablation.
- Offerer-side `OnClose` after Hub rejects a second DataChannel remains unreliable. The close oracle is the observation file plus the test-only marker written only after a lost claim and `Ok(Ok(()))` from `timeout(local_close)`.
- Current production ownership is arrival-order. The dual-offer IsolatedHub test now accepts either `botster-client` or `botster-extra` as the rejected label. Downstream tickets must replace that with subscription and generation binding.
- `webrtc_peer_rejects_a_second_data_channel` now waits for `so-2ch-ready` on the surviving channel before sampling the rejected channel. That same-window positive control was missing after `d8aa2b9` and is restored for `review_1787628419_852343`.
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
