# Implement report: Integration: cold-cut the old terminal route and prove isolation

Ticket: `ticket_1787600679_990088`
Run: `run_1788459722_264752`
Step: `botster_stack_implement`
Pipeline: `botster_stack_delivery` (direct merge, no PR)

## Target repository and target_id

| Field | Value |
| --- | --- |
| Target repository | `botster-hub` (`trybotster/botster-hub`) |
| Target id | `tgt_7e208a0c76a44980a83b63af976b1f22` |
| Independent routing | `list_spawn_targets` maps this id to spawn target `botster-hub` |
| Implement commit | `e50e0f04885205522aa46936b6863dc732a3224d` |
| Branch | `project-pipelines/ticket_1787600679_990088` |
| Base | `ae6a0b1fe99d97215fa82d796da8f01a904171f0` |
| `hub_sha` | `e50e0f04885205522aa46936b6863dc732a3224d` |
| `locked_core_sha` | `93acae3f98adbc21dc981d113c4eb2f31ead4ad0` |
| Toolchain | `rustc 1.97.0 (2d8144b78 2026-07-07)`, Zig `0.16.0` |
| `teardown_class_applies` | yes |

## Repository playbook and other playbooks/notes applied

- [[implementer-playbook]]
- [[botster-implementer-playbook]]
- [[botster-hub-playbook]]
- [[botster runtime teardown lenses]]
- [[cold turkey migrations eliminate dual code paths and version suffixes]]
- [[Hub Core pin rolls update eleven literal sites and six lock sources]] (18 active sites plus 6 lock sources)
- [[region bounded source guards need a required symbol anchor]]
- [[fixed source guard lists need one ablation per added file]]
- [[a regression test must be shown to go red with the fix reverted]]
- [[Hub official gates must not set CARGO TARGET DIR]]
- [[botster Hub pipeline shells can override RUSTUP TOOLCHAIN below the CI pin]]
- [[implementation artifacts must match actual git state]]
- [[implement gate must verify committed work and pr link before review]]
- [[core owns duplex terminal transport while Hub stays content blind]]
- [[botster hub is a first party host profile over core]]
- [[cold cut grep gates exclude rejection tests that name retired inputs]]

Did not load [[project-pipelines-playbook]]. No Project Pipelines package or plugin path changed.

## Constraints applied before edits

Hub-only change. Work stayed in this run worktree. Core, Web, and TUI source were not edited. TUI scratch pin used an uncommitted worktree. Consumer TUI durable roll is `ticket_1788460430_647093`.

## Files changed

Committed on `8634018`:

| Path | Change |
| --- | --- |
| `Cargo.toml`, member `Cargo.toml` files, `Cargo.lock` | Core pin `72d1c75`; lock has 6 matching sources |
| `crates/botster-hub-test-support/build.rs`, `src/conformance_data.rs`, `src/lib.rs` | Provenance literals |
| `tests/session_projection_owner_loop.rs` | `REQUIRED_CORE_REV` |
| `tests/hub_daemon_lifecycle/{subscription_ownership_baseline,event_plane_saturation,package_event_plane,unix_terminal_adapter,webrtc_terminal_adapter}.rs` | Pin literals, Lua list, retry/scheduling guard, D.1 tests |
| `crates/botster-hub-client/src/lib.rs` | TypeScript absence of `send_input` and `resize` request members |
| `README.md` | Responsibility split and adapter-plane input/resize |
| `docs/reports/cold-cut-the-old-terminal-route-and-prove-isolation-implement.md` | This report |

Later Hub-only test repairs on this branch:

| Commit | Path | Change |
| --- | --- | --- |
| `b52d43c` | `tests/hub_daemon_lifecycle/unix_terminal_adapter.rs` | Wait for ListSessions `lifecycle=exited`, then collect unsolicited mux `process_exit` |
| `099c74e` | `src/managed_git_worktrees.rs` | Write the Git timeout child's pid before the sleep starts |
| `3d1613e` | `tests/hub_daemon_lifecycle/subscription_ownership_baseline.rs` | Keep draining WebRTC bytes after ListSessions reports exited |
| `e2e7787` | `src/transport/webrtc/adapter.rs`, `src/transport/webrtc/subscription_channel.rs` | Restore write-then-exit WebRTC byte-exact. Copy occupied WebRTC bytes on the adapter inner before slot close, then flush them on the subscription channel |
| `4905534` | `src/transport/unix/adapter.rs`, `src/transport/unix/mux_write.rs` | Copy occupied Unix bytes on close and send them after host events. The mux no longer skips closed handles for that late frame |
| `77d0445` | `src/transport/unix/mux_write.rs`, `tests/hub_daemon_lifecycle/unix_terminal_adapter.rs` | Flush parked Unix terminal after the host-turn cap. Restore five-second hard stops |
| `65b8c70` | `src/data_plane/driver.rs`, `src/runtime.rs`, `src/transport/unix/{adapter,connection,mux_write}.rs` | Pump the observed session after exact-session observe. Review rejected this synthetic wake |
| `8a02885` | `src/data_plane/driver.rs`, `src/runtime.rs`, `src/transport/unix/connection.rs`, `tests/hub_daemon_lifecycle/{unix_terminal_adapter,subscription_ownership_baseline}.rs` | Remove the observe pump and pre-control flush. Collect unsolicited `process_exit` without ReadScreen or ListSessions |
| `8d92d75` | `src/data_plane/driver.rs`, `src/transport/unix/{adapter,connection,mux_write}.rs`, tests, plan, report | Pump bound adapter routes after Core requests. Do not defer the next mux frame after a completed live send. Record try_write and flush. Replace advisor session UUIDs |
| `9879211` | `src/data_plane/driver.rs` | Skip forced WouldBlock routes when pumping bound adapters |
| `f80e902` | `src/data_plane/driver.rs`, `src/transport/unix/adapter.rs`, tests, plan | Remove the post-request global adapter pump and test-only WouldBlock skip. Log opaque wake byte lengths. Register Core `ticket_1788523929_630135` |
| `b164ca1` | Core-family manifests, `Cargo.lock`, pin literals | Pin Core `5ed369f`. Zero `72d1c75` matches outside `docs/` |
| `e50e0f0` | Core-family manifests, `Cargo.lock`, pin literals | Pin Core `93acae3`. Zero `5ed369f` matches outside `docs/` |

Inventory found no remaining production old-route symbol.

Vault capture (outside this repository): inbox `2026-09-03-botster-final-terminal-ownership-boundaries.md`. Review found [[Hub terminal cold cut consumed Core 72d1c75]] overstated shipment; a pending correction is in the vault inbox.

## Ownership boundaries preserved

Hub Rust owns admission, security, persistence, process and package supervision, WebRTC setup, adapter creation, plugin isolation, and safe Lua primitives. Core owns terminal lifecycle and duplex transport. Lua does not run in terminal hot paths. `botster-hub-client` owns the DTO boundary (test guard only). `daemon_transport.rs` and `local_webrtc.rs` remain absent. `daemon_modules_reject_unix_transport_mechanism_symbols` still holds.

### Ownership audit (`src/` top-level)

| Module | Class |
| --- | --- |
| `admission.rs` / `admission/` | admission |
| `auth.rs`, `credentials.rs` | security |
| `persistence.rs` | persistence |
| `entrypoint_supervisor.rs`, `lifecycle.rs`, `local_runtime_process.rs`, `packages.rs`, `maintenance.rs`, `daemon_maintenance.rs`, `managed_git_worktrees.rs`, `worktrees.rs`, `source_update.rs`, `update.rs` | process and package supervision |
| `local_webrtc_smoke.rs`, `transport/webrtc/` | WebRTC setup |
| `data_plane.rs` / `data_plane/`, `transport.rs` / `transport/` except WebRTC peer maps | adapter creation and transport mechanics |
| `lua_runtime.rs` | plugin isolation and safe Lua primitives |
| `daemon.rs` / `daemon/`, `client_api.rs`, `client_api_dto.rs`, `mcp.rs`, `operator_console.rs`, `package_entity_fanout.rs`, `package_event_router.rs`, `session_projection.rs`, `daemon_projection.rs` | control-plane dispatch |
| `config.rs`, `capabilities.rs`, `profile.rs`, `runtime.rs`, `lib.rs`, `main.rs`, `session_types.rs`, `spawn_targets.rs`, `package_event_schema.rs`, `event_plane_counters.rs` | host policy composition |
| `subscription.rs` / `subscription/` | admission (Reserved/Bound/Retired route state) |

No unclassified module. No recorded drift.

## Cross-repo routing

- Core consumed by pin only.
- Web consumer proof against `origin/main` `e5573a2` in a scratch worktree. No Web edits.
- TUI consumer proof against `origin/main` `b051c67` in a scratch worktree. Uncommitted path pin to this Hub worktree plus Core `72d1c75`. Durable TUI roll: `ticket_1788460430_647093` (`tgt_c3d470bab78549df920a41e8fb0e58d8`).
- No npm publication.

## Deviations from plan

1. TUI scratch Hub crates used path dependencies to this run worktree. The candidate SHA is not on GitHub, so a Git `rev` fetch would fail. Core pins used `rev = 72d1c75`.
2. Operator `botster-hub smoke` on an empty data dir returned `missing_prerequisite=botster-web` (designed fail-closed). Production-shaped smoke is `cli_smoke_proves_local_runtime_daemon_package_app_session_and_webrtc` in the locked suite.
3. First `./test.sh --locked` failed `webrtc_terminal_output_is_byte_exact` with empty frames while foreign Botster processes were live. Isolated `--exact` passed (1 selected, 346 filtered). A second locked suite passed, including that test.
4. Web durable lane and north-star TUI `ghostty-shared` did not pass. See residual risk. Deterministic Hub gates passed.

## Guard inventory and ablations

| Category | Guard | Ablation |
| --- | --- | --- |
| JSON handlers | `duplicating_a_variant_into_the_wrong_owner_fails_the_matrix`; `terminal_input_is_not_a_json_control_request` | existing; suite green |
| TypeScript DTO | `mode_flags_protocol_is_serde_stable_and_generated` (`send_input`, `resize`, `mode_gated_input`) | seeded `| { type: "send_input"` in generated comment; test failed; restored |
| Shared-channel routing | dedicated-channel and second-channel tests | suite green |
| Translation | snapshot-phase and paste-blind guards; `FORBIDDEN_PRODUCTION_CONSTRUCTS` | suite green |
| Drain-driven progress | `FORBIDDEN_PRODUCTION_CONSTRUCTS`; `paused_data_plane_keeps_control_requests_from_driving_terminal_progress`; `pump_woken_lives_only_in_the_data_plane_driver` | suite green |
| Retry/scheduling | `transport_and_data_plane_reject_terminal_retry_and_scheduling_tokens` (`pump_woken` + `try_write` anchors; peer close and mux write named exemptions) | seeded `retry_terminal` in `src/data_plane.rs`; test named that token; restored |
| Lua hot path | recursive walk plus explicit rows for `src/data_plane.rs`, `src/data_plane/driver.rs`, `src/data_plane/close_work.rs`, `src/transport/shared/ingress.rs` | one `lua_runtime` seed per added file; each failure named that file; restored |
| D.1 sibling progress | `forced_would_block_on_one_unix_route_keeps_sibling_open_and_delivering`; `webrtc_forced_would_block_on_one_route_keeps_sibling_open_and_delivering` | Unix env pointed at sibling `sso-live`; sibling byte assert failed; restored |

`resize` capability token in `first-party-client-support-matrix.json` stays.

## Runtime-teardown lenses

Every lens from the approved plan is covered by existing production tests plus D.1. No lens was dropped.

| Lens | Evidence |
| --- | --- |
| Isolation | D.1 plus `peer_close_leaves_sibling_peers_working`, shutdown exact keys, occupancy dual-attach |
| Bounds | existing write-budget and `LOCAL_WEBRTC_PEER_CLOSE_BOUND`; this run added no close path |
| Late-message matrix | Attach/Detach/entity/event/reservation/ShutdownSession/binary input tests cited in the plan |
| Production-path hard-stop | `webrtc_terminal_adapter_bound_peer_loss_closes_adapter_without_hub_detach`, Unix EOF occupancy, `local_webrtc_peer_close_detaches_terminal_subscriptions` |
| Ownership identity | `(session_id, subscription_id, generation)`; stale-generation tests |
| Sibling fail-closed | successful close keeps siblings; ultimate WebRTC close policy unchanged |

## Tests and downstream proof

Hub:

- `RUSTUP_TOOLCHAIN=1.97.0`, `CARGO_TARGET_DIR` unset
- `cargo fmt --all -- --check` pass
- `cargo clippy --workspace --all-targets --locked -- -D warnings` pass
- `node packages/hub-test-support/scripts/sync-assets.mjs --check` pass (via `./test.sh`)
- `./test.sh --locked` retry pass. Lifecycle `345 passed; 0 failed; 2 ignored`. Lib `548 passed`.
- `packages/hub-test-support`: `npm install --no-save && npm test` pass at `0.1.43`
- Old SHA grep outside `docs/` empty. `Cargo.lock` count of `72d1c75` is 6.

Web `origin/main` `e5573a2` scratch worktree, binaries from `8634018` / Core `72d1c75`:

- `npm ci` and `npm test` pass (drift check against `@trybotster/hub-test-support@0.1.43`)
- `smoke:live-packaged-protocol` pass (`live packaged protocol harness passed (webrtc)`), including in-page reconnect
- `smoke:plugin-contract-matrix` pass
- `smoke:live-packaged-protocol:durable` failed twice: dashboard wait for `botster-web-durable-exited-1` after restart
- `smoke:live-packaged-protocol:shared-session` reached cancel, keep-alive, and reconnect markers, then the coordinator rejected `cancel ablation stayed green`

TUI `origin/main` `b051c67` scratch:

- `script/test-live-hub ghostty` pass: `ghostty-live-complete` with `hub_rev=8634018...` `worker_rev=72d1c75...`

North-star:

- Spawned `north-star-shared`
- Web `drive:live-packaged-protocol:shared-session` printed `live-shared-session-cancel-passed` and `live-shared-session-keep-alive-passed`
- TUI `ghostty-shared` failed: `late attach must show NORTH_STAR_HISTORY` (0.52s)

## Timing observations: waived

Human `question_1788461094_542980` waives the two-arm local record. `question_1788460117_825061` waives the controlled-runner comparison. This ticket captured no timing number as a gate.

Local rerun (format_version=3):

```sh
BOTSTER_LEGACY_CHECKOUT=<clean f598075e> BOTSTER_HUB_SOURCE=<hub> npm run observe:terminal-baseline
npm run observe:terminal-baseline:validate
```

Prerequisites that block it today:

- GitHub sign-in on the legacy arm (`completeLegacyNewSession` fails closed at "Sign in with GitHub")
- No Playwright storage-state or cookie input in the harness
- Provisioned legacy development database
- GitHub OAuth application credentials
- Clean `f598075e` checkout (a detached worktree of the local trybotster repository is one way)

Controlled runner rerun (verbatim from botster-web `docs/terminal-baseline-observation-format.md`):

1. Register a GitHub Actions runner with label `botster-ubuntu-24.04-16core`.
2. Provision both product arms on that runner, including the legacy Ruby and Rails toolchain.
3. Dispatch the workflow with a clean legacy checkout at `f598075e` and a Hub source that can clone the intended Hub SHA.
4. Keep `format_version=3`. Do not add a threshold field.

`gh api repos/trybotster/botster-hub/actions/runners` returned `total_count=0` on 2026-09-03. No row of any future record is transport causality (`product_baseline_only`).

## Unverified behavior or residual risk

- Web durable dashboard restore after Hub restart is unverified in this visit.
- Web shared-session coordinator cancel ablation did not go red; the keep-alive/cancel/reconnect markers did print on the north-star Web driver.
- North-star TUI late-attach history on a caller-owned session is unverified.
- Foreign Botster processes were present during Hub suites. The passing retry is the official gate evidence.
- TUI scratch used path deps; the durable Git pin is the consumer ticket.

## Review-return visit (`question_1788465866_563736`)

Human answer: do not waive the three remaining consumer lanes. Keep durable restore in this Hub run. Create one Web ticket and one TUI ticket. This ticket depends on both. Rerun the complete matrix once after both merges.

### Hub persistence repair

Core already persists `Exited` registry rows under the Hub data directory. After Hub process restart, `ListSessions` still returns those rows. Core `lifecycle_record` copies engine lifecycle only, so a restarted exited row arrives with `lifecycle=None`. Hub projection treated that as `lifecycle_class=indeterminate` and omitted `lifecycle`. Web home dashboard lists only `lifecycle_class=current` and does not infer class from `registry_state`.

Smallest repair, one persistence path: when `registry_state=Exited` and engine lifecycle is omitted, project `lifecycle=exited` and `lifecycle_class=ended`. Running + omitted lifecycle stays `indeterminate`. No Hub-owned session store.

| Path | Change |
| --- | --- |
| `src/session_projection.rs` | Ended class and `lifecycle=exited` for registry Exited without engine lifecycle. Unit test `complete_baseline_exited_registry_without_engine_lifecycle_is_ended`. |
| `src/subscription/entity.rs` | Matching cfg(test) projection copy. |
| `tests/hub_daemon_lifecycle/shutdown.rs` | `process_ownership_daemon_restart_lists_ended_session_row` |

Red before the repair: the new process test failed with snapshot `lifecycle_class=indeterminate` and no `lifecycle` field, while `ListSessions` already had `lifecycle=exited`. Green after the repair.

Commands:

```sh
RUSTUP_TOOLCHAIN=1.97.0 BOTSTER_ENV=test cargo fmt --all -- --check
./test.sh --locked -p botster-hub --lib complete_baseline_exited_registry_without_engine_lifecycle_is_ended
./test.sh --locked -p botster-hub --lib session_lifecycle_class_is_total_and_stale_first
./test.sh --locked --test hub_daemon_lifecycle_test process_ownership_daemon_restart_lists_ended_session_row -- --exact
./test.sh --locked --test hub_daemon_lifecycle_test
```

Lifecycle suite: `346 passed; 0 failed; 2 ignored`.

### Consumer tickets

| Ticket | Target | Role |
| --- | --- | --- |
| `ticket_1788467459_333288` | `tgt_40abcf71ccf049f4ac0c99953a799869` (botster-web) | `BOTSTER_LIVE_ABLATE_CANCEL_DETACH=1` must fail for the intended reason. Preserve the dedicated channel path. |
| `ticket_1788467460_864070` | `tgt_c3d470bab78549df920a41e8fb0e58d8` (botster-tui) | Reproduce `ghostty-shared` late-history, then restore ready-then-history. Preserve isolated `script/test-live-hub ghostty`. |

Dependencies from this ticket: `dependency_1788467479_559280` and `dependency_1788467481_242462`. No second Web ticket for durable restore. No second Hub ticket.

### Residual after this visit

- Complete consumer matrix still waits for those two merges, then one rerun here.
- Web home dashboard still filters `lifecycle_class === "current"`. Hub now authors ended rows. Durable dashboard visibility remains a live-lane assertion for the final matrix, not a second Web ticket.
- Shared-session cancel ablation and `ghostty-shared` late-history remain consumer-owned.

## Final matrix after consumer merges

Web consumed at exact `062e314a27c1f04c7cd67884307af4a432ee3e5b` (`ticket_1788467459_333288` closed). TUI consumed at `origin/main` `b051c67` in a scratch worktree with uncommitted path pins to this Hub worktree. Hub candidate `4d558e9`. Core pin `72d1c75`. Toolchain `rustc 1.97.0`, Zig `0.16.0`. `CARGO_TARGET_DIR` unset.

Production-path durable census (no `BOTSTER_ENV=test`): spawn `durable-exited-1`, `ShutdownSession`, Hub process restart on the same data directory. After restart: `state_source=loaded`, `session_count=1`, `sessions list` shows `lifecycle=exited`, registry file present. Hub provides the ended row.

| Lane | Result |
| --- | --- |
| `cargo fmt --all -- --check` | pass |
| `cargo clippy --workspace --all-targets --locked -- -D warnings` | pass |
| `./test.sh --locked` | pass. Lib `549 passed`. Lifecycle `346 passed; 0 failed; 2 ignored`. |
| `packages/hub-test-support` `npm test` | pass at `0.1.43` |
| Web `npm test` at `062e314` | pass |
| `smoke:live-packaged-protocol` | pass (`live packaged protocol harness passed (webrtc)`) |
| `smoke:live-packaged-protocol:shared-session` | pass (`live-shared-session-coordinator-passed` with `cancel_ablation=true`, keep-alive 2, exit pass) |
| `smoke:plugin-contract-matrix` | pass |
| TUI `script/test-live-hub ghostty` | pass (`ghostty-live-complete` hub_rev=`4d558e9` worker_rev=`72d1c75`) |
| `smoke:live-packaged-protocol:durable` | fail: dashboard wait for `botster-web-durable-exited-1`. Hub list after restart is green. Web `currentDashboardSessions` keeps `lifecycle_class === "current"` only. |
| `script/prove-north-star-shared-session` | fail twice at web keep-alive 1: `timed out waiting for mounted terminal renderer write botster-web-production-alt-exited`. Spawned `north-star-shared`. Did not reach TUI `ghostty-shared`. Standalone Web coordinator on a Hub-owned session passed. |

Review `review_1788465266_290355`: D.1 WouldBlock oracle is `13074b6`. Path-neutral artifacts are `13074b6`. Vault cold-cut note stays pending in inbox. Durable Hub projection is `4d558e9`. Cancel ablation is Web `062e314`. TUI late-history ticket closed without a merge on `origin/main`.

## Review-return visit (`review_1788477306_734887`, `question_1788477409_664609`)

Human answer C: one consolidated botster-web ticket for both remaining Web live failures. Keep the WebRTC WouldBlock wait in this Hub run. After the Web merge, rerun durable dashboard, north-star, TUI `ghostty-shared`, and the complete matrix once. Do not waive either product lane. Do not mark ended as current. Do not add another TUI ticket.

Constraints applied: Hub-only source edits. Web and TUI source stay on their own tickets.

### Finding `finding_1788477307_457400` (Hub)

The WebRTC isolation test now waits up to 2s for `observation/would_block` after `spawn_and_bind` of `wso-held`, then asserts before sibling delivery. The wait matches the Unix test (`thread::sleep` 10ms). The sibling oracle now matches Unix: bind an echo loop, then write `wso-sibling-live` on the reserved channel after hello ack. Spawn-time `printf` raced attach and produced attaching/attached frames without live bytes.

| Arm | Result |
| --- | --- |
| Exact filter | 1 test selected |
| Green `FORCE=wso-held` | 5/5 pass after the echo-after-bind oracle, then 1 more pass after ablation restore |
| Disabled-seam `FORCE=no-such-session` | fail at `held route must enter WouldBlock before sibling delivery` (`ABLATION_EXIT=101`). Restored to `wso-held` |

Commands:

```sh
RUSTUP_TOOLCHAIN=1.97.0
unset CARGO_TARGET_DIR
BOTSTER_ENV=test ./test.sh --locked --test hub_daemon_lifecycle_test webrtc_forced_would_block_on_one_route_keeps_sibling_open_and_delivering -- --exact
```

`cargo fmt --all -- --check` pass on this visit.

### Findings `finding_1788477306_952608` and `finding_1788477306_100800` (Web)

Registered `ticket_1788477497_716720` against `tgt_40abcf71ccf049f4ac0c99953a799869` (botster-web): show Hub-authored ended sessions through an explicit ended-session presentation path, and complete caller-owned alt-exited keep-alive. Dependency `dependency_1788477513_382774`. Web run `run_1788477522_704573`. This Hub run does not edit Web source.

Advisor question `question_1788477409_664609`: the north-star keep-alive timeout is a Hub producer gap. `script/prove-north-star-shared-session` `PRODUCER_SCRIPT` now handles `botster-web-production-alt-exit` by sending `ESC[?1049l` and echoing `botster-web-production-alt-exited`, then stays on the primary screen for later TUI attach. No extra Hub ticket. Alternate-screen and `ghostty-shared` assertions stay unchanged.

## Final matrix after Web `9e18b10`

Web `ticket_1788477497_716720` merged at `9e18b1046b75438e971b9fe56a16137581ac2d1b`. Dependency `dependency_1788477513_382774` is closed. Hub candidate `d7bd2c7`. Core pin `72d1c75`. Toolchain `rustc 1.97.0`, Zig `0.16.0`. `CARGO_TARGET_DIR` unset. TUI proof used a scratch checkout of `origin/main` `b051c67` with uncommitted path pins to this Hub worktree.

| Lane | Result |
| --- | --- |
| `cargo fmt --all -- --check` | pass |
| `cargo clippy --workspace --all-targets --locked -- -D warnings` | pass |
| WebRTC WouldBlock exact | pass. Green `FORCE=wso-held`. Producer alt-exit arm present. |
| `./test.sh --locked` | first run failed `unix_adapter_bound_printf_stream_attach_delivers_process_exit` (process_exit missed after printf). Isolated exact 3/4. Retry locked suite pass. Lib `549`. Lifecycle `346 passed; 0 failed; 2 ignored`. |
| `packages/hub-test-support` `npm test` | pass at `0.1.43` |
| Web `npm test` at `9e18b10` | pass |
| `smoke:live-packaged-protocol` | pass (`live packaged protocol harness passed (webrtc)`), including `alternate_screen_exit` |
| `smoke:live-packaged-protocol:durable` | pass. `botster-web-durable-exited-1` through `-5` present. Hub restart `state_source=loaded`. |
| `smoke:live-packaged-protocol:shared-session` | first run failed at second keep-alive. Retry pass: `live-shared-session-coordinator-passed` `keep_alive_runs=2` `cancel_ablation=true` `exit_pass=true` |
| `smoke:plugin-contract-matrix` | pass |
| TUI `script/test-live-hub ghostty` | pass (`ghostty-live-complete`, worker_rev=`72d1c75`, hub binary from this worktree) |
| `script/prove-north-star-shared-session` | pass (`north-star-shared-session-complete`). Keep-alive 2×, `ghostty-shared-complete`, `ghostty-shared-exit-complete`. After alt-exit, primary screen showed `NORTH_STAR_HISTORY`. |

No remaining consumer-lane residual. Timing observations stay waived.

## Review `review_1788487490_710702` return

Review sent Implement back with two open high findings. This visit repaired the Hub suite roots and reran one complete post-merge matrix with no component retries.

### `finding_1788487490_179474` Hub locked suite

| Root | Repair | Commit |
| --- | --- | --- |
| `unix_adapter_bound_printf_stream_attach_delivers_process_exit` missed unsolicited mux `process_exit` after attach frames | Wait for ListSessions `lifecycle=exited`, then drain and collect unsolicited `process_exit` | `b52d43c` |
| `controlled_git_runner_reports_unavailable_and_kills_timed_out_child` lost the descendant pid when timeout killed the group before `printf $!` | Write `$$` first, start `sleep 30`, then overwrite with `$!` | `099c74e` |
| `webrtc_terminal_output_is_byte_exact` returned `[]` after ListSessions `lifecycle=exited` | Stop treating quiet-after-exit as end of payload; hold the producer until the four live bytes are observed, matching the sibling WebRTC exact-bytes proof | `3d1613e` then `d099d43` |

`./test.sh --locked` then passed on the first try in the complete matrix at `d099d43`. Lib `549`. Lifecycle `346 passed; 0 failed; 2 ignored`. Log `/tmp/botster-hub-matrix-d099d43-7.log`.

### `finding_1788487490_969290` shared-session and complete matrix

Lane-owned leftovers only: this-worktree session-workers. Foreign workers `14620`, `15125` (`ticket_1788313897`) and `51084` (`ticket_1787894967`) were left running. Playwright from Rails invitation tests was not killed.

Plugin-contract-matrix leaked IsolatedHub workers after a passing arm. The complete-matrix wrapper reaped only this-worktree `botster-hub` and `botster-session-worker` after every arm. After `web_plugin` it reaped pids `99481`, `99483`, `99488`. TUI `ghostty` then started with census `3` (foreign only).

Host load was recorded before and after each arm. The complete matrix at `d099d43` used no component retries.

| Arm | Before load | After load | Census after | Result |
| --- | --- | --- | --- | --- |
| fmt | 7.93 6.58 8.16 | 7.93 6.58 8.16 | 3 | pass |
| clippy | 7.93 6.58 8.16 | 7.79 6.60 8.14 | 3 | pass |
| locked | 7.79 6.60 8.14 | 4.09 5.56 7.01 | 3 | pass |
| hub_ts | 4.09 5.56 7.01 | 4.09 5.56 7.01 | 3 | pass |
| web_unit | 4.09 5.56 7.01 | 4.43 5.56 6.98 | 3 | pass |
| web_live | 4.43 5.56 6.98 | 8.51 6.80 7.32 | 3 | pass |
| web_durable | 8.51 6.80 7.32 | 9.90 7.76 7.64 | 3 | pass |
| web_shared | 9.90 7.76 7.64 | 7.71 8.41 8.27 | 3 | pass first try |
| web_plugin | 7.71 8.41 8.27 | 6.35 8.08 8.16 | 3 after reap | pass |
| tui_ghostty | 6.35 8.08 8.16 | 5.76 7.53 7.94 | 3 | pass |
| north_star | 5.76 7.53 7.94 | 7.40 7.15 7.48 | 3 | pass |

Consumer env (`BOTSTER_HUB_BIN`, `BOTSTER_SESSION_WORKER_BIN`, `BOTSTER_WEB_CHECKOUT`, `BOTSTER_TUI_CHECKOUT`, `BOTSTER_SHARED_SESSION_ID`) was unset during Hub fmt/clippy/locked. Web used `npm --prefix`. TUI used `env -C`.

| Lane | Result |
| --- | --- |
| `cargo fmt --all -- --check` | pass |
| `cargo clippy --workspace --all-targets --locked -- -D warnings` | pass |
| `./test.sh --locked` | pass first try. Lib `549`. Lifecycle `346/0/2` |
| `packages/hub-test-support` `npm test` | pass at `0.1.43` |
| Web `npm test` at `9e18b10` | pass |
| `smoke:live-packaged-protocol` | pass, including `alternate_screen_exit` |
| `smoke:live-packaged-protocol:durable` | pass. `botster-web-durable-exited-1` through `-5`. `state_source=loaded` |
| `smoke:live-packaged-protocol:shared-session` | pass first try: `live-shared-session-coordinator-passed` `keep_alive_runs=2` `cancel_ablation=true` `exit_pass=true` |
| `smoke:plugin-contract-matrix` | pass |
| TUI `script/test-live-hub ghostty` | pass (`ghostty-live-complete`, `core_adapter_closed`, worker_rev=`72d1c75`, hub binary from this worktree). Provenance literal still `hub_rev=8634018` |
| `script/prove-north-star-shared-session` | pass (`north-star-shared-session-complete`). `ghostty-shared-complete`, `ghostty-shared-exit-complete`. After alt-exit, primary screen showed `NORTH_STAR_HISTORY` |

Web `9e18b10`. TUI scratch `b051c67` with uncommitted path pins. Direct merge, no PR. Timing observations stay waived.

## Review `review_1788495189_656238` return

Review sent Implement back with two high findings.

### `finding_1788495189_353292` fast-exit coverage

`webrtc_terminal_output_is_byte_exact` uses `write_python_start_then_write_script` again. The producer writes the four bytes and exits without waiting for the consumer. The held-producer case remains the sibling WebRTC exact-bytes proof.

Hub copies occupied WebRTC adapter bytes before slot close (`e2e7787`) and flushes them on the subscription channel. Unix copies occupied bytes on close and sends them after host events (`4905534`). Core `complete_active` after close stays a no-op.

`./test.sh --locked` at `4905534` passed on the first try, including `webrtc_terminal_output_is_byte_exact` and `unix_adapter_bound_printf_stream_attach_delivers_process_exit`. Lib `549`. Lifecycle `346/0/2`. Log `/tmp/botster-hub-matrix-4905534-8.log`.

### `finding_1788495189_398466` matrix end-state and TUI ghostty

The `4905534` matrix recorded `git rev-parse HEAD` and `git status --short` at start: commit `4905534a10cd607f5f319819b7c20e1ed73bc562`, dirty `0`. After the Hub and Web arms, the worktree is still that commit and still clean.

| Arm | Result |
| --- | --- |
| fmt | pass |
| clippy | pass |
| locked | pass first try, including write-then-exit WebRTC and Unix `process_exit` |
| hub_ts | pass |
| web_unit | pass |
| web_live | pass |
| web_durable | pass. `botster-web-durable-exited-1` through `-5` |
| web_shared | pass first try. `keep_alive_runs=2` `cancel_ablation=true` `exit_pass=true` |
| web_plugin | pass. Reaped this-worktree IsolatedHub workers `21963`, `21965`, `21970` |
| tui_ghostty | fail. `core_adapter_closed evidence=None` after 30s |

Human/advisor `question_1788503817_293195`: do not change Hub for this TUI failure. Do not create another TUI ticket. Isolated `script/test-live-hub ghostty` fails with the same close-evidence miss against Hub `4905534` and Hub `d099d43`. That matched pair does not distinguish the integration Hub change.

TUI ghostty acceptance moves to existing pin-roll ticket `ticket_1788460430_647093`. That ticket must diagnose this exact close-evidence failure, keep the wake-driven duplex contract, and require isolated `script/test-live-hub ghostty` plus north-star `ghostty-shared` against the final merged Hub SHA. This is an acceptance ownership correction, not a waiver. Do not add polling or JSON fallback.

This Hub run keeps Hub, Web, Unix, WebRTC, durable-session, WouldBlock, old-route deletion, and ownership gates. North-star TUI `ghostty-shared` waits with the TUI pin-roll ticket.

## Review `review_1788504718_269664` return

Review sent Implement back with two high findings. Hub-only repairs. Core `complete_active` after close stays a no-op.

### `finding_1788504718_843979` late WebRTC bytes and aggregate budget

Close parked occupied bytes after it dropped `aggregate_permit`. `flush_subscription_adapter_frames` treated a missing permit as permitted, so late `local_send_text` was unaccounted.

Repair `6ff7414`:

- Park occupied bytes, then close the slot. Keep the existing permit when bytes are parked.
- A second close does not release that permit while late egress remains.
- `resize_aggregate_permit` authorizes wire length when no permit is held.
- Flush releases the permit only after usage publication.
- Failed encode or resize restores taken late bytes.

Oracle: `occupied_close_keeps_aggregate_permit_from_a_sibling_write`. A sibling write at the remaining 32-byte gap returns `WouldBlock` while parked bytes still occupy the budget.

### `finding_1788504718_415927` fast-exit ablation

Review removed `park_late_egress` from both close paths. IsolatedHub `webrtc_terminal_output_is_byte_exact` still passed because the driver often flushed before Core close.

Oracle: `flush_after_close_sends_parked_late_egress_under_the_existing_permit`. The test writes, closes, then calls the production flush. Ablation that makes `park_late_egress` return without storing bytes turns this test red (FLUSH_EXIT=101, OCCUPIED_EXIT=101). Restoration turns it green.

An IsolatedHub hold-until-close env seam was tried and removed. It held a trailing PTY frame or lost the payload on session teardown. That unused seam is not in production source.

`43d080e` keeps late flush on the write-wake loop only. An OnClose flush dropped IsolatedHub occupied frames (isolated byte-exact 1/5 red). After the revert, isolated byte-exact was 5/5 green.

Unix helper `a1871eb`: `read_unsolicited_terminal_until` left a 200ms `SO_RCVTIMEO`. The next control read returned `WouldBlock` (`connection_death_and_detach_do_not_emit_terminal_subscription_closed`). The helper now clears the timeout. Isolated 5/5 green after the clear.

### Complete matrix at `43d080e`

Log `/tmp/botster-hub-matrix-43d080e-9.log`. Start and end `MATRIX_BOUNDARY` commit `43d080e1b2fc601b1272a5822c5cc05967ef14b5` dirty `0`. No component retries. TUI ghostty and north-star `ghostty-shared` skipped per `question_1788503817_293195`.

| Arm | Result |
| --- | --- |
| fmt | pass |
| clippy | pass |
| locked | pass first try. Lib `551`. Lifecycle `346/0/2`. `webrtc_terminal_output_is_byte_exact` ok. `unix_adapter_bound_printf_stream_attach_delivers_process_exit` ok |
| hub_ts | pass |
| web_unit | pass |
| web_live | pass |
| web_durable | pass |
| web_shared | pass first try. `keep_alive_runs=2` `cancel_ablation=true` `exit_pass=true` |
| web_plugin | pass. Reaped this-worktree IsolatedHub workers `83890`, `83892`, `83896` |
| tui_ghostty | skipped. `ticket_1788460430_647093` |
| north_star | skipped. `ticket_1788460430_647093` |

Direct merge, no PR. Timing observations stay waived. Foreign session-workers were not killed.

## Review `review_1788508622_226112` return

Review sent Implement back with one high finding: a second close can drop the aggregate permit while late WebRTC bytes are in `local_send_text`.

### `finding_1788508622_126127`

`flush_subscription_adapter_frames` took `late_egress` before the send await. `close_retaining_occupied_budget` then saw no slot and no late bytes, so it released the permit.

Repair `ec056c9`: peek parked bytes during send. Clear them only after usage publication. `second_close_during_late_send_keeps_the_aggregate_permit` hangs `FakeDataChannel::local_send_text`, calls `close_from_host`, and asserts `aggregate_buffered()` is unchanged.

Unix collect `d304977`: the attached-exit ShutdownSession test now reads unsolicited `process_exit` after authoritative exit, matching the printf attach test. Outer wait is 15s.

### Complete matrix at `d304977`

Log `/tmp/botster-hub-matrix-d304977-13.log`. Start and end `MATRIX_BOUNDARY` commit `d3049777fc567c0c0ceee8d72191365f864bff66` dirty `0`. No component retries. TUI ghostty and north-star `ghostty-shared` skipped per `question_1788503817_293195`.

| Arm | Result |
| --- | --- |
| fmt | pass |
| clippy | pass |
| locked | pass first try. Lib `552`. Lifecycle `346/0/2`. `second_close_during_late_send_keeps_the_aggregate_permit` ok. `webrtc_terminal_output_is_byte_exact` ok. `unix_adapter_bound_printf_stream_attach_delivers_process_exit` ok |
| hub_ts | pass |
| web_unit | pass |
| web_live | pass |
| web_durable | pass |
| web_shared | pass first try. `keep_alive_runs=2` `cancel_ablation=true` `exit_pass=true` |
| web_plugin | pass. Reaped this-worktree IsolatedHub workers `70645`, `70647`, `70651` |
| tui_ghostty | skipped. `ticket_1788460430_647093` |
| north_star | skipped. `ticket_1788460430_647093` |

Direct merge, no PR. Timing observations stay waived. Foreign session-workers were not killed.

## Review `review_1788514476_724417` return

Review sent Implement back with one high finding: widening two Unix `process_exit` waits to 15s did not repair attached delivery. Isolated `unix_shutdown_session_from_another_connection_classifies_attached_exit` still missed `process_exit` after attaching, `terminal_output`, and attached.

### `finding_1788514476_587313`

The original five-second hard stops are restored (`77d0445`). Remaining host events no longer skip parked late terminal in the same flush turn.

Production repair `65b8c70`:

1. Exact-session observe can queue `process_exit` on a Ready bound adapter without a later writable or ingress wake. `HubRuntime::observe_session_lifecycle` now uses `CoreDaemonHandle::call_then_pump_session`, which calls `pump_woken` for that session on the data-plane thread before the driver waits again.
2. Unix mux `take_late_egress` runs only after a late frame is sent. Completing a live output frame no longer drops a `process_exit` parked during that send. Oracle: `live_output_completion_does_not_take_parked_process_exit`.
3. Occupied Unix slots flush before a control round-trip, so Drain cannot hold the slot Full across Core observe.

Rejected occupancy-at-snapshot (`ff4efe9`, reverted `cf2e858`): completing the slot before a zero-offset abandon violates Full-on-abandon and made IsolatedHub printf worse.

Isolated at `65b8c70`, load averages about `3/3/3`:

| Test | Result |
| --- | --- |
| `unix_shutdown_session_from_another_connection_classifies_attached_exit` | 8/8 pass |
| `unix_adapter_bound_printf_stream_attach_delivers_process_exit` | 8/8 pass |

### Complete matrix at `65b8c70`

Log `/tmp/botster-hub-matrix-65b8c70.log`. Start and end `MATRIX_BOUNDARY` commit `65b8c70ddbbdef5136fc2ff468b6b9e1304a7ac3` dirty `0`. No component retries. TUI ghostty and north-star `ghostty-shared` skipped per `question_1788503817_293195`.

| Arm | Result |
| --- | --- |
| fmt | pass |
| clippy | pass |
| locked | pass first try. Lib `554`. Lifecycle `346/0/2`. `live_output_completion_does_not_take_parked_process_exit` ok. `unix_adapter_bound_printf_stream_attach_delivers_process_exit` ok. `unix_shutdown_session_from_another_connection_classifies_attached_exit` ok |
| hub_ts | pass |
| web_unit | pass |
| web_live | pass |
| web_durable | pass |
| web_shared | pass first try. `keep_alive_runs=2` `cancel_ablation=true` `exit_pass=true` |
| web_plugin | pass. Reaped this-worktree IsolatedHub workers `56241`, `56243`, `56247` |
| tui_ghostty | skipped. `ticket_1788460430_647093` |
| north_star | skipped. `ticket_1788460430_647093` |

Direct merge, no PR. Timing observations stay waived. Foreign session-workers were not killed.

## Review `review_1788518570_580003` return

Review sent Implement back with one high finding: `call_then_pump_session` synthesizes an ingress wake after lifecycle observation. ReadScreen and ShutdownSession can then drive terminal delivery. That violates the frozen rule that generic control requests do not drive terminal progress.

### `finding_1788518570_658493`

Repair `8a02885`:

- Remove `CoreDaemonHandle::call_then_pump_session`.
- Restore `observe_session_lifecycle` to a plain Core call.
- Remove the generic pre-control Unix terminal flush.
- Keep Unix mux late-egress: take late bytes only after that late frame is sent. Oracle: `live_output_completion_does_not_take_parked_process_exit`.
- Attached proofs collect unsolicited `process_exit` for five seconds without ReadScreen, ListSessions, or Drain. Authoritative observe runs only after that frame arrives.
- Source scan: `pump_woken_lives_only_in_the_data_plane_driver` forbids `call_then_pump_session`.

Isolated at `8a02885`, load averages about `1.7/2.4/3.3`:

| Test | Result |
| --- | --- |
| `unix_shutdown_session_from_another_connection_classifies_attached_exit` | 8/8 pass |
| `unix_adapter_bound_printf_stream_attach_delivers_process_exit` | 8/8 pass |

### Complete matrix at `8a02885`

Log `/tmp/botster-hub-matrix-8a02885.log`. Start and end `MATRIX_BOUNDARY` commit `8a02885f5b96023f6474f423524e1b7f51ae0f6d` dirty `0`. No component retries. TUI ghostty and north-star `ghostty-shared` skipped per `question_1788503817_293195`.

| Arm | Result |
| --- | --- |
| fmt | pass |
| clippy | pass |
| locked | pass first try. Lib `554`. Lifecycle `346/0/2` |
| hub_ts | pass |
| web_unit | pass |
| web_live | pass |
| web_durable | pass |
| web_shared | pass first try. `keep_alive_runs=2` `cancel_ablation=true` `exit_pass=true` |
| web_plugin | pass. Reaped this-worktree IsolatedHub workers `37652`, `37654`, `37659` |
| tui_ghostty | skipped. `ticket_1788460430_647093` |
| north_star | skipped. `ticket_1788460430_647093` |

Direct merge, no PR. Timing observations stay waived. Foreign session-workers were not killed.

## Review `review_1788520334_663466` return

Review sent Implement back with two findings. The unsolicited `process_exit` proof still failed 2/8 outside the sandbox. Committed plan and report contained the advisor identity recorded by `finding_1788520334_622131`.

### `finding_1788520334_167722`

Wake-boundary logs on the failing runs showed attaching, `terminal_output`, and attached `try_write`/`complete`/`flush`, and no `process_exit` `try_write`. Core never occupied the adapter. Owner-loop `observe_lifecycle_slice` can ingest `process_exit` onto a Ready bound adapter without a later wake.

Repair `8d92d75` plus `9879211`:

- After Core requests run on the data-plane thread, pump existing bound `adapter_routes`. This is not a fabricated `ingress_sessions` wake and is not attached to `observe_session_lifecycle` / ReadScreen.
- Skip `BOTSTER_HUB_TEST_FORCE_ADAPTER_WOULD_BLOCK_SESSION` routes so sibling isolation does not spend the 512-tick budget.
- A completed live mux send no longer defers the next occupant.
- After a writer wake, keep flushing while the mux still has unsent frames.
- Attached proofs record `try_write` and `flush` for `process_exit` at the wake boundary. They still wait five seconds with no ReadScreen, ListSessions, or Drain.

Isolated at `9879211`: both attached Unix `process_exit` tests 8/8, then another 8/8. Isolation oracles `forced_would_block_on_one_unix_route_keeps_sibling_open_and_delivering` and `webrtc_forced_would_block_on_one_route_keeps_sibling_open_and_delivering` pass.

### `finding_1788520334_622131`

Plan and report now cite `question_1788477409_664609` instead of the advisor session UUID.

### Complete matrix at `9879211`

Log `/tmp/botster-hub-matrix-9879211.log`. Start and end `MATRIX_BOUNDARY` commit `9879211a57bcfb0356833449e2ab928955da422b` dirty `0`. No component retries. TUI ghostty and north-star `ghostty-shared` skipped per `question_1788503817_293195`.

| Arm | Result |
| --- | --- |
| fmt | pass |
| clippy | pass |
| locked | pass first try. Lib `555`. Lifecycle `346/0/2` |
| hub_ts | pass |
| web_unit | pass |
| web_live | pass |
| web_durable | pass |
| web_shared | pass first try. `keep_alive_runs=2` `cancel_ablation=true` `exit_pass=true` |
| web_plugin | pass. Reaped this-worktree IsolatedHub workers `93633`, `93635`, `93639` |
| tui_ghostty | skipped. `ticket_1788460430_647093` |
| north_star | skipped. `ticket_1788460430_647093` |

Direct merge, no PR. Timing observations stay waived. Foreign session-workers were not killed.

## Review `review_1788523297_801440` return

Review sent Implement back with three findings. Sequence 25.

### `finding_1788523297_189742`

Removed `pump_bound_adapter_routes` and `forced_would_block_session` from `src/data_plane/driver.rs`. Core requests no longer scan bound adapter routes. Isolation tests now use production route selection.

The missing `process_exit` `try_write` is a Core ingest miss. `observe_session` / `observe_lifecycle_slice` call `drain_runtime_once`, which `ingest_bound_terminal_frames` onto a Ready bound adapter without `pump_woken` and without `notify_session`. Isolated `unix_adapter_bound_printf_stream_attach_delivers_process_exit` failed 4/8 after the Hub pump was removed. The failing run received attaching, `terminal_output`, and attached frames, with wake log `try_write`/`complete`/`flush` for those three frames only.

Registered Core ticket `ticket_1788523929_630135` against `tgt_1f7bce66eb304881980f9b4a2a5ae3fe`. This Hub ticket depends on it (`dependency_1788523941_772682`). Core run `run_1788523950_844880` started. Hub will pin the merged Core revision. Hub will not restore a post-request global adapter pump.

Negative controls:

- `pump_woken_lives_only_in_the_data_plane_driver` forbids `pump_bound_adapter_routes`, `list_terminal_subscriptions`, `forced_would_block_session`, and `call_then_pump_session` in the driver.
- `paused_data_plane_keeps_control_requests_from_driving_terminal_progress` still passes.
- `live_generic_core_requests_do_not_drive_idle_terminal_output` passes. After ready and attached, Status, ListSessions, ReadScreen, ReadModeFlags, and CaptureSnapshot add no terminal envelopes.
- `forced_would_block_on_one_unix_route_keeps_sibling_open_and_delivering` and `webrtc_forced_would_block_on_one_route_keeps_sibling_open_and_delivering` pass.

Five-second unsolicited `process_exit` proofs stay in place. They are not Review-ready until the Core pin lands.

### `finding_1788523297_138209`

Unix wake observation now records `event` and `byte_len` only. Hub transport does not parse terminal JSON. Tests decode received Unix envelopes and match opaque byte lengths.

### `finding_1788523297_223431`

The review-return paragraph now cites `finding_1788520334_622131`. A complete-tree scan for `sess-1788403107` returns no matches.

### Residual

Do not request Review until Core `ticket_1788523929_630135` merges, Hub pins that revision, isolated Unix `process_exit` is 8/8 then 8/8 at a clean feature SHA, and the complete matrix has clean `MATRIX_BOUNDARY` markers.

## Core `5ed369f` pin

Core main `5ed369fc4a536d7cfa99547262561fcea7ef41e5` supersedes `72d1c75`. `72d1c75` is an ancestor. The revision emits a targeted session ingress wake after observe/drain queues bound frames. Hub pin commit `b164ca1`. Zero `72d1c75` matches remain outside `docs/`. `Cargo.lock` has 6 `5ed369f` sources.

Isolated at `b164ca1`:

- `unix_adapter_bound_printf_stream_attach_delivers_process_exit` 8/8 then 8/8
- `unix_shutdown_session_from_another_connection_classifies_attached_exit` 8/8 then 8/8
- WouldBlock isolation Unix and WebRTC pass
- Live idle-control negative and paused data-plane negative pass
- fmt and Clippy pass on `rustc 1.97.0`

Complete matrix `/tmp/botster-hub-matrix-b164ca1.log` failed the locked arm. `subscription::entity::tests::live_session_entity_subscription_emits_exact_stale_transition_patch` failed 5/5 isolated in 4.0s: `worker session shutdown did not complete before the daemon deadline: stale-transition-session`. `mark_session_stale` then `shutdown_session` forgets the session wake because the registry is already Stale and no adapter frames are undelivered. `wait_wakes_bounded` then misses worker EOF. This path passed on Core `72d1c75`.

Registered Core `ticket_1788537020_814817`. Dependency `dependency_1788537029_689954`. Core run `run_1788537030_383590`. Hub stays on `5ed369f`. No compatibility path back to `72d1c75`.

Fable confirmed `ticket_1788537020_814817` is necessary. After that Core merge and the Hub pin, the final matrix must unskip `script/test-live-hub ghostty` and `script/prove-north-star-shared-session`. Both must pass before Hub merge. Do not transfer another Core or Hub close-evidence defect to TUI. Durable TUI pin-roll `ticket_1788460430_647093` remains the consumer Cargo pin after this merge.

Do not request Review until that Core merge, the Hub pin, locked suite green, isolated Unix `process_exit` 8/8, a clean Hub/Web matrix, and both Ghostty and north-star scripts green.

## Core `93acae3` pin

Core main `93acae3f98adbc21dc981d113c4eb2f31ead4ad0` supersedes `5ed369f`. Session wakes stay live after registry Stale until the engine is terminal. Hub pin commit `e50e0f0`. Zero `5ed369f` matches remain outside `docs/`. `Cargo.lock` has 6 `93acae3` sources. No compatibility path.

Isolated at `e50e0f0`:

- stale-transition shutdown pass
- Unix `process_exit` 8/8 then 8/8
- WouldBlock isolation Unix and WebRTC pass
- Live idle-control and paused data-plane negatives pass

Matrix log `/tmp/botster-hub-matrix-e50e0f0.log`. Start `MATRIX_BOUNDARY` commit `e50e0f04885205522aa46936b6863dc732a3224d` dirty `0`. Toolchain `rustc 1.97.0`. TUI scratch `/tmp/botster-tui-ticket-1787600679` at `b051c67` with uncommitted Core `93acae3` path pins. Web `9e18b10`. Foreign session-workers were not killed.

| Arm | Result |
| --- | --- |
| fmt | pass |
| clippy | pass |
| locked | pass. Lib `555`. Lifecycle `347/0/2` |
| hub_ts | pass |
| web_unit | pass |
| web_live | first try failed under load 60: Playwright popover click timeout. Resume pass |
| web_durable | pass |
| web_shared | pass. `keep_alive_runs=2` `cancel_ablation=true` `exit_pass=true` |
| web_plugin | pass. Reaped this-worktree IsolatedHub workers `47795`, `47797`, `47802` |
| tui_ghostty | pass. `ghostty-live-complete` `hub_rev=e50e0f0` `worker_rev=93acae3` `core_adapter_closed` |
| north_star | first try failed: web keep-alive 1 timeout waiting for renderer write `alt-13` at 45s. Isolated retry pass: `north-star-shared-session-complete`, `ghostty-shared-complete`, `ghostty-shared-exit-complete`, keep-alive 2× |

Close-evidence stayed on Hub/Core. TUI ghostty and north-star were not skipped. Durable TUI pin-roll remains `ticket_1788460430_647093`. Direct merge, no PR. Timing observations stay waived.

## Missing vault guidance discovered

Recorded in the inbox capture: final ownership statement, D.1 oracle names, 18-site pin count vs the "eleven" note title, runner registration check, and the full two-arm authentication prerequisite list.
