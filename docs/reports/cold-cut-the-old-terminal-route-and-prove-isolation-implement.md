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

Hub-only change. Work stayed in this run worktree. Core, Web, and TUI source were not edited. Temporary TUI proof lives at `/tmp/botster-tui-ticket-1787600679` on local branch `proof/ticket_1787600679-e50e0f0` commit `38e5717`. That branch is not merged or pushed. Consumer TUI durable roll is `ticket_1788460430_647093`.

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
| `99d9a9e` | this report | Replace remaining advisor-session identifier wording |

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
- TUI consumer proof against local committed scratch `38e5717` (`proof/ticket_1787600679-e50e0f0`). Path pin to this Hub worktree plus Core `93acae3`. Do not merge or push that branch. Durable TUI roll: `ticket_1788460430_647093` (`tgt_c3d470bab78549df920a41e8fb0e58d8`).
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

The review-return paragraph now cites `finding_1788520334_622131`. A complete-tree scan for advisor session identifiers returns no matches.

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

## Review `review_1788545287_586937` return

Review sent Implement back with two high findings. Sequence 27. Existing medium finding `finding_1788523297_223431` stayed open.

### `finding_1788545287_923360`

The bounced `e50e0f0` matrix had `ARM_FAIL` on `web_live` and `north_star`, no end `MATRIX_BOUNDARY`, and spliced isolated retries. That log is not the Review subject.

The Review-ready subject is one complete run: `/tmp/botster-hub-matrix-99d9a9e-final.log`. Start and end `MATRIX_BOUNDARY` Hub `99d9a9efee4e1fd35a100431e1bfe96d61da2a88` dirty `0`, TUI `38e5717e2253cfafa6718d8b7424ff68fd9fda88` dirty `0`. `ARM_FAIL` count is `0`. Every arm is `ARM_PASS` on first try. `MATRIX_COMPLETE` start and end SHAs match. Feature pin stays `e50e0f0`. This report commit is docs-only after that matrix.

### `finding_1788545287_970173`

TUI proof is now a committed local revision. Path `/tmp/botster-tui-ticket-1787600679`, branch `proof/ticket_1787600679-e50e0f0`, commit `38e5717e2253cfafa6718d8b7424ff68fd9fda88`. `git status --porcelain` is empty at both matrix boundaries. The branch path-pins Hub crates in this worktree and Core `93acae3`. Do not merge or push that TUI branch. Durable consumer pin-roll remains `ticket_1788460430_647093`. Both TUI lanes ran inside the same complete matrix.

### `finding_1788523297_223431`

Hub commit `99d9a9e` already replaced the remaining identifier wording. A complete-tree scan for advisor session identifiers returns no matches on this worktree, including this report.

### Complete matrix at `99d9a9e`

Log `/tmp/botster-hub-matrix-99d9a9e-final.log`. Toolchain `rustc 1.97.0`. Web `9e18b1046b75438e971b9fe56a16137581ac2d1b`. Foreign session-workers were not killed. Start load `10.52 9.54 10.52`. End load `11.75 10.08 9.84`.

| Arm | Result |
| --- | --- |
| fmt | pass first try |
| clippy | pass first try |
| locked | pass first try. Lib `555` in 17.12s. Lifecycle `347/0/2` in 300.09s |
| hub_ts | pass first try |
| web_unit | pass first try |
| web_live | pass first try |
| web_durable | pass first try |
| web_shared | pass first try. `keep_alive_runs=2` `cancel_ablation=true` `exit_pass=true` |
| web_plugin | pass first try. Reaped this-worktree IsolatedHub workers `86544`, `86546`, `86550` |
| tui_ghostty | pass first try. `ghostty-live-complete` `hub_rev=99d9a9efee4e1fd35a100431e1bfe96d61da2a88` `worker_rev=93acae3f98adbc21dc981d113c4eb2f31ead4ad0` `ghostty_rev=eb72ec61304ea256be1d86ed8fa961c84e43ecbd`. Write-budget `core_adapter_closed generation=Some(1)` |
| north_star | pass first try. `north-star-shared-session-complete` `ghostty-shared-complete` `ghostty-shared-exit-complete` |

Direct merge, no PR. Timing observations stay waived. Close-evidence stayed on Hub/Core. Do not splice earlier failed runs as evidence.

Residual after this complete matrix:

- Earlier aborted or spliced matrices are not passing product evidence.
- TUI proof uses Hub path pins. Durable Git pin remains `ticket_1788460430_647093`.

## Review `review_1788548922_449717` return

Review sent Implement back with `finding_1788548922_130878`. Sequence 29. The one-shot matrix at `99d9a9e` stayed complete and clean. Review required attribution or repair of the two earlier lane failures before merge.

### `finding_1788548922_130878`

The spliced `e50e0f0` matrix is not the merge-gate subject. It recorded two failures. Those runs are not passing product evidence.

1. `web_live` at load `55.82 34.78 20.09`. Playwright `locator.click` timeout `30000ms` in `botster-web` `scripts/live-packaged-protocol-harness.mjs` `setSessionTypeFormSelect` (about line 5529). Locator `ion-popover` / `ion-radio` text `Relative path under source root` resolved, then stayed outside the viewport after scroll. The stack never entered Hub terminal transport.
2. `north_star` at load `12.58 22.96 39.68`. `waitForTerminalRendererWrite` timeout `45000ms` for `alt-13-mtn908yf-live` in the same Web harness (about line 6998). That wait inspects browser `renderer_write` telemetry. It does not wait for daemon `terminal_output`. A later `/tmp/botster-hub-matrix-99d9a9e-rerun.log` durable fail used the same wait at `alt-19-mtnai3mr-live`.

Human `question_1788549952_531189` chose B. Do not open a Web ticket. Classify those two signatures as Web-harness and host-load failures outside this Hub change.

Attribution commands used Web `9e18b10`, `RUSTUP_TOOLCHAIN=1.97.0`, `CARGO_TARGET_DIR` unset, `BOTSTER_ENV=test`. Logs `/tmp/botster-hub-attr-ae6a0b1.log` and `/tmp/botster-hub-attr-continue.log`.

| Arm | Hub | Worker | Load | Result |
| --- | --- | --- | --- | --- |
| `smoke:live-packaged-protocol` | head `63a36ce` | `93acae3` | `9.03 9.50 10.41` | pass |
| `smoke:live-packaged-protocol:durable` | head `63a36ce` | `93acae3` | `11.33 9.92 10.45` | fail. Different signature: `waitForDaemonTerminalOutputBytes` `nul/esc/invalid` at 45s |
| same durable retry | head `63a36ce` | `93acae3` | `10.60 10.17 10.42` | pass |
| `smoke:live-packaged-protocol` | base `ae6a0b1` | `48a4370` | `10.35 10.06 10.34` | pass |
| `smoke:live-packaged-protocol:durable` | base `ae6a0b1` | `48a4370` | `15.76 11.84 10.97` | fail. `timed out waiting for durable seeded session botster-web-durable-exited-1 lifecycle_class ended`. This is the ended-row gap this Hub branch already repaired |

The original two signatures did not reproduce on those isolated arms. The matched-base ended-row failure is evidence for the Hub product repair. It is not the Playwright popover signature and it is not the renderer-write signature.

Merge gate remains `/tmp/botster-hub-matrix-99d9a9e-final.log` at clean Hub `99d9a9efee4e1fd35a100431e1bfe96d61da2a88` and TUI `38e5717e2253cfafa6718d8b7424ff68fd9fda88`. `11` `ARM_PASS`. `0` `ARM_FAIL`. Start and end boundaries match. Do not require another full matrix unless Hub source or a consumed revision changes after `99d9a9e`. This report commit is docs-only.

Direct merge, no PR. Timing observations stay waived.

## Missing vault guidance discovered

Recorded in the inbox capture: final ownership statement, D.1 oracle names, 18-site pin count vs the "eleven" note title, runner registration check, and the full two-arm authentication prerequisite list.


## 2026-09-04 hard-close correction and focused verification

This correction supersedes earlier sections that require `late_egress` after adapter close. Those requirements conflict with Core's contract.

Target: `botster-hub`, target_id `tgt_7e208a0c76a44980a83b63af976b1f22`. Run `run_1788459722_264752` uses the registered Hub worktree and branch `project-pipelines/ticket_1787600679_990088`. The approved plan routes to this same target. The starting commit was `11facecf371271907f7d20d83e390601c4011966`.

Applied guidance: `implementer-playbook`, `botster-implementer-playbook`, `botster-hub-playbook`, and `botster runtime teardown lenses`. Targeted notes include `Core subscription hard-stop is synchronous close and drop on the host tick`, `terminal adapters emit coalesced writable and closed wakes`, and `a ready WebRTC send must win over a queued DataChannel close`. Earlier client, wrapper, exact-pin, and process-ownership guidance still applies. This correction changes no Project Pipelines package or plugin path.

The coordinator authorized removal of the invalid replay workaround in `msg_plugin-w_1788586011_ef7122`. The coordinator clarified partial-envelope completion in `msg_plugin-w_1788586619_fbc1fa`. Core published that clarification at `c47eadbf476501ec611e18572d8e4afc87d4304d`, in `crates/botster-core/src/contract/terminal_adapter.rs`. This is contract documentation authority, not a dependency roll. Hub remains pinned to Core `93acae3f98adbc21dc981d113c4eb2f31ead4ad0`.

Changed files and behavior:

- `src/transport/unix/adapter.rs`: remove occupied-frame parking and post-close snapshots.
- `src/transport/unix/mux_write.rs`: abandon unsent copies after close. Keep the slot Full until the complete envelope is written. Finish an already-started envelope once from its existing buffer before host and sibling frames.
- `src/transport/webrtc/adapter.rs`: remove occupied-frame parking. Close uses a nonblocking permit-lock attempt. Contended holders release abandoned authorization after unlocking. Write admission returns Full on lock contention. Completion releases authorization before the final writable wake.
- `src/transport/webrtc/subscription_channel.rs`: cancel pending sends on adapter close. Transfer the authorized wire-byte bound into channel usage before sending. Refresh usage after completion or cancellation. Failed or timed-out channel close invokes existing peer supervision. The wrapper does not request ordinary channel retirement after peer cleanup starts.
- `src/admission/connection_budget.rs`: read authorization before published usage. Add a deterministic transfer-between-reads test.
- `src/transport/webrtc/test_support.rs`: add bounded fake-channel controls for partial-send and failed-close proofs.
- `tests/hub_daemon_lifecycle/webrtc_fixtures.rs`: preserve the label and underlying receive error before removing a failed subscription mailbox.
- `tests/hub_daemon_lifecycle/webrtc_terminal_adapter.rs`: include those errors in the existing ProcessExit failure report.
- This implementation report records the corrected contract and evidence.

Ownership boundaries remain intact. Hub handles opaque terminal bytes and transport accounting. Hub adds no semantic decoder, retry queue, compatibility path, public API, manifest change, or dependency pin change. Core, TUI, and Web source remain unchanged. TUI ticket `ticket_1788460430_647093` remains separately routed. The coordinator controls subsequent consumer repinning and the complete matrix. This run uses direct merge; no PR is required.

Ordering argument: the old inner completion could not release a successor permit while the old permit still occupied its mutex. The later unconditional flush release had no such ownership protection. The correction removes that release. For transfer, the writer adds the complete wire bound to usage before releasing authorization. A reader that sees the old authorization counts that bound. A reader that acquires the released authorization then reads the prior usage publication. `try_extend_authorized` already reads authorization first, reads usage, and uses compare-exchange; a changed authorization retries. The new snapshot test forces a transfer between the actual reads. Restoring the old usage-first order makes its assertion fail.

Normal channel retirement follows the channel driver and successful local close. Failed close publishes `ChannelError` and invokes `cleanup_once`. The wrapper suppresses ordinary retirement when cleanup has started. The production `LocalWebrtcPeerClosed` handler calls `remove_peer` before removing the connection budget (`src/daemon/control/webrtc.rs`). Existing peer supervision closes the peer or drops the dedicated runtime on ultimate failure. The new fake-channel tests prove the driver retains usage and requests supervision while the fake transport remains live. Existing real-peer module tests prove the subsequent handler and runtime teardown. These are separate proof stages.

Runtime teardown lenses:

- `teardown_isolation`: a successful adapter close abandons that route's unsent bytes. Unix host and sibling frames continue. WebRTC cancellation leaves a sibling adapter writable.
- `teardown_bounds`: Core close performs no I/O wait and does not wait for the writer's permit lock. Channel close retains the existing three-second production bound and 200-millisecond unit-test bound. Failure enters existing peer supervision.
- `late_message_matrix`: the approved plan's grant, peer-generation, route-generation, and owner-tagged admission matrix remains unchanged. Closed adapters reject further writes. Copied but unsent Unix frames cannot start after close. Failed-channel cleanup follows the existing peer admission rejection and owner sweep.
- `production_path_proof`: `connection.rs` calls `flush_unix_mux_writes`; the bound WebRTC driver calls `flush_subscription_adapter_frames`. Tests use those functions. Real lifecycle checks use the built Hub and pinned worker. The WebRTC module also exercises real peer-close supervision and worker teardown.
- `ownership_identity`: this correction preserves reservation labels, route generations, host/Core close causes, and existing replacement-owner checks. The failure path uses the existing peer cleanup identity rather than fabricating a route owner.
- `sibling_fail_closed_policy`: successful close preserves healthy siblings. A failed local channel close requests peer supervision. Only ultimate peer-close failure invokes the existing dedicated-runtime sacrifice policy. No Unix socket-wide failure was added for adapter close.

The partial-write rule does not retract bytes from a nonblocking write already in progress when close occurs. If that attempt accepts bytes, its existing envelope can finish. If it makes no progress, no new attempt starts after close. The deterministic Unix test requires one complete envelope, then host response/close event and sibling output, with no replay.

Verification used Rust 1.97.0, two Cargo jobs, `BOTSTER_ENV=test`, the repository `./test.sh --locked` wrapper, and the default target directory. Every supervised arm had a 300-second watchdog and identity-scoped cleanup. The six negative controls each failed at the intended assertion: pending-send cancellation, accepted-byte transfer, failed-close supervision, unsent Unix copy, partial-write Full, and snapshot read order. Each ran one test with 562 filtered tests. Source restoration was exact.

Final focused results:

| Check | Result |
| --- | --- |
| Pinned worker build and Hub build | pass |
| `cargo fmt --all -- --check` | pass |
| `cargo clippy --workspace --all-targets --locked -- -D warnings` | pass |
| WebRTC module | 85 passed, 478 filtered; its child proof also passed 1 test |
| Unix module | 25 passed, 538 filtered |
| Connection budget module | 7 passed, 556 filtered |
| Unix printf ProcessExit | 1 passed, 349 filtered |
| WebRTC exact bytes | 1 passed, 349 filtered |
| Combined WebRTC detach/peer-death/ProcessExit/shutdown | 1 failed, 349 filtered |
| Ended-session rejection and host-close positive | not run in this final sequence because the previous arm failed |

The final ProcessExit failure includes admitted input, 19 bytes of terminal output, and an underlying `channel_closed` error. The label is `r-0f18b190a958e6ec40453e76456cb38c`; the recorded chunk state is `message_id=pending next_chunk=0 expected_chunks=pending`. The output decodes to `wnx-release\r\ndone\r\n`. No ProcessExit arrived. The log does not map that channel label back to a reservation owner, so this report does not attribute the closed channel to `sub-exit`.

An earlier focused run passed all five lifecycle checks before the complete accounting correction. That pass does not attribute the historical failure or waive the final failure. Earlier setup errors (`MissingType` in the new fake frame and an incorrect outer pressure call) were corrected; their failed logs remain preserved.

Final lifecycle Hub SHA256: `6ab3c4217f32ea0a3aac86242736739780adb5a6e1668efb588bf9736fcbdcbc`. Worker SHA256: `7358688b9025fe3bdeda74fe7df8edbfa3f242af8cbc556bd7955b7fae98631b`. The build and unit arms used the separately recorded build hash. Receipts preserve each arm's binary identity.

Evidence is under `/tmp/hub-hard-close/final-focused`, `/tmp/hub-hard-close/ablations`, and `/tmp/hub-hard-close/partial-conflict`. The original rejected retention patch and controls remain under `/tmp/hub-write-ownership`; `artifact_1788585929_754863` records that stopped approach. The strict no-more-bytes partial-Unix experiment failed before the Core owner clarified framing. That temporary test was removed and replaced with the explicit framing test.

The final supervisor reports no owned survivors before or after cleanup. No cleanup signal was needed. The five approved foreign workers remain unchanged, and the Botster zombie census is empty. The build window was released to the coordinator after the failed arm. No later build or test ran.

Residual blocker: unsolicited ProcessExit delivery remains intermittent and unattributed. This candidate is for source and log review; it cannot advance the pipeline. No complete matrix, downstream rerun, merge, or pin change occurred. No lens is waived. Missing durable guidance was the conflict between earlier Hub retention reports and Core close semantics; Core now documents the boundary, and this report supersedes the invalid local requirements. No new vault exception was created.

### 2026-09-04 idempotent close correction

Target: `trybotster/botster-hub`, `tgt_7e208a0c76a44980a83b63af976b1f22`.
The coordinator authorized this separate correction after review of `bbecda6`.

`LocalWebrtcDataChannel::local_close` now treats only typed `ErrDataChannelClosed` as successful cleanup.
Other errors and the existing close timeout still trigger peer cleanup.
The change preserves Hub transport ownership and the pinned Core contract.
It changes no terminal protocol, dependency, or sibling repository.

The new test uses a real removed DataChannel handle and the production subscription cleanup helper.
The test requires the dependency's absent-channel error and then successful request/response traffic on the host sibling.
The test passed. The existing genuine-error and timeout tests also passed.
A negative control removed only the correction. The new test then failed at the peer-cleanup assertion.
The correction was restored exactly.
Evidence: `/tmp/hub-hard-close/idempotent-absent` and `/tmp/hub-hard-close/idempotent-ablation`.
Both supervisors recorded no owned survivors and no cleanup signals.

This helper test does not verify the whole remote-close lifecycle.
Earlier entity-shaped and terminal-shaped tests timed out while waiting for Hub target retirement.
Those failures remain preserved in `idempotent-focused` and `idempotent-terminal` under the same evidence root.
The coordinator requires one terminal-shaped retry after the separate dependency repair.
No matrix or gate advancement occurred.

The implementer, Botster implementer, and Hub playbooks constrained this correction.
The runtime teardown lenses and peer-cleanup notes require explicit sibling and failure-path evidence.
No convention was waived. No missing guidance was discovered for this correction.

### 2026-09-04 vendored rtc ordered-close repair: build, retry, and focused gates

Target: `trybotster/botster-hub`, `tgt_7e208a0c76a44980a83b63af976b1f22`.
Implementer: Fable session `sess-1788590641-004c-c690f3f33319fb80558a0ef4f7de8dc6`, after the Codex release in `artifact_1788590263_218370`.
Base: `1d489646a02ee3bca6e51e206b310eca70be68a8`. Core pin `93acae3` unchanged.

Dependency change: `vendor/rtc-0.21.0-beta.2` with `BOTSTER-PATCH.md`, a workspace `exclude` for that directory, and a root `[patch.crates-io]` entry.
`Cargo.lock` loses only the registry source and checksum of the `rtc` entry.
The four changed upstream files, provenance, licenses, and the real-peer positive and negative evidence are recorded in `artifact_1788590134_343889`.
This commit does not include the restored remote-close test.

Environment for every command: Rust 1.97.0, Zig 0.16.0, `CARGO_BUILD_JOBS=2`, `CARGO_TARGET_DIR` unset, worktree root, repository `test.sh` wrapper for tests, bounded supervisor with 300-second limits and process census.

Build proof, `/tmp/hub-hard-close/fable-build/`:

- `cargo metadata --locked --offline` selects `rtc 0.21.0-beta.2` with `source = None` from `vendor/rtc-0.21.0-beta.2`; `rtc` is not a workspace member.
- `cargo build --locked -p botster-core-daemon --bin botster-session-worker`: exit 0.
- `cargo build --locked --bin botster-hub`: exit 0. The log shows `Compiling rtc v0.21.0-beta.2 (.../vendor/rtc-0.21.0-beta.2)`.

Exact retry, `/tmp/hub-hard-close/fable-remote-close-retry/`, one execution:

- `./test.sh --locked --lib transport::webrtc::subscription_channel::tests::remote_closed_subscription_keeps_host_sibling_live -- --exact --nocapture`: exit 101 in 11.56 s.
- Panic: `timed out waiting for target retirement: receiving on an empty channel` at the `pump_test_control_until` deadline.
- No owned survivors before or after cleanup. Source diff unchanged.

Source finding for the retry (no fixture change, no further run):
The test waits for `pending_runtime.is_adapter_bound` to become false while pumping only `control_rx`.
The production remote-close path is driver exit, then `RetireReservedSubscription`, then `retire_reserved_subscription` in `src/daemon/control/connection.rs`.
Its `retire_route_owner` arm for `ChannelClass::Terminal` is empty. The function forgets the label and releases budget. It never calls `pending_runtime.close_adapter`.
`adapter_bound` is cleared only by Detach and ShutdownSession request handling, `fail_closed_pre_bind_attach`, grant or session adapter closes, and inventory reconcile.
None of those run from this fixture's control pump. The panic text is the post-deadline Empty branch and does not show whether `RetireReservedSubscription` arrived.
The fixture oracle is therefore not a control-plane-observable outcome for a Terminal reservation. This does not prove that the Hub observed the remote close.
rtc-sctp surfaces a peer stream reset as `AssociationLost { reason: Reset }`, which rtc maps to `SCTPStreamClosed` and then `OnClose`, and it retransmits reconfig requests.

Focused gates, `/tmp/hub-hard-close/fable-focused-gates/`, one execution of each arm:

| Arm | Command | Result |
| --- | --- | --- |
| webrtc_unit | `./test.sh --locked --lib transport::webrtc:: -- --nocapture --skip remote_closed_subscription_keeps_host_sibling_live` | 86 passed |
| unix_unit | `./test.sh --locked --lib transport::unix::` | 25 passed |
| budget_unit | `./test.sh --locked --lib admission::connection_budget::` | 7 passed |
| unix_exit | `unix_adapter_bound_printf_stream_attach_delivers_process_exit` | passed |
| webrtc_bytes | `webrtc_terminal_output_is_byte_exact` | passed |
| webrtc_exit | `webrtc_terminal_adapter_detach_peer_death_process_exit_and_shutdown_do_not_emit_close_event` | FAILED, exit 101 |
| ended | `webrtc_terminal_adapter_attach_after_authoritative_exit_rejects` | passed |
| host_close | `webrtc_terminal_adapter_host_close_emits_negotiated_terminal_subscription_closed` | passed |
| fmt | `cargo fmt --all -- --check` | exit 0 |
| clippy | `cargo clippy --workspace --all-targets --locked -- -D warnings` | exit 0 |

The `webrtc_exit` failure is identical to the `final-focused` failure before the dependency repair.
The retained frames end with `terminal_output` bytes `done`. No `ProcessExit` arrives before the receive timeout.
`subscription_receive_errors` records `cause=channel_closed message_id=pending next_chunk=0 expected_chunks=pending`.
The vendored queue-order repair does not clear this failure. The failure remains unattributed.
The skipped restored test is the one already executed in the exact retry. It was not run a second time.

Status: the vendor repair is a separate commit. The restored remote-close test stays uncommitted as preserved evidence.
No complete matrix, downstream rerun, merge, push, pin change, or gate advancement occurred. The implementer does not approve this candidate.

#### Correction: vendored lockfile and retirement assessment

Codex review of `fb58248` found that `vendor/rtc-0.21.0-beta.2/.gitignore` excluded the vendored `Cargo.lock` from that commit.
Commit `4c8fa76` tracks the file explicitly. No dependency version was regenerated.
SHA-256 of the committed file: `e6697428d8d79939f1e071e44eaece9b0f4272e5a7079374595524a1878a5b02`.
The same hash matches the lockfile in the standalone proof directory `/tmp/hub-hard-close/rtc-repair` and the registry copy of the published crate.
The earlier proof artifact did not record a lockfile hash; it recorded "exact published lock" and listed no lockfile change. The match above is the verification.
A clean-checkout repeat of the two standalone rtc tests is prepared at `/tmp/hub-hard-close/fable-clean-checkout/run.py` and has not run.

Retirement assessment including the production owner path:

- Hub driver observes `OnClose` on a bound terminal channel, calls `handle.close()`, and sends `RetireReservedSubscription`.
- Pinned Core `client_worker.rs` `pump_one` treats adapter pressure `Closed` as `retire_and_hard_stop`, so Core retires the route and drops it from `list_terminal_subscriptions`.
- `retire_reserved_subscription` forgets the label and releases budget; its Terminal arm does no registry cleanup by design.
- The Hub registry row is cleared by the owner loop `run_inventory_reconcile_phase` (`src/daemon/owner_loop.rs`), which calls `reconcile_inventory_slice`; a route absent from Core inventory is stale and receives `close_adapter` and `cancel_stream`.
- The unit fixture pumps only control messages and never runs the owner loop reconcile phase, so `is_adapter_bound` cannot clear there.

The control-only fixture timeout is therefore not proof of a leaked adapter. It is also not proof that the Hub observed the remote close.
An unapplied diagnostic patch at `/tmp/hub-hard-close/fable-instrumentation/` records received control messages, reservation state, the Core inventory row, and the Hub registry row during the wait. It keeps the acceptance condition unchanged and has not run.
A later fixture correction must keep the real sibling-traffic assertion and prove adapter cleanup through the production owner path, not through label removal alone.

### 2026-09-05 close-state coverage and production-owner retirement proof

Commits: `c0ccbba` (vendor tests only), `fbc3c5f` (Hub test and crate-visible reconcile phase), `5ac1b69` (error propagation, live-pressure assertion, bounded reconcile loop).
Group executed once on `5ac1b69` under `/tmp/hub-hard-close/fable-close-states-run.py`; evidence in `/tmp/hub-hard-close/fable-close-states/` and `/tmp/hub-hard-close/fable-clean-checkout/`.

| Arm | Result |
| --- | --- |
| clean-checkout vendor: `git archive` of HEAD, 346 files, lockfile hash matches the proof | exported |
| `accepted_final_payload_precedes_remote_close` (real peer) | 1 passed |
| `accepted_payload_under_pressure_precedes_remote_close` (real peer, Hub high-water threshold, live pressure asserted before the final send) | 1 passed |
| `enqueue_then_close_preserves_payload_order` (lib) | not executed: filter lacked the module path, 0 passed, 271 filtered out |
| `public_close_before_open_ignores_late_ack_and_orders_close_after_open` (lib) | not executed: same filter defect; compiled, never run |
| `remote_closed_subscription_keeps_host_sibling_live` (production owner path) | 1 passed in 1.00 s |
| `webrtc_terminal_adapter_detach_peer_death_process_exit_and_shutdown_do_not_emit_close_event` | FAILED, exit 101 |
| `cargo fmt --all -- --check` | exit 0 |
| `cargo clippy --workspace --all-targets --locked -- -D warnings` | exit 0 |

The two lib arms are not evidence. The command list passed the bare probe-module path instead of `peer_connection::handler::botster_enqueue_close_probe::...`.
A corrected script with the full module path is prepared at `/tmp/hub-hard-close/fable-clean-checkout-units-run.py` and has not run.

The remote-close test now proves cleanup through the production owner path: Core retired the route on adapter pressure `Closed`, the reservation reached `Unknown`, the owner-loop inventory reconcile cleared the registry route, the budget slot released, the peer was not cleaned up, and the host sibling carried a request and response.
The earlier control-only timeouts remain preserved under `idempotent-focused`, `idempotent-terminal`, and `fable-remote-close-retry`.

The combined lifecycle failure is now attributed to its channel.
The exit channel label was `r-9b4557bde1add9ba40c6caf155a6367b`, and that exact label recorded `cause=channel_closed message_id=pending next_chunk=0 expected_chunks=pending`.
The terminal channel for `wnx-exit/sub-exit` closed after its `done` output frame and before any `ProcessExit` event. The cause of that close is not yet established.

### 2026-09-05 corrected vendor unit arms and exit-channel owner inspection

Commit `b22d25d` corrects the close-before-open case to the exact DCEP sequence that `RTCDataChannelInternal::dial` queues: open, low threshold, high threshold, then one close. The old failed log stays in `/tmp/hub-hard-close/fable-clean-checkout-units/close_before_open.log`.
The corrected unit arms ran once on archived `5ac1b69` under `/tmp/hub-hard-close/fable-clean-checkout-units-run.py`:
`enqueue_then_close_preserves_payload_order` executed once and passed; `public_close_before_open_ignores_late_ack_and_orders_close_after_open` executed once and failed on the threshold markers described above. The corrected expectation in `b22d25d` has not run.

Exit-channel owner inspection (source only, pinned Core `93acae3`, Hub `b22d25d`). Observed: `terminal_output` `done` reached the client on the exit channel, then that exact channel closed with no `ProcessExit`.

Hub-owned closes of a bound terminal channel, all in `run_bound_terminal_channel` and `flush_subscription_adapter_frames` (`src/transport/webrtc/subscription_channel.rs`):

- `adapter_closed`: `snapshot_active` returns none and the handle is closed. The Hub follows a Core close. Any frame Core had not yet handed to `local_send_text` is abandoned by contract.
- `flush_permit`: `transfer_aggregate_permit` returns false when the stored permit is absent while an aggregate exists, or when `try_resize` to the wire length is refused. The Hub then closes the channel and abandons the active frame. This close is Hub-owned.
- `flush_send`, `flush_frame`: `local_send_text` or `framed_daemon_terminal_frame` fails.
- ingress decode, ingress push, threshold, and usage failures; and the dependency reporting `OnClose`, `OnError`, or end of events.

Core-owned adapter closes that drop a queued or in-flight `ProcessExit` before delivery (`crates/botster-core/src/engine`):

- `client_worker.rs` `pump_one`: `WRITE_ATTEMPT_BUDGET` (512) unsuccessful writes while the adapter reports `Full` or `WouldBlock`.
- `managed_session_runtime.rs:388`: `ControlAdmission::Sealed` while a woken route still holds terminal input calls `hard_stop_owner`. `probe_ordinary` returns `Sealed` for a sealed or missing session, which is the state after process exit. `hard_stop` clears the owner queue and closes the adapter.
- `client_worker.rs` `hard_stop_key` on queue capacity, encode failure, or a `ProcessExit` for an unbound route.
- The normal path, `process_exit_delivered` after adapter `Ready`, retires the route only after the Hub accepted the frame into the dependency queue, and the vendored dependency now orders the close after that payload.

The normal path cannot produce the observed result with the vendored dependency. The `flush_permit` and `Sealed` paths can. The existing logs cannot distinguish them because the Hub records no reason when it closes a terminal channel.

Proposed next step, not implemented: push one `RuntimeObservation` host event, `terminal_channel_closed:{session}:{subscription}:{reason}`, from the terminal driver at every Hub-initiated channel close, using the same pattern as the entity overflow observation. The combined lifecycle test already retains host events, so its failure text can then name the closer. No sleep, replay, or timeout change.

### 2026-09-05 close-reason observation and attributed run

Commit `39443fc` adds `TerminalDriverExit` and one `RuntimeObservation` per terminal driver termination, `terminal_channel_closed:{subscription}:{generation}:{reason}`, with no change to close actions, ordering, adapter state, budgets, or protocol shape. The exit fixture waiter prints those observations in its failure text.

Reason meanings, as bounded by the Codex review:

- `adapter_closed` and `adapter_closed_in_flight` mean only that the driver observed a closed adapter, before or after starting a send. `adapter_closed_in_flight` can occur before the send future's first poll, so it does not prove that bytes were accepted or sent.
- `permit_refused` means the transfer or resize was refused while the handle read as open at the recheck. A close that races with that recheck is not distinguished atomically.
- `remote_close` means the driver observed `OnClose`; it is not proof of remote initiation.
- Publication follows the bounded close and possible peer cleanup, so a missing observation does not prove that the driver did not terminate.

Group executed once on `39443fc` under `/tmp/hub-hard-close/fable-attributed-exit-run.py`; evidence in `/tmp/hub-hard-close/fable-attributed-exit/` and `/tmp/hub-hard-close/fable-clean-checkout-cbo/`.

| Arm | Result |
| --- | --- |
| `public_close_before_open_ignores_late_ack_and_orders_close_after_open` (archived `39443fc`, full module path) | 1 executed, 1 passed |
| `webrtc_terminal_adapter_detach_peer_death_process_exit_and_shutdown_do_not_emit_close_event` | 1 executed, 1 passed in 6.50 s |
| `cargo fmt --all -- --check` | exit 0 |
| `cargo clippy --workspace --all-targets --locked -- -D warnings` | exit 0 |

The combined lifecycle test passed on this execution, so no close observation was captured. Across the recorded single executions it has now passed twice and failed three times. One execution does not decide the natural-exit condition; the close reason can be read only from a failing execution.

### 2026-09-05 bounded tally: first execution attributed

Plan: at most five single executions of the combined lifecycle test on frozen `c65f2f0`, stopping at the first failure. Supervisor `/tmp/hub-hard-close/fable-exit-tally-run.py`; evidence `/tmp/hub-hard-close/fable-exit-tally/`.
Execution 1 failed (exit 101, 0 passed, 1 failed, 11.4 s). The tally stopped there. Source and binary hashes are in the receipt.

Failure text: `exit_label=r-84e1a0d22c74f83d6db991d8b36df9f7`, `exit_channel_errors=[cause=channel_closed message_id=pending next_chunk=0 expected_chunks=pending]`, `terminal_channel_closed=["terminal_channel_closed:sub-exit:1:adapter_closed"]`.

Reading within the documented limits: the exit channel's driver terminated because it observed a closed adapter while no frame was active. The driver did not abandon an in-flight frame, did not refuse a permit, did not fail a send, and did not react to a remote close. The channel close was the driver's ordered follow-up to an adapter close. The observation does not say who closed the adapter.

Remaining scenarios:

- N: Core retired the route after the Hub completed a `ProcessExit` send. The frame was accepted by the dependency before the ordered close and was lost between acceptance and the client.
- P: Core closed the adapter before writing `ProcessExit` into the slot, through a hard-stop path.

The driver treats egress frames as opaque and records no count, so this evidence cannot separate N from P. A further narrow observation (frames completed, and whether the last completed frame was a `process_exit`) was proposed to the coordinator and is not implemented.

### 2026-09-05 isolated Core hard-stop probe: ten passing executions, no failure captured

Provenance under `/tmp/hub-hard-close/fable-core-probe/` (`PROVENANCE.md`, `hashes.txt`, `core-hard-stop-probe.diff`, `fixture-hashes.txt`, `build-rs-applied.sha256`).
Core export at exact `93acae3` with the Ghostty gitlink `eb72ec6` filled from the verified cached submodule; only `client_worker.rs` differs from the pinned checkout.
Hub probe worktree detached at `c112fb5` with an untracked Cargo config patching the six pinned Core crates to the export; the vendored rtc repair unchanged; one coordinator-applied disposable edit to `crates/botster-hub-test-support/build.rs` (hash `0ef19b1d…`) that accepts the protocol crate only at the exact exported manifest path.
Resolution proof recorded each run: six crates to the export, rtc to vendor. Lock delta, source hashes, and binary hashes recorded; binaries unchanged across executions.

| Run | Probe | Executions | Result |
| --- | --- | --- | --- |
| run | stderr probe | build only | stopped: build script rejected path-sourced protocol crate before the override |
| run2 | stderr probe | 1 | zero selection, same build-script rejection, preserved |
| run3 | stderr probe (`db9c83b0…`) | 5 | 5 passed, one selected test each |
| run4 | stderr plus bounded file sink (`cc262630…`) | 5 | 5 passed, one selected test each, sink present each time |

Every `sub-exit` teardown in run4 recorded the same state at the patched-file site `client_worker.rs:1500`, which is the `process_exit_delivered` retirement in `pump_one`: `process_exit_enqueued=true process_exit_delivered=true in_flight=false unsuccessful_writes=0 queue_len=0 queue_process_exit=0`. The only other site was `client_worker.rs:1357` (`detach_live`) for the explicit Detach routes.

Limits: the failure did not reproduce under the probe in ten executions, while the unprobed build failed four of six single executions. The probe's synchronous write sits on Core's pumping thread immediately before the adapter close, so a passing probe run is inconclusive about the race. The run4 records establish the normal-completion signature only. No Core owner state was captured for a failing execution.

#### Control tally on the probe build with the probe gate unset

`run5-control`: the exact run4 binaries (`botster-hub` `669d20f8…`, `botster-session-worker` `b5cb95ee…`, unchanged after the group), `BOTSTER_CORE_PROBE_HARD_STOP` and its sink path unset, no sink produced.
Executions: 1 to 3 passed with one selected test each; execution 4 failed (exit 101) with the delivery signature: missing `ProcessExit` on `wnx-exit/sub-exit`, `cause=channel_closed`, `terminal_channel_closed:sub-exit:1:adapter_closed`. The group stopped there.
Reading: the probe build reproduces the delivery failure when the recorder is inactive (1 of 4) and did not fail in ten executions with the recorder active. The synchronous record on the pumping thread suppresses the race. No Core owner state exists for a failing execution.

#### Probe v3 (post-close recorder): failing owner state captured

`run6` on probe v3 (`client_worker.rs` `ef7e6ffe…`, resolution proven, binaries `botster-hub` `71183765…`, `botster-session-worker` `37e13a89…`, unchanged after the group): execution 1 failed with the delivery signature (`terminal_channel_closed:sub-exit:1:adapter_closed`) and the sink recorded the exit owner at hard stop: site `client_worker.rs:1500` (the `process_exit_delivered` retirement in `pump_one`), `process_exit_enqueued=true process_exit_delivered=true in_flight=false unsuccessful_writes=0 queue_len=0 queue_process_exit=0 held_len=0 input_queue_len=0 adapter_bound=true`.
Reading: Core completed its ProcessExit bookkeeping normally. The Hub adapter reports Ready only after `local_send_text` returned Ok for every frame, so the dependency wrapper accepted the ProcessExit frame before the ordered close. The premature hard-stop scenario is excluded for this execution.

Source attribution after acceptance:

- Sender SCTP ordering is sound: `rtc-sctp` `send_reset_request` queues an empty EOS DATA chunk behind pending payload, and the receiver defers a reset on `sender_last_tsn` and unread data.
- The receiving `webrtc` wrapper driver runs `poll_writes`, `poll_events`, `poll_reads` in that order every iteration after a batch datagram drain. `poll_events` delivers `OnClose`; `poll_reads` delivers `OnMessage`. When the final DATA and the stream reset are processed in one iteration, the channel receives `OnClose` before the message. The driver's own comment requires the opposite order but enforces it only inside the per-channel retain queue.
- `rtc`'s `DataChannelHandler` keeps `read_outs` and `event_outs` as separate queues; `SCTPStreamClosed` pushes `OnClose` into `event_outs` while the last payload still sits in `read_outs`.
- Consumers stop at `OnClose` (the Hub terminal driver and the test fixture), so the queued message is never read.

Proposed minimal production fix, in the already-vendored `rtc` crate: hold `OnClose` for a stream while `read_outs` still holds a message for that stream, emit it from `poll_event` once those messages are drained, and report `poll_timeout = now` while holding so the driver runs the next iteration immediately. Not implemented.

#### Receive-side close barrier: source-only plan (vendored rtc, not implemented)

Layer: `RTCPeerConnection` in `vendor/rtc-0.21.0-beta.2/src/peer_connection/handler/mod.rs`. The handler-level guard is wrong because `handle_read` (about lines 255-300) drains every handler's `poll_read` at once into `pipeline_context.data_read_outs`; by the time a driver polls events, the datachannel handler's own queue is already empty while application data still waits in the public queue.

State added to `PipelineContext`: `held_data_channel_closes: VecDeque<(RTCDataChannelId, RTCPeerConnectionEvent)>` and `held_close_ready: bool`.

Barrier in the public `poll_event` (about line 384):

1. Before popping `event_outs`, release any held close whose channel has no message left in `data_read_outs`; release preserves the held order and returns the first released event.
2. When the next `event_outs` entry is `OnDataChannel(OnClose(id))` or `OnClosing(id)` and `data_read_outs` still contains a `DataChannelMessage(id, _)`, move it to the held queue and continue with the following event. Events for other channels and all non-channel events pass unchanged, so one channel's backlog never blocks another channel's close.

Eligibility and wake: in `poll_data_read` and in the data branch of the public `poll_read`, after popping a message for channel `id`, if a close for `id` is held and no message for `id` remains, set `held_close_ready = true`. The public `poll_timeout` returns `Some(now)` only while `held_close_ready` is true; `poll_event` clears it when it releases. While a consumer intentionally leaves data undrained (back-pressure), no timer is due and no spin occurs; the close becomes due exactly when the last relevant read drains. Handshake-timeout closes from `handle_timeout` never coexist with delivered data and are unaffected. `close()` of the whole connection flushes held closes unchanged.

Tests, all through the public `RTCPeerConnection` API with events polled before reads, as the wrapper does:

- single channel: DATA then reset in one intake; `poll_event` yields no close; `poll_data_read` yields the message; `poll_timeout` is due; `poll_event` then yields `OnClose`.
- two channels: A has pending data and a reset, B has a reset only; `poll_event` yields B's close immediately and holds A's; A's close follows A's read.
- back-pressured consumer: with A's data undrained, `poll_timeout` reports no due timer across repeated polls; after the read it is due once.
- public `poll_read` variant of the single-channel case.
- real-peer `data_channel_backpressure_rtc2rtc.rs` case that drains events before reads.
- acceptance: the exact combined Hub lifecycle test under the bounded tally on the delivery build.

Provenance requirement: four vendor files change (`handler/mod.rs`, the two test files, and `BOTSTER-PATCH.md`); the send-side ordered-close repair is unchanged.

#### Barrier plan amendments (coordinator review)

- Pending counts, not queue scans: `PipelineContext` keeps `pending_data_by_channel: HashMap<RTCDataChannelId, usize>`. The single enqueue site in `handle_read` (the `DataChannelMessage` push into `data_read_outs`, about line 294) increments; both dequeue paths, `poll_data_read` (about line 180) and the data branch of the public `poll_read` (about line 304), decrement and remove the entry at zero. A held close for channel `id` becomes eligible when its count is absent. No work is proportional to sibling backlog: an unrelated channel's read touches only its own entry.
- Held-close state bounded by channel lifecycle: at most one held close per channel, keyed by `id` in insertion order (`VecDeque<(id, event)>` plus the count map). A second close event for a channel that already holds one is dropped once, since the datachannel handler already emits `OnClose` at most once per stream (`close_emitted`), and this is the single duplicate-handling point. Entries are removed on release and on connection `close()`.
- `OnClosing` is not held: the vendored `rtc` source never emits `RTCDataChannelEvent::OnClosing` on any path (defined in `event/data_channel_event.rs`, no producer), so holding it would be untested behavior. Only `OnClose` ordering is the demonstrated defect; local close semantics are unchanged.
- Logical time preserved: the held event is the original event; `poll_timeout` returns `Some(ctx.now)` from the datachannel handler context's last observed instant only while at least one held close is eligible and not yet released (`held_close_ready`), and nothing otherwise. No timer is due while a channel's data stays undrained under back-pressure.
- Tests, public `RTCPeerConnection` API polled events-first: event order for one channel; two channels with one back-pressured; both read APIs (`poll_data_read` and public `poll_read`); no timer spin under back-pressure across repeated `poll_timeout` calls; duplicate close notification handled once; connection teardown with a held close (no leak, no late event after `close()`); the real-peer rtc2rtc events-first case; acceptance by the exact combined Hub lifecycle test.

Handoff: the ticket write claim is released for a fresh worktree-scoped implementation session. No source was edited for this plan.

### 2026-09-05 receive-side close barrier: implemented in the vendored rtc public queue boundary

Source revision: HEAD `eff54e3` plus four uncommitted files under `vendor/rtc-0.21.0-beta.2`: `src/peer_connection/handler/mod.rs`, `src/peer_connection/internal.rs`, `tests/data_channel_backpressure_rtc2rtc.rs`, `BOTSTER-PATCH.md`. The send-side ordered-close repair files are unchanged. Diff `target/.botster-foundation-evidence/receive-barrier.diff` (sha256 `9f78630d…5a61`, 934 insertions, 73 deletions).

Mechanism, as approved by the coordinator: `route_read_out` is the single counted enqueue into `data_read_outs`; `pop_data_read_out` serves `poll_data_read` and both data branches of the public `poll_read`; the public `poll_event` holds `OnDataChannel(OnClose(id))` only while channel `id` has a pending count, releases eligible held closes first in held order, and recomputes the readiness flag from the entries that remain; `poll_timeout` keeps every handler deadline and folds in the datachannel context's last observed instant only while a held close is eligible; `close()` moves held closes into `event_outs` ahead of the `Closed` state event and clears counts and flag; after teardown a close is never held again and no wake is scheduled. Duplicate `OnClose` for an already-held channel is dropped; the datachannel handler already emits at most one per stream. `OnClosing` is not held.

Complexity note for review: when a channel's count reaches zero, `pop_data_read_out` scans `held_data_channel_closes` for that channel. That scan is bounded by the number of channels with a held close, not O(1). Its comment overstates the guarantee; it is not rewritten during this frozen validation group.

The vendored crate cannot be tested in place: it is excluded from the Hub workspace and also patched by path, so `cargo test -p rtc` and `--manifest-path` both fail. All rtc checks ran in a disposable copy at `target/.botster-foundation-evidence/rtc-unit/` (rsync of the vendor directory, `target/` excluded, one appended empty `[workspace]` table; `diff -rq` against the vendor directory shows only that `Cargo.toml` line). Rust 1.97.0, `-j 2`, one check per command.

| Check | Command (copy unless noted) | Result | Log |
| --- | --- | --- | --- |
| barrier units | `cargo test --lib -j 2 botster_receive_close_barrier` | 8 passed | `01-unit-barrier.log` |
| handler module | `cargo test --lib -j 2 peer_connection::handler::` | 45 passed, includes both `botster_enqueue_close_probe` sender-repair tests and the rerouted `handler_test` fixtures | `02-unit-handler-module.log` |
| real peer, events first | `cargo test -j 2 --test data_channel_backpressure_rtc2rtc accepted_final_payload_precedes_remote_close_when_events_are_polled_first` | 1 passed | `03-rtc2rtc-events-first.log` |
| fmt | `cargo fmt --check` after `rustfmt --edition 2024` on the three changed files | clean | `04-fmt-check.log` |
| strict clippy on rtc | `cargo clippy -j 2 --lib --tests -- -D warnings` | exit 101, 11 diagnostics, all in upstream files outside this repair (`sdp_semantics.rs`, `rtp_receiver/internal.rs`, `statistics/accumulator/*`, examples, `tests/save_to_disk_vpx_interop`) | `05-clippy.log` |
| Hub check (worktree root) | `cargo check -j 2 --workspace --tests` | exit 0 | `06-hub-cargo-check.log` |

Negative controls, all in the copy, hashes in `A0`, `A1`, `A3`, `B1`, `B4`:

- Arm A, red on revert isolating the receive repair: the copy's `handler/mod.rs` and `internal.rs` replaced by their HEAD `eff54e3` blobs (hashes `955d6fb9…`, `51a1ef92…` match `git show HEAD:`), sender repair files untouched, new test file kept. Attempt 1 of 5 rebuilt (`Compiling rtc` present) and failed at `tests/data_channel_backpressure_rtc2rtc.rs:515` with `close surfaced before the accepted payload was readable`, left `[]`, right the payload. Stopped at the first expected failure. `A2-attempt1.log`.
- Invalid restoration check, preserved: after `rsync -a` restore the first green rerun failed with no `Compiling rtc` line, so it reran the arm A binary. This is not a source regression. `A4-green.log`. The restored files were touched; the rebuilt rerun passed, `Compiling rtc` present. `A5-green-rebuilt.log`.
- Arm B, ablation (not a revert): `hold_close_behind_pending_data` returns the event unconditionally in the copy. Rebuilt; 8 of 8 barrier units failed with real assertions: single channel `close must not surface while the payload is unread: [OnClose(1)]`; sibling `only the drained channel closes now` 2 versus 1; multiple eligible 3 closes versus 1; the remaining five at the no-close-before-read assertion. `B2-ablation.log`, `B3-assertions.txt`. Restored, touched, hashes match delivery, 8 passed with `Compiling rtc` present. `B5-green.log`.

Acceptance, Hub worktree root, source hashes unchanged from the pre-A snapshot, `cargo tree --locked -p botster-hub -i rtc` resolving to `vendor/rtc-0.21.0-beta.2`:
`RUSTUP_TOOLCHAIN=1.97.0 ./test.sh --locked -j 2 --test hub_daemon_lifecycle_test webrtc_terminal_adapter_detach_peer_death_process_exit_and_shutdown_do_not_emit_close_event -- --exact --nocapture`, six separate executions, stop at first failure. Execution 1 rebuilt `rtc`, `webrtc`, and `botster-hub`. Every execution: `running 1 test`, `1 passed`, exit 0. Binaries `target/debug/botster-hub` `200fba23…7b21` and `target/debug/botster-session-worker` `7358688b…631b`, unchanged across executions. Logs `C0-pre.txt` to `C8-cleanup-and-hashes.txt`. Before this repair the unprobed build failed 4 of 6 single executions. No process from this worktree remained after execution 6.

Not run: optional real-peer cases D, the Hub workspace lint gate, the locked suites, and the matrix. The strict clippy failure evidence for the vendored crate is preserved and the workspace lint gate stays pending.

### 2026-09-05 rtc 0.21.0-rc.1 vendor roll, sender repair reapplied, SCTP drain repair

Scope, as assigned by the coordinator: replace the `rtc 0.21.0-beta.2` vendor with the published `0.21.0-rc.1` family, keep the send-side ordered-close repair, and correct `SctpHandler::drain_stream` for rc.1's deferred stream reset. The beta receive-side close barrier from `f78457a` is not carried: rc.1 separates the public channel handle (`RTCDataChannelId = usize`, monotonic, never reused) from the SCTP stream id, so its stream-id lifecycle assumptions no longer apply, and a per-handle barrier is a separate phase. Codex confirmed by read-only trace that the `webrtc` wrapper and Hub keep the handle end to end and never substitute the wire id.

Commits on `project-pipelines/ticket_1787600679_990088-rc1`, each with a clean status:

| Commit | Content |
| --- | --- |
| `412f456` | vendor swap to rc.1 (upstream `51558ffb`, crate sha256 `f1c97fa1…`, 365 files), manifest exclude/webrtc/patch moved, sender repair reapplied, first drain correction and four tests, `BOTSTER-PATCH.md` with all 16 family checksums |
| `31dfa9b` | `Cargo.lock` correction: `cargo update -p webrtc --precise 0.21.0-rc.1` had also re-selected eight unrelated dependency edges (windows-sys and getrandom choices for errno, nu-ansi-term, quinn-udp, rustix, socket2, tempfile, uds_windows, winapi-util). The lock was rebuilt from the pre-update copy with only the seventeen rtc-family blocks replaced; every other block is byte-identical, and `cargo metadata --locked --offline` accepts it. The `412f456` message's claim that all non-rtc entries were unchanged was wrong and is superseded here. |
| `db2cd91` | drain follow-up: `forward_association_event` shared by `handle_read` and `resume_pending_reads`; a resumed drain forwards the `AssociationLost` its reset produced; `poll_timeout` reports the retained logical instant while `event_outs` is non-empty, cleared by the next `poll_event`, never set by parked data |
| `7099998`, `aa089d2` | rustfmt of new test code only |
| `fb5d1b1`, `d275058` | the reapplied sender tests compiled against beta only: rc.1 takes the handle from `DataChannelRegistry::insert` and `create_data_channel(...).id()`, and an in-band channel cannot dial before its stream id is bound. Fixtures adapted; assertions unchanged. The sender diffs applied textually but did not compile until this. |

Codex source review: clearance through `db2cd91` for provenance, lock, sender, and drain, and for the fixture deltas `7099998` and `fb5d1b1`; `d275058` was under review at this writing. No receive barrier is approved by that clearance.

Defect corrected. `rtc-sctp` rc.1 defers a peer's outgoing stream reset while the stream holds a complete unread message and performs it inside the `read_sctp()` that drains the last one; the next read in the same loop reports `ErrStreamClosed`. The published `?` in `drain_stream` aborted `handle_read` or `resume_pending_reads` before the collected batch reached `read_outs`, so the accepted payload was lost and a parked entry stayed parked. `ErrStreamNotExisted` on the initial lookup and `ErrStreamClosed` after a successful read are now terminal states of that stream; every other error propagates. The reset's `AssociationLost` is forwarded from the resume path too, and the local wake in `poll_timeout` guarantees the next driver pass without relying on the peer, whose reconfig timer stops on the in-progress reply.

The vendored crate cannot be tested in place (`current package believes it's in a workspace when it's not`). Checks ran in `target/.botster-foundation-evidence/rc1-prep/rtc-copy`, a byte copy of the vendor directory plus one appended empty `[workspace]` table (`rtc-copy-manifest.diff`); source hashes in `rtc-copy-source-hashes.txt` were re-recorded after every resync and equal the vendor tree. The copy's `Cargo.lock` stayed byte-identical to the crate-shipped lock (sha256 `2e27d88f…`); a first `--offline` run only exposed an uncached dev-dependency (`rtc-signal`) and was rerun with fetch allowed. The delivery `Cargo.lock` is untouched since `31dfa9b`. `-j 2`, `--locked`, one command at a time.

| Check | Command (copy manifest) | Result | Log |
| --- | --- | --- | --- |
| C1 sctp handler units, on `fb5d1b1` source | `cargo test -j 2 --locked --lib -- peer_connection::handler::sctp::tests` | `Compiling rtc` present; 13 selected (9 upstream, 4 new), 13 passed | `c1-sctp-tests.log` |
| C2 red arm: upstream `sctp.rs` + the 4 new tests only, zero removed lines, own target dir | same filter on `rtc-copy-red` | `Compiling rtc` present, own binary path; 10 passed, 3 failed on the predicted assertions: parked batch delivered 0 of 3, missing parked stream `ErrStreamNotExisted`, immediate drain `ErrStreamClosed`; the unrelated-error test passes on both arms | `c2-red-arm-sctp-tests.log`, `c2-red-arm-vs-upstream.diff` |
| C3 sender units, C1 binary unchanged | `--lib -- botster_enqueue_close_probe` | 2 passed | `c3-sender-unit-tests.log` |
| C4 real peer, on `aa089d2` source | `--test data_channel_backpressure_rtc2rtc` | `Compiling rtc` present; 3 passed (upstream slow-consumer case and both sender cases) | `c4-real-peer-tests.log` |

Invalid arm, preserved: C2 attempt 1 shared the green copy's target directory and reported 13 passed with no `Compiling` line, a 0.39 s finish, and C1's binary path. Cargo treated the two path copies as one fresh package. It counts for nothing; `c2-attempt1-INVALID-stale-binary.log`. Earlier failed attempts are kept as `c1-attempt1-offline-failed.log`, `c1-attempt2-sender-test-compile-failed.log`, `c4-attempt1-test-compile-failed.log`, and the passing pre-format run `c4-attempt2-pass-on-d275058.log`. Evidence hashes: `EVIDENCE-HASHES.txt`.

Limits of this proof. The immediate-drain unit test asserts a data count and a close count on two separate handler queues; it does not prove public data-before-close ordering, which belongs to the per-handle barrier phase and the real-peer tests. The Hub workspace build, the exact `hub_daemon_lifecycle_test` execution, the workspace fmt and clippy gates under Rust 1.97.0, the Core pin roll, and the consumer consolidation are not run in this phase and remain pending on the coordinator's schedule.

### 2026-09-05 receive-side close barrier on rc.1, keyed by public handle

Scope, as approved by the coordinator after Codex's design agreement: hold a channel's `OnClose` at the public queue boundary only while that channel has unread public data, keyed by rc.1's public handle. No stream-id parking, generation queues, allocator exclusion, or local-close change. This replaces the beta barrier from `f78457a`, which is not carried.

Commits, each with a clean status: `ab06deb` barrier and tests; `b5642d9` fixture intake order (test only); `3659abf` dequeue cost comment. Codex source-cleared the delta through `3659abf`. Diffs against the cached upstream crate: `barrier-08-handler-mod.diff`, `barrier-09-internal.diff`, `barrier-10-rtc2rtc.diff`.

Mechanism. `PipelineContext` keeps `pending_data_by_channel` (per-handle unread count, entry only while non-zero) and `held_data_channel_closes` (at most one held close per handle, only while that handle has a count, so held closes never exceed unread messages). `route_read_out` is the single enqueue site into `data_read_outs` and increments; `pop_data_read_out` serves `poll_data_read` and both `poll_read` data branches, decrements, and at zero moves that handle's held close into the ordinary `event_outs` queue, so released closes surface in read order. `poll_event` holds only `OnDataChannel(OnClose(handle))` with a live count; a duplicate for a held handle is dropped; after connection teardown nothing is held. `poll_timeout` keeps every handler deadline and reports the datachannel handler's last observed logical instant while `event_outs` is non-empty, which is the case only for a close a read released after the driver's event stage; the next `poll_event` clears it, and undrained data schedules nothing. `close()` flushes held closes ahead of the `Closed` state event and clears the counts. The zero-count scan and the duplicate check are linear in held closes.

Checks on the isolated copy (source hashes equal to the vendor tree at each run, `-j 2`, `--locked`, one command at a time, each with a fresh `Compiling rtc` line):

| Check | Command (copy manifest) | Result | Log |
| --- | --- | --- | --- |
| B1 barrier units, on `b5642d9` | `--lib -- botster_receive_close_barrier` | 9 selected, 9 passed | `b1-barrier-units.log` |
| B2 whole handler module, on `3659abf` | `--lib -- peer_connection::handler::` | 54 selected, 54 passed (upstream handler tests, drain tests, sender probes, barrier) | `b2-handler-module.log` |
| B3 real peer, events first, on `3659abf` | `--test data_channel_backpressure_rtc2rtc accepted_final_payload_precedes_remote_close_when_events_are_polled_first --exact` | 1 passed | `b3-events-first-real-peer.log` |
| B4 ablation arm: `hold_close_behind_pending_data` returns the event unconditionally in `rtc-copy-ablate` (own target dir; differs from the green copy in that one file, `b4-ablation-vs-vendor.diff`, 4 added lines) | `--lib -- botster_receive_close_barrier` | 9 failed, 0 passed, on the no-close-before-read assertions: single channel `[OnClose(0)]` surfaced unread; sibling 2 closes versus 1; read-order case 3 closes versus 1; the rest at `!any(is_any_close)` | `b4-ablation-barrier-units.log` |

Earlier attempts preserved: `b1-attempt1-import-compile-failed.log` (a bare `sctp` path resolved to the handler submodule; fixed by `::sctp` before the commit was announced) and `b1-attempt2-test-order-failed.log` (the fixture assumed a non-FIFO read; `b5642d9`).

Unit tests: one channel with the wake absent while data is unread and due after the read; a reused stream id whose successor gets a distinct handle and its own count; a back-pressured sibling; both `poll_read` data branches; no timer spin with a surviving DCEP handshake deadline; two released closes in read order with the wake due until both are consumed; duplicate close notifications; whole-connection teardown flushing ahead of `Closed` with retained data readable through both APIs and a post-teardown close never held.

Limits. The events-first real-peer case passes on the barrier build, but its batch intake does not force the failing schedule in every run; the ablation arm is the sensitivity evidence. Hub workspace build and lifecycle execution remain pending, as above.

### 2026-09-05 combined candidate: Core bf6e7d99 pin roll, consumer consolidation, build, and focused checks

Commits, each with a clean status: `d7ba579` rolls all eighteen literal Core pin sites from `93acae3f` to `bf6e7d996bca2786ad4142c870a13c57a490e241` and re-resolves only the six Core git packages in `Cargo.lock` (one added edge, `sha2 0.11.0` on `botster-core-daemon`, already locked; 148 other entries unchanged; `cargo-lock-core-roll.diff`); `c16d9db` ports the root consumer preparation (base `11facecf`, an ancestor; `git apply` clean, no manual edits): registry-identity worker lookup in `src/update.rs` and the `ExplicitResizeBusy` class in `src/runtime.rs`, with their tests; `a2704b3` consolidates the four absent root plans, the consumer report, the original patch under `docs/plans/pending`, and the worktree ownership note. Codex source-cleared `d7ba579` and `c16d9db` at `a2704b3`.

Builds on `a2704b3`, Rust 1.97.0, `--locked -j 2`:

| Step | Command | Result | Log |
| --- | --- | --- | --- |
| H1 root package | `cargo build --locked -j 2` | rtc rc.1 from `vendor/rtc-0.21.0-rc.1`, Core crates at `bf6e7d99`, Finished 1m40s | `h1-hub-build.log` |
| H2 workspace | `cargo build --workspace --locked -j 2` | installer and test-support built, Finished | `h2-hub-workspace-build.log` |
| H3 worker | `cargo build --locked -j 2 -p botster-core-daemon --bin botster-session-worker` | no prior binary; built from `bf6e7d99` | `h3-worker-build.log` |

Identity (`h4-binary-identity.txt`, `h5-final-binary-identity.txt`): `botster-hub` `0db7ea0e…db28d`, `botster-session-worker` `59760bd1…d221`; `cargo tree --locked -p botster-hub -i rtc` resolves the vendor path under `webrtc 0.21.0-rc.1`; `botster-core` and `botster-core-daemon` resolve to the git revision. The lifecycle test build recompiled `botster-hub` for the test profile (`d8b6277a…`, dev-dependency feature unification); the final plain workspace build uplifted the dev artifact back to `0db7ea0e…` without recompiling, and the worker hash never changed.

Focused checks, one at a time:

| Check | Command | Result | Log |
| --- | --- | --- | --- |
| F1a consumer identity | `./test.sh --locked -j 2 --bin botster-hub -- update::tests::durable_worker_identity` | 2 passed (a `--lib` attempt selected 0: `update` is a bin module; `f1a-attempt1-lib-target-zero-selected.log`) | `f1a-durable-identity.log` |
| F1b resize class | `./test.sh --locked -j 2 --lib -- runtime::tests::explicit_resize_busy_class_is_path_neutral_and_distinct_from_control_plane_failure --exact` | 1 passed | `f1b-resize-class.log` |
| F2 lifecycle, three separate executions | `./test.sh --locked -j 2 --test hub_daemon_lifecycle_test webrtc_terminal_adapter_detach_peer_death_process_exit_and_shutdown_do_not_emit_close_event -- --exact --nocapture` | 1 passed each, 6.93 s, 6.67 s, 6.69 s | `f2-1..3-lifecycle.log` |
| F3 fmt | `cargo fmt --all -- --check` | clean | `f3-fmt-check.log` |
| F3 clippy | `cargo clippy --workspace --all-targets --locked -j 2 -- -D warnings` | exit 0, no warnings | `f3-clippy.log` |

The old-Core collision negative control was not run: it would require re-pinning, and the coordinator ruled the published Core and consumer regression evidence sufficient. Not run: the full matrix, any push or merge, and the Web live run, which the coordinator schedules against the handoff hashes above.
