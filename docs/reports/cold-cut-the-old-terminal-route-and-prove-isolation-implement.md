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
| Implement commit | `8634018bc2fa1f4af2e079a0dde4963dd2b94e0d` |
| Branch | `project-pipelines/ticket_1787600679_990088` |
| Base | `ae6a0b1fe99d97215fa82d796da8f01a904171f0` |
| `hub_sha` | `8634018bc2fa1f4af2e079a0dde4963dd2b94e0d` |
| `locked_core_sha` | `72d1c7571bc229dbb2cbd67aa979b6504ac150a5` |
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

Unchanged production transport, data-plane, subscription, admission, and daemon modules. Inventory found no remaining production old-route symbol.

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

## Missing vault guidance discovered

Recorded in the inbox capture: final ownership statement, D.1 oracle names, 18-site pin count vs the "eleven" note title, runner registration check, and the full two-arm authentication prerequisite list.
