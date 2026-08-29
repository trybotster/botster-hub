# Implement report: Hub decomposition 2

## Target repository and target_id

| Field | Value |
| --- | --- |
| Target repository | `botster-hub` (`trybotster/botster-hub`) |
| `target_id` | `tgt_7e208a0c76a44980a83b63af976b1f22` |
| Authoritative spawn target | `list_spawn_targets` maps this id to `botster-hub` at the hub checkout |
| Pipeline worktree | this run worktree |
| Ticket | `ticket_1787894416_777916` |
| Run | `run_1787977061_443918` |
| Step | `botster_stack_implement` (`run_step_1787987449_135341`; prior visits `run_step_1787984753_947995`, `run_step_1787981023_284700`) |
| Approved plan | `docs/plans/hub-decomposition-2-extract-admission-and-subscription-ownership.md` |
| Merge policy | direct into `main`; do not create a PR |
| Base | `origin/main` `fd540b6b21bdfe23f9280e13f650dff573fc5ae9` (0 behind at Implement start) |
| Verified product commit | `706244571980eec29e3bc02d2c54e6f7431fd84f` |
| Prior occupancy repair | `f777cd542eec7392140fc30606fb3d4463463cd4` |
| Prior product commit | `f7ebf6a74e709c3d8f10e603cd09c184746f4543` |
| Runtime-teardown class | applies; every lens is implemented as a survive-the-move invariant |

Independent routing: `project_pipelines_current_context` ticket/run `target_id` and `list_spawn_targets` both map `tgt_7e208a0c76a44980a83b63af976b1f22` to `trybotster/botster-hub`. The approved plan used the same routing. Implementation stayed in this run worktree.

## Repository playbook and other playbooks/notes applied

### Playbooks

- [[implementer-playbook]]
- [[botster-implementer-playbook]]
- [[botster-hub-playbook]]

[[project-pipelines-playbook]] was not loaded. This ticket changes no Project Pipelines package, plugin, or workflow-policy path.

### Targeted atomic notes

- [[daemon transport extraction moves ownership before deleting the facade]]
- [[Hub extraction must reduce ownership rather than only split files]]
- [[hub moves must extend source scanning guard file lists]]
- [[fixed source guard lists need one ablation per added file]]
- [[botster hub gravity must be watched before it becomes the new monolith]]
- [[botster hub is a first party host profile over core]]
- [[botster Hub Rust stays a trusted host kernel]]
- [[Hub route registry names describe ownership not attach queues]]
- [[ShutdownSession suppresses exact route generations before Core teardown]]
- [[ShutdownSession suppression live tests are not a red oracle]]
- [[a public occupancy oracle must union Hub routes with Core inventory]]
- [[PeerClosed attach occupancy must use the live attach route set]]
- [[Unix EOF occupancy must share the live attach route set]]
- [[Unix Hello can reject terminal admission while host operations remain available]]
- [[WebRTC terminal admission requires an encrypted DataChannel Hello]]
- [[webrtc bootstrap origin must be requested after the package server binds]]
- [[Client event holders are connection-scoped]]
- [[exact owner plus name is the only package event subscription key]]
- [[terminal subscription lifecycle is Core owned while host session policy is Hub owned]]
- [[botster runtime teardown lenses]]
- [[Hub ultimate WebRTC close failure sacrifices every peer on the dedicated runtime]]
- [[botster Hub pipeline shells can override RUSTUP TOOLCHAIN below the CI pin]]
- [[Hub official gates must not set CARGO TARGET DIR]]
- [[Hub suite runs prebuild the session worker before the locked test wrapper]]
- [[strict clippy can hide later crate diagnostics behind the first compile failure]]
- [[a ui contract import line change costs one test line in each generic client]]
- [[a regression test must be shown to go red with the fix reverted]]
- [[implement gate must verify committed work and pr link before review]]
- [[implementation steps must persist report artifacts for review]]
- [[pipeline vault checklists must cite exact resolvable note titles]]
- [[test script required for rust tests not cargo test]]

### Constraints applied before edits

- Work only in the Hub run worktree for `tgt_7e208a0c76a44980a83b63af976b1f22`.
- Follow the approved plan. Keep Hub host-policy ownership.
- Every code commit is one of: relocation, import repair, extraction, guard restore.
- New modules are `pub(crate)`. Preserve existing public paths.
- Do not change the Core pin, DTO shapes, serde names, protocol version, or test names.
- Run every Rust gate with `RUSTUP_TOOLCHAIN=1.97.0` and `CARGO_TARGET_DIR` unset.

## Files changed

| Path | Why |
| --- | --- |
| `src/subscription.rs` | crate-private subscription tree |
| `src/subscription/attach_routes.rs` | relocated attach registry plus occupancy/route-ownership functions |
| `src/subscription/entity.rs` | relocated entity subscription family |
| `src/subscription/package_events.rs` | relocated package-event family |
| `src/subscription/closed_events.rs` | single close-event ledger, suppression, close phase |
| `src/admission.rs` | crate-private admission tree |
| `src/admission/unix_hello.rs` | Hello admission types, registries, decisions |
| `src/admission/budgets.rs` | Hub resource budget constants |
| `src/admission/grants.rs` | grant issue, validation, origin, session-key derivation |
| `src/admission/peer_generation.rs` | existing grant-id peer identity comparison |
| `src/lib.rs` | `pub(crate) mod admission` / `subscription`; source-scan list |
| `src/daemon_transport.rs` | loses admission, occupancy functions and fields, close phase, budgets; composes `pending_runtime` and `attach_close`; keeps dispatch and owner loop |
| `src/local_webrtc.rs` | loses grant registry and secret derivation; keeps handshake, framing, peers |
| `src/unix_terminal_adapter.rs` | mux delegates close bookkeeping |
| `src/webrtc_terminal_adapter.rs` | mux delegates close bookkeeping; keeps `close_events_admitted` and `drop_pending_events` |
| `src/package_event_router.rs` | import path to `subscription::package_events` |
| `tests/hub_daemon_lifecycle/event_plane_saturation.rs` | path-reference rows 10 and 11 |
| `tests/hub_daemon_lifecycle/subscription_ownership_baseline.rs` | attach path row 9; suppression unit-test location |
| `tests/session_projection_owner_loop.rs` | entity path row 8 |
| `docs/plans/hub-decomposition-2-extract-admission-and-subscription-ownership.md` | plan (Plan visits) |
| `docs/reports/hub-decomposition-2-extract-admission-and-subscription-ownership-implement.md` | this report |

Unchanged, as required: `src/main.rs`, `crates/botster-hub-client/`, `packages/hub-test-support/`, `Cargo.toml`, `Cargo.lock`.

Review relocations with:

```
git show --stat -M --summary 8e0dad8 e95babe e741b6d
git show --color-moved=dimmed-zebra ddb9982 a99d35c e73299e fae9047
git show --color-moved=dimmed-zebra f777cd5
```

## Ownership boundaries preserved

- Hub still owns Hello admission, grants, origin policy, budgets, route occupancy, and close bookkeeping.
- Core still owns terminal subscription identity, attach phases, and generations.
- `botster-hub-client` still owns DTO shapes, serde names, `PROTOCOL`, and generated TypeScript.
- Transport modules no longer store grant secrets or origin policy. `answer_offer` takes a derived `AesGcmKey`.
- `src/daemon_transport.rs` declares no `#[path]` submodule for attach routes or entity subscriptions.
- `src/lib.rs` declares `pub(crate) mod admission` and `pub(crate) mod subscription` and no longer declares `mod daemon_event_subscriptions`.
- `live_attach_routes` lives on `AttachStreamRegistry`. `released_attach_generations` lives on `closed_events::AttachCloseBookkeeping`. `DaemonControlState` composes `pending_runtime` (deref to the registry) and `attach_close`. Occupancy functions take those owners, not the full control state.

## Cross-repo routing

None. No new ticket dependency. Downstream Web/TUI cost is zero because no DTO field, serde name, or protocol version changed. `generated_typescript_protocol_matches_checked_artifact` passed and `crates/botster-hub-client/generated/daemon-protocol.ts` is byte-identical.

## Review findings addressed

Review `review_1787984719_909687` returned this ticket to Implement with two open findings. Commit `f777cd5` applies the suggested fixes. It does not waive the plan.

| Finding | Repair |
| --- | --- |
| `finding_1787984719_993379` (high): attach occupancy state still on `DaemonControlState` | `live_attach_routes` is a field of `AttachStreamRegistry`. `released_attach_generations` is a field of `AttachCloseBookkeeping` in `closed_events`. `DaemonControlState` holds `pending_runtime` and `attach_close`. `record_attached_subscription_change(registry, close, lifecycle, change, owner_grant_id)` and `overlay_live_attach_occupancy(status, daemon, hub_routes, pending)` take owner state. |
| `finding_1787984719_792783` (medium): state-machine tests stayed under old owners | Occupancy unit tests moved to `subscription::attach_routes`. `admission_cursor_uses_exclusive_range_not_a_prefix_scan` moved to `admission::unix_hello`. Shared close-ledger invariants moved to `subscription::closed_events`. Unix and WebRTC copies of those ledger tests were deleted. `daemon_transport` keeps `handle_connection` tests. Adapter modules keep mux-delegation slice tests (`close_event_slice_bounds_*`) and content-blind source scans. |

Review with:

```
git show --color-moved=dimmed-zebra f777cd5
git show 7062445
```

## Verify finding addressed

Verify `review_1787987402_654728` returned this ticket to Implement. The findings array could not attach through the MCP bridge, so the finding lives in the Verify report and the review summary. Plan acceptance check 19 was only partly satisfied.

Commit `7062445` adds the two missing absences to `close_events_phase_source_does_not_take_journal_wake` in `src/subscription/closed_events.rs`. The scanned region is still `fn run_close_events_phase` .. `#[cfg(test)]`. The test now also asserts the close-events phase does not contain `list_terminal_subscriptions` or `list_sessions`. Test name is unchanged.

Red and green arms, each with the production seed restored afterwards:

| Arm | Seed | Result |
| --- | --- | --- |
| A | `let _ = "list_sessions";` as the first line of `run_close_events_phase` | `close_events_phase_source_does_not_take_journal_wake` FAILED: `Pump close classification must not list sessions`. `pump_phases_do_not_list_subscriptions_or_sessions` still passed. |
| D | `let _ = "list_terminal_subscriptions";` as the first line of `run_close_events_phase` | `close_events_phase_source_does_not_take_journal_wake` FAILED: `Pump must use the exact membership query`. `pump_phases_do_not_list_subscriptions_or_sessions` still passed. |
| Restored | no seed | both guards passed (`2 passed; 0 failed`) |

Arm D is the independent later-assertion arm required by [[an ablation that reddens at the first assertion does not vouch for later ones]].

## Deviations from plan

The occupancy-counter deviation from the first Implement visit is withdrawn. The approved plan required those fields to move with their owners. `f777cd5` does that.

Remaining documented deviations:

1. Socket-driven route tests `client_eof_detaches_connection_subscriptions`, `attach_operator_error_does_not_detach_on_client_eof`, and `drain_does_not_inspect_legacy_attach_state_for_ownership` stay in `daemon_transport.rs` because they drive `handle_connection`. Adapter mux-delegation tests stay in the adapter modules. Test names are unchanged.
2. Unknown 7: `peer_generation.rs` owns only `grant_ids_match`. `AttachStreamOwner` and `owner_matches` stay in `attach_routes`.
3. Unknown 8: `origin_from_local_url` moved to `admission/grants.rs`. `issue_local_webrtc_bootstrap_response` dispatch stays in `daemon_transport`.
4. `secret_stream_key` is `pub(crate)` on grants so live WebRTC tests that act as the browser can still derive the session key from the issued secret. Production `answer_offer` does not take the secret.
5. The unified close-event queue helper carries `#[allow(clippy::too_many_arguments, clippy::explicit_counter_loop)]` so the extracted algorithm stays the original two-copy loop.
6. `pump_phases_do_not_list_subscriptions_or_sessions` still splits at `DaemonControlState` because `overlay_live_attach_occupancy` left `daemon_transport.rs`. The two list-API absences that used to live only in that region now also live on `close_events_phase_source_does_not_take_journal_wake`, so check 19 follows the protected text.

No wire, DTO, limit, or protocol change. The remaining deviations do not change occupancy ownership or the plan's test-home requirement, so the committed plan's acceptance checks still describe this branch.

## Runtime-teardown lenses

- Isolation: unchanged. One peer's ownership set stays grant-scoped. Sibling sacrifice path in `local_webrtc.rs` was not edited.
- Bounds: `LOCAL_WEBRTC_PEER_CLOSE_BOUND` and join deadline stay in `local_webrtc.rs` with the same values. Grant validation is synchronous.
- Late-message matrix: Attach/Detach/entity/events relocated with import repair. Hello and grant redemption moved with registries. ShutdownSession suppression moved to `closed_events.rs`; classification stays in `daemon/shutdown.rs`.
- Production-path proof: Hello still reaches `unix_hello_admission` through daemon dispatch. Grants redeem through `GrantRegistry::redeem` then `answer_offer` with a derived key. Suppression still installs before Core teardown (`shutdown_session_arm_installs_exact_suppression_before_core_request` now scans both `closed_events.rs` helpers and the `daemon_transport` arm). Occupancy still unions Hub routes with Core inventory.
- Ownership identity: peer owner id remains `grant_id`. `grant_ids_match` records that identity. No new counter.
- Sibling fail-closed policy: unchanged.

## Tests and downstream proof

All commands used `RUSTUP_TOOLCHAIN=1.97.0`. `rustc --version` was `rustc 1.97.0 (2d8144b78 2026-07-07)`. `CARGO_TARGET_DIR` was unset.

Inventory grep at base printed `12`. After the last import-repair pair it prints `0`.

| Check | Result |
| --- | --- |
| Temporary `tests/public_path_probe.rs` | green: existing `LocalWebrtcTransport` and `daemon_transport::PROTOCOL`. Red ablation `botster_hub::admission::...` failed `E0603` module `admission` is private. Probe deleted, never committed. |
| Named-file tests at pair boundaries | each reported `1 passed; 0 failed` with bare filters |
| Per-file GHOSTSNP ablation of every new `src/admission/` and `src/subscription/` file plus `src/local_webrtc.rs` liveness | each run failed and named that exact file |
| `cargo fmt --all -- --check` | exit 0 |
| `cargo clippy --workspace --all-targets --locked -- -D warnings` | exit 0 after full rerun |
| `cargo test -p botster-hub-client --locked` | 81 lib + 4 doctests passed, including `generated_typescript_protocol_matches_checked_artifact` |
| `git diff --exit-code -- crates/botster-hub-client/generated/daemon-protocol.ts` | no change |
| `node packages/hub-test-support/scripts/sync-assets.mjs --check` | pass |
| Prebuild worker + `botster-hub` | exit 0 |
| `./test.sh --locked` first run | `hub_daemon_lifecycle_test` 318 passed, 1 failed: `cli_daily_commands_share_canonical_default_data_directory` (`attach failed before adapter bind` during smoke) |
| Same test isolated | `1 passed; 0 failed` in 3.54s |
| `./test.sh --locked` second run (first Implement visit, product `f7ebf6a`) | exit 0. Lib 498 passed. Lifecycle 319 passed, 2 ignored. No failures. |
| Review-return `cargo clippy --workspace --all-targets --locked -- -D warnings` at `f777cd5` | exit 0 |
| Review-return lib unit tests at `f777cd5` | 495 passed (three duplicate adapter ledger tests removed) |
| Review-return `./test.sh --locked` at `f777cd5` | exit 0 (`DONE:0`). Lifecycle 319 passed, 2 ignored. hub-client 81 passed. ui-contract 90 passed. No failures. |
| Verify-return close-events self-scan red arms A and D | each FAILED the relocated guard and named the seeded string; pump-phase guard stayed green |
| Verify-return restored close-events self-scan | `2 passed; 0 failed` |
| Verify-return `cargo fmt --all -- --check` at `7062445` | exit 0 |
| Verify-return `cargo clippy --workspace --all-targets --locked -- -D warnings` at `7062445` | exit 0 |
| Verify-return official `./test.sh --locked` at `7062445` | exit 0 (`DONE:0`). Lib 495 passed. Lifecycle 319 passed, 2 ignored. hub-client 81 passed, including `generated_typescript_protocol_matches_checked_artifact`. ui-contract 90 passed. No failures. |
| `git status --porcelain` after the passing suite | only this uncommitted report; empty after the report commit |

The first suite failure is attributed to parallel-load smoke attach, not to the extraction. Isolated green plus a second default-concurrency green suite is the recorded official result for the first visit. The Review-return official result is one default-concurrency green `./test.sh --locked` on `f777cd5`. The Verify-return official result is one default-concurrency green `./test.sh --locked` on `7062445`.

## Unverified behavior or residual risk

- First-run smoke attach flake can still appear under a busy host. Isolated and second-suite evidence is recorded for the first visit. The Review-return suite did not reproduce it.
- Browser grant-secret derivation in production clients was not re-proven beyond Hub live WebRTC tests.
- Adapter mux-delegation tests still exercise `queue_closed_subscription_events_bounded` through Unix and WebRTC mux types. The shared ledger invariants live in `closed_events`. Review should confirm that remaining adapter tests are transport-delegation, not a second ledger owner.

## Missing vault guidance discovered

Plan vault gaps 1–13 remain capture candidates. Verify also named three capture candidates about region-bounded self-scans, per-assertion red arms, and `include_str!` versus path-scan clean-target rules. This Implement visit did not write those notes. The load-bearing new facts are: a `#[path]` submodule relocation reduces module parentage; close-event state had two near-identical owners; grant secrets must not enter WebRTC transport; peer identity is the grant id; matching lines vs repair rows; `hub_daemon_lifecycle_test` flattens submodule names; a self-scan whose region is bounded by two symbols loses coverage when a function between them moves.

No convention conflict with loaded playbooks.
