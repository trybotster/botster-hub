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
| Step | `botster_stack_implement` (`run_step_1787981023_284700`) |
| Approved plan | `docs/plans/hub-decomposition-2-extract-admission-and-subscription-ownership.md` |
| Merge policy | direct into `main`; do not create a PR |
| Base | `origin/main` `fd540b6b21bdfe23f9280e13f650dff573fc5ae9` (0 behind at Implement start) |
| Verified commit | `f7ebf6a74e709c3d8f10e603cd09c184746f4543` |
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
| `src/daemon_transport.rs` | loses admission, occupancy functions, close phase, budgets; keeps dispatch and owner loop |
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
```

## Ownership boundaries preserved

- Hub still owns Hello admission, grants, origin policy, budgets, route occupancy, and close bookkeeping.
- Core still owns terminal subscription identity, attach phases, and generations.
- `botster-hub-client` still owns DTO shapes, serde names, `PROTOCOL`, and generated TypeScript.
- Transport modules no longer store grant secrets or origin policy. `answer_offer` takes a derived `AesGcmKey`.
- `src/daemon_transport.rs` declares no `#[path]` submodule for attach routes or entity subscriptions.
- `src/lib.rs` declares `pub(crate) mod admission` and `pub(crate) mod subscription` and no longer declares `mod daemon_event_subscriptions`.

## Cross-repo routing

None. No new ticket dependency. Downstream Web/TUI cost is zero because no DTO field, serde name, or protocol version changed. `generated_typescript_protocol_matches_checked_artifact` passed and `crates/botster-hub-client/generated/daemon-protocol.ts` is byte-identical.

## Deviations from plan

1. `released_attach_generations` and `live_attach_routes` remain fields on `DaemonControlState`. Occupancy *functions* moved to `attach_routes.rs` and mutate those fields. The close-event *ledger* (pending events, suppression keys, slice classification) moved to `closed_events.rs`. Splitting the occupancy counters into a second state object would have been a larger control-state reshape than the occupancy extraction required.
2. Socket-driven route tests `client_eof_detaches_connection_subscriptions`, `attach_operator_error_does_not_detach_on_client_eof`, and `drain_does_not_inspect_legacy_attach_state_for_ownership` stay in `daemon_transport.rs` because they drive `handle_connection`. Occupancy unit tests stay in `daemon_transport` as callers of the moved functions. Test names are unchanged.
3. Unknown 7: `peer_generation.rs` owns only `grant_ids_match`. `AttachStreamOwner` and `owner_matches` stay in `attach_routes`.
4. Unknown 8: `origin_from_local_url` moved to `admission/grants.rs`. `issue_local_webrtc_bootstrap_response` dispatch stays in `daemon_transport`.
5. `secret_stream_key` is `pub(crate)` on grants so live WebRTC tests that act as the browser can still derive the session key from the issued secret. Production `answer_offer` does not take the secret.
6. The unified close-event queue helper carries `#[allow(clippy::too_many_arguments, clippy::explicit_counter_loop)]` so the extracted algorithm stays the original two-copy loop.
7. `pump_phases_do_not_list_subscriptions_or_sessions` now splits at `DaemonControlState` because `overlay_live_attach_occupancy` left `daemon_transport.rs`.

No wire, DTO, limit, or protocol change.

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
| `./test.sh --locked` second run | exit 0. Lib 498 passed. Lifecycle 319 passed, 2 ignored. No failures. |
| `git status --porcelain` after the passing suite | empty |

The first suite failure is attributed to parallel-load smoke attach, not to the extraction. Isolated green plus a second default-concurrency green suite is the recorded official result.

## Unverified behavior or residual risk

- `released_attach_generations` / `live_attach_routes` still sit on `DaemonControlState`. Later transport-split tickets should move those counters with the occupancy owner if they become a second writer.
- First-run smoke attach flake can still appear under a busy host. Isolated and second-suite evidence is recorded.
- Browser grant-secret derivation in production clients was not re-proven beyond Hub live WebRTC tests.

## Missing vault guidance discovered

Plan vault gaps 1–13 remain capture candidates. This Implement visit did not write those notes. The load-bearing new facts are: a `#[path]` submodule relocation reduces module parentage; close-event state had two near-identical owners; grant secrets must not enter WebRTC transport; peer identity is the grant id; matching lines vs repair rows; `hub_daemon_lifecycle_test` flattens submodule names.

No convention conflict with loaded playbooks.
