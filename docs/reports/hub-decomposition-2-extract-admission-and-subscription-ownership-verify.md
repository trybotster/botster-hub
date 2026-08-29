# Verify report: Hub decomposition 2 (admission and subscription ownership)

## Target repository and target_id

| Field | Value |
| --- | --- |
| Target repository | `botster-hub` (`trybotster/botster-hub`) |
| `target_id` | `tgt_7e208a0c76a44980a83b63af976b1f22` |
| Independent resolution | ticket and run `target_id` from `project_pipelines_current_context`; `git remote -v` in the run worktree prints `https://github.com/trybotster/botster-hub.git` |
| Ticket | `ticket_1787894416_777916` |
| Run | `run_1787977061_443918` |
| Step | `botster_stack_verify` (`run_step_1787986430_575145`) |
| Verified commit | `9b70f3c4055344684552f74ade342f5fd2326274` (Verify visit 2). Visit 1 verified `2a0d5e5eb3688c749dff07c40ff37fb2795de30d`. |
| Product commit under it | `706244571980eec29e3bc02d2c54e6f7431fd84f` (guard repair), over `f777cd542eec7392140fc30606fb3d4463463cd4` (occupancy repair) |
| Base | `fd540b6b21bdfe23f9280e13f650dff573fc5ae9` (`git merge-base HEAD origin/main`) |
| Worktree | clean before and after every probe (`git status --porcelain` empty) |
| Toolchain | `RUSTUP_TOOLCHAIN=1.97.0`, `rustc 1.97.0 (2d8144b78 2026-07-07)`, `cargo 1.97.0`; `CARGO_TARGET_DIR` unset |
| Verdict | approved (Verify visit 2). Visit 1 returned the run to Implement. |

## Playbooks and notes loaded

Role: [[verifier-playbook]], [[botster-verifier-playbook]].
Repository charter: [[botster-hub-playbook]].
Surface overlay: runtime/transport/lifecycle -> [[botster-runtime-verifier-playbook]] (routing) and [[botster runtime teardown lenses]] (teardown class).
No web, package, or Project Pipelines overlay applies. The diff changes no Ionic React path, no package manifest, and no pipeline plugin path.

Targeted notes:

- [[hub moves must extend source scanning guard file lists]]
- [[fixed source guard lists need one ablation per added file]]
- [[a source scan known positive control needs a clean Cargo target directory]]
- [[Hub extraction must reduce ownership rather than only split files]]
- [[daemon transport extraction moves ownership before deleting the facade]]
- [[ShutdownSession suppresses exact route generations before Core teardown]]
- [[Hub official gates must not set CARGO TARGET DIR]]
- [[botster Hub pipeline shells can override RUSTUP TOOLCHAIN below the CI pin]]
- [[a regression test must be shown to go red with the fix reverted]]
- [[verify must recheck resolved findings against the live worktree]]

## Commands and results

All commands ran in the run worktree at `2a0d5e5`, with `RUSTUP_TOOLCHAIN=1.97.0` and no `CARGO_TARGET_DIR`.

| Command | Result | What it proves |
| --- | --- | --- |
| `cargo fmt --all -- --check` | exit 0 | repository format gate |
| `cargo clippy --workspace --all-targets --locked -- -D warnings` | exit 0, zero diagnostics; rerun after `touch src/lib.rs` also exit 0 | strict lint gate over the moved code, not a cached no-op |
| `cargo build --locked -p botster-core-daemon --bin botster-session-worker` | exit 0 | worker prebuild required before the locked wrapper |
| `cargo build --locked --bin botster-hub` | exit 0 | Hub binary prebuild |
| `./test.sh --locked` | exit 0 on the first run; 32 `test result: ok` lines, zero `FAILED`. Lib 495 passed. `hub_daemon_lifecycle_test` 319 passed, 2 ignored. `botster-hub-client` 81 passed. ui-contract 34 passed. | official repository gate, including live daemon, WebRTC, and lifecycle paths |
| `git diff --exit-code fd540b6 HEAD -- crates/botster-hub-client/generated/daemon-protocol.ts` | exit 0 | generated DTO bytes unchanged |
| `node packages/hub-test-support/scripts/sync-assets.mjs --check` | `hub test-support package assets are current`, exit 0 | test-support asset synchronization |
| `git status --porcelain -- crates/ packages/` | empty | no generated drift after the suite |
| `git diff --check fd540b6...HEAD` | exit 0 | no whitespace defects |
| `git diff fd540b6...HEAD \| grep -cE '/Users/\|/home/\|<user handle>\|<user domain>'` | `0` | no absolute-path or PII leak in committed artifacts |
| `cargo test --locked --test session_projection_owner_loop -- --exact owner_loop_and_projection_sources_reject_unbounded_and_product_policy` | 1 passed | inventory row 8 guard executes by name |
| `BOTSTER_ENV=test cargo test --locked --test hub_daemon_lifecycle_test -- --exact event_plane_saturation_source_guards_hold` | 1 passed | inventory rows 10 and 11 |
| `BOTSTER_ENV=test cargo test --locked --test hub_daemon_lifecycle_test -- --exact attach_ready_precedes_history_finish` | 1 passed | inventory row 9 |
| `BOTSTER_ENV=test cargo test --locked --test hub_daemon_lifecycle_test -- --exact shutdown_suppresses_exact_route_generations_before_core_teardown` | 1 passed | suppression-before-Core-teardown guard follows the relocated helpers |

### Source-guard ablation matrix, one arm per added file

Method: insert `// GHOSTSNP` as the first line of the target file, run
`cargo test --locked --lib -- --exact tests::production_sources_reject_terminal_drain_and_snapshot_phase_decode`,
record the exact failure message, then restore the file. `include_str!` forces recompilation, so no stale scan is possible; `git status --porcelain` was empty after the loop.

| Ablated file | Exit | Failure message |
| --- | --- | --- |
| `src/subscription.rs` | 101 | `src/subscription.rs production source must not contain GHOSTSNP` |
| `src/subscription/entity.rs` | 101 | `src/subscription/entity.rs production source must not contain GHOSTSNP` |
| `src/subscription/attach_routes.rs` | 101 | `src/subscription/attach_routes.rs production source must not contain GHOSTSNP` |
| `src/subscription/package_events.rs` | 101 | `src/subscription/package_events.rs production source must not contain GHOSTSNP` |
| `src/subscription/closed_events.rs` | 101 | `src/subscription/closed_events.rs production source must not contain GHOSTSNP` |
| `src/admission.rs` | 101 | `src/admission.rs production source must not contain GHOSTSNP` |
| `src/admission/unix_hello.rs` | 101 | `src/admission/unix_hello.rs production source must not contain GHOSTSNP` |
| `src/admission/grants.rs` | 101 | `src/admission/grants.rs production source must not contain GHOSTSNP` |
| `src/admission/peer_generation.rs` | 101 | `src/admission/peer_generation.rs production source must not contain GHOSTSNP` |
| `src/admission/budgets.rs` | 101 | `src/admission/budgets.rs production source must not contain GHOSTSNP` |
| `src/daemon_transport.rs` (still-listed control) | 101 | `src/daemon_transport.rs production source must not contain GHOSTSNP` |

Every added list entry has its own red arm, and the scanner-liveness arm is red as well.

## Ownership acceptance, verified independently

| Plan check | Evidence |
| --- | --- |
| 1 | `src/daemon_transport.rs` defines none of `unix_hello_admission`, `daemon_hello_ack`, `terminal_compatibility_attach_error`, `UnixTerminalAdmission`, `WebrtcTerminalAdmission`, `HostCompatibilityRecord`, `next_admission_key`, `suppress_unix_session_close_events`, `suppress_webrtc_session_close_events`. Each definition grep returns 0. |
| 2 and 14 | `src/local_webrtc.rs` defines none of `LocalWebrtcGrant`, `GRANT_TTL_SECONDS`, `prune_expired_grants`, `random_secret_token`, `secret_stream_key`. `grant_secret` appears only twice, both inside `mod tests` in the browser-shaped `signal_peer` helper: the `LocalWebrtcSignalRequest` field and the `crate::admission::grants::secret_stream_key` call. Production `answer_offer` takes `stream_key: AesGcmKey`. |
| 4 | `src/daemon_transport.rs` keeps only `#[path = "daemon_package_control.rs"]`. `src/lib.rs:52` and `:84` declare `pub(crate) mod admission;` and `pub(crate) mod subscription;`. No `daemon_event_subscriptions` declaration remains. |
| 5 | Test-name multiset over `src/` and `tests/`: base 1004 names, head 1003. Only difference: the three duplicated adapter ledger tests drop from two copies to one (`close_event_slice_uses_keyed_suppression_without_cloning_the_prefix`, `empty_session_snapshot_installs_no_suppression_keys`, `exact_generation_suppression_silences_running_close_and_preserves_later_generation`), and two new behavior-neutrality tests appear (`grant_admission_error_display_matches_local_webrtc_error`, `grant_validation_runs_redeemed_expiry_secret_then_origin`). No existing test name was renamed or deleted. |
| 6, 7, 8a | `8e0dad8`, `e95babe`, and `e741b6d` each report `add=0 del=0` over one path pair, and each is immediately followed by its own import-repair commit (`9f223c4`, `9da5d96`, `f804431`). |
| 8b | `grep -rn "daemon_attach_stream\.rs\|daemon_entity_subscriptions\.rs\|daemon_event_subscriptions\.rs" src/ tests/ crates/ script/ \| wc -l` prints `0` (12 at base). |
| 9, 10, 11, 24 | See the command table. Downstream client cost is zero. |
| 13 | `src/admission/grants.rs` validates in the order redeemed (line 67), expired (70), secret (73), origin (76), with `MissingGrant` first at 134. All five `Display` strings are byte-identical to the base `src/local_webrtc.rs` strings. |
| 15, 16 | `src/lib.rs` adds no `pub mod` and no `pub use` for the new trees. The `pub` items in `src/subscription/package_events.rs` sit inside `pub(crate) mod`, so they have no public path. The `pub` item lists of `src/daemon_transport.rs` and `src/local_webrtc.rs`, the two public modules, are byte-identical between `fd540b6` and `2a0d5e5`. |
| 17, 18 | `production_sources_reject_terminal_drain_and_snapshot_phase_decode` names all ten new files. `no_lua_dispatch_in_terminal_input_or_output` already uses a recursive `src/` walk, so it needs no list update. `host_control_fair_write.rs` pins positive assertions, which cannot go silently blind. |
| 19 | **Partially satisfied. See the finding below.** |
| 20, 21, 22, 23 | Base is `fd540b6`, unchanged. Gates green. |

## Review findings rechecked against the live worktree

| Finding | Status | Live evidence |
| --- | --- | --- |
| `finding_1787984719_993379` (attach state still on `DaemonControlState`) | resolved | `DaemonControlState` no longer declares `live_attach_routes` or `released_attach_generations`. `live_attach_routes` is a field of `AttachStreamRegistry` (`src/subscription/attach_routes.rs:142`); `released_attach_generations` is a field of `AttachCloseBookkeeping` (`src/subscription/closed_events.rs:48`). |
| `finding_1787984719_792783` (tests under old owners) | resolved | Occupancy tests live in `subscription::attach_routes`; `admission_cursor_uses_exclusive_range_not_a_prefix_scan` lives in `admission::unix_hello`; shared ledger invariants live in `subscription::closed_events`, with the duplicate adapter copies deleted and no test name lost. |
| Six Plan-stage findings | resolved | The committed plan carries every repair, and its acceptance checks are the ones verified above. |

## Finding: the pump-phase guard lost its close-events coverage

Commit `f7ebf6a` retargeted `pump_phases_do_not_list_subscriptions_or_sessions` in `src/daemon_transport.rs`. The scanned region changed from
`fn run_one_pump_phase` .. `fn overlay_live_attach_occupancy` to
`fn run_one_pump_phase` .. `pub(crate) struct DaemonControlState`.

At `fd540b6` the region spanned 11,340 characters and contained the whole body of `run_close_events_phase`. At `2a0d5e5` the region spans 2,358 characters and contains only `run_one_pump_phase`, `run_inventory_reconcile_phase`, and `run_pump_observe_phase`. `run_close_events_phase` moved to `src/subscription/closed_events.rs:246`.

The relocated guard `close_events_phase_source_does_not_take_journal_wake` reproduces the `take_journal_advanced_wake`, `observe_session_lifecycle`, `observe_lifecycle_slice`, `prefer_close_events`, `queue_unix_subscription_closed_events`, `queue_webrtc_subscription_closed_events`, and `keys().find` assertions. It does **not** reproduce the two remaining assertions of the pump guard:

- `!pump.contains("list_terminal_subscriptions")`
- `!pump.contains("list_sessions")`

No other test asserts either absence over the close-events phase.

Red and green arms, each run with a restored worktree afterwards:

| Arm | Seed | Result |
| --- | --- | --- |
| A | `let _ = "list_sessions";` as the first line of `run_close_events_phase` in `src/subscription/closed_events.rs` | both guards pass: `test result: ok. 2 passed` — the guard is blind |
| A at base | the same seed simulated against `fd540b6:src/daemon_transport.rs` with the base split bounds | the base region contains `list_sessions`, so the base guard would have failed |
| B (control) | the same seed in `run_pump_observe_phase`, still inside the head region | `pump_phases_do_not_list_subscriptions_or_sessions ... FAILED` — the scanner and pattern stay live |
| C (control) | `let _ = "prefer_close_events";` in `run_close_events_phase` | `close_events_phase_source_does_not_take_journal_wake ... FAILED` with `close work must not rewrite the Pump phase pointer` — the relocated guard is live for the assertions it did copy |

This is the exact failure class of [[hub moves must extend source scanning guard file lists]] and plan risk 2, and plan acceptance check 19 requires each in-file self-scan to follow its protected text.

Suggested repair, which stays inside move scope: add the two absence assertions to `close_events_phase_source_does_not_take_journal_wake` in `src/subscription/closed_events.rs`, and record the red arm from A.

## Cross-repository consumer proof

- `botster-hub-client`: 81 lib tests plus 4 doctests pass inside `./test.sh --locked`, including `generated_typescript_protocol_matches_checked_artifact`.
- `crates/botster-hub-client/generated/daemon-protocol.ts` is byte-identical to `fd540b6`.
- `packages/hub-test-support`: asset synchronization check passes and the tree stays clean after the suite.
- `Cargo.toml`, `Cargo.lock`, `src/main.rs`, `crates/`, and `packages/` carry zero diff against `fd540b6`, so the Core pin is unchanged and `botster-web` and `botster-tui` need no edit.

## Runtime-teardown class

The ticket is move-only, so every lens is a survive-the-move invariant rather than a new failure path.

- Exact-generation suppression before Core teardown: `shutdown_suppresses_exact_route_generations_before_core_teardown` and `shutdown_session_arm_installs_exact_suppression_before_core_request` both pass. The second still reads the live `DaemonRequest::ShutdownSession` arm from `src/daemon_transport.rs` and requires `suppress_session_route_generations` in `closed_events.rs`, so the guard follows the split code.
- Late-message and close-ledger behavior: the full `hub_daemon_lifecycle_test` target passes with 319 tests, including the live Unix and WebRTC close lanes.
- Sibling policy and bounded close: `LOCAL_WEBRTC_PEER_CLOSE_BOUND` and the join deadline are untouched in `src/local_webrtc.rs`.
- Ownership identity: peer owner identity stays the grant id; `peer_generation.rs` records only `grant_ids_match`. No new counter or wire field.

## Unverified behavior

- Browser-side grant-secret derivation in a real `botster-web` client. Hub live WebRTC tests act as the browser through `secret_stream_key`; no packaged-browser smoke ran in this step.
- Repeated `./test.sh --locked` runs under host load. This step recorded one green first run at load average 4.7 on 12 cores; the first Implement visit recorded a smoke attach flake that did not reproduce here.
- Behavior equivalence of the unified close-event ledger beyond the existing per-transport tests. The two former copies were near-identical; unification is proved by the existing suite, not by a byte-level algorithm diff rerun in this step.

## Remaining risk

- The guard hole above is the only open item. It changes no runtime behavior today, but it removes enforcement that the ticket promised to preserve, and Hub has already shipped this failure once in commit `468bf7f`.
- `entity_subscriptions`, `event_plane`, `pending_runtime`, and `released_entity_generations` remain fields of `DaemonControlState`. The approved plan required only the two attach fields to move, so this is in scope, but `daemon_transport.rs` is not yet free of all subscription state.
- Three `handle_connection` tests and the adapter mux-delegation tests stay with their driving surfaces. That placement is recorded in the Implement report and matches the code they drive.

## Vault gaps worth capturing

1. A self-scan whose region is bounded by two symbols loses coverage silently when a function between them moves out. Retargeting only the end bound is not enough; the assertions must move with the protected text. This extends [[hub moves must extend source scanning guard file lists]] to region-bounded self-scans, not only fixed file lists.
2. A relocated guard can copy most of its assertions and still drop some. A per-assertion red arm is needed, not one red arm for the relocated test. This is the source-guard form of [[an ablation that reddens at the first assertion does not vouch for later ones]].
3. `include_str!` guards need no fresh `CARGO_TARGET_DIR`, because the macro makes the file a compile input. The clean-target rule from [[a source scan known positive control needs a clean Cargo target directory]] applies to `env!("CARGO_MANIFEST_DIR")` path scans instead. Recording that distinction saves a full scratch build per ablation.
4. Hub decomposition 2 confirmed that a byte-pure `git mv` plus a paired import-repair commit keeps `similarity index 100%` and `add=0 del=0`, which makes commit-kind verification a pure `git show --numstat -M` check.

---

# Verify visit 2: the guard finding is closed

Second Verify visit at `9b70f3c`, product commit `7062445`. Base is still `fd540b6`. The worktree was clean before and after every probe.

## What changed since Verify visit 1

`git diff --stat 2a0d5e5 HEAD` shows three files: this report, the Implement report, and eight added lines in `src/subscription/closed_events.rs`. The eight lines land at line 402, inside the `#[cfg(test)] mod tests` block that opens at line 334. They add the two missing absences to `close_events_phase_source_does_not_take_journal_wake`:

```rust
assert!(
    !close.contains("list_terminal_subscriptions"),
    "Pump must use the exact membership query"
);
assert!(
    !close.contains("list_sessions"),
    "Pump close classification must not list sessions"
);
```

No production line changed. This is the exact repair the finding suggested, and nothing else.

## Per-assertion red arms

Each new assertion needs its own arm, because an arm that reddens at the first assertion does not vouch for the second. Both seeds went into the first line of `run_close_events_phase`, and the file was restored after each arm.

| Arm | Seed | Result |
| --- | --- | --- |
| control | none | `close_events_phase_source_does_not_take_journal_wake` and `pump_phases_do_not_list_subscriptions_or_sessions`: `2 passed; 0 failed` |
| A | `let _ = "list_sessions";` | FAILED at `Pump close classification must not list sessions` |
| D | `let _ = "list_terminal_subscriptions";` | FAILED at `Pump must use the exact membership query` |

Arm A is the exact seed that stayed green at `2a0d5e5`. It is now red, so the coverage hole is closed. Neither token is a substring of the other, so the two arms are independent.

The `src/subscription/closed_events.rs` entry in `production_sources_reject_terminal_drain_and_snapshot_phase_decode` was re-proved after the file changed: the seeded arm exits 101 with `src/subscription/closed_events.rs production source must not contain GHOSTSNP`, and the restored control passes.

## Gates at `9b70f3c`

`RUSTUP_TOOLCHAIN=1.97.0`, `rustc 1.97.0 (2d8144b78 2026-07-07)`, `CARGO_TARGET_DIR` unset.

| Command | Result |
| --- | --- |
| `cargo fmt --all -- --check` | exit 0 |
| `cargo clippy --workspace --all-targets --locked -- -D warnings` | exit 0 |
| `cargo build --locked -p botster-core-daemon --bin botster-session-worker` | exit 0 |
| `cargo build --locked --bin botster-hub` | exit 0 |
| `./test.sh --locked`, run 1 | exit 101. `hub_daemon_lifecycle_test` 318 passed, 1 failed, 2 ignored. Every other target ok. |
| `BOTSTER_ENV=test cargo test --locked --test hub_daemon_lifecycle_test -- --exact webrtc_terminal_output_is_byte_exact` | `1 passed; 0 failed` |
| `./test.sh --locked`, run 2 | exit 0. 32 `test result: ok` lines, zero FAILED. Lib 495 passed. Lifecycle 319 passed, 2 ignored. hub-client 81 passed. |

## The one suite failure, attributed

Run 1 failed `webrtc_terminal_output_is_byte_exact` at `tests/hub_daemon_lifecycle/subscription_ownership_baseline.rs:885` with `WebRTC adapter frames must preserve exact bytes, got []`.

Evidence that the extraction did not cause it:

1. The test body is byte-identical to base. `diff` over lines 780-900 of that file between `fd540b6` and `9b70f3c` is empty, and the whole file carries only the two path-repair lines against base.
2. The failure signature is an empty frame vector, not wrong bytes. The loop has a fixed eight-second wall-clock deadline, so `got []` means no frame arrived in time, not that a byte differed. A broken grant, key derivation, or admission path would fail deterministically, not once.
3. Load average was 6.41 on 12 cores at the start of run 1, against 4.72 during the green suite of Verify visit 1.
4. The isolated rerun at the same commit passed, and run 2 at the same commit passed with the full lifecycle target green.
5. The only delta from the green suite of Verify visit 1 is eight lines inside `#[cfg(test)] mod tests` of the library. An integration test binary links the library compiled without `cfg(test)`, so `7062445` cannot reach this test at all.
6. Counting both Verify visits and both Implement visits, this test has four green full-suite observations on this branch and one red.

Not attempted: reproducing the same failure on `fd540b6` under matched load. The claim here is a host-load flake on a fixed deadline, not a pre-existing base failure, and points 1 and 5 establish that the branch did not change the code under this test. A loaded-base reproduction would strengthen the attribution, and it stays on the residual-risk list.

## Findings status after visit 2

- The Verify visit 1 finding is closed, with per-assertion red arms recorded above.
- `finding_1787984719_993379` and `finding_1787984719_792783` remain closed. Nothing in `7062445` touches the state owners or the test homes.
- Every acceptance result from Verify visit 1 still holds, because no production line changed. That includes the eleven source-guard ablation arms, the ownership greps, the byte-identical public item lists, the commit-kind proof, and the downstream client proof.

## Residual risk after visit 2

- `webrtc_terminal_output_is_byte_exact` has an eight-second wall-clock deadline and fails on a loaded host. It is a pre-existing flake shape rather than a branch defect, but it will keep costing suite reruns until the deadline becomes adaptive or the test waits on a producer acknowledgement.
- The unverified items from visit 1 are unchanged: packaged-browser grant-secret derivation, and byte-level equivalence of the unified close-event ledger beyond the existing per-transport tests.
- `entity_subscriptions`, `event_plane`, `pending_runtime`, and `released_entity_generations` still sit on `DaemonControlState`, which the approved plan allowed.

## Vault gaps after visit 2

The four candidates from visit 1 stand. Add a fifth: a live WebRTC frame test that carries a fixed wall-clock deadline and asserts on an accumulated byte buffer reports an empty buffer under host load, which reads like a byte-exactness defect. The empty-versus-wrong distinction is what separates a deadline flake from a real transport regression, and it belongs beside [[live acceptance tests must not depend on a loop tick window]].
