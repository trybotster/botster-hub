# Implement report: Hub decomposition 1

## Target repository and target_id

| Field | Value |
| --- | --- |
| Target repository | `botster-hub` (`trybotster/botster-hub`) |
| `target_id` | `tgt_7e208a0c76a44980a83b63af976b1f22` |
| Authoritative spawn target | `list_spawn_targets` maps this id to `botster-hub` |
| Pipeline worktree | this run worktree |
| Ticket | `ticket_1787894414_324976` |
| Run | `run_1787956038_959297` |
| Step | `botster_stack_implement` (`run_step_1787961279_454276`) |
| Approved plan | `docs/plans/hub-decomposition-1-extract-client-dto-shutdown-and-daemon-support-modules.md` |
| Merge policy | direct into `main`; do not create a PR |
| Base | `origin/main` `8137d16907b98e60c6714a1dedc157f04e5367ae` (0 behind at Implement start) |
| Move-only code commit | `468bf7f034f19d9fb5b8eee16fca8fc4f3323f79` |
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
- [[botster hub gravity must be watched before it becomes the new monolith]]
- [[botster hub is a first party host profile over core]]
- [[botster Hub Rust stays a trusted host kernel]]
- [[host ShutdownSession classification must call the exact-session Core query]]
- [[ShutdownSession suppresses exact route generations before Core teardown]]
- [[botster runtime teardown lenses]]
- [[botster Hub pipeline shells can override RUSTUP TOOLCHAIN below the CI pin]]
- [[Hub official gates must not set CARGO TARGET DIR]]
- [[Hub suite runs prebuild the session worker before the locked test wrapper]]
- [[strict clippy can hide later crate diagnostics behind the first compile failure]]
- [[a ui contract import line change costs one test line in each generic client]]
- [[test script required for rust tests not cargo test]]
- [[implementation artifacts must match actual git state]]
- [[implement gate must verify committed work and pr link before review]]
- [[implementation steps must persist report artifacts for review]]
- [[pipeline vault checklists must cite exact resolvable note titles]]
- [[git-visible Hub member manifests must use the UI contract tag]]
- [[a ui contract git tag is unusable by external Cargo until pushed]]

### Constraints applied before edits

- Work only in the Hub run worktree for `tgt_7e208a0c76a44980a83b63af976b1f22`.
- Follow the approved plan. Keep Hub host-policy ownership.
- One move-only code commit. Do not add a tracked public-path guard.
- New modules are `pub(crate)`. Preserve every existing public path.
- Do not move close-event suppression or `HubDaemon` dispatch builders.
- Do not change the Core pin, DTO shapes, serde names, or protocol version.
- Run every Rust gate with `RUSTUP_TOOLCHAIN=1.97.0` and `CARGO_TARGET_DIR` unset.

## Files changed

Move-only code commit `468bf7f`:

| Path | Why |
| --- | --- |
| `src/client_api_dto.rs` | crate-private module root for DTO family files |
| `src/client_api_dto/response.rs` | `daemon_response_base` and response-envelope constructors |
| `src/client_api_dto/session.rs` | session, session-type, screen, mode, capture, and label mappers |
| `src/client_api_dto/package.rs` | package, pin, availability, and action mappers |
| `src/client_api_dto/workspace.rs` | spawn-target and worktree DTO mappers |
| `src/client_api_dto/plugin.rs` | plugin, coordination, and envelope mappers |
| `src/daemon/shutdown.rs` | shutdown classification plus its unit tests |
| `src/daemon/error.rs` | `DaemonTransportError` family, mapping functions, compensation test |
| `src/daemon.rs` | `pub(crate) mod error;` and `pub(crate) mod shutdown;` |
| `src/lib.rs` | one added line: `pub(crate) mod client_api_dto;` |
| `src/daemon_transport.rs` | loses the three responsibilities; keeps transport, owner loop, dispatch, suppression, and a `pub use` path alias |
| `src/daemon_attach_stream.rs` | error `use` path only |
| `src/daemon_entity_subscriptions.rs` | error `use` path only |
| `src/daemon_event_subscriptions.rs` | `daemon_response_base` `use` path only |
| `src/daemon_package_control.rs` | error `use` path only |

`git diff --stat` for the code commit: 15 files, 2895 insertions, 2694 deletions.

Unchanged, as required: `src/main.rs`, `src/update.rs`, `src/local_runtime_process.rs`, `crates/**`, `Cargo.toml`, `Cargo.lock`, `test.sh`.

Review the move with:

```
git show --color-moved=dimmed-zebra 468bf7f
git show --color-moved=dimmed-zebra --color-moved-ws=allow-indentation-change 468bf7f
```

## Ownership boundaries preserved

- Hub still owns mapping from Hub/Core domain values into client DTOs. `botster-hub-client` still owns DTO shapes, serde names, `PROTOCOL`, and generated TypeScript.
- Shutdown classification still interprets `SessionLifecycleLookup` through `observe_session_lifecycle`. It does not call Drain, baseline, or capped pagination.
- Close-event suppression stays in `daemon_transport.rs`. The source guard `fn shutdown_session_arm_installs_exact_suppression_before_core_request` still lives in that file.
- `PendingRuntimeState` and every ownership-creating registry stay in their pre-move modules.
- Public path set is unchanged in both directions: no new `pub mod`, and `botster_hub::daemon_transport::{DaemonTransportError, DaemonTransportResult, PackageRollbackFailure}` still resolve.

## Cross-repo routing

None. No new ticket dependency. Downstream Web/TUI cost is zero because no DTO field, serde name, or protocol version changed.

## Deviations from plan

1. `forced_stopping_classify_inject_requires_test_mode` still proves the same inject gate, but its `include_str!` now reads `shutdown.rs` instead of `daemon_transport.rs`. Leaving the old path would scan a file that no longer defines `classify_shutdown_session`. This is a path retarget required by the move, not a behavior change.
2. `WEBRTC_SIGNAL_OPERATION` moved into `src/daemon/error.rs` with `daemon_operator_error_from_local_webrtc`, its only remaining user. Leaving it in `daemon_transport.rs` would be an unused constant.
3. `#[rustfmt::skip]` sits on the exact `pub use crate::daemon::error::{DaemonTransportError, DaemonTransportResult, PackageRollbackFailure};` line so acceptance check 23's exact string survives rustfmt wrapping.
4. rustfmt rewrapped some `pub(crate)` signatures and reindented moved test bodies. Reviewers should use `--color-moved-ws=allow-indentation-change` as the plan already allows.

No scope expansion. The committed plan's acceptance checks remain the contract.

## Tests and downstream proof

All commands used `RUSTUP_TOOLCHAIN=1.97.0`. `rustc --version` was `rustc 1.97.0 (2d8144b78 2026-07-07)`. `CARGO_TARGET_DIR` was unset.

| Check | Result |
| --- | --- |
| Temporary `tests/public_path_probe.rs` at base | `cargo test --locked --test public_path_probe`: 1 passed, 0 failed. Probe deleted before the move commit. |
| Same probe after the move | 1 passed, 0 failed. Probe deleted again and never staged. |
| `cargo fmt --all -- --check` | exit 0 |
| `cargo clippy --workspace --all-targets --locked -- -D warnings` | exit 0 after the move |
| `cargo test -p botster-hub-client --locked` | 81 passed, 0 failed; doctests 4 passed |
| `git diff --exit-code -- crates/botster-hub-client/generated/daemon-protocol.ts` | no change |
| `node packages/hub-test-support/scripts/sync-assets.mjs --check` | assets current |
| Prebuild `botster-session-worker` and `botster-hub` | both succeeded into default `target/` |
| Named production-path `ShutdownSession` tests (8) | all passed through live daemon sockets into the moved classifier |
| `./test.sh --locked` (official rerun) | exit 0 |

Production entry point: `DaemonRequest::ShutdownSession` in `src/daemon_transport.rs` still calls `classify_shutdown_session`, which now resolves to `crate::daemon::shutdown::classify_shutdown_session`. This is not scaffold-only.

Moved unit tests now run as `daemon::shutdown::tests::*` and `daemon::error::tests::package_compensation_projects_every_rollback_to_socket_diagnostics`.

First `./test.sh --locked` failed on `real_lua_plugin_cross_package_managed_session_type_spawning` (`cross-package command marker: NotFound`). Isolation on this branch with the exact filter passed (exit 0). The full `hub_lua_runtime_test` file then passed 62/0 under default concurrency. The official wrapper rerun passed. That failure is host-load noise around a git fixture marker, not a product change from this move.

## Runtime-teardown lenses

| Lens | Survive-the-move evidence |
| --- | --- |
| Isolation | Classification still reads one exact session. Moved tests `recover_exact_missing_returns_unknown_session` and `recover_exact_exited_cleanup_stays_already_exited` passed. |
| Bounded teardown | No wait, timeout, or `block_on` moved. Typed `Err` arms remain. |
| Late-message matrix | Attach/entity/event/spawn/hello registries did not move. Registry modules changed only `use` lines. Suppression stayed in `daemon_transport.rs`. |
| Production-path hard-stop | The eight named live `ShutdownSession` tests passed unmodified. |
| Ownership identity | Exact `session_id` plus `observe_session_lifecycle`; `Absent` and `Err` stay distinct. |
| Sibling / fail-closed | One-record unit tests and `external_hub_shutdown_session_failure_keeps_daemon_and_sibling_usable` passed. |

## Unverified behavior or residual risk

- `git show --color-moved=dimmed-zebra` is the reviewer oracle for move detection. rustfmt wrapping on `pub(crate)` lines can hide some hunks unless `--color-moved-ws=allow-indentation-change` is used.
- Close-event suppression remains in `daemon_transport.rs` for migration step 3. That is intentional.
- Downstream Web and TUI were not rebuilt. Zero DTO change makes that cost assertion hold; it is not a live consumer rebuild.

## Missing vault guidance discovered

The plan already listed these gaps. They are still missing as dedicated notes:

1. `src/daemon.rs` plus a sibling `src/daemon/` directory is the chosen Hub module shape.
2. A Hub move-only commit is reviewed with `git diff --color-moved=dimmed-zebra`, and `pub(crate)` widening is move-forced.
3. Close-event suppression belongs to `subscription/closed_events.rs`, not `daemon/shutdown.rs`.
4. The Hub client-contract oracle for a zero-DTO-change slice is `cargo test -p botster-hub-client` plus byte-identity of `daemon-protocol.ts`.
5. A pipeline worktree can start behind `origin/main`; Plan must compare and rebase before baseline evidence.
6. Hub `hub_source()` guards must be enumerated before a move.
7. Extraction must check the public path set in both directions, including `pub(super)` meaning change and `PackageRollbackFailure` as the only-path case.
8. A `pub use` type alias is not the forwarding wrapper an extraction forbids.

No new convention conflict with the loaded notes.
