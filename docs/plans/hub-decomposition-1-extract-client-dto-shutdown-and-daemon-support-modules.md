# Hub Decomposition 1: Extract Client DTO, Shutdown, and Daemon Support Modules

## Target Repository And Target Id

- Target repository: `botster-hub` (`trybotster/botster-hub`, `/Users/jasonconigliari/Projects/botster-hub`).
- Target id: `tgt_7e208a0c76a44980a83b63af976b1f22`.
- Ticket: `ticket_1787894414_324976`. Run: `run_1787956038_959297`. Step: `botster_stack_plan`, run step `run_step_1787960403_555931` (sixth Plan visit; the five earlier visits each returned `changes_required`).
- Base commit: `8137d16` (`origin/main` after the round-2 rebase). The first Plan visit used the stale base `a0c7141`.
- This section names the current run step only. Per visit, the authoritative enumeration of plan commits and run step lives in that visit's gate evidence, not in this document, so the prose cannot go stale between rounds.
- The pipeline resolved the target id through `list_spawn_targets`. The plan does not infer the repository from the working directory.
- Blocking dependency `ticket_1787894962_603665` (fanout owner-loop flake and IsolatedHub worker reaping) is `closed`. No open blocking dependency remains.

## Repository Playbook Loaded

- [[botster-hub-playbook]] -- the repository ownership charter for `botster-hub`.

## Other Role And Surface Playbooks And Atomic Notes Loaded

Role playbooks:

- [[planner-playbook]]
- [[botster-planner-playbook]]

Atomic notes:

- [[daemon transport extraction moves ownership before deleting the facade]] -- the migration order and the frozen target directory map.
- [[Hub extraction must reduce ownership rather than only split files]] -- an extraction must move implementation, state, policy, and tests.
- [[botster hub gravity must be watched before it becomes the new monolith]] -- the drift this decomposition answers.
- [[botster hub is a first party host profile over core]] -- Hub owns trusted product policy over policy-free Core.
- [[botster Hub Rust stays a trusted host kernel]] -- Hub Rust owns privileged boundaries only.
- [[host ShutdownSession classification must call the exact-session Core query]] -- the invariant the moved shutdown code must preserve.
- [[ShutdownSession suppresses exact route generations before Core teardown]] -- the suppression responsibility this ticket does **not** move.
- [[botster runtime teardown lenses]] -- loaded because the ticket moves daemon shutdown classification. See the runtime-teardown section below.
- [[botster Hub pipeline shells can override RUSTUP TOOLCHAIN below the CI pin]] -- strict gates must run under Rust `1.97.0`.
- [[Hub official gates must not set CARGO TARGET DIR]] -- the official locked gate needs the default worktree `target/`.
- [[Hub suite runs prebuild the session worker before the locked test wrapper]] -- prebuild before `./test.sh --locked`.
- [[strict clippy can hide later crate diagnostics behind the first compile failure]] -- rerun the full workspace Clippy after each repair.
- [[a ui contract import line change costs one test line in each generic client]] -- the downstream cost rule that a zero-DTO-change move must keep at zero.

Required Botster planning context from [[botster-planner-playbook]]:

- [[botster-architecture]] -- the Botster domain map and source of architectural truth. It names `daemon transport extraction moves ownership before deleting the facade` and `Hub extraction must reduce ownership rather than only split files` as current architecture, which confirms this ticket is migration step 2 of a ratified plan and not an opportunistic refactor.
- [[cli-patterns]] -- Rust CLI, TUI, PTY, and terminal-layer constraints. The constraint that binds this ticket is [[integration tests should use public agent apis not crate-internal test-only helpers]]: the moved tests are crate-internal unit tests and must stay unit tests inside their new modules. They must not become `tests/` integration tests, because that would force new public API in a move-only commit.
- [[spa-patterns]] -- React SPA and entity-store constraints. This ticket touches no SPA surface. The relevant entry is [[botster hub client state sync is entity frame only]], which holds because this ticket changes no entity frame, DTO shape, or serde name. The SPA layer therefore needs no change and no proof beyond the byte-identity oracle in check 7.
- [[botster orchestration should spawn agents with explicit target ids]] and [[botster orchestration prompts must bind agents to explicit worktrees]] -- satisfied: this run binds `tgt_7e208a0c76a44980a83b63af976b1f22` and the worktree `/Users/jasonconigliari/botster-sessions/trybotster-botster-hub-project-pipelines-ticket_1787894414_324976`.

The remaining [[botster-planner-playbook]] must-load entries are the Project Pipelines orchestration notes. They do not constrain this ticket, because it changes no Project Pipelines package, plugin, or operator surface.

[[project-pipelines-playbook]] is **not** loaded. This ticket changes no Project Pipelines package, plugin, or workflow policy path.

## Context Loaded

- Vault capture: `/Users/jasonconigliari/knowledge/ops/archive/inbox/2026-08-27-botster-wake-driven-data-plane-and-hub-decomposition.md` (vault commit `8ef01f56`). It freezes the target Hub directory map and puts this ticket at migration step 2.
- Project record `project_1787600579_585482`: decomposition order, cold-cut rules, and the rule that every extraction commits move-only before behavior changes.
- Repository evidence at HEAD `a0c7141`:
  - `src/daemon_transport.rs` is 10,573 lines and owns the three responsibilities this ticket moves.
  - `src/daemon.rs` already exists and `src/lib.rs:57` already declares `pub mod daemon;`. Rust 2018+ module resolution allows `src/daemon.rs` plus a sibling `src/daemon/` directory, so this ticket adds `src/daemon/shutdown.rs` and `src/daemon/error.rs` without renaming `daemon.rs`.
  - `src/client_api_dto/` and `src/daemon/` do not exist yet.
  - `DaemonTransportError` is declared at `src/daemon_transport.rs:8314`, with `Display`, `Error`, and twelve `From` implementations through roughly line 8490, plus `pub type DaemonTransportResult<T>` at line 8493.
  - Shutdown classification lives at `src/daemon_transport.rs:4957-5237`: `ShutdownSessionClassification`, `response_after_core_shutdown_error`, `recover_after_core_shutdown_error`, `recover_from_exact_classify`, `shutdown_error_is_already_gone`, `shutdown_error_response`, `forced_shutdown_classify_stopping`, `forced_shutdown_classify_stopping_from`, `classify_shutdown_session`, `classify_found_session_lifecycle`, and `shutdown_lookup_error`.
  - The pure client DTO mappers occupy roughly `src/daemon_transport.rs:6198-8310`. Twelve functions in that range take `&mut HubDaemon` or `&HubDaemon` and are therefore control dispatch, not DTO mapping.
  - `DaemonTransportError` is referenced from `src/daemon_attach_stream.rs`, `src/daemon_entity_subscriptions.rs`, `src/daemon_package_control.rs`, `src/local_runtime_process.rs`, `src/update.rs`, `src/main.rs`, and `src/lib.rs`.
  - The `Daemon*` DTO types themselves are defined in `crates/botster-hub-client/src/lib.rs`. Hub owns only the mapping into them.
  - `crates/botster-hub-client/generated/daemon-protocol.ts` is the checked generated TypeScript artifact. `crates/botster-hub-client/src/lib.rs` holds `generated_typescript_protocol_matches_checked_artifact`, the byte-identity oracle.
  - `test.sh` runs `node packages/hub-test-support/scripts/sync-assets.mjs --check` and then `BOTSTER_ENV=test cargo test --workspace "$@"`.
  - `.github/workflows/ci.yml` pins Rust `1.97.0` and runs `cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets --locked -- -D warnings`, and `./test.sh --locked`.

## Refreshed Official Baseline (Plan Review Round 2)

Plan Review round 1 returned `changes_required` with a high finding titled "The refreshed official baseline is not clean". The finding carried no details, so this section records an independent measurement.

The real defect was base staleness. The run created its worktree at `a0c7141`, but `origin/main` had advanced fifteen commits to `8137d16`. Those fifteen commits are the merge of the closed blocking dependency `ticket_1787894962_603665`. The plan had never been verified against them.

Correction applied: this branch is rebased onto `origin/main` at `8137d16`. The rebase was clean and carried the two documentation commits that existed at that moment, `c14bf70` and `f6db523`.

Commit accounting is stated as an invariant rather than a count, because a fixed count goes stale on every review round and did so twice: **every commit on this branch above `8137d16` is a documentation commit, and zero code commits exist yet.** The move-only code commit does not exist; the Implementer writes it. Each visit's gate evidence carries the exact commit list for that visit, and `git rev-list 8137d16..HEAD` is the live answer.

The fifteen new commits touch `crates/botster-hub-test-support/src/{isolated_hub.rs,lib.rs}`, `src/daemon_maintenance.rs`, `src/package_event_router.rs`, `src/runtime.rs`, `tests/hub_daemon_lifecycle/shutdown.rs`, and two plan documents. They touch none of the files this ticket moves code out of or into. Every line anchor cited in this plan survives the rebase unchanged: `src/daemon_transport.rs` is still 10,573 lines, `ShutdownSessionClassification` is still at line 4959, `DaemonTransportError` at 8314, `DaemonTransportResult` at 8493, and the `ShutdownSession` dispatch arm at 3893.

Official gate results on the refreshed base `8137d16`, all run with `RUSTUP_TOOLCHAIN=1.97.0` and `CARGO_TARGET_DIR` unset:

| Gate | Result |
|---|---|
| `rustc --version` | `rustc 1.97.0 (2d8144b78 2026-07-07)` |
| `cargo fmt --all -- --check` | exit 0 |
| `cargo clippy --workspace --all-targets --locked -- -D warnings` | exit 0, zero warnings and zero errors |
| `cargo test -p botster-hub-client --locked` | 81 passed, 0 failed; doc-tests 4 passed, 0 failed |
| `git diff --exit-code -- crates/botster-hub-client/generated/daemon-protocol.ts` | no change |
| `cargo build --locked -p botster-core-daemon --bin botster-session-worker` and `cargo build --locked --bin botster-hub` | both succeeded into the default worktree `target/` |
| `./test.sh --locked` | exit 0; 1381 passed, 0 failed across all suites |
| `git status --porcelain` | empty |

Control arm: the same gate set ran green at the stale base `a0c7141` before the rebase (`./test.sh --locked` exit 0, zero failures). The baseline is therefore clean both before and after the refresh.

This measurement does not reproduce a dirty refreshed baseline. Two explanations remain open and Plan Review should say which applies: the reviewer's shell may have run without the `RUSTUP_TOOLCHAIN=1.97.0` pin or with `CARGO_TARGET_DIR` inherited, or the lifecycle suite may have hit its known quiet-host sensitivity. If Plan Review holds the finding, it should attach the exact failing test name and command so the next round can attribute the failure instead of guessing.

## Scope

This ticket produces **one move-only code commit** that compiles. The plan itself lands in separate documentation commits, which are not part of the move-only commit and are excluded from every move-only proof.

In scope:

1. Create `src/client_api_dto.rs` plus `src/client_api_dto/` and move every pure client DTO mapper out of `src/daemon_transport.rs`. Split by DTO family so no single new file becomes the next monolith:
   - `client_api_dto/response.rs` -- `daemon_response_base` and the response-envelope constructors that wrap an already-built body.
   - `client_api_dto/session.rs` -- session, session-type, session-context, screen, mode-flag, capture, and cleanup mappers in both directions, plus `lifecycle_label` and `guarded_write_delivery_state_label`.
   - `client_api_dto/package.rs` -- package, availability, install-plan, pin, update-status, and entrypoint-action mappers, plus the package label helpers.
   - `client_api_dto/workspace.rs` -- spawn-target and worktree DTO mappers, `sanitized_worktree_display_path`, `sanitize_worktree_error_message`, `worktree_lifecycle_event`, and `worktree_failure_event`.
   - `client_api_dto/plugin.rs` -- plugin surface, plugin lifecycle, plugin worker counters, coordination, and routed-envelope mappers, plus `envelope_target_label`.
2. Create `src/daemon/shutdown.rs` and move the eleven shutdown-classification items listed under Context Loaded, together with their tests.
3. Create `src/daemon/error.rs` and move `DaemonTransportError`, `PackageRollbackFailure`, `DaemonTransportResult`, the `Display` and `Error` implementations, the twelve `From` implementations, and the error-to-`DaemonResponse` mapping functions: `daemon_operator_error`, `daemon_package_error`, `daemon_spawn_target_error`, `daemon_worktree_error`, `daemon_state_error`, `daemon_entrypoint_error`, `daemon_local_webrtc_error`, `daemon_snapshot_stream_forbidden_error`, `daemon_package_compensation_error`, `bound_compensation_message`, `hub_update_execution_error`, `local_webrtc_bootstrap_issue_error`, `daemon_app_launch_error`, `daemon_package_route_error`, `daemon_plugin_tool_error`, `daemon_operator_error_from_state`, `daemon_operator_error_from_entrypoint`, and `daemon_operator_error_from_local_webrtc`.
4. Move each test with the responsibility it proves. The shutdown tests are `forced_stopping_classify_inject_requires_test_mode`, `production_core_shutdown_error_keeps_active_runtime_as_operator_error`, `production_core_shutdown_error_keeps_active_state_as_operator_error`, `shutdown_unknown_session_error_while_active_is_already_exited_cleanup`, `shutdown_exited_classification_returns_cleanup_for_any_shutdown_error`, `shutdown_stopping_record_is_host_cleanup_not_active`, `recover_classify_err_preserves_typed_runtime_error`, `recover_recorded_stopping_after_classify_err_preserves_typed_error`, `recover_classify_err_preserves_typed_state_error`, `recover_exact_missing_returns_unknown_session`, `recover_exact_exited_cleanup_stays_already_exited`, `recover_exact_stale_cleanup_stays_stale_session`, `shutdown_active_runtime_error_remains_operator_error`, `shutdown_active_state_error_remains_operator_error`, and the local helper `shutdown_runtime_error`. The error test is `package_compensation_projects_every_rollback_to_socket_diagnostics`.
5. Preserve the public path set with **one** design, not a choice between two. `src/daemon_transport.rs` keeps exactly this line:

   ```rust
   pub use crate::daemon::error::{DaemonTransportError, DaemonTransportResult, PackageRollbackFailure};
   ```

   `src/lib.rs` is **not** an acceptable substitute. Its `pub use daemon_transport::{...}` block at lines 132-153 can only restore `botster_hub::DaemonTransportError` and `botster_hub::DaemonTransportResult`; it cannot restore `botster_hub::daemon_transport::PackageRollbackFailure`, which is that type's only public path. Round 1 wrote this item as an either/or and round 5 left the stale wording in place; the `src/lib.rs` arm is now struck. `src/lib.rs` keeps the crate-root block untouched. The separate `pub use botster_hub_client::{...}` block at `daemon_transport.rs:34` also stays where it is; those are client-crate re-exports, not Hub mappers.
6. Widen moved private items to `pub(crate)` only where the move requires it. This is the one visibility change the move forces and it adds no new public API.
7. Update `src/lib.rs` with one added line, `pub(crate) mod client_api_dto;`, and update `src/daemon.rs` with `pub(crate) mod error;` and `pub(crate) mod shutdown;`. Every new module is crate-private, so the ticket adds **no** new public path. Keep every existing public path by leaving one `pub use crate::daemon::error::{DaemonTransportError, DaemonTransportResult, PackageRollbackFailure};` line in `src/daemon_transport.rs`, so the ticket removes **no** public path either.

### Public surface invariant

Round 3 wrote `pub mod client_api_dto;` while also claiming the commit adds no public API. Those two statements contradicted each other: `pub mod` would publish `botster_hub::client_api_dto::*` as a new public path. The module is crate-private instead, which the repository evidence supports:

- Not one client DTO mapper in the moved range is declared bare `pub` today. Counting the whole mapper region gives one `pub(crate) fn`, four `pub(super) fn`, and the rest private. There is nothing public to preserve.
- No mapper is referenced outside the library crate. `src/main.rs` uses none of them, and no integration test under `tests/` calls one. The only `tests/` hit for a mapper name is the unrelated test function `daemon_worktree_crud_scopes_paths_to_spawn_targets_without_requiring_git`.
- The four `pub(super) fn` mappers must become `pub(crate) fn` on the move. Inside `daemon_transport`, `super` is the crate root, so `pub(super)` already meant crate-visible; inside `client_api_dto`, `super` would mean the `client_api_dto` module and would narrow visibility. Rewriting them to `pub(crate)` preserves the exact reach they have today rather than widening it.

`src/daemon/error.rs` is also `pub(crate)`, while `DaemonTransportError`, `PackageRollbackFailure`, and `DaemonTransportResult` stay `pub` **inside** it. A `pub` item inside a crate-private module, re-exported elsewhere, is the standard facade pattern. Because those types stay `pub`, the `pub fn` signatures in `daemon_transport` that return `DaemonTransportResult<T>` — `serve_daemon`, `request`, and `stream_attach` — keep a public type in a public signature and cannot trip the `private_interfaces` lint.

#### Existing public paths that must survive the error move

`src/lib.rs:61` declares `pub mod daemon_transport;`. Three moved items are `pub` inside that public module, so each already has a public path that this ticket must not delete:

| Existing public path | Also reachable at the crate root? |
|---|---|
| `botster_hub::daemon_transport::DaemonTransportError` | Yes, `botster_hub::DaemonTransportError` |
| `botster_hub::daemon_transport::DaemonTransportResult` | Yes, `botster_hub::DaemonTransportResult` |
| `botster_hub::daemon_transport::PackageRollbackFailure` | **No.** This is its only public path |

Round 4 would have deleted all three. `PackageRollbackFailure` is the sharpest case, because it is absent from the crate-root `pub use` list at `src/lib.rs:132-153`, so repointing that list would not have replaced it. Round 5 fixes this: `src/daemon_transport.rs` keeps

```rust
pub use crate::daemon::error::{DaemonTransportError, DaemonTransportResult, PackageRollbackFailure};
```

so all three `daemon_transport::` paths resolve exactly as before. The crate-root `pub use daemon_transport::{...}` block then needs **no edit at all**: it keeps re-exporting through `daemon_transport`, and its name set and source path are both byte-identical. The only `src/lib.rs` change in the whole ticket becomes the single added `pub(crate) mod client_api_dto;` line.

**A type re-export is not a forwarding wrapper.** The ticket forbids a wrapper that *retains implementation* in `daemon_transport.rs`, and [[daemon transport extraction moves ownership before deleting the facade]] requires implementation, state, policy, and tests to move. A `pub use` line moves all four and keeps only a path alias, which carries no body, no state, and no policy. `daemon_transport.rs` already uses this exact pattern at line 34, where it re-exports roughly ninety `Daemon*` types it does not define from `botster_hub_client`. Acceptance check 23 draws the line mechanically: `daemon_transport.rs` may name a moved item only on a `pub use` line and must contain zero `fn`, `impl`, `enum`, `struct`, or `type` definitions of it.

The other two moved sets need no such alias, and the repository confirms it: every client DTO mapper is `pub(crate)`, `pub(super)`, or private, and `ShutdownSessionClassification` is declared without `pub`. Neither set has a public path to preserve.

Out of scope (explicit non-scope):

- Any behavior, wire, serde-name, protocol-version, or proof-name change.
- Any change to the crate's public path set, in **either** direction. The commit must add no public path and remove none. Round 4 would have added one; the first draft of round 5 would have removed three.
- Any change to scheduling, admission, routes, transports, terminal pumping, owner-loop policy, or maintenance slices.
- ShutdownSession **close-event suppression**: `suppress_unix_session_close_events`, `suppress_webrtc_session_close_events`, `session_close_event_decision_for`, `session_close_event_decision`, and the tests `shutdown_session_arm_installs_exact_suppression_before_core_request` and `close_event_suppression_matrix_matches_prior_predicate`. The frozen map assigns closed-event route policy to `subscription/closed_events.rs`, which is migration step 3.
- The twelve `HubDaemon`-taking response builders in the mapper range (`list_spawn_targets_response`, `show_spawn_target_response`, `mutate_spawn_targets_response`, `mutate_spawn_targets_with_worktrees_response`, `list_worktrees_response`, `show_worktree_response`, `create_worktree_response`, `delete_worktree_response`, `daemon_targets`, `emit_worktree_lifecycle_event`, `check_package_update_response`, `preview_package_update_response`, `package_update_status`, `package_update_plan`). These are control dispatch and belong to migration step 4.
- The Core pin. `botster-core` stays at rev `7eafa470a18025895995bbedc20d34b58106a03b`.
- `botster-hub-client`, `botster-ui-contract`, and `botster-hub-test-support` source changes. No package version bump.
- Deleting `daemon_transport.rs`. That is migration step 6.
- Adding forwarding wrappers. A wrapper that retains implementation in `daemon_transport.rs` fails the ticket.

## Repository Ownership Boundaries And Cross-Repo Dependencies

- `botster-hub` owns the mapping from Hub and Core domain values into client DTOs. It does **not** own the DTO shapes. `botster-hub-client` owns `Daemon*` type definitions, serde names, `PROTOCOL`, and the generated TypeScript. This ticket moves only Hub-side mapping code and touches no client-crate source.
- `botster-core` and `botster-core-daemon` own session lifecycle records, registry state, and the exact-session lifecycle query. Hub shutdown classification only interprets `SessionLifecycleLookup` results. That interpretation is Hub host policy and correctly stays in Hub.
- `botster-web` and `botster-tui` are downstream client consumers of the generated protocol. This ticket asserts zero downstream cost because it changes no DTO field, no serde name, and no protocol version.
- Cross-repository prerequisites: none. This ticket registers no new ticket dependency. Its one dependency, `ticket_1787894962_603665`, is closed.
- Sibling coordination: later decomposition tickets edit the same source file. This slice must merge before migration steps 3 through 6 begin, per the project record's dependency-order rule.

## Assumptions And Unknowns

Assumptions, stated explicitly:

1. `src/daemon.rs` stays a file and gains a sibling `src/daemon/` directory. This is valid Rust 2018+ module resolution and avoids an unrelated rename in a move-only commit. If the Implementer finds the repository prefers `daemon/mod.rs`, that rename would be a second, separate commit.
2. `client_api_dto/` is a directory in the frozen map, so a family split is the intended shape. The five files above are the split; the Implementer may merge two families if a file would hold fewer than roughly fifty lines.
3. Close-event suppression is subscription route policy, not shutdown classification. This plan reads the ticket phrase "shutdown classification" as the `ShutdownSessionClassification` family only. The Plan Review agent should reject this reading if the project owner intended suppression to move now.
4. Private items become `pub(crate)` where the move requires it. The reviewer should treat visibility widening as move-forced, not as new API.
5. `git diff --color-moved=dimmed-zebra` detects these moves because top-level functions keep their indentation. If detection is weak on the moved test bodies, add `--color-moved-ws=allow-indentation-change`; do not reformat code to help the diff.

Unknowns for the Implementer or Plan Review to resolve:

- Whether any moved mapper is referenced by a `#[cfg(test)]` item that also needs to move. Resolve by compiling under `--all-targets`, not by reading alone.
- Whether `use super::*;` in the `daemon_transport` test module hides an import that a moved test needs. Resolve by writing explicit imports in the new test modules.
- The exact final line count of `daemon_transport.rs`. The estimate is a reduction of roughly 2,100 to 2,400 lines. Line count is evidence, not proof; the ownership check below is the proof.

## Affected Surfaces And Files

Created before the move, in its own commit:

- `tests/public_path_guard.rs` -- the external-crate public path guard from acceptance check 24. It is green at the base and must stay green and unmodified through the move.

Created by the move-only commit:

- `src/client_api_dto.rs`
- `src/client_api_dto/response.rs`
- `src/client_api_dto/session.rs`
- `src/client_api_dto/package.rs`
- `src/client_api_dto/workspace.rs`
- `src/client_api_dto/plugin.rs`
- `src/daemon/shutdown.rs`
- `src/daemon/error.rs`

Modified, with the exact reason each file changes:

- `src/daemon_transport.rs` -- loses the three responsibilities and their tests; keeps transport, owner loop, control dispatch, close-event suppression, and `PendingRuntimeState`.
- `src/lib.rs` -- adds exactly one line, `pub(crate) mod client_api_dto;`. The existing `pub use daemon_transport::{...}` block at lines 132-153 is **not** touched, because `daemon_transport` keeps re-exporting the three error types (see Existing Public Paths above). Round 4 planned to repoint that block; round 5 does not need to, which makes the crate-root surface byte-identical rather than merely name-identical.
- `src/daemon.rs` -- adds the `error` and `shutdown` module declarations.
- `src/daemon_attach_stream.rs` -- one `use` line. This file is a **submodule of `daemon_transport`** (declared at `src/daemon_transport.rs:123`), so its `use super::{DaemonTransportError, DaemonTransportResult};` at line 22 must become a `crate::daemon::error` path.
- `src/daemon_entity_subscriptions.rs` -- one `use` block. Also a `daemon_transport` submodule (declared at `src/daemon_transport.rs:142`); its `use super::{...}` list at line 25 splits so the two error names come from `crate::daemon::error`.
- `src/daemon_package_control.rs` -- one `use` block. Also a `daemon_transport` submodule (declared at `src/daemon_transport.rs:134`); its `use super::{...}` list at line 14 splits for `DaemonTransportError`, `DaemonTransportResult`, and `PackageRollbackFailure`.
- `src/daemon_event_subscriptions.rs` -- one `use` line. Its `use crate::daemon_transport::daemon_response_base;` at line 19 becomes `crate::client_api_dto::response::daemon_response_base`.
- `docs/plans/hub-decomposition-1-extract-client-dto-shutdown-and-daemon-support-modules.md` -- this plan, in its own commits.

Correction from round 2: `src/main.rs`, `src/update.rs`, and `src/local_runtime_process.rs` now move to the unchanged list. Round 2 wrongly listed them as needing import edits. They consume `botster_hub::DaemonTransportError` through the crate-root re-export, and that re-export is preserved, so they need zero changes. If any of the three does change, the move altered the public surface and the commit is wrong.

Unchanged, and required to stay unchanged:

- `src/main.rs`, `src/update.rs`, `src/local_runtime_process.rs` -- they consume the preserved crate-root re-export.
- `src/daemon_event_subscriptions.rs` beyond its single `use` line, and every ownership registry listed in the late-message matrix.
- `crates/botster-hub-client/generated/daemon-protocol.ts`
- `crates/botster-hub-client/src/lib.rs` and `crates/botster-hub-client/src/typescript.rs`
- `crates/botster-hub-test-support/**` and `crates/botster-ui-contract/**`
- `Cargo.toml`, `Cargo.lock`, `test.sh`, `.github/workflows/**`

## Risks

1. **Wrapper risk.** The easiest wrong outcome is a private helper left behind in `daemon_transport.rs` that still holds implementation. Mitigation: after the move, `grep` `daemon_transport.rs` for each moved function name and require zero definition hits.
2. **Hidden behavior change under a move label.** A reordered `match` arm or a changed `env::var` read would alter shutdown classification silently. Mitigation: `--color-moved=dimmed-zebra` must show every moved line as moved, and every moved test must pass unmodified.
3. **Public path drift, in three directions.** Widening a moved private item to bare `pub` would *add* public API; checks 21 and 22 catch that. Copying a `pub(super)` mapper verbatim would *narrow* its reach, because `super` is the crate root inside `daemon_transport` but would be the `client_api_dto` module after the move; those four must be rewritten to `pub(crate)`. Moving a `pub` item out of the `pub mod daemon_transport` without leaving an alias would *remove* an existing public path; check 23 catches that, and `PackageRollbackFailure` is the one item with no crate-root fallback.
4. **Import churn masking the move.** Large `use` rewrites can drown the move diff. Mitigation: keep import edits minimal and grouped at the top of each file.
5. **Clippy cascade.** Strict Clippy can hide later diagnostics behind the first compile failure. Mitigation: rerun the full workspace Clippy after every repair, per [[strict clippy can hide later crate diagnostics behind the first compile failure]].
6. **Toolchain drift.** A pipeline shell can pin Rust below the CI pin, so a bare strict Clippy can pass on unfixed code. Mitigation: run every Rust gate with `RUSTUP_TOOLCHAIN=1.97.0` and record `rustc --version` from that shell.
7. **Gate environment.** `CARGO_TARGET_DIR` must be unset for the official gate. The worktree path holds no `:` character, so no relocation is needed.
8. **Suite host noise.** The lifecycle suite needs a quiet host. Mitigation: run `./test.sh --locked` on a quiet host and re-run a failing test in isolation before classifying it.
9. **Source-scanning guard tests.** Four committed assertions read Hub source text and fail if a named item leaves the file they scan. `terminal_input_travels_as_a_json_control_request` requires `src/daemon_transport.rs` to contain `DaemonRequest::SendInput { session_id, data } =>` and `HubClientRequest::Input {`. `shutdown_suppresses_exact_route_generations_before_core_teardown` requires `src/daemon_transport.rs` to contain `fn shutdown_session_arm_installs_exact_suppression_before_core_request`. A third requires `src/daemon_attach_stream.rs` to contain `for_ready_then_history_attach()`. All four scanned items stay in place under this plan's scope, so the guards should stay green. The suppression guard is independent evidence for assumption 3: moving close-event suppression out of `daemon_transport.rs` would break a committed guard, which is a further reason to leave suppression to migration step 3. Mitigation: acceptance check 19 greps every `hub_source(` assertion before the move commit and confirms each scanned string still lives in the file it names.
10. **Base staleness.** The pipeline worktree started fifteen commits behind `origin/main`, which invalidated the round-1 baseline claim. Mitigation: acceptance check 16 compares the base against `origin/main` and rebases before the move commit.

## Acceptance Checks And Tests

Ownership proof (the real acceptance test):

1. `grep -n` in `src/daemon_transport.rs` returns **zero** *definitions* of `DaemonTransportError`, `PackageRollbackFailure`, `DaemonTransportResult`, `ShutdownSessionClassification`, `classify_shutdown_session`, `classify_found_session_lifecycle`, `forced_shutdown_classify_stopping`, `forced_shutdown_classify_stopping_from`, `shutdown_error_response`, `shutdown_error_is_already_gone`, `shutdown_lookup_error`, `recover_from_exact_classify`, `recover_after_core_shutdown_error`, `response_after_core_shutdown_error`, and every moved DTO mapper name. A `use` or `pub use` line naming one of these is not a definition and is permitted; check 23 governs which aliases may remain and proves they carry no body.
2. No `fn` in `daemon_transport.rs` forwards to a moved item while retaining its body.
3. The moved shutdown and error tests live in `src/daemon/shutdown.rs` and `src/daemon/error.rs` and run there.

Move-only proof:

4. `git show --color-moved=dimmed-zebra <commit>` renders the extraction as moved lines. Record the command and the reviewer instruction in the commit message.
5. `git diff --stat` for the move commit shows additions and deletions that roughly balance, excluding the module declarations and required import lines.

Client-contract oracle (authoritative, from the ticket):

6. `RUSTUP_TOOLCHAIN=1.97.0 cargo test -p botster-hub-client` passes, including `generated_typescript_protocol_matches_checked_artifact`.
7. `git diff --exit-code -- crates/botster-hub-client/generated/daemon-protocol.ts` reports no change. Byte-identity, not "equivalent".
8. `node packages/hub-test-support/scripts/sync-assets.mjs --check` passes (also run inside `test.sh`).
9. `git status --porcelain -- crates/` is empty after the full test run.

Strict Rust gates, each with `RUSTUP_TOOLCHAIN=1.97.0` and `rustc --version` recorded from the same shell:

10. `cargo fmt --all -- --check`
11. `cargo clippy --workspace --all-targets --locked -- -D warnings`, rerun in full after every repair.

Repository test wrapper:

12. `unset CARGO_TARGET_DIR`, then `cargo build --locked -p botster-core-daemon --bin botster-session-worker` and `cargo build --locked --bin botster-hub` into the default worktree `target/`.
13. `./test.sh --locked` on a quiet host.

Downstream proof:

14. Downstream client cost is asserted at zero and proved by checks 6 through 8. Because no DTO field, serde name, or protocol version changes, `botster-web` and `botster-tui` need no edit. If check 7 shows any diff, the ticket has left move-only scope and must stop.

Provenance:

15. Record the exact verified commit SHA, `git status --porcelain` output showing a clean tracked worktree, and `rustc --version`. Renew review after any semantic rebase.
16. Re-verify the base before Implement starts. Compare the worktree base against `origin/main`; if `origin/main` has advanced, rebase and rerun checks 10 through 13 before writing the move commit. The Refreshed Official Baseline section records the round-2 measurement at `8137d16`.
17. `git diff` shows that `src/daemon_attach_stream.rs`, `src/daemon_entity_subscriptions.rs`, and `src/daemon_event_subscriptions.rs` changed **only `use` lines**. Every other line in those three files is byte-identical, which proves no ownership-creating surface moved.
18. Run the named production-path set directly, not only through the wrapper: `BOTSTER_ENV=test cargo test --locked --test hub_daemon_lifecycle_test -- shutdown_session_classifies_parked_exit_beyond_one_baseline_page external_hub_shutdown_session_failure_keeps_daemon_and_sibling_usable unix_shutdown_session_stuck_stopping_without_exit_evidence_stays_operator_error unix_shutdown_session_from_another_connection_classifies_attached_exit shutdown_session_exact_keys_preserve_replacement_owner_and_siblings attached_stopping_shutdown_session_suppresses_exact_generation process_exit_and_shutdown_session_do_not_emit_terminal_subscription_closed shutdown_suppresses_exact_route_generations_before_core_teardown`. All eight must pass with unmodified bodies. These drive a real `DaemonRequest::ShutdownSession` through a live daemon socket into the moved classifier, which is the production-path proof the runtime-teardown lens requires.
19. `grep -rn "hub_source(" tests/` enumerates every source-scanning guard, and each scanned string still lives in the file that guard names. No guard is edited to accommodate the move.
20. `git diff --exit-code` reports no change in `src/main.rs`, `src/update.rs`, and `src/local_runtime_process.rs`. A change in any of the three means the crate-root re-export surface moved, and the commit is wrong.
21. Public surface adds nothing. `git diff src/lib.rs` shows exactly one added line, `pub(crate) mod client_api_dto;`, and **no other change**. The `pub use daemon_transport::{...}` block at lines 132-153 is untouched. Every new module (`client_api_dto`, `daemon::error`, `daemon::shutdown`) is declared `pub(crate)`.
22. `grep -nE "^pub (fn|enum|struct|type) " src/client_api_dto.rs src/client_api_dto/*.rs` returns nothing. No moved mapper is declared bare `pub`; each is `pub(crate)`, which matches the reach it had before the move. The four former `pub(super) fn` mappers are `pub(crate) fn` after the move for the reason given in the Public Surface Invariant.
23. Public surface loses nothing, and the surviving alias is a path alias rather than a wrapper. Both arms must hold:
    - `src/daemon_transport.rs` contains `pub use crate::daemon::error::{DaemonTransportError, DaemonTransportResult, PackageRollbackFailure};`, so `botster_hub::daemon_transport::DaemonTransportError`, `::DaemonTransportResult`, and `::PackageRollbackFailure` all still resolve. `PackageRollbackFailure` matters most: it has no crate-root alias, so this line is its only surviving public path.
    - `grep -nE "^(pub )?(fn|impl|enum|struct|type) " src/daemon_transport.rs` shows **zero** definitions of any moved item. The file may name a moved item only on a `use` or `pub use` line. This is the mechanical line between a permitted path alias and the forwarding wrapper the ticket forbids.
24. **External-crate compile proof for every existing public path.** Round 5 offered a throwaway check or a `cargo doc` comparison; neither is exact and neither is reproducible by a reviewer. Replace both with one committed integration test. Files under `tests/` compile as separate crates that consume `botster_hub` as an external dependency, so this is a genuine external-crate resolution check rather than an intra-crate one.

    Add `tests/public_path_guard.rs`:

    ```rust
    //! Guards the exact public paths the decomposition must preserve.
    //! Each alias fails to compile if its path stops resolving.
    type _TransportError = botster_hub::daemon_transport::DaemonTransportError;
    type _TransportResult = botster_hub::daemon_transport::DaemonTransportResult<()>;
    type _RollbackFailure = botster_hub::daemon_transport::PackageRollbackFailure;
    type _RootError = botster_hub::DaemonTransportError;
    type _RootResult = botster_hub::DaemonTransportResult<()>;

    #[test]
    fn hub_public_paths_resolve() {}
    ```

    That covers all five existing public paths: the three through `pub mod daemon_transport` and the two crate-root aliases. Run it with the exact command:

    ```
    RUSTUP_TOOLCHAIN=1.97.0 cargo test --locked --test public_path_guard
    ```

    **Commit it before the move, in its own commit.** The test must be green at the base `8137d16` first, which establishes the control: the paths resolve today. The move-only commit must then leave it green with the file unmodified. A guard written after the move would only restate whatever the move produced and would prove nothing. Modifying this file to accommodate the move is a failure, exactly as check 19 forbids for the `hub_source()` guards.

## Runtime-Teardown Class

The class applies, because the ticket moves daemon control-plane shutdown classification. This slice is move-only, so every answer below states the invariant that must survive the move rather than a new design.

1. **Isolation.** No ownership set changes. `classify_shutdown_session` reads one exact session through `observe_session_lifecycle`. It touches no sibling session and no peer map. Proof: `recover_exact_missing_returns_unknown_session` and `recover_exact_exited_cleanup_stays_already_exited` stay green after the move.
2. **Bounded teardown.** No wait, timeout, or `block_on` moves. Classification is a synchronous exact-session query with a typed `Err` arm; it introduces no unbounded wait. Proof: `recover_classify_err_preserves_typed_runtime_error` and `recover_classify_err_preserves_typed_state_error` stay green.
3. **Late-message admission matrix.** The lens requires every message type that creates durable ownership, not a blanket statement. The table below lists each one, its owner tag, its post-failure rejection, and its `PeerClosed` sweep. The final column is the claim this ticket must prove: the owning code does not move.

| Message | Durable row and owning module | Owner tag | Rejection after terminal failure | Sweep on `PeerClosed` race | Moves in this ticket |
|---|---|---|---|---|---|
| `Attach` | `AttachStreamRegistry.streams`, `active_subscriptions`, `attach_owner_grant_ids`, `connection_bound_routes` in `src/daemon_attach_stream.rs` | `AttachStreamOwner.grant_id` for WebRTC; the connection id for Unix | Pre-READY attach failure creates no route and increments no lifecycle count | Route-aware idempotent cleanup keyed on route identity; cannot decrement another route | No |
| `Detach` | Same registry, removal path | Same route identity | Detach failure cleanup is route-aware | Shares the live attach route set | No |
| `SubscribeEntities` / `UnsubscribeEntities` | Entity subscription rows in `src/daemon_entity_subscriptions.rs` | `owner_grant_id` | Typed operator error without dropping transport | Owner-scoped removal on peer close | No |
| `SubscribeEvents` / `UnsubscribeEvents` | Connection-scoped holders and bounded mailboxes in `src/daemon_event_subscriptions.rs` | Connection-scoped holder identity (private Unix client or WebRTC grant owner) | Bounded shed and typed rejection; at most 64 subscriptions per connection | Connection-scoped unsubscribe and cleanup | No |
| `Spawn` / `SpawnSessionType` | Session ownership in Core, reached through `HubRuntime` | Core session id | Typed operator error; no Hub row on failure | Session ownership is Core-owned and survives peer close | No |
| Unix `Hello` terminal admission | `PendingRuntimeState.unix_admissions` and `host_compatibility` in `src/daemon_transport.rs` | Connection id | Terminal admission can be rejected while host operations stay available | EOF cleanup shares the live attach route set | No |
| WebRTC `Hello` terminal admission | `PendingRuntimeState.webrtc_admissions` in `src/daemon_transport.rs` | `grant_id` | Encrypted admission required before bind | `PeerClosed` occupancy uses the live attach route set | No |
| `ShutdownSession` | Creates no durable ownership; it removes ownership | Exact `session_id` from the request | Typed `Absent` and `Err` arms stay distinct | Exact `(session_id, subscription_id, generation)` suppression before Core teardown | **Classification only.** Dispatch, suppression, and every registry above stay put |

`ShutdownSession` is therefore the only row this ticket touches, and it touches the classification decision rather than any ownership-creating surface. No row in the table is created, tagged, rejected, or swept by moved code, and `PendingRuntimeState` stays in `daemon_transport.rs`.

Round 2 stated this as "zero changed lines in the three registry modules", which contradicted the affected-files list that already expected import edits in two of them. The precise rule, which acceptance check 17 enforces, is: **`src/daemon_attach_stream.rs`, `src/daemon_entity_subscriptions.rs`, and `src/daemon_event_subscriptions.rs` may change only `use` lines.** Every other line in those three files must be byte-identical. Their registry types, owner-tag fields, insertion paths, rejection paths, and cleanup paths therefore cannot move, and no import edit can disguise a behavior change. The permitted edits are exactly the three named in Affected Surfaces And Files: one `use` line in `daemon_attach_stream.rs`, one `use` block in `daemon_entity_subscriptions.rs`, and one `use` line in `daemon_event_subscriptions.rs`.

Proof: `shutdown_session_arm_installs_exact_suppression_before_core_request` and `close_event_suppression_matrix_matches_prior_predicate` stay in place and green, which is also the guard that suppression did not move by accident.
4. **Production-path hard-stop proof.** The production path is unchanged: `DaemonRequest::ShutdownSession` at `src/daemon_transport.rs:3893` calls `classify_shutdown_session`, which after the move resolves to `crate::daemon::shutdown::classify_shutdown_session`. The production entry point therefore uses the moved code; this is not scaffold-only.

Round 2 named only the full `./test.sh --locked` wrapper, which is not a production-path oracle. The named integration tests below each drive a real `DaemonRequest::ShutdownSession` through a live daemon socket into the moved classifier. Every one must stay green and unmodified, and together they cover all four classification arms:

| Test | File | Classification arm it drives |
|---|---|---|
| `shutdown_session_classifies_parked_exit_beyond_one_baseline_page` | `tests/hub_daemon_lifecycle/sessions.rs:4213` | `Cleanup`, and specifically the exact-session query rather than baseline paging. This is the production oracle for [[host ShutdownSession classification must call the exact-session Core query]] |
| `external_hub_shutdown_session_failure_keeps_daemon_and_sibling_usable` | `tests/hub_daemon_lifecycle/sessions.rs:3639` | `Active` with a Core error, plus the sibling-survival policy |
| `unix_shutdown_session_stuck_stopping_without_exit_evidence_stays_operator_error` | `tests/hub_daemon_lifecycle/unix_terminal_adapter.rs:2421` | `Active` and `Stopping` separation over Unix; proves a stuck session stays a typed operator error rather than a false cleanup |
| `unix_shutdown_session_from_another_connection_classifies_attached_exit` | `tests/hub_daemon_lifecycle/unix_terminal_adapter.rs:2265` | Classification requested by a connection that does not own the attach |
| `shutdown_session_exact_keys_preserve_replacement_owner_and_siblings` | `tests/hub_daemon_lifecycle/unix_terminal_adapter.rs:1900` | Ownership identity under a reused subscription id |
| `attached_stopping_shutdown_session_suppresses_exact_generation` | `tests/hub_daemon_lifecycle/unix_terminal_adapter.rs:2188` | Suppression ordering ahead of Core teardown |
| `process_exit_and_shutdown_session_do_not_emit_terminal_subscription_closed` | `tests/hub_daemon_lifecycle/unix_terminal_adapter.rs:1797` | Teardown and pre-READY failure keep separate lifecycle meanings |
| `shutdown_suppresses_exact_route_generations_before_core_teardown` | `tests/hub_daemon_lifecycle/subscription_ownership_baseline.rs:727` | Live suppression proof, plus the source guard described under Risks |

The harness helper `classify_shutdown_session_response` at `tests/hub_daemon_lifecycle/harness.rs:601` reads these responses and must not change. Acceptance check 18 runs this named set directly rather than relying on the wrapper alone.
5. **Ownership identity.** Classification keys on the exact `session_id` string supplied by the request. No generation, subscription id, or peer id semantics move. Per [[host ShutdownSession classification must call the exact-session Core query]], the moved code must keep calling `observe_session_lifecycle` and must keep the typed `Absent` and `Err` arms distinct. Proof: `shutdown_unknown_session_error_while_active_is_already_exited_cleanup` and `shutdown_stopping_record_is_host_cleanup_not_active`.
6. **Sibling and fail-closed policy.** Unchanged. One session's classification cannot affect a sibling, and no blast radius widens. Proof: the moved tests are pure-value tests over one record each.

## Vault Gaps Worth Capturing

1. A note that `src/daemon.rs` plus a sibling `src/daemon/` directory is the chosen Hub module shape, so later decomposition tickets do not rename `daemon.rs` mid-migration.
2. A note that a Hub move-only commit is reviewed with `git diff --color-moved=dimmed-zebra`, and that visibility widening to `pub(crate)` is move-forced rather than new API.
3. A note that close-event suppression belongs to `subscription/closed_events.rs` and not to `daemon/shutdown.rs`, so migration steps 2 and 3 do not contend for the same functions.
4. A note that the Hub client-contract oracle for a zero-DTO-change slice is `cargo test -p botster-hub-client` plus byte-identity of `crates/botster-hub-client/generated/daemon-protocol.ts`.
5. A note that a pipeline worktree can start fifteen commits behind `origin/main`, so a Plan step must compare its base against `origin/main` and rebase before it records baseline gate evidence. This run recorded a stale-base plan in round 1 and had to redo the baseline in round 2.
6. A note that Hub carries source-scanning guard tests that assert on Hub source text through `hub_source()`. Any Hub decomposition ticket must enumerate every such assertion before moving code, because a move can break a guard without changing behavior, and editing a guard to accommodate a move would silently retire the proof.
7. A note that a Hub extraction must check the public path set in **both** directions. Moving a `pub` item out of a `pub mod` deletes a public path unless a `pub use` alias stays behind, and an item absent from the crate-root re-export list has no fallback; `PackageRollbackFailure` is the worked example. The companion rule is that new extracted modules are declared `pub(crate)`, because `pub mod` on an internal layer silently adds a public path to a commit that claims no API change. A third rule sits between them: `pub(super)` changes meaning when an item moves, so it must be rewritten to `pub(crate)` to preserve reach.
8. A note that a `pub use` type alias is not the forwarding wrapper an extraction forbids. The ban targets retained implementation, state, and policy; a path alias carries none. `daemon_transport.rs` already re-exports roughly ninety client types it does not define, which is the in-repository precedent.
