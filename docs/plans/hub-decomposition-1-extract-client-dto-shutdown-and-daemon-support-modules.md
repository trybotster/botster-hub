# Hub Decomposition 1: Extract Client DTO, Shutdown, and Daemon Support Modules

## Target Repository And Target Id

- Target repository: `botster-hub` (`trybotster/botster-hub`, `/Users/jasonconigliari/Projects/botster-hub`).
- Target id: `tgt_7e208a0c76a44980a83b63af976b1f22`.
- Ticket: `ticket_1787894414_324976`. Run: `run_1787956038_959297`. Step: `botster_stack_plan`.
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

## Scope

This ticket produces **one move-only commit** that compiles, plus a separate documentation commit for this plan.

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
5. Re-export the moved names from `src/lib.rs` or keep the existing `crate::daemon_transport::` call sites compiling through direct `use` paths, whichever preserves the current public surface exactly. `pub use botster_hub_client::{...}` in `daemon_transport.rs` stays where it is; those are client-crate re-exports, not Hub mappers.
6. Widen moved private items to `pub(crate)` only where the move requires it. This is the one visibility change the move forces and it adds no new public API.
7. Update `src/lib.rs` with `pub mod client_api_dto;`, and update `src/daemon.rs` with `pub(crate) mod error;` and `pub(crate) mod shutdown;`.

Out of scope (explicit non-scope):

- Any behavior, wire, serde-name, protocol-version, or proof-name change.
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

Created:

- `src/client_api_dto.rs`
- `src/client_api_dto/response.rs`
- `src/client_api_dto/session.rs`
- `src/client_api_dto/package.rs`
- `src/client_api_dto/workspace.rs`
- `src/client_api_dto/plugin.rs`
- `src/daemon/shutdown.rs`
- `src/daemon/error.rs`

Modified:

- `src/daemon_transport.rs` -- loses the three responsibilities and their tests; keeps transport, owner loop, control dispatch, and suppression.
- `src/lib.rs` -- adds the `client_api_dto` module declaration and preserves the existing public re-export surface.
- `src/daemon.rs` -- adds the `error` and `shutdown` module declarations.
- Call-site import lines only, in `src/daemon_attach_stream.rs`, `src/daemon_entity_subscriptions.rs`, `src/daemon_package_control.rs`, `src/local_runtime_process.rs`, `src/update.rs`, and `src/main.rs`.
- `docs/plans/hub-decomposition-1-extract-client-dto-shutdown-and-daemon-support-modules.md` -- this plan, in its own commit.

Unchanged, and required to stay unchanged:

- `crates/botster-hub-client/generated/daemon-protocol.ts`
- `crates/botster-hub-client/src/lib.rs` and `crates/botster-hub-client/src/typescript.rs`
- `crates/botster-hub-test-support/**` and `crates/botster-ui-contract/**`
- `Cargo.toml`, `Cargo.lock`, `test.sh`, `.github/workflows/**`

## Risks

1. **Wrapper risk.** The easiest wrong outcome is a private helper left behind in `daemon_transport.rs` that still holds implementation. Mitigation: after the move, `grep` `daemon_transport.rs` for each moved function name and require zero definition hits.
2. **Hidden behavior change under a move label.** A reordered `match` arm or a changed `env::var` read would alter shutdown classification silently. Mitigation: `--color-moved=dimmed-zebra` must show every moved line as moved, and every moved test must pass unmodified.
3. **Visibility over-widening.** Making moved items `pub` instead of `pub(crate)` would add public API in a move-only commit. Mitigation: default to `pub(crate)`; use `pub` only where the item was already `pub`.
4. **Import churn masking the move.** Large `use` rewrites can drown the move diff. Mitigation: keep import edits minimal and grouped at the top of each file.
5. **Clippy cascade.** Strict Clippy can hide later diagnostics behind the first compile failure. Mitigation: rerun the full workspace Clippy after every repair, per [[strict clippy can hide later crate diagnostics behind the first compile failure]].
6. **Toolchain drift.** A pipeline shell can pin Rust below the CI pin, so a bare strict Clippy can pass on unfixed code. Mitigation: run every Rust gate with `RUSTUP_TOOLCHAIN=1.97.0` and record `rustc --version` from that shell.
7. **Gate environment.** `CARGO_TARGET_DIR` must be unset for the official gate. The worktree path holds no `:` character, so no relocation is needed.
8. **Suite host noise.** The lifecycle suite needs a quiet host. Mitigation: run `./test.sh --locked` on a quiet host and re-run a failing test in isolation before classifying it.

## Acceptance Checks And Tests

Ownership proof (the real acceptance test):

1. `grep -n` in `src/daemon_transport.rs` returns **zero** definitions of `DaemonTransportError`, `PackageRollbackFailure`, `DaemonTransportResult`, `ShutdownSessionClassification`, `classify_shutdown_session`, `classify_found_session_lifecycle`, `forced_shutdown_classify_stopping`, `forced_shutdown_classify_stopping_from`, `shutdown_error_response`, `shutdown_error_is_already_gone`, `shutdown_lookup_error`, `recover_from_exact_classify`, `recover_after_core_shutdown_error`, `response_after_core_shutdown_error`, and every moved DTO mapper name.
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

## Runtime-Teardown Class

The class applies, because the ticket moves daemon control-plane shutdown classification. This slice is move-only, so every answer below states the invariant that must survive the move rather than a new design.

1. **Isolation.** No ownership set changes. `classify_shutdown_session` reads one exact session through `observe_session_lifecycle`. It touches no sibling session and no peer map. Proof: `recover_exact_missing_returns_unknown_session` and `recover_exact_exited_cleanup_stays_already_exited` stay green after the move.
2. **Bounded teardown.** No wait, timeout, or `block_on` moves. Classification is a synchronous exact-session query with a typed `Err` arm; it introduces no unbounded wait. Proof: `recover_classify_err_preserves_typed_runtime_error` and `recover_classify_err_preserves_typed_state_error` stay green.
3. **Late-message admission matrix.** No message type gains or loses durable ownership. `ShutdownSession` dispatch, route suppression, and admission all stay in `daemon_transport.rs`. Proof: `shutdown_session_arm_installs_exact_suppression_before_core_request` and `close_event_suppression_matrix_matches_prior_predicate` stay in place and green, which is also the guard that suppression did not move by accident.
4. **Production-path hard-stop proof.** The production path is unchanged: `DaemonRequest::ShutdownSession` at `src/daemon_transport.rs:3893` calls `classify_shutdown_session`, which after the move resolves to `crate::daemon::shutdown::classify_shutdown_session`. The production entry point therefore uses the moved code; this is not scaffold-only. Proof: the call site compiles against the new path, and `./test.sh --locked` exercises the daemon lifecycle suite.
5. **Ownership identity.** Classification keys on the exact `session_id` string supplied by the request. No generation, subscription id, or peer id semantics move. Per [[host ShutdownSession classification must call the exact-session Core query]], the moved code must keep calling `observe_session_lifecycle` and must keep the typed `Absent` and `Err` arms distinct. Proof: `shutdown_unknown_session_error_while_active_is_already_exited_cleanup` and `shutdown_stopping_record_is_host_cleanup_not_active`.
6. **Sibling and fail-closed policy.** Unchanged. One session's classification cannot affect a sibling, and no blast radius widens. Proof: the moved tests are pure-value tests over one record each.

## Vault Gaps Worth Capturing

1. A note that `src/daemon.rs` plus a sibling `src/daemon/` directory is the chosen Hub module shape, so later decomposition tickets do not rename `daemon.rs` mid-migration.
2. A note that a Hub move-only commit is reviewed with `git diff --color-moved=dimmed-zebra`, and that visibility widening to `pub(crate)` is move-forced rather than new API.
3. A note that close-event suppression belongs to `subscription/closed_events.rs` and not to `daemon/shutdown.rs`, so migration steps 2 and 3 do not contend for the same functions.
4. A note that the Hub client-contract oracle for a zero-DTO-change slice is `cargo test -p botster-hub-client` plus byte-identity of `crates/botster-hub-client/generated/daemon-protocol.ts`.
