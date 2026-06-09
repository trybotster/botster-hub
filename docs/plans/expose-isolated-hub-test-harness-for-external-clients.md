# Expose Isolated Hub Test Harness For External Clients

## Context Loaded

- Project Pipelines context: ticket `ticket_1780982092_413591`, run `run_1780982099_677461`, active step `hotwire_plan`, gate `hotwire_plan_gate`, no prior findings/questions/artifacts.
- Orchestrator correction: this is a Rust `botster-hub` ticket, not a Hotwire/Rails app. Hotwire-specific guidance was loaded only because the pipeline requested it, then intentionally ignored for architecture decisions.
- Vault/playbook context: [[identity]], [[goals]], [[planner-playbook]], [[hotwire-app-planner-playbook]], [[botster-planner-playbook]], [[botster-architecture]], [[cli-patterns]], [[project pipeline orchestration belongs in a device-level botster plugin]], [[project pipelines needs an operator workbench not more primitives]], [[project pipelines ui contract belongs in the plugin readme]], [[botster orchestration should spawn agents with explicit target ids]], [[botster orchestration prompts must bind agents to explicit worktrees]], [[plan steps need reviewable plan artifacts]], [[project pipelines checklist worker timeouts require artifact evidence fallback]], [[plan agents must author vault context as wikilinks not home paths]], [[pipeline artifacts should cite vault notes by wikilink not home path]], [[pipeline artifacts should use path neutral worktree references]], [[test script required for rust tests not cargo test]], and [[rust repo strict lints must be verified before dismissing warnings]].
- Botster skill context: `botster-customize-hub`, for hub lifecycle/daemon/API boundary guidance.
- Repo context inspected: `Cargo.toml`, `crates/botster-hub-client/Cargo.toml`, `crates/botster-hub-client/src/lib.rs`, `src/lib.rs`, `src/config.rs`, `src/daemon.rs`, `src/daemon_transport.rs`, `src/main.rs`, `tests/support/mod.rs`, `tests/hub_daemon_lifecycle_test.rs`, `tests/hub_client_api_test.rs`, `test.sh`, and `docs/client-protocol.md`.
- Checklist discipline: `project_pipelines_checklist_instructions` loaded. `project_pipelines_create_vault_checklist` was attempted for this run and failed with `plugin worker invoke timeout`, so checklist evidence is preserved here and in gate evidence per [[project pipelines checklist worker timeouts require artifact evidence fallback]].

## Scope

- Add a narrow hub-owned test-support surface for downstream crates that need to start an isolated local `botster-hub` daemon/socket and connect with `botster-hub-client`.
- Prefer a new workspace crate such as `crates/botster-hub-test-support` rather than making downstream test crates depend on the full `botster-hub` library and its embedded TUI/Lua/runtime internals.
- The test-support API should:
  - create a disposable data directory under a caller-provided or target-local root;
  - use synthetic non-PII host identity and socket paths;
  - ensure the `botster-session-worker` binary is available;
  - start `botster-hub start --data-dir <isolated-dir>` as a child process;
  - wait for daemon readiness through the real protocol handshake/status path;
  - expose the `botster_hub_client::DaemonEndpoint` plus small lifecycle helpers for request/connection/shutdown/teardown;
  - clean up by sending daemon shutdown and waiting for the child process, with a kill-on-drop fallback for failed tests if practical.
- Move or duplicate only the minimal existing test-only helper logic needed for external clients. `tests/support/mod.rs` already contains worker-binary build logic that is a strong extraction candidate.
- Add a hub-owned integration test that uses only the public test-support crate plus `botster-hub-client` to prove the downstream path: start isolated daemon/socket, status, list sessions, spawn, attach/drain terminal output, send input, resize, detach/shutdown session, daemon teardown.
- Update `docs/client-protocol.md` with the exact downstream dependency shape and API sketch.
- Fix stale `src/tui.rs` compile errors only if the new crate/test path or workspace checks require it, but keep that as enabling cleanup rather than the ticket outcome.

## Non-Scope

- Do not implement botster-tui changes or add botster-tui as a repo dependency.
- Do not expose hub TUI, Lua runtime, plugin internals, or raw core/session-worker protocol types to downstream clients.
- Do not add product workflow primitives, Project Pipelines policy, Rails/Hotwire work, browser/WebRTC behavior, or broad daemon refactors.
- Do not mutate real user/device identity, default runtime data directories, or HOME/XDG state in tests.
- Do not replace the existing `botster-hub-client` protocol; the harness should compose it.

## Botster Layers Touched

- Rust hub daemon lifecycle and socket transport test harness.
- External client protocol crate dependency path.
- Session/client worker runtime path only through existing daemon protocol operations.
- Docs for external client/test-support usage.
- No plugin, SPA, Rails relay, or Hotwire layer.

## Assumptions And Unknowns

- Assumption: a separate `botster-hub-test-support` crate is acceptable as a dev/test support artifact and is the cleanest way to avoid stale embedded hub TUI internals for external clients.
- Assumption: Unix-only support is acceptable initially because current daemon/socket/PTTY integration tests are already `#![cfg(unix)]`.
- Assumption: using the `CARGO_BIN_EXE_botster-hub` test binary in hub-owned integration tests is fine, while downstream crates may need a documented binary resolution strategy. Implementation should decide whether the helper requires an explicit binary path or can discover one reliably.
- Assumption: the harness can depend on `botster-hub-client` and standard library process/filesystem/socket primitives without adding new crates.
- Unknown: external git/path dependency shape for a subcrate in this repository should be verified by Implementation. The documentation should be exact enough for botster-tui to consume, for example a `package = "botster-hub-test-support"` dev-dependency from the same git rev.
- Unknown: current `src/tui.rs` may still fail when compiling the full `botster-hub` crate. If workspace tests require compiling it, fix only the stale `UiActionStatus`/`UiActionResult.state` references needed to keep the hub build green.
- Worktree/target assumption: all work happens in the pipeline-provided ticket worktree; no external client repo work is included.

## Affected Surfaces And Files

- `Cargo.toml`: add the test-support crate as a workspace member and, if needed, dev-dependencies for hub-owned tests.
- `crates/botster-hub-test-support/Cargo.toml`: new package metadata and dependencies.
- `crates/botster-hub-test-support/src/lib.rs`: public isolated daemon harness API.
- `tests/support/mod.rs`: either remove duplicated worker-build logic in favor of the new crate or keep a thin compatibility wrapper for existing tests.
- `tests/hub_daemon_lifecycle_test.rs`: add or refactor one integration test to prove the public harness path.
- `docs/client-protocol.md`: document downstream usage and exact dependency/API shape.
- Potentially `src/tui.rs`: narrow stale compile fix only if required.
- Potentially `src/lib.rs`: avoid adding public re-exports for the test support crate unless implementation proves a re-export is necessary; the cleaner dependency boundary is a separate crate.

## Risks

- Full-hub dependency leakage: if the helper lives in `botster-hub`, downstream clients still compile stale TUI/Lua/runtime internals. Mitigation: separate workspace crate with narrow dependencies.
- Binary discovery risk: hub-owned tests can use Cargo test binary env vars, but external clients may not have the hub binary built. Mitigation: require or document explicit binary path configuration if reliable auto-discovery is not possible.
- Isolation risk: tests could accidentally use runtime defaults and touch real device identity/data. Mitigation: explicit data directory, synthetic host id/display name, local socket under the isolated directory, and path-neutral status checks.
- Teardown risk: failed tests can leave a daemon process or socket behind. Mitigation: lifecycle guard with shutdown and kill-on-drop fallback.
- Race/flakiness risk: real daemon/socket tests can race. Mitigation: keep process-wide serialization like `daemon_test_lock()` for hub-owned daemon tests and unique data dirs.
- Protocol drift risk: helper abstractions could wrap too much and mask actual downstream usage. Mitigation: the acceptance test must use `botster_hub_client::DaemonEndpoint`, `DaemonConnection`, and real daemon requests.
- Documentation drift risk: docs can claim a dependency shape that Cargo does not accept. Mitigation: use the exact crate/package names and a compile-checked hub-owned example where possible.

## Acceptance Checks And Tests

- `./test.sh --test hub_daemon_lifecycle_test external_hub_test_support_drives_isolated_daemon_socket_protocol` or the final exact test name chosen by Implementation.
- `./test.sh --test hub_daemon_lifecycle_test external_hub_client_crate_drives_real_daemon_socket_protocol` to preserve the existing external client proof.
- `./test.sh -p botster-hub-test-support` if the new crate has unit tests.
- `./test.sh --workspace` or `./test.sh` for full regression if runtime permits.
- `cargo clippy --all-targets --all-features -- -D warnings` should be run if the repo establishes strict lint enforcement during implementation/review. Planning inspection found no `[lints]` section in the current workspace manifests, but the strict-lint convention still requires checking before dismissing warnings.
- Documentation acceptance: `docs/client-protocol.md` includes an exact downstream dev-dependency/API example for starting an isolated daemon, constructing a `DaemonEndpoint`, issuing status/list/spawn/attach/drain/input/resize requests, and tearing down.
- PII/isolation acceptance: new tests assert or inspect that generated host id/display name are synthetic, data dirs live under test/target-local scratch space, socket paths are under the isolated data dir, and status/output does not print the absolute data dir where existing scrubbed-output checks apply.

## Pipeline Gates And Artifacts

- Plan artifact: this file.
- Gate evidence should attach this plan path plus the same checklist fallback evidence because checklist creation timed out.
- Plan Review should reject the plan if Implementation would need to depend downstream clients on `botster-hub` internals instead of a narrow test-support surface.

## Vault Gaps Worth Capturing

- Capture after implementation if a durable convention emerges for external-client test harness crates: hub-side test-support should be a separate crate with explicit daemon binary/data-dir/socket lifecycle, not a full hub dependency.
- Capture after implementation if binary discovery for downstream Rust integration tests needs a repeatable Botster-wide pattern.
- No new capture is needed at Plan time for the checklist timeout; [[project pipelines checklist worker timeouts require artifact evidence fallback]] already covers the observed failure and fallback.

## Checklist Evidence Fallback

- Vault/context evidence: notes listed in `Context Loaded` were read and constrained the plan to Rust hub/client/session-worker boundaries, repo-visible plan artifacts, path-neutral references, and external-client protocol composition.
- Convention-conflict evidence: no conflict found after applying the orchestrator correction. The only pipeline conflict is the selected Hotwire step, which is treated as a workflow routing error rather than architecture authority for this Rust repo.
- Verification evidence gathered during planning: repository inspection confirmed `botster-hub-client` is already a separate protocol crate; existing `external_hub_client_crate_drives_real_daemon_socket_protocol` proves daemon requests over the real socket but still relies on in-repo helper code; `tests/support/mod.rs` contains extractable worker-binary build logic; `docs/client-protocol.md` documents the client protocol but not an isolated downstream test harness.
- Capture evidence: no immediate vault write; capture only after implementation resolves the reusable harness and downstream binary-discovery pattern.
