---
description: Plan for making daemon-resolved foreground terminal launches inject absolute runtime paths independent of package working directory.
---

# Launch TUI with absolute runtime data and socket paths

## Target and context loaded

- Target repository: `botster-hub` (`trybotster/botster-hub`).
- Target ID: `tgt_7e208a0c76a44980a83b63af976b1f22`.
- Pipeline context: ticket `ticket_1784744558_183987`, run `run_1784744567_775614`, active step `botster_stack_plan`, and gate `botster_stack_plan_gate`. The run has no dependencies, prior artifacts, findings, reviews, questions, or answers.
- Repository routing proof: the admitted Hub spawn-target record maps the target ID to `trybotster/botster-hub`; the assigned worktree's `origin` is `https://github.com/trybotster/botster-hub.git` on `project-pipelines/ticket_1784744558_183987`.
- Repository charter loaded: [[botster-hub-playbook]].
- Role and surface playbooks loaded: [[planner-playbook]], [[botster-planner-playbook]], [[botster-runtime-reviewer-playbook]], and [[project-pipelines-playbook]] for workflow/gate discipline only. Project Pipelines package code is not in scope.
- Required maps and ownership notes loaded: [[botster-architecture]], [[cli-patterns]], [[spa-patterns]], [[project pipeline orchestration belongs in a device-level botster plugin]], [[project pipelines needs an operator workbench not more primitives]], [[project pipelines ui contract belongs in the plugin readme]], [[botster orchestration should spawn agents with explicit target ids]], [[botster orchestration prompts must bind agents to explicit worktrees]], [[botster pipeline needs continuous product owner between agent steps]], [[botster hub is a first party host profile over core]], [[botster hub gravity must be watched before it becomes the new monolith]], [[botster data plane bypasses the hub through session and client actors]], [[botster local client api lives over hubruntime not raw core routers]], and [[botster hub events use bounded priority lanes instead of unbounded queue fuses]].
- Targeted atomic notes loaded: [[apps cli uses exact selectors and daemon resolved terminal launch contracts]], [[botster runnable entrypoints are hub owned launch contracts]], [[botster runtime artifact resolution should be read only]], [[foreground terminal app open conformance belongs in hub test support]], [[manifest required injections must be consumed by the launched runtime]], [[hub socket discovery uses manifest-path injection not cwd resolution]], [[botster hub socket liveness requires a protocol handshake]], [[external client hub tests use subprocess spawned hub test support]], and [[pty integration tests that spawn botster start must be serialized to avoid socket-path races]].
- Repository evidence inspected: `src/daemon_transport.rs`, `src/packages.rs`, `src/main.rs`, `crates/botster-hub-test-support/src/lib.rs`, `tests/hub_daemon_lifecycle_test.rs`, `README.md`, `docs/client-protocol.md`, prior related plans under `docs/plans/`, and `test.sh`.

## Current production path and defect

`botster-hub open tui` reaches `operator_open_alias`, `open_app_by_selector`, and `open_terminal_app` in `src/main.rs`. `open_terminal_app` requests `DaemonRequest::ResolveAppLaunch`, then preserves the daemon-returned command, arguments, package-root working directory, environment, inherited stdio, and child exit status.

The daemon handler in `src/daemon_transport.rs` calls `resolve_app_launch_response`, which currently passes `config.data_directory` and `socket_path(&config)` unchanged to `resolve_foreground_launch_contract`. That package helper serializes the values into `BOTSTER_HUB_DATA_DIR` and `BOTSTER_HUB_SOCKET`. When the selected runtime directory is relative, `open_terminal_app` changes cwd to the package root before spawning, so the child resolves both values relative to the wrong checkout.

The same daemon module already has the required host-path rule: supervised entrypoints call `runtime_path` for both the selected data directory and socket before injection. Foreground resolution is the inconsistent call site.

## Scope

- In `resolve_app_launch_response`, resolve the selected hub data directory and socket through the existing `runtime_path` rule before passing them to `resolve_foreground_launch_contract`.
- Preserve the foreground contract's daemon ownership, package-root working directory, inherited stdio, command/argument resolution, manifest defaults, and exact `BOTSTER_HUB_DATA_DIR` / `BOTSTER_HUB_SOCKET` names.
- Preserve explicit `--data-dir` overrides: absolute paths remain unchanged; relative overrides become absolute against the daemon process cwd, which is the cwd that selected the runtime.
- Extend foreground terminal app conformance reporting and assertions so both injected runtime paths are explicitly proven absolute, not merely present.
- Add focused live integration coverage using a relative test-support root and a package-root cwd distinct from the hub checkout. Execute the daemon-returned launch contract and require the child to find the injected socket and complete a real `Status` request.
- Update the public foreground launch documentation to state that host-injected runtime paths are absolute and independent of the package working directory.

## Non-scope

- Do not change the canonical daily default, daily CLI parsing, app selectors, `open tui` alias behavior, daemon protocol DTO shape, package manifest schema, or working-directory policies.
- Do not add fallback socket discovery, cwd probing in the TUI, legacy environment names, dual relative/absolute behavior, versioned helpers, or new configuration.
- Do not move path policy into `botster-core`, `botster-hub-client`, `botster-tui`, package manifests, or client-side reconstruction.
- Do not canonicalize package roots or refactor unrelated launch, supervision, lifecycle, cleanup, or documentation surfaces.
- Do not modify Project Pipelines package/plugin code; its playbook applies only to this run's durable gate and checklist evidence.

## Repository ownership boundaries and dependencies

- `botster-hub` owns the selected runtime, daemon socket, package admission, and request-scoped foreground launch environment, so absolutization belongs at its daemon transport boundary.
- `src/packages.rs` remains the policy-neutral assembler of a supplied foreground launch contract. It should not inspect process cwd or rediscover host runtime state.
- `botster-hub-client` remains the DTO/request boundary; no protocol field changes are required because the fix changes values, not wire shape.
- `botster-tui` consumes the injected variables and remains intentionally unchanged. The Hub repository proves downstream behavior by launching a terminal-client fixture through `botster-hub-test-support` and making the child speak the real daemon protocol.
- `botster-core`, `botster-web`, and Project Pipelines own no part of this correction.
- No cross-repository prerequisite is currently required, so no dependency target should be registered. If implementation discovers that the real TUI ignores these canonical variables, stop and register a `botster-tui` dependency rather than broadening this run.

## Assumptions and unknowns

- Assumption: `runtime_path` is the intended single rule because the same module already uses it for supervised host-injected paths. Reusing it keeps foreground and supervised launch behavior consistent without a new abstraction.
- Assumption: absolute means `Path::is_absolute()` and runtime equivalence, not filesystem canonicalization. The selected data directory may not have existed when initially resolved, and canonicalization would add failure and symlink semantics the ticket does not request.
- Assumption: the daemon process cwd is authoritative for a relative configured runtime because daemon startup and daily commands already resolve that runtime from the invoking checkout.
- Assumption: `IsolatedHubBuilder` can expose the regression by receiving a relative `.root(...)`; its daemon and test process share the launch cwd, while the terminal child switches to its installed package root.
- Unknown: the cleanest focused test may be a new test beside `external_hub_test_support_drives_isolated_daemon_socket_protocol` or a narrowly extracted companion. Keep it separate from the broad conformance matrix so the relative-path regression has one obvious failure name.
- Unknown: whether the conformance report should add only `*_env_absolute` booleans or also retain parsed values internally. Prefer booleans to avoid expanding durable path exposure; no public DTO change is needed.
- No human question is needed: the ticket, current code, prior plan, and charter agree on path ownership, names, and required runtime proof.

## Affected surfaces and files

- `src/daemon_transport.rs`
  - Apply `runtime_path` to `config.data_directory` and the configured socket in `resolve_app_launch_response` before calling `resolve_foreground_launch_contract`.
  - Reuse the existing helper already used by `supervised_launch_environment`; do not introduce a second resolver.
- `crates/botster-hub-test-support/src/lib.rs`
  - Strengthen `run_foreground_terminal_app_open_conformance` and `ForegroundTerminalAppOpenConformanceReport` with explicit absolute-path observations for `BOTSTER_HUB_SOCKET` and `BOTSTER_HUB_DATA_DIR`.
  - Keep executing the returned command from the package root; retain filesystem checks and the real hello/status exchange over the injected socket.
- `tests/hub_daemon_lifecycle_test.rs`
  - Add a serialized focused integration test that starts an `IsolatedHub` beneath a relative root, runs foreground terminal app-open conformance, asserts the launch cwd differs from the hub checkout/runtime cwd, asserts both injected values are absolute, and verifies the child reports daemon lifecycle `running`.
  - Keep existing explicit absolute/isolated data-directory coverage intact.
- `docs/client-protocol.md`
  - Clarify that the two canonical foreground environment values are daemon-resolved absolute host paths and must not be reinterpreted relative to the package cwd.
- `README.md`
  - Clarify the foreground `apps open` / `open tui` contract at the existing launch documentation, without changing daily command guidance.
- `docs/plans/launch-tui-with-absolute-runtime-data-and-socket-paths.md`
  - This plan artifact only.

No change is expected in `src/main.rs`, `src/packages.rs`, `crates/botster-hub-client`, generated TypeScript, Cargo manifests, or the `botster-tui` repository.

## Implementation sequence

1. In the daemon foreground-resolution handler, derive absolute runtime data and socket paths with the existing `runtime_path` helper, then pass those values into the unchanged package launch-contract assembler.
2. Extend the shared foreground terminal conformance report with absolute-path observations while retaining its distinct package-root cwd, socket existence check, protocol handshake, and real `Status` request.
3. Add the focused relative-root live-hub regression and assert path absoluteness plus successful daemon status from the child. Keep the daemon test guard and deterministic shutdown/cleanup.
4. Update the two existing documentation passages to describe the absolute host-injection invariant.
5. Run focused tests, formatting/lints through the repository wrapper, then the full repository-prescribed verification.

## Risks

- Applying absolutization after switching to the package cwd would preserve the bug. It must occur in the daemon-owned resolution path before the launch contract crosses to the CLI.
- Moving cwd resolution into `src/packages.rs` would mix host process state into package contract assembly and could alter direct helper tests. Keep host-path policy in daemon transport.
- A test with an already absolute temp directory cannot catch this regression. The daemon must start from a deliberately relative root, while the child must execute from a different package root.
- Presence-only assertions are false confidence. The child must verify the socket exists and complete a real protocol status request, and Rust assertions must independently require both paths to be absolute.
- Canonicalizing instead of absolutizing could fail on not-yet-created paths or change symlink behavior. Reuse `runtime_path` exactly unless implementation evidence invalidates that assumption.
- Real-daemon tests can race on sockets or leak children. Use the existing daemon test guard and `IsolatedHub` shutdown/drop cleanup; do not run raw parallel subprocess tests outside the repository harness.
- Documentation could imply clients may still pass relative injected values. Update the contract language wherever foreground injection is described, but avoid broad unrelated docs cleanup.

## Acceptance checks and tests

- Focused regression, with its final name chosen during implementation:
  - `./test.sh --test hub_daemon_lifecycle_test <foreground_relative_runtime_path_test> -- --test-threads=1`
  - Start the real hub binary with a relative test-support root.
  - Install and resolve a `terminal_app` / `foreground_stdio` package through public daemon requests.
  - Launch the returned contract from the distinct package-root cwd.
  - Assert `BOTSTER_HUB_DATA_DIR` and `BOTSTER_HUB_SOCKET` are absolute.
  - Assert the injected socket exists and the child completes a real `Status` request returning lifecycle `running`.
- Existing shared conformance remains green:
  - `./test.sh --test hub_daemon_lifecycle_test external_hub_test_support_drives_isolated_daemon_socket_protocol -- --test-threads=1`
- Existing foreground resolver and daily no-flag production paths remain green:
  - `./test.sh --test hub_daemon_lifecycle_test daemon_resolves_terminal_app_foreground_launch_contract -- --test-threads=1`
  - `./test.sh --test hub_daemon_lifecycle_test cli_daily_commands_share_canonical_default_data_directory -- --test-threads=1`
- Test-support crate checks, including any report API assertions:
  - `./test.sh -p botster-hub-test-support`
- Formatting and strict repository verification:
  - `cargo fmt --check`
  - `./test.sh`
- Static review:
  - `rg -n "runtime_path|resolve_app_launch_response|BOTSTER_HUB_SOCKET|BOTSTER_HUB_DATA_DIR|run_foreground_terminal_app_open_conformance" src/daemon_transport.rs crates/botster-hub-test-support/src/lib.rs tests/hub_daemon_lifecycle_test.rs README.md docs/client-protocol.md`
  - Confirm foreground and supervised launch paths share the same runtime-path rule, no fallback branch was added, and `open_terminal_app` still executes the daemon-returned contract unchanged.

## Downstream runtime proof

Passing unit serialization tests is insufficient. Required evidence must trace the actual route:

`botster-hub open tui` / public conformance launcher -> `ListApps` -> `ResolveAppLaunch` -> daemon `resolve_app_launch_response` -> absolute host-injected values -> package-root child process -> injected Unix socket -> real daemon hello/status response.

The focused test intentionally uses a relative daemon configuration and a different child cwd, so it fails on the pre-fix behavior even though the socket exists beneath the Hub checkout.

## Pipeline gates, artifacts, and checklist evidence

- Plan artifact: this document.
- Project Pipelines workflow checklist: `checklist_1784744664_611803`.
- Vault checklist: `checklist_1784744659_508648`.
- Plan gate evidence must attach the repository/target routing, this artifact, explicit assumptions, ownership boundaries, affected files, runtime proof, and focused/full verification commands.
- Implement evidence must include the committed change, focused pre-fix-sensitive test output, full `./test.sh` output, and the production path trace above.
- Review must reject presence-only environment tests, client-side reconstruction, canonicalization without evidence, a second resolver, or code that leaves `open_terminal_app` unwired from the corrected daemon response.

## Vault gaps worth capturing

- Candidate durable convention after implementation: host-injected Botster runtime paths are absolute before crossing any package working-directory boundary. Existing notes establish daemon ownership and cwd-independent discovery, but none states this foreground/supervised launch invariant directly.
- Capture only if the implementation and verification confirm the rule applies generally to host-injected package launch paths; otherwise record no durable capture rather than restating this one ticket.
- No convention conflict was found. The surgical daemon-boundary fix follows [[apps cli uses exact selectors and daemon resolved terminal launch contracts]], [[foreground terminal app open conformance belongs in hub test support]], and the cold-turkey single-path rule.
