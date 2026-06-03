# Wire Local Dogfood Session Workflows Through Hub API And Core Engine

Ticket: `ticket_1780508733_111676`

## Context Loaded

- Project Pipelines context loaded for run `run_1780527012_499364`, current step `botster_plan`, gate `botster_plan_gate`, ticket `Wire local dogfood session workflows through hub API and core engine`.
- Pipeline state had no prior artifacts, reviews, findings, questions, or question answers.
- Dependencies were already closed:
  - `Expose stable local client API over hub commands and events`
  - `Add local hub daemon runtime with explicit startup lifecycle`
- Required planning playbooks loaded:
  - [[planner-playbook]]
  - [[botster-planner-playbook]]
- Botster planning notes loaded:
  - [[botster-architecture]]
  - [[cli-patterns]]
  - [[spa-patterns]]
  - [[project pipeline orchestration belongs in a device-level botster plugin]]
  - [[project pipelines needs an operator workbench not more primitives]]
  - [[project pipelines ui contract belongs in the plugin readme]]
  - [[botster orchestration should spawn agents with explicit target ids]]
  - [[botster orchestration prompts must bind agents to explicit worktrees]]
- Relevant repo context inspected:
  - `README.md` defines `botster-hub` as a first-party host profile over `botster-core`, with client admission as hub policy and terminal bytes/session mechanics in core.
  - `src/runtime.rs` exports `HubRuntime` over `DefaultBotsterEngine` with spawn, attach, write, drain, inspect/activity, and shutdown methods.
  - `src/main.rs` has a `run-one` smoke path that already crosses `HubRuntime -> DefaultBotsterEngine` through spawn, attach, drain, marker observation, and shutdown.
  - `tests/hub_runtime_test.rs` proves a single fake client can spawn, attach, write, observe output, inspect activity, and shut down through the facade.
  - `Cargo.lock` currently resolves `botster-core` from git branch `main` at revision `6ae1c601ef6d9963a0dcd460257a24f5d3e0775c`.
- Workflow checklist note:
  - `project_pipelines_create_vault_checklist` timed out twice in the plugin worker. Per [[project pipelines checklist worker timeouts require artifact evidence fallback]], checklist evidence is preserved in this plan and gate evidence.

## Scope

In scope:

- Extend the stable hub local API/facade so the full dogfood workflow is named and usable through `HubRuntime`, not through raw `DefaultBotsterEngine` calls or test-only helpers.
- Cover the ticket's workflow verbs through hub-owned methods or a small hub-local workflow type: create/spawn a PTY session, list sessions, attach a local client, send input, resize, drain/output events, inspect activity/session state, detach a client when the core API supports it, and shut down.
- Preserve the existing boundary: hub owns host-profile policy and local API shape; `botster-core` owns process/session mechanics, terminal egress, fanout, activity accounting, and cleanup.
- Add integration coverage that exercises the public hub API path end to end with real local runtime behavior.
- Add or update a smoke harness only where it proves the production/operator path, not just that code compiles.
- Update README/docs to describe dogfood limitations and the exact supported local workflow surface.
- Keep committed artifacts free of PII, local absolute paths, hostnames, tokens, or environment dumps.

Non-scope:

- No Rails, TryBotster Cloud, WebRTC, browser shell, TUI UI, ActionCable, OAuth/device-code, provider package, marketplace, plugin workflow policy, or persistence database implementation.
- No direct use of core command routers from callers that are supposed to use the stable hub local API.
- No second data-plane implementation in the hub. Do not translate PTY runtime events manually when core already emits client egress.
- No broad refactor of config, package lifecycle, plugin lifecycle, auth, persistence, or profile modules unless a compile break from the targeted API change requires a small adjustment.
- No speculative configurability around runtimes, client transport kinds, reconnect policy, or retention policy.

Botster layers touched:

- Rust hub crate: primary.
- Rust `botster-core` default local runtime API: consumed, not modified.
- Session/client worker surface: verified through public core-backed egress, not reimplemented.
- CLI/smoke docs: only if needed to prove the dogfood workflow entry point.
- Docs: README and this plan.

## Assumptions And Unknowns

Assumptions:

- The pipeline workspace is the implementation checkout for this run, even though the ticket description names a canonical project path.
- The closed dependency for the stable local client API means implementation should build on the existing hub-facing API shape instead of inventing a parallel transport.
- This ticket is not scaffold-only. Acceptance requires a runtime/user-path proof that the local session workflow crosses the hub API and core engine.
- Existing `HubRuntime` methods are acceptable as the low-level facade, but the ticket likely needs missing verbs and stronger workflow-level tests: resize, multi-client output isolation, detach semantics, and shutdown cleanup.
- The test may remain Unix-only with `#[cfg(unix)]` because the local PTY smoke path uses shell commands.
- Deadline-bounded polling is the correct way to observe PTY output; exact frame chunking is not stable.
- If `botster-core` lacks a currently exported detach or resize method at the locked revision, implementation should either expose the available core method through `HubRuntime` or document the limitation and ask a human if that would waive part of acceptance.

Unknowns:

- Whether the locked `botster-core` API still exports `detach_client` and `resize` as described in the earlier runtime plan. `src/runtime.rs` does not currently wrap either method, so implementation must confirm the exact signatures before editing.
- Whether "drain/output events" should remain an imperative drain method for dogfood CLI smoke or become a named local-client event stream abstraction. Prefer the smallest hub API that maps to current core egress and tests the actual user path.
- Whether client attach/detach isolation can be proven entirely in `tests/hub_runtime_test.rs` with two fake `ClientId`/`SubscriptionId` pairs, or whether a binary smoke subcommand should be added for a closer operator path.
- Whether shutdown cleanup should assert core's retained session lifecycle state, a typed `SessionNotFound` live-handle result, or another locked-core cleanup signal. Use the core API's concrete behavior rather than guessing.

No human question blocks planning. Implementation should ask one if locked `botster-core` cannot satisfy detach or resize through any stable public API, because that would leave explicit ticket acceptance unmet.

## Affected Surfaces / Files

Expected changes:

- `src/runtime.rs`
  - Add missing hub facade methods for resize and detach if present in locked `botster-core`.
  - Consider a small, typed local-client workflow wrapper only if it reduces repeated request/ID plumbing without hiding policy-relevant inputs.
  - Keep method signatures close to core types and return `HubRuntimeOutput`/`HubRuntimeError`.
- `src/lib.rs`
  - Export any new public workflow/request/result types.
  - Add facade audit rows for newly exposed core operations such as `resize` and `detach_client`.
- `src/main.rs`
  - Extend or add a smoke subcommand only if it proves the dogfood flow through the production binary without turning the binary into a full client transport.
  - Preserve scrubbed output: IDs, marker, byte counts, lifecycle facts; no paths or environment dumps.
- `tests/hub_runtime_test.rs`
  - Add focused integration coverage for resize, output delivery to attached clients, attach/detach isolation, activity inspection, and shutdown cleanup.
  - Keep helpers local to the test unless they become public product API.
- `README.md`
  - Document the local dogfood session workflow, current limitations, and the supported smoke command.
- `docs/plans/wire-local-dogfood-session-workflows-through-hub-api-and-core-engine.md`
  - Keep this plan artifact current if Plan Review asks for clarification.

Likely unchanged:

- `src/config.rs`
- `src/packages.rs`
- `src/lifecycle.rs`
- `src/auth.rs`
- `src/persistence.rs`
- `src/profile.rs`
- `tests/hub_plugin_lifecycle_test.rs`
- `Cargo.toml`, unless the locked dependency API requires a deliberate `Cargo.lock` refresh. Do not add new crates for this ticket.

## Risks

- Locked core API mismatch: detach/resize may have changed since the earlier plan. Resolve from code/compiler before choosing a workaround.
- False-positive tests: tests that call raw `DefaultBotsterEngine` directly or use cfg(test) shortcuts would miss the ticket's hub API requirement.
- Data-plane duplication: implementing output fanout in hub would violate the core/session/client boundary and create a second runtime path.
- Multi-client isolation bugs: a single-client smoke path is already covered; this ticket needs at least one assertion that output delivery and detach behavior are scoped by client/subscription identity.
- PTY nondeterminism: output timing and chunking vary. Tests should search accumulated output within a deadline.
- Process leaks: tests must shut down spawned sessions even when adding multi-client or detach assertions.
- PII leakage: smoke output and docs must stay path-neutral and avoid user names, hostnames, environment values, or command transcripts with sensitive data.
- Scope creep: adding a real socket server, browser/TUI adapter, cloud provider, package policy, or workflow plugin behavior would exceed the smallest useful change.

## Acceptance Checks / Tests

Required commands after implementation:

- `cargo fmt`
- `cargo test --test hub_runtime_test`
- `cargo test`
- `cargo clippy --all-targets --all-features -- -D warnings`
- `cargo run -- run-one --data-dir target/botster-hub-smoke-data -- /bin/sh -c "printf 'botster-hub-smoke-ok\n'"`, or the updated dogfood smoke command if implementation adds a more complete one.

Baseline evidence during planning:

- `cargo test --test hub_runtime_test` passed: 2 tests.
- `cargo test` passed: 25 unit tests, 5 plugin lifecycle integration tests, 2 hub runtime integration tests, 1 doctest.
- `cargo clippy --all-targets --all-features -- -D warnings` passed.
- `./test.sh --unit` failed because this repo's `test.sh` forwards arguments directly to `cargo test`, and `--unit` is not a cargo test flag. Use `./test.sh` or targeted `cargo test` commands instead.

Targeted acceptance assertions:

- A local session can be spawned through the stable hub local API/facade and appears in `list_sessions`.
- A local client can attach through the hub API and receive core-backed terminal output.
- Input sent through the hub API reaches the PTY and produces observable output.
- Resize goes through the hub API into core if the locked core surface supports it.
- Two client/subscription identities do not receive or retain output incorrectly after attach/detach transitions.
- `inspect_session` or equivalent activity inspection reflects output activity through core.
- Shutdown goes through the hub API and proves process/session resources are no longer live using the core's typed cleanup signal.
- README/docs name dogfood limitations instead of implying browser/TUI/cloud support exists.
- The final diff has no PII and no unrelated refactors.

Runtime path proof:

- The production/library entry point must be `botster_hub::HubRuntime` or a thin public wrapper over it.
- If the smoke command is updated, it must call the same public hub API used by tests.
- Evidence that `botster-core` supports an operation is not enough; the hub crate must expose and exercise the operation through its stable local API.

## Vault Gaps Worth Capturing

- Capture a durable note if implementation confirms a standing local dogfood workflow contract for `botster-hub`, especially the mapping of hub API verbs to core engine methods and any documented detach/resize limitation.
- Capture a note if `project_pipelines_create_vault_checklist` continues timing out, because this run hit the documented checklist worker failure path twice and relied on gate/artifact fallback.
- No convention conflict was found during planning. The plan follows the loaded Botster boundary notes: hub policy/facade over core mechanics, stable local clients over `HubRuntime`, and real runtime tests rather than test-only shortcuts.
