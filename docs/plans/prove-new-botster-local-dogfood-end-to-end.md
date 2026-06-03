# Prove New Botster Local Dogfood End To End

Ticket: `ticket_1780508733_890421`

## Context Loaded

- Project Pipelines context loaded with `project_pipelines_current_context` for run `run_1780529648_712632`, run step `run_step_1780529648_425621`, current step `botster_plan`, gate `botster_plan_gate`, ticket `Prove new Botster local dogfood end-to-end`.
- Pipeline state: no prior artifacts, findings, reviews, questions, or question answers.
- Closed dependencies loaded from run context:
  - `Add thin operator CLI for local dogfood`
  - `Implement local package provider loading from manifest paths`
  - `Provide local concrete capability runtimes for dogfood plugins`
  - `Wire local dogfood session workflows through hub API and core engine`
  - `Add local hub daemon runtime with explicit startup lifecycle`
- Required planner notes loaded:
  - [[planner-playbook]]
  - [[botster-planner-playbook]]
- Required Botster overlay notes loaded:
  - [[botster-architecture]]
  - [[cli-patterns]]
  - [[spa-patterns]]
  - [[project pipeline orchestration belongs in a device-level botster plugin]]
  - [[project pipelines needs an operator workbench not more primitives]]
  - [[project pipelines ui contract belongs in the plugin readme]]
  - [[botster orchestration should spawn agents with explicit target ids]]
  - [[botster orchestration prompts must bind agents to explicit worktrees]]
- Identity/goals context loaded from [[identity]] and [[goals]].
- Repo context inspected:
  - `README.md`
  - `Cargo.toml`
  - `test.sh`
  - `src/main.rs`
  - `src/runtime.rs`
  - `src/client_api.rs`
  - `src/packages.rs`
  - `src/persistence.rs`
  - `tests/hub_client_api_test.rs`
  - `tests/hub_daemon_lifecycle_test.rs`
  - `tests/hub_plugin_lifecycle_test.rs`
  - prior `docs/plans/*`

## Scope

In scope:

- Add one documented, reproducible local dogfood proof that runs without Rails, network, cloud, WebRTC, browser, TUI, or PII.
- Prove the full constrained workflow in one runtime/user path:
  - start an explicit local hub lifecycle with durable state,
  - inspect status through the operator CLI or local API,
  - install and enable a synthetic local package/provider from a manifest path,
  - observe package/provider state after reloading from `hub-state.json`,
  - spawn a local PTY session,
  - attach a local client,
  - send input,
  - drain output,
  - invoke or observe plugin lifecycle where the current scaffold supports it,
  - shut down the session/runtime cleanly.
- Keep the implementation production-shaped: use `HubDaemon`, `FileHubStateStore`, `PackageRegistry`, `HubRuntime`, `HubClientApi`, and `HubPluginLifecycle`/capability runtime facades instead of raw `DefaultBotsterEngine` calls or test-only shortcuts.
- Add a focused integration test or binary smoke flow that composes the existing pieces into one end-to-end proof. Existing separate tests are useful baseline coverage, but they do not by themselves satisfy the final dogfood acceptance.
- Update `README.md` to document the exact command/test flow and clearly state what is dogfood-ready versus still missing for feature parity.
- If adding a checked-in synthetic fixture, keep it minimal and local-only: manifest plus stub plugin/provider entrypoint, no secrets, no real host paths, no external URLs except `.invalid` test metadata if needed.

Non-scope:

- No Rails relay, ActionCable, WebRTC, browser SPA, TUI, cloud provider, OAuth/device-code flow, marketplace fetch, socket server, or cross-process long-lived daemon protocol.
- No new package/capability taxonomy in `botster-hub`; use `botster-core` package manifests, capability contracts, and existing hub policy.
- No broad refactor of `HubRuntime`, `HubClientApi`, package admission, lifecycle, persistence, or CLI parsing beyond what the proof needs.
- No attempt to make separately invoked session CLI commands see live runtime-only sessions. `README.md` already says that requires a future socket attach protocol.
- No speculative abstractions for future providers or plugin workflow policy.

Botster layers touched:

- Rust hub crate: primary.
- Thin operator CLI: likely touched for a single dogfood command or README-documented flow.
- Local package/provider policy and durable state: exercised, likely not structurally changed.
- Plugin lifecycle/capability runtime: exercised or minimally surfaced if the proof needs a status/invocation bridge.
- Docs: `README.md` and this plan.

## Assumptions And Unknowns

Assumptions:

- The assigned pipeline worktree is the implementation checkout for target `tgt_7e208a0c76a44980a83b63af976b1f22`; no additional agent spawn is needed for this Plan step.
- This ticket is not asking for a new architecture layer. It asks for a final proof that the existing dependency work can replace the current monolith for a constrained local workflow.
- The smallest acceptable proof can be an integration test command if it exercises the same production-shaped facades a user/operator would use. It does not need a real socket daemon until the ticket explicitly asks for cross-process live-session continuity.
- The phrase "CLI or local API" allows the proof to combine the CLI for status/package persistence with an in-process local API for live attach/input/drain, as long as the docs are explicit about the current in-process limitation.
- "Invoke or observe plugin lifecycle where available" can be satisfied by loading/invoking a synthetic runtime bundle in an integration proof or by showing lifecycle status through `HubClientApi::PluginLifecycleStatus`, provided the path uses the hub lifecycle adapter.
- Unix-only test coverage is acceptable for the PTY proof because existing session tests are already `#[cfg(unix)]`.

Unknowns:

- Whether the best user-facing proof should be a new `botster-hub dogfood`/`run-dogfood` CLI subcommand or a documented `cargo test --test ...` flow. Prefer the CLI only if it can reuse `HubClientApi` without duplicating integration-test harness logic or pretending to be a socket transport.
- Whether a checked-in `examples/synthetic-plugin` should be added. `README.md` currently references `examples/synthetic-plugin`, but no such path exists in the repo. Implementation should either add the fixture or change the docs to point at the real test-generated fixture/command.
- Whether provider lifecycle should be part of the same synthetic package fixture or only listed as a provider manifest/policy record. If a real provider runtime would exceed scope, document that provider policy is dogfood-ready while provider process lifecycle remains feature-parity work.

No human question blocks planning. If implementation cannot satisfy one of the ticket's verbs without waiving it, ask a human instead of silently narrowing the proof.

## Affected Surfaces / Files

Expected changes:

- `tests/hub_local_dogfood_test.rs` or an extension to an existing integration test:
  - Compose daemon startup, package install/enable, persisted reload, local API status/lifecycle/package reads, session spawn, attach, input, drain output, plugin lifecycle observation/invocation, and shutdown.
  - Assert sanitized output/state and deadline-bounded PTY observations.
- `src/main.rs`:
  - Optional: add one thin dogfood/smoke command only if needed to provide a better documented command flow than `cargo test`.
  - If touched, route through `HubDaemon`/`HubClientApi`/`HubRuntime`; do not call raw core routers.
- `README.md`:
  - Replace the current split/ephemeral CLI example with the accepted single documented proof.
  - Explicitly state dogfood-ready pieces and feature-parity gaps.
  - Remove or fix the nonexistent `examples/synthetic-plugin` reference.
- Optional fixture path such as `examples/synthetic-plugin/`:
  - Add only if the documented CLI flow needs a durable local package path.
  - Keep synthetic content minimal and non-sensitive.
- `src/lib.rs`, `src/runtime.rs`, `src/client_api.rs`, `src/packages.rs`, `src/lifecycle.rs`:
  - Touch only if the proof reveals a missing narrow public method needed to exercise an already implemented production path.

Likely unchanged:

- `Cargo.toml` and dependencies. Do not add crates.
- `src/auth.rs`, `src/config.rs`, `src/persistence.rs`, `src/profile.rs`, except for mechanical compile fixes if a narrow API change requires them.
- SPA/TUI/Rails files, because this ticket explicitly excludes those runtime dependencies.

## Risks

- Overclaiming risk: separate CLI invocations currently start fresh in-process daemons, so live session attach/list/inspect across commands is not supported. Docs and tests must not imply cross-process live session persistence.
- Unwired proof risk: a test that manually mutates registries or calls `DefaultBotsterEngine` directly would not prove the replacement stack. The proof must enter through hub-owned facades.
- Scope creep risk: adding a socket daemon, browser/TUI adapter, provider process supervisor, or cloud/WebRTC behavior would exceed the final local dogfood proof.
- Lifecycle ambiguity: package policy, plugin lifecycle, provider policy, and capability runtime are adjacent but distinct. The proof should name which one it proves and what remains.
- PTY timing risk: output chunking is nondeterministic. Tests should accumulate bytes until markers appear under a timeout.
- Cleanup risk: spawned sessions and capability timers/resources must be shut down even when assertions fail late; use explicit shutdown paths and short-lived synthetic commands.
- PII/path leakage risk: package provenance and CLI/test output can expose local absolute paths. Assertions should reject data-dir/package-dir path leakage in user-facing output.
- Stale dependency risk: this run was cut after closed dependencies, but implementation should still compile against the locked `Cargo.lock` revision before assuming APIs from prior plans.

## Acceptance Checks / Tests

Required verification:

- `cargo fmt`
- `cargo test --test hub_local_dogfood_test` if a new integration test is added, otherwise the updated targeted integration test command.
- `cargo test`
- `cargo clippy --all-targets --all-features -- -D warnings`
- The documented command/test flow from `README.md` must be run exactly as documented.

Targeted acceptance assertions:

- Starting with an empty explicit data directory initializes `hub-state.json` and reports scrubbed running/stopped status.
- Enabling a synthetic local package/provider from a manifest path persists package registry state.
- A second load from the same data directory reports `state_source=loaded` or equivalent and sees the enabled package/provider state.
- `HubClientApi::Status`, `ListPackages`, and `PluginLifecycleStatus` or equivalent local API reads return typed, sanitized state.
- A synthetic plugin lifecycle path is observed or invoked through `HubRuntime::load_plugin_package`/`invoke_plugin` or documented as unavailable with a human-approved waiver.
- A local PTY session is spawned through `HubClientApi::Spawn` or a thin CLI wrapper over it.
- A client attaches through `HubClientApi::Attach`.
- Input sent through `HubClientApi::Input` reaches the PTY and produces an observed marker.
- Output is drained through `HubClientApi::DrainRuntime` or the production `HubRuntime` drain facade and includes the expected marker.
- Shutdown goes through `HubClientApi::Shutdown` or the production `HubRuntime` shutdown facade and produces a typed lifecycle/cleanup observation.
- User-facing output and committed docs contain no local absolute paths, hostnames, secrets, tokens, or personal data.
- README says the current dogfood proof is local, in-process for live session continuity, file-backed for durable package/hub state, and not yet feature parity with cloud/Rails/WebRTC/browser/TUI.

Runtime path proof:

- The implementer must identify the exact production entry point used by the proof, for example `HubClientApi::handle_request` behind a documented test/CLI command.
- Evidence that each module has tests is not enough; the final proof must compose the modules in one end-to-end flow.

## Vault Gaps Worth Capturing

- Capture a note if implementation settles the local dogfood proof contract, especially the accepted split between durable cross-invocation package state and in-process live session continuity.
- Capture a note if the nonexistent `examples/synthetic-plugin` README reference reflects a recurring docs/fixture drift pattern.
- Capture a note if Project Pipelines checklist creation continues to time out; this run initially hit a plugin worker timeout and then relied on updating the created checklist plus gate evidence.
- No convention conflict found. The plan follows the loaded Botster notes: hub is a first-party host profile over core, product policy stays in plugin/hub layers, local clients use `HubRuntime`/`HubClientApi`, and runtime proof must exercise production-shaped boundaries.
