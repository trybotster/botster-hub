# Embed DefaultBotsterEngine In Hub Runtime Skeleton

Ticket: `ticket_1780276002_456394`

## Context Loaded

- Project Pipelines context loaded for run `run_1780281590_760433`, current step `botster_plan`, gate `botster_plan_gate`, ticket `Embed botster-core DefaultBotsterEngine in hub runtime skeleton`, and Plan Review findings from `review_1780281989_176714`.
- Run routing: proceed from `main`. The run has stale historical base fields in older events, but an orchestrator inbox message corrected this run to main-rooted with no stacked base run or ticket.
- Required Plan playbooks loaded:
  - [[planner-playbook]]
  - [[botster-planner-playbook]]
- Botster architecture notes loaded:
  - [[botster-architecture]]
  - [[cli-patterns]]
  - [[spa-patterns]]
  - [[project pipeline orchestration belongs in a device-level botster plugin]]
  - [[project pipelines needs an operator workbench not more primitives]]
  - [[project pipelines ui contract belongs in the plugin readme]]
  - [[botster orchestration should spawn agents with explicit target ids]]
  - [[botster orchestration prompts must bind agents to explicit worktrees]]
- Plan Review notes loaded:
  - [[plan steps need reviewable plan artifacts]]
  - [[botster dev harnesses must drive real engine types]]
  - [[botster-core local process runtime is feature-gated from contract-only embeds]]
  - [[botster data plane bypasses the hub through session and client actors]]
  - [[botster core lua owns plugin framework primitives not product policy]]
  - [[botster review and verify must scan all committed artifacts for pii]]
  - [[pty integration tests that spawn botster start must be serialized to avoid socket-path races]]
- Repo context inspected:
  - `Cargo.toml` depends on `botster-core` from git branch `main` with default features enabled and `Cargo.lock` pins commit `a6b4a7a92a09028c9abe239ba8aab2385d7f8835`.
  - `src/config.rs` already builds explicit `HubStartupOptions` and `HubConfig`, including `CoreEngineOptions` sourced from `botster-core` queue, coalescing, and plugin-worker defaults.
  - `src/core.rs` currently documents the core/hub boundary but constructs no engine.
  - `src/lib.rs` exports architecture/config facade types only.
  - `src/main.rs` only builds config and prints architecture summary.
  - `README.md` states this scaffold is intentionally shallow and excludes auth, cloud, WebRTC, Rails, TUI, provider, and client transport implementations.
  - Locked `botster-core` exports default-on `local-runtime` items: `DefaultBotsterEngine`, `DefaultBotsterEngineError`, `LocalProcessRuntime`, and `LocalProcessRuntimeOptions`.
  - Locked `botster-core::DefaultBotsterEngine` exposes `spawn_session`, `attach_client`, `detach_client`, `write_bytes`, `resize`, `drain_runtime_once`, `classify_activity`, and `shutdown_session`.
- Workflow checklist evidence:
  - Initial Plan checklist creation hit a Project Pipelines SQLite write lock, so evidence was preserved in gate payload.
  - Plan Review created run-level vault checklists and identified the prior lack of a repo-visible plan artifact as a blocker.

## Scope

In scope:

- Add a hub-owned runtime skeleton that is constructed from explicit `HubConfig` and owns `botster_core::DefaultBotsterEngine`.
- Keep the `botster-core` dependency on default features. Do not set `default-features = false`; this ticket requires the default-on `local-runtime` feature.
- Name and use the feature-gated core surface explicitly: `DefaultBotsterEngine`, `DefaultBotsterEngineError`, `LocalProcessRuntime`, and `LocalProcessRuntimeOptions`.
- Add a narrow hub runtime facade, likely `HubRuntime`, that owns hub policy inputs while delegating spawn, attach, write, read/drain, activity classification, and shutdown to `DefaultBotsterEngine`.
- Keep hub policy in `src/config.rs` and the hub boundary module. The hub may translate validated config defaults into explicit core spawn requests, but it must not own PTY byte flow or reimplement runtime-event translation.
- Exercise the real core engine boundary. Runtime-originated output should pass through core engine behavior such as `DefaultBotsterEngine::drain_runtime_once`, which uses the managed session worker and subscription path behind the facade, rather than a hub helper that remaps runtime output into session events.
- Add a real runtime integration test at `tests/hub_runtime_test.rs` that constructs the hub runtime with explicit config, spawns a disposable local command through core, attaches a fake client/subscription, writes input, deterministically observes output, classifies activity, and shuts down.
- Keep all committed docs, tests, examples, metadata, and output free of PII and local absolute user paths.
- Optionally update `src/main.rs` with a minimal smoke path that constructs the runtime from config, if doing so stays small and improves the production entry-point proof.
- Optionally update `README.md` to mention the runtime skeleton boundary if the public API would otherwise be hard to discover.

Non-scope:

- No auth, cloud, WebRTC, Rails, TUI, React SPA, ActionCable, provider runtime, package marketplace, plugin execution, persistent database, socket server, or client transport implementation.
- No executable discovery, target admission, provider policy, user identity, environment inheritance policy, reconnect policy, retention policy, or presentation policy.
- No custom data-plane loop in the hub. PTY bytes and terminal egress remain core/session/client-engine behavior, not hub policy behavior.
- No broad refactor of the existing config, adapter, provider, auth, package, or persistence seams.
- No speculative abstraction over multiple core engines. Use `DefaultBotsterEngine` unless the locked API forces a small equivalent facade.
- No stacked branch or stacked PR behavior for this run.

Botster layers touched:

- Rust hub crate: primary.
- Rust `botster-core` default local runtime API: embedded and exercised, not changed.
- Rust integration tests: real local process/runtime test.
- Docs: this plan artifact and optional README wording.

## Assumptions And Unknowns

Assumptions:

- This is not scaffold-only. The ticket requires a wired runtime path that proves the hub can drive core spawn/attach/write/read/classify/shutdown behavior.
- `botster-core` at locked commit `a6b4a7a92a09028c9abe239ba8aab2385d7f8835` has the public API needed for the implementation.
- `Cargo.lock` is the reproducibility anchor for the branch because `Cargo.toml` still points at git branch `main`.
- The implementation should keep `botster-core` default features enabled so `DefaultBotsterEngine` and `LocalProcessRuntime` remain available.
- The disposable command can be Unix-focused if the integration test is `#[cfg(unix)]`. Prefer an explicit command such as `sh -c "printf 'ready\n'; while IFS= read -r line; do printf 'echo:%s\n' \"$line\"; done"`.
- The fake client/transport adapter in acceptance is represented by deterministic `ClientId` and `SubscriptionId` values attached through `DefaultBotsterEngine::attach_client`; no concrete browser/TUI/socket adapter is needed.
- Real output observation should poll/drain until expected substrings appear within a short deadline, not rely on fixed sleeps or exact PTY chunking.
- The test does not boot `botster start` or bind shared socket paths, so the socket-race serialization note is a risk to check but likely does not require a global lock. If implementation adds any real hub process/socket path, serialize that test.

Unknowns:

- Whether the cleanest module name is `src/runtime.rs` or an extension to `src/core.rs`. Prefer `src/runtime.rs` if it avoids mixing boundary docs with executable runtime code.
- Whether `LocalProcessRuntimeOptions` needs explicit construction in this crate. If `DefaultBotsterEngine::new()` is sufficient and uses default local options, still name the feature-gated type in docs/tests and avoid disabling the feature. If custom runtime options are available and useful, prefer `LocalProcessRuntimeOptions` only for explicitness, not configurability sprawl.
- Whether `src/main.rs` should run a command smoke path. The minimum production/library proof is a public `botster_hub::HubRuntime` path plus integration test; a binary smoke path is useful only if it stays small and non-flaky.
- Whether the integration test needs a small local drain helper. If so, it should only repeat polling/collection logic around `DefaultBotsterEngine::drain_runtime_once`; it must not translate runtime events itself.

No human question is blocking implementation.

## Affected Surfaces / Files

Expected changes:

- `src/runtime.rs`
  - Define `HubRuntime` or equivalent.
  - Store `HubConfig` and `DefaultBotsterEngine`.
  - Provide narrow methods for spawn, attach, write, drain/read, classify, and shutdown.
  - Keep byte flow delegated to `DefaultBotsterEngine`.
- `src/lib.rs`
  - Export the runtime module and public runtime facade/error/observation types.
- `src/core.rs`
  - Keep or lightly update boundary docs so they point at the runtime skeleton without moving policy into core.
- `src/config.rs`
  - No broad changes expected. Add only small conversion/accessor helpers if needed to build explicit core spawn requests from `SessionDefaults`.
- `tests/hub_runtime_test.rs`
  - Add the ticket acceptance integration test.
- `src/main.rs`
  - Optional: construct `HubRuntime` after config build and report runtime readiness. Avoid spawning a process here unless the smoke path remains deterministic and non-invasive.
- `README.md`
  - Optional: mention that the scaffold now includes a minimal local runtime skeleton over `DefaultBotsterEngine`.
- `Cargo.toml`
  - Keep `botster-core` default features enabled. Add no dependency unless implementation proves one is required.

Not expected:

- `src/adapters/*`
- `src/auth.rs`
- `src/providers.rs`
- `src/packages.rs`
- `src/persistence.rs`
- Any Rails, WebRTC, TUI, React, Lua plugin, MCP, or cloud/provider files.

## Implementation Shape

Suggested minimal API:

```rust
pub struct HubRuntime {
    config: HubConfig,
    engine: botster_core::DefaultBotsterEngine,
}

impl HubRuntime {
    pub fn new(config: HubConfig) -> Self;
    pub fn config(&self) -> &HubConfig;
    pub fn engine(&self) -> &botster_core::DefaultBotsterEngine;
    pub fn spawn_session(...explicit inputs...) -> Result<..., botster_core::DefaultBotsterEngineError>;
    pub fn attach_client(...ids...) -> Result<..., botster_core::DefaultBotsterEngineError>;
    pub fn write_bytes(...ids/data...) -> Result<..., botster_core::DefaultBotsterEngineError>;
    pub fn drain_runtime_once(...) -> Result<..., botster_core::DefaultBotsterEngineError>;
    pub fn classify_activity(...) -> Result<..., botster_core::DefaultBotsterEngineError>;
    pub fn shutdown_session(...) -> Result<..., botster_core::DefaultBotsterEngineError>;
}
```

The concrete method signatures can mirror `DefaultBotsterEngine` closely. The hub wrapper should remove only the friction this repo owns, such as applying validated `SessionDefaults` when constructing a `SessionSpawnRequest`. It should not hide the command, working directory, environment, session id, request id, client id, or subscription id behind product policy.

Suggested integration test flow in `tests/hub_runtime_test.rs`:

1. Build explicit `HubStartupOptions` with `DataDirectoryOption::Explicit("target/botster-hub-test-data/runtime")`, deterministic host identity, deterministic session defaults, and local socket disabled if the runtime does not need it.
2. Build `HubConfig` from injected `RuntimeEnvironment`.
3. Construct `HubRuntime::new(config)`.
4. Build a `SessionSpawnRequest` for an explicit disposable command:
   - executable: `sh`
   - arguments: `["-c", "printf 'ready\\n'; while IFS= read -r line; do printf 'echo:%s\\n' \"$line\"; done"]`
   - working directory: generic repo-relative/current test directory value that is not committed as an absolute user path
   - initial PTY size: deterministic rows/cols
   - empty or explicit deterministic environment
5. Call the hub runtime spawn method and assert the spawned `SessionId`.
6. Attach a fake client represented by deterministic `ClientId` and `SubscriptionId` through the runtime, which delegates to `DefaultBotsterEngine::attach_client`.
7. Drain through the runtime/core until `ready` is observed in client egress.
8. Call runtime write with `ping-hub\n`.
9. Drain through the runtime/core until `echo:ping-hub` is observed.
10. Call runtime classify and assert `SessionActivityStatus::Active` after output.
11. Call runtime shutdown and assert shutdown/stopped observation or absence of live session according to the core API.

The drain helper may collect `BotsterEngineOutput` frames and search terminal-output bytes. It must not convert `SessionRuntimeOutput` into `SessionIoEvent` or otherwise bypass `SessionWorkerEngine` and `SubscriptionMultiplexer`.

## Risks

- `botster-core` dependency drift: `Cargo.toml` points at branch `main`; this ticket should rely on the existing `Cargo.lock` pin and avoid lockfile churn unless necessary.
- Feature-gating drift: disabling default features would remove `DefaultBotsterEngine`, `DefaultBotsterEngineError`, `LocalProcessRuntime`, and `LocalProcessRuntimeOptions`.
- Cross-platform command availability: `sh` is a Unix assumption. Gate the integration test with `#[cfg(unix)]` if needed rather than pretending it is portable.
- PTY output nondeterminism: output may be chunked or delayed. Use deadline-bounded polling/draining for substrings, not exact chunk equality or sleep-only assertions.
- Process cleanup: a shell read loop can leak if shutdown is skipped after a successful spawn. Keep the test linear and call shutdown explicitly; if the test grows, add cleanup on failure paths.
- Data-plane boundary regression: a hub helper that translates runtime output itself would create a second implementation of production core behavior. Keep runtime bytes on the core engine path.
- Policy leakage: adding auth, admission, executable discovery, environment inheritance, cloud/provider, or presentation decisions would violate the ticket.
- PII leakage: committed plan/test/docs must not include local absolute worktree paths, usernames, hostnames, or sensitive values. Review should scan the whole branch diff, including this plan.
- Socket/path races: this test should not boot `botster start` or bind deterministic hub sockets. If implementation introduces a real hub process or socket binding, serialize the integration test.

## Acceptance Checks / Tests

Required checks after implementation:

- `cargo fmt`
- `./test.sh`
- `cargo test --test hub_runtime_test`
- `cargo test`
- `cargo clippy --all-targets --all-features -- -D warnings`

Targeted acceptance assertions:

- `tests/hub_runtime_test.rs` constructs the runtime from explicit `HubStartupOptions`/`HubConfig`.
- `HubRuntime` owns `botster_core::DefaultBotsterEngine`.
- The `botster-core` dependency keeps default features enabled.
- The test spawns a disposable local command through `DefaultBotsterEngine`, not a fake session runtime.
- The test attaches a fake client/subscription through the runtime/core boundary.
- The test observes real command output through core-drained client egress.
- The test writes input through the runtime/core boundary and observes deterministic echoed output.
- The test classifies activity through core and observes `SessionActivityStatus::Active`.
- The test shuts down through core and asserts a clean shutdown signal/state.
- The branch diff contains no auth/cloud/WebRTC/Rails/TUI/provider implementation and no PII.

Runtime path proof:

- The changed production/library path must be `botster_hub::HubRuntime` or equivalent exported facade.
- Evidence that `botster-core` already provides `DefaultBotsterEngine` is insufficient. The hub crate must construct and use it.
- If `src/main.rs` is updated, it should prove runtime construction from config. The integration test remains the proof for spawn/attach/write/read/classify/shutdown.

## Vault Gaps Worth Capturing

No durable vault gap is required before implementation. Existing notes already cover:

- reviewable plan artifacts,
- default-on local-runtime feature gating,
- real engine boundary testing,
- core-vs-hub policy separation,
- data-plane ownership,
- PII scanning for committed artifacts.

Potential capture after implementation:

- If `HubRuntime` becomes the standing embedding pattern, capture a Botster note that `botster-hub` wraps `DefaultBotsterEngine` from explicit `HubConfig`, while local process mechanics and terminal egress remain in `botster-core`.
