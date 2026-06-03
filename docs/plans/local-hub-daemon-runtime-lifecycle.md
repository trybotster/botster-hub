# Add Local Hub Daemon Runtime Lifecycle

Ticket: `ticket_1780508731_350400`
Run: `run_1780517595_147192`

## Context Loaded

- Project Pipelines context loaded for the returned Plan step: run `run_1780517595_147192`, current run step `run_step_1780518077_347000`, gate `botster_plan_gate`, ticket `ticket_1780508731_350400`, review `review_1780518056_753275`, and six open Plan Review findings.
- Required planner notes loaded: [[planner-playbook]] and [[botster-planner-playbook]].
- Botster overlay notes loaded: [[botster-architecture]], [[cli-patterns]], [[spa-patterns]], [[project pipeline orchestration belongs in a device-level botster plugin]], [[project pipelines needs an operator workbench not more primitives]], [[project pipelines ui contract belongs in the plugin readme]], [[botster orchestration should spawn agents with explicit target ids]], and [[botster orchestration prompts must bind agents to explicit worktrees]].
- Ticket-specific notes loaded: [[botster hub is a first party host profile over core]], [[botster packages should enforce core hub cli plugin provider boundaries]], [[botster package manifests and lockfiles should declare capabilities and provenance]], [[botster hub smoke cli entrypoints stay thin explicit and facade backed]], [[plan steps need reviewable plan artifacts]], [[test script required for rust tests not cargo test]], [[rust repo strict lints must be verified before dismissing warnings]], and [[project pipelines checklist worker timeouts require artifact evidence fallback]].
- Closed dependency reconciled: `ticket_1780508731_136973` merged PR #14 at `6a3563d195e76c5f9c0b4ce3c4ad32f5fca1c4b5`. `git fetch origin` updated `origin/main` from `79ff14d` to that merge commit. The dependency delivered concrete durable-state code and docs: `HubState`, `FileHubStateStore`, `HubStateStore`, `PackageRegistrySnapshot`, `HubRuntime::load`, `docs/adr/durable-hub-state-v1.md`, and `docs/plans/durable-hub-state-model-storage-boundary.md`.
- Current worktree note: before syncing, this branch is still based on pre-PR #14 `main`; implementation must first integrate `origin/main` so it extends the dependency deliverable rather than recreating it.
- Core serde check: locked `botster-core@6ae1c60` derives `Serialize`/`Deserialize` for `PackageManifest`, `PackageSource`, `ExtensionKind`, `Capability`, `CapabilitySurface`, and `HostProfileMetadata`; `AdmittedHostProfile` does not derive serde. The dependency already handled this by keeping `admitted_host_profile` runtime-derived and persisting hub-owned admission decision records.
- Repo files inspected from current branch and/or `origin/main`: `README.md`, `Cargo.toml`, `test.sh`, `src/lib.rs`, `src/config.rs`, `src/runtime.rs`, `src/lifecycle.rs`, `src/packages.rs`, `src/persistence.rs`, `src/main.rs`, `tests/hub_runtime_test.rs`, `tests/hub_plugin_lifecycle_test.rs`, `docs/adr/durable-hub-state-v1.md`, dependency implementation reports, and prior plan docs.
- Checklist discipline: the run checklist was eventually created as `checklist_1780517660_800683` after the first call timed out. This revised plan updates the checklist evidence and preserves the same evidence in gate payload.
- Baseline verification before the original plan: `./test.sh` passed with 25 lib tests, 5 plugin lifecycle integration tests, 2 hub runtime integration tests, and 1 doctest. Dependency verification on PR #14 reported `./test.sh`, strict clippy, `run-one --data-dir`, merge-tree proof, and PR merge.

## Current Repo Shape

After integrating `origin/main`, the baseline has a real durable state boundary:

- `src/persistence.rs` defines versioned `HubState` with root `schema_version: 1`, `FileHubStateStore`, `HubStateStore`, local JSON persistence at `<data_directory>/hub-state.json`, corrupt/unknown-version errors, and v1 single-writer/last-writer-wins posture.
- `src/packages.rs` includes `PackageRegistrySnapshot` and snapshot import/export support. Restore must re-run registry/admission behavior rather than trusting serialized runtime admission state.
- `src/runtime.rs` includes `HubRuntime::load(config)` and `HubRuntime::load_from_store(config, store)`, which load or initialize durable state and then construct `DefaultBotsterEngine` through public core APIs.
- `src/main.rs` on `origin/main` currently routes no-arg boot through `boot_runtime() -> build_default_config_for_runtime() -> HubRuntime::load(config)`. That is now a problem for this ticket: no-arg boot can resolve HOME/XDG and touch durable state. The new daemon lifecycle must not use this implicit path as its proof.
- `run-one --data-dir` remains a thin explicit smoke path over `HubRuntime::load`, spawn, attach, drain, and shutdown.

## Scope

- First integrate `origin/main` / PR #14 into this ticket branch so implementation extends the durable state boundary delivered by the dependency. Do not recreate a second state model.
- Add an explicit local daemon/runtime lifecycle object, likely `HubDaemon` or `LocalHubDaemon`, with deterministic `start`, `stop`, and `status`.
- Require explicit startup input for daemon persistence: `HubStartupOptions` with `DataDirectoryOption::Explicit`, an explicit `HubConfig`, or a thin binary command `start --data-dir <path>`. This is the production/user-path proof. It must not rely on no-arg HOME/XDG fallback.
- Add a thin `start --data-dir <path>` CLI entrypoint in `src/main.rs` that constructs the lifecycle, starts it, prints scrubbed deterministic status, and stops cleanly if the command is a bounded smoke-style entrypoint. Keep parsing dependency-free and facade-backed per [[botster hub smoke cli entrypoints stay thin explicit and facade backed]].
- Keep no-arg `botster-hub` as summary/config-only, or require explicit data-dir before it touches durable state. It must not silently load/save `hub-state.json` under HOME/XDG.
- Startup must load resolved config and durable `HubState`, construct `host_profile()` and `default_package_policy()`, restore enabled package/provider policy records from `HubState.package_registry`, and initialize core through `HubRuntime::load_from_store` / `HubRuntime::load` and `DefaultBotsterEngine`.
- Restoration should replay or validate through existing package registry/admission APIs where possible. Because `AdmittedHostProfile` is not serde-stable, enabled providers must have admission re-derived on restore.
- `status` should be a typed deterministic value, not prose parsing. It should report lifecycle state, host id/display name, schema version, data-dir configured flag, core initialized flag, package/provider counts, enabled counts, and whether the state file was loaded or initialized. Avoid local absolute paths in display output.
- `stop` should be explicit and idempotent. It should leave durable state consistent and release owned lifecycle state. If the daemon does not spawn sessions on startup, stop still records a clean stopped status; session shutdown remains core-owned if sessions are added by future lifecycle extensions.
- Update `src/lib.rs` exports and README/ADR text to document startup ownership and how later transports/providers attach to this lifecycle.
- Add focused tests for empty durable state startup, existing durable state startup, status, clean stop, explicit-data-dir CLI/user path, no-arg path not mutating durable state, and real core runtime proof.

## Non-Scope

- No cloud, Rails, ActionCable, WebRTC, marketplace browsing/fetching, remote package index, git clone/fetch, browser shell, SSO, or provider implementation behavior.
- No replacement of the PR #14 durable state boundary, no second storage format, and no broad rewrite of `HubState`, `FileHubStateStore`, or `PackageRegistrySnapshot`.
- No broad refactor of `HubRuntime` session methods, package admission internals, or core `DefaultBotsterEngine` mechanics.
- No new database, SQLite layer, background supervisor, signal handling, socket accept loop, daemonization/forking, PID files, or optional configuration matrix.
- No hub-owned PTY byte/data-plane behavior. Session I/O and client worker behavior stay in core.
- No user-specific paths, fingerprints, keys, environment dumps, or PII in docs, status output, persisted fixtures, or tests.

## Assumptions And Unknowns

- Assumption: the dependency deliverable is authoritative. Implementation should merge or otherwise integrate `origin/main` first and build on `HubState`, `HubStateStore`, `FileHubStateStore`, `PackageRegistrySnapshot`, and `HubRuntime::load`.
- Assumption: `start --data-dir <path>` is the required production/user path for this ticket. This removes the prior ambiguity and satisfies explicit data-directory/config acceptance.
- Assumption: no-arg boot summary should not perform durable-state load/save unless it is changed to require explicit startup input. The safest plan is to keep no-arg boot summary side-effect-light and use `start --data-dir` for lifecycle startup.
- Assumption: restored enabled packages/providers are policy records. This ticket should reconstruct registry state and lifecycle status; it should not load real Lua/process provider runtimes without supplied runtime bundles.
- Settled serialization fact: most relevant core package types are serde-capable, but `AdmittedHostProfile` is not. Do not persist it; re-run admission when reconstructing a live registry from snapshots.
- Settled schema decision: durable state already has `schema_version`. New lifecycle status/tests should assert schema version remains visible and supported.
- Unknown: exact public type names for daemon status/error can be chosen by the implementer, but they should be small and exported only where useful.
- Unknown: whether the `start --data-dir` command should run until interrupted or be a bounded lifecycle smoke command. Given the no signal-handling/supervisor non-scope, prefer bounded start/status/stop proof unless implementation can keep a long-running loop tiny and testable.

## Affected Surfaces / Files

- `src/main.rs`
  - Add explicit `start --data-dir <path>` dispatch.
  - Keep `run-one --data-dir` as the runtime smoke path.
  - Change no-arg boot so it does not implicitly persist under HOME/XDG, or gate persistence behind explicit input.
- `src/daemon.rs` or equivalent new narrow module
  - Define lifecycle object, lifecycle status, start/stop errors, and ownership of config/state/runtime/policy handles.
- `src/runtime.rs`
  - Reuse `HubRuntime::load` / `load_from_store`; add only minimal lifecycle-facing helpers if needed.
- `src/persistence.rs`
  - Reuse `HubState`, `schema_version`, and `FileHubStateStore`; avoid new model work unless tests need a tiny read-only status helper.
- `src/packages.rs`
  - Reuse `PackageRegistrySnapshot` and restore helpers. Add narrow count/filter helpers if lifecycle status needs deterministic package/provider/enabled counts.
- `src/lib.rs`
  - Export daemon/status types and any small helper types.
- `README.md` and possibly `docs/adr/durable-hub-state-v1.md`
  - Document startup ownership: explicit data-dir start owns durable lifecycle; no-arg summary does not mutate HOME/XDG state; future transports/providers attach after lifecycle start via existing runtime/package/provider seams.
- `tests/hub_daemon_lifecycle_test.rs` or equivalent
  - Add empty/existing state lifecycle tests and CLI/user-path proof.
- Existing `tests/hub_runtime_test.rs` and `tests/hub_plugin_lifecycle_test.rs`
  - Preserve existing coverage; add assertions only if public API wiring changes.
- Branch integration
  - Implementation must account for `origin/main` now being `6a3563d...` and may need a merge commit before feature work.

## Risks

- Stale baseline risk: this branch initially lacks PR #14. Implementing without integrating `origin/main` would recreate or diverge from the dependency boundary.
- Runtime-path risk: using no-arg boot as lifecycle proof would silently read/write under HOME/XDG. The plan forbids that; use explicit `start --data-dir`.
- Underwiring risk: adding a daemon type but not routing `src/main.rs start --data-dir` through it would fail the user-path proof.
- Thick-wrapper risk: daemon code could duplicate core session/runtime mechanics. Keep it to startup ordering, policy restoration, status, and stop ownership.
- Persistence drift risk: loading serialized package state directly could bypass registry/admission policy. Restore through existing snapshot/import/admission helpers and re-derive non-serde runtime admission data.
- Provider overreach risk: "providers" can tempt privileged provider execution. Keep this ticket to restored policy records and attachment points.
- PII risk: status/docs/tests can accidentally expose absolute data directories or operator audit reasons. Use scrubbed output and synthetic fixtures.
- Verification risk: direct `cargo test` misses the repo wrapper convention. Use `./test.sh` plus strict clippy.

## Acceptance Checks / Tests

Required commands after implementation:

- `cargo fmt`
- `./test.sh`
- `cargo clippy --all-targets --all-features -- -D warnings`
- `cargo run -- start --data-dir target/botster-hub-daemon-smoke-data` or equivalent explicit-data-dir lifecycle smoke command, with scrubbed output.
- `cargo run -- run-one --data-dir target/botster-hub-smoke-data -- /bin/sh -c "printf 'botster-hub-smoke-ok\n'"` remains passing.

Functional acceptance:

- Empty-state lifecycle test:
  - builds explicit config/data dir,
  - starts the daemon lifecycle,
  - loads or initializes `HubState` schema version 1,
  - reports `core_initialized == true`,
  - reports deterministic zero package/provider counts,
  - performs a real `spawn_session` plus `list_sessions` round-trip through the embedded runtime or otherwise exercises an equivalent public runtime operation,
  - stops cleanly and idempotently,
  - reloads state unchanged.
- Existing-state lifecycle test:
  - seeds `hub-state.json` through `FileHubStateStore`/`HubStateStore`, not ad hoc string writes,
  - includes enabled plugin and provider policy records if the existing fixtures can satisfy admission,
  - starts lifecycle from explicit data dir,
  - restores package/provider records through snapshot/import/admission logic,
  - reports package/provider and enabled counts,
  - stops and reloads without losing state.
- CLI/user-path proof:
  - `start --data-dir <path>` constructs and starts the lifecycle object.
  - no-arg `botster-hub` does not create or mutate a durable state file under HOME/XDG fallback.
  - output is scrubbed: no local paths, usernames, fingerprints, keys, tokens, or environment dumps.
- Docs:
  - README explains startup ownership and future transport/provider attachment points.
  - Docs state durable state schema version remains owned by PR #14's `HubState` boundary.
- Regression:
  - existing runtime and plugin lifecycle tests still pass.

## Vault Gaps Worth Capturing

- Capture if implementation settles the `start --data-dir` command as the durable local daemon entrypoint for Botster Hub.
- Capture if implementation establishes a reusable rule: no-arg hub smoke/summary commands must not touch durable state through HOME/XDG fallback.
- Capture if implementation clarifies how daemon lifecycle status should expose package/provider restored policy counts without leaking local paths.
- Existing dependency already captured durable-state v1 posture in repo docs; no new vault note is needed at plan time.
- Convention conflict from the original plan has been resolved: this plan cites [[botster hub smoke cli entrypoints stay thin explicit and facade backed]] and requires explicit data-dir lifecycle startup.
