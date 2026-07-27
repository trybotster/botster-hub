---
description: Plan for making every stateful Hub CLI command resolve one optional data-directory policy with $HOME/.botster/hub as the canonical default.
---

# Use `$HOME/.botster/hub` as the shared optional Hub data directory

## Target repository

- Repository: `trybotster/botster-hub`
- Target ID: `tgt_7e208a0c76a44980a83b63af976b1f22`
- Assigned worktree: the Project Pipelines worktree for ticket `ticket_1785028327_620941`
- Ownership charter: [[botster-hub-playbook]]

## Context loaded

- Pipeline context: run `run_1785028467_149855`, Plan step `botster_stack_plan`,
  gate `botster_stack_plan_gate`, ticket description and acceptance, open
  findings/reviews/dependencies (none), and the answered precedence question
  `question_1785028653_878753`.
- Role and surface playbooks: [[planner-playbook]],
  [[botster-planner-playbook]], [[botster-hub-playbook]],
  [[botster-architecture]], [[cli-patterns]], and [[spa-patterns]].
- Required Botster notes:
  [[project pipeline orchestration belongs in a device-level botster plugin]],
  [[project pipelines needs an operator workbench not more primitives]],
  [[project pipelines ui contract belongs in the plugin readme]],
  [[botster orchestration should spawn agents with explicit target ids]],
  [[botster orchestration prompts must bind agents to explicit worktrees]],
  [[botster pipeline needs continuous product owner between agent steps]],
  [[plan agents must author vault context as wikilinks not home paths]], and
  [[vault example paths are not repository placement conventions]].
- Hub ownership and task notes:
  [[botster hub is a first party host profile over core]],
  [[botster hub gravity must be watched before it becomes the new monolith]],
  [[botster data plane bypasses the hub through session and client actors]],
  [[botster local client api lives over hubruntime not raw core routers]],
  [[botster hub events use bounded priority lanes instead of unbounded queue fuses]],
  [[may supervise permits the hub to supervise the package entrypoint]],
  [[hub supervision admission changes require exact live hub launch proof]],
  [[webrtc bootstrap origin must be requested after the package server binds]],
  [[botster hub daemon startup requires explicit data dir]],
  [[botster hub no arg summary must not touch durable home state]],
  [[botster hub smoke cli entrypoints stay thin explicit and facade backed]],
  [[botster hub daemon status should be typed and path neutral]],
  [[botster runtime artifact resolution should be read only]],
  [[botster host injected runtime paths are absolute before package cwd boundaries]],
  [[external client hub tests use subprocess spawned hub test support]],
  [[dogfood generated data dirs use short tmp paths for unix sockets]],
  [[botster plugin runtime data must not live in the plugin source tree]], and
  [[cold turkey migrations eliminate dual code paths and version suffixes]].
- Repository evidence: `README.md`, `src/main.rs`, `src/config.rs`,
  `tests/hub_daemon_lifecycle_test.rs`, `docs/client-protocol.md`,
  `docs/loaded-daemon-lifecycle-runner.md`,
  `script/test-production-package-runtime`,
  `.github/workflows/loaded-daemon-lifecycle.yml`, `test.sh`, and the prior narrower plan
  `docs/plans/use-canonical-local-runtime-data-directory-across-daily-hub-commands.md`.
- Project Pipelines workflow-policy or package code is not in scope, so
  [[project-pipelines-playbook]] was intentionally not loaded as a repository
  overlay.

## Binding product decision and precedence

The answered human question establishes one policy:

1. Explicit `--data-dir <path>`.
2. Explicit `BOTSTER_HUB_DATA_DIR`.
3. `$HOME/.botster/hub`.

Remove `XDG_DATA_HOME` from Hub CLI runtime selection. With neither explicit
override present, every stateful command must resolve exactly
`$HOME/.botster/hub`, regardless of current working directory. The existing
Botster device/configuration siblings under `$HOME/.botster` are not Hub package
or plugin roots and must not be scanned or loaded.

This ticket supersedes the older explicit-only daemon-startup convention for
stateful CLI commands. It does not require the no-argument host-profile summary
to become stateful. The change is cold turkey: no `target/` fallback, XDG
fallback, migration, alias resolver, legacy sibling scan, or compatibility
branch remains.

## Scope

- Introduce one Hub-owned data-directory resolver used by both CLI parsing and
  `HubStartupOptions::RuntimeDefault`.
- Replace `DataDirOptions` / `DailyDataDirOptions` and command-specific
  defaulting with one optional `--data-dir` extraction path that preserves
  command operands and applies the binding precedence once.
- Route every stateful Hub command through it: `up`, `down`, `start`,
  `shutdown`, `status`, `doctor`, `smoke`, `open`, `reload`, `mcp-serve`,
  packages/providers, apps, sessions, session templates, spawn targets,
  worktrees, context, inspect, and related daemon-backed commands.
- Keep `--data-dir` ordering consistent. Prefer one parser that extracts the
  option from the command argument vector and returns the remaining operands,
  instead of fixed slices such as `args[1..3]`; reject duplicates, missing
  values, and unexpected operands with the existing command-specific usage.
- Preserve explicit relative-path semantics already required by package launch:
  resolve host-injected runtime paths to absolute paths before crossing a child
  `cwd` boundary.
- Update help, remediation text, ready output, README/operator examples, and
  protocol/operator docs to describe the single precedence model and optional
  flag.
- Add focused resolver tests and production-shaped compiled-binary tests with
  isolated `HOME` and scrubbed `BOTSTER_HUB_DATA_DIR`.

## Non-scope

- No migration or import from `target/botster-hub-runtime-data`,
  `$XDG_DATA_HOME/botster-hub`, or legacy/current-monolith `$HOME/.botster`
  contents.
- No compatibility aliases, dual parsers, versioned resolver names, or fallback
  probing.
- No changes to daemon protocol DTOs, `botster-hub-client`, core runtime
  contracts, session-worker ownership, package manifest schema, plugin workflow
  policy, Web/TUI clients, Rails/cloud, or marketplace behavior.
- No scanning of `$HOME/.botster/{plugins,agents,lua,profiles,shared,workspaces}`.
- No broad `src/main.rs` cleanup beyond parser/default/remediation code made
  obsolete by this ticket.
- `run-one` remains an explicit-data-dir scrubbed smoke path. It must not
  default to the operator's shared runtime root because it writes one-shot
  diagnostic state; the living contract is documented in `README.md`.
- No mutation of the developer's real home directory in tests or manual proof.
- Mechanical updates required by removing the public
  `RuntimeEnvironment::from_values` XDG parameter are in scope even outside
  `src/config.rs`; retaining a placeholder argument or deprecated
  three-argument shim is not.

## Repository ownership and cross-repository dependencies

- `botster-hub` owns CLI parsing, host-profile configuration, persistence roots,
  daemon/socket selection, package/app/session operator commands, MCP startup,
  documentation, and live-Hub tests. All required implementation belongs here.
- `botster-core` remains the policy-free runtime and session data-plane owner;
  no core contract change is required.
- `botster-hub-client`, `botster-web`, `botster-tui`, first-party plugins, and
  Project Pipelines consume the selected Hub through existing socket/env
  contracts. They require downstream proof, not source changes.
- No cross-repository prerequisite is currently identified. If implementation
  discovers a consumer that hard-requires `--data-dir`, register a dependency
  ticket against that repository's target rather than broadening this run.

## Assumptions and unknowns

- Assumption: `HOME` is required only when neither CLI nor
  `BOTSTER_HUB_DATA_DIR` supplies a path. Missing/empty `HOME` in that case is a
  typed configuration error, not a cwd fallback.
- Assumption: `BOTSTER_HUB_DATA_DIR` remains intentional automation/test
  configuration and has the same validation and path semantics as today.
- Assumption: the no-argument `botster-hub` host-profile summary remains
  side-effect-light; it may resolve config for display but must not create or
  load durable state.
- Assumption: all named commands should accept the same option position policy.
  The implementation should preserve the documented operand order while
  avoiding command-specific fixed slices.
- Assumption: explicit temp `--data-dir` remains the default for tests that
  exercise many sockets, both for isolation and Unix socket path length.
- Unknown to resolve during implementation: the smallest internal parser shape
  may be a resolved options value plus remaining args, or a helper operating on
  slices. It must remain private, dependency-free, and singular.
- Unknown to verify, not assume: whether every remediation/ready-output string
  should omit `--data-dir` for the canonical path. Output should teach the
  no-flag path without losing an explicit override when one was selected.

## Affected surfaces and files

- `src/config.rs`
  - Make `RuntimeEnvironment` and `DataDirectoryOption::RuntimeDefault` implement
    the binding precedence.
  - Remove `XDG_DATA_HOME` state, validation, error wording, and tests.
  - Change the public `RuntimeEnvironment::from_values` signature instead of
    retaining a vestigial XDG parameter or compatibility shim.
  - Add unit coverage for CLI-equivalent explicit selection, environment
    override, canonical home default, missing inputs, and sibling isolation.
- Mechanical `RuntimeEnvironment::from_values` call-site updates in
  `src/main.rs`, `src/persistence.rs`, `src/runtime.rs`, `src/packages.rs`, and
  tests under `hub_runtime`, `hub_capability_runtime`, `hub_mcp`,
  `hub_lua_runtime`, `hub_daemon_lifecycle`, `hub_plugin_lifecycle`,
  `hub_local_runtime`, and `hub_client_api`. These edits remove the obsolete
  positional XDG argument only and are cleanup made necessary by this change.
- `src/main.rs`
  - Replace `DataDirOptions`, `DailyDataDirOptions`, `StartOptions`,
    `LocalRuntimeOptions`, `SmokeOptions`, and
    `default_local_runtime_data_dir()` with the shared optional resolver or
    thin command-specific operand parsers that receive its result.
  - After the change, no parser type may retain a private data-directory
    default or mandatory `--data-dir` arity check.
  - Wire every stateful dispatch/parser to the resolved path and remove
    fixed-position duplicate parsing.
  - Update internally spawned `start`, `reload`, MCP, remediation, ready output,
    global help, and `usage_for` strings.
  - Keep `boot_summary()` non-mutating.
- `tests/hub_daemon_lifecycle_test.rs`
  - Extend compiled-binary and live-daemon coverage across lifecycle,
    package/provider/app, session/template, target/worktree/context, reload, and
    MCP representatives.
  - Isolate `HOME`, remove inherited `BOTSTER_HUB_DATA_DIR`/`XDG_DATA_HOME`
    unless the test is proving their behavior, and assert the real home remains
    untouched.
  - Retain explicit temporary-root coverage and child-cwd absolutization proof.
- `README.md`
  - Replace the checkout-relative/explicit-only topology and examples with the
    canonical home root and optional override.
  - Document precedence once and state that sibling legacy directories are
    neither loaded nor migrated.
- `docs/client-protocol.md`, `docs/loaded-daemon-lifecycle-runner.md`, and any
  current operator/remediation documentation found by the final stale-string
  audit.
- `script/test-production-package-runtime`
  - Replace its operator-root policy with
    `BOTSTER_HUB_DATA_DIR` then `$HOME/.botster/hub`; the script supplies no CLI
    override for the ambient operator-root collision check.
  - Repoint the fixture-socket collision guard at the new operator default
    socket.
  - Snapshot the resolved operator Hub root and the sibling
    `$HOME/.botster/{plugins,agents,lua,profiles,shared,workspaces}` paths before
    and after the production run.
  - Retain `target/botster-hub-runtime-data` only as negative proof that the
    removed cwd-relative root is not created or mutated; do not treat it as a
    selectable runtime.
- Existing historical plans/reports remain historical; do not rewrite them.

## Implementation sequence

1. Add the single resolver in `src/config.rs` with the answered precedence and
   focused unit tests. Delete XDG selection and update every
   `RuntimeEnvironment::from_values` caller in the same change.
2. Replace both CLI option types with one optional extractor/resolver in
   `src/main.rs`; explicitly eliminate private data-directory behavior from
   `StartOptions`, `LocalRuntimeOptions`, and `SmokeOptions`, then migrate
   stateful commands by command family while preserving their existing
   daemon/runtime calls.
3. Remove `default_local_runtime_data_dir()` and all cwd-relative state
   construction. Update child-process forwarding to pass the already-resolved
   path explicitly.
4. Update help, usage, remediation, and ready output in lockstep with parsing.
5. Update `script/test-production-package-runtime` so its collision and
   before/after filesystem assertions observe the new real operator root,
   protected `.botster` siblings, and removed `target/` root.
6. Add HOME-isolated compiled-binary coverage proving one live daemon is reached
   by representative commands from different working directories, plus
   explicit CLI and environment override isolation.
7. Update current README/operator/protocol documentation and run stale policy
   scans.
8. Run focused tests, strict Rust gates, the full repo wrapper, the repository
   production runtime script, and a production-shaped CLI acceptance with a
   disposable HOME.

## Risks

- Parser underwiring: changing a helper without migrating one command family
  leaves a silent required flag or a second state root.
- Option-order regression: fixed argument slices are widespread; careless
  replacement can consume command operands or permit duplicates.
- Real-home mutation: subprocess tests inherit environment by default. Every
  no-flag test must set disposable `HOME` and deliberately clear or set
  `BOTSTER_HUB_DATA_DIR`.
- Legacy sibling ingestion: joining `.botster` instead of `.botster/hub` would
  expose current device configuration as package/plugin state.
- XDG drift: leaving `XDG_DATA_HOME` in `RuntimeEnvironment`, docs, or tests
  preserves a competing implicit root.
- Spawned-child drift: `up`, reload, package apps, and MCP may construct child
  commands; they must forward the selected absolute meaning rather than
  re-resolve under another cwd/environment.
- Durable daemon-owner metadata: moving
  `.botster-hub-runtime-daemon.json` from a disposable checkout `target/` root
  to a cross-checkout home root increases exposure to crash/reboot leftovers,
  stale sockets, and PID reuse. Startup must not falsely reuse an unrelated
  process, and cleanup/takeover must remain ownership-safe.
- Socket path length: `$HOME/.botster/hub` is normally short, but tests should
  keep explicit short temporary roots where many nested fixtures are involved.
- Documentation/history confusion: current docs strongly claim explicit-only
  operation. Update living docs, but do not rewrite historical plans/reports.
- Test runtime: one exhaustive live test can become slow and fragile. Use
  focused resolver/parser tests plus a small production-shaped representative
  matrix rather than starting a daemon per spelling.

## Acceptance checks and downstream proof

- Resolver unit tests in `src/config.rs` prove:
  - `--data-dir` selection wins before config construction;
  - `BOTSTER_HUB_DATA_DIR` wins over `HOME`;
  - plain `HOME=/tmp/botster-fixture-home` resolves `/tmp/botster-fixture-home/.botster/hub`;
  - `XDG_DATA_HOME` has no effect;
  - missing CLI/env/home returns a typed error;
  - plugin/provider defaults are children of the resolved `hub` root only.
- Parser/help tests prove every stateful command accepts omission and a
  consistent explicit option order, rejects duplicate/missing values, and has
  optional usage text. Assertions must name `start` and `smoke` as well as
  `up`, `status`, `doctor`, and `open`.
- Focused production-path test through `./test.sh --test
  hub_daemon_lifecycle_test <ticket-test> -- --exact --nocapture
  --test-threads=1`:
  - set a disposable `HOME`, clear `BOTSTER_HUB_DATA_DIR` and
    `XDG_DATA_HOME`, and run from multiple arbitrary current directories;
  - start with `botster-hub up` or `start` without the flag;
  - exercise representative package install/enable/list, app list/open,
    session list/spawn, session-template list, spawn-target/worktree/context,
    status/doctor, reload, MCP startup, and down/shutdown paths without the flag;
  - prove they all reach the same `$HOME/.botster/hub` daemon/state;
  - assert `$HOME/.botster/{plugins,agents,lua,profiles,shared,workspaces}` is
    unchanged and not loaded.
- Durable-root recovery test:
  - pre-populate `$HOME/.botster/hub` with stale
    `.botster-hub-runtime-daemon.json` metadata and a leftover socket;
  - cover both a dead PID and safely simulatable mismatched/reused PID evidence;
  - assert `up`/`start` rejects unrelated ownership, or performs the existing
    sanctioned stale-runtime takeover and cleanup, without false reuse.
- Override tests prove:
  - explicit `--data-dir` beats both environment and home;
  - `BOTSTER_HUB_DATA_DIR` beats home when the flag is absent;
  - two isolated roots do not share state;
  - relative explicit overrides still produce absolute injected package
    runtime/socket paths across child cwd changes.
- Production runtime proof:
  `script/test-production-package-runtime` passes with an isolated `HOME`, its
  collision guard points at `$HOME/.botster/hub/botster-hub.sock`, its
  before/after snapshots cover the resolved Hub root and every protected
  `.botster` sibling, and its negative `target/` assertion proves the removed
  cwd-relative root is not recreated.
- Static stale-policy audit:
  `rg -n "target/botster-hub-runtime-data|XDG_DATA_HOME|DataDirOptions|default_local_runtime_data_dir|StartOptions|\\.local/share|--data-dir <path>" src tests script .github README.md docs`
  with each remaining hit classified as removed, intentionally explicit test
  isolation, generic placeholder, or historical plan/report.
- Required repository gates:
  - `cargo fmt --all -- --check`
  - `cargo clippy --workspace --all-targets --all-features --locked -- -D warnings`
  - focused resolver and lifecycle tests through `./test.sh --locked ...`
  - full `./test.sh --locked`
  - `./test.sh --locked --test hub_daemon_lifecycle_test cli_daemon_restart_recovers_worker_backed_session_through_transport -- --exact --nocapture`
    to preserve the charter-required restart/adoption proof
  - `script/test-production-package-runtime` with disposable environment inputs
- Runtime proof must name the compiled entry points and show they reach the real
  daemon/socket path. Parser existence or source inspection alone is not
  acceptance.
- Because no package supervision-admission field changes, the charter's exact
  supervised package launch gate is not independently triggered. Existing
  package/app representative proof still runs through the exact compiled Hub
  binary.

## Pipeline gates and artifacts

- Plan artifact: this document.
- Project Pipelines checklist: `checklist_1785028633_218579`.
- Vault checklist: `checklist_1785028610_843503`.
- Binding human answer: `question_1785028653_878753`.
- Plan gate evidence must include target routing, charter/notes, the precedence
  decision, scope/non-scope, ownership, files, risks, runtime-path tests, and
  vault disposition.

## Vault gaps worth capturing

- The answered precedence is durable and supersedes older explicit-only and XDG
  runtime-default guidance. Capture a new atomic note through the inbox
  pipeline after implementation verifies the production path, then mark or
  connect the older startup/default notes as superseded where appropriate.
- Capture an additional gotcha only if implementation reveals a reusable parser
  or child-process re-resolution failure. Do not duplicate this plan as a note.
