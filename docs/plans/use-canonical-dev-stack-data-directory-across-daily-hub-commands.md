---
description: Plan for routing every daily Botster Hub command through the canonical dev-stack data-directory default while preserving explicit operator surfaces.
---

# Use the canonical dev-stack data directory across daily Hub commands

## Context loaded

- Pipeline context: ticket `ticket_1784739481_134497`, run `run_1784739484_961139`, active Plan step `botster_plan`, and required gate `botster_plan_gate`. There were no prior artifacts, findings, reviews, questions, answers, or dependencies.
- Vault context: [[identity]], [[goals]], [[planner-playbook]], [[botster-planner-playbook]], [[botster-architecture]], [[cli-patterns]], [[spa-patterns]], [[project pipeline orchestration belongs in a device-level botster plugin]], [[project pipelines needs an operator workbench not more primitives]], [[project pipelines ui contract belongs in the plugin readme]], [[botster orchestration should spawn agents with explicit target ids]], and [[botster orchestration prompts must bind agents to explicit worktrees]].
- Repo context: `src/main.rs`, `tests/hub_daemon_lifecycle_test.rs`, `README.md`, `test.sh`, `docs/plans/document-daily-use-dev-workflow-new-botster.md`, and `docs/plans/add-botster-hub-doctor-and-smoke-commands-for-local-runtime.md`.
- Current production path: `main` dispatches daily commands in `src/main.rs`; their parsers choose a data directory; `explicit_config` builds the daemon endpoint; status/doctor/down use daemon transport; `open web|tui` resolves fixed selectors through `open_app_by_selector`; `smoke` composes the dev-stack preparation and live daemon/session/WebRTC checks.
- Current gap: `up` and `down` already allow omission and resolve `target/botster-hub-dev-stack-data`; `open`, `doctor`, and `status` require `DataDirOptions`; `smoke` parses the default and then rejects it. Help and README still describe most daily overrides as mandatory.
- Workflow evidence is recorded in run checklists `checklist_1784739577_703475` and `checklist_1784739582_416732`. Initial create calls timed out, but listing proved both checklists and their items were durably created before they were updated.

## Scope

- Make `up`, `down`, `open web`, `open tui`, `doctor`, `smoke`, and `status` accept an omitted `--data-dir` and resolve it through one CLI-owned canonical daily default.
- Preserve `--data-dir <path>` as the highest-priority explicit override for every daily command.
- Replace the current split parsing with one small daily data-directory option/resolution path, reusing the existing `target/botster-hub-dev-stack-data` value rather than adding another constant or compatibility branch.
- Keep daily aliases thin: `open web|tui` must continue into the existing app-open production path, and the other commands must continue into their existing daemon-backed implementations.
- Update command/global help and README daily examples so brackets accurately mark the override as optional and the no-flag flow is the primary daily workflow.
- Add focused compiled-binary CLI coverage for the shared default and explicit isolated override behavior.

## Non-scope

- Do not make `start`, `shutdown`, `sessions`, `packages`, raw `apps` operations, `mcp-serve`, `reload`, `inspect`, providers, spawn targets, session templates, or other lower-level/operator commands implicit.
- Do not rename or version commands, retain dual parsers for compatibility, introduce environment/config-file selection, or add another default constant.
- Do not change daemon protocol, hub/runtime state ownership, package discovery, app selector semantics, smoke proof contents, lifecycle cleanup, TUI, SPA, plugins, MCP, Rails relay, or workspace orchestration.
- Do not refactor unrelated `src/main.rs` command parsing or README sections.

## Assumptions and unknowns

- Assumption: the canonical daily default remains the checkout-relative `target/botster-hub-dev-stack-data`; this is the value used by `default_dev_stack_data_dir()` and documented for `up` today.
- Assumption: “centralized resolver” means all seven named daily commands reach the same function/value when the flag is omitted. A narrow daily options type/helper is appropriate; broadening `DataDirOptions` would accidentally weaken explicit lower-level commands.
- Assumption: an explicit relative or absolute `--data-dir` is passed through unchanged, matching current behavior.
- Assumption: `smoke` without a flag should use the same runtime as `up`; its existing started-versus-reused ownership cleanup rules must remain intact.
- Assumption: `open` may route internally through `open_app_by_selector`, while raw `apps open` remains explicit, exactly as the ticket permits.
- Unknown: the cleanest test organization may extend the existing local-runtime integration test or add a dedicated test that runs all daily commands under a temporary current directory. The implementer should prefer one shared fixture to avoid repeatedly starting the dev stack.
- Unknown: whether `data_dir=explicit` output from doctor/smoke should become a neutral/default-aware label. If it becomes false when the flag is omitted, update only that presentation and its focused assertions; do not expose new local paths where output is intentionally scrubbed.
- No human question is needed: repo code and ticket text agree on the default, command boundary, and override precedence.

## Botster layers touched

- Rust Hub CLI command parsing and dispatch only.
- Existing daemon-backed daily runtime paths, exercised but not redesigned.
- Rust compiled-binary lifecycle integration tests.
- Repository user documentation and CLI usage output.

## Affected surfaces/files

- `src/main.rs`
  - Introduce or reshape a private daily data-directory options/helper that accepts either no arguments or exactly `--data-dir <path>` and calls the single canonical default resolver on omission.
  - Route `DevStackOptions`/`up`, `LocalRuntimeDownOptions`/`down`, `doctor`, `smoke`, `status`, and `open web|tui` through that daily resolver without changing lower-level `DataDirOptions` explicit parsing.
  - Remove `SmokeOptions`' current rejection of `DevStackOptions.default_data_dir`; retain any default/explicit marker only where output legitimately needs it.
  - Update `print_global_help`/`usage_for` strings for the seven daily commands to show `[--data-dir <path>]`, leaving explicit surfaces unchanged.
- `tests/hub_daemon_lifecycle_test.rs`
  - Add or extend compiled-binary tests that run from a temporary checkout-like current directory, start the dev stack without `--data-dir` using explicit fixture package paths, then prove no-flag `status`, `doctor`, `open web`, `open tui`, and `down` target that runtime.
  - Prove no-flag `smoke` targets the same canonical directory and preserves ownership semantics; structure this to avoid unnecessary duplicate daemon startup if the current fixture supports reuse.
  - Retain/add explicit custom `--data-dir` assertions so daily commands continue to isolate state and never fall back to the canonical directory when overridden.
  - Update help assertions to distinguish optional daily overrides from mandatory lower-level data directories.
- `README.md`
  - Make the no-flag `up`, `open`, `doctor`, `smoke`, `status`, and `down` sequence the canonical daily example.
  - Explain the shared checkout-relative default once, show `--data-dir <path>` as an optional isolation override, and preserve explicit examples for lower-level commands.

No Cargo dependency, protocol crate, daemon module, plugin, client, or frontend file should change.

## Implementation sequence

1. Add the private shared daily parser/resolver beside the existing data-directory option types. Preserve one canonical `target/botster-hub-dev-stack-data` definition and reject malformed/extra arguments with the command-specific usage string.
2. Wire all seven daily dispatch paths to it. Keep raw operator parsers explicit and keep `open` delegating to the existing app launch function.
3. Update usage/help text and focused help tests in the same change so the public contract cannot drift from parsing.
4. Add a live compiled-binary regression that proves commands after no-flag `up` talk to its daemon, plus explicit isolated-directory regression coverage.
5. Update only the README daily workflow and override explanation required by the behavior.
6. Run focused tests, formatting, then the full repository-prescribed test script.

## Risks

- Broadly changing `DataDirOptions::parse` would silently make lower-level surfaces implicit. Keep a separate daily-only entry point and assert lower-level help/parsing remains explicit.
- Relative defaults depend on process current directory. Integration tests must set the same temporary `current_dir` for every daily subprocess so they prove the checkout workflow rather than accidentally use the repository's real `target/` state.
- A parser-only test is insufficient. The test must start a live daemon and prove later no-flag commands reach it; otherwise an unwired or differently resolved runtime could pass.
- `smoke` owns cleanup only for a daemon it starts. Reusing the daemon created by `up` must not stop it unexpectedly; the test should verify `status` still succeeds afterward when reuse is expected.
- `open web` may launch a supervised process and `open tui` uses a foreground terminal contract. Reuse current deterministic fixture packages and cleanup guards to avoid leaked processes or flaky real-browser/TUI dependencies.
- Updating printed `data_dir` labels can create unnecessary output churn or leak paths. Change output only where the existing claim would become inaccurate.
- Full `./test.sh` can expose unrelated parallel failures; any failure must be rerun narrowly and classified with exact evidence, not dismissed wholesale.

## Acceptance checks/tests

- Focused compiled-binary integration test via `./test.sh --test hub_daemon_lifecycle_test <new_or_extended_daily_default_test> -- --test-threads=1`:
  - set a temporary current directory;
  - run `botster-hub up` without `--data-dir` and deterministic package fixture flags;
  - run `status`, `doctor`, `open web`, and `open tui` without the flag and assert they resolve the live runtime rather than return usage/not-running errors;
  - run `smoke` without the flag and assert its existing daemon/core/package/app/session/WebRTC proof contract while preserving reused-daemon ownership;
  - run `down` without the flag and assert clean shutdown.
- Explicit override regression via the same harness or a focused companion test:
  - start/use an isolated `--data-dir <temp>`;
  - prove representative daily commands target it;
  - prove lower-level commands still reject omission.
- Help contract test: global help and command usage show `[--data-dir <path>]` for `up`, `down`, `open`, `doctor`, `smoke`, and `status`, while `start`, `shutdown`, `apps`, `sessions`, and `packages` retain required `--data-dir <path>`.
- Static review: `rg -n "default_dev_stack_data_dir|botster-hub (up|down|doctor|smoke|status|open)" src/main.rs README.md tests/hub_daemon_lifecycle_test.rs` shows one default value/resolver and no stale mandatory daily examples.
- Format: `cargo fmt --check` (or run `cargo fmt` before the check).
- Required repository verification: `./test.sh`. Do not substitute raw `cargo test`; `test.sh` supplies `BOTSTER_ENV=test` to avoid host credential prompts.

## Runtime path proof required

- Evidence must name the compiled CLI entry points in `src/main.rs` and show each receives the shared resolved path.
- Live integration output must prove `up` creates/reuses the daemon at the canonical default and subsequent no-flag daily commands reach that same daemon.
- `open web|tui` evidence must traverse the existing `operator_open_alias -> open_app_by_selector -> daemon transport` path; proving a parser helper exists is not enough.
- Explicit custom-directory tests must prove override precedence and isolation.

## Pipeline gates and artifacts

- Plan artifact: this document.
- Project Pipelines workflow checklist: `checklist_1784739577_703475` (all Plan checkpoints done).
- Vault checklist: `checklist_1784739582_416732` (notes, conflict result, plan-stage verification evidence, and capture decision recorded).
- Plan gate evidence should attach the artifact path, explicit assumptions, exact affected surfaces, runtime proof, focused/full verification commands, and the “no convention conflict” decision.

## Vault gaps worth capturing

- No vault write is needed at Plan time. The ticket is a concrete product-policy correction and existing [[cli-patterns]] already constrains thin aliases, explicit operator surfaces, daemon-backed runtime proof, and use of `./test.sh`.
- Capture a new atomic note only if implementation reveals a reusable gotcha, such as relative default directories resolving inconsistently across internally spawned daily commands or smoke ownership changing when it reuses an `up` daemon.
- If implementation only centralizes the resolver exactly as planned, record “no durable knowledge discovered” rather than duplicating the ticket as a vault convention.
