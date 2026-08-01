# Execute and retain the focused Ubuntu idle CPU resource bound

## Ticket and target

- Ticket: `ticket_1785549893_470247` — Hub CI: execute and retain the focused Ubuntu idle CPU resource bound.
- Target repository: `trybotster/botster-hub` (`botster-hub`).
- Target ID: `tgt_7e208a0c76a44980a83b63af976b1f22`.
- Routed worktree: the Project Pipelines worktree for this ticket, verified at merged Hub main `281db04523503c5cf692813ea313344aa6067644` with `origin/main` at the same commit during Plan.
- Repository charter: `[[botster-hub-playbook]]`.

The target was resolved from `project_pipelines_current_context` and the Hub spawn-target registry. It was not inferred from the ambient directory.

## Context loaded

Role and repository guidance:

- `[[planner-playbook]]`
- `[[botster-planner-playbook]]`
- `[[botster-hub-playbook]]`
- `[[botster-architecture]]`
- `[[cli-patterns]]`
- `[[spa-patterns]]`

Targeted constraints:

- `[[botster hub is a first party host profile over core]]`
- `[[plugin worker queue capacity and executor concurrency are independent host profile knobs]]`
- `[[botster plugin runtime uses supervisor plus per plugin workers]]`
- `[[live hub proof records distinct hub and locked core binary provenance]]`
- `[[loaded lifecycle ci precompiles the exact test target before synthetic cpu stress]]`
- `[[acceptance environment substitutions require pipeline recorded human authorization]]`
- `[[workflow cancellation cleanup is idempotent across campaign traps and outer steps]]`
- `[[cancellation artifact upload is best effort evidence separate from survivor cleanup]]`
- `[[sid scoped census is blind to setsid session leaks]]`
- `[[argv marker censuses cannot see zombie survivors]]`
- `[[test script required for rust tests not cargo test]]`
- `[[suite wide acceptance criteria make every observed test failure in scope]]`
- `[[plan agents must author vault context as wikilinks not home paths]]`
- `[[pipeline vault checklists must cite exact resolvable note titles]]`
- `[[vault example paths are not repository placement conventions]]`

Repository and CI context inspected:

- `README.md`
- `.github/workflows/loaded-daemon-lifecycle.yml`
- `script/run-loaded-daemon-lifecycle`
- `script/run-loaded-daemon-lifecycle-selftest`
- `tests/hub_daemon_lifecycle_test.rs`
- `docs/hub-resource-proof.md`
- `docs/plans/prove-bounded-hub-resources-with-four-packages-and-reconnect-churn.md`
- `docs/reports/prove-bounded-hub-resources-with-four-packages-and-reconnect-churn.md`
- merge commit `281db04523503c5cf692813ea313344aa6067644` and current GitHub Actions history.

`[[project-pipelines-playbook]]` is not a task-surface overlay for this plan. Project Pipelines supplies the delivery record, but no Project Pipelines package/plugin path or workflow policy is being changed.

## Binding human decision

Project Pipelines question `question_1785550196_163602` resolved the only material input ambiguity:

- acceptance requires `repetitions=20`, the workflow default;
- `stress_profile=none` on `ubuntu-24.04` is mandatory;
- every repetition must execute and pass the 250 ms assertion;
- every repetition must retain its raw CPU sample, threshold evaluation, elapsed time, and cleanup census;
- zero owned survivors are required after every repetition and at final workflow cleanup;
- stop at the first red and attribute it before rerunning;
- do not average away an outlier, inflate the bound, add blind retries, or substitute another environment or load profile.

A one-repetition wiring smoke is optional diagnostic evidence only and cannot close the ticket.

## Existing production path and identified gap

The real workflow already provides the required runtime path:

1. `.github/workflows/loaded-daemon-lifecycle.yml` runs on `ubuntu-24.04`, validates bounded inputs, checks out an exact subject SHA, precompiles the exact integration-test binary, records Hub and lockfile-pinned Core provenance, invokes the loaded runner, enforces cleanup under `always()`, and uploads the complete artifact directory for 14 days.
2. `script/run-loaded-daemon-lifecycle` rejects `focused-plugin-resource-bounds` unless `stress_profile=none`, maps that selector to `env BOTSTER_ASSERT_IDLE_CPU_BOUND=1 ./test.sh --test hub_daemon_lifecycle_test focused_plugin_resources_are_bounded_across_reconnect_reload_idle_and_unload -- --exact --nocapture`, stores each repetition in `run-NNN.log`, and emits per-run live-survivor and zombie-survivor TSVs.
3. `tests/hub_daemon_lifecycle_test.rs` reads Linux process CPU ticks before and after a five-second converged idle window. When `BOTSTER_ASSERT_IDLE_CPU_BOUND` is present, it asserts `delta_ticks * 4 <= ticks_per_second`, which is the 250 ms ceiling.

The environment variable is therefore consumed by the production test path, not merely set. The remaining evidence gap is narrower: a successful asserted repetition emits raw `idle_cpu_delta_ticks` and the Rust test success line, but no explicit machine-readable threshold verdict. That makes the requested per-repetition threshold result inferential. The smallest repair is to emit the evaluated limit and `pass`/`fail` result immediately before the existing assertion, without changing the condition or ceiling.

## Scope

1. In `tests/hub_daemon_lifecycle_test.rs`, calculate the existing assertion predicate once and print an explicit bounded line for asserted Linux runs containing at least raw delta ticks, ticks per second, the 250 ms ceiling, and `result=pass|fail`; then feed the same predicate into the unchanged assertion. Preserve the current observational message when the selector signal is absent.
2. Add or adjust the smallest focused source-level/test coverage needed to prove the asserted branch emits a verdict and still fails when the bound is exceeded. Do not introduce a resource-evidence abstraction solely for formatting one line.
3. Run repository-approved local checks for the surgical output change. Local macOS results are diagnostic only and cannot satisfy the Ubuntu acceptance gate.
4. Commit/push the implementation and dispatch `.github/workflows/loaded-daemon-lifecycle.yml` against the resulting full 40-character Hub SHA with explicit inputs:
   - `subject_sha=<ticket implementation SHA, which must contain merged main 281db04523503c5cf692813ea313344aa6067644 or newer>`
   - `test_target=focused-plugin-resource-bounds`
   - `repetitions=20`
   - `stress_profile=none`
5. Observe the workflow through completion. If any repetition is red, stop at that first red, preserve its artifact, attribute whether the root is product behavior, harness/evidence behavior, or unrelated infrastructure, and fix only an attributable ticket-owned defect before a fresh full 20-repetition campaign. Never rerun blindly.
6. Download and inspect the complete uploaded artifact. Produce a bounded repository-visible evidence JSON plus a short report under `docs/reports/` that retain:
   - workflow run URL/ID/attempt, artifact name/digest/expiry, workflow harness SHA, exact subject Hub SHA, locked Core SHA, runner image/architecture/CPU count, and all explicit workflow inputs;
   - for repetitions 1 through 20: elapsed seconds, raw idle delta ticks, ticks per second, explicit threshold result, Rust test result, campaign exit status, and names plus emptiness/count results for owned-live and zombie survivor evidence;
   - final campaign and cleanup statuses, empty active ownership ledgers, and final workflow cleanup outcome;
   - SHA-256 hashes for the retained raw artifact files used to construct the bounded record.
7. Link the durable report to the GitHub run. Keep the full uploaded diagnostics as the raw review packet while it is available; the committed bounded record preserves the ticket-defining facts after GitHub's 14-day artifact expiry.

## Non-scope

- No change to the 250 ms ceiling, five-second observation window, production queue/executor defaults, test parallelism, runner image, repetition count, timeout, or cleanup semantics.
- No residual/moderate stress, macOS substitution, averaging across repetitions, retries that hide a red, or relaxed acceptance.
- No managed-Git marker readiness work and no broad lifecycle cleanup refactor.
- No rerun of the four-package production campaign and no claim that this focused deterministic fixture replaces it.
- No changes to `botster-core`, Botster clients, package/plugin APIs, Project Pipelines, TUI, Web, or generated DTOs.
- No adjacent documentation cleanup or new CI/evidence framework.

## Ownership boundaries and dependencies

Botster Hub owns the GitHub workflow, host-profile selector policy, exact Hub/Core provenance, integration test, process cleanup harness, and Hub evidence report. Core continues to own reusable plugin-worker mechanics and live debug counters; this ticket consumes the lockfile-pinned Core through the real Hub binary and does not change Core.

There are no open cross-repository prerequisites in `project_pipelines_current_context`. The merged resource-proof ticket `ticket_1785199716_875648` is the base prerequisite and is present at Hub main `281db04523503c5cf692813ea313344aa6067644`. If investigation identifies a genuine Core defect, stop and register a dependency ticket against the authoritative `botster-core` target rather than broadening this Hub run.

## Assumptions and unknowns

- Assumption: a successful 20-repetition campaign can retain complete raw evidence with the existing per-run logs, status files, cleanup logs, survivor TSVs, metadata, commands, and upload step once the explicit threshold-verdict line is added.
- Assumption: `docs/reports/` is the correct durable destination because current Hub main uses it for the parent resource-proof implementation and committed evidence JSON.
- Assumption: the 14-day raw artifact plus a committed bounded evidence record satisfies durable retention without increasing GitHub artifact retention.
- Unknown until execution: actual Ubuntu tick rate and each repetition's raw CPU delta.
- Unknown until execution: workflow wall-clock cost. If 20 repetitions are unexpectedly excessive, report the measured cost and ask a new human question; do not reduce the campaign.
- Unknown until execution: whether the first campaign exposes a real bound failure or an artifact-shape defect. First-red attribution controls the next action.

## Expected affected surfaces and files

- `tests/hub_daemon_lifecycle_test.rs` — explicit per-repetition asserted threshold verdict while preserving the existing Linux assertion.
- `docs/plans/execute-and-retain-focused-ubuntu-idle-cpu-resource-bound.md` — this reviewable plan.
- `docs/reports/execute-and-retain-focused-ubuntu-idle-cpu-resource-bound.md` — final workflow and attribution report.
- `docs/reports/focused-ubuntu-idle-cpu-resource-bound-evidence.json` — bounded durable inputs, per-repetition raw counters/results, provenance, cleanup results, and hashes.

Only if the real run proves the existing capture path incomplete may the implementer touch:

- `script/run-loaded-daemon-lifecycle` — only to retain missing per-repetition evidence already produced by the test or cleanup path.
- `.github/workflows/loaded-daemon-lifecycle.yml` — only to retain missing workflow metadata/artifacts deterministically.

Those conditional files are not pre-authorized cleanup surfaces; the first run's concrete evidence must justify each changed line.

## Risks and controls

- **A green test without proof that the assertion ran.** Retain the exact `commands.txt` selector command, the explicit asserted verdict from every `run-NNN.log`, and the source/subject SHA that consumes `BOTSTER_ASSERT_IDLE_CPU_BOUND`.
- **Scheduler-sensitive red.** Preserve first-red raw counters and cleanup, attribute before code changes, and rerun the entire 20 only after a concrete fix. Do not inflate or average the limit.
- **Partial campaign mistaken for acceptance.** Require 20 finished repetition rows and 20 passing asserted verdicts; fail the evidence audit on missing or duplicate indices.
- **Cleanup inferred from test exit.** Inspect independent all-session owned-process and settled zombie TSVs after every repetition plus final workflow cleanup and empty ownership ledgers.
- **Artifact upload mistaken for cleanup proof.** Report upload status separately from campaign and cleanup status, following the vault cancellation guidance.
- **Wrong source or stale binary.** Retain requested/resolved subject SHA, fresh-target realpaths, Hub SHA, and separately resolved locked Core SHA.
- **Expired evidence.** Commit a bounded evidence record and hashes before the 14-day raw artifact expires.
- **PII or machine-path leakage.** Keep the committed evidence bounded to approved metadata/counters/results; inspect the diff and run the repository's applicable artifact/PII checks before commit.
- **Scope creep after a red.** Classify the red first; register cross-repository ownership instead of editing Core or adjacent clients in this Hub ticket.

## Acceptance checks and downstream proof

Implementation checks:

1. `git diff --check`.
2. `cargo fmt --check`.
3. `cargo clippy --all-targets --all-features -- -D warnings`.
4. `./test.sh --test hub_daemon_lifecycle_test focused_plugin_resources_are_bounded_across_reconnect_reload_idle_and_unload -- --exact --nocapture` as local mechanism coverage. On macOS it does not execute or satisfy the Linux bound.
5. Any new narrow formatter/predicate unit test must prove both `pass` and `fail` verdict formatting without weakening the integration assertion.

Authoritative runtime acceptance:

6. One GitHub Actions campaign on `ubuntu-24.04` using the exact explicit inputs above completes all 20 repetitions on one qualifying Hub subject SHA.
7. `metadata.txt` proves requested/resolved Hub SHA equality, runner identity, CPU count, fresh target realpaths, and distinct Hub/locked-Core provenance; validation metadata proves `focused-plugin-resource-bounds`, `repetitions=20`, and `stress_profile=none`.
8. `commands.txt` proves every repetition invokes the exact focused Rust test through `./test.sh` with `BOTSTER_ASSERT_IDLE_CPU_BOUND=1`.
9. Each `run-001.log` through `run-020.log` contains one raw CPU sample and one explicit asserted threshold evaluation with `result=pass`; each Rust test result is green. No aggregate average substitutes for any repetition.
10. `campaign-status.tsv` contains 20 completed zero-exit repetitions with elapsed times, and campaign/final cleanup status is zero.
11. Every per-repetition owned-survivor and zombie-survivor evidence file is empty after the bounded settle path; final cleanup reports no owned live processes or zombies and the active ownership ledgers are empty.
12. The complete diagnostics artifact uploads successfully and its digest is retained. The committed report/evidence JSON contains all 20 rows and hashes back to the inspected raw files.
13. The final diff contains only lines traceable to explicit threshold observability, this plan, and retained evidence. No bound, load, lifecycle, or unrelated behavior changes.

This is downstream proof through the actual production entry point: the GitHub workflow selects the real loaded runner, the runner invokes the exact repository wrapper and test, and the integration test launches the real isolated Hub/Core/session-worker topology. Source existence or a local macOS run is not acceptance.

## Vault gaps worth capturing

No new vault gap is known during Plan. Existing atomic notes already cover named-environment authority, exact-target precompilation, assertion/evidence attribution, artifact-versus-cleanup separation, cross-session live censuses, zombie baseline censuses, and distinct Hub/Core provenance.

After execution, capture durable knowledge only if the Ubuntu campaign reveals a reusable CI rule not already represented—for example, a stable cross-platform threshold-verdict evidence format or a new GitHub artifact-retention limitation. Ticket-specific raw values belong in the Hub report, not the vault.
