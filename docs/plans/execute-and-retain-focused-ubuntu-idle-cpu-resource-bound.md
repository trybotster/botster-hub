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

No focused reviewer/verifier overlay is added. This is CI execution and evidence retention, not daemon/actor/transport behavior (`[[botster-runtime-reviewer-playbook]]`) or package/plugin behavior (`[[botster-package-reviewer-playbook]]`). If first-red attribution creates a separately owned runtime or package ticket, that ticket must load its matching reviewer and verifier overlays.

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

## Existing production path and evidence decision

The real workflow already provides the required runtime path:

1. `.github/workflows/loaded-daemon-lifecycle.yml` runs on `ubuntu-24.04`, validates bounded inputs, checks out an exact subject SHA, precompiles the exact integration-test binary, records Hub and lockfile-pinned Core provenance, invokes the loaded runner, enforces cleanup under `always()`, and uploads the complete artifact directory for 14 days.
2. `script/run-loaded-daemon-lifecycle` rejects `focused-plugin-resource-bounds` unless `stress_profile=none`, maps that selector to `env BOTSTER_ASSERT_IDLE_CPU_BOUND=1 ./test.sh --test hub_daemon_lifecycle_test focused_plugin_resources_are_bounded_across_reconnect_reload_idle_and_unload -- --exact --nocapture`, stores each repetition in `run-NNN.log`, and emits independent run-token, SID-session, and settled-zombie survivor TSVs.
3. `tests/hub_daemon_lifecycle_test.rs` reads Linux process CPU ticks before and after a five-second converged idle window. When `BOTSTER_ASSERT_IDLE_CPU_BOUND` is present, it asserts `delta_ticks * 4 <= ticks_per_second`, which is the 250 ms ceiling.

The environment variable is therefore consumed by the production test path, not merely set. Existing artifacts also discriminate asserted from observational execution: every asserted run retains the env-bearing command, raw `idle_cpu_delta_ticks`, and a green/red Rust result, while the unasserted branch emits the distinct `idle_cpu_bound=observed_not_asserted` line. An explicit positive verdict would improve readability, but repository inspection does not prove it is required for deterministic retention. Per the ticket, the first authoritative campaign runs merged main unchanged; code changes are conditional on concrete artifact insufficiency.

## Scope

1. Re-resolve remote `main` immediately before dispatch and require it to equal `281db04523503c5cf692813ea313344aa6067644` or a documented newer SHA. Then dispatch `.github/workflows/loaded-daemon-lifecycle.yml` from that explicit workflow ref with the exact same full subject SHA:
   - `gh workflow run loaded-daemon-lifecycle.yml --repo trybotster/botster-hub --ref main -f subject_sha=<resolved-40-character-main-SHA> -f test_target=focused-plugin-resource-bounds -f repetitions=20 -f stress_profile=none`
   - Retain the pre-dispatch `main` resolution and require `metadata.txt` `workflow_sha` and `resolved_sha` to equal that recorded full SHA. A moving-ref mismatch invalidates the run.
2. Observe the workflow through completion. If any repetition is red, stop at that first red, preserve its artifact, and attribute whether the root is ticket-owned evidence behavior, an out-of-scope Hub defect, a Core defect, or unrelated infrastructure. Never rerun blindly.
3. Route an attributed red before any repair:
   - Fix only a ticket-owned evidence-retention defect in this ticket, using the smallest Hub change.
   - For a Hub defect outside this ticket, create an owner ticket against `tgt_7e208a0c76a44980a83b63af976b1f22`, register it as a blocking dependency, and stop.
   - For a Core defect, create an owner ticket against the authoritative `botster-core` target, register it as a blocking dependency, and stop.
   - Preserve an infrastructure red with exact unrelatedness evidence and ask for human disposition; do not carry it as a caveat.
4. Download and inspect the complete uploaded artifact. First determine whether the existing command, raw counter, branch discriminator, Rust result, and cleanup files retain the proof deterministically. Only if a specific field cannot be recovered may the implementer change Hub code or the harness. If a change is required, commit/push it and dispatch the workflow from the ticket branch so the executed harness and subject are both pinned:
   - `gh workflow run loaded-daemon-lifecycle.yml --repo trybotster/botster-hub --ref project-pipelines/ticket_1785549893_470247 -f subject_sha=<full-ticket-branch-SHA> -f test_target=focused-plugin-resource-bounds -f repetitions=20 -f stress_profile=none`
   - When `script/run-loaded-daemon-lifecycle` or `.github/workflows/loaded-daemon-lifecycle.yml` changes, `metadata.txt` must record `workflow_sha` equal to the full ticket-branch SHA. Otherwise the run did not exercise the changed harness and cannot count.
   - When only subject code changes, retain the workflow/subject SHA pair and prove the pinned workflow SHA contains merged main `281db04523503c5cf692813ea313344aa6067644` or newer.
5. Produce a bounded repository-visible evidence JSON plus a short report under `docs/reports/` that retain:
   - workflow run URL/ID/attempt, artifact name/digest/expiry, workflow harness SHA, exact subject Hub SHA, the workflow/subject SHA pair, locked Core SHA, runner image/architecture/CPU count, `stress_workers=0`, and all explicit workflow inputs;
   - `test_target=focused-plugin-resource-bounds`
   - `repetitions=20`
   - `stress_profile=none`
   - for repetitions 1 through 20: elapsed seconds, raw idle delta ticks, ticks per second, threshold result derived from the executed assertion and Rust result (or an explicit verdict if a conditional evidence change proves necessary), campaign exit status, and names plus emptiness/count results for `run-NNN-owned-survivors.tsv`, `run-NNN-session-survivors.tsv`, and `run-NNN-zombie-survivors.tsv`;
   - final campaign and cleanup statuses, `cleanup.log`, and empty `active-pgids.tsv`, `active-sessions.tsv`, and `active-run-tokens.tsv` ledgers;
   - SHA-256 hashes for the retained raw artifact files used to construct the bounded record.
6. Link the durable report to the GitHub run. Keep the full uploaded diagnostics as the raw review packet while it is available; the committed bounded record preserves the ticket-defining facts after GitHub's 14-day artifact expiry.

## Non-scope

- No change to the 250 ms ceiling, five-second observation window, production queue/executor defaults, test parallelism, runner image, repetition count, timeout, or cleanup semantics.
- No residual/moderate stress, macOS substitution, averaging across repetitions, retries that hide a red, or relaxed acceptance.
- No managed-Git marker readiness work and no broad lifecycle cleanup refactor.
- No rerun of the four-package production campaign and no claim that this focused deterministic fixture replaces it.
- No changes to `botster-core`, Botster clients, package/plugin APIs, Project Pipelines, TUI, Web, or generated DTOs.
- No adjacent documentation cleanup or new CI/evidence framework.

## Ownership boundaries and dependencies

Botster Hub owns the GitHub workflow, host-profile selector policy, exact Hub/Core provenance, integration test, process cleanup harness, and Hub evidence report. Core continues to own reusable plugin-worker mechanics and live debug counters; this ticket consumes the lockfile-pinned Core through the real Hub binary and does not change Core.

There are no open cross-repository prerequisites in `project_pipelines_current_context`. The merged resource-proof ticket `ticket_1785199716_875648` is the base prerequisite and is present at Hub main `281db04523503c5cf692813ea313344aa6067644`. First-red routing creates and registers a dependency against the authoritative owner target before stopping: `botster-core` for a Core defect, or this same `botster-hub` target for a Hub defect outside the narrow evidence ticket.

Open sibling `ticket_1785548694_519212` edits an unrelated managed-Git test in `tests/hub_daemon_lifecycle_test.rs`. It has no deliverable overlap and needs no dependency, but it is a same-file merge surface if this ticket later requires a conditional test-output edit. Whoever lands second rebases onto the other's commit. A rebase that leaves the focused resource test, loaded runner, and workflow unchanged does not invalidate already-retained Ubuntu evidence.

## Assumptions and unknowns

- Assumption: a successful 20-repetition campaign can retain complete raw evidence with the existing env-bearing command, per-run raw counter and Rust result, status files, three survivor TSVs, metadata, cleanup files, and upload step; this assumption is tested by inspecting the first unchanged-main artifact before authorizing code.
- Assumption: `docs/reports/` is the correct durable destination because current Hub main uses it for the parent resource-proof implementation and committed evidence JSON.
- Assumption: the 14-day raw artifact plus a committed bounded evidence record satisfies durable retention without increasing GitHub artifact retention.
- Unknown until execution: actual Ubuntu tick rate and each repetition's raw CPU delta.
- Unknown until execution: workflow wall-clock cost. If 20 repetitions are unexpectedly excessive, report the measured cost and ask a new human question; do not reduce the campaign.
- Unknown until execution: whether the first campaign exposes a real bound failure or an artifact-shape defect. First-red attribution controls the next action.

## Expected affected surfaces and files

- `docs/plans/execute-and-retain-focused-ubuntu-idle-cpu-resource-bound.md` — this reviewable plan.
- `docs/reports/execute-and-retain-focused-ubuntu-idle-cpu-resource-bound.md` — final workflow and attribution report.
- `docs/reports/focused-ubuntu-idle-cpu-resource-bound-evidence.json` — bounded durable inputs, per-repetition raw counters/results, provenance, cleanup results, and hashes.

Only if the real run proves the existing capture path incomplete may the implementer touch:

- `tests/hub_daemon_lifecycle_test.rs` — only to emit a missing asserted threshold field while preserving the existing Linux assertion.
- `script/run-loaded-daemon-lifecycle` — only to retain missing per-repetition evidence already produced by the test or cleanup path.
- `.github/workflows/loaded-daemon-lifecycle.yml` — only to retain missing workflow metadata/artifacts deterministically.

Those conditional files are not pre-authorized cleanup surfaces; the first run's concrete evidence must justify each changed line.

## Risks and controls

- **A green test without proof that the assertion ran.** Retain the exact `commands.txt` selector command, raw counter, absence of the distinct unasserted-branch line, green/red Rust result, evaluated predicate result, and source/subject SHA that consumes `BOTSTER_ASSERT_IDLE_CPU_BOUND`. If that chain is incomplete in the actual artifact, add the smallest explicit verdict output and rerun all 20.
- **Scheduler-sensitive red.** Preserve first-red raw counters and cleanup, attribute before code changes, and rerun the entire 20 only after a concrete fix. Do not inflate or average the limit.
- **Partial campaign mistaken for acceptance.** Require 20 finished repetition rows and 20 passing assertion evaluations; fail the evidence audit on missing or duplicate indices.
- **Cleanup inferred from test exit.** Inspect all three complementary run-token, SID-session, and settled-zombie TSVs after every repetition plus final workflow cleanup and the three named empty ownership ledgers.
- **Artifact upload mistaken for cleanup proof.** Report upload status separately from campaign and cleanup status, following the vault cancellation guidance.
- **Wrong source or stale binary.** Retain requested/resolved subject SHA, fresh-target realpaths, Hub SHA, and separately resolved locked Core SHA.
- **Requested no-stress input mistaken for an unstressed run.** Retain `stress_workers=0`. The Linux resource sampler remains the sole always-on observer and wakes every five seconds, but it reads external process/system state and cannot add CPU ticks to the Hub PID whose `/proc` delta is asserted.
- **Expired evidence.** Commit a bounded evidence record and hashes before the 14-day raw artifact expires.
- **PII or machine-path leakage.** Keep the committed evidence bounded to approved metadata/counters/results; inspect the diff and run the repository's applicable artifact/PII checks before commit.
- **Scope creep after a red.** Classify the red first; create and register a blocking owner ticket for either an out-of-scope Hub defect or a Core defect instead of absorbing it here.

## Acceptance checks and downstream proof

Implementation checks:

1. `git diff --check`.
2. If the first unchanged-main artifact is sufficient and only reports/evidence are added, validate the JSON, source-file hashes, report links, and applicable artifact/PII scans; do not manufacture a Rust change or unrelated test run.
3. If a conditional Rust or harness change becomes necessary, run `cargo fmt --check`, `cargo clippy --all-targets --all-features -- -D warnings`, the affected runner self-test, and `./test.sh --test hub_daemon_lifecycle_test focused_plugin_resources_are_bounded_across_reconnect_reload_idle_and_unload -- --exact --nocapture` as local mechanism coverage. On macOS the focused test does not execute or satisfy the Linux CPU block.
4. No unit-test seam is planned for a conditional one-line Linux verdict. Its coverage is the rerun 20-repetition Ubuntu campaign, which must retain the asserted evidence from every repetition; do not extract a formatting abstraction solely to unit-test one evidence line.

Authoritative runtime acceptance:

5. One GitHub Actions campaign on `ubuntu-24.04` using the exact explicit inputs above completes all 20 repetitions on one qualifying Hub subject SHA. If the first artifact is insufficient and causes a conditional change, only the fresh post-change full 20 campaign counts.
6. The retained pre-dispatch main resolution equals `metadata.txt` `workflow_sha` and `resolved_sha`; `requested_sha == resolved_sha`. Metadata also proves runner identity, CPU count, `stress_workers=0`, fresh target realpaths, and distinct Hub/locked-Core provenance. Validation metadata proves `focused-plugin-resource-bounds`, `repetitions=20`, and `stress_profile=none`.
7. If any harness file changes, `workflow_sha` equals the full ticket-branch subject SHA used in the qualifying rerun. Otherwise retain the workflow/subject SHA pair and prove both contain merged main `281db04523503c5cf692813ea313344aa6067644` or newer.
8. `commands.txt` proves every repetition invokes the exact focused Rust test through `./test.sh` with `BOTSTER_ASSERT_IDLE_CPU_BOUND=1`.
9. Each `run-001.log` through `run-020.log` contains one raw CPU sample, no `idle_cpu_bound=observed_not_asserted` line, and a green Rust result. The bounded evidence evaluates the committed predicate `delta_ticks * 4 <= ticks_per_second` for each row and retains `result=pass`; if this cannot be established deterministically, add the conditional explicit verdict and rerun all 20. No aggregate average substitutes for any repetition.
10. `campaign-status.tsv` contains 20 completed zero-exit repetitions with elapsed times, and campaign/final cleanup status is zero.
11. For every repetition, `run-NNN-owned-survivors.tsv`, `run-NNN-session-survivors.tsv`, and `run-NNN-zombie-survivors.tsv` are empty after their bounded settle paths. Final cleanup reports zero survivors, `cleanup.log` succeeds, and `active-pgids.tsv`, `active-sessions.tsv`, and `active-run-tokens.tsv` are empty.
12. The complete diagnostics artifact uploads successfully and its digest is retained. The committed report/evidence JSON contains all 20 rows and hashes back to the inspected raw files.
13. The final diff contains only lines traceable to the plan, retained evidence, and any concrete first-run retention gap. No bound, load, lifecycle, managed-Git, or unrelated behavior changes.

This is downstream proof through the actual production entry point: the GitHub workflow selects the real loaded runner, the runner invokes the exact repository wrapper and test, and the integration test launches the real isolated Hub/Core/session-worker topology. Source existence or a local macOS run is not acceptance.

## Vault gaps worth capturing

No new vault gap is known during Plan. Existing atomic notes already cover named-environment authority, exact-target precompilation, assertion/evidence attribution, artifact-versus-cleanup separation, cross-session live censuses, zombie baseline censuses, and distinct Hub/Core provenance.

After execution, capture durable knowledge only if the Ubuntu campaign reveals a reusable CI rule not already represented—for example, a stable cross-platform threshold-verdict evidence format or a new GitHub artifact-retention limitation. Ticket-specific raw values belong in the Hub report, not the vault.
