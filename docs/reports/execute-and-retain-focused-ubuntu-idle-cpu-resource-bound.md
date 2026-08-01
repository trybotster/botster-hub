# Implementation report: focused Ubuntu idle CPU resource bound

## Target and applied guidance

- Target repository: `trybotster/botster-hub`.
- Target ID: `tgt_7e208a0c76a44980a83b63af976b1f22`.
- Applied in order: [[implementer-playbook]],
  [[botster-implementer-playbook]], [[botster-hub-playbook]], and the targeted
  atomic notes named in the approved plan, including exact Hub/Core provenance,
  repository test-wrapper use, named-runner authority, loaded-workflow
  precompilation, layered cleanup, and independent live/zombie survivor oracles.
- [[project-pipelines-playbook]] was not applied as a task-surface overlay. This
  run uses Project Pipelines for delivery records, but changes no Project
  Pipelines package/plugin path or workflow policy.
- Assumption: the existing workflow artifact is sufficient only if its actual
  downloaded bytes prove every input, assertion result, raw counter, and cleanup
  predicate without a harness change.

## Outcome

The existing merged-main path is sufficient. GitHub Actions run
[30680145233](https://github.com/trybotster/botster-hub/actions/runs/30680145233)
executed the exact `focused-plugin-resource-bounds` selector 20 times on
`ubuntu-24.04`, with `stress_profile=none` and `stress_workers=0`. All 20
repetitions passed the asserted 250 ms ceiling. Observed five-second idle CPU
was 0 ticks in 17 repetitions and 1 tick in 3 repetitions at 100 ticks/second,
or 0-10 ms.

This campaign discharges the authoritative unstressed Linux carry-forward from
merged ticket `ticket_1785199716_875648`, recorded in
`docs/reports/bounded-hub-resources-fresh-campaign-evidence.json`
(`residual_risk[0]`) and
`docs/reports/prove-bounded-hub-resources-with-four-packages-and-reconnect-churn.md`.
Those parent files remain unchanged as historical records.

The real production entry point is proven end to end: the dispatched workflow
checked out workflow and subject SHA
`281db04523503c5cf692813ea313344aa6067644`, precompiled the exact integration
test target and locked Core session worker, then the loaded runner invoked
`env BOTSTER_ASSERT_IDLE_CPU_BOUND=1 ./test.sh --test
hub_daemon_lifecycle_test
focused_plugin_resources_are_bounded_across_reconnect_reload_idle_and_unload
-- --exact --nocapture`. Every raw run log contains one
`idle_cpu_delta_ticks`/`ticks_per_second` sample and a green Rust result, and no
log contains `idle_cpu_bound=observed_not_asserted`.

## Files changed

- `docs/plans/execute-and-retain-focused-ubuntu-idle-cpu-resource-bound.md` —
  synchronizes the reviewer-approved header-only survivor predicate with the
  implemented audit.
- `docs/reports/focused-ubuntu-idle-cpu-resource-bound-evidence.json` — bounded
  workflow inputs, provenance, 20 raw counter/result rows, corrected survivor
  predicates, final cleanup state, artifact identity, and raw-file SHA-256
  hashes.
- `docs/reports/execute-and-retain-focused-ubuntu-idle-cpu-resource-bound.md` —
  this implementation and attribution report.

No Rust, runner, or workflow file changed. The unchanged-main artifact retained
the proof deterministically, so the plan did not authorize a speculative
positive-verdict formatter or harness change.

## Ownership boundaries and cross-repository work

Hub ownership was preserved. This report consumes the Hub-owned workflow,
selector policy, integration test, process cleanup harness, and evidence packet.
Core remains the owner of plugin-worker mechanisms and the session-worker
binary. Its exact consumed revision is retained separately as
`5846fc776d31e2b6c98a8d932f50a31078743901`; no Core file was edited.

There are no cross-repository dependencies or separately routed changes. The
managed-Git sibling ticket remains an unrelated same-file merge surface only;
this ticket did not touch that test file.

## Deviations from plan

The only correction is the approved Plan Review finding: clean survivor TSVs
are not empty because each capture helper always writes a header. The audit and
bounded evidence therefore require each of the 60 files to be present, contain
exactly one header line and zero data rows, and contain no `truncated` row.
Every file passed. Missing or zero-byte files would have failed the audit.

No scope deviation or conditional implementation change occurred.

## Tests and downstream proof

Passed:

- Authoritative GitHub Actions run `30680145233`, job `91315379265`, on runner
  image `ubuntu24 20260720.247.2` / X64 / 4 CPUs: all workflow steps passed in
  5m50s.
- Exact workflow inputs: subject SHA `281db045...`,
  `test_target=focused-plugin-resource-bounds`, `repetitions=20`, and
  `stress_profile=none`.
- Workflow, requested, resolved, and Hub SHA equality; separate locked Core SHA;
  fresh-target Hub and session-worker realpaths.
- 20/20 zero-exit repetitions, 20/20 asserted predicate evaluations, and 20/20
  green Rust results. Maximum observed idle CPU was 10 ms against 250 ms.
- 60/60 survivor files present and header-only with no truncation; final
  campaign and cleanup status zero; `active-pgids.tsv`, `active-sessions.tsv`,
  and `active-run-tokens.tsv` all zero bytes.
- Complete artifact upload: `loaded-daemon-lifecycle-30680145233-1`, digest
  `sha256:8fc44e7b5163cd9bc74e5993378bc185294d9ab9795bb7e4d45bc7b0a000c768`,
  retained through `2026-08-15T02:36:24Z`.
- Local evidence audit, JSON parse/semantic checks, raw-file hash verification,
  repository artifact/PII scan, and `git diff --check`.

The bounded record is
`docs/reports/focused-ubuntu-idle-cpu-resource-bound-evidence.json`; the raw
GitHub artifact remains the complete diagnostics packet while its 14-day
retention window is open.

## Unverified behavior and residual risk

No ticket acceptance behavior remains unverified. The GitHub annotation that
`mlugg/setup-zig` currently targets deprecated Node.js 20 is unrelated to the
CPU assertion or retained evidence; GitHub forced that action to Node.js 24 and
the toolchain step passed. Future artifact download depends on GitHub's stated
14-day retention, so the committed bounded record and hashes are the durable
proof after expiry.

The focused deterministic fixture is not evidence for the broader four-package
production campaign, and this ticket makes no such claim.

## Missing vault guidance

Review identified and captured two reusable rules that were missing from the
processed vault: [[header bearing survivor evidence files invert the emptiness
predicate]] and [[env gated assertions are proven executed by the absent else
branch marker]]. Both rules have since been processed into canonical vault
notes, with their source captures retained in the vault archive. Existing notes
also cover the underlying need for independent live, session, and zombie
cleanup oracles.
