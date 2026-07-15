# Loaded daemon lifecycle runner

The **Loaded daemon lifecycle diagnostics** GitHub Actions workflow provides the
isolated, on-demand runner for the real-daemon lifecycle tail. Each dispatch gets
a fresh GitHub-hosted `ubuntu-24.04` VM; it does not use a developer machine,
production system, shared self-hosted runner, secret, or permanent resource.

The workflow checks out its harness and the selected subject into separate
directories. The subject must be a full commit SHA in this repository. The
default, `20871abafdd227a1c145e07035731f916938ff28`, is the diagnostics commit
for `stalled_attach_stdout_does_not_block_other_daemon_commands`.

## Dispatch

Repository operators who can run Actions may use the **Run workflow** control in
GitHub Actions. Keep these defaults for the ticket acceptance campaign:

- `test_target`: `lifecycle-suite`
- `repetitions`: `20`
- `stress_profile`: `residual-tail`

The equivalent CLI dispatch is:

```sh
gh workflow run loaded-daemon-lifecycle.yml \
  --ref main \
  -f subject_sha=20871abafdd227a1c145e07035731f916938ff28 \
  -f test_target=lifecycle-suite \
  -F repetitions=20 \
  -f stress_profile=residual-tail
```

Use `focused-stalled-attach` with one repetition for compilation and diagnostic
triage. A focused run does not satisfy the required 20-or-more default-parallel
lifecycle-suite campaign. The runner stops at the first red repetition so the
failure remains easy to locate and no later retry can hide it.

## Bounded stress and time budgets

The profiles start only job-local CPU workers:

- `residual-tail`: 12 workers per detected CPU, capped at 64 workers.
- `moderate`: 2 workers per detected CPU, capped at 16 workers.
- `none`: no synthetic load worker.

The label describes requested workers, not achieved contention. Use the recorded
`resource-samples.log` (`/proc/loadavg`, CPU, memory, and process samples every
five seconds) when comparing a run with the residual tail.

Each repetition has a 15-minute outer deadline. The campaign has a 330-minute
inner deadline inside the 360-minute GitHub job timeout, leaving 30 minutes for
process teardown and artifact upload. These deadlines do not change any timeout
inside the lifecycle tests.

## Evidence and artifacts

Every command streams combined stdout/stderr into the workflow log and writes a
copy into the artifact bundle. The bundle is named
`loaded-daemon-lifecycle-<run-id>-<attempt>` and is retained for 14 days. It
contains:

- `metadata.txt`: requested/resolved subject, workflow commit, pinned Rust/Cargo
  and Zig, runner image, architecture, CPU count, and selected inputs.
- `commands.txt` and `campaign-status.tsv`: exact wrapper command, repetition,
  stage times, elapsed time, and exit status.
- `run-NNN.log`: complete combined stdout/stderr, including assertion or panic.
- `resource-samples.log`: observed load, CPU, memory, and process data.
- `owned-pgids.tsv`, `cleanup.log`, and `campaign-summary.txt`: ownership,
  TERM/KILL/wait decisions, post-clean checks, and final statuses.

Download from the run's **Artifacts** section or with:

```sh
gh run list --workflow loaded-daemon-lifecycle.yml
gh run download RUN_ID --name loaded-daemon-lifecycle-RUN_ID-1
```

Red tests and inner timeouts still run cleanup and upload. A hard GitHub
cancellation can prevent the final upload step from completing, so the harness
also streams test and cleanup evidence to the durable workflow log.

## Cancellation, timeout, and teardown

Cancel from the Actions run page or with `gh run cancel RUN_ID`. The harness
traps termination, sends TERM to its owned test, sampler, and load process
groups, waits up to 30 seconds, escalates to KILL, reaps its direct children, and
records whether each group is gone. An `always()` workflow step repeats the
recorded process-group cleanup as a second boundary. GitHub then destroys the
fresh VM, which is the final isolation boundary even after a forced runner stop.

There is no runner to deprovision after a run and no recurring idle resource.
For another attempt, dispatch a new run with the same exact subject SHA and
inputs; do not use a retry loop that discards an earlier red result.
