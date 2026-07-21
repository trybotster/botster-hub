# Loaded daemon lifecycle runner

The **Loaded daemon lifecycle diagnostics** GitHub Actions workflow provides the
isolated, on-demand runner for the real-daemon lifecycle tail. Each dispatch gets
a fresh GitHub-hosted `ubuntu-24.04` VM; it does not use a developer machine,
production system, shared self-hosted runner, secret, or permanent resource.

The workflow checks out its harness and the selected subject into separate
directories. The required subject must be a full commit SHA in this repository.
For this investigation, use diagnostics commit
`1c4af771d6ed9c09b4b6e0e6f1f8b0c906c79895` for
`stalled_attach_stdout_does_not_block_other_daemon_commands`.

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
  -f subject_sha=1c4af771d6ed9c09b4b6e0e6f1f8b0c906c79895 \
  -f test_target=lifecycle-suite \
  -F repetitions=20 \
  -f stress_profile=residual-tail
```

Use `focused-stalled-attach` with one repetition for compilation and diagnostic
triage. A focused run does not satisfy the required 20-or-more default-parallel
lifecycle-suite campaign. The runner stops at the first red repetition so the
failure remains easy to locate and no later retry can hide it.

Use `focused-oversized-webrtc` to repeat the existing
`local_webrtc_chunks_oversized_encrypted_daemon_response` test under the selected
stress profile without changing its body, assertions, deadlines, or cleanup.
This target exists only to capture the bounded sender terminal record when
unrelated lifecycle-suite failures would otherwise stop the campaign first. It
does not replace full-suite acceptance.

Use `focused-cli-smoke` to repeat the exact
`cli_smoke_proves_local_runtime_daemon_package_app_session_and_webrtc` test
under the selected stress profile. This keeps the smoke-owned daemon
start/stop path and its daemon-gone assertion while bypassing unrelated
first-red lifecycle tests. On a red, the test requires bounded client progress
and the matching fixed-schema sender terminal record from the smoke data
directory; missing, stale, malformed, mismatched, or oversized evidence remains
a test failure. This selector is diagnostic evidence only and does not replace
the default-parallel lifecycle-suite campaign.

Use `focused-lua-worker-suite` to run the unchanged, default-parallel
`hub_lua_runtime_test` binary while preserving the first control-socket failure.
For the Lua worker investigation, the fixed diagnostic budget is 20 repetitions
with `residual-tail`. The runner stops on the first red; save that artifact before
any rerun. If all 20 repetitions pass, report the defect as not reproduced under
that exact budget, not resolved, and move reproduction to whole-suite contention
before proposing a lifecycle repair.

Use `focused-lua-session-worker-callers` only after a distinct first red in the
full Lua binary has been preserved and durably assigned elsewhere. It runs the
existing `session_template` test subset at default parallelism so control-socket
reproduction can continue without discarding the unrelated first-red artifact.
This selector is diagnostic-only: it does not satisfy the required full
`focused-lua-worker-suite` campaign.

Use `full-suite-contention` only after the 20-repetition
`focused-lua-worker-suite` campaign completes without a control-socket failure.
It runs the ordinary, unchanged, default-concurrency `./test.sh` command under
the selected stress profile, preserving the original contention topology. The
bounded exhaustion budget is five repetitions with `residual-tail`. Five green
repetitions mean the failure was not reproduced under that exact budget; they do
not establish a deterministic regression or prove the defect resolved.

## Bounded stress and time budgets

The profiles start only job-local CPU workers:

- `residual-tail`: 12 workers per detected CPU, capped at 64 workers.
- `moderate`: 2 workers per detected CPU, capped at 16 workers.
- `none`: no synthetic load worker.

The label describes requested workers, not achieved contention. Use the recorded
`resource-samples.log` (`/proc/loadavg`, CPU, memory, and process samples every
five seconds) when comparing a run with the residual tail.

The workflow precompiles the exact selected test binary before starting those
workers. The bounded run deadline therefore measures test execution under load,
not a fresh dependency build competing with the load generators.

GitHub Actions run `29439289277` proved that
`botster-terminal-ghostty`'s `libghostty-vt` build requires Zig `0.15.2`.

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
- `precompile.log`: output from compiling the exact lifecycle test binary before
  synthetic load starts, so the per-run deadline measures loaded test execution.
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
