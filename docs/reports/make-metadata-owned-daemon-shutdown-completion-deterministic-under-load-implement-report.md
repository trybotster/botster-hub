# Deterministic metadata-owned daemon shutdown implementation report

## Target and routing

- Ticket: `ticket_1785470554_126900`.
- Run: `run_1785475712_806874`.
- Target repository: `trybotster/botster-hub`.
- Target ID: `tgt_7e208a0c76a44980a83b63af976b1f22`.
- Implementation SHA:
  `7c9cd67bc98f3ea562544291980c00a56b0a93a4`.
- Authoritative base SHA:
  `b1bca77a16c36276ffba6ea726b54ae0664e905b`.
- Pull request:
  [#184](https://github.com/trybotster/botster-hub/pull/184).

The spawn-target registry, approved plan, branch, and run worktree all resolve
to `trybotster/botster-hub`. Work remained inside the routed run worktree.

## Playbooks and notes applied

- [[implementer-playbook]]
- [[botster-implementer-playbook]]
- [[botster-hub-playbook]]
- [[project-pipelines-playbook]] for workflow discipline only; no Project
  Pipelines package/plugin product source changed.
- `/Users/jasonconigliari/knowledge/self/identity.md`
- `/Users/jasonconigliari/knowledge/self/goals.md`
- [[daemon shutdown disconnects count as success only after clean owned process exit]]
- [[worker shutdown completion requires lifecycle transport and process termination]]
- [[bounded command execution requires process group termination and reaping]]
- [[a regression test must be shown to go red with the fix reverted]]
- [[test script required for rust tests not cargo test]]
- [[rust repo strict lints must be verified before dismissing warnings]]
- [[sid scoped census is blind to setsid session leaks]]
- [[empty gate output is not success without a valid exit status]]
- [[self asserting artifacts are not reviewer evidence]]
- [[pre existing failure waivers must isolate the first non cascade failure on base]]
- [[suite wide acceptance criteria make every observed test failure in scope]]

The Hub ownership notes and architecture maps loaded in the original Implement
step continued to apply. No convention conflict was found.

## Implementation and production path

`operator_shutdown` and `local_runtime_down` still resolve verified
metadata-owned runtime state, receive the daemon response, and call
`complete_owned_runtime_daemon_shutdown`. Completion still requires PID
absence before socket and metadata removal.

Two parent topologies now complete deterministically:

1. When the command performing shutdown is the exact daemon parent,
   `reap_owned_child_if_exited` uses `waitpid(WNOHANG)`. A zero exit is accepted;
   a signal or nonzero exit returns
   `LocalRuntimeError::OwnedDaemonExitedAbnormally`.
2. When a long-lived operator console started the daemon and a separate CLI
   performs shutdown, the console owns a background `Child::wait()` thread.
   The console remains usable while that thread reaps the daemon, allowing the
   non-parent shutdown CLI's PID-absence wait to finish.

Short-lived `up` behavior remains unchanged in effect: its process exits after
readiness and the daemon is adopted normally. The ten-second shutdown budget
and 50 ms polling cadence were not increased.

The prior full-suite red also exposed a real diagnostic-finalization race in
the repository-owned generic web-port fixture. `EntrypointSupervisor` could
observe process exit before its stderr reader published `EADDRINUSE`, snapshot
an empty diagnostic, and fail the structured-readiness assertion under load.
`wait_for_launch_result` now waits for the existing pending terminal-state
finalization window before creating `ReadinessFailed`. No new timeout or retry
was added.

## Controlled red/green proof

- Original pre-fix shutdown characterization used instrumented commit
  `3638fdc5c83991b0c4f3cb974683c20cc3fd558c`, whose production `src/main.rs`
  matched authoritative base. Shutdown returned exit 0 while the held daemon
  remained `Z`; the regression wrapper returned 101.
- Restoring zombie-as-success in the owned completion predicate made the exact
  metadata-owned regression red again; restoring the fix returned it green.
- Removing only `reap_local_runtime_daemon_on_exit(child)` made
  `cli_shutdown_reaps_metadata_owned_daemon_started_by_live_operator_console`
  receive the shutdown response, wait approximately ten seconds, then fail
  with `TerminateDaemonTimeout`. Restoring the waiter made the production
  topology complete in under five seconds with the console still alive and
  PID, socket, and metadata absent.
- Restoring the entrypoint supervisor's immediate-exit snapshot made
  `readiness_failure_waits_for_delayed_exit_diagnostics` red because the
  delayed diagnostic was absent. Restoring pending-terminal-state finalization
  made it green.

## Returned Review findings addressed

- The role-zombie awk condition is portable single-line syntax.
- Every zombie scanner stage is checked. Scanner failure returns status 2 and
  is logged as `zombie_census=error`; empty output is clean only after status 0.
- The baseline census runs before test ownership is recorded and fails the
  repetition loudly on scanner error.
- The Linux self-test injects a scanner failure and requires status 2.
- The Linux self-test creates real cross-session `botster-hub` and
  `botster-session` zombies, proves `new-role` evidence, subtracts a nonempty
  pre-existing baseline, checks the nonzero survivor status, and verifies
  disappearance after parent exit.
- The role matcher contains only real process names. Linux's truncated
  session-worker comm is matched as `botster-session`; the imaginary
  `fixture-shell` role was removed.
- `python3` is required by the self-test where it is used, not by the campaign
  validator.
- The Darwin session-pointer caveat is restored next to `all_process_rows`;
  numeric session zombie assertions remain Linux-only.
- A live operator-console production topology is tested and reaped.
- Direct-child abnormal exit status is rejected and documented.
- The occupied web-port first root was isolated on branch and base with the
  same command/stress, and the diagnostic race was fixed before rerunning the
  complete suite.

The earlier runs 30609814800, 30610014273, 30609817554, and 30611916117 remain
historical characterization only. Their role-zombie scans hit an awk parse
error whose exit status was discarded, so none is cited as valid zombie
acceptance evidence after Review.

## Corrected Linux workflow evidence

All corrected runs use the workflow harness from implementation SHA
`7c9cd67bc98f3ea562544291980c00a56b0a93a4`, Ubuntu 24.04, default Cargo test
parallelism, four CPUs, 48 residual-tail stress workers, and the locked Core
revision resolved by the repository. Subject checkouts are the implementation
SHA except where an authoritative-base comparison is named explicitly.

- Focused metadata-owned shutdown:
  [30617403730](https://github.com/trybotster/botster-hub/actions/runs/30617403730).
  Both the held-child regression and live-console production topology passed
  20/20 repetitions. Raw campaign statuses are all 0, survivor TSVs contain no
  data rows, active PGID/SID/run-token ledgers are empty, and cleanup is 0.
- Exact occupied-port branch isolation:
  [30617479869](https://github.com/trybotster/botster-hub/actions/runs/30617479869).
  Three repetitions passed with raw exit codes `0, 0, 0`, zero survivor data
  rows, empty active ledgers, and cleanup 0.
- Exact occupied-port authoritative-base isolation:
  [30617992426](https://github.com/trybotster/botster-hub/actions/runs/30617992426).
  The same command and stress against
  `b1bca77a16c36276ffba6ea726b54ae0664e905b` passed with raw exit codes
  `0, 0, 0`, zero survivor data rows, empty active ledgers, and cleanup 0.
- Branch full-suite contention:
  [30618193380](https://github.com/trybotster/botster-hub/actions/runs/30618193380).
  Five complete repetitions passed with raw exit codes `0, 0, 0, 0, 0`.
  Each lifecycle binary reported 119 passed, 0 failed, and 1 documented
  ignored test. Both metadata-owned tests and the occupied-port test passed
  5/5. Survivor TSVs contain zero data rows, active ownership ledgers are
  empty, and cleanup is 0.
- Authoritative-base full-suite contention:
  [30618488667](https://github.com/trybotster/botster-hub/actions/runs/30618488667).
  Five complete repetitions against
  `b1bca77a16c36276ffba6ea726b54ae0664e905b` passed with raw exit codes
  `0, 0, 0, 0, 0`. Each lifecycle binary reported 117 passed, 0 failed, and 1
  documented ignored test; the ordinary metadata-owned cleanup test and
  occupied-port test passed 5/5. Survivor TSVs contain zero data rows, active
  ownership ledgers are empty, and cleanup is 0.

The corrected Linux self-test passed in every run. Its log contains the
deliberately injected `zombie_census=error`, immediately followed by
`ok - zombie census fails closed when its process scanner fails`; it then
reports the new cross-session role zombie, excludes the pre-existing baseline,
and verifies both zombies disappear after their parents exit. No awk parse
error appears.

## Files changed

- `.github/workflows/loaded-daemon-lifecycle.yml`
- `README.md`
- `docs/loaded-daemon-lifecycle-runner.md`
- `docs/reports/make-metadata-owned-daemon-shutdown-completion-deterministic-under-load-implement-report.md`
- `script/run-loaded-daemon-lifecycle`
- `script/run-loaded-daemon-lifecycle-selftest`
- `src/entrypoint_supervisor.rs`
- `src/main.rs`
- `tests/hub_daemon_lifecycle_test.rs`

## Ownership boundaries and cross-repository work

The change stays in Hub-owned local control-plane lifecycle, Hub entrypoint
supervision, repository-owned integration tests/CI, and Hub documentation. No
Core mechanism, hub-client DTO, package/plugin product workflow, Web, TUI,
TUI-kit, or Ghostty source changed.

There are no cross-repository dependencies and no separately routed
implementation tickets.

## Deviations from the approved plan

- The existing loaded-runner self-test changed because the approved zombie
  acceptance gate required an executable positive control.
- The operator-console parent waiter was added after the required
  production-shaped topology proved that a live parent otherwise preserves the
  daemon zombie and blocks a non-parent shutdown CLI.
- `src/entrypoint_supervisor.rs` changed because suite-wide acceptance exposed
  a load-dependent first-root diagnostic race. The change uses the supervisor's
  existing terminal-output finalization mechanism and was required to make the
  binding full-suite criterion green.
- A `focused-occupied-web-port` selector was added to provide the exact
  branch/base isolation requested by Review.

No acceptance criterion was narrowed or waived.

## Local verification

- `cargo fmt --all -- --check`: passed.
- `cargo clippy --workspace --all-targets --all-features -- -D warnings`:
  passed.
- `git diff --check`: passed.
- `bash -n script/run-loaded-daemon-lifecycle
  script/run-loaded-daemon-lifecycle-selftest`: passed.
- `./test.sh --workspace --no-run`: passed.
- `./test.sh --workspace`: passed.
- Default-parallel `hub_daemon_lifecycle_test`: 119 passed, 0 failed, 1
  documented ignored test.
- `./test.sh --test hub_daemon_lifecycle_test metadata_owned_daemon --
  --nocapture`: 2 passed.
- `readiness_failure_waits_for_delayed_exit_diagnostics`: passed.
- `owned_runtime_reap_rejects_abnormal_direct_child_exit`: passed.
- Existing operator-console and ordinary metadata-owned shutdown paths:
  passed.

Two lifecycle test binaries were once launched independently in parallel during
diagnosis and contended for repository-global fixtures. Those contradictory
outputs were discarded. Serial targeted reruns and the repository's complete
default wrapper are the cited local evidence.

## Assumptions, unverified behavior, and residual risk

- A non-parent shutdown client cannot retrieve another process's wait status.
  Its success therefore combines a successful daemon shutdown response with
  independently observed PID absence. An exact parent additionally requires
  clean wait status.
- The console's background waiter intentionally discards status because the
  external shutdown command is the success authority in that topology; its PID
  absence wait and daemon response remain required.
- Local Darwin cannot execute the Linux `setsid`/numeric-SID positive control.
  The corrected GitHub Ubuntu self-test is the binding proof.
- The setup-zig action emits a GitHub Node.js deprecation annotation. It does
  not affect the campaign result and is outside this ticket's lifecycle scope.
- Final residual risk is limited to behaviors outside the exercised
  metadata-owned shutdown, entrypoint readiness, and repository-wide test
  surfaces. No known ticket behavior remains unverified once the final base
  campaign is inspected.

## Missing vault guidance discovered

Existing notes correctly require clean owned-process exit and explain that a
non-parent cannot reap another process. They do not explicitly record the
complementary long-lived-owner rule: a console that starts a metadata-owned
daemon must keep a waiter for that exact child so an external shutdown can
observe PID absence while the console remains alive.

No vault file was changed from this repository-owned run. If that rule recurs,
capture it through the separately owned vault workflow. No other missing vault
guidance was discovered.
