# Deterministic metadata-owned daemon shutdown implementation report

## Target and routing

- Ticket: `ticket_1785470554_126900`.
- Run: `run_1785475712_806874`.
- Target repository: `trybotster/botster-hub`.
- Target ID: `tgt_7e208a0c76a44980a83b63af976b1f22`.
- The spawn-target registry, approved plan, and run worktree all resolve to the
  same repository. Work remained inside this run worktree.
- Instrumented pre-fix commit:
  `3638fdc5c83991b0c4f3cb974683c20cc3fd558c`. Its production
  `src/main.rs` is unchanged from authoritative base
  `b1bca77a16c36276ffba6ea726b54ae0664e905b`.

## Playbooks and notes applied

- Role playbooks: [[implementer-playbook]] and
  [[botster-implementer-playbook]].
- Repository charter: [[botster-hub-playbook]].
- Workflow policy: [[project-pipelines-playbook]]. It constrained committed
  work, PR linkage, report persistence, checklists, and gate evidence only; no
  Project Pipelines package/plugin source is changed.
- Architecture maps: [[botster-architecture]], [[cli-patterns]], and
  [[spa-patterns]]; the SPA map supplied generic Botster context but no SPA
  surface changed.
- Lifecycle and verification notes:
  [[daemon shutdown disconnects count as success only after clean owned process exit]],
  [[worker shutdown completion requires lifecycle transport and process termination]],
  [[subprocess harnesses must kill child on failed readiness]],
  [[bounded command execution requires process group termination and reaping]],
  [[daemon probe order changes require lifecycle integration tests]],
  [[poisoned rust mutex test locks cascade one failure across parallel suite]],
  [[a regression test must be shown to go red with the fix reverted]],
  [[test script required for rust tests not cargo test]],
  [[rust repo strict lints must be verified before dismissing warnings]],
  [[external client hub tests use subprocess spawned hub test support]],
  [[workflow cancellation cleanup is idempotent across campaign traps and outer steps]],
  [[sid scoped census is blind to setsid session leaks]], and
  [[loaded lifecycle ci precompiles the exact test target before synthetic cpu stress]].
- Hub ownership notes required by the charter:
  [[botster hub is a first party host profile over core]],
  [[botster hub gravity must be watched before it becomes the new monolith]],
  [[botster data plane bypasses the hub through session and client actors]],
  [[botster local client api lives over hubruntime not raw core routers]],
  [[botster hub events use bounded priority lanes instead of unbounded queue fuses]],
  [[may supervise permits the hub to supervise the package entrypoint]],
  [[hub supervision admission changes require exact live hub launch proof]],
  [[live hub proof records distinct hub and locked core binary provenance]],
  [[webrtc bootstrap origin must be requested after the package server binds]],
  [[plugin worker queue capacity and executor concurrency are independent host profile knobs]],
  and
  [[durable state version preflight must precede shape deserialization after cold turkey changes]].

No convention conflict was found.

## Implementation and production path

`operator_shutdown` and `local_runtime_down` still resolve the verified
metadata-owned PID, receive the existing daemon response, and call
`complete_owned_runtime_daemon_shutdown`. That helper now calls
`wait_for_owned_runtime_daemon_reaped`, which succeeds only after the PID is
absent or after this process, when it is the daemon's actual parent, reaps the
exited child with `waitpid(WNOHANG)`. It then removes the socket and metadata in
the existing order.

The parent-side reap is required by the operator-console topology. The first
adjacent run exposed that the long-lived console is the daemon parent; an
absence-only process-table poll from that same blocked parent timed out after
ten seconds because nobody could reap its zombie. Non-parent `shutdown`
processes receive `ECHILD` and continue waiting for PID absence from PID 1 or
the applicable subreaper. The ten-second budget and 50 ms cadence are
unchanged.

`recover_owned_stale_runtime_daemon` and `StartedRuntimeCleanup::drop` remain on
the original `wait_for_runtime_daemon_exit` predicate, including its
`None | Z` socket-release success semantics. No boolean mode or compatibility
path was added.

## Characterization, red, green, and ablation

- Controlled pre-fix regression: daemon PID `98737`, expected reaper/test PID
  `98186`, PGID `98737`, state `Z`, command `<defunct>`. Shutdown exited 0 after
  122 ms, printed `response=shutdown`, removed metadata/socket, and returned
  while the PID still existed. The wrapper exited 101 as required.
- Fixed controlled regression: the same real-daemon topology held shutdown
  pending throughout the unreaped zombie boundary. Local runs observed
  exit-to-reap intervals from about 0.54 to 0.56 seconds; shutdown returned only
  after the test parent reaped the child, then PID, metadata, and socket were
  absent.
- Ordinary production topology: the daemon's pre-shutdown PPID was `1`, and
  shutdown returned with the PID absent in about 70-75 ms on the local macOS
  host.
- Narrow ablation: restoring only `Z => Ok(())` in the new owned predicate made
  the exact regression exit 101 after shutdown returned 0 in 94 ms with the
  daemon still `Z` and metadata/socket already removed. Restoring absence-only
  behavior returned the same test to green.

## Loaded runner behavior

- Added `focused-metadata-owned-shutdown`, which runs the exact regression
  through `./test.sh`, `--exact`, `--nocapture`, and default Cargo concurrency.
- Preserved zombie exclusion in `group_is_alive`,
  `direct_child_is_running`, `session_process_rows`, and
  `run_token_process_rows`.
- Added a separate observation-only five-second settled zombie delta. It
  combines newly appearing Hub/session-worker/fixture-shell role zombies
  against a pre-repetition baseline with zombies in the recorded test SID.
  Remaining rows fail the repetition as `zombie_survivors` but never enter
  TERM/KILL loops or set cleanup machinery status.
- Extended the existing Linux runner self-test with a real held zombie. The
  fixture must fail the settled evidence gate, remain non-live to group and
  direct-child predicates, and disappear after its owning parent exits.
- Exact branch/pre-fix loaded workflow evidence is pending remote dispatch and
  will be appended before Review.

## Files changed

- `src/main.rs`
- `tests/hub_daemon_lifecycle_test.rs`
- `script/run-loaded-daemon-lifecycle`
- `script/run-loaded-daemon-lifecycle-selftest`
- `.github/workflows/loaded-daemon-lifecycle.yml`
- `docs/loaded-daemon-lifecycle-runner.md`
- `README.md`
- This report.

## Ownership boundaries and cross-repository work

The change stays in Hub-owned local control-plane lifecycle, repository-owned
integration tests, and Hub CI/documentation. No Core mechanism, hub-client DTO,
package/plugin workflow, Web, TUI, TUI-kit, or Ghostty code changed. There are
no cross-repository dependencies and no separately routed implementation work.

## Deviations from the approved plan

- `script/run-loaded-daemon-lifecycle-selftest` is changed because the approved
  plan requires a runner zombie self-check; the affected-files list named the
  main runner but not its existing dedicated self-test.
- The owned wait also reaps the daemon when the current CLI process is its
  parent. This was not explicit in the plan because retained evidence described
  PID 1/subreaper adoption. The required operator-console acceptance test
  deterministically exposed the live non-reaping parent topology. The extension
  preserves the plan's PID-absence completion state and avoids the forbidden
  timeout increase or supervisor.
- The Project Pipelines playbook was loaded for implementation workflow policy
  because the Implement role requires durable checklists, report artifact, PR
  linkage, and gate submission. No plugin product scope was added.

## Verification completed

- Deterministic pre-fix red and fixed green.
- Narrow zombie-as-success ablation red, then restored green.
- Exact ordinary metadata-owned shutdown test.
- Startup/reuse/down and restart/adoption/worker cleanup tests.
- Operator-console lifecycle test.
- Four stale/recovery tests.
- Failed-start unwind test without a new ten-second delay.
- Four daemon response-delivery unit tests.
- Default-parallel `hub_daemon_lifecycle_test`: 118 passed, 1 ignored.
- `./test.sh --workspace --no-run`: passed.
- `./test.sh --workspace`: passed.
- `cargo fmt --all -- --check`: passed.
- `cargo clippy --workspace --all-targets --all-features -- -D warnings`:
  passed.
- `git diff --check`: passed.
- Runner Bash syntax and focused selector validation: passed locally.

## Unverified behavior and residual risk

- Linux-only real-zombie runner self-test and the required residual-tail loaded
  campaigns are pending GitHub Actions execution.
- Local proof observed PID 1 as the production orphan reaper and bounded total
  shutdown completion, but the zombie interval was shorter than the 20 ms
  process-snapshot sampling resolution in that topology. The controlled-parent
  regression supplies the exact exit-to-reap interval and predicate proof.
- The settled cross-session role census depends on Linux `ps` retaining the
  executable role in zombie `comm`/args; recorded-SID evidence independently
  covers the campaign's own session.

## Missing vault guidance discovered

Existing ownership notes say a non-parent client cannot reap another process,
but do not record the complementary operator-console rule: when the CLI process
that is waiting for metadata-owned shutdown is also the daemon's parent, it
must reap that exact exited child or its own wait prevents PID absence.

This was not written directly to the vault from the repository run because the
vault is a separately owned target. It should be captured through the vault
pipeline as a Botster lifecycle gotcha after this implementation is reviewed.

