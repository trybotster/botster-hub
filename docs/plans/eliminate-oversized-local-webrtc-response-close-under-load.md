# Eliminate loaded oversized-WebRTC peer disconnect under suite pressure

## Context loaded

- Project Pipelines ticket `ticket_1784651055_336867`, run
  `run_1784651972_646458`, returned Plan step `botster_plan` sequence 3, required
  gate `botster_plan_gate`, no questions, answers, artifacts, or blocking
  dependencies, and Plan Review `review_1784652615_529890`. Its sole open
  finding `finding_1784652615_194898` verified the diagnosis and requested one
  correction: name a concrete deterministic runner-oracle surface because the
  plan's presumed existing shell-test prior art does not exist.
- Ticket provenance: this is the residual owner carved from shutdown-convergence
  ticket `ticket_1784608438_764334`. The captured subject is exact commit
  `16ae5fd6348e79b46cc7302bbb6ba2f77b0b7ec2`, which contains the shutdown
  response-ordering repair and its deterministic ablation.
- Planning authority: [[planner-playbook]], [[botster-planner-playbook]],
  [[botster-architecture]], [[cli-patterns]], [[spa-patterns]], [[test script
  required for rust tests not cargo test]], [[suite wide acceptance criteria
  make every observed test failure in scope]], [[loaded lifecycle ci precompiles
  the exact test target before synthetic cpu stress]], [[a regression test must
  be shown to go red with the fix reverted]], and [[narrow ablation at the
  enforcement point is the cleanest regression negative control]].
- Repository placement authority: the existing artifact at this path and the
  current `docs/plans/` hierarchy. `README.md` does not redirect plans elsewhere.
- Current clean planning worktree: target
  `tgt_7e208a0c76a44980a83b63af976b1f22`, branch
  `project-pipelines/ticket_1784651055_336867`, plan HEAD `480112e` over
  base/main `a0b61235b21824814f86e540912988ef8e3ec932` before this review response.
- Retained full-suite run
  `https://github.com/trybotster/botster-hub/actions/runs/29842263908` checked out
  exact subject `16ae5fd...`, precompiled `hub_daemon_lifecycle_test`, used
  `lifecycle-suite`, default Cargo concurrency, 48 residual-tail CPU workers on
  four CPUs, and stopped first-red on repetition 3. Repetitions 1-2 passed all
  100 active tests. Repetition 3 failed only
  `local_webrtc_chunks_oversized_encrypted_daemon_response` after 31 of 33
  chunks: client `channel_closed`, sender `pressured=true` and
  `peer_disconnected`; suite result 99 passed/1 failed/1 ignored, campaign 101.
- Retained focused run
  `https://github.com/trybotster/botster-hub/actions/runs/29844262812` used the
  same exact SHA and residual-tail profile and passed the unchanged oversized
  test 10/10 with clean teardown. This rules out an intrinsic focused failure
  and makes full-suite concurrency/repetition residue part of the causal path.
- Artifact process samples map durable workers to three concrete tests. Each
  full-suite repetition leaves `botster-session-worker` processes for
  `eof-session`, `notify-socket-session`, and `slow-consumer`; by repetition 3,
  workers from prior repetitions remain reparented to PID 1 in child process
  groups within each recorded test session. Source inspection confirms all
  three tests stop the daemon without first sending `ShutdownSession` for their
  intentionally long-lived session.
- The current runner records and cleans only the outer test process group.
  `cleanup_status=0` therefore does not prove that child process groups in the
  same owned test session are gone. The run artifact contains those survivors
  after the corresponding outer groups were reported `post_clean=gone`.
- Runner-test surface inspection found no existing shell harness: `script/`
  contains only `run-loaded-daemon-lifecycle`; there is no Bats/shUnit test,
  script-test workflow, or sourceable runner entry point. The runner currently
  parses arguments, requires an artifact directory, installs traps, and starts
  work at file scope, so its cleanup functions cannot be exercised directly by
  a deterministic fixture without first separating definition from execution.
- Production path inspected: the oversized lifecycle test starts the real hub
  daemon and botster-web entrypoint, signals a real local WebRTC peer, sends the
  encrypted 300,000-byte response through `run_data_channel` and
  `send_response_frames`, checks ordered bounded chunks and a same-peer
  follow-up, and shuts down through product APIs.

## Scope

1. Make the three named lifecycle tests explicitly shut down the sessions they
   spawn before shutting down their daemon. Preserve every behavioral assertion
   that gives each test meaning; this is teardown completion, not serialization
   or a reduced workload.
2. Strengthen the Linux loaded runner's ownership accounting at the existing
   `setsid` boundary. After each repetition, inspect the explicitly recorded
   test session for surviving descendant process groups, record bounded process
   evidence, clean only those resolved owned groups, and make the repetition
   fail if any survivor existed. A zero cleanup result must mean no process in
   that owned session remains, not merely that its outer group leader exited.
3. Make `script/run-loaded-daemon-lifecycle` sourceable without creating a test
   framework: move its current argument parsing, artifact initialization, traps,
   validation, and campaign orchestration into `main`, retain process functions
   as ordinary shell functions, and call `main "$@"` only when
   `BASH_SOURCE[0] == $0`. Add executable sibling
   `script/run-loaded-daemon-lifecycle-selftest`, which sources the runner and
   uses only its existing Linux tool family (`bash`, `awk`, `ps`, `setsid`, and
   `kill`) to create an explicitly owned session with a nested child process
   group and invoke the real census/cleanup functions. The self-test must cover
   no-survivor green, child-PGID detection, bounded evidence, nonzero leak
   verdict, exact owned cleanup, and idempotent second cleanup. Prove the oracle
   red with the survivor-verdict enforcement narrowly bypassed, then restore
   exactly and prove green.
4. Keep `.github/workflows/loaded-daemon-lifecycle.yml` on the existing exact
   subject checkout, exact-target precompile, default test concurrency,
   residual-tail stress, first-red stop, artifact upload, and always-run cleanup
   path.
5. Run the final committed 40-character SHA through the binding 20-repetition
   `lifecycle-suite`/`residual-tail` campaign. Require all repetitions green and
   require the per-repetition ownership evidence to show no surviving test,
   daemon, entrypoint, session-worker, sampler, or load process.
6. If the same oversized-WebRTC failure recurs after the deterministic leak
   oracle is green, stop at the first red and re-plan from correlated
   client/sender/process evidence. Only that new evidence may authorize a
   production WebRTC change.

## Non-scope

- No retry, timeout increase, fixed sleep, `--test-threads=1`, test
  serialization, reduced residual-tail load, response-size reduction, partial
  chunk acceptance, weakened byte/order/encryption/cleanup assertions, or
  pre-existing-failure waiver.
- No speculative ICE-consent, scheduler, WebRTC flow-control, chunk framing,
  peer registry, daemon, session-worker production, or dependency change. ICE
  consent starvation remains an unproven explanation of why leaked suite
  pressure ends the peer, not an authorized repair target.
- No change to the product guarantee that worker-backed sessions survive daemon
  restart or daemon shutdown. Tests that create live sessions own explicit
  session cleanup; production daemon shutdown must not begin killing durable
  sessions globally.
- No broad process-name kill, `pkill botster-session-worker`, runner-wide sweep,
  or cleanup outside the exact PID/session/process-group identities created and
  recorded by this campaign.
- No Lua/plugin, MCP, TUI, React SPA, Rails relay, docs restructuring,
  dependency update, optional configuration, or adjacent lifecycle cleanup.
- No claim that the historical focused 10/10 run satisfies acceptance. It is a
  diagnostic contrast only; the ticket binds the full default-concurrency
  lifecycle suite for 20 consecutive repetitions.

## Assumptions and unknowns

- Fact, not assumption: three named session-worker families survive each full
  suite repetition because their owner tests omit `ShutdownSession`; artifact
  PIDs, control-socket arguments, sessions/process groups, and source endings
  agree.
- Assumption to test: accumulated test-owned workers are the suite-repetition
  pressure necessary for the observed peer disconnect. Their correlation is
  strong but not yet causal proof. Red-on-revert survivor evidence plus a green
  exact-SHA 20-run full campaign is the required causal bridge.
- Unknown: whether worker thread/process residue alone, its PTY children, or its
  interaction with the 48 CPU workers and parallel lifecycle tests causes ICE
  consent/scheduler loss. This distinction does not justify a production fix
  unless the failure remains after cleanup.
- Assumption: `ShutdownSession` is the existing product primitive for these test
  fixtures and completes the worker cleanup path. The implementation must
  assert its response and use bounded process evidence rather than inserting a
  delay.
- Assumption: on the Linux runner, the `setsid` test leader's session ID is a
  stable ownership boundary even when session workers create child process
  groups. The runner fixture must prove this against the actual `ps` fields used.
- Decision resolving Plan Review: organize session census/cleanup as shell
  functions using universal tools already required by the runner; expose them
  behind a guarded `main` and exercise them from the named framework-free
  sibling self-test. Do not add a Rust artifact manager, Bats/shUnit dependency,
  or generalized process abstraction.
- Worktree/target assumption: implementation remains in this pipeline worktree
  for explicit target `tgt_7e208a0c76a44980a83b63af976b1f22`; loaded CI must
  test the final committed SHA, not ambient branch HEAD.
- Convention conflicts: none. Explicit test-owned cleanup and universal
  process/session accounting preserve production durability and follow the
  smallest surgical path.

## Affected surfaces and files

- `tests/hub_daemon_lifecycle_test.rs`
  - Add explicit `ShutdownSession` plus response assertions for `eof-session`
    and `notify-socket-session` before daemon shutdown.
  - End `slow-consumer` through the existing session-shutdown CLI path and reap
    the stalled attach before daemon shutdown, while preserving its blocked
    stdout and concurrent list/input/resize assertions.
  - Add no shared abstraction unless repetition in the touched teardown makes a
    tiny existing-style helper clearly simpler.
- `script/run-loaded-daemon-lifecycle`
  - Move current file-scope CLI/campaign execution behind a `BASH_SOURCE`
    guarded `main` so process functions are sourceable without side effects;
    preserve every current direct invocation mode and argument.
  - Track the owned test session in addition to the outer process group.
  - Detect, log, and clean resolved survivor process groups after every
    repetition; return nonzero when the test leaked any process even if its test
    assertions passed.
  - Preserve stop-at-first-red and idempotent outer cleanup semantics.
- `script/run-loaded-daemon-lifecycle-selftest`
  - New framework-free Linux executable that sources the guarded runner,
    creates an owned fixture session with a nested child process group, and
    directly proves the real census, verdict, cleanup, and idempotence paths.
- `docs/loaded-daemon-lifecycle-runner.md`
  - Document that cleanup is session-complete and that a detected survivor is a
    failing repetition, including the artifact evidence field.
- `docs/plans/eliminate-oversized-local-webrtc-response-close-under-load.md`
  - This superseding plan and later exact verification ledger.
- Inspection/acceptance only, not expected edits:
  `.github/workflows/loaded-daemon-lifecycle.yml`, `src/local_webrtc.rs`,
  `src/daemon_transport.rs`, and `test.sh`.

Botster layers touched are the Rust real-daemon lifecycle test harness and the
Linux loaded-campaign harness/docs. No production Botster runtime layer is
planned to change.

## Implementation sequence

1. Reconfirm a clean worktree, exact base, and the three leaking source endings.
   From retained artifacts, record one PID/SID/PGID/control-socket row for each
   named family and at least one cross-repetition survivor row.
2. Add explicit session shutdown to each named test at its natural teardown
   boundary. Assert the existing structured response; do not sleep or weaken the
   scenario. Run each exact test separately through `./test.sh` and confirm it
   exits without its named control socket/worker surviving.
3. Move only the runner's file-scope execution into guarded `main`, leaving its
   process operations as sourceable functions. Extend explicit ownership
   record/census, then add the named sibling self-test with a deterministic
   nested-process-group fixture. Detection must happen before cleanup so a leak
   cannot be reported as a green repetition; cleanup must remain idempotent for
   the trap and workflow's outer always-run pass. Run the existing direct
   `--validate-only` and `--cleanup-only` entry points as compatibility checks so
   the main guard cannot silently unwire workflow execution.
4. Perform a narrow negative control: remove only the three explicit session
   shutdowns, or bypass only the runner survivor-verdict decision while leaving
   census and cleanup intact. Run the full relevant filter, require a nonzero
   status with the named survivor evidence, restore exactly, and rerun green.
5. Run focused lifecycle tests, the named runner self-test and direct entrypoint
   compatibility checks, the complete
   default-concurrency lifecycle target, formatting, strict lint, full workspace
   tests, whitespace checks, and a final diff/artifact privacy audit.
6. Commit the implementation and dispatch the unchanged loaded workflow for the
   exact 40-character commit with `test_target=lifecycle-suite`,
   `repetitions=20`, and `stress_profile=residual-tail`.
7. Stop at the first red. Any new full-suite root remains blocking until fixed,
   consumed from a merged owner and rerun, or explicitly re-scoped by a human.
   A repeated oversized-WebRTC disconnect after leak cleanup requires a reviewed
   plan amendment before touching production transport behavior.

## Risks

- **Correlation mistaken for cause:** fixing obvious leaks may not eliminate the
  peer disconnect. Keep the production path unchanged and let the binding
  campaign decide; do not call a local leak test sufficient.
- **Destroying product durability:** changing daemon shutdown to kill session
  workers would violate the production contract. Cleanup belongs in the three
  tests that created the sessions.
- **False-clean runner result:** outer PGID disappearance misses worker child
  groups. Census the recorded SID and fail on any survivor before cleanup.
- **Overbroad cleanup:** process-name matching could kill unrelated agents or
  ambient hubs. Resolve only processes inside the campaign-recorded session and
  record PID/PGID/SID before signalling.
- **Cleanup race or zombie classification:** inspect process state, wait/reap
  where owned, use bounded TERM/KILL behavior already present, and test success,
  already-gone, and child-group cases.
- **Main-guard wiring drift:** moving file-scope execution could make direct
  workflow invocation or `--cleanup-only` a no-op. Keep the current CLI contract
  inside `main` and explicitly test validate, cleanup, invalid-input, and sourced
  no-side-effect behavior.
- **Masking the regression:** automatically cleaning a leaked child while
  returning success would make later repetitions green for the wrong reason.
  Survivor discovery itself must fail the repetition.
- **Changing stalled-attach semantics:** ending the session too early could
  remove the stdout backpressure condition under test. Perform session teardown
  only after list, input, resize, and attach-liveness assertions complete.
- **Suite-wide newly exposed roots:** every first non-cascade failure in the
  promised 20-run campaign remains blocking; no blanket “pre-existing” claim is
  acceptable.

## Acceptance checks and tests

1. Historical evidence ledger preserves both exact-SHA run URLs, target/profile,
   default concurrency, 48-worker load, repetition results, chunk 31/33 client
   and sender causes, and the mapped cross-repetition survivor rows.
2. Focused test-owned cleanup through the repository wrapper:

   ```sh
   ./test.sh --test hub_daemon_lifecycle_test daemon_detaches_subscription_when_attach_connection_drops -- --exact --nocapture
   ./test.sh --test hub_daemon_lifecycle_test daemon_notify_session_defers_without_observed_readiness_over_socket -- --exact --nocapture
   ./test.sh --test hub_daemon_lifecycle_test stalled_attach_stdout_does_not_block_other_daemon_commands -- --exact --nocapture
   ```

   Each must preserve its behavioral assertions, assert session shutdown, and
   leave no named worker/control socket.
3. `script/run-loaded-daemon-lifecycle-selftest` sources the real runner and
   proves: sourced no-side-effect behavior, no-survivor green,
   child-process-group detection inside the recorded SID, bounded evidence,
   nonzero leak verdict, exact owned cleanup, and idempotent second cleanup. It
   must return nonzero on any failed assertion and leave no fixture process.
4. Negative-control evidence records the exact enforcement lines removed or
   bypassed, the valid `./test.sh`/runner command, its nonzero exit status, named
   survivors detected, exact restoration, and the same command green. No
   ablation diff may remain.
5. Local gates:
   - `./test.sh --test hub_daemon_lifecycle_test` at default concurrency;
   - `script/run-loaded-daemon-lifecycle-selftest` on Linux;
   - direct `script/run-loaded-daemon-lifecycle --validate-only ...` valid and
     invalid-input checks plus an empty-artifact `--cleanup-only` check, using
     bounded temporary directories and the current documented arguments;
   - `./test.sh`;
   - `cargo fmt --all -- --check`;
   - strict workspace Clippy using the current `Cargo.toml` lint policy;
   - `git diff --check`, clean status after commit, and final diff review.
6. Runtime proof: dispatch the unchanged workflow against the final exact SHA:

   ```sh
   gh workflow run loaded-daemon-lifecycle.yml \
     --ref <branch-containing-final-commit> \
     -f subject_sha=<final-40-character-sha> \
     -f test_target=lifecycle-suite \
     -F repetitions=20 \
     -f stress_profile=residual-tail
   ```

   Require exact checkout, exact-target precompile, 20/20 suite-green at default
   concurrency, the unchanged oversized response equality/chunk/frame/follow-up
   assertions every repetition, achieved load evidence, no detected owned
   survivor after any repetition, `campaign_exit_status=0`, and
   `cleanup_status=0` with empty active ownership.
7. Audit committed files and retained artifacts for secrets, response payloads,
   usernames, absolute local paths, dead/unwired code, deprecated branches, and
   unrelated cleanup. Every changed line must map to test-owned session teardown,
   truthful owned-process verification, its tests/docs, or this plan ledger.

## Project Pipelines gates and checklists

- Plan artifact: this file, with assumptions explicitly separated from proven
  facts and a conditional re-plan boundary for any post-cleanup recurrence.
- Plan gate evidence: context loaded, bounded scope/non-scope, assumptions and
  unknowns, affected files, risks, acceptance tests, and vault gaps.
- Workflow checklist: current context loaded; repository/runtime and retained
  campaigns inspected; plan attached; gate submitted; advancement requested
  only after evidence submission.
- Vault checklist: notes read; convention conflicts (`none`); planning evidence
  commands and retained run evidence; durable capture disposition.
- Implement artifact must include red/green survivor oracle, exact commands and
  statuses, final diff, final SHA, workflow URL/artifact, 20-run table, achieved
  load, and process-cleanup census.

## Vault gaps worth capturing

- The retained artifact exposes a durable gotcha not yet stated precisely in the
  vault: killing an owned outer process group does not clean child process groups
  that remain inside its `setsid` session, so `cleanup_status=0` can be false
  assurance unless ownership is verified at the session boundary.
- After implementation proves the mechanism and command shape, capture that
  claim through the inbox-first vault pipeline and connect it to [[loaded
  lifecycle ci precompiles the exact test target before synthetic cpu stress]]
  and the existing graceful-cleanup notes.
- If explicit `ShutdownSession` plus truthful SID census makes the exact-SHA
  campaign pass, capture the test convention that real-daemon fixtures creating
  durable sessions must close those sessions explicitly before daemon teardown.
- Do not capture ICE-consent starvation as knowledge unless a post-cleanup red
  provides exact evidence; it is currently only an advisor hypothesis.
