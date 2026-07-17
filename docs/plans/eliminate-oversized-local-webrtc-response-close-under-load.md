# Eliminate local WebRTC response close under load

## Context loaded

- Project Pipelines context loaded for ticket `ticket_1784168176_163113`, run
  `run_1784325316_726684`, active Plan step `botster_plan`, required
  `botster_plan_gate`, the post-PR #145 recurrence artifact, both prior human
  answers, questions, events, and the absence of open findings or dependencies.
- The binding human decisions remain: diagnose a real loaded failure before
  changing production pressure policy; do not add retries, timeout inflation,
  serialization, or weaker assertions; and leave the final suite-wide 20-run
  campaign to the convergence ticket.
- Planning authority loaded: [[identity]], [[goals]], [[planner-playbook]],
  [[botster-planner-playbook]], [[botster-architecture]], [[cli-patterns]],
  [[spa-patterns]], and the Botster planner overlay's required notes. Targeted
  testing constraints also loaded: [[suite wide acceptance criteria make every
  observed test failure in scope]], [[a regression test must be shown to go red
  with the fix reverted]], [[narrow ablation at the enforcement point is the
  cleanest regression negative control]], [[webrtc peer cleanup removes every
  per peer owner together]], [[conformance harnesses gate on deterministic
  invariants not timing]], [[loaded lifecycle ci precompiles the exact test
  target before synthetic cpu stress]], and [[pre existing failure waivers must
  isolate the first non cascade failure on base]]. No convention conflicts were
  found.
- The ticket branch was clean at the merged PR #145 head and was fast-forwarded
  to current `origin/main` commit `319f7ff` before this plan was refreshed.
- GitHub Actions run `29615143681` tested exact subject
  `c84337845165565f7ed059dcb650061ad0f14d76` with the default-parallel
  lifecycle suite, residual-tail load, and 20 requested repetitions. Repetition
  1 stopped at the first red with load average above 60,
  `campaign_exit_status=101`, and `cleanup_status=0`.
- The failure was
  `cli_smoke_proves_local_runtime_daemon_package_app_session_and_webrtc`, whose
  production `botster-hub smoke` process reported
  `local WebRTC data channel closed before response`. The original
  `local_webrtc_chunks_oversized_encrypted_daemon_response` target passed later
  in the same loaded suite, as did its bounded-diagnostic test and peer-close
  cleanup test. The refreshed plan therefore cannot treat the original
  oversized target or the old `pressure_deadline` hypothesis as the reproduced
  cause.
- Production path inspected:
  `local_runtime_smoke` -> `smoke_local_webrtc_round_trip` ->
  `LocalWebrtcOfferPeer::encrypted_request` -> daemon local-WebRTC signaling ->
  `LocalWebrtcHandler::on_data_channel` -> `run_data_channel_with_deadline` ->
  daemon `ControlMessage::Request` -> `framed_daemon_response` ->
  `send_response_frames_with_deadline` -> real ordered
  `DataChannel::send_text`.
- Diagnostic gap inspected: the smoke client drops request identity, message id,
  and chunk progress when its response receiver closes; `spawn_dev_stack_daemon`
  sends daemon stderr to `Stdio::null`; and the PR #145 panic guard wraps only
  the oversized test's directly owned daemon. Run `29615143681` therefore
  cannot distinguish `pressure_deadline`, send failure, channel/peer teardown,
  polling end, framing/request failure, or cleanup initiated elsewhere.

## Scope

Botster layers: Rust hub local-WebRTC data-plane delivery, the production
`botster-hub smoke` client path, the real-daemon lifecycle test harness, and the
existing loaded lifecycle workflow.

1. Strengthen the existing production-path smoke test without replacing its
   body or assertions. Start its daemon through the existing
   `LocalWebrtcDiagnosticDaemon` guard, then run the unchanged `botster-hub
   smoke` command against that reused real daemon. On panic, the guard must
   complete bounded cleanup and surface the daemon's sender terminal record.
2. Make the smoke client's response-close and response-timeout errors identify
   the current daemon request operation and bounded receive progress: message
   id when known, next chunk, expected chunk count, and terminal client cause.
   Do not include request bodies, encrypted payloads, grant secrets, or local
   paths.
3. Extend the existing sender terminal record only as far as needed to classify
   every exit exercised by this smoke path: request operation, message/chunk
   progress, pressure state, latest peer-connection state, channel terminal
   signal, typed cause, and whether peer cleanup was newly sent or already
   complete. Keep one bounded record and the existing exactly-once cleanup
   owner; do not add a diagnostics subsystem or duplicate transport path.
4. Add one selector-only loaded target for the existing
   `cli_smoke_proves_local_runtime_daemon_package_app_session_and_webrtc` test.
   It must reuse the current precompile, residual-tail workers, default test
   semantics, per-run/campaign bounds, stop-at-first-red behavior, artifacts,
   and process-group cleanup. Leave `lifecycle-suite`,
   `focused-oversized-webrtc`, and every other target unchanged.
5. Run that exact smoke target repeatedly under residual-tail load before
   changing local-WebRTC lifecycle policy. A real red must retain both client
   progress and the sender terminal record with `cleanup_status=0`.
6. Repair only the mechanism named by that record, then resynchronize this plan
   if it differs from the branches below:
   - If `pressure_deadline` closes an otherwise live pressured peer, replace
     elapsed-time transport death with an explicit peer-liveness signal. Use
     Tokio's existing `watch` primitive from `LocalWebrtcPeerState`; publish the
     first `Disconnected`, `Failed`, or `Closed` state and select it alongside
     every blocking channel poll. Low water resumes ordered delivery; scheduler
     delay alone does not close a live channel. Remove deadline policy and names
     cold turkey so no dual elapsed/liveness path remains.
   - If the record identifies channel closure, peer teardown, send failure,
     request dispatch/reply failure, framing, or another cause, repair that
     evidenced branch instead and add the smallest deterministic regression at
     its current seam.
   - If the focused target does not reproduce, retain the bounded diagnostics,
     report the unresolved evidence, and make no speculative production
     lifecycle change.
7. Preserve encrypted chunk framing, ordered reliable delivery, FIFO request
   handling, queue bounds, frame/assembly limits, same-peer multi-request use,
   grant cleanup, subscription cleanup, and exactly-once peer cleanup.

Every changed line must either expose the observed smoke failure, repair its
recorded cause, prove the real production path, or document the required
verification.

## Non-scope

- No retries, timeout increases, fixed sleeps, reduced load, test
  serialization, `--test-threads=1` acceptance substitute, or weaker response,
  chunk, frame, terminal-output, or cleanup assertions.
- No assumption that the passing oversized test disproves the smoke recurrence,
  or that the recurrence proves the old pressure hypothesis.
- No response codec, encryption, chunk-size/count, response-cap, DTO,
  TypeScript, browser, SPA, TUI, Lua/plugin, Rails, or conformance-fixture
  changes unless the captured cause directly proves one of those contracts is
  defective and the plan is returned for review.
- No WebRTC dependency change, configurable deadline/watermark, generic sender
  abstraction, background queue, concurrent response task, or adjacent cleanup.
- No loaded-runner framework rewrite and no change to ordinary lifecycle-suite
  acceptance. The new selector exists only to bypass sibling first-reds while
  reproducing this exact production smoke.
- No fixes for independent lifecycle roots. Their sibling tickets and the
  umbrella convergence run retain ownership.

## Assumptions and unknowns

- Assumption: the new recurrence belongs to this ticket because it is the same
  local WebRTC response-close invariant on the real CLI smoke path, and the
  reopening artifact explicitly assigns diagnosis here. It does not authorize
  unrelated smoke or package changes.
- Unknown: which request was active when the channel closed. The smoke sequence
  includes `Status`, `Spawn`, `Attach`, `SendInput`, repeated `Drain`, and
  shutdown; current output does not identify the failing operation.
- Unknown: whether any response chunk arrived, whether sender and client message
  ids agree, and whether the sender ended through pressure, channel/peer state,
  send failure, polling end, request/reply failure, or framing.
- Unknown: whether the focused exact test will recreate the default-parallel
  contention needed for the failure. A focused green is diagnostic evidence,
  not proof that the full lifecycle suite is healthy.
- Assumption: one channel task remains the sole owner of response order, flow
  state, and sends. No fix may introduce concurrent senders or unbounded
  buffering.
- Assumption: if the terminal record proves pressure without a channel or peer
  terminal signal, an ordered reliable channel remains live and low water is
  the delivery-resume condition. A deterministic fake-channel regression and
  the real loaded path must prove that assumption before policy changes.
- Worktree/target: only this pipeline worktree on explicit target
  `tgt_7e208a0c76a44980a83b63af976b1f22` is authorized.

## Affected surfaces and files

- `src/main.rs` — production `botster-hub smoke` request-progress diagnostics;
  no alternate smoke flow.
- `src/local_webrtc.rs` — bounded sender terminal classification and, only
  after a real record, the evidenced lifecycle repair plus deterministic unit
  regression.
- `tests/hub_daemon_lifecycle_test.rs` — reuse the existing panic-safe daemon
  guard around the exact CLI smoke and preserve the existing oversized,
  same-peer, payload, and cleanup assertions.
- `script/run-loaded-daemon-lifecycle`,
  `.github/workflows/loaded-daemon-lifecycle.yml`, and
  `docs/loaded-daemon-lifecycle-runner.md` — one exact smoke selector and its
  operator contract.
- `docs/plans/eliminate-oversized-local-webrtc-response-close-under-load.md` —
  this reviewable handoff and later cause/evidence updates.

Runtime proof must cross the production entry point listed in Context loaded.
Unit tests or the mere presence of diagnostic code do not prove the smoke
process, spawned daemon, real DataChannel, and cleanup path are wired together.

## Risks

- **Wrong root cause:** client `channel_closed` is downstream evidence only.
  Require the sender record before production behavior changes.
- **Diagnostic still disappears:** the smoke command normally starts a detached
  daemon with discarded stderr. Reuse the directly owned diagnostic daemon in
  the test and prove panic-time output and cleanup.
- **Silent non-send exit:** current structured evidence covers response-send
  failures but not every outer-loop break. Classify the actual loop exit without
  inventing duplicate lifecycle ownership.
- **Diagnostic leakage or blockage:** keep one bounded structured line, redact
  paths, and exclude secrets, request bodies, response bodies, and encrypted
  frames.
- **Immortal peer after removing a deadline:** if pressure is the cause, a bare
  `local_poll()` can park after peer death. Carry peer terminal state through a
  race-safe watched value and prove it wakes both active and idle pressure.
- **Buffer growth or reordering:** never bypass high/low-water flow control,
  the bounded pending-request FIFO, or one-response-at-a-time ordering.
- **Cleanup races:** sender, peer callback, smoke cleanup, and test panic may all
  converge. Preserve `cleanup_once`, record whether cleanup was already sent,
  and prove no owned process group survives.
- **False confidence from focused green:** the exact target removes sibling
  test contention. Report achieved load and keep suite-wide convergence
  acceptance separate.
- **Plan drift after diagnosis:** a terminal cause other than the conditional
  pressure design requires this artifact and its acceptance checks to be
  updated before implementation continues.

## Acceptance checks and tests

1. Diagnostic wiring:
   - force the exact CLI smoke to fail after the daemon is running and prove the
     test exits nonzero without aborting;
   - require a bounded client record with request operation and chunk progress;
   - require a bounded sender record with typed cause, peer/channel state,
     pressure/chunk progress, and cleanup disposition, or an explicit bounded
     `unavailable` record;
   - verify daemon, session-worker, package entrypoint, sampler, test, and load
     processes are gone; and
   - scan output for secrets, payload bodies, usernames, and absolute paths.
2. Focused loaded cause gate: dispatch the exact CLI-smoke selector for the
   diagnostics SHA with `residual-tail` and 20 repetitions. On red, preserve
   full subject SHA, request/chunk and sender records, achieved load,
   `campaign_exit_status`, and `cleanup_status=0`. On 20 green repetitions,
   stop after diagnostics and report the unresolved evidence.
3. Deterministic regression after a recorded cause:
   - reproduce the exact terminal state through the current fake
     `LocalWebrtcDataChannel` seam;
   - prove ordering, bounded pending requests, no premature send, task
     termination, channel close, and exactly-once cleanup;
   - if pressure is confirmed, prove delayed low water resumes all chunks while
     elapsed scheduler time alone does not close the peer, and peer
     `Disconnected`/`Failed`/`Closed` wakes active and idle pressure with a
     distinct typed cause;
   - preserve distinct `OnClose`, `OnError`, poll-end, send-failure, invalid
     request, and queue-overflow behavior.
4. Negative control: ablate only the repaired enforcement decision while
   retaining diagnostics and run the full relevant local-WebRTC filter. Record
   the nonzero command, expected red tests, independent green tests, and
   restoration with unchanged `HEAD`.
5. Real-path local checks:
   - run the exact CLI smoke five consecutive times through `./test.sh`;
   - run `local_webrtc_chunks_oversized_encrypted_daemon_response` five
     consecutive times and preserve its 300,000-byte equality, response kind,
     ordered chunks, frame ceiling, same-peer follow-up, grant cleanup, and peer
     cleanup;
   - run adjacent `src/local_webrtc.rs` and peer-close/subscription tests.
6. Repository gates: `cargo fmt --check`, the repository-configured strict
   Clippy command, and `./test.sh`. Record exact commands, SHAs, exit codes, and
   investigate every failure rather than applying a blanket pre-existing-failure
   label.
7. Fixed-SHA loaded proof: after an evidenced repair, rerun the exact focused
   CLI smoke for 20 residual-tail repetitions with unchanged assertions and
   clean teardown. The umbrella convergence ticket still owns renewed
   suite-wide 20-run default-parallel acceptance.
8. Diff/artifact audit: every changed line traces to the ticket, the committed
   plan and PR body contain no personal or absolute worktree paths, and no
   deprecated or unwired code path remains.

## Project Pipelines and vault checklist evidence

- The standard run-scoped vault checklist records notes loaded, no convention
  conflicts, Plan-time repository and Actions evidence, and deferred durable
  capture disposition.
- The Plan workflow checklist records context loading, bounded scope,
  production-entry-point proof, explicit unknown cause, acceptance/negative
  controls, and artifact/gate submission.
- Implement handoff must attach the diagnostic SHA and loaded cause run before
  claiming a production fix. Review and Verify must compare the implemented
  cause with this artifact and reject speculative branches.

## Vault gaps worth capturing

- The recurrence exposes a durable observability gap: a production smoke command
  can own a detached daemon yet discard the daemon-side terminal cause needed
  to diagnose its own failure. Capture a general note only after the bounded
  reused-daemon diagnostic pattern proves useful.
- Existing local-WebRTC notes describe cleanup and chunk flow but do not yet
  state whether peer-connection state must be a wakeable input to a pressured
  DataChannel sender. Capture that rule only if a real failure and regression
  prove it.
- No vault write is justified during Plan. The current evidence is a symptom
  plus an observability gap, not a proven lifecycle decision.
