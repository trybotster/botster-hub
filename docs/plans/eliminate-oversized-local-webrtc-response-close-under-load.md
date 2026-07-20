# Eliminate local WebRTC response close under load

## Context loaded

- Project Pipelines context loaded for ticket `ticket_1784168176_163113`, run
  `run_1784325316_726684`, active Plan step `botster_plan`, required
  `botster_plan_gate`, the post-PR #145 recurrence artifact, both prior human
  answers, questions, events, the three open Plan Review findings, and the
  absence of blocking dependencies.
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
  `LocalWebrtcHandler::on_data_channel` -> `run_data_channel` ->
  daemon `ControlMessage::Request` -> `framed_daemon_response` ->
  `send_response_frames` -> real ordered
  `DataChannel::send_text`.
- Diagnostic gap inspected: the smoke client drops request identity, message id,
  and chunk progress when its response receiver closes; `spawn_dev_stack_daemon`
  sends daemon stderr to `Stdio::null`; and the PR #145 panic guard wraps only
  the oversized test's directly owned daemon. Run `29615143681` therefore
  cannot distinguish `pressure_deadline`, send failure, channel/peer teardown,
  polling end, framing/request failure, or cleanup initiated elsewhere.
- Plan Review `review_1784326164_270373` rejected the first revision's proposal
  to run smoke against a reused test-owned daemon. That would change the
  smoke-owned process tree, startup timing, cleanup ownership, and the existing
  assertion that smoke stops the daemon it started. This revision preserves the
  exact smoke-owned daemon lifecycle and makes the daemon persist its bounded
  terminal record under the explicit data directory.

## Scope

Botster layers: Rust hub local-WebRTC data-plane delivery, the production
`botster-hub smoke` client path, the real-daemon lifecycle test harness, and the
existing loaded lifecycle workflow.

1. Preserve the exact production `botster-hub smoke` ownership path:
   `ensure_dev_stack_daemon` must still start the daemon with the same process
   tree and timing, `SmokeRuntimeCleanup` must still stop that daemon, and the
   existing test must still assert that the smoke-owned daemon is gone. Do not
   substitute a prestarted or reused test-owned daemon.
2. Make the smoke client's response-close and response-timeout errors identify
   the current daemon request operation and bounded receive progress: message
   id when known, next chunk, expected chunk count, and terminal client cause.
   Do not include request bodies, encrypted payloads, grant secrets, or local
   paths.
3. Extend the existing sender terminal record only as far as needed to classify
   every exit exercised by this smoke path: grant id for run correlation,
   request operation, message/chunk progress, pressure state, latest
   peer-connection state, channel terminal signal, typed cause, and whether
   peer cleanup was newly sent or already complete. Keep one fixed-schema,
   size-bounded record and the existing exactly-once cleanup owner.
4. Deliver that record to the existing daemon owner through `ControlMessage`
   before peer cleanup. The owner writes one latest-record JSON artifact to a
   fixed filename under the `HubConfig.data_directory` retained by
   `serve_daemon`, using Rust filesystem primitives and same-directory replace
   semantics. Do not add data-directory state to `LocalWebrtcTransport` or a
   new storage abstraction. The record must survive the smoke-owned daemon's
   shutdown, contain no payloads/secrets/absolute paths, and be correlated to
   the failing smoke grant so stale records cannot satisfy the test. Continue
   the current bounded `eprintln!` as immediate fallback, but do not rely on
   stderr for the loaded evidence.
5. Add one selector-only loaded target for the existing
   `cli_smoke_proves_local_runtime_daemon_package_app_session_and_webrtc` test.
   It must reuse the current precompile, residual-tail workers, default test
   semantics, per-run/campaign bounds, stop-at-first-red behavior, artifacts,
   and process-group cleanup. Leave `lifecycle-suite`,
   `focused-oversized-webrtc`, and every other target unchanged.
6. Run that exact smoke target repeatedly under residual-tail load before
   changing local-WebRTC lifecycle policy. A real red must retain both client
   progress and the matching persisted sender terminal record with
   `cleanup_status=0`. A reproduced red with a missing, stale, malformed, or
   `unavailable` sender record fails the diagnostic gate and must be fixed
   before another production hypothesis or repair proceeds.
7. Repair only the mechanism named by that record, then resynchronize this plan
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
8. Preserve encrypted chunk framing, ordered reliable delivery, FIFO request
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
  abstraction, logging framework, diagnostic database, background queue,
  concurrent response task, or adjacent cleanup. The single fixed data-directory
  artifact is the complete new diagnostic persistence surface.
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
- Assumption: the existing daemon-owner `ControlMessage` queue is the smallest
  ordering boundary for persisting the record before the same sender requests
  peer cleanup. The sender task must not perform blocking filesystem work.
- Assumption: the smoke test's fresh explicit data directory plus matching
  ephemeral grant id distinguishes this attempt's terminal record from stale
  artifacts. A record for another peer or grant is equivalent to unavailable
  evidence.
- Assumption: if the terminal record proves pressure without a channel or peer
  terminal signal, an ordered reliable channel remains live and low water is
  the delivery-resume condition. A deterministic fake-channel regression and
  the real loaded path must prove that assumption before policy changes.
- Worktree/target: only this pipeline worktree on explicit target
  `tgt_7e208a0c76a44980a83b63af976b1f22` is authorized.

## Affected surfaces and files

- `src/main.rs` — production `botster-hub smoke` request-progress diagnostics;
  preserve smoke-owned daemon startup/shutdown and expose the fixed diagnostic
  artifact path to the test without an alternate smoke flow.
- `src/local_webrtc.rs` — bounded sender terminal classification and, only
  after a real record, the evidenced lifecycle repair plus deterministic unit
  regression.
- `src/daemon_transport.rs` — retain the configured data-directory diagnostic
  path in `serve_daemon`, receive the typed terminal record on the existing
  daemon-owner control queue, and persist one bounded latest-record artifact
  before peer cleanup.
- `tests/hub_daemon_lifecycle_test.rs` — read and validate the persisted,
  grant-correlated sender record after the unchanged smoke-owned daemon path
  fails; preserve the daemon-gone assertion plus existing oversized, same-peer,
  payload, and cleanup assertions.
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
- **Diagnostic still disappears:** the smoke command starts a detached daemon
  with discarded stderr. Persist the fixed record through the daemon owner
  before cleanup, then require the smoke test to read the matching artifact
  after the owned daemon stops.
- **Changed reproduction topology:** substituting a reused diagnostic daemon
  would alter the process tree, startup contention, and cleanup ownership that
  failed in run `29615143681`. Keep the smoke-owned spawn/stop path and its
  daemon-gone assertion byte-for-byte in intent; diagnostics must observe that
  path rather than replace it.
- **Stale or partial artifact:** a previous peer record or interrupted write can
  masquerade as this attempt. Correlate by grant id, use a fixed bounded schema
  and same-directory replacement, and treat missing/malformed/mismatched data
  as a failed evidence gate.
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
   - force the exact smoke-owned daemon path to fail after its local WebRTC peer
     is running and prove the test exits nonzero without aborting;
   - prove smoke still starts and stops its own daemon, retains the existing
     post-smoke daemon-gone assertion, and does not attach to a prestarted
     daemon;
   - require a bounded client record with request operation and chunk progress;
   - require a persisted sender record for the same grant with typed cause,
     peer/channel state, pressure/chunk progress, and cleanup disposition;
   - prove fixed schema/size limits, same-directory replacement, stale-grant
     rejection, malformed/truncated-record rejection, and path/payload/secret
     exclusion;
   - treat a reproduced red with an absent, mismatched, malformed, or
     `unavailable` record as a failing diagnostic gate, never successful
     evidence;
   - verify daemon, session-worker, package entrypoint, sampler, test, and load
     processes are gone; and
   - scan output for secrets, payload bodies, usernames, and absolute paths.
2. Focused loaded cause gate: dispatch the exact CLI-smoke selector for the
   diagnostics SHA with `residual-tail` and 20 repetitions. On red, preserve
   full subject SHA, request/chunk record, matching persisted sender record,
   achieved load, `campaign_exit_status`, and `cleanup_status=0`. If a red lacks
   the record, stop and repair the diagnostic path before another loaded
   campaign. On 20 green repetitions, stop after diagnostics and report the
   unresolved evidence.
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
  data-directory terminal artifact proves useful without changing daemon
  ownership.
- Existing local-WebRTC notes describe cleanup and chunk flow but do not yet
  state whether peer-connection state must be a wakeable input to a pressured
  DataChannel sender. Capture that rule only if a real failure and regression
  prove it.
- No vault write is justified during Plan. The current evidence is a symptom
  plus an observability gap, not a proven lifecycle decision.

## Implement evidence

- Diagnostic implementation SHA:
  `44f878d291bdfa0dca32880da3c58ef49a994b73`.
- Fixed-SHA loaded cause gate:
  [GitHub Actions run 29619164321](https://github.com/trybotster/botster-hub/actions/runs/29619164321)
  ran `focused-cli-smoke` for 20 repetitions with the `residual-tail` profile
  and 48 load workers. All 20 repetitions exited 0, the campaign exited 0,
  owned-process cleanup exited 0, and the complete diagnostics artifact was
  uploaded.
- The reported close did not recur in the approved cause gate. Per the
  diagnostic-first decision rule, that Implement pass stopped with the bounded,
  grant-correlated terminal evidence path retained and made no speculative
  lifecycle change.
- Review hardening SHA
  `d97f566dd4127e5bfe56f95b7ced2ae0e83f3c36` removed diagnostic-path
  masking and instrumentation overhead. A subsequent fixed-SHA
  `focused-cli-smoke` residual-tail campaign
  [GitHub Actions run 29665295517](https://github.com/trybotster/botster-hub/actions/runs/29665295517)
  reproduced on repetition 1 with `campaign_exit_status=101` and
  `cleanup_status=0`. The client stopped at chunk 62 of 85 with
  `channel_closed`; its matching grant-correlated sender record named
  `pressure_deadline`, peer state `connected`, pressure active, last sent chunk
  61, and no channel terminal signal. This enters the approved
  peer-liveness-repair branch.
- The repair removes the five-second delivery deadline, its state, terminal
  cause, and deadline-named functions. `LocalWebrtcPeerState` now publishes
  only the first peer `Disconnected`, `Failed`, or `Closed` state through a
  Tokio watch channel. Both the idle request loop and active pressured response
  loop select blocking DataChannel polls against that signal. Low water resumes
  FIFO ordered delivery; scheduler delay alone has no transport-death path.
- Deterministic fault injection on the unchanged smoke-owned daemon topology
  proved a matching sender record is persisted and consumed before the
  existing daemon-gone assertion. Healthy exact smoke and oversized-response
  targets each passed five consecutive local repetitions.
- The full repository suite passed with 266 compiled tests plus one doctest
  green and the one documented larger local adversarial test ignored.
  Formatting, strict Clippy, local
  WebRTC unit tests, record validation, peer-close cleanup, and diagnostic
  redaction checks also passed.
- Final fixed-SHA loaded proof, negative-control evidence, and final repository
  verification are recorded in the returned Implement report after completion.
- The loaded evidence establishes the durable lifecycle rule that elapsed
  scheduler time is not peer liveness: pressured senders require a wakeable
  terminal peer-state input.
