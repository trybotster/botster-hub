# Verify the oversized local WebRTC repair under load

## Context loaded

- Project Pipelines ticket `ticket_1784168176_163113`, run
  `run_1784596374_444453`, active Plan step `botster_plan`, required
  `botster_plan_gate`, prior artifacts, questions, answers, and the absence of
  open findings or blocking dependencies.
- Planning authority: [[identity]], [[goals]], [[planner-playbook]],
  [[botster-planner-playbook]], [[botster-architecture]], [[cli-patterns]],
  [[spa-patterns]], [[loaded lifecycle ci precompiles the exact test target
  before synthetic cpu stress]], [[webrtc peer registry owns production data
  plane receivers]], and [[webrtc peer cleanup removes every per peer owner
  together]].
- Repository placement authority: the existing plan at this path and current
  `docs/plans/` prior art. No repository README redirects this plan elsewhere.
- Current branch was clean and fast-forwarded to merged main
  `afbf412c5db9ef3538155ca6fec6637f4cb1726c`, a descendant of PR #148 merge
  `2cd58291e0da56dc6ac190fc48ccd665f96b7332` and repair commit
  `2b4ffe0872563f2335cca89c90679c66c13304cc`.
- The ticket's claimed second post-merge recurrence is invalid attribution.
  Failing subject `b95b038c0324e47de6f268a19cbe020a6e2a3d4c`
  has merge-base `319f7ffd76740a521f7ddb0883af945079aaf746` with
  `2b4ffe0`, still contains the removed `PressureDeadline` branch, and is not a
  descendant of PR #148. Its `pressure_deadline` failure therefore cannot
  establish recurrence of the repaired code.
- Human answer `question_1784596936_977126` directs a verification-first plan:
  pin the existing focused oversized-WebRTC residual-tail campaign to a true
  PR #148 descendant, preserve current assertions and diagnostics, and make no
  code change unless that repaired path actually recurs.
- Production path inspected:
  `local_webrtc_chunks_oversized_encrypted_daemon_response` starts the real
  daemon and botster-web entrypoint, signals a real local WebRTC peer, and sends
  an encrypted oversized daemon request through `run_data_channel` and
  `send_response_frames`. The repaired sender waits for low water while
  selecting the Tokio peer-terminal watch; elapsed scheduler time no longer
  closes an otherwise live pressured channel.
- Existing loaded entry point inspected: workflow dispatch
  `.github/workflows/loaded-daemon-lifecycle.yml` selects
  `focused-oversized-webrtc`; `script/run-loaded-daemon-lifecycle` precompiles
  and repeatedly runs the exact lifecycle test under `residual-tail`, stops at
  the first red, and records bounded run and cleanup evidence.

## Scope

Botster layers touched are the Project Pipelines plan artifact and the existing
Rust hub lifecycle verification path. The first implementation action is
evidence collection, not source modification.

1. Record the exact true-descendant subject SHA. Use
   `afbf412c5db9ef3538155ca6fec6637f4cb1726c` unless Implement advances to a
   later main commit and records both its full SHA and ancestry proof against
   `2cd5829`.
2. Dispatch the existing `focused-oversized-webrtc` target for 20 repetitions
   with `stress_profile=residual-tail`. Do not change the workflow, runner,
   selector, test body, assertions, deadlines, or cleanup before this campaign.
3. Preserve the complete campaign evidence: requested and resolved subject
   SHAs, ancestry check, exact command, precompile result, each repetition log,
   resource samples, chunk/peer/pressure diagnostics on failure,
   `campaign_exit_status`, and cleanup proof.
4. If all 20 repetitions pass, make no production or test code change. Attach
   the run as verification evidence and report `b95b038` as an invalidly
   attributed pre-repair failure for ticket close or human re-disposition.
5. If a repetition fails, stop at the first red and require the newly captured
   true-descendant client and sender records to identify message/chunk progress,
   pressure state, peer/channel state, typed terminal cause, and cleanup status.
   Return the plan for review with that evidence before changing behavior.
6. Any later repair must target only the newly evidenced mechanism and add the
   smallest deterministic regression at the existing
   `LocalWebrtcDataChannel` seam plus red-when-reverted proof.

Every implementation change must trace to the loaded campaign result. A green
campaign authorizes evidence updates only; it does not authorize speculative
hardening.

## Non-scope

- No production Rust change, test change, runner change, workflow change, or
  dependency update before a true-descendant recurrence is captured.
- No retries, timeout inflation, sleeps, reduced load, serialization,
  `--test-threads=1`, response-size reduction, or weaker payload, chunk,
  encryption, ordering, follow-up-request, grant-cleanup, peer-cleanup, or
  process-cleanup assertions.
- No second repair for `pressure_deadline`; PR #148 already removed that policy
  cold turkey from the actual descendant under test.
- No CLI smoke, browser, SPA, TUI, Lua/plugin, Rails relay, response codec,
  framing, queue-bound, or adjacent lifecycle work without a newly captured
  failure that identifies that surface and a reviewed plan amendment.
- No full lifecycle-suite convergence campaign. The focused leaf verification
  proves this ticket target; the umbrella convergence ticket retains ownership
  of suite-wide 20-run acceptance and sibling failures.

## Assumptions and unknowns

- Assumption: `afbf412` is the intended initial verification subject. Git
  history proves it descends from PR #148; a later subject is acceptable only
  with the same explicit ancestry and full-SHA evidence.
- Assumption: the existing `focused-oversized-webrtc` selector preserves the
  target's production daemon, package entrypoint, real DataChannel, encrypted
  300,000-byte payload, chunk ordering, same-peer follow-up, and cleanup path.
- Assumption: focused target success is sufficient for this leaf ticket under
  the prior human scope disposition; it is not suite-wide health evidence.
- Unknown: whether the repaired path recurs under the bounded residual-tail
  campaign. This is intentionally answered before code is changed.
- Unknown: if it recurs, whether the cause is peer-terminal publication,
  DataChannel closure/error, send failure, poll end, framing, runtime reply, or
  another branch. The terminal record, not the historical pre-repair run, must
  decide the next scope.
- Worktree/target assumption: work is confined to this pipeline worktree on
  explicit target `tgt_7e208a0c76a44980a83b63af976b1f22`; the remote Actions
  run must check out the recorded full subject SHA.
- There are no convention conflicts after the human correction. The
  verification-first scope is the smallest surgical response to the actual
  repository history.

## Affected surfaces and files

- `docs/plans/eliminate-oversized-local-webrtc-response-close-under-load.md` —
  corrected plan and evidence ledger; this is the only planned repository edit
  before campaign results exist.
- `.github/workflows/loaded-daemon-lifecycle.yml` — unchanged production
  dispatch entry point used with `focused-oversized-webrtc`.
- `script/run-loaded-daemon-lifecycle` — unchanged precompile, residual-load,
  exact-selector, stop-at-first-red, artifact, and cleanup harness.
- `tests/hub_daemon_lifecycle_test.rs` — unchanged production-path target
  `local_webrtc_chunks_oversized_encrypted_daemon_response`.
- `src/local_webrtc.rs` — unchanged repaired sender and deterministic unit-test
  seam unless a true-descendant failure later proves a defect.

The runtime proof is the workflow invoking the repository wrapper, which runs
the exact real-daemon lifecycle test through the production local-WebRTC sender.
The existence of watch-based code or isolated unit tests alone is not acceptance
evidence.

## Risks

- **Testing the wrong code:** a branch can claim to include a repair while its
  subject predates the merge. Require a full SHA and
  `git merge-base --is-ancestor 2cd5829 <subject>` before dispatch.
- **Speculative churn:** the invalid `b95b038` attribution could prompt a second
  fix for code that was never exercised. Green means no code change.
- **Focused-run overclaim:** a focused campaign removes sibling-test
  contention. Treat it as leaf verification only and leave suite convergence
  to its owner.
- **Missing causal evidence on red:** `channel_closed` at the client is not a
  root cause. A red without bounded sender/chunk/peer evidence is a diagnostic
  failure and cannot authorize behavior changes.
- **Load not achieved:** requested workers are not proof of contention. Review
  `resource-samples.log` and report observed load with the verdict.
- **Cleanup masking:** a useful red still fails acceptance if owned test, daemon,
  session-worker, entrypoint, sampler, or load process groups survive.
- **Stale plan after a red:** any newly evidenced cause changes affected files,
  risks, and tests. Return the artifact for Plan Review rather than silently
  choosing a repair.

## Acceptance checks and tests

1. Pre-dispatch identity gate:
   - `git rev-parse <subject>` resolves the recorded 40-character SHA;
   - `git merge-base --is-ancestor 2cd58291e0da56dc6ac190fc48ccd665f96b7332 <subject>` exits 0;
   - `git show <subject>:src/local_webrtc.rs` contains the peer-terminal watch
     path and does not contain the `PressureDeadline` variant.
2. Dispatch the unchanged campaign:

   ```sh
   gh workflow run loaded-daemon-lifecycle.yml \
     --ref main \
     -f subject_sha=afbf412c5db9ef3538155ca6fec6637f4cb1726c \
     -f test_target=focused-oversized-webrtc \
     -F repetitions=20 \
     -f stress_profile=residual-tail
   ```

3. Inspect the completed run and artifact. Require exact-target precompile
   success, actual execution of
   `local_webrtc_chunks_oversized_encrypted_daemon_response`, achieved load
   evidence, and `cleanup_status=0` with all owned process groups gone.
4. Green branch: require 20/20 exit-zero repetitions with the existing exact
   300,000-byte response equality, encrypted ordered chunk delivery, response
   kind, frame ceiling, same-peer follow-up, grant cleanup, and peer cleanup.
   Attach the run URL and full SHA; make no source change.
5. Red branch: preserve the first failing repetition and require correlated
   client/sender progress, `next_chunk`, `expected_chunks`, pressure, peer and
   channel state, typed cause, and cleanup disposition. Do not rerun to replace
   the red. Amend and review the plan before implementation.
6. Only if a true-descendant defect is repaired: run the relevant
   `src/local_webrtc.rs` unit filter, the exact oversized test five consecutive
   local repetitions through `./test.sh`, red-when-reverted ablation at the
   repaired enforcement point, `cargo fmt --check`, repository strict Clippy,
   and `./test.sh`; then repeat the fixed-SHA loaded campaign.
7. Audit the final diff and artifacts for secrets, payload bodies, usernames,
   absolute local paths, unwired code, deprecated branches, and unrelated
   cleanup. Every changed line must map to the corrected evidence scope.

## Project Pipelines gates and artifacts

- Plan artifact: this committed document, with the corrected ancestry premise,
  explicit green/red branches, and no silent production assumption.
- Implement green artifact: exact subject ancestry, workflow run URL, 20-run
  result, achieved-load summary, assertion preservation, and cleanup result.
- Implement red artifact: the above plus the first correlated terminal record
  and a reviewed plan amendment naming the evidenced repair.
- Vault checklist: notes loaded, convention conflicts (`none` after human
  disposition), verification commands/evidence, and capture disposition.
- Workflow checklist: context, repo/runtime inspection, resolved human question,
  attached plan, gate submission, and advancement request.

## Vault gaps worth capturing

- The history check reveals a durable campaign rule worth capturing after this
  run: statements that a diagnostic SHA includes a repair must be backed by an
  explicit ancestry check, not branch chronology or nearby merge timing.
- If the verification campaign passes and the invalid attribution is formally
  dispositioned, capture that rule through the inbox-first vault pipeline.
- No transport lifecycle note should be added unless a true-descendant failure
  proves new behavior beyond the existing peer-terminal watch convention.
