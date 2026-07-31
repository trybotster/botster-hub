---
ticket: ticket_1785437787_254990
run: run_1785447809_642444
step: botster_stack_implement
pull_request: 182
---

# Stabilize session entity resize transition under load

## Target and outcome

- Target repository: `trybotster/botster-hub`
- Target id: `tgt_7e208a0c76a44980a83b63af976b1f22`
- Pull request: [#182](https://github.com/trybotster/botster-hub/pull/182)

Project Pipelines context and the Hub spawn-target registry independently route
the ticket to `botster-hub`, matching the approved plan revision at commit
`68e549d`.

The direct real-daemon regression no longer gives a short-lived shell a
scheduler-dependent 250 ms tail. Its shell publishes `entity-ready`, blocks on
terminal input, accepts the `rows=31` / `cols=101` resize while provably live,
and exits naturally with code 0 only after the test sends `release` through
`DaemonRequest::SendInput`.

Both entity subscribers must now receive the resize patch as their immediate
post-resize frame at the same strictly increasing sequence, then receive the
natural `exit_code=0` patch as their immediate post-release frame at the same
next sequence. Every unexpected frame is included in the assertion diagnostic;
the published conformance-report booleans are not used as proof that no
out-of-contract frame preceded either transition. Terminal egress is still
proved through the real attach/drain path before readiness and through natural
exit.

No production reconciliation code changed. Controlled evidence showed a test
readiness race, not a Hub delivery, ordering, or lifecycle-truth defect.

## Guidance applied

- Role playbooks: [[implementer-playbook]] and
  [[botster-implementer-playbook]].
- Repository charter: [[botster-hub-playbook]].
- Runtime surface overlay: [[botster-runtime-reviewer-playbook]].
- Workflow policy: [[project-pipelines-playbook]], limited to checklist,
  artifact, gate, PR-link, and loaded-workflow evidence.
- Architecture maps: [[botster-architecture]], [[cli-patterns]], and
  [[spa-patterns]].
- Targeted lifecycle and test notes:
  [[lifecycle guards evaluated before the reconciling drain are one call stale]],
  [[retention without a reachable flush is data loss]],
  [[worker shutdown completion requires lifecycle transport and process termination]],
  [[an mpsc round trip is not a durability barrier]],
  [[PTY integration tests poll for readiness not fixed sleeps]],
  [[botster core hosts need an explicit drain loop contract]],
  [[clients subscribe to entities not ptys]],
  [[shared conformance fixtures that contradict the core contract teach clients the wrong state machine]],
  [[subprocess harnesses must kill child on failed readiness]],
  [[input gated fixtures require explicit session cleanup]],
  [[loaded lifecycle ci precompiles the exact test target before synthetic cpu stress]],
  [[live hub proof records distinct hub and locked core binary provenance]],
  [[graceful-termination-requires-explicit-cleanup-hooks]], and
  [[test script required for rust tests not cargo test]].
- Handoff discipline:
  [[implementation artifacts must match actual git state]],
  [[implement gate must verify committed work and pr link before review]], and
  [[implementation steps must persist report artifacts for review]].

No loaded convention conflicted with the approved plan.

## Files changed

- `tests/hub_daemon_lifecycle_test.rs`
  - replaces elapsed fixture readiness with terminal-output plus input
    synchronization;
  - classifies both subscribers' immediate resize and exit frames;
  - asserts `exit_code=0`;
  - keeps terminal-egress, removal, disconnect cleanup, and fresh authoritative
    reconnect coverage;
  - arms `SessionCleanupGuard` immediately after spawn and disarms it only after
    successful removal, so a red assertion removes the unbounded session,
    worker, shell, and socket before the panic-safe daemon owner reaps the Hub;
  - keeps WebRTC-only stderr classification behind the WebRTC constructor while
    emitting session-entity diagnostics with the correct subsystem label.
- `script/run-loaded-daemon-lifecycle`
  - admits and dispatches `focused-session-entity-resize` to the exact
    regression through `./test.sh`.
- `.github/workflows/loaded-daemon-lifecycle.yml`
  - exposes the same selector;
  - records the full Hub SHA and locked Core SHA;
  - builds the worker with `--locked`;
  - resolves the Hub and worker binaries and rejects either path outside the
    fresh subject target directory.
- `docs/loaded-daemon-lifecycle-runner.md`
  - documents the exact focused session-entity selector, its diagnostic
    purpose, and the five-repetition full-suite campaign it does not replace.
- `docs/plans/stabilize-session-entity-resize-transition-under-load.md`
  - approved plan and Plan Review amendments.
- `docs/reports/stabilize-session-entity-resize-transition-under-load-implement-report.md`
  - this durable handoff.

The final branch has no change to
`crates/botster-hub-test-support/src/lib.rs`; its timing probe was temporary
measurement instrumentation and was removed after evidence capture.

## Ownership boundaries and cross-repository work

The implementation remains inside Hub-owned real-daemon/entity proof,
test-process ownership, and loaded-workflow policy. Terminal bytes remain on
the existing SessionIo/ClientWorker path. `botster-core` remains the
policy-free authority for size, lifecycle-journal ordering, and process exit;
`botster-hub-client` remains the unchanged DTO/socket boundary.

No Web, TUI, package/plugin, Project Pipelines package, or production
reconciliation implementation changed. No cross-repository dependency or
separately routed work was required. The locked Core SHA remained
`5846fc776d31e2b6c98a8d932f50a31078743901`.

## Assumptions

- Human answer `question_1785448022_413436` is authoritative:
  resize means rows 31 and columns 101, followed by natural process
  `exit_code=0`; the ticket's original exit-code 31 wording was a transcription
  error.
- A bounded five-second deadline protects terminal reads from hangs but is not
  readiness evidence. The marker and release input are the readiness
  synchronization.
- A successful resize response plus the immediate two-subscriber entity
  frames is sufficient to distinguish an accepted live resize from the
  pre-fix process-exit race.
- The conformance runner may keep its separate two-second fixture only if its
  measured spawn-to-resize acknowledgement has adequate margin under the same
  residual-tail profile.

## Reproducible negative control

The red used approved-plan commit
`68e549d7b6d2f9164a3cf3b22912cbe11b27b31d`, locked Core SHA
`5846fc776d31e2b6c98a8d932f50a31078743901`, and this exact command:

```text
./test.sh --test hub_daemon_lifecycle_test session_entity_subscription_pushes_snapshot_ordered_deltas_and_fresh_reconnect -- --exact --nocapture
```

The verbatim temporary patch was:

```diff
@@
     assert!(
         terminal_output.contains("entity-after"),
         "entity lifecycle pumping must retain terminal egress, got {terminal_output:?}"
     );
+    thread::sleep(Duration::from_millis(400));
-    botster_hub_client::request(
+    let resize_response = botster_hub_client::request(
         &endpoint,
         botster_hub_client::DaemonRequest::Resize {
             session_id: "entity-session".to_string(),
             rows: 31,
             cols: 101,
         },
     )
     .expect("resize entity session");
-    let resize_patch = first.next_frame().expect("resize patch");
+    let resize_patch = first.next_frame().expect("first subscriber transition");
+    let second_resize_patch = second.next_frame().expect("second subscriber transition");
+    panic!(
+        "negative-control resize_response={resize_response:?} first={resize_patch:?} second={second_resize_patch:?}"
+    );
```

It failed with exit status 101. Resize returned the typed
`runtime_error` / `daemon-sessions-resize` operator response. Both subscribers'
first next frame was the same `snapshot_seq=2` patch containing
`lifecycle=exited`, `lifecycle_class=ended`, and `exit_code=0`; neither frame
contained rows or columns. This classifies the race as process exit winning
before resize, not a dropped or divergent entity delivery.

That pre-fix negative control used the original self-terminating fixture. Its
intentional panic exposed one exact Hub daemon survivor, which was terminated;
the session itself had already exited before the follow-up census. That census
therefore proved cleanup only for the pre-fix fixture and is not evidence for
the final input-gated fixture.

Review's equivalent panic ablation on the input-gated fixture exposed the
missing ownership boundary: the Hub daemon was reaped, but the durable
session-worker, its read-blocked shell, and its control socket survived. The
final test now combines the panic-safe daemon owner with a
`SessionCleanupGuard` armed immediately after spawn and disarmed only after
`SessionRemoved`.

The post-fix ablation inserted this exact temporary statement after both
subscribers' resize assertions:

```diff
@@
     assert_eq!(
         second_resize_sequence, resize_sequence,
         "subscriber resize sequences diverged: first={first_resize:?} second={second_resize:?}"
     );
+    panic!("panic-ablation-after-entity-resize");
```

The exact focused command failed with the intended exit status 101 and printed
`session entity daemon evidence: ... daemon_status=exit status: 0`. An immediate
census scoped to this run worktree, `entity-session`, and the
`session-entity-subscription` data directory found no Hub, session worker,
fixture shell, or control socket. The temporary panic was removed, and the
focused test then passed. No temporary delay, panic, or timing output remains in
the final tree.

## Conformance-runner measurement and decision

Temporary instrumentation started `Instant` immediately before the conformance
runner's spawn request and printed elapsed milliseconds immediately after the
resize request acknowledged. The exact temporary patch was:

```diff
@@
+    let spawn_to_resize_started_at = Instant::now();
     let spawn = botster_hub_client::request(
@@
     )
     .map_err(|error| session_lifecycle_error("lifecycle patch", error.to_string()))?;
+    eprintln!(
+        "session_lifecycle_conformance_spawn_to_resize_ack_ms={}",
+        spawn_to_resize_started_at.elapsed().as_millis()
+    );
```

Local ten-run results were 165–368 ms. Linux residual-tail workflow
[30586317679](https://github.com/trybotster/botster-hub/actions/runs/30586317679)
ran the instrumented commit
`ee9891320bdfcc042fdb58de7a90f91a921d4879` ten times and measured 68–119 ms
against the fixture's 2,000 ms process budget.

Decision: keep the published conformance runner unchanged. Its measured loaded
margin is at least 1,881 ms, and the direct live-daemon two-subscriber section,
not its discard-until-match booleans, remains the ordered-delivery authority
for this ticket.

## Verification and downstream proof

Passed locally:

- `cargo fmt --all -- --check`
- `cargo clippy --workspace --all-targets --all-features -- -D warnings`
- `./test.sh --workspace --no-run`
- `./test.sh`
- `git diff --check`
- exact focused regression after removal of all instrumentation: 1/1
- forced panic after the two-subscriber resize transition: intended exit 101,
  followed by zero run-worktree/session processes and zero fixture sockets
- repo-owned focused harness, 10/10 repetitions at default test concurrency:
  every repetition passed, every run-token survivor census was zero, every test
  group was `post_clean=gone`, and `cleanup_status=0`
- lifecycle integration binary twice at default parallelism:
  115 passed, 0 failed, 1 documented large local adversarial test ignored on
  each run

Linux loaded proof:

- Instrumented measurement workflow
  [30586317679](https://github.com/trybotster/botster-hub/actions/runs/30586317679):
  10/10 focused residual-tail repetitions passed, with 48 load workers on four
  CPUs, zero test/session survivors, all test/load/sampler groups gone, and
  `cleanup_status=0`.
- Provenance-complete focused workflow
  [30586799729](https://github.com/trybotster/botster-hub/actions/runs/30586799729):
  10/10 residual-tail repetitions passed on immutable subject
  `43cf04b11947d6b875120214ca79ce9f540ff497`; metadata records the matching Hub
  SHA, locked Core SHA
  `5846fc776d31e2b6c98a8d932f50a31078743901`, and both executable realpaths
  beneath the fresh subject target. Every run-token and session survivor census
  was zero, all test/load/sampler groups were gone, and `cleanup_status=0`.
- Final current-code focused workflow
  [30587948860](https://github.com/trybotster/botster-hub/actions/runs/30587948860):
  10/10 residual-tail repetitions passed on immutable subject
  `344b9c9a4f8216c19bd3914ebaf2b8972e1f26a1`. Metadata records the matching Hub
  SHA, locked Core SHA
  `5846fc776d31e2b6c98a8d932f50a31078743901`, and both executable realpaths
  beneath the fresh subject target. Every repetition had zero run-token and
  session survivors; every test/load/sampler process group was gone; campaign
  `exit_status=0` and `cleanup_status=0`.
- Earlier full-suite attribution workflow
  [30587184664](https://github.com/trybotster/botster-hub/actions/runs/30587184664):
  the target resize regression passed. The run stopped in repetition 1 after
  394 seconds with 114 passed, 1 failed, and 1 ignored because
  `cli_shutdown_waits_for_metadata_owned_runtime_daemon_cleanup` asserted that
  shutdown returned before its metadata-owned daemon exited. The authoritative
  base workflow
  [30570139134](https://github.com/trybotster/botster-hub/actions/runs/30570139134)
  has the same failure and exact assertion under the same residual-tail
  profile, in addition to the old resize failure. The subject therefore
  removed the ticket failure while preserving an exact base-attributed,
  unrelated cleanup failure. Subject cleanup still reported zero run/session
  survivors, all test/load/sampler groups gone, and `cleanup_status=0`.
- Final current-code full-suite contention workflow
  [30587962499](https://github.com/trybotster/botster-hub/actions/runs/30587962499):
  the one bounded residual-tail repetition passed every suite on immutable
  subject `344b9c9a4f8216c19bd3914ebaf2b8972e1f26a1`. The lifecycle integration
  binary reported 115 passed, 0 failed, and 1 documented ignored test; all
  other test binaries also passed. Metadata records the matching Hub SHA,
  locked Core SHA, and Hub/worker realpaths beneath the fresh subject target.
  The run-token and session survivor censuses were zero, all
  test/load/sampler groups were gone, and campaign `exit_status=0` with
  `cleanup_status=0`.
- Review-remediation five-repetition full-suite contention workflow
  [30590359513](https://github.com/trybotster/botster-hub/actions/runs/30590359513):
  requested the repository's full five-repetition residual-tail budget on
  immutable code subject
  `a94981bab4f786a449d66f043694bf1ef3bb3c92`. Repetitions 1–3 passed every
  suite. Repetition 4 stopped first-red with the unchanged
  `cli_operator_console_starts_reuses_detaches_handles_ctrl_c_and_stops` test
  waiting 30 seconds for a second `foreground app ` output occurrence; the
  lifecycle binary reported 114 passed, 1 failed, and 1 ignored. The ticket's
  session-entity regression passed in all four executed repetitions. Per-run
  exit statuses were 0, 0, 0, and 101. Every repetition reported zero
  run-token and session survivors, every test/load/sampler process group was
  gone, and campaign `cleanup_status=0`.
- Same-input authoritative-base workflow
  [30592300735](https://github.com/trybotster/botster-hub/actions/runs/30592300735):
  used the same workflow harness, requested repetitions, target, and
  residual-tail profile against
  `95e829ab039198177e14e17a494f93963951ea6f`. It stopped in repetition 1 with
  the original session-entity resize failure plus
  `package_entrypoint_supervision_passes_environment_overrides`; the
  operator-console test passed. Cleanup detected three run-token descendants,
  terminated them, verified the run-token group gone, reported zero session
  survivors, removed every test/load/sampler process group, and finished with
  `cleanup_status=0`.

The exact operator-console test passed independently on both the final branch
and authoritative base with the same `./test.sh --test
hub_daemon_lifecycle_test ... --exact --nocapture` command. Combined with its
passing status in branch repetitions 1–3, its passing status in the
same-input base run, the absence of any changed operator-console path, and the
branch's four loaded passes of the ticket regression, the repetition-4 red is
attributed to a nondeterministic unchanged-path load flake rather than this
session-entity change. First-red behavior correctly prevented a fifth
repetition from hiding that evidence.

## Deviations from plan

No scope deviation. The negative control proved that the direct regression's
plain `Child` ownership could survive a panic, so the implementation also
renamed and reused the file's existing panic-safe daemon owner. That change is
cleanup made necessary by the approved red/green procedure and teardown
acceptance, not adjacent lifecycle refactoring.

Review then proved that daemon ownership alone was insufficient for the new
input-gated fixture. The added session guard, role-correct panic diagnostics,
operator documentation, and five-repetition full-suite campaign are direct
repairs for Review findings and the ticket's cleanup/repeated-load acceptance,
not broader production work.

The workflow's explicit Core-SHA and binary-realpath recording was added after
the first measurement artifact showed that build provenance was inferable but
not durably asserted. This implements the approved acceptance check; it does
not broaden runtime behavior.

## Unverified behavior or residual risk

- No production Hub/Core code changed because the semantic barrier produced
  correct immediate resize and exit transitions. A distinct production
  lifecycle race, if discovered later, is not masked by fallback polling or a
  weakened assertion.
- The workflow emitted the runner image's Node.js 20 deprecation annotation for
  pinned `mlugg/setup-zig`; setup and every ticket gate still passed. Tool-action
  modernization is unrelated to this lifecycle ticket.
- `SessionLifecycleSubscriptionConformanceReport` still proves eventual
  matching delivery rather than absence of preceding frames. This report does
  not upgrade that claim.
- The input-gated fixture would be a permanent orphan risk without explicit
  session ownership. That risk is now guarded and the forced-panic path has
  zero-survivor proof; Verify should repeat that ablation rather than infer
  cleanup from a clean Hub exit.
- The requested five-repetition full-suite campaign stopped on an unchanged
  operator-console load flake in repetition 4, so repetition 5 did not execute.
  Exact branch/base isolation is green and the same-input base aggregate passed
  that test, but this ticket does not repair or claim to resolve the separate
  operator-console output-progress flake.

## Missing vault guidance and durable capture

Review identified and captured the reusable gap as
[[input gated fixtures require explicit session cleanup]]. It records that an
input barrier trades a transient readiness race for an unbounded fixture and
therefore requires session-scoped unwind ownership through successful removal.
The note is linked from the Botster reviewer/verifier guidance and CLI patterns.
No additional capture is needed. The rows-versus-exit-code correction remains
ticket-specific and is durable in the human answer, approved plan, and this
report.
