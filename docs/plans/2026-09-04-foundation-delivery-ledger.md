# Foundation delivery ledger

The root coordinator owns delivery decisions.
The previous project orchestrator released its claims in the September 4 handoff.
This ledger records work in progress. It does not certify the complete stack.

## Current integration

Hub ticket: `ticket_1787600679_990088`.
Hub run: `run_1788459722_264752`, Implement.
Hub writer: `sess-1788571117-003d-9a6151624be4beecbac1ea4524065c98`.
Hub steward: `sess-1788551411-0026-dca615f53e6251ef8cba7a8bb25213fe`.

Candidate `11facecf371271907f7d20d83e390601c4011966` adds a permanent exit barrier and Attach failure context.
The barrier waits for Attached before sending release input through the terminal channel.
The ended-session Attach rejection test passed.
The combined lifecycle test still failed while waiting for ProcessExit.
Its log records admitted input with `bytes_written=12`.
That result does not prove that the shell exited or that the receive path preserved every event.
Evidence: `/tmp/hub-focused-fixture-repair/exit.log`.
The implementer must correct the demonstrated fault before the complete matrix runs.
The separate disposable observer campaign is stopped.

TUI ticket: `ticket_1788460430_647093`.
TUI run: `run_1788570301_694931`, Verify.
TUI verifier: `sess-1788572753-0044-49ac9f03fcc0196b1cc7b2593aa0eb78`.
Reviewed candidate: `812b2004ab1bcc8ff24b824c42b0b7c5e5059550`.
That candidate consumes Hub `9a02e55`, not the new fixture commit.
The verifier confirmed a clean tree and matching remote branch at handoff.

Required order:

1. Correct and independently review the Hub candidate.
2. Update the TUI pin and review the affected changes.
3. Freeze the exact Hub, Core, TUI, and Web revisions.
4. Run one complete matrix without combining results from separate runs.
5. Merge Hub first.
6. Confirm the final TUI pin, finish Verify, and merge TUI.

Keep Core `93acae3` in this integration series.
Do not substitute the foundation resize candidate into this matrix.
Do not add ticket dependency cycles or compatibility paths.
Read review prose as well as structured findings.
The review tool rejected structured findings during the TUI run.
An empty `open_findings` result does not prove that all findings are resolved.

## Nonblocking resize

Core candidate: `5923bf1847979e2897796fadb9863183ffa5e3f1`.
Core implementer: `sess-1788570969-003c-9a66d9610480b52a05aabcfbdc52f3aa`.
Independent reviewer: `sess-1788561640-0030-9ab4035a5a3867e8f4970b3a6e440510`.
The Core implementation is merged and pushed to main at `c47eadbf476501ec611e18572d8e4afc87d4304d`.
The delivery commit combines approved resize code `5923bf1`, reports `55d2b53`, and reviewed close comments `2cfc7eb`.
A reviewed architecture-documentation follow-up is pushed at `68ca23dd87dad0fca86935f538fa3682a9407d89`, the current verified remote main.
It removes the old blocking-resize description and private registry pathname. It changes no code.
Fable verified that the combination changes no other Core code and has no merge blocker.
The active Hub integration still pins `93acae3`; the registry repair is not included in this merge.
The isolated Hub classification regression passed with one test executed.
Its negative control failed at the required assertion: `control_plane_failed` differed from `explicit_resize_busy`.
The implementer restored the approved source and confirmed its SHA-256 hash.
The matching worker and Hub builds passed.
The resize-entity, stale-transition, and attached-exit checks each passed with one test executed.
The implementer confirmed zero isolated target processes, unchanged source inputs, and no running validation command.
The coordinator returned the test window to Hub.
Fable approved the final logs and provenance in review commit `f61161b`.
The implementer committed four report files at `55d2b539e40df99488c8119f8f2e318b623eeef2` and confirmed a clean worktree.
That report commit changes no Core source from approved `5923bf1`.
Evidence is in `/private/tmp/botster-resize-downstream.g2c6fu/foundation-validation`.
The Hub implementer confirmed cleanup and released its build window.
The coordinator authorized these foundation checks. Results remain pending.
Use the existing isolated source and provenance reports. Do not restart the implementation.

## Adapter close contract correction

Astra identified a source-backed race in `flush_subscription_adapter_frames` at Hub `11facecf`.
The function selects active or parked bytes without retaining which source supplied them.
It then completes the active slot and unconditionally consumes parked bytes.
Completing the active slot wakes Core, which can admit a successor before parked bytes are consumed.
A concurrent close can park that successor. The preceding completion can then delete the successor without sending it.
The coordinator initially authorized a separate repair through the actual flush function.
A source review then identified a more fundamental contract conflict.
Pinned Core `93acae3`, `contract/terminal_adapter.rs:25-62`, requires close to abandon an in-flight frame without later delivery.
It also forbids waiting for transport I/O or a writer-held lock during close.
Existing Hub late-egress delivery and its retention tests conflict with that contract.
The coordinator stopped the proposed retention repair before a positive test or commit.
The implementer restored its uncommitted repair after controlled negative tests and confirmed that no command remains active.
The source patches and negative-test evidence remain under `/tmp/hub-write-ownership`.
Those negative tests show retention behavior. They do not establish the correct close contract.

The implementer and Astra found no explicit authority that supersedes the pinned Core contract.
The coordinator ruled that close must abandon data, and any premature natural-exit close must be corrected at its owner.
Core's normal ProcessExit completion waits for adapter Ready before retiring the route.
The sole Hub writer is assigned to remove post-close replay from Unix and WebRTC adapters and their consumers.
Hard-close tests must prove permanent abandonment. Natural-exit tests must still prove final output and ProcessExit before close.
The rejected retention redesign stays preserved as evidence and must not be committed as the repair.
An already-partially-written Unix envelope needs explicit framing review if removal exposes a conflict.
The repair must not kill sibling routes or add a replay queue.
The Hub writer has the focused test window for these changes.
The writer found that Unix marked a frame complete after a partial socket write.
The coordinator approved keeping the adapter Full until the complete envelope is written.
The current uncommitted repair passed 24 Unix writer tests and 82 WebRTC tests in `/tmp/hub-hard-close/units`.
Fable reviewed the partial-envelope boundary in review commit `7fd224e`.
The coordinator adopted an explicit narrow clarification: close abandons unsent adapter and writer copies.
A writer-owned envelope with positive transport progress may finish once to preserve shared-stream framing.
An in-progress nonblocking write cannot retract accepted bytes; a Pending or zero-progress write must not retry after close.
No new envelope may start after close, and no envelope may be replayed.
The Hub writer must gate zero-offset attempts on close and test both abandonment and partial-frame completion.
Normal operation keeps the adapter Full until the full envelope completes.
The Core contract clarification is committed at `2cfc7eb` on `foundation/adapter-close-contract`.
Its worktree is `/private/tmp/botster-close-contract.x815BM/core`, based on `93acae3`.
Only comments in `contract/terminal_adapter.rs` changed.
Fable approved exact commit `2cfc7eb61f784b472d8d518df98df66c1f3bf01b` after both precision changes.
The comments now preserve sibling traffic and require a partial envelope to finish or its affected stream to end.
The clarification is included in Core main `c47eadb`. No active integration dependency pin changed.
No complete matrix or merge is authorized during this correction.
The original missing-ProcessExit failure remains unattributed.
The implementer reports five focused natural-exit and close checks passed on the corrected uncommitted source.
They include Unix printf ProcessExit, WebRTC exact bytes, the combined lifecycle fixture, ended-session rejection, and host-close delivery.
Each selected test executed once. Evidence is under `/tmp/hub-hard-close/natural-exit`.
WebRTC accounting for partially accepted sends remains a required repair before independent review.

## Registry identity implementation

The existing foundation Codex implementer is assigned a separate `foundation/registry-identity` worktree from report commit `55d2b53`.
The resize worktree and isolated Hub validation export stay frozen.
The new work is source-only while Hub uses the build window.
The first registry source candidate is `b767d7df4b952325344e95be92c1515962e34858`, with a clean worktree reported.
Ten registry unit tests and four daemon tests are prepared but have not run.
Fable is reviewing this exact source candidate and the separate Hub consumer patch.
Fable approved both as source and required one registry correction before testing: a malformed temporary write must not block that identity permanently.
The implementer is assigned that recovery test and correction while preserving checks on the primary record and valid foreign temporary records.
The registry must check stored identity before reads, overwrites, and removal.
The filename encoding stays private and uses a full collision-resistant digest with an explicit format discriminator.
Unsupported legacy records must remain on disk and produce an explicit error where applicable.
No migration framework, compatibility lookup, or fabricated lifecycle state is authorized.
Exact-session operations must not scan the full registry.
The public record type and worker socket identity stay unchanged unless a concrete requirement warrants review.
Hub's offline updater must use the existing identity-checked `SessionRegistry::load` API before consuming this change.
The foundation implementer must not edit the active Hub worktree or change its pins.
The daemon's paginated baseline index also interpreted filenames as IDs.
The coordinator approved a crate-private identity-checked entry reader for that path, preserving its per-entry budget.

The root coordinator owns the matching Hub consumer patch in `/private/tmp/botster-registry-consumer.tuGcwH/hub`.
Its branch is `foundation/registry-identity-consumer`, based on Hub `11facec`.
Only `src/update.rs` changes: it reads through `SessionRegistry::load` and verifies the requested identity before using recovery data.
Tests cover punctuation and Unicode IDs, missing records, and independently retained colliding-sanitizer IDs.
Formatting passed. Tests remain pending the exact Core registry candidate and a build window.
No active integration pin changed.
The current updater already blocks incompatible-worker termination when identity is missing; this repair preserves that fail-closed behavior.

## Latest handoff: frozen Hub candidate and registry test window

The Hub implementer froze and pushed `bbecda6b4b505b25e43e05baa013b8daaec923d6`.
The implementer reports a clean worktree and complete cleanup.
The active Hub candidate still consumes Core `93acae3`.
The implementer reports that strict formatting and Clippy checks passed.
Focused checks passed: 85 WebRTC tests, one child test, 25 Unix tests, and seven budget tests.
The Unix ProcessExit test and the WebRTC exact-bytes test passed.
The final combined WebRTC lifecycle test failed: zero passed, one failed, and 349 filtered out.
The run stopped before the ended-session and host-close tests.
The earlier combined-test pass does not clear this final failure.

The final failure log contains terminal output with `done`, but no observed ProcessExit event.
The closed channel label is not yet mapped to the exit subscription.
The evidence does not establish which caller caused the close.
The log is `/tmp/hub-hard-close/final-focused/webrtc_exit.log`.
The implementation report is recorded in `artifact_1788587814_171553`.
Hub remains at Implement. No complete matrix or merge is authorized.
Fable now reviews the frozen source and existing evidence without builds.
The Hub implementer must inspect existing source and logs before proposing one further bounded check.

The Hub implementer released the build window.
The registry implementer now owns that window for Core candidate `cfc51fb7a7528e6c0c848a81375c514ff7a468e7`.
The registry implementer must use at most two Cargo jobs and preserve test failures.
Registry tests have authorization to start; this handoff does not claim that they passed.

The separate Hub consumer worktree also contains the previously reviewed ExplicitResizeBusy classification change in `src/runtime.rs`.
Its preparation report explains that the unchanged Core pin cannot compile that new enum arm.
That worktree remains source preparation, not a validated delivery candidate.

## Client foundation preparation

The TUI scheduling plan is `docs/plans/2026-09-04-tui-scheduling-foundation.md`.
The TUI integration owner checked the plan against candidate `812b200` without finding a direct contract conflict.
The plan includes the owner's requirements for timed-out uncorrelated responses and EventSubscribed ordering.
No TUI scheduling implementation has started.

Root owns Web worktree `/private/tmp/botster-web-foundation.sm0cZt/web` on `foundation/web-readiness`, based on `9e18b104`.
Its uncommitted telemetry repair removes terminal payload DOM attributes from both renderer paths.
It installs no render observer without an existing harness terminal recorder.
An enabled recorder receives one encoded payload per chunk and retains dynamic suppression support.
A focused Node test passed for the actual fallback bridge and shared observer factory.
The test covers absent collection, suppressed collection, enabled collection, and recorder replacement.
The mounted Restty path, full Web checks, and independent review remain pending.
Root prepared `scripts/mounted-renderer-telemetry.mjs` and connected it to the existing mounted keyboard script.
The smoke fixture can now omit the live recorder before mounting.
The new check reads actual viewport content and counts terminal payload encodings for disabled, enabled, and suppressed collection.
Only its syntax check has run. Browser execution remains pending a test window.
Astra now reviews this patch without builds or edits.
Astra's source review found no blocking defect and confirmed that existing harness consumers install their recorder before mounting.
Root added deferred-write tests after that review. Those tests pass under Node `v22.21.1`.
They verify that encoding waits for rendering and stops if collection is suppressed or the harness is replaced before completion.
The temporary worktree uses a symlink to the existing Web node_modules directory. No dependencies were installed or changed.
The active integration matrix still uses Web `9e18b104`.

The Web telemetry repair is now committed at `67aa0df` on `foundation/web-readiness`.
Full Web tests, TypeScript/Vite build, and lint pass. Lint reports five warnings in unchanged files.
The mounted script passes actual Restty checks with collection disabled, enabled, and suppressed, followed by existing keyboard and exit-order checks.
Its first readiness condition timed out because it required an initial fixture output. The corrected condition waits for the runtime and subscription.
The test then emits and requires its own marker in the actual viewport; telemetry is not the rendering oracle.
Root replaced the dependency symlink with a private copy after a sandbox cache-write error.
No dependency version changed. The approved browser run used a local Vite listener.
See the Web report `docs/reports/2026-09-04-renderer-telemetry-repair.md` for command results and limitations.
Astra now reviews the frozen final source and logs. Web main has not changed.
Final outcome: Astra approved `67aa0dfe8013833318ecdebe33f1bb627517787d` with no blocking finding.
Root verified clean local and remote main at `9e18b104`, then fast-forwarded and pushed Web main to `67aa0df`.
The active integration matrix still uses `9e18b104`; root explicitly notified the Hub implementer not to repin it.
This delivers the renderer telemetry repair, not the remaining Web input or recovery work.

Root has started the separate Web startup-isolation repair after `67aa0df` in the same worktree.
The uncommitted source launches session load-status completion and optional pulls independently after essential connection and subscription setup.
Rejected optional pulls retain family-specific error states and warnings instead of entering the connection-failure path.
Cancellation checks guard each startup stage and late state updates.
Astra found no blocking source defect, but corrected an important behavioral claim.
The real transport already starts session subscription during `hub.subscribe()`; the repair does not introduce that first subscription.
Root corrected the audit's claim and prepared actual-hook React tests using the real client and Hub transport over a controlled bridge.
Those tests cover optional failure/pending work, essential failures, cancellation, visible session rows, and independent load-status completion.
Only syntax and diff checks have run. Full execution remains pending while Hub owns the test window.
This change does not claim optional-family replay after reconnect.
Final startup outcome: Fable completed all eight actual-hook scenarios, full Web tests, build, and lint.
The old-hook negative control failed at the expected session load-status assertion; the repaired hook was restored.
An initial new-test lint error was corrected. Its original full log was overwritten; a preserved excerpt records that limitation.
Evidence is frozen under `/private/tmp/botster-web-foundation.sm0cZt/evidence/startup-isolation/` with six verified hashes.
Source/test commit is `7fd4ae25d1a99cf6f5282d924227e8c4153dd19e`; report correction is `13f89ba937970b70c6194acf493b5f4b820e751e`.
Root Codex approved the final source, tests, and evidence scope.
Root verified clean local/remote main at `67aa0df`, then fast-forwarded and pushed Web main to `13f89ba` and verified the remote.
The integration matrix still uses Web `9e18b104`.
Fable Web now prepares the separate reconnect state model and test plan, source-only until root review and window assignment.

## Current focused evidence

The registry implementer reports 11 registry tests, three exact-operation tests, and both new baseline tests passed.
The first real-worker test failed its final PID-existence assertion.
The diagnostic run found terminated worker processes in the zombie state, absent PTY children, and absent sockets.
The implementer changed only the test's termination predicate to the existing `process_has_exited` helper.
The five-second bound and socket checks remain unchanged.
The corrected real-worker test and the legacy-adoption test passed.
The original failures remain in `/private/tmp/botster-registry-validation`.
Root requested explicit evidence about Child-handle ownership and reaping before claiming complete cleanup.
The implementer found that the in-process restart simulation drops original Child handles without waiting; adoption has no replacement handles.
The implementer reports no diagnostic PIDs or candidate workers remained after the test process exited.
Do not equate that observation with reaping by the restarted daemon inside the test.
Further checks passed: daemon library 29/29, baseline integration 9/9, registry integration 12/12, oversized metadata 1/1, and three persistence tests.
Strict Clippy failed on an existing nested terminal_output match in `daemon_integration_test.rs`.
Root inspected that block and authorized an equivalent match guard in a separate test-only commit, followed by focused verification.
The cleanup-test correction is `3f29a8de1e8764ac6d134fa8a6cfb029319cb299`.
The lint-only correction is `d10e57aafb0578857b033fbba0cdc857cf41a2b7`.
The implementer reports strict daemon Clippy and formatting now pass.
Three negative controls failed at their intended assertions and were restored.
The registry implementer released the test window; broader workspace and Hub consumer checks remain pending.
Astra now reviews the registry source and saved evidence without builds.

The Hub implementer recorded a possible transport ordering defect in `artifact_1788588289_683970`.
The dependency can complete send_text after queue admission, before the driver forwards the payload.
Its close marker enters a lower queue, which may let the close precede that payload.
This is a source inference. It does not identify the caller or subscription in the failed test.
Root authorized preparation of one enqueue-then-close check, but not execution during the registry window.
After the registry window ended, root assigned the Hub implementer the window to execute that one prepared check.
Fable will review the dependency ordering and the distinction between normal completion and abortive close.
No sleep, replay buffer, or unsupported delivery claim is authorized as a repair.

Fable completed source/log review in reviewer commit `7b91a6e` and did not approve Hub delivery.
The review confirms the Unix framing repair, WebRTC accounting changes, and six intended negative-control failures.
It also finds a separate sibling-isolation regression: closing an already-removed channel can escalate to whole-peer cleanup.
Root assigned the writer an idempotent already-closed path and a live-sibling regression test after the current dependency probe.
Timeouts and genuine close errors must still trigger the required supervision.
This finding does not establish the cause of the missing ProcessExit event.

Fable could not confirm the writer's queue-order inference in the locked dependency.
The writer must compare the exact source paths and report the current probe result before another diagnostic run.
Outstanding bytes at close alone do not prove overtaking; the same counter also decreases when bytes are abandoned.

Astra completed registry source/log review at `d10e57a` without finding a new source blocker.
Production hashes match `cfc51fb`; later commits change tests only.
The report and evidence index were still unfinished when Astra read them.
Root requested final revision attribution, corrected Clippy evidence, and precise cleanup claims from the writer.
Full workspace, contract/documentation, workspace Clippy, and exact Hub consumer checks remain required before delivery.

The final registry report is clean at `3b0b51374a6e54659e2f4649bab37cca532b94a5`.
Astra verified 35 evidence hashes, five source hashes, the worker binary hash, and all 23 recorded command results without mismatch.
The documentation no longer blocks focused acceptance. Broader gates remain pending.

The corrected RTC probe executed and failed its required order assertion: control `[payload, close]`, queued case `[close, payload]`.
Fable independently confirmed that the probe models reachable production ordering in the exact locked dependency.
This demonstrates a dependency defect but does not independently attribute the original Hub failure.
Root approved a minimal licensed vendor copy of `rtc 0.21.0-beta.2` with an explicit root Cargo patch.
The repair must queue application close through the same endpoint FIFO as earlier application data.
It must reject sends after close, preserve repeated-close behavior, and pass both queue-order and real-peer delivery tests.
The implementer must prove that supported runtime builds use the patched dependency.
If a supported consumer embeds the runtime outside the Hub workspace and loses the root patch, the dependency strategy requires correction.
No remote fork publication is authorized.
Root released the Web window after its commands exited. The Hub implementer now owns the test window with at most two Cargo jobs.

## Model allocation change

The user requests Fable implementation and Codex review because Codex quota is low.
Root instructed the Hub Codex writer to finish only the current check, freeze the handoff, and release ownership.
Fable session `sess-1788561640-0030-9ab4035a5a3867e8f4970b3a6e440510` will take implementation ownership after that explicit release.
The former writer and root will review Fable's new changes. Fable must not approve its own implementation.
The ownership transfer is now authorized from explicit Codex release in `artifact_1788590263_218370`.
No command or owned process was running at release. HEAD `1d48964` is not pushed.
Fable now owns the existing Hub worktree and test window, with at most two Cargo jobs.
Its next group is the patched Hub build, one exact terminal remote-close retry, and affected focused gates.
Owner correction: the prior Fable reviewer did not acknowledge or read this assignment; its PTY was paused in `/usage`.
Root revoked that pending implementation assignment explicitly and left the session review-only/idle.
Fresh Fable Hub implementer `sess-1788590641-004c-c690f3f33319fb80558a0ef4f7de8dc6` is now running from request `msg_plugin-w_1788590641_b494fc`.
The new agent owns the preserved Hub worktree and test window, with the same exact handoff and limits.
The new Hub implementer acknowledged ownership and completed both locked worker and Hub builds successfully.
`/tmp/hub-hard-close/fable-build/build.log` confirms compilation of the vendored RTC source.
Binary hashes are in the adjacent `binary-hashes.txt`. No owned build process remained at release.
Root assigned the next short test window to Web for sequential tests, build, and lint.
Hub must wait for window return before its exact remote-close retry and focused gates.
It must not edit the main checkout where its session starts.
The former Hub writer is review-only and must not edit or start builds.
The registry Codex writer must keep `3b0b513` frozen and await a bounded review assignment.
Root will not start another Web implementation step during this transfer.
Fable Web implementer `sess-1788590456-004b-ce0765c58b97c7a04bc35b302542a316` is running in Botster Foundation.
Root transferred the temporary Web worktree write claim to this agent in spawn request `msg_plugin-w_1788590456_f962bd`.
The session starts in the Web main checkout but must perform all edits in `/private/tmp/botster-web-foundation.sm0cZt/web`.
Its bounded task is completion and validation of the prepared startup-isolation repair, followed by Codex review.
It must remain source-only until root assigns a test window. The Hub Fable implementer retains the current window.
The Web Fable implementer explicitly acknowledged its worktree ownership and source-only hold in message `msg_plugin-w_1788590595_0231e3`.

The latest Hub check group has ended and its test window is released.
The RTC queue-order test and real-peer final-payload-before-close test passed.
The real-peer test failed with original RTC production code restored: the receiver had no payload when close arrived.
The repair was restored. This gives positive and negative peer-delivery evidence.
The idempotent-close repair is committed at `1d489646a02ee3bca6e51e206b310eca70be68a8`.
Root Codex reviewed its exact production and test diff. The mapping accepts only typed ErrDataChannelClosed and preserves other errors.
The test uses a real removed dependency channel, the production cleanup helper, and live host sibling traffic.
No source blocker was found in that narrow fix. Actual remote retirement and full Hub acceptance remain unproven.
The vendor repair remains uncommitted in Cargo.toml, Cargo.lock, vendor/, and the restored remote-close test in subscription_channel.rs.
Artifact `artifact_1788590134_343889` records the dependency patch, provenance, graph selection, and test evidence.
Runtime build, the exact post-repair remote-close retry, and focused Hub gates remain pending.

The Web startup work remains uncommitted in `/private/tmp/botster-web-foundation.sm0cZt/web` after `67aa0df`.
Changed source: `src/app/useProductionHubConnection.ts`.
Prepared tests: `src/app/productionHubConnection.test.mjs`, its React fixture, and the App.test.mjs entry point.
Only syntax and diff checks have run. The next implementer must execute the actual-hook tests before claiming completion.

## Latest handoff: Hub review and Web reconnect

This update supersedes the pending states above.
Web main is `13f89ba937970b70c6194acf493b5f4b820e751e` after the reviewed startup repair.
The integration matrix still uses Web `9e18b1046b75438e971b9fe56a16137581ac2d1b`.

Fable froze Hub candidate `9b08743` without pushing it.
Commit `fb58248` contains the vendor repair. Commit `9b08743` adds the report.
The restored remote-close test remains uncommitted evidence.
Artifact `artifact_1788591665_945881` records the handoff.
Fable reports that the focused WebRTC, Unix, budget, and other selected gates passed.
Fable also reports that formatting and strict workspace Clippy passed.
The final ProcessExit test and exact remote-close retirement test still fail.
The dependency repair does not establish full Hub acceptance.
The former Hub writer now reviews the frozen candidate without builds.
Fable investigates terminal adapter ownership before changing the retirement test's expected result.
No matrix, pin change, pipeline advance, merge, or push is authorized.

Fable Web now owns the test window for the bounded reconnect repair.
Root approved one active attempt, one retry timer, capped retry delays, and an absolute attempt deadline.
Retries continue while recovery demand exists, until explicit disconnect.
Authenticated Hello resets the retry count.
An obsolete attempt must not change a newer attempt's resources or state.
Tests must include terminal-only recovery and obsolete rejection after a newer attempt succeeds.
Web must release the test window after this group. Registry broad checks remain pending.

Codex review `review_1788591822_123157` requires the missing vendor Cargo.lock in the committed dependency proof inputs.
Root requested a separate packaging correction without dependency updates.
Root compared the four modified vendor files with the published registry source and found no narrow queue-order source blocker.
That review does not establish Hub acceptance or coverage of every channel close state.

The registry owner supplied the remaining CI commands in message `msg_plugin-w_1788591873_3f8e13`:

```sh
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
cargo test -p botster-core --no-default-features --lib
cargo test --doc --workspace
cargo doc --workspace --no-deps
script/terminal-protocol-node-smoke.sh
```

Use the frozen registry worktree at `3b0b51374a6e54659e2f4649bab37cca532b94a5`.
Use a non-login shell, Rust 1.97.0, Node 22.21.1, Zig 0.16.0, `BOTSTER_ENV=test`, and two Cargo jobs.
Keep `CARGO_TARGET_DIR` unset and preserve the existing initialized Ghostty source.
Use new evidence files. Do not replace `/private/tmp/botster-registry-validation`.
These commands have not run under this handoff. Root must assign the test window before execution.
Exact Hub consumer validation remains a separate required step.

Hub committed the missing vendor lockfile in `4c8fa76` and the report correction in `bc04553`.
Root verified its SHA-256 against both the published registry copy and the standalone proof copy:
`e6697428d8d79939f1e071e44eaece9b0f4272e5a7079374595524a1878a5b02`.
Review `review_1788592146_703744` clears that packaging finding, but retains the unresolved acceptance conditions.
The clean-checkout dependency proof has not run.
Root authorized close-before-open and backpressure coverage plus a remote-close test through the production owner path.
The test must verify adapter cleanup, reservation and budget release, and continued sibling traffic.
The former control-only fixture omitted inventory reconciliation. Its timeout proves neither a leak nor successful cleanup.

Fresh Fable Core implementer `sess-1788592185-004d-be119fb4032accb998816af2ad75ea5c` owns the frozen registry worktree.
Spawn request: `msg_plugin-w_1788592185_2d0b95`.
The agent prepares broad validation but has no test window yet.
The current order is Web reconnect, the bounded Hub close group, then registry broad validation.
Root must review each window release before assigning the next group.

Hub candidate `5ac1b6999ebf972cf3597bb040fde373e15ac536` includes the reviewed test corrections.
Root assigned its five-arm proof group in message `msg_plugin-w_1788592932_831f95` after Web released the window.
The group includes the archived vendor proof, production-owner retirement, attributed ProcessExit, formatting, and strict Clippy.
Core broad validation remains ready and waiting.

Web froze reconnect candidate `76b1f896b10ddad78bae57368e836634ce17c885` over source commit `543d90e48b5c9a90a16e781a823d6381c47ec593`.
Fable reports passing targeted tests, full tests, build, and lint. The old-client control fails on retry lifecycle observation.
Evidence is under `/private/tmp/botster-web-foundation.sm0cZt/evidence/web-reconnect/`.
Root review requires corrections before merge:

- Synchronous lifecycle callbacks can disconnect before the client publishes readiness or installs a timer. Recheck ownership after those callbacks.
- An old close callback must not fail a new attempt that a lifecycle listener starts.
- The terminal-only test must complete the recovered binding and receive output, not only emit a new Attach request.
- A bootstrap provider that never settles needs an absolute-deadline regression, separate from explicit cancellation.
- The test header must describe the actual scenario coverage.

Root assigned these bounded source corrections to Fable in `msg_plugin-w_1788593010_179c13`.
Web has no test window during Hub's proof group. No Web merge or push is authorized.

The Hub production-owner remote-close test passed one test on `5ac1b69`.
The clean-checkout real-peer final-payload and active-pressure tests each passed one test.
The corrected queue-order filter passed one test. The original zero-selection logs remain preserved.
The close-before-open test executed and failed because it treated known buffer-threshold controls as unknown messages.
Root approved an exact-variant test correction after the independent reviewer traced those controls to `dial`.
The attributed ProcessExit test still fails on the exit subscription's own channel.
Evidence: `/tmp/hub-hard-close/fable-close-states/` and `/tmp/hub-hard-close/fable-clean-checkout-units/`.

Core broad Clippy first failed on a pre-existing test-only redundant `.into_iter()`.
Root approved only that correction in `3da05cbc8653ad87984ae3da604a38b721660fbe`.
Workspace Clippy then passed on that revision. Workspace tests are running under Fable Core's window.
Core must hold at the workspace-test boundary before starting the remaining four commands.

Web source corrections reached `b2f59cd721c1dce032d21faeeecb1f00ce13668e`.
Root also required recovery demand capture before synchronous callbacks and a linear pending-request partition.
Typecheck passed. The targeted run stopped at an old source-text guard before reaching the new recovery scenarios.
The test-only guard correction is `f01e221e50375a36f9bc8d28700262e4e2720227`.
Root then identified obsolete timer-event indexing after the deadline-installation order changed.
Fable must correct that test helper before the next assigned Web run.
No corrected Web acceptance or publication is claimed.

Core workspace tests initially failed with 804 passes and one parked-exit fixture failure.
The fixture waited for OS termination, which does not establish PTY reader finalization or exit-event publication.
Root approved a test-only positive-observation loop under the existing bound, preserving all negative non-mutating assertions.
That correction is `af798291b08f161de981827458e7926a27cc1a80`; production registry source is unchanged.
The exact corrected test passed. CI-exact workspace tests then passed with 984 passes and one existing opt-in ignored test.
The parallel daemon integration binary passed all 154 tests. All previously unreached crates and seven doc-test sections ran.
Evidence: `/private/tmp/botster-registry-broad-validation/02-test-workspace-rerun.log`.
Core released the window with no owned build, test, or worker process. Remaining broad gates still require completion.

Hub's bounded tally stopped on its first failure at `c65f2f0`.
The observation was `terminal_channel_closed:sub-exit:1:adapter_closed`.
This establishes the branch observed by the Hub driver, not the original cause of the adapter close.
Root rejected Hub-side parsing of opaque Core frames.
Fable may prepare a reversible Core-owned probe in isolated sources at exact pin `93acae3`, with close-site tags and bounded owner state.
The probe may not change active pins, shared caches, or production source. Root must review its provenance before execution.

Web's callback test required ownership of its rejected readiness promise and replies to both pending control requests.
Fable corrected those tests and added named scenarios with real, bounded timers.
Root assigned the next Web group on `caa33d29bf3d37a84ad6b5658d107eb1fd995912` after Core's workspace release.
Web must stop on the first unexpected failure. No Web merge is authorized.

## Latest delivered Web repair and active follow-up

Root reviewed Web source, all correction evidence hashes, and the final report precision changes.
All eleven named reconnect scenarios and full Web checks passed on `caa33d2`.
The callback mutant failed at its intended no-ready-after-disconnect assertion, with the exact repaired source restored afterward.
Root fast-forwarded and pushed Web main to `b55682250f83d02cbbd853c0967ec6d6f64dd1a8`.
Remote main matches that SHA and the main worktree is clean.
This supersedes the earlier publication holds for the reconnect repair only.
The integration matrix still uses Web `9e18b104`; no active integration pin changed.
Fable Web now prepares a plan for mounted clipboard transactions, with no source edits or test window yet.

Core's workspace run regenerated two nested consumer lockfiles with the new daemon `sha2` edge.
The earlier clean-tree report was incorrect. Root reviewed and approved committing those exact generated edges.
The passing workspace run exercised that resolution. Original logs and status snapshots remain preserved.
Core retains the window for its remaining broad gates after that provenance commit.
The isolated Hub/Core close probe remains prepared and reviewed, but unbuilt and unexecuted.

## Latest coordination: Core report accepted, isolated Hub probe resumed

Core registry source remains `57265996877349a69d3bef9a02e2222dac0bd868`.
The final report revision is `0c0a55b7e05fe94476f688401baa6cfaf99851ad`.
Root verified all 27 available source, correction, and run hashes against the committed manifest.
The report preserves the failed runs, lock drift, and overwritten passing log limitation.
Root accepted the broad validation report. Exact Hub consumer acceptance and publication remain pending.

The isolated Hub probe built both binaries but selected no tests because the fixture build script rejected a path dependency.
Root approved an isolated build-script exception for the exact exported protocol manifest path with a null Cargo source.
The pinned Git source check remains unchanged. The active ticket worktree must not change.
Hub owns the test window for at most five exact combined lifecycle runs, stopping at the first failure.
The implementer must preserve the failed attempt and verify the five protocol fixture files before the new run.

Web paste candidate `3e40e8f1fda3dacfc82a08e77fd0f193791d0388` remains unvalidated and unpublished.
Root requested source corrections for partial-delivery outcomes, clipboard allocation, context-menu fallback, and resolver ownership across attachment changes.
Root also required best-effort Abort on the captured live stream after a result bound, without claiming that Abort retracts PTY writes.
Fable owns these source corrections. Web has no test window yet.

## Probe permission checks and paste review follow-up

Fable's permission classifier denied the disposable build-script edit.
Root requested explicit escalation and received approval for that exact file.
Root applied the edit. Its SHA-256 is `0ef19b1d706ce28b64613eceafa7d74d96fd75ef2bc31640ccff480ecc3731d8`.
The predicate accepts only the exact protocol export manifest with a null Cargo source.
The active integration source and pins remain unchanged.

Fable's classifier then denied supervisor execution. No run3 test started, and Fable released the window.
Root read the entire supervisor before requesting execution approval.
Root required explicit zero-selection and build-survivor checks, plus exact pre-run source hashes.
Fable may correct the supervisor but must not execute it before approval.

Web correction `83823b415f0fd7c5cbe3cdf9f4c8fad7e78cedfa` adds distinct partial-delivery outcomes and generation-scoped result ownership.
It also removes the second clipboard read and fences outcome subscription setup after an asynchronous attach.
Root requested distinct exact-byte and lower-bound fields, and a bounded best-effort Abort.
These source changes remain unvalidated and unpublished.

Root assigned Fable Core to merge exact main `68ca23d` into the reviewed registry branch without changing main or publishing.
The merge must preserve main's close-contract comments and stop on any conflict.
Root requires source equality evidence before accepting the merge candidate.

## Reviewed merge candidate and active probe execution

Core registry candidate is `6f005c026db454ac776d97de2a6ef6e17ad7c8d1`.
Root verified its parents, clean tree, and diff from reviewed source `5726599`.
Only the approved close-contract comments and three validation reports differ from that source.
Core main remains `68ca23d`. Publication and exact Hub consumer acceptance remain pending.

Root requested explicit escalation for the supervisor corrections and execution. Both requests were approved.
The executed supervisor hash is `6996dc69d4f4d5741dd5f8d0c523749d93f042379f1e4dda5c3a3e22cada2698`.
Root started the isolated probe under terminal handle `16077`, with evidence in `/tmp/hub-hard-close/fable-core-probe/run3`.
Both binary builds passed. The exact lifecycle test is compiling and has not reported a result yet.
Fable Hub remains at its own permission prompt; root did not change that prompt or its general permissions.

Web typecheck passed at `653fc2b`. Its first paste test failed while waiting for frames.
The test-only correction `99cafe8` pumps control replies during frame waits and uses a two-second wait within each bounded scenario.
That run passed paste scenarios p1 through p5, then failed during p6 reserved-channel admission before paste submission.
Neither the initial unanswered-mode-request hypothesis nor accumulated fixture state is proven as the cause.
Fable may add cleanup in finally blocks and admission diagnostics, but Web has no test window during the Hub probe.

## Isolated probe run3 result

Root's terminal handle `16077` completed. Both builds and all five exact lifecycle executions passed.
Each execution selected exactly one test. The supervisor found no tracked survivors.
The five logs contain zero `core_hard_stop` lines. They do not identify the earlier intermittent close cause.
The test fixture pipes Hub stderr. Its successful shutdown path joins that stderr but does not publish it.
The fixture uses `CARGO_BIN_EXE_botster-hub`, not an independently pinned pre-test executable.
Cargo test rebuilt Hub: its hash changed from `1f66502d…` before tests to `db12ce0b…` afterward.
The worker and Core probe source hashes stayed unchanged. Preserve this binary-provenance limit.
Do not treat these five passes as approval of the earlier unresolved failure.

Web test-only candidate is `a16fad5b0388031976d396f011f62ff6dcbb6721`.
It adds fixture cleanup in finally blocks and bounded admission diagnostics.
After run3 completed, root assigned exactly one targeted Web run on that candidate.
No full integration matrix, source publication, or additional Hub run is authorized.

## Web paste fixture defect identified

The targeted run on `a16fad5` again passed p1 through p5 and timed out in p6.
Its last stage label did not identify the current await. Root required labels for p6's refusal and overflow waits as well as admission.
Root then found a concrete defect in `src/App.test.mjs` near line 3317.
The generated CommonJS protocol module declares `MAX_PASTE_BYTES` but does not export it.
The tested production module therefore imports `undefined`, which also makes its queued-byte limit `NaN`.
This invalidates the fixture's size-limit behavior; it is not evidence that the installed protocol package lacks the constant.
Root assigned Fable to export the actual package constants and assert parity before scenarios.
Fable must preserve earlier logs and stop after the single authorized targeted run.

## Core registry repair published

Root received explicit approval to fast-forward and publish Core main to `6f005c026db454ac776d97de2a6ef6e17ad7c8d1`.
The normal push completed. A fresh remote check matches that revision, and the main worktree is clean.
This delivers the reviewed Core registry source and preserves the close-contract comments.
Exact Hub consumer acceptance remains pending. No active Hub pin changed.

Web's corrected shim at `4fdef5a` passed package parity and finite-bound assertions.
Paste scenarios p1 through p7 passed, including size limits and key/resize/paste ordering.
The next failure was p8 waiting without servicing a required control request.
Root approved a test-only control-pump helper with one responder owner and one targeted rerun.

The proposed Hub stderr-forwarding patch cannot capture the intermittent panic path because that cleanup never joins stderr.
Root did not apply it. Fable Core may propose a bounded file sink inside the isolated Core probe instead.
The proposal must preserve production behavior and record the timing limit of diagnostic writes.

## Captured Hub probe and Web cleanup correction

Root reviewed Fable's bounded file-sink patch and applied it to the isolated Core export through explicit approval.
The modified `client_worker.rs` hash is `cc262630aa26e3d3d37e9384ac28642be4e1ce5ac3f58ef931e83e0ba1e95ec9`.
The sink writes at most 256 state records plus a limit marker per process, with each complete record capped at 1024 bytes.
It records no terminal payload. Its synchronous writes can change race timing.
Published Core and active Hub source remain unchanged by the probe.

Root ran the approved supervisor into `/tmp/hub-hard-close/fable-core-probe/run4` under terminal handle `64343`.
Worker build, Hub build, and test precompilation passed. All five exact lifecycle executions passed.
Each run produced its sink file, retained binary hashes, selected one test, and left no tracked survivors.
Every `wnx-exit` record identifies the normal completion branch at isolated `client_worker.rs:1500`.
Those records have `process_exit_delivered=true`, no in-flight write, and empty queues.
This proves capture works and describes those passing runs only. It does not establish the earlier failure's cause.
No further Hub run is authorized.

Web's eleven paste scenarios passed at `a3fec66` and again at `7934e87`.
Root found and corrected the view-host test's missing unmount action before the delayed attach release.
The corrected test then exposed a duplicate production unsubscribe call.
Fable fixed reference ownership at `aff42d7ba92e5022f30742bb5eba03700fab57b2`; root reviewed that source delta.
Web now owns one targeted run on that candidate. No publication or broad test group is authorized yet.

## Coordination permission hold

The Web run on `aff42d7` passed all eleven paste scenarios and the complete v1 cleanup test.
Its first failure was the v2 input-message finder.
Root verified that the minimal DOM stores React's class attribute in `attributes`, while the finder reads only `className`.
Root also identified a related test-helper risk: `textOf` ignores element `textContent`, which this minimal DOM stores independently of child nodes.
Neither observation establishes a production rendering failure.

Permission review rejected root's correction message to the established Web implementer session.
The earlier result message to the Core implementer was also rejected.
Root did not retry either rejected message through another tool or agent.
The correction instructions were not delivered, and no new Web run is authorized.
User approval is required to resume these agent messages. Local findings and existing evidence remain preserved.

## Coordination resumed with user approval

The user explicitly approved the messages and session inspection needed for orchestration.
Root sent the previously blocked Web fixture correction and Core result update through the normal Botster messaging tool.
The Hub implementer is responsive again. The active ticket worktree is clean at docs-only report revision `5e0f102`.
Core main remains clean at `6f005c0`.

Root assigned Hub a recording-disabled control tally on the exact run4 probe build.
Both probe environment variables must be unset. Source, lock, and binary hashes must remain fixed.
The group permits at most five exact lifecycle executions and stops at the first failure, wrong selection, changed binary, or owned survivor.
Hub owns the test window. Web may prepare its test-only DOM helper correction but must not run checks concurrently.
No production source change or publication is authorized by this test assignment.

## Recording-disabled control reproduced the Hub failure

Hub completed `run5-control` on the exact run4 binaries, with both recording variables unset.
The first three exact tests passed. The fourth failed with the original missing-ProcessExit and adapter-closed signature.
The group stopped. No sink appeared, binary hashes stayed fixed, and the implementer reported no owned survivors.
This establishes reproduction on the same probe build without diagnostic output.
It supports timing perturbation but does not prove that recording suppresses every occurrence.

Root rejected a background recorder as the next step pending a simpler probe proposal.
The implementer may capture fixed-size state before close, then format and write it after the existing close/drop sequence.
The proposal must preserve queue and adapter order and document its remaining timing and panic limits.
No implementation or next Hub run is authorized yet.
Web owns one targeted run on `b8f5f1d` after root reviewed its test-helper correction.

## Fresh Web agent with correct startup directory

The user authorized fresh agents and requested an unattended setup without repeated directory prompts.
Root verified Botster's `from_worktree` launch option in its running implementation.
The prior Web writer released its claim at clean `e969c74200aaba2812539e4aeb4bd372260c58ce`, with no owned process.
Root copied the immutable paste evidence into the worktree's ignored `node_modules/.botster-foundation-evidence/web-paste` directory.
The original evidence remains unchanged.

Fresh Fable session: `sess-1788622754-004e-e3480086588720801fd430e927269c3d`.
Botster confirms its startup worktree is `/private/tmp/botster-web-foundation.sm0cZt/web`, branch `foundation/web-readiness`.
Root transferred write ownership. The initial assignment is evidence verification and a negative-control plan, without execution.
The replacement must keep new scratch and evidence paths inside its worktree.
Root did not disable permission checks or change global settings.

Hub run6 captured the failing exit route in normal Core completion state.
The failure therefore occurs after adapter acceptance in that execution, not through a premature Core hard stop.
Hub is checking the dependency send/reset path and receiving event order, source-only.

## Receive-ordering review after the failing capture

The Hub implementer identified events-before-reads in the receiving WebRTC wrapper.
Root verified that order in `webrtc` driver source.
Root rejected a proposed guard limited to `DataChannelHandler.read_outs`.
`RTCPeerConnection::handle_read` drains that handler queue into `pipeline_context.data_read_outs` before the wrapper polls events.
A handler-only guard can therefore miss the pending application message.

The next source-only proposal must place the ordering barrier at the public peer-connection queue or the wrapper boundary.
It must cover both public read methods, preserve unrelated channel progress, and avoid timer spin during backpressure.
Required tests include the public events-first API, two channels, backpressure, and the existing failing wrapper lifecycle case.
No production change or new test group is authorized yet.

## Remaining foundation scope

The audit remains the source of the full requirements:
`docs/reports/2026-09-04-multiplexer-foundation-audit.md`.

| Work | Completion evidence still required |
| --- | --- |
| Shared-pump liveness | Delayed sibling progress during resize, spawn, snapshot preparation, and teardown; reviewed integration |
| Registry identity | Distinct IDs cannot overwrite, load, adopt, or remove each other's records; explicit old-record handling |
| TUI scheduling | Output wakes the event loop; requests do not block rendering; projection occurs once per required paint |
| Web user paths | Mounted large paste succeeds; bounded input preserves mode ordering; failures reach the user |
| Web recovery | Reconnect survives an unsuccessful attempt; optional subscriptions cannot block session discovery |
| Web instrumentation | Disabled instrumentation performs no terminal payload encoding or DOM payload writes |
| Server terminal authority | One authoritative server parser supplies replies, modes, readback, and snapshots; restart and attach remain correct |
| Retained history | Hub selects finite budgets; Core accounts for and releases retained memory; narrow reads avoid full snapshot copies |
| Runtime scaling | Targeted wake work avoids population scans; opaque frames avoid repeated encoding and unnecessary copies |
| Hub background work | Catalog refresh does not occupy the owner with filesystem work; keyed store reads avoid namespace scans |
| Supported stack | One exact compatible revision set passes actual first-party user paths continuously |
| Performance | Current optimized stack has recorded latency and total process cost on named hardware for the audit workloads |

Do not mark the full goal complete when only integration or resize is complete.
Do not infer numeric performance results from source complexity or old idle measurements.
Keep plugin workflow cleanup outside this foundation milestone.
