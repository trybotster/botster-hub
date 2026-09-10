# Final delivery dependencies

Started on September 8, 2026, at approximately 21:02 America/Los_Angeles.
This report records dependency checks and chronological checkpoints. It does not establish final acceptance.

## Current delivery state — September 9

- The user approved publication of all current changes, including direct pushes to `main`.
- Root verified Core main ancestry and pushed `b9e989be3232c72e966ce3fdb63878c82b70d94d` to both `main` and `foundation/completion-reservation` atomically.
- Root verified both remote refs after the push. Core publication is no longer blocked.
- Root reran Restty's three visible-screen-text tests; all passed. Root pushed consumed commit `71fbfeb9cbd356b112c922d101a94bab7413675d` to Restty `main` and verified the remote ref.
- Restty's dirty Ghostty patch matches its tracked build patch. Root preserved that working state and unrelated environment files without staging them.
- Main committed the published Core revision, combined allowance patch, provider error mapping, and inventory callers as Hub `b60ca68dcae7c8025d69784c9f77efdb9ea5a827`.
- Core, Restty, Hub, Web, and TUI are published on `main`. Source publication does not establish full foundation acceptance.
- The Core-pinned Hub check passed. All 130 focused tests passed against the new worker.
- The first broad library run had 926 passes and 18 failures. The serial rerun had three passes and 15 failures; concurrency does not explain all failures.
- Read-only review identified obsolete router, listener, and detach source guards. It also identified pressure fixtures that did not enqueue Core output or tracked future events incorrectly.
- Main and 0098 corrected test setup and lifecycle assertions under separate file ownership. Independent review found no production changes in these repairs.
- Root checked `hub-core-b9-repairs-final.log`: all 19 selected tests passed in 4.40 seconds, with 926 filtered tests.
- Shutdown now checks the exact cancelled Host job while workers are blocked. Required model cleanup can precede payload disposal.
- Root checked `hub-core-b9-library-final.log`: all 945 library tests passed in 43.98 seconds, with no ignored or filtered tests.
- Main confirmed exit status 0. Root verified a clean worktree and the exact 34 source files plus two authorized delivery documents.
- Root verified the candidate manifest against Hub `b60ca68` and Core `b9e989b`. All three binary sizes and SHA256 hashes match.
- The matched runtime inventory test passed: one test, zero failures, seven filtered, 5.47 seconds. Main confirmed exit status 0.
- Root pushed Hub `b60ca68dcae7c8025d69784c9f77efdb9ea5a827` to `main` without force and verified the remote ref.
- Web completed static checks against the committed exports. Root independently verified two codecs and 16 support files against committed source and recorded hashes.
- Web passed W-S1 through W-S5 against the matched artifacts. The run includes multiline-paste consent and in-page reconnect with new input and output.
- The Web negative control passed W-S1 through W-S4. Suppressing the channel close caused the exact expected W-S5 observer deadline failure.
- Both Web commands exited 0 and stopped their Hub processes. Root accepted the raw logs and authorized the exact nine-file source commit.
- Root verified Web commit `3072a2421db9e13f5636ef3f2c6764b465a46b97` and its unchanged accepted diff hash.
- Root pushed that exact Web commit to `main` without force and verified the remote ref. Recovery evidence remains untracked and excluded.
- TUI resolved all seven changed Git dependencies from canonical Hub and Core sources. No temporary local source remains in its manifest or lockfile.
- Root checked the TUI workspace log: 163 unit tests and two integration tests passed. The five live tests were ignored in that command.
- TUI's first live command failed during Hub startup with `Operation not permitted`, before it reached a client assertion.
- An unchanged retry received explicit escalation approval. All five exact live cases passed, but that runner used the default Rust 1.92 toolchain.
- A separate run selected Rust 1.97 explicitly and passed all five cases. The original failure and both passing logs remain preserved.
- Root checked the live log. The cases cover attach, reattach, paste consent, resize, and reconnect to fresh sessions after a Hub restart.
- TUI confirmed exit status 0 and froze its files. Root found no remaining matched candidate or live-test processes.
- Root updated the README checkpoint and committed the exact six files as `305399e5154b52293c81c8d7564cebff6f62581c`.
- Root verified a clean TUI worktree, pushed that exact commit to `main` without force, and verified the remote ref.
- Raw failing logs remain preserved as `hub-core-b9-library.log` and `hub-core-b9-failures-serial.log` in `/private/tmp`.

## Matched artifact and evidence record

The candidate directory is `/private/tmp/hub-core-b9-candidate-b60ca68`.
Its manifest records Hub `b60ca68dcae7c8025d69784c9f77efdb9ea5a827` and Core `b9e989be3232c72e966ce3fdb63878c82b70d94d`.
The final Hub documentation update does not change these tested source revisions or require client repins.

| Artifact | Bytes | SHA256 |
| --- | ---: | --- |
| `botster-hub` | 110910224 | `9923aefc3e5aa74fcc667ed1cbe92b07fd7af1bece1fa0cedf72571f2d2a15c1` |
| `botster-session-worker` | 5335960 | `97408440b5e3f71be34d9ade473e93d76d33d4bf006f30e78b11ca280b72dc0b` |
| `harness_control` | 12909808 | `12ed1c332411e02cbd51ae534529572a103e8931f9c674adc4f147b000381041` |

The manifest SHA256 is `7c08bce2895efc81f7c8d591eea13e94ff357085c118209503b90afaa6b6ba20`.
Root verified all three artifacts, including the optional adapter that the default two-binary verifier does not check.

Final Hub logs are in `/private/tmp`: `hub-core-b9-library-final.log`, `hub-core-b9-candidate-b60ca68-build.log`, and `hub-core-b9-matched-inventory.log`.
Final Web logs are in its recovery worktree under `recovery-evidence/final-b60ca68-static/`.
The Web directory contains static command results, `smoke-positive.log`, and `smoke-negative.log`. It remains excluded from publication.
Final TUI logs are `/private/tmp/tui-final-workspace-b60ca68-b9.log` and `/private/tmp/tui-final-live-b60ca68-b9-rust197.log`.
The latter SHA256 is `3e33d494eb31c7896a60173c620400a3de5f0645eec8f9a9c14e9bc4375d08e5`.
The tested TUI binary SHA256 is `cbdb60dcddbd099229ce622b7a38d5b88372ce03e2490234d946148a42118a17`.

Web's source lint passed with five warnings after the command excluded preserved browser evidence.
Plain lint traversed that evidence and failed. Root did not add a source configuration change to hide that failure.
Other test and build warnings remain in the raw logs.

This checkpoint does not establish complete owner work or memory accounting, optimized performance, software frame presentation, or the unresolved Host-worker loss policy.
Root did not update or restart the user's installed runtime.

## Earlier integration checkpoints

The entries below preserve intermediate states. They do not override the current delivery state above.

- Status work now shares the Hub integration worktree above `effa6b8`. Three writers own separate files; no Status build has run yet.
- Main completed borrowed-source preflight helpers for reconciliation, peer records, egress diagnostics, package iteration, and installation HOME.
- Status uses HOME captured at daemon startup. It does not reread HOME for each request. Missing, empty, and native paths retain distinct input states.
- Agent 0098 owns the aggregate input check and retained preparation lifetime. Carver owns bounded occupancy construction and installation receipt acquisition.
- The selected contract uses one original Host allowance across overlapping inputs and scratch storage. It adds no independent allowance or input limit.
- The receipt helper must exclude borrowed HOME storage and include owned derived paths, receipt parsing, and output overlap before acquisition.
- Independent review and focused tests remain pending. Core producer admission remains open until Hub consumes the reviewed Core revision.
- All three Status writers have frozen their files. Independent source review approved the coherent aggregate and receipt formulas.
- The receipt bound uses explicit diagnostic terms and exact-toolchain error-formatting evidence. It does not depend on source-file length or a new input limit.
- Three production-path tests cover source growth after Core submission and retained reservations across queued work and constructed results. Their execution is pending.
- Main now owns integration formatting and the sole Rust compiler window. The selected run covers 26 functions, including the existing terminal disposal test.
- This checkpoint still uses Core `ca24328`. Source review does not close Core inventory allocation before admission or establish allocator-capacity bounds.
- Status is committed as `d02b6102d0766fb4110af8953fd84fb5021cf512`. Root verified the clean integration worktree and exact nine-file scope.
- Root and the independent reviewer checked the raw logs: compilation passed in 24.07 seconds; all 26 focused tests passed in 4.66 seconds.
- The focused run includes actual source growth, queued/result reservation lifetime, Shutdown refusal, and normal/error terminal disposal. Main needed no source repairs.
- James adapted the separate allowance patch in `config.rs` and `lifecycle.rs`. The original eight-file index remains unchanged; review is pending and no build ran.
- Independent source review approved that adaptation against Core `b9e989b`. Canonical integration and execution remain pending publication approval.
- Root selected existing `entity_provider_frame_too_large` for provider `CompletionTooLarge`. The provider-specific integration and result-charge test remain pending.
- The convergence audit found no additional stranded implementation beyond the known allowance and client changes. The sampler changes are already integrated.
- The allowance patch applies to `effa6b8`, but its exact-capacity tests need adjustment for Core's logical metadata charge. Applicability is not acceptance.
- The allowance audit separates payload allowance from retained reservation bytes. Saturation tests must use Core's public metadata measurement, not fixed request counts.
- Two Hub cleanup/attach callers need client-aware live ownership. The existing generation accessor omits client identity and cannot safely replace their inventory scans.
- Root selected a borrowed Core lookup returning the live client and generation. James will implement it without builds, commits, or publication during Status work.
- The selected iterator removes inventory allocation but remains linear in live routes. These callers execute inside Core turns; no constant-time claim is made.
- This addition remains separate from reviewed Core `14da3895` until review and tests finish. The existing exact publication request does not authorize a later target.
- The lookup review and focused verification are complete. Root committed the five files as `b9e989be3232c72e966ce3fdb63878c82b70d94d` and verified a clean Core worktree.
- Root checked both raw logs: four lookup tests passed; the no-default-features library check passed with warnings. Populated cases exercise ClientWorker; daemon forwarding cases use empty engines.
- Root verified that the remote still points to `56cb0d8`. Root requested exact publication approval for `b9e989b`, replacing the earlier request for `14da3895`. No push occurred.
- Hub integration is 263 commits ahead of local `main`. This is an integration branch, not a completed mainline merge.
- Current delivery documents remain outside that integration checkpoint. Root must select and preserve them before handoff without adding unrelated files.
- The user approved the routing recommendation, safe cleanup, and local integration. Root assigned independent terminal-consumer review to 0095.
- Main integrated the four reviewed sampler commits as `38f9b86`, `71b22aa`, `cdf644e`, and `a66dfee` at an idle compiler checkpoint.
- Root verified identical sampler files against `81d7edd`, an empty index, and preserved unstaged terminal edits. No push occurred.
- Root removed five stale worktree records after a dry run confirmed their working directories were missing. All referenced commits remain reachable from branches.
- Root preserved existing directories, branches, uncommitted changes, and test evidence. Remote Core publication still requires the exact push confirmation.
- Independent follow-up review closed catalog duplicate receipt loss, client additional-receipt loss during disposal, and omitted snapshot admission retention.
- The shared regression run had six passes and one catalog fixture failure. The corrected catalog test and disposal-refusal test both passed in a separate run.
- The catalog terminal-slot duplicate branch has source review only; its duplicate test exercises the buffered-completion branch.
- Main implemented the catalog-owned identity from the same source retained by control state. Root inspected construction and the exhaustion-after-construction test.
- Root checked `hub-terminal-catalog-identity.log`: four catalog tests passed. Disposal retains the cache identity after later source exhaustion.
- Independent review approved the engine terminal phase. It takes the actual lifecycle owner after ordinary Host disposal and retains an existing-pool slot until acknowledgment.
- Root checked `hub-terminal-lifecycle-engine.log`: the actual normal/error serve test and four catalog tests passed.
- Separate destructor gates establish shared-engine and supplied test-runtime destruction on Host before mailbox closure. They do not establish Lua-specific or bridge-queue teardown.
- Independent review approved explicit coordination, publication, and spawner queue disposal after engine disposal. The bridge phase reuses the same permit and identity.
- Root checked `hub-terminal-plugin-bridges.log`: five focused tests passed, including actual normal/error serve exits and poisoned queue cases.
- Retained aliases observe emptied queues. Publication tests preserve independently selected descendant charges. Queue fixtures do not reproduce Lua timeout races or prove rejection of later external producers.
- Caller review excluded plain-spawn inflight cleanup from the daemon path: only synchronous compatibility helpers populate that vector. No additional inflight machinery was added.
- Main committed the local terminal checkpoint as `effa6b8f55fa29fdf6ed8612fab0d7972b352086`. Root verified its clean worktree and 25-file scope.
- Engine and bridge tests still use Core `ca24328` and require rerun after the reviewed Core pin update. No remote push occurred.
- Main committed the reviewed entity-model replacement as `95e2d849`. Root verified the clean worktree at that checkpoint.
- Provider preparation, model execution, publication, cleanup, resync, and scalar readiness have scoped implementation and review evidence.
- Root verified the final 69-test log. Main reports final compilation, formatting, and patch checks passed. This is not final artifact acceptance.
- Main committed the causal reservation prerequisite as `56db35c`. Root verified its source, 69 regression passes, seven later focused passes, and the final compilation log.
- The prerequisite retains queue capacity across owner phases and reports exact table application. No production model phase consumed receipts at that checkpoint.
- The model review transfer completed after approval. Reviewer 0095 closed the model, publication, cleanup, and resync findings; a separate reviewer closed scalar readiness verification.
- Client cleanup is committed as `3738b45`. Root verified the clean checkpoint after final review, 12 cleanup tests, three WebRTC tests, and 21 lower subscription tests.
- Root selected original connection permits plus retained bounded subscription slots, subject to complete provisional, retiring, and recovery lifetime coverage. No extra subscription Owner permit is selected.
- Agent 0098 implemented diagnostic construction and encoded-output accounting. Root inspected logs for two Status tests and five exact worker/disposal tests; all passed.
- Reviewer 0095 checked the test source and logs. This checkpoint does not close Owner seed admission, Core producer admission, buffered completions, or recovery teardown.
- Core checkpoint `c14383e` adds caller byte admission before terminal inventory allocation. Root reviewed source and logs for six passing tests: four inventory, one daemon forwarding, and one overflow.
- The no-default-features library test build failed before execution. Its feature-gated references exist in baseline source, but no paired baseline build ran.
- Core inventory output includes wrapper, rows, ID bytes, and capability storage. The contract excludes allocator metadata and spare capacity.
- Status must reserve its input allowance before the Core request. A non-fitting inventory must return the existing structured capacity error.
- Main owns the Unix connection wake repair within client cleanup. Root confirmed that the current transport waits on one selected mailbox and cannot wake from a missing lookup.
- The repair uses one stable connection reader/wake handle and releases the slot guard before blocking cleanup. The real Unix socket test proves sibling progress across the wake-registration race without timer ticks.
- Source review found that oversized Shutdown output could prevent the admitted stop operation. The selected correction carries an existing structured capacity error through stop and charged delivery.
- This correction changes the oversized-output response from transport failure to an explicit capacity error. Tests must preserve stop if fallback encoding also fails.
- Core completion draining still clones every `WorkerState` and sorts the resulting vector. These clones include plugin keys, manifests, handlers, and descriptors, not only handles.
- Root traced this call from Hub maintenance on the Owner thread. Core has no ready-worker index; its notifier carries no worker identity.
- Completion output limits did not bound that scan. Core replaced it with the reviewed completion store in `14da3895`; the worktree is clean.
- Core passed 57 no-default library tests, 22 worker unit tests, 46 worker integration tests, and the corrected allocation regression.
- The first allocation regression counted Darwin mutex initialization. Separate probes measured one allocation on first lock and none on later locks.
- The corrected regression compares empty and populated first-use drains, then checks repeated drains. Candidate passes; baseline fails with 196 versus three allocations.
- Root authorized a shared completion-store replacement after independent lifecycle review. It removes the old mailboxes, migration path, and worker census.
- The replacement preserves per-generation FIFO and retired FIFO priority. A fitting active completion can bypass an oversized retired head; active selection uses numeric front length.
- The unchanged global byte pool must charge payload allowance plus logical metadata before acceptance. Payload encoding limits remain separate from that total charge.
- A retired generation must remain addressable while any reservation remains, including a deadline winner that has sealed but not published its completion.
- Core added the missing test feature guard in `edb5c55` and clarified completion order in `b1ad9a1`. The full no-default-features library suite now passes.
- Diagnostic review found a delivery-submission refusal after the Shutdown stop phase. The fix must preserve the reply sender, shutdown result, encoded payload, and original permit.
- Root assigned guarded completion closure and exact-pool checks to 0098. Its fallible API must return the failed item and all remaining buffered completions.
- Main owns terminal-exit integration across normal shutdown and error exits. No selected mechanism yet resolves exceptional disposal refusal from `Drop`.
- Client cleanup run `hub-client-event-cleanup-tests-6.log` passed eight tests and failed two fixture assertions. Sibling delivery and both no-timer assertions passed.
- The Unix fixture sent final cleanup through a generic handler that does not process it. The other fixture exhausted the global queue before the client completion dispatcher ran.
- Root corrected its initial classification of those failures. Main fixed fixture routing and isolated client readiness; Root verified all ten tests passed in `hub-client-event-cleanup-tests-7.log`.
- Global ready-serial exhaustion now records an explicit Host drain fault. Its separate regression verifies retained receipt ownership and no later drain retry.
- Full control-state transfer to Host is rejected. A captured family work record contains an Owner-local causal reservation through `Rc<QueueState>`.
- Terminal disposal must leave causal reservation retirement on Owner while Host clears variable payloads under the original permits. Root authorized typed extraction and normal-exit integration.
- Root requested a user choice for complete Host worker loss: immediate process termination or a retained terminal fault state. No exceptional final-exit policy is selected.
- Status checkpoint `6266579` passed its isolated compilation and 12 exact tests. Guarded-close checkpoint `bf58bcc` passed compilation and four exact mechanism tests.
- Main integrated those checkpoints as `019e129` and `6464612`. Root verified the clean integration checkpoint; the later typed continuation check passed.
- Root inspected the gated Status test and `/private/tmp/hub-terminal-status-eight-gated.log`: one test passed in 0.19 seconds.
- The test retains all eight original request records, Owner permits, and Host permits while Host destructors remain blocked. A ninth Host reservation fails.
- After gate release, Host workers destroy all eight probes. The test then verifies empty request records, zero outstanding permits, and zero prepared bytes.
- This proves the exercised Status disposal ordering. The production shutdown driver, other retained record types, and terminal fault policy remain open.
- A bounded Chromium trace correlated animation identity with presentation feedback, but did not correlate the actual canvas resource. It does not establish key-to-paint latency.
- Root verified the installed ScreenCaptureKit display timestamp contract. Read-only preflight reported Screen Recording unavailable and Accessibility available.
- The TUI agent's scope guard denied the two new Hub harness scripts. Root requested explicit user approval and did not bypass that denial.
- Agent 0098 is implementing a raw Darwin process sampler in an isolated worktree. It must report unresolved lifecycle accounting instead of inventing complete CPU totals.
- No performance benchmark ran. Resource sampling must convert Mach CPU ticks correctly; executable UUIDs identify executables, not process lifetimes.
- Reviewer 0099 completed the initial accounting inventory. Independent reviews corrected its blanket no-bound and no-charge conclusions.
- Existing admission limits constrain causal occupancy; final allocation and inner-work accounting remain open.
- Current Status input copies precede aggregate admission: observability, reconciliation, and occupancy scratch overlap with the Core inventory. One original Host allowance must cover those live bytes.
- Status still resolves and parses the installation receipt on Owner. The selected correction moves this bounded input acquisition to the existing Host preparation phase.
- Current production cleanup reasons form a finite set of at most 44 keys. The large test key is injected; no new map cap is selected.
- Root rejected the sampler's first live check because unrelated process errors invalidated owned observations. Corrected discovery must select owned candidates before resource reads.
- The corrected sampler is accepted at `7819efd` in its isolated branch. It uses recursive child discovery and exact process identities, not a global resource census.
- Its native CPU conversion falls within the process-clock bracket. Two controlled samples contain exactly the owned root and child, with valid observations and no sampling errors.
- Complete lifecycle accounting remains unsupported. The optional stable-baseline mode passed source review and its focused checks.
- That mode must observe lifecycle events throughout the measured interval. Its scope is the fixed baseline rooted set, not all historical descendants or an atomic RSS peak.
- Root inspected `/private/tmp/measure-processes-stable-check-1.log` and the native stable, hidden-fork, and setup-limit evidence. All focused checks passed.
- Five warning-as-error builds, three test executables, three invalid-argument cases, and four controlled cases passed. The controlled cases include the unchanged raw mode.
- The stable case produced two valid samples. A process that started and exited between samples triggered `NOTE_FORK`, invalidated the interval, and suppressed aggregates.
- The setup-limit case also suppressed aggregates. These results validate the exercised sampler behavior; no Botster performance benchmark ran.
- Agent 0098 delivered the source coverage map after approval. The map inspected `301dec9` and ran no tests; it is not current acceptance evidence.
- The harness framing repair is integrated as `1931fe7`. Root reviewed source, reran nine process tests, and verified three Rust test passes and the adapter build log.
- Optional adapter artifact preparation is integrated as `476e3c8`. Root reviewed source and reran seven controlled-fixture tests. No full matched build ran.
- The protocol string remains `botster-hub-daemon-v1`; the repaired defects were framing, envelopes, correlation, and compatibility checks. The resource probe already reused one connection during polling.
- The optional adapter requires independent manifest size/hash verification; the existing two-binary verifier does not check it.
- The user activated unrestricted access for Root and reports the same change for the other agents. No review-transfer permission blocker remains.
- Core publication remains pending. The current remote check found `56cb0d8`; the clean local target is `14da3895`.
- Root verified fast-forward ancestry and requested approval for this exact publication. No push occurred.
- Root confirmed that `8435972` descends from the currently consumed `ca24328`. The terminal protocol, protocol client, and Ghostty crate paths have no diff between those revisions.
- Final completion allowance integration, provider error mapping, and canonical pins remain pending.
- Root repeated the eight-file allowance patch check against `6464612`; it applies. No patch was applied, and applicability is not compilation evidence.
- Web's recorded temporary checkout has no Git metadata and lacks 210 tracked files. The cause is unknown.
- Web recovery is complete at `/Users/jasonconigliari/botster-sessions/botster-web-foundation-recovery-20260909`, branch `foundation/web-recovery-20260909`, HEAD `7fca82be0ad6a79c653b9f3b32727471aa7dee15`.
- Root verified 478 tracked files with none missing, exactly seven modified files, and the unchanged binary patch SHA-256 `4665a7d75a994ca5ed83f4c74d0af9e5c51b3aa8498f934e8d62eb240bf71198`.
- The Web owner preserved the incomplete directory and copied its verified evidence. No build, pin roll, commit, or push formed part of recovery.
- TUI retains six modified files at `89c0be3`. Its owner confirms no independent prerequisite remains before final artifacts.
- Final matched client behavior, complete owner work/memory accounting, and optimized performance acceptance remain open.

Older sections below describe the source and evidence at each named checkpoint.
Later corrections supersede earlier open-item descriptions; they do not erase historical failures.

## Core publication

- The remote `foundation/completion-reservation` branch points to `56cb0d856169c318d1bf981a1274db3b285e1586`.
- The local branch points to `84359722bd34ad88b0249bc79cbd1ff9c087d784` and has no working changes.
- The two missing commits are `c895eff040d02dc5078d981f4d2f275330bfe335` and `84359722bd34ad88b0249bc79cbd1ff9c087d784`.
- `git ls-remote` verified the remote branch. `git merge-base --is-ancestor` verified that publication can use a fast-forward.
- Push approval remains pending. No push or merge occurred during this check.

## Hub completion allowances

The allowance worktree retains eight staged files at base `53aa28b2b9301bc9e8371c7215412fc2824e17f1`.
A read-only `git apply --check` accepted that staged patch against the current integration worktree.
This result establishes patch applicability, not compilation or behavior.

The source changes and canonical dependency updates must enter one integration commit.
The dependency update affects seven files:

- `Cargo.toml`
- `Cargo.lock`
- `crates/botster-hub-client/Cargo.toml`
- `crates/botster-hub-test-support/Cargo.toml`
- `crates/botster-hub-test-support/build.rs`
- `crates/botster-hub-test-support/src/lib.rs`
- `crates/botster-hub-test-support/src/conformance_data.rs`

The earlier allowance checks used Rust 1.92 and temporary path dependencies.
The canonical integration still needs Rust 1.97 checks and the provider error mapping.
Do not use the temporary path lockfile as the delivered lockfile.

## First-party clients

| Client | Current evidence source | Final dependency work |
| --- | --- | --- |
| Web | HEAD `7fca82b`; working generated artifacts identify Hub `50fc2f2` and Core `ca243281` | Import final generated artifacts and verify their provenance. |
| TUI | HEAD `89c0be3`; working dependencies identify Core `ca243281` and local Hub `76c4df8` | Replace temporary `file://` Hub dependencies with canonical final revisions and regenerate the lockfile. |

Both client worktrees contain uncommitted changes. This check preserved those changes.
Their intermediate results do not establish acceptance against the final Hub and Core revisions.

## Work that can proceed before publication

- Review diagnostic cleanup against the current owner-loop implementation.
- Check the final client test commands and evidence requirements without starting another compiler.
- Review source changes that do not depend on the unpublished Core API.

## Diagnostic scope correction

The current Hub source already removes diagnostic rows at exact lifecycle retirement.
Commits `279abd4` and `9411ad7` removed the two global pruning scans and protected replacement cells.
Do not assign that removal again.

The remaining source question concerns snapshot cost and lock progress:

- `EventPlaneCounters::snapshot_queue_ages` holds a registry read lock while it builds and sorts all rows.
- The Status continuation in `src/daemon/control/sessions.rs` calls that snapshot on the owner path.
- The Shutdown continuation in `src/daemon/control/host.rs` also calls that snapshot on the owner path.
- Registry registration and retirement use the corresponding write lock.

These source observations do not measure contention or establish its runtime impact.
Any proposed change must respect the existing limit on status redesign.

## Causal byte-accounting gate

The causal scheduler passes focused item-budget and no-control-traffic tests.
Those results do not establish the 256 KiB inspected-byte limit.
The current `opaque_move` charge assigns zero bytes while causal updates compare and clone identity strings.
The checked event-name and entity-type validators enforce shape but contain no length limit.
No new name-length policy has been approved or implemented.
The byte-accounting repair and its verification remain required.

## Combined causal verification

The integration worktree contains bridge notifier commit `21eba1f` and fair-store cursor commit `23754d4`.
The scheduler changes remain uncommitted above those revisions.
Root checked these logs after the integration agent ran the commands with Rust 1.97, two Cargo jobs, and incremental compilation disabled:

| Log in `/private/tmp` | Result | Scope |
| --- | --- | --- |
| `hub-causal-fair-bridge-tests.log` | 11 passed | Bridge readiness, store fairness, and retained operations. |
| `hub-causal-phase-combined-tests.log` | 15 passed | Causal phases, owner item budget, and production wake paths. |
| `hub-causal-phase-runtime-final.log` | 31 passed | Runtime tests. This selection overlaps the causal selection. |
| `hub-causal-phase-package-final.log` | 1 passed | Production package cleanup at full host capacity. |
| `hub-causal-phase-final-check.log` | Completed | `cargo +1.97.0 check -j 2 --tests`; warnings remain. |

The bridge tests deliberately poison stores. Their caught panic output does not indicate test failure.
The integration agent changed an integration test after the full test check.
That check does not verify the later test edit.
Matched integration runs and the final committed candidate remain unverified at this checkpoint.
These results do not establish the inspected-byte limit or final client acceptance.

### Local queue fairness finding

The integration agent found a separate fairness risk before committing the scheduler.
Root confirmed the relevant source behavior:

- `CausalOwnerPhase::next` places `Scope` before the same sequence of local phases in each cycle.
- `CausalScopeTable::flush_pending` removes at most one queued operation.
- Causal admission appends an operation when the pending queue has space.
- A rejected local operation returns to the front of its local queue.

If the pending queue stays full, a continuously refilled early local queue can take each slot that `Scope` frees.
Later local queues can then retain operations indefinitely despite receiving phase attempts.
The passing tests above do not disprove this case.
The integration agent is preparing a regression and an alternating `Scope`/local schedule.
The candidate integration must wait for the correction and its verification.

The regression `causal_capacity_reaches_later_finish_queues_under_continuous_refill` failed on the old scheduler at the expected admission assertion.
Root checked `/private/tmp/hub-causal-capacity-fairness-before.log`: one test failed; compilation completed successfully.
The reviewer independently confirmed that the mechanism affects any early local phase, not only the finish queues.
Verification of the correction must cover the general ordering property.

The first corrected run passed all 16 causal tests in `/private/tmp/hub-causal-capacity-fairness-after.log`.
The expanded regression covers finish-to-overflow and unsettled-to-bridge progress under continuous refill.
Both cases check eventual removal of the exact lease after admission.
The reviewer requested `pending_ready` as the table selector; the checked source still used `pending_ops` at this checkpoint.
This passing run does not verify that pending selector edit or establish final review acceptance.

### Finish operation ordering

The integration agent identified a separate ordering issue in the three finish queues.
Root confirmed that admission fills the first available queue and that separate phases drain the queues independently.
An operation in a later queue can therefore run before an older operation near the end of the first queue.
A release that runs before the transfer that creates its target identity cannot remove that identity.
The later transfer can then leave the identity retained.

The proposed correction uses one owner queue with the existing total limit of 768 operations.
It removes two queue fields and their phases. Other source stores remain outside this correction.
The agent is preparing an ordered transfer/release regression.
The correction, regression result, and final review remain pending.

The old implementation failed `causal_finish_fifo_preserves_transfer_before_release_across_old_segments` after compilation succeeded.
Root checked `/private/tmp/hub-causal-finish-fifo-before.log`: one test failed at the expected final identity assertion.
The test places a transfer at the end of the first segment and its target release in the next segment.
After all operations drain, the target identity incorrectly remains.
The reviewer confirmed the ordering mechanisms. Verification of the replacement queue remains pending.

The replacement queue passed all 17 causal tests in `/private/tmp/hub-causal-fifo-after.log`.
That run includes the previously failing transfer/release regression and the one-operation phase check.
The source retains the 768-operation limit and removes the two extra finish phases.

### Direct admission can bypass retained operations

The production review identified a separate path that the unified finish queue does not repair.
Root checked the corresponding admission and completion methods in the integration worktree:

1. Entity publication can retain a rejected transfer through `park_production` when the causal table queue is full.
2. The publication method still places the leased mutation in the fanout queue.
3. `Completion::Reclaimed` calls `finish_package_entity_fanout`.
4. The finish path tries direct causal admission before retaining the release.
5. If table capacity becomes available first, the release can enter the table before the retained transfer.

This is source evidence, not a completed runtime reproduction.
The reported sequence can retain an identity after its release has already run.
Production contention conditions, the correction, and decisive verification remain open.
Passing the finish-queue tests does not close this ordering requirement.

## Bridge ownership correction

The release-store reviewer withdrew the claim that first-party production workers access the five bridge release stores.
Root independently checked the repository callers and the Lua publication closure.

- Owner runtime methods call `mark_orphan`, `leave_source`, `park_release`, and `take_release`.
- `take_source` has no production caller.
- The Lua worker closure calls `publish`, which uses the separate publication queue.
- Publication timeout removes a request from that publication queue. The error branches do not retain causal operations.
- The release-store fields are private. The bridge has no destructor that adds release operations.

Worker access in synthetic tests establishes an API capability, not a production requirement.
The passing bridge tests do not establish production contention on the release stores.
The integration must reconsider the notifier machinery against this corrected ownership evidence.
Do not retain that machinery solely because its tests pass.

The integration agent reverted `23754d4` and `21eba1f` without creating a commit.
The working source no longer contains the bridge notifier or fair-store cursor.
The agent removed the corresponding integration hooks and synthetic worker test.
Root confirmed that the runtime still binds the genuinely shared causal table notifier.
`cargo +1.97.0 check -j 2 --tests` completed in 16.34 seconds with warnings.
Root checked `/private/tmp/hub-causal-owner-only-check.log`.
This build check does not verify runtime behavior after removal or close the broader ordering defect.

The subsequent causal test run passed 16 tests in 0.51 seconds.
Root checked `/private/tmp/hub-causal-owner-only-tests.log`.
The selection includes retained finish ordering, local phase fairness, and owner item-budget tests.
The removed synthetic bridge worker test no longer contributes to that count.
The production-method bypass regression remains separate and has not yet run at this checkpoint.

## Committed local correction checkpoint

Commit `e34f3c0` records the bounded causal phases, unified finish queue, and removal of the unnecessary bridge machinery.
The integration agent also ran the runtime selection: 33 tests passed in 0.60 seconds.
Root checked `/private/tmp/hub-causal-owner-only-runtime-tests.log`.
The production package cleanup selection passed one test containing four cases.
Root checked `/private/tmp/hub-causal-owner-only-package-tests.log`.

This is a local correction checkpoint, not final causal ordering acceptance.
The production-method bypass regression still requires a matched candidate and execution.

The candidate build subsequently completed in `/private/tmp/hub-causal-phase-candidate-2026-09-09`.
Root checked the build log and compared both binary hashes with `install-manifest.json`.
The manifest identifies Hub `e34f3c0eb862e3d0e6f7e4c168c45f539c334fe5` and Core `ca243281bc21adb30f13913d38669bbad27083a8`.

| Artifact | SHA-256 |
| --- | --- |
| `botster-hub` | `31581e03b1126b2701c834673879a04f20c77a4dcb8f29a9a66c5af37728f6a6` |
| `botster-session-worker` | `8066d2897656398cae7ae1183a1c602da62e8047d0d7f3560f9ceb3ddf654dcf` |

This development candidate supports the pending ordering regression.
It does not include the unpublished Core completion changes and is not the final performance candidate.

The matched lease and retry integration run passed 21 tests and failed three in 8.62 seconds.
Root checked `/private/tmp/hub-causal-phase-integration-tests.log`.
The failing tests are:

- `family_causal_is_one_global_fifo_and_258th_stays_at_source`
- `keep_owned_park_retry_stays_at_source_when_every_store_is_full`
- `production_fanout_finish_returns_the_513th_op_without_spinning`

The integration agent is checking the assertions against the current contract.
These failures remain unresolved. Do not classify them as obsolete fixtures without verifying their intended invariants.
The separate direct-admission bypass regression has not yet run.

The agent corrected the three scheduling assertions to inspect causal ownership and wait for the applicable phase.
The family assertion now requires exactly one operation to leave the queue when its phase runs.
The final identity assertions remain unchanged.
Root reviewed the test diff and `/private/tmp/hub-causal-phase-integration-retest.log`.
The retest passed 24 tests in 8.39 seconds and explicitly excluded the new bypass regression.

The new `production_fanout_finish_preserves_a_retained_publish_transfer` regression failed at its intended final closure assertion.
Root checked `/private/tmp/hub-causal-order-regression.log`.
The actual remaining identity was `AdmittedEntityMutation { family: "lease-probe.item", generation: 0, seq: 1 }`.
This run used production publication and fanout-finish methods with controlled queue pressure, not production Host scheduling.
The log also contains a second scheduling-related failure from before the final fixture corrections.
The later 24-test retest does not erase the reproduced bypass defect.

## Event retirement under backpressure

The event admission path discards the result of a causal release when plugin admission returns `Backpressured`.
Root confirmed that source path in `src/daemon_maintenance.rs`.
The agent added `backpressured_event_admission_retains_release_when_causal_table_is_full` using the existing forced-backpressure test hook.

The first run omitted `BOTSTER_ENV=test` and failed an earlier queued-holder assertion.
That run did not prove a release leak.
The corrected run used `BOTSTER_ENV=test` and failed the intended final scope-closure assertion.
Root checked the overwritten `/private/tmp/hub-event-causal-release-before.log`.
The actual remaining identity was `EventInFlight { request_id: "package-event-hub-worktree_created-1-1" }`.
The corrected run failed one test in 0.16 seconds.

The ordered retention replacement must resolve this defect and the publication/fanout ordering defect.
Both new regressions remain failing and uncommitted at this checkpoint.

## Asynchronous publication consumer gap

The agent reported a missing consumer for publication requests from asynchronous daemon handlers.
Root checked the current callers of the publication drain:

- Production calls reach the drain through synchronous `invoke_plugin`.
- The other caller is the explicit test helper.
- Asynchronous daemon handlers use `try_admit_plugin`.
- Worker publication queues a request and waits for its response. Timeout can retract a request that remains queued.

The checked asynchronous path has no publication consumer or publication wake mechanism.
This source finding concerns the genuinely shared publication queue, not the owner-only causal release stores.
The agent plans to add a bounded consumer and wake mechanism after the ordered-retention checks.
The asynchronous behavior still needs a production-path regression.

## Uncommitted ordered-retention replacement

The integration agent prepared one owner queue in `src/runtime/causal.rs`.
Root inspected its immediate reservation guard, shared reservation/operation count, FIFO insertion, and returned capacity on guard destruction.
The current queue limit is 256 operations plus reservations combined.
This inspection does not establish a complete capacity-policy or byte-accounting review.

Root checked `/private/tmp/hub-causal-owner-integration-first.log`: 11 integrations passed in 6.55 seconds.
The selection includes the previously failing publication/fanout ordering regression, retained finish state, and cleanup under full capacity.
Root also checked `/private/tmp/hub-causal-owner-tests-migration-check.log`: the full test build completed in 10.98 seconds with warnings.

The second library selection is still running at this checkpoint.
Its output already records a passing event-backpressure regression and passing reservation tests.
Do not treat that partial output as completion of the whole selection.
The changes remain uncommitted. Legacy table queue removal and the asynchronous publication consumer remain unfinished.

The second library selection completed: 36 tests passed in 2.13 seconds.
Root checked `/private/tmp/hub-causal-owner-runtime-second.log`.

The agent identified another publication identity risk during the asynchronous consumer review.
The current `PendingEntityPublish` identity contains the plugin key but not the publication token.
Two pending publications from the same plugin in one scope can therefore share an identity.
The agent plans to use the exact publication token and add wait-aware lease acquisition.
This identity correction and its production regression remain pending.

The agent subsequently integrated the table changes locally and added asynchronous publication service.
Root checked `/private/tmp/hub-publication-owner-first.log`: 28 selected bridge/table and causal tests passed in 4.14 seconds.
Root checked `/private/tmp/hub-publication-async-family.log`: two tests passed in 0.88 seconds.

The asynchronous test installs and enables a real Lua package through daemon methods.
Its tool handler publishes two mutations through `botster.entity_publish`.
The test requires both successful responses and an empty publication queue.
Root inspected `asynchronous_lua_publications_complete_through_owner_ready_work` to confirm that scope.
The other passing test covers four package cleanup cases at full Host capacity.

These are library tests against working source, not a final delivered Hub binary or matched first-party client run.
They do not alone establish same-scope publication identity safety or owner byte-budget compliance.
The current changes remain uncommitted above `e34f3c0`.

## Broader Lua integration result

Root checked `/private/tmp/hub-publication-integration-unsandboxed.log`: 46 tests passed and one failed in 45.23 seconds, with no filtered tests.
`real_lua_plugin_cross_package_managed_session_type_spawning` failed because the plugin handler exceeded its timeout.
The agent reports that the synchronous helper lacks a managed-queue consumer.
No pre-change failure baseline has run, so the historical attribution remains unverified.

The passing selection includes `two_publications_keep_distinct_pending_leases_before_owner_transfers_apply`.
Root inspected that test: two publications share one scope but retain tokens 1 and 2 separately.
Applying the first transition preserves the second pending identity.
After both fanout finishes and queue draining, the scope closes.
This establishes the tested same-scope identity behavior, not full foundation acceptance.

## Ordered publication checkpoint

Commit `e2b18e72ff39cc1657b14a04cb71ba0da09c2acd` records ordered causal ownership and asynchronous publication service.
Root verified the commit and a clean integration worktree.
The commit changes 15 files, with 2,947 insertions and 3,830 deletions.
Section 14 of the owner replacement plan records the current contract and remaining limits.

This checkpoint does not establish final acceptance.
The managed-spawn timeout remains unresolved.
Causal identity byte accounting remains incomplete.
Publication parsing, validation, and rejected-payload destruction still run on the owner and require further work.
No final matched Hub binary, client run, or publication is established by this commit.

Final client runs require the integrated candidate and matching generated artifacts.
Performance acceptance requires final artifacts and the measurement conditions in the acceptance plan.
Neither requirement is complete at this checkpoint.

## Publication disposal checkpoint

Root verified commit `c67c053b1629262bf8e4ae440f97b4ccf31a05c6` and a clean integration worktree.
Root inspected the daemon dispatcher, publication state, retirement transitions, completion routing, and focused test bodies.
The daemon reserves Host and Owner capacity before it removes a publication from the bridge.
Rejected mutations retain their original payload for disposal by a Host worker.
The daemon retains the completion and both permits while causal retirement waits for capacity or encounters a fault.
Stopped Host submission retains the original command.
Resync scheduling preserves the pending publication identity until disposal finishes.
The focused regression checks both disposal and resync release orders.
Shutdown waits for publication ownership, and the final bridge retraction wakes its waiter.

Root inspected these completed logs:

- `/private/tmp/hub-publication-disposal-verified.log`: 32 library tests passed in 4.20 seconds.
- `/private/tmp/hub-publication-disposal-integration-verified.log`: 13 integration tests passed in 6.63 seconds.
- `/private/tmp/hub-publication-disposal-async.log`: the asynchronous Lua daemon publication test passed in 0.32 seconds.

These results support the tested disposal ownership contract, not complete owner-budget compliance.
Family admission still releases consecutive pending mutations in one owner step.
The owner still clones the provider-family set and checks current family ownership.
Incremental family admission and causal identity byte accounting remain open.
The managed-spawn timeout, final artifacts, matched clients, performance acceptance, and publication requirements also remain open.
The integration tests still use the earlier candidate worker with the current linked Hub library.

## Incremental publication admission checkpoint

Root verified commit `7b6ef986eb257ce8715c30ebc1300509e8c7d939` and a clean integration worktree.
Root inspected the family admission change, daemon continuation, runtime transitions, and regression bodies.
Family admission no longer drains a complete consecutive run in one activation.
Each continuation moves at most one pending mutation with its existing lease.
The continuation retains the exact family generation, response, and original Host and Owner permits.
A gap or removed generation ends the continuation.
Sequence exhaustion preserves the pending row and retains recovery ownership.
Tests also check response cancellation, replacement generations, snapshot progress, and shutdown after continuation.

Root inspected these completed logs:

- `/private/tmp/hub-publication-incremental-verified.log`: 16 publication tests passed in 6.24 seconds.
- `/private/tmp/hub-publication-incremental-family.log`: 20 family tests passed.
- `/private/tmp/hub-publication-incremental-final.log`: four continuation, asynchronous publication, and shutdown tests passed in 2.37 seconds.
- `/private/tmp/hub-publication-incremental-integration.log`: eight Lua entity tests passed in 6.92 seconds.
- `/private/tmp/hub-publication-incremental-causal-integration-verified.log`: ten causal, fanout, and resync tests passed in 6.14 seconds.

These selections overlap. Their counts are not a unique test total.
The earlier `hub-publication-incremental-owner-final.log` failed a test fixture assertion before the exhaustion check.
The later four-test run passed the corrected continuation test. The earlier failure does not prove the target defect.

This checkpoint addresses the consecutive-run admission requirement.
It does not establish byte or time bounds for the work inside each activation.
Causal identity storage bounds and owner inspection charging remain the next checks.
The other final acceptance requirements remain open.

## Partial fixed-size causal identity checkpoint

Root verified commits `1c924af1c06ffa63811fb9f86587175bd59f231a` and `497e04ceeb6863f5b5dd04c684629d66f9f75e9f` and a clean integration worktree.
Root inspected the identity definitions, token allocation, transfer application, event scope creation, and provider retirement changes.
Transfers now contain three optional targets instead of a variable-length vector.
Pending publication identities contain the publication token, without the plugin string.
Each event uses a fixed identity inside its newly minted scope.
Provider identities use invocation tokens instead of request strings.
Provider retirement retains the original scope ID and invocation token.
Scope allocation stops after exhaustion instead of wrapping into existing identities.
The provider regression checks distinct leases with the same request ID, token exhaustion, and retirement after causal capacity returns.

Root checked `/private/tmp/hub-causal-event-provider-verified.log`: 24 selected tests passed in 2.49 seconds.
Root checked `/private/tmp/hub-causal-provider-admission.log`: the refused-admission retirement test passed in 0.20 seconds.
Root checked `/private/tmp/hub-causal-event-provider-check.log`: the check with tests completed in 11.60 seconds, with warnings.

Family identities still contain strings. Owner inspection accounting remains open.
These changes therefore establish only part of the required identity storage bound.
The agent reports no push. Its report of user permission in another session does not change Root's prior permission boundary.

## Fixed-size family identity checkpoint

Root verified commit `0391be95c330fec03a227a837dd28d4688ab6230` and a clean integration worktree.
Root inspected family token allocation, admission, cleanup releases, identity definitions, and the two token regression bodies.
All `LeaseIdentity` variants now contain only fixed-size values.
Transfers retain their three inline optional targets.
Family tokens distinguish different families and recreated families, including recreation with the same generation value.
Admission allocates the token before it changes the family for the first scoped publication.
Exhaustion returns the original payload for disposal. Existing tokens and releases remain usable.
Cleanup releases use the retained token instead of reconstructing identity from the current family.

Root checked these logs:

- `hub-family-token-lifecycle.log`: 119 passes and four socket setup failures.
- `hub-family-token-socket-rerun.log`: all four socket-dependent tests passed on rerun.
- `hub-family-token-integration.log`: 11 selected Lua integration tests passed in 6.50 seconds.
- `hub-family-token-final.log`: six focused tests passed in 0.29 seconds.
- `hub-family-token-final-check.log`: the check with tests completed in 11.40 seconds, with warnings.

These files are in `/private/tmp`. The selections overlap and do not establish a unique test total.
This checkpoint completes the fixed-size causal identity representation, not queue byte accounting or owner inspection accounting.
Other retained structures still contain family strings.
The integration still uses the `e34f3c0` candidate worker with the current linked library.
Final artifact and full foundation acceptance requirements remain open.

## Causal queue payload and metadata checkpoint

Root verified commit `0749e4f2d250cc8b05b356a4de0758d08c61f2df` and a clean integration worktree.
Root inspected queue construction, the payload bound, the daemon charge, and the new regression bodies.
The queue preallocates space for 256 operations.
Its occupied payload bound is `256 * size_of::<CausalOp>()`, excluding the queue header and allocator overhead.
The daemon charges supplied operation metadata before it removes the head.
If the charge fails, the daemon preserves the head and schedules another activation.
The allocation regression fills and wraps the queue while checking that capacity remains unchanged.
Root checked `/private/tmp/hub-causal-fixed-queue-test.log`: all three selected tests passed in 0.27 seconds.

This is not complete owner inspection accounting.
Table lookup, comparison, and allocation costs remain separate.
Provider-family cloning and descriptor scans also remain open.
Main will trace worker preparation and registry lifetime before selecting a replacement for those costs.

## Retained resync scope lifetime finding

Root inspected the uncommitted production-event regression above `0749e4f`.
The test installs and enables a Lua package through daemon requests.
Nine event handlers publish out-of-window sequence 100 through asynchronous owner work.
Each iteration waits for successful handler completion, empty causal work, completed publication retirement, and zero outstanding Owner permits.
The final assertion finds nine scopes, each containing only its resync identity.
Root checked `/private/tmp/hub-resync-scope-lifetime.log`: the test passed in 0.31 seconds.

This disproves the assumption that execution permits alone bound retained resync scopes.
It does not measure indefinite growth or prove that provider snapshots ran during the test.
Main separately reports a source path where later publications rearm resync while snapshots remain below the high-water mark.
Retained scope lifetime remains a blocker to the existing memory requirement, not an expansion of product scope.
The closed causal queue payload bound remains valid at its stated scope.
No table limit or replacement policy has been selected.

## Publication lifetime and registration checkpoints

Root verified commits `9401c8b1b1c9e0089d903c1429d4727a5e696b50` and `f148470b4a452fd10d7b3d8b8c0ec8710dc4981e` and a clean integration worktree.
The lifetime change retains publication count and byte credits through descendant retirement instead of returning them at queue removal.
Root inspected the accounting record and the resync saturation regression.
The regression retains 256 resync scopes, refuses another publication, and returns credits only after queued retirement applies.
It also invokes a Lua provider at saturation: the provider rejects a new publication but still returns its snapshot.
This addresses the demonstrated resync retention path. Root has not completed a full memory inventory.

The registration change moves package/family selection and namespace validation to the Lua worker before enqueue.
Root inspected the registration index, lifecycle ordering, bridge preparation, and queued-publication regression.
Queued requests retain a fixed live record. Replacement permanently invalidates old records.
The index contains current records only; retained requests own old references.
Lifecycle code calls Core unload, replaces registrations, and then calls Core load.
This ordering depends on serial package effects. The index does not serialize concurrent lifecycle calls.
An old request can pass admission before the registration swap during unload.
Worker shutdown and record invalidation are not atomic.

Root checked these logs in `/private/tmp`:

- `hub-publication-lifetime-rust197-final.log`: 93 library tests passed in 11.40 seconds.
- `hub-publication-lifetime-provider-saturation.log`: ten lifetime tests passed in 1.36 seconds.
- `hub-registration-final-tests-with-worker.log`: 37 library tests passed in 8.47 seconds; a separate integration selection had four passes and one failure.
- `hub-registration-integration-final.log`: the corrected reserved-release integration passed, followed by the Core cleanup lifecycle integration.
- `hub-registration-contract-final.log`: the namespace and format refusal test passed.
- `hub-registration-check-final.log`: the check with tests completed in 12.22 seconds, with warnings.

The reserved-release fixture now queues a valid request and unloads its package before admission.
The former unowned-family fixture fails before enqueue under the new contract.
The corrected test preserves the reserved-release requirement.
Earlier failed logs remain separate from the passing results. Test selections overlap.
These tests use the existing candidate worker and the current linked library, not final matched binaries.
Family-string routing, complete owner work and memory accounting, final Core pins, and matched client verification remain open.

## Finite provider admission checkpoint

Root verified commit `6960a0fa045be2d698566c8565569fb751534700` and a clean integration worktree.
Root inspected request preparation, Host admission, retained stages, and the exact-lease boundary test.
Host workers construct provider requests and execute Core admission encoding.
The owner acquires the selected causal token before Host admission.
Confirmed admission releases Host capacity while a separate shared-view charge retains the minimal expectation metadata.
The charge accounts for logical record and string bytes, not allocator overhead.
An early Core result waits for the Host admission result.
Ambiguous Host failure retains the exact lease, result bytes, metadata charge, and original permits after cancellation.

Root checked the 57-test regression log and the two-test boundary log.
The latter passed in 0.60 seconds and includes eight admitted providers that publish through real Lua while Host slots remain available.
Root checked `/private/tmp/hub-provider-cancel-final.log`: the expanded five-case test passed in 0.94 seconds.
Root inspected its preparation cancellation, acquisition cancellation, delivery, admitted cancellation, and fault selection.
Root checked `/private/tmp/hub-provider-check-final.log`: the check with tests completed in 11.33 seconds, with warnings.
The injected failure is a generic Host failure result, not a demonstrated panic inside Core admission.
These results establish the exercised provider phase boundary, not complete owner work or memory accounting.
Family selection, family model execution, descriptor readiness, and variable-length completion routing remain on the owner.
Final Core allowance integration, canonical pins, and matched artifacts remain pending.
