# Foundation integration and final acceptance

Status: source publication complete on September 9, 2026. Full foundation acceptance remains open.

## Active repair: plugin rendering on September 9

Jason requested orchestration of the Workspaces render failure under the existing north star.
The required outcome is a usable plugin surface with configured spawn targets and responsive sibling operations.
Jason explicitly included both the stall repair and callback memory accounting in this delivery.
Callback accounting is not deferred as a known limit of this repair.
This repair does not certify the remaining foundation performance or accounting gates.

- Core retains generic execution and storage mechanisms.
- Hub owns capability admission, session-type resolution, completion delivery, and error correlation.
- Lua retains workspace behavior. Web validates and renders the canonical surface response.
- Root owns reproduction, integration, and the compiler window. Diagnostic agents have read-only source assignments.
- Preserve the user's running runtime. Use an isolated data directory for mutation and restart tests.
- Do not restore the synchronous plugin pump to the daemon. Do not add owner waits, periodic polling, or compatibility paths.
- Retain accepted work and its budget through completion, cancellation, failure, and disposal.

Root reproduced successful Workspaces rendering with no spawn targets on the published matched artifacts.
Rendering also succeeded after an isolated restart.
Adding one directory spawn target caused the render request to exceed an eight-second observation deadline.
The repeatable isolated probe is `/private/tmp/botster-plugin-render.MmT98Z/probe-render.mjs`.
Against the published candidate, it failed at the render-deadline assertion after 8,004 milliseconds; concurrent status completed in 12 milliseconds.
The probe verified the configured target before rendering. Its earlier sandbox-denied setup run is not defect evidence.
This Unix-path check does not establish the WebRTC timeout response or browser rendering.
On September 10, Root ran the same probe against the separate diagnostic candidate in `/private/tmp/botster-plugin-render-candidate.QqYTjY`.
The probe passed with exit status 0. Rendering returned the canonical snapshot in 7 milliseconds; concurrent status completed in 7 milliseconds.
Root verified all three artifact hashes against the candidate manifest and stopped the isolated daemon cleanly.
The candidate records dirty source separately in `dirty-source-provenance.txt`; it is not a release artifact.
The observed result is preserved in `root-render-proof.json` in that candidate directory.
This closes the representative stall regression for that candidate, not callback accounting, browser behavior, or installed-runtime delivery.
Subsequent corrections changed `src/lua_runtime.rs` and `src/session_types.rs` from their recorded build hashes.
The corrected candidate must pass the render probe before delivery acceptance. The earlier result remains valid only for its recorded artifact.
Source review found that the queued session-type lookup has no asynchronous daemon consumer.
The ordinary session-type spawn queue shares the obsolete synchronous consumer and needs review in this repair.

WebRTC timeout responses also omit structured errors. Web can incorrectly accept those responses as render success.
The missing-snapshot error is therefore a separate error-reporting defect, not the lookup stall's cause.

Acceptance requires the following evidence:

1. A regression must fail on the current production path with one configured spawn target.
2. The repaired daemon must return the canonical snapshot without a synchronous helper pump.
3. Session-type list, show, and ordinary spawn must use reachable consumers with explicit capacity and completion wakes.
4. Cancellation and shutdown must preserve admitted work ownership and release retained capacity.
5. Timeout and closed-channel failures must retain request correlation and must not trigger automatic action retries.
6. Web must display the actual error and must continue rejecting malformed successful snapshots.
7. A matched client check must demonstrate the repaired surface while sibling status and session operations remain responsive.
8. Callback accounting must cover fresh Rust allocations, conversion overlap, and retained Lua results through release.
9. Ordinary-spawn admission must charge retained payloads before construction or queueing and preserve cleanup ownership after caller timeout.
10. Shared session-type resolution must not perform repository I/O on the owner thread, including the managed-spawn caller.

Current blockers: callback builders need pre-allocation accounting, and spawn needs a reachable asynchronous consumer.
The representative direct-read daemon proof passed; full list/show semantics and memory accounting still need verification.
Ordinary-spawn cleanup needs reviewed ownership through Lua conversion failure, not only channel delivery.
A Claude agent through Botster confirmed the missing consumers and reviewed the proposed repair.
The review rejected immediate shutdown on spawn timeout because shutdown can precede session creation and cleanup admission can fail.
The review also corrected the response-retirement diagnosis: status-family rows are exposed; plugin rows already require completion.

Root approved implementation of structured WebRTC errors and the status-family response-retirement fix, with focused regressions.
The server keeps its resolved daemon-response contract. Errors must include the request ID, operation, code, and existing diagnostic.
Hub phase 1 is committed locally at `f4717afe7cd271e7fb3f99bcba9a4484d0e70177` after Claude review.
Web's generated protocol and provenance match that revision. Root ran the full Web suite and type check successfully.
The Web suite covers local timeout, interrupted requests, send failure, and untyped rejection. React `act()` warnings remain.
Other Web action branches retain their existing error projection; this phase changes plugin rendering only.
No repair is installed. The callback and spawn work has scope approval; production memory limits still require Jason's decision.
Root will integrate the accounting boundary with the direct read path and the retained spawn lifecycle.
Independent review must check both allocation ownership and the actual daemon path before acceptance.

Root approved parameterized implementation after the memory measurements. Production limit values await Jason's response.
The proposed limits are 16 MiB per Lua state, 128 MiB across states, 8 MiB per Rust callback, and 64 MiB across callbacks.
These are new policy values, not a reuse of completion accounting. The implementation must not retain an unbounded fallback.
The retained-callback probe sampled 8,220,420 bytes after 64 results containing 128 targets each.
The proposed state limit provides about twice that sample. The aggregate permits four first-party states and four overlapping replacements.
The first implementation slice must prove the actual one-target render path before the remaining spawn lifecycle work accumulates.
The initial source provides explicit-limit reservations and a bounded loader that production does not yet use.
An aggregate reservation alone does not enforce the allocation limit within one callback.
Source reads must enforce the limit during reading, because file metadata can change before reading.
Spawn cleanup must retain ownership until Lua conversion succeeds or cleanup completes.
This requirement applies to ordinary spawn and managed spawn.
Managed cleanup must preserve existing or reused worktrees and follow the existing rollback policy for newly created artifacts.

The first builder estimates did not establish the required allocation bound. Root paused those edits before acceptance.
The estimates omitted a relative-path field, collection storage, and parser scratch. Unexplained margins did not correct those omissions.
The proposed replacement borrows package and device definitions and uses flat source references instead of grouping maps.
A repo catalog projection can validate environment fields without retaining their map, because list/show do not expose environment values.
The remaining design check concerns the temporary allocations inside the exact JSON parser and Rust toolchain.
The byte contract covers live allocation requests and distinct overlapping copies; it does not claim a bound on process RSS or allocator bookkeeping.
Root has not approved a weaker encoded-content estimate, a custom JSON parser, or a process-wide allocator replacement.
Independent review accepted an exact-version mechanism with checks for dependency and toolchain changes.
The audit uses `serde_json` 1.0.150 with `std` and `raw_value`, without numeric-allocation or unbounded-depth features.
The proposed scratch reservation is `3 * max(8, raw_json_bytes)`, including old and new storage during reallocation.
Input capacity and retained projection allocations require separate charges. Fixed-message visitors must cover error paths.
Rust 1.97.0 uses the audited vector growth behavior. Implementation will record the toolchain and parser versions explicitly.
The corrected implementation and its feature checks still require source review and tests.
The reviewer verified the scratch calculation with three conditions: use `from_slice`, use fixed-message schema errors, and destroy the admission parser before the owned pass.
The audit must also include the scratch stack used to skip nested containers.
Root completed the build-contract gate while the implementer continued the catalog code.
All 14 gate tests passed, including command failures, invalid metadata, and unsupported compiler, version, and feature cases.
The real gate rejected the shell's `RUSTUP_TOOLCHAIN=1.92.0` override, then passed with `RUSTUP_TOOLCHAIN=1.97.0` and offline Cargo resolution.
CI and `test.sh` run the gate tests and the real gate. Independent review accepted the gate with no blockers.
Root added the requested audit reference and clean metadata errors, then reran all 14 cases and the real gate successfully.
The owned parse also depends on `serde_core` collection visitors and `serde_derive` output.
Root pinned `serde` to 1.0.228 and checked its direct dependency edges for matching `serde_core` and `serde_derive` versions.
The expanded gate passed 25 cases and the real offline Rust 1.97 check. Independent review accepted this extension with no findings.
The real check exposed Cargo's `proc-macro` annotation. The fixtures now include that annotation and deduplicated edges.
Root also found uncharged path construction and validation collections in catalog helpers. The implementer corrected those paths; verification continues.
The corrected checkpoint passed nine catalog tests, one bounded Lua test, and one direct Lua integration test without spawning.
Two earlier integration runs failed candidate-manifest verification before they tested behavior. The corrected manifest passed verification.
The bounded Lua test uses an explicit account. The integration test still uses the unbounded production loader and proves only direct-read behavior.
Root then identified missing error storage. Serde constructs owned errors even when visitors use fixed messages.
Independent review confirmed that tiny invalid inputs can exceed the parser scratch reservation through these fixed allocations.
The implementer must add an unconditional, source-derived error reserve before the catalog slice can pass allocation acceptance.
Root completed that reserve and charged the temporary C string used by Unix `File::open` for long paths.
The corrected catalog passed 12 focused tests and independent source review with no findings.
The bounded Lua test also passed against this source. This closes catalog-builder admission, not complete callback accounting.
The broader session-type filter passed 13 tests, including the daemon catalog check.
A subsequent test-only change verified the long-path C-string reservation at its exact boundary; that test passed separately.
Root found that `mlua` stores Rust traceback strings and error causes in Lua userdata after a callback returns an error.
Lua can retain that userdata after a stack-local callback charge drops. Callback integration must account for this retained Rust storage.
An error-size bound alone does not establish retained ownership. The charge must survive for as long as the error storage survives.
Root rejected the proposed `MultiValue` input because argument conversion can clone retained errors before callback admission.
The reviewer also withdrew an unsafe-API requirement after Root traced the exact single-`Value` return implementation.
A known non-error `Value` uses a different implementation from `MultiValue`; the cited stack check does not occur on that return path.
The proposed boundary validates argument types in a trusted Lua wrapper and passes only the required strings to Rust.
Rust would handle conversion failures inside the callback and return one Lua-owned result or error string.
The Lua wrapper would raise the error string after Rust returns. Runtime cache allocations still require review.
This changes caught error values from userdata to strings. Jason approved the Lua-owned error architecture after checking the vault's ownership goals.
Hub keeps admission and capability enforcement. Host and client protocols keep structured failure codes and request correlation.
The diagnostic `session_type_error_storage_outlives_callback_charge` passed with one test on Rust 1.97.0.
It retained 64 error userdata values across Lua collection while the callback account reported zero bytes.
This passing diagnostic confirms the current ownership defect. It is not an acceptance test for the proposed repair.
Root then implemented the list/show wrapper. The Rust callback returns one non-error Lua value and never propagates an expected Rust error.
The wrapper raises Lua strings. Catalog failures cross through a private Lua table before the wrapper constructs the diagnostic string.
Invalid UTF-8 checks use borrowed bytes and `std::str::from_utf8`, not the allocating `mlua::String::to_str` error path.
The retention test now requires 64 Lua string errors with zero callback usage after collection.
The four focused Lua tests passed against the final wrapper source. Independent review found no blockers to the retained-error change.
The tests cover retained strings, invalid and foreign-error arguments, global rebinding, aggregate refusal, and result-conversion allocation failure.
Runtime cache accounting and transient conversion-error admission remain open.
The first two pressure tests reached Lua's own memory failure, not the intended result-conversion fallback. Those failures are not conversion-path evidence.
The third fixture exceeded the existing description limit during warmup. The corrected fixture uses 16 valid descriptions and reaches conversion failure.
Root pinned `mlua` to 0.11.6 because the boundary depends on its exact argument and single-value return implementations.
The build gate checks direct and resolved `mlua` features. All 31 gate cases and the real offline Rust 1.97 check passed.
These checks preserve the audited source assumptions. They do not establish complete `mlua` cache or transient allocation bounds.
The existing cross-package list/show integration test also passed once against the fresh linked Hub library and verified Core worker.
That test uses the unbounded production loader. It does not prove the packaged Hub binary, daemon rendering, or production memory policy wiring.
Argument metamethods can call other plugin or Hub functions. Their failures remain outside this narrow list/show error-construction guarantee.

The next spawn slice will reuse the existing owner request rows and Host executor.
Ordinary spawn needs an asynchronous consumer; managed spawn needs its shared resolver moved off the owner thread.
Both paths must retain the original callback charge through Host resolution, Core execution, and Lua result conversion.
After session creation, a failed channel send, caller timeout, or failed Lua conversion must keep a cleanup obligation.
Cleanup must observe the session shutdown result before it releases the original operation ownership.
The current detached-shutdown helpers do not establish that result and must not serve as acceptance evidence.
Managed rollback must follow session cleanup and preserve reused worktrees under the existing rollback policy.
Capacity refusal, lost completion, and successful completion require distinct outcomes. A lost completion is not proof of cleanup.
Terminal disposal must retain callback charges until the existing engine-disposal receipt establishes that Core producers have stopped.
The implementation must include queued Core payloads in that ownership check, not only the callback's local frame.

The user authorized orchestration through integrated, proven software that meets the north star.
This plan continues the canonical September 6 implementation contract.
It does not reopen superseded migration plans or certify the whole architecture as complete.

## Source publication checkpoint

Root pushed these reviewed revisions to each repository's `main` branch and verified the remote refs.

| Repository | Published implementation revision |
| --- | --- |
| Core | `b9e989be3232c72e966ce3fdb63878c82b70d94d` |
| Hub | `b60ca68dcae7c8025d69784c9f77efdb9ea5a827` |
| Web | `3072a2421db9e13f5636ef3f2c6764b465a46b97` |
| TUI | `305399e5154b52293c81c8d7564cebff6f62581c` |
| Restty | `71fbfeb9cbd356b112c922d101a94bab7413675d` |

The consumed Kit revision remains `7940306b0d7461a12575b3856a96c0fbb23784f3`.
Hub's later documentation-only checkpoint does not change the tested implementation or require client repins.

Hub passed 945 library tests and the matched runtime inventory test.
Web passed W-S1 through W-S5 and the reconnect negative control against the same Hub and Core artifacts.
TUI passed 165 non-live tests and all five live cases with Rust 1.97 against those artifacts.
The Web and TUI cases include explicit multiline-paste consent and their distinct reconnect paths.
These results supersede the corresponding intermediate limits in the starting-point section below.

Full owner work and memory accounting, optimized performance, software frame presentation, and the Host-worker loss policy remain open.
The user has not received an installed-runtime update through this publication step.
See `docs/reports/2026-09-08-final-delivery-dependencies.md` for exact artifacts, raw evidence paths, and preserved failed runs.

## Ownership and working rules

- Root owns scope, shared-contract decisions, integration order, and evidence acceptance.
- Codex agents through Botster implement. Claude agents through Botster review independently. Codex sub-agents do not satisfy the review gate.
- Each repository has one writer in its canonical worktree.
- Verify the actual spawn location before editing. A prompt does not set the working directory.
- Each phase ends with a scoped commit. Preserve unrelated changes and historical evidence.
- Use one compiler or runtime window, at most two Cargo jobs, and `CARGO_INCREMENTAL=0`.
- Keep validation proportional to changed behavior. Do not repeat live tests for documentation-only changes.
- Do not treat source-string guards, log sizes, or absent errors as proof of behavior.
- Record exact commands, exit status, revisions, dependencies, artifacts, and remaining limits.
- Remove completed agents after their handoff and evidence preservation.

## September 10 orchestration restart

Jason requested that Root delegate the remaining delivery to Botster agents, with Codex implementation and Claude review.
Jason also required all current delivery work on `main` before new agents start.
Root will publish the reviewed Hub checkpoint and Web's reviewed `c6daed2` commit before creating agent branches.
New agents must verify their worktree, branch, and exact published base before editing. Root will record those identities in each assignment.
The existing unfinished goal remains open. The goal tracker rejected creating a replacement goal; Root must not mark the old objective complete to bypass that restriction.

The remaining delivery follows this order:

1. A Hub Codex implementer and Claude reviewer close callback allocation admission, retained ownership, and runtime cache accounting.
2. A separate Codex agent prepares the ordinary/managed spawn lifecycle while the first pair works. Its Claude reviewer checks the actual producer and consumer.
3. Spawn implementation starts only after Root approves the lifecycle and transfers Hub source ownership. The implementer updates its branch to the integrated memory contract.
4. Root integrates production policy and removes the unbounded loader after Jason selects numerical limits. Initialization and reload overlap need the same accounting.
5. Botster agents verify the matched daemon, Web, TUI, and first-party plugin paths. The verified package bytes must match the delivered revisions.
6. The remaining foundation audit and optimized performance work follow the current acceptance contract, including software frame presentation.

Root keeps one Hub source writer and one compiler/runtime window. Parallel planning and review are read-only outside assigned report files.
Agents may commit reviewed task work on their assigned branches. Root owns merging and publication.
No assignment authorizes installation, stopping the user's runtime, inventing numerical limits, or selecting a new Host-worker loss policy.
The proposed memory limits remain undecided: 16 MiB per Lua state, 128 MiB across states, 8 MiB per Rust callback, and 64 MiB across callbacks.
Spawn cleanup must preserve ownership after timeout, failed conversion, lost completion, or refused cleanup admission.
A joined Core driver is not sufficient cleanup evidence when its shutdown result was discarded. A successful session shutdown receipt is required for normal rollback.
The planner must establish a capacity wake and a completion wake without owner waits, periodic polling, or a synchronous helper pump.

The published checkpoint includes two crate-internal constructor visibility changes for tests: `HubStatePublication::new` and `HubSessionTypeSpawner::new`.
The first full library run lacked local socket access. The local-socket retry then exposed three tests that require `BOTSTER_ENV=test`.
Root preserved those failed runs. The final check used local socket access and `BOTSTER_ENV=test`; all 965 library tests passed with exit status 0.
The final log is `/private/tmp/botster-main-checkpoint.UHBXZd/library-test-mode.log`. The full run took 46.97 seconds.

### Agent startup and premise review

Both agent pairs verified clean branches at published Hub `9e95ff8ee8013ee9cd77a70221e5d3b7ddbea823`.
The callback pair uses `delivery/callback-accounting-20260910`. The spawn pair uses `delivery/spawn-lifecycle-20260910`.
Codex owns implementation in each pair. Claude owns independent review. Spawn work remains read-only until the stated transfer gate.

The callback investigation found a retained Rust error-print buffer in mlua 0.11.6.
`error_tostring` grows that buffer before copying the result into Lua. Clearing the buffer retains its capacity.
The current `events.on` path can propagate a variable-length error from a plugin-controlled table metamethod into this buffer.
Root checked the source path. This evidence does not establish an unlimited size under a fixed Lua heap limit.
The callback pair must establish allocation bounds and ownership before dependent implementation. No shared accounting interface is approved yet.

The spawn review identified these blockers through source inspection. No new regression test has run yet.

- Ordinary Lua spawn lacks a daemon consumer. The synchronous helper is not acceptance evidence.
- Managed rollback can start before confirmed session cleanup. A successful response send does not prove Lua conversion.
- Ordinary spawn can overwrite context keys before Core admission, then delete another session's context after refusal.
- The Core tracker combines begin-stage and completion-stage loss. The replacement must preserve the stage and pending identity.
- The rollback command inherits the admission deadline. An expired deadline leaves no useful time for a still-running rollback command.
- Separate waiter sources may collide in the shared completion index. A focused saturation regression must establish this Hub defect.

Root preserves caller-supplied session IDs pending a separate policy decision. The plan must prevent context overwrite and unrelated context removal.
Jason approved a separate rollback deadline on September 10: a fresh 20-second total allowance starts when confirmed session cleanup or authoritative non-creation proof permits rollback.
The allowance includes Host queue delay. It does not restart for each command or Host phase.
The Lua coordination bridge also lacks a daemon consumer. Root classifies this as separate, in-scope non-spawn work.
The callback pair records that consumer requirement without interrupting the current allocation investigation.
The spawn pair must revise its plan before approval. The revision must define recovery ownership, terminal behavior, and the exact wake protocol.
The pair withdrew its all-row disposition scan and its prescription to retain unresolved spawn rows indefinitely during shutdown.
The replacement must use a reviewed wake mechanism. No indefinite shutdown or disposal-and-return policy is approved.
Successful session contexts currently lack a retirement caller. Context-conflict refusal alone would therefore prevent later session-ID reuse.
The pair must identify existing context consumers and authoritative lifecycle events before proposing context retirement or replacement.
This approval closes the rollback timing decision only. Core settlement and terminal recovery behavior remain separate decisions.
The callback review also found that Lua disables instruction hooks while it runs `__gc` finalizers.
Root verified this source behavior in Lua 5.4.8. A non-terminating plugin finalizer can therefore bypass the current instruction budget.
Root classifies this as a separate, in-scope execution and teardown isolation defect. No runtime experiment or compatibility change is approved.
Any diagnostic must isolate the finalizer in a child process with an external bounded termination mechanism. The user's runtime remains out of scope.

### Diagnostic evidence and independent cleanup

The first reference-growth diagnostic ran two tests and exited with status 101: one passed and one failed during setup.
The direct `Table::raw_get` test panicked when reference growth beyond 34 slots was required, with Lua usage and limit both equal to 16,941 bytes.
This test-only Lua state had zero heap headroom.
The unpressured control retained 128 references successfully. Releasing one reference permitted reuse under the same memory limit.
The list/show wrapper test failed during setup at its first pressured case. The cause remains unattributed, and the test did not establish the wrapper panic path.
The log is `/private/tmp/callback-reference-growth-20260910-attempt-1.log`. This evidence proves no repair or daemon-path acceptance.
Root stopped the proposed second diagnostic after another safeguard notice. The test changes and first-run evidence remain preserved.
The agent's tool batch started that second run before the agent read Root's stop message. The agent then interrupted it; exit status was 130.
The stopped run has no acceptance claim. Its artifacts use the `callback-reference-growth-20260910-attempt-2` prefix in `/private/tmp`.
Root assigned the callback pair an independent implementation slice: remove the obsolete queued list/show path and its test seams.
That cleanup must preserve direct list/show behavior and the remaining spawn queues' disposal invariants. It does not close memory accounting.
The cleanup is now integrated on `main` at `ef08c897bd4b09e6245f48620ec081616bc91aad` after Codex implementation and Claude review.
The commit changes only `src/runtime.rs` and `src/daemon/owner_loop.rs`. It intentionally removes unused public Rust `HubSessionTypeSpawner::list/show` methods.
Five focused tests passed: two queue-disposal tests, one owner-disposal test, one bounded direct-read test, and one in-process Lua integration test.
The integration test first failed setup because candidate paths were absent. Its retry used the existing verified candidate set only for fixture validation.
The integration result covers the current in-process library, not a newly packaged cleanup binary. The compiled test tree also contained uncommitted diagnostic tests, which did not run.
Evidence is `/private/tmp/callback-queued-read-cleanup-20260910-evidence.md`. The diagnostic edits remain outside the cleanup commit.

### Completion registration ownership repair

Root integrated the reviewed repair at `b1282e75497eb218ff3af60b24e46fc93d8ea360`.
An unregistered `CoreDaemonHandle::submit` request no longer retires an owner registration when admission fails.
The production change removes two retirement calls in `src/data_plane/driver.rs`. Registered submission paths remain unchanged.
Three regression tests construct a colliding owner identity and exercise full, stopped, and disconnected admission.
Before the repair, all three tests failed at the registration-survival assertion. An earlier compilation failure ran zero tests and remains separately preserved.
After the repair, all three tests passed. The driver module passed all 19 tests, including those three; both runs exited with status 0.
The passing tests cover both pending and published owner registrations, payload destruction, and result delivery.
This evidence covers constructed collisions at unit level. It does not reproduce a production-path collision or establish broader identity uniqueness.
The compiled test tree contained preserved Lua diagnostic edits, but no diagnostic ran. No packaged binary acceptance follows.
The evidence manifest is `/private/tmp/core-submit-owner-20260910-evidence.md`.
The independent review is `/private/tmp/claude-callback-review.OFb8pb/submit-retire-final-review.md`.
Memory accounting and spawn lifecycle acceptance remain open.

## Verified starting points

| Component | Revision | Evidence scope |
| --- | --- | --- |
| Core | `98a50ef43f62bd9da6e062bd4d44d961fa3c78c9` | Shared harness and foundation changes; earlier focused evidence applies only to named tests. |
| Hub | `3fd99050a859f8d9db2f6100f4e0acb57f22e412` | Published chunk limit; generator and package checks ran before commit. |
| TUI | `3ed7d20` | Warning-free all-target check and actual PTY attach/reattach tests. |
| Web | `073dc0130ec04a965762d100e2455b2b177500d8` | Baseline and build pass; equal-valued generated constant import. |
| Restty | `71fbfeb9cbd356b112c922d101a94bab7413675d` | Screen readout source; corrected frozen dependency build and focused checks. |

Web runtime evidence is at `a01ca30`, against Hub `e2cc0ed5` and Core `98a50ef`.
The run passed keyboard, resize, two peers, 65,536-byte printable paste, and restored visible screen state.
The final Web import preserves the tested 12,288-byte plaintext chunk limit.
No live test ran at `073dc01`; source equivalence and its baseline support reuse of the earlier evidence.

The TUI evidence does not establish reconnect, resync, mouse-mode input, or paste acceptance.
The Web paste evidence confirms current unsafe-paste rejection, not usable multiline-paste consent.
No optimized performance baseline or fastest-multiplexer claim is established.
No merged or published final stack is established by this table.

## Work packages

### 1. Integration inventory

Hub Sol verifies branch ancestry, main and remote state, exact dependency pins, local-only objects, and active tickets.
Include Core, Hub, TUI, Web, Restty, and the consumed Kit revision.
Root approves exact merge and publication actions after the inventory and review.
Fresh-checkout resolution must not rely on private Cargo cache objects, path substitutions, or dirty artifacts.
Preserve the validated candidate binaries. Do not stop the user's active Botster runtime to test a replacement.

### 2. Usable multiline paste

Web Sol proposes a small implementation using the existing typed rejection and input queue.
Root reviews the plan before implementation.

- Ask for explicit consent for one rejected unsafe paste operation.
- Offer retry only after a definite unsafe rejection with no PTY write.
- Use a new operation ID on confirmation.
- Keep one bounded pending payload within the declared input memory budget.
- Clear it on cancellation, expiry, detach, or attachment replacement.
- Never replay input after partial or unknown delivery.
- Never let an old prompt approve replacement content.
- Do not log or persist clipboard content.
- Preserve keyboard focus, accessible controls, and normal input after cancellation.
- Do not add a global unsafe flag or duplicate Core's terminal safety policy.

### 3. Independent architecture and evidence review

Claude checks current candidate source against the full live implementation contract, including its dispatch addenda.
Prioritize concrete gaps in these invariants:

- One server terminal parser in the worker, active even with no attached client.
- No worker, filesystem, or network wait on shared owner turns.
- Bounded item and byte accounting, including pending results and capture state.
- Opaque shared terminal bytes without per-subscriber payload copies or disabled telemetry work.
- Correlated input outcomes, generation fences, ordered exit, and hard-close abandonment.
- Reconnect, live-worker adoption, retention limits, and explicit unavailable history.
- Clear Core, Hub, client, Kit, and plugin responsibilities.

Each finding needs a current source reference, owner, user impact, and one decisive verification method.
Do not infer a defect solely from a large file, an old name, or an unexecuted test.

### 4. Final behavior and performance evidence

Use upstream harnesses and actual client paths. Do not build a second terminal parser or revive the legacy comparison runner.
The reviewer proposes the smallest matrix that closes the remaining live-plan gaps.
It must cover flood and stalled-client sibling isolation, reconnect, worker adoption, exit/close, and relevant input modes.
Test the always-active worker with no client attached and inspect its state after attachment.
Measure retention against declared caps rather than an arbitrary memory snapshot.

Performance uses optimized artifacts and one recorded host configuration.
Record a direct-PTY control, output throughput, interactive latency distributions, whole-process CPU/RSS, and sibling latency under pressure.
Distinguish terminal screen-state evidence from paint evidence.
Include client, Hub, worker, and browser process costs where applicable.
State warmup, workload, repetitions, measurement overhead, and host load.
Set evaluation criteria before measurement. Do not redefine a target to make a result pass.
Compare products only under a comparable workload and environment.

The latency endpoint is verified software frame presentation for the correlated output mutation.
This endpoint excludes physical keyboard acquisition and display scanout.
Screen-state readback, animation callbacks, GPU completion, and TUI output flush are separate upstream measurements.
They do not substitute for presentation evidence.
Web must correlate the output mutation with the presented canvas content.
TUI must correlate the output mutation with the outer terminal's presented content.
Renderer traces or timestamped display capture can establish this relationship.
Capture must distinguish the first observed matching presentation from the first presentation when frames can be skipped.
Physical measurement hardware and custom browser or terminal builds require a separate decision.

Keep host CPU stress separate from terminal sibling pressure.
Record both conditions for every run.
Use versioned resource artifacts and preserve older logs unchanged.
Resource measurements require process start identities and explicit birth, exit, and reparenting accounting.
An executable UUID is not a process identity.
Report RSS as requested; any physical-footprint measurement is supplemental and must state its accounting rules.

### 5. Delivery and cleanup

Integrate reviewed commits in dependency order without compatibility scaffolding.
Verify fresh-checkout build and actual first-party use of the integrated revisions.
Record package and artifact provenance from the build that produced the delivered bytes.
Capture durable gotchas and current implementation boundaries, not every transient debugging step.
Remove obsolete executable paths and completed agent sessions only after preservation checks.
Do not delete unrelated data or treat deletion as fulfillment of an evidence requirement.

## Completion gate

This phase completes only when integration, required product behavior, architecture findings, and measured acceptance are resolved or explicitly returned for a user decision.
The final report separates completed work, evidence, known limits, and remaining product choices.
Neither a green test count nor clean source formatting is a substitute for this gate.
