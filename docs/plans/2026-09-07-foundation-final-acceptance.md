# Foundation integration and final acceptance

Status: source publication complete on September 9, 2026. Full foundation acceptance remains open.

## Active repair: plugin rendering on September 9

### Current phase: connect acknowledgement accounting

Jason authorized this phase after the local Core worker-resource checkpoint `abee4bb` passed nine focused tests.
The outcome is accounted acknowledgement execution through the actual daemon, including refusal, timeout, result conversion, and disposal.
Core owns generic attachment lifetimes. Hub owns admission, byte policy, and charge transfer. Lua retains plugin behavior.
Root coordinates the existing writers and reviewer. No new agent is required.

The first gate resolves attachment metadata ownership and fixes the exact Hub implementation boundary.
The Core writer will propose ownership for the input vector, resource Box, join-record vector, and executor Arc.
The Hub writer will map account construction, both production load paths, and the acknowledgement storage owners to concrete changes.
The reviewer will check those premises before Root authorizes source changes or new measurement execution.
Both assignments reuse the verified worker-lifetime, shared-admission, charge-split, and consumer evidence.
Root and the reviewer accepted the metadata contract after reviewing its complete destruction order.
Core will replace the new resource alias with typed release, add a guarded resource batch, and make the executor handle private.
Every executor handle will use `Arc::into_inner`; no raw Arc or Weak handle may escape.
The final handle must destroy covered owners before explicitly releasing metadata. Failed owner cleanup retains the metadata charge.
A valid batch may release metadata after destroying its vector, even when allocation unwinds. Allocation failure alone does not require retention.
The Core writer may implement this contract and focused test source in the same three files. Execution and integration remain separately gated.
The Core implementation subsequently passed source review, formatting review, its build, and all 21 exact resource tests.
The tests preserve nine worker-lifetime assertions and add twelve metadata ownership cases, including surviving clones, concurrent release, cleanup panic, and mutex poison.
Each run executed one test with 124 filtered tests, exit zero, and no deadline expiry. Root verified all 360 source hashes.
The executable SHA256 is `b725109e6f9982d22fe9b63bcfce2ea1346f10c9eada18c146049780551d4c75`.
Evidence is `/private/tmp/core-worker-metadata-build-20260910-1` and `/private/tmp/core-worker-metadata-tests-20260910-1`.
Payload probes establish ownership and destruction behavior. Exact source establishes the reviewed deallocation ordering; no allocation size was measured in this gate.
Hub integration, final layout sizing, and production policy remain open. No publication or installation occurred.
The Hub source pass stopped before connection because acknowledgement admission has no complete sizing input.
The first unresolved inputs are Rust error/conversion storage and the final charged reply channel, including its lease allocation.
The next bounded slice defines private storage owners with required pre-admitted charges and an explicit sizing contract, without defaults or placeholders.
Those definitions will establish exact types for sizing before the live admission path changes. They do not establish connected accounting.
The first definitions slice is frozen in `/private/tmp/c1-acknowledgement-ownership-review-20260910-1`.
It requires separate result, error-string, and conversion charges, plus leased reply endpoints and caller handles.
The shared candidate transport preserves Publish and Drain payloads. Its acknowledgement sender accepts only charged results and errors.
Root checked the source and hashes. Independent review accepted the definitions. Pinned formatting then completed without changing either hash.
The Hub writer will next propose the actual shared response and Host command replacement, including required sizing inputs and final dependent layouts.
That assignment permits planning only. Source changes, builds, measurements, and production activation remain separately gated.
The replacement plan is `/private/tmp/c1-acknowledgement-replacement-plan-20260910-1/plan.md`.
Root verified its first blocker: production `load_prepared` supplies `memory: None`, so required charged results cannot replace the live path yet.
Jason approved production policy wiring on September 10: 16 MiB per Lua state, 128 MiB across states, 8 MiB per Rust callback, and 64 MiB across callbacks.
This approval permits implementation. It does not permit publication or installation.
The Hub writer resumes a bounded configuration and account-propagation slice through both production load paths, preserving refusal before replacement.
The approved limits belong in a crate-private Hub policy function. Existing public `HubStartupOptions` and `HubConfig` struct shapes must remain unchanged.
The external construction tests explicitly preserve exhaustive literals. Policy wiring does not require new public configuration fields or weaker compatibility tests.
The Lua Host API instead requires the runtime's account. Its existing factory will become public, and the direct reload fixture will use that factory.
The public prepared loader must use that account too; an unfunded compatibility path is not permitted.
The writer may adapt `tests/hub_lua_runtime_test.rs` and map the new configuration failure to the existing Runtime client category in `src/client_api.rs`.
These caller changes must preserve reload assertions, request identity, and operation identity. They add no client protocol category.
The reviewer will separately trace acknowledgement conversion allocations in pinned source. No build, test, or measurement execution is authorized yet.
Independent review accepted the plan with one correction: conversion storage is not yet proven independent of borrowed input size B.
Admission must compute all eleven sizing fields from verified sizing rules and B. Neither constant nor linear conversion storage is established yet.
The serializer-local trace is `/private/tmp/claude-callback-review.OFb8pb/c1-ack-conversion-sizing-premise.md`.
It identifies fixed Rust reference-handle overlap while payload-dependent string bytes use Lua allocation. The full conversion allowance remains unaccepted.
The return trace found four argument handles allocated before body admission. The returned handle releases after body-local charges drop.
A result record's conversion charge therefore cannot cover the entire callback frame. This reopens that ownership premise, not the policy/account slice.
Root selected longer-lived state or worker funding for design review. No hidden unsafe return wrapper or numeric allowance is approved.
State-owned reference-cache and failure-pool growth, the known reference-stack panic, and allocations in Host finalizers remain unresolved obligations.
Policy approval will not resolve exact allocation sizes, final connected layouts, or their verification.
Connection must fund producer-side errors and must not convert impossible shared reply variants into uncharged acknowledgement errors.
Continuation and disposal layouts still depend on the actual Host command change. This slice does not duplicate that command.
Earlier zero-warning reports were incorrect because compiler diagnostics were in JSON stdout, not stderr.
Root checked both Core build streams: each contains two `unused_mut` diagnostics for the same statement, once per target.
The build and test results remain unchanged. The definitions review records the correction and the separate Hub warning counts.
The Hub writer will separately prove each affected container's population bound, including retained failures and stale identities.
Root and the reviewer accepted those element-count proofs: pending requests and coordination capacity waiters are each bounded by Owner capacity.
Host completions are bounded by the runtime's eight Host permits, including unmatched terminal completions. Pending-row membership is not required for that bound.
The proof is `/private/tmp/c1-owner-container-population-proof-20260910.md`. Changed node layouts and byte reservations remain unverified.
The accepted pinned tree derivation gives at most `floor(n / 5) + 1` nodes for each proven population, including insertion peaks and retained empty roots.
The Host completion map therefore has a conservative two-node ceiling. This does not establish either node's byte size.
The reviewer also recorded an unverified shutdown hazard involving unmatched terminal completions. Reachability remains unresolved; this is not a demonstrated C1 defect.
Core-private layout facts may support later sizing. They do not by themselves establish actual allocation requests or Arc header sizes.

Admission must precede allocation. Capacity refusal must preserve the old registration during reload.
The caller trace places that refusal boundary before event-generation replacement and capability cleanup in `runtime/package_effect.rs`.
Admission only inside `lifecycle.reload_package` is too late. The refusal test must also preserve the old event generation and capability resources.
Production package loading still uses the unbounded loader. Account construction and production policy wiring remain explicit implementation requirements.
Every charge must survive its allocation through completion, abandonment, failure, and actual destruction.
The implementation must remain event driven, without a new Owner wait, timer, or polling path.
The decisive check will exercise acknowledgement through the daemon and verify retained charges before disposal and release after disposal.
Production limit values are approved above. Publication and installation still require their separate approvals.

This is not the final repair phase. Other callback and spawn obligations, matched-client checks, and delivery remain open below.
The broader foundation accounting and performance requirements also remain separate acceptance gates.

### Repair contract and evidence

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
Jason subsequently approved terminal recovery behavior: Hub may finish shutdown with an explicit `recovery required` result when it cannot confirm session cleanup.
Hub must preserve the unresolved session and worktree records for recovery at the next start.
Hub must not delete the worktree or report successful cleanup without confirmation.
This approval does not select a shutdown settlement duration or authorize forced termination.
The spawn pair must define durable record ownership, write-failure behavior, and startup recovery before implementation approval.
Jason subsequently requested an Astra audit of timer paths and event-driven alternatives.
For the current spawn work, the pair must establish event-driven progress before proposing additional wait limits.
The design must identify completion, capacity, and fault signals, their consumers, and any missing cleanup receipt.
Timer expiration must not substitute for successful cleanup. Zero additional shutdown wait and operator record resolution remain unapproved.
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

## Consolidated remaining delivery plan — September 10

Status: Root and Botster Claude agreed on this plan on September 10. Jason requested agreement before further implementation.
Claude's final correction check is `/private/tmp/botster-consolidated-plan-claude-review-20260910.md`. It resolves R-1 through R-7 and N-1/N-2.
This section supersedes the September 10 restart's blanket memory-before-all-spawn ordering. Historical evidence above remains valid at its stated scope.
Root will report the reviewed plan to Jason. Review agreement does not authorize implementation, numerical policy, installation, or additional timer repairs.
Jason subsequently approved execution of this plan through full implementation and testing.
The code must remain modular, readable, and maintainable for humans and agents. Existing policy and external-action gates remain explicit below.

### Outcome, scope, and current baseline

Deliver usable Workspaces rendering with configured targets, working asynchronous coordination and spawn, responsive siblings, and accounted ownership through shutdown.
Complete the remaining foundation behavior, accounting, and performance acceptance afterward. Do not redefine the work as timer removal alone.
Core owns generic execution. Hub owns admission, correlation, recovery policy, and its persistence document. Host workers perform repository and filesystem work.
Lua owns product composition. Web and TUI own their client behavior and presentation evidence.

Hub `main` at `3edaa2b7ea2b89abf2bb9dc95399bedc61cf96f3` includes the reviewed queued-read removal and completion-registration repair.
The latter passed 19 driver tests. These repairs do not close callback accounting or spawn acceptance.
Earlier published multiline-paste and reconnect evidence remains closed at its recorded revisions. Recheck it only when a relevant change invalidates that evidence.
No installed-runtime update follows from the source checkpoints. Preserve the user's running runtime and unrelated worktree changes.

Use these existing inputs, not new competing plans:

- This acceptance plan and the September 6 implementation contract define the complete delivery scope.
- `/private/tmp/botster-spawn-lifecycle-plan.EE5C6d/plan.md`, revision 6, defines the reviewed recovery and event-driven drain direction.
- `/private/tmp/botster-spawn-lifecycle-claude-premise-review-20260910.md`, through T4, records independent source review and unresolved technical gates.
- `/private/tmp/callback-allocation-ledger-20260910.md` records the callback allocation findings. Its evidence does not establish a completed accounting contract.
- `/private/tmp/botster-astra-timer-audit-nlgt8b64/timer-audit.md` inventories 42 Hub mechanisms, not the whole repository family.

### Work packages and decisive checks

| Package | Bounded deliverable and owner | Actual prerequisite | First decisive verification |
| --- | --- | --- | --- |
| M1: callback accounting boundary | Hub Codex implements reservations before construction, shared retained ownership, conversion overlap, and fixed/error storage coverage. Claude reviews the exact source contract. | Reuse the existing allocation evidence. Resolve only the unsafe allocation boundary that the next implementation would cross. Do not resume or reroute the stopped diagnostic. | Target the first still-unproved allocation or lifetime boundary, such as retained error-buffer storage. Reuse valid list/show success, refusal, conversion-failure, and retained-error tests. Then repeat one-target rendering through the real daemon with sibling status. |
| M2: state accounting and production wiring | Hub Codex closes retained runtime caches, session-context storage charges, initialization/reload overlap, and bounded production loading. Root integrates approved limits. | M1's ownership interface; source-derived state charges; Jason's numerical limits before production activation. Context retention semantics remain subject to S1's compatibility requirement. | Initialization, overlapping reload, repeated callbacks, and unload remain within declared accounts. Retained contexts stay charged. The production loader has no unbounded fallback. |
| C1: asynchronous coordination | Hub Codex adds the missing daemon consumer with existing owner rows, Core completion routing, and explicit cancellation/disposal ownership. | M1's minimal movable-charge interface and a callback-specific allocation proof. Complete state-cache accounting is not a blanket prerequisite. | A real asynchronous Lua tool coordinates twice and returns correlated results without the synchronous helper. Hold capacity and confirm wake-driven recovery, cancellation ownership, and responsive sibling status. |
| S1: ordinary spawn lifecycle | Hub Codex implements Host resolution, ordinary daemon consumption, Core stage tracking, callback conversion acknowledgement, and context lifetime ownership as one lifecycle. | Minimal charge interface; session/context reservation proof; accounted wake storage and retirement. Integration requires operator resolution, retention, and recovery-capacity policy from R1, unless Jason explicitly accepts an unresolved limit. | A real daemon Lua tool spawns two explicit IDs. Verify command, environment, working directory, startup and retained context reads, result conversion, and responsive sibling operations. Then test abandonment at each effect boundary. |
| S2: managed and direct spawn | Hub Codex moves full resolution off the owner and applies S1's ownership rules to managed and direct client entry points. Remove obsolete helper consumers only after callers migrate. | S1 interfaces and shared-artifact exclusion. Preserve current IDs, context reads, and managed reuse semantics except the approved exact-conflict exclusion. | Exercise new and reused worktrees, concurrent conflicting attempts, failed conversion, refused cleanup, and late completion. Shutdown proof must precede rollback; unrelated worktrees and contexts survive. |
| R1: durable recovery and event-driven shutdown | Hub Codex adds write-ahead records, marker-safe mutation/removal, continued completion service during shutdown, and explicit recovery outcomes. | Reviewed drain design; record schema/accounting; operator resolution, retention, recovery-capacity policy, and the exceptional-stop decision. The abort keep/change decision precedes R1 acceptance, not all S1 implementation. | Interrupt each admitted phase in an isolated daemon. Verify no lost wake or obligation, persisted records across restart, and no rollback based only on timeout, channel send, join, or adoption. If the existing abort remains, prove durable-record survival and startup reporting across that abort. |
| I1: finalizer isolation | Root and Claude select the smallest architecture boundary that prevents a plugin finalizer from bypassing execution/teardown isolation. Codex implements only after approval of any compatibility or process-boundary change. | Existing Lua source finding; exact boundary proposal. No shared-process nonterminating-finalizer experiment. | After authorization, an externally bounded child-process test proves sibling responsiveness and retained-resource ownership during failed teardown. |
| A1: remaining foundation accounting | Root assigns Hub/Core/client findings to their owning repositories. Codex implements and Claude reviews each bounded finding. | Current source audit of still-open acceptance requirements, not a repeat of closed migration work. | Source-derived item/byte/work bounds plus production saturation, cancellation, loss, and disposal tests. Include pending payloads and retained results, not only queue entries. |
| V1: matched behavior and delivery | Root coordinates clean builds and exact-artifact Web, TUI, plugin, and daemon checks. | Required repairs integrated; production policy approved; manifests identify the built source. | Render with configured targets, sibling isolation, ordinary/managed spawn, errors, shutdown/restart, and affected reconnect/input cases on the same artifact set. |
| P1: optimized performance and presentation | Root coordinates the existing upstream harnesses and real clients. | Stable matched candidate and criteria fixed before measurement. | Direct-PTY control, throughput, latency distributions, CPU/RSS, and sibling pressure. Correlate output with software frame presentation in Web and the outer TUI terminal. |

M1 and M2 are separate delivery requirements. A test-parameterized M1 interface can unblock C1 and S1 without claiming production accounting is finished.
C1 is independent of spawn recovery policy, but not independent of its own allocation and disposal contract.
Define one shared bridge protocol for ingress publication, wake ordering, capacity, completion, fault, and terminal binding across C1 and S1.
Reuse that protocol where the actual producer/consumer contracts match. This does not prescribe one shared queue or wake instance.
Coordination must preserve its own effect/result semantics; it does not inherit session cleanup or conversion acknowledgement merely because it shares the bridge protocol.
S1, S2, and R1 have shared invariants. Implement their vertical slices together where needed; do not merge a success-only spawn path that loses failure ownership.
I1 must not disappear behind the memory work. It remains an open execution-isolation gate, with separate authority for a changed runtime boundary.
S1 must not claim complete callback-abandonment coverage while a nonterminating finalizer can prevent the guard from publishing. That acceptance depends on I1.

Session contexts currently remain readable after session exit/removal. No successful-spawn context-retirement caller exists.
S1 must define ownership across replacement, failed admission, rollback, and runtime disposal; M2/A1 must account for all retained context storage.
Identify an authoritative release event for each actual release. Do not silently introduce context deletion on session exit.
Any new context-retention or operator-deletion policy needs Jason's approval. Preserve current retained reads until that decision changes the contract.
Continued creation of retained contexts can exhaust their account and refuse new spawns. Jason must consider that consequence when selecting a retention policy.

Before removing helper consumers, C1/S2 must cover the in-process `HubClientApi` render/action callers of `invoke_plugin` and its public wrappers.
Preserve their bridge behavior through a supported event-driven consumer, or obtain explicit approval to end that support. Daemon adoption alone is not a migration.
Astra did not audit external users of the public wrappers. Missing caller evidence is not permission to remove the API.

### First implementation sequence after Jason accepts this plan

1. Root gives the callback pair one bounded decision pass using the existing ledger: identify the first unproved allocation boundary and a concrete safe implementation slice.
2. The pair returns the exact API, owned files, preserved lifetimes, and first test. If no safe slice exists, it returns one architectural conflict instead of another open-ended audit.
3. Root assigns the single Hub writer to M1. The spawn pair completes only the specific reservation and wake proofs that S1 requires, using the existing evidence.
4. Once the minimal charge interface is reviewed, Root chooses C1 or the next M1/M2 slice based on which prerequisites are closed. Root does not wait for all state accounting by default.
5. Root assigns S1/S2/R1 as complete effect-to-retirement slices when their exact gates close. Root obtains unresolved policy decisions before assigning dependent code.
6. Root integrates each reviewed slice with its evidence. Then Root proceeds through the remaining accounting, matched behavior, and performance gates.

One Hub source writer remains the default. This plan does not promise parallel Hub implementation across overlapping files.
Read-only review, bounded proof work, and preparation in other repositories can proceed beside that writer.
Root must give every active assignment a deliverable and stopping condition. An acknowledgment or idle prompt is not progress.
Root checks the actual session and artifact before reporting work as active or complete.
After one bounded investigation pass and its review, Root must select implementation, isolate the unresolved boundary, or return the exact decision to Jason.
Root must not send the same open question through repeated planning rounds.

The bounded S1 proof assignment must name these existing open items:

- M3: identify the account that owns the wake instance's lifetime storage, separate from per-callback charges.
- O2: approve the owed-removal / peek-mark-remove protocol, or a bounded alternative, before implementing its retirement path.
- K7: bound the complete scheduler population, including reservation deadlines and non-permit client/background entries, before deriving tree height.
- L1: keep byte figures conditional until the exact layout assumptions have authorized verification. A node count alone is not a byte bound.
- D1(a): retain session-ID reservation ownership across no-owner client paths, run-one, and adoption. Match Core admission's live-and-pending predicate, not registry presence alone.

### Timer audit disposition

Astra found 19 polling/progress mechanisms, one watchdog, 20 deadlines, and two backoff/rate policies in its defined Hub scope.
Five progress paths expose suitable events. Fifteen paths, including the watchdog, need missing event contracts. These counts are source findings, not tested repairs.

| Audit group | Placement in this plan | Required boundary |
| --- | --- | --- |
| Missing coordination/spawn consumers and their wakes | Required C1/S1/S2/R1 work. | Completion, capacity, cancellation, and fault events must drive progress. A longer timeout cannot replace a consumer. |
| Unix output P01/P02 | Proposed separate transport slice, relevant to the no-polling and performance gates. Root must confirm inclusion before assignment. | Select socket readiness, producer wakes, and shutdown while preserving partial writes, fairness, correlation, and charges. Test progress with the retry timer absent. |
| Close-work watchdog W01 | Proposed separate ownership/wake slice, relevant to event-driven terminal progress. Root must confirm inclusion before assignment. | Publish after enqueue; continue bounded ready batches; wait for actual lifecycle/fault changes on unresolved work. Do not substitute a busy loop. |
| Host process waits P03/P04/P10/P11 | Inspect only the portions touched by spawn/teardown. Broader replacement remains a separate proposal. | Child reap, output completion, and group absence are distinct. Existing child-exit events alone do not prove cleanup or justify deleting deadlines. |
| Other directly supported paths P06/P12/P17 | Independent repair candidates, not blanket prerequisites for rendering or spawn. | Preserve diagnostics, shutdown task retirement, or absolute client wait semantics. Obtain scope approval before adding these repairs. |
| Other polling paths and public helpers | Recorded backlog with explicit source scope. | No full-repository-family timer rewrite is approved. Do not restore synchronous helper pumping to daemon paths. |
| Existing deadlines and backoff/rate policies | Preserve unless a separate decision changes their policy. | Events end normal waits. Expiry is an exceptional outcome, never a cleanup receipt. The audit does not validate numerical values. |

### Decisions and permission gates

- Approved: recovery-required shutdown may preserve unresolved session/worktree records for the next start, without deleting the worktree or claiming cleanup success.
- Approved: D4 provides a fresh 20-second total rollback allowance after cleanup or authoritative non-creation proof, including Host queue delay.
- Root's design direction: exclude conflicting use of the exact uncertain session/worktree identity. Alias, race, and live-reuser proofs remain required.
- Unapproved: production memory numbers, zero additional shutdown wait, operator record-resolution command and retention policy, and a new exceptional shutdown boundary.
- The current driver-stop timeout can abort Hub. Root must return the keep/change decision explicitly; a plan cannot guarantee a typed result on that path.
- Unresolved records can exhaust the shared view budget. Recovery admission needs a reserved-capacity or separate-budget policy for resolution and unrelated required mutations.
- Root must derive the capacity requirements and obtain Jason's approval for any new numerical allocation or retention limit. No silent eviction is allowed.
- A changed finalizer isolation boundary or Host-worker loss policy requires explicit approval. Existing source findings do not supply that approval.
- Jason approved implementation and testing after Root reported Claude's agreement. This approval does not select the unresolved policy values above.
- Installation, runtime replacement, destructive recovery, and publication require their applicable authority. Preserve the user's runtime during all isolated verification.

### Completion and review gate

Claude reviews this section against the source findings and existing contracts, with special attention to false dependencies and omitted lifecycle work.
Root resolves concrete review findings in this section and records the final verdict. Review must distinguish agreement on a plan from proof of an implementation.
The final handoff names agreed work order, still-open product decisions, and the first bounded assignment. It does not claim foundation completion.

### Execution checkpoint: first accounting slice

Root selected M1a after the callback pair's bounded decision and review. The dependency-change conclusion was premature; supported callback boundaries remain available.
M1a adds sized/trimmed charges and budgeted `coordination.acknowledge` input through its actual request lifetime.
The callback Codex agent owns the scoped Hub edits and focused compiler window. Claude reviews the boundary, final diff, and raw evidence.
The slice must preserve input ownership through queueing, Core consumption, refusal, timeout, and disposal. It must not stage or run the stopped diagnostic edits.
Output allocation, other coordination operations, full state accounting, and the asynchronous consumer remain separate requirements.
The spawn pair prepares only the existing bridge/reservation gates. Astra prepares the finalizer-isolation proposal without runtime experiments.

The sized-charge API is local commit `f48696c50de56ae1afb889c3469febdd30355601`, based on `b3f0fd1`. Main integration remains held.
Root inspected the patch, raw log, and exit file. Five focused tests passed; 967 tests were filtered out; the command exited with zero.
Evidence: `/private/tmp/m1a-sized-charge-20260910-evidence.md`. The build included preserved diagnostic source, but the test filter did not execute those diagnostics.
The API has no production caller yet. Its new `shrink_to` dead-code warning must disappear through the caller implementation before integration.
The acknowledge-input change waits for Jason's decision on ignoring unused fields that currently fail whole-input validation. No input-accounting claim is closed.
The bridge readiness artifact is revision 7, confirmed by Claude in U6. C1 does not require the complete K7 scheduler census.
Root selected the generic Core prelaunch-reservation direction. Its concrete operation contract and source assignment remain open; Hub-only ID narrowing is not approved.

The I1 proposal recommends one Lua process per package generation: `/private/tmp/botster-i1-lua-isolation-proposal.md`.
Claude's source review confirms the finalizer defect but identifies a smaller compatibility change: prohibit plugin registration of `__gc` finalizers.
Review: `/private/tmp/botster-i1-isolation-claude-review-20260910.md`. Root requested a precise stop/cancellation contract and coverage of mlua's protected array metatable.
The searched first-party Lua sources contain no literal `__gc` references. This does not prove external-plugin compatibility or exclude dynamically constructed keys.
Jason approved prohibiting plugin `__gc` registration after the reviewed compatibility question. The process boundary remains unapproved.
Native-stall containment remains a separate scope decision; no I1 runtime experiment ran.
Claude added the protected array-metatable source and separated nonblocking stop from cancellation checks.
Root does not accept a one-hook-interval invocation-completion guarantee: a cancellation error check does not prove termination through plugin protected calls.
The narrow user decision is whether plugins may lose `__gc` registration. That decision does not close general cancellation or native-stall containment.
Jason's approval resolves that narrow decision. Root assigned A7 the sandbox boundary before entrypoint execution, with A8 as independent reviewer.
The first checkpoint is a source diff and finite registration tests. Builds and commits wait for Root's approval; stopped diagnostics remain excluded.
This assignment excludes F4 stop/cancellation changes. The separate acknowledge-input compatibility decision remains open.
I1's corrected focused run passed all seven tests, with 972 filtered tests and exit zero. Root inspected the raw log and evidence manifest.
Evidence: `/private/tmp/i1-finalizer-boundary-20260910-evidence.md`. The initial six-pass, one-failure run remains preserved.
The corrected test requires the exact Hub location prefix and preserves complete error-body comparisons. Production code did not change between runs.
Claude approved patch `08fc481d7d35f6565e5e5db48be9195121dc2b3d078f9f78bee05fe9deaad8e7` for a scoped commit.
Root approved that commit and selected I1-only integration. The independent F3 checkpoint remains held; no accounting acceptance follows from I1.
The tests cover finite controls and the actual runtime constructor and invocation. They do not establish full reload, prompt cancellation, or native-stall containment.
Root integrated only I1 as `51aef9d030fc341ca69ec3057c271c7701f48155`, selecting source commit `28732ddc61b2790bac0e6711062a40d47fa9db52`.
The canonical commit diff has SHA256 `4ef40f5074395f6c33c16b39b0c7e5eb1b039f1355be853902221bcbba311c41`.
Its file order and index headers differ from the reviewed patch; both have stable patch ID `db833c401686ef354fe2b3c82baf3c2155badbbe`.
Root reran the same seven focused tests on main without F3 or diagnostic edits. All seven passed, with 968 filtered tests and exit zero.
Raw log: `/private/tmp/i1-main-51aef9d-test.log`. The build reported 66 library warnings and 39 library-test warnings.
This closes the tested registration boundary only. No publication, installation, full-foundation acceptance, or production-memory policy follows from this checkpoint.

### Execution checkpoint: acknowledge input approval

Jason approved validating only `target` and `envelope_id` for `coordination.acknowledge`, while ignoring unused fields.
Unused fields may therefore contain values that whole-input validation previously rejected. Consumed-field validation and all seven target variants remain in scope.
Root resumes A7's M1a implementation with A8 as independent reviewer. The held sized-charge API must gain its actual production caller.
Reserve before constructing retained Rust input. Preserve the charge through queueing, Core ownership, refusal, timeout, and disposal.
The first handoff contains the exact source diff, allocation/lifetime evidence, and focused tests. Root approves the test command before execution.
The daemon consumer and its output, wake, and disposal obligations remain C1 work. This approval does not select production memory limits or close M2.

Root integrated the sized-charge API and acknowledge input together as `2d11f8a`, selecting `f48696c` and `e2b81c61aff228848b9414303162748148766b09`.
The combined source omits `shrink_to` and the diagnostic edits. All five affected files match the reviewed implementation commit.
Root verified eight input tests, five memory tests, and one exact integration test on main. Each command exited with zero.
Logs: `/private/tmp/m1a-main-2d11f8a-{input,memory,integration}.log`. Filtered counts were 977, 980, and 47, respectively.
The integration test exercises the in-process runtime and Core submission with the unbounded loader. Its old binary fixtures do not prove a matched packaged candidate.
Input identifier ownership is the closed slice. Queue, channel, Core closure, retained result, state, and asynchronous consumer accounting remain open.
Root selects C1 next, using revision 7's shared bridge contract and the existing owner rows and registered Core completion path.

### Execution checkpoint: C1 allocation measurement approval

Jason approved a counting allocator in one isolated test executable to establish allocation sizes for the pinned compiler and target.
This exception does not permit a replacement allocator in the Hub library or production executable. It does not resume the stopped Lua diagnostics.
Root accepts compiler-and-target-specific size constants when source review and the isolated evidence support them. Untested targets remain unverified.
Measurements establish allocation sizes and exercised lifetimes, not production memory budgets or universal worst-case behavior.
The reviewed request owner keeps the boxed operation before its charge and releases the charge after the boxed call returns.
The reviewed channel owner keeps a private lease at each endpoint. The last lease releases its charge after both endpoints and the shared allocation are destroyed.
The charged stopped-admission path must return the original disconnected ticket, without constructing a second channel.
Root assigns A7 the finite test implementation and A8 its independent review. Execution follows review of isolation, allocation capture, exact types, and finite scenarios.
The test must cover channel construction and both drop orders, refusal and loss, and the relevant registration allocation paths. No live Hub or Lua state participates.

The isolated build and single measured run passed. Raw evidence is `/private/tmp/c1-allocation-oracle-20260910-run-1`.
Root verified exit zero, no deadline expiry, all 19 scenarios, and no remaining captured allocations after thread exit.
Claude accepted the evidence in `/private/tmp/claude-callback-review.OFb8pb/c1-oracle-results-review.md`.
For the exact test-profile ticket type, the channel requests 816 bytes and its proposed lease requests 32 bytes. The combined reservation input is 848 bytes.
The shared wake's fixed storage is 840 bytes including three empty roots. The measured node layouts are 192-byte leaves and 288-byte internal nodes.
These are requested allocation sizes, not production budgets or observed-peak bounds. Release builds and changed concrete types need separate verification.
Root resumed A7's C1 implementation and assigned A9 the bounded Core wake population/transient proof. A8 reviews the implementation and proof.
The callback reply channel, executor-thread wait storage, changed queue slots, and Core lookup/result overlap remain explicit C1 obligations.

Root accepted the shared wake derivation with an explicit condition: all retained phase-map keys must belong to at most 2,112 live waiters.
The retained bound is 608,808 requested bytes for the measured test-profile layouts. This is not an unconditional bound on the current shared instance.
The proof is `/private/tmp/botster-spawn-lifecycle-plan.EE5C6d/c1-o1-tree-proof.md`; Claude checked its arithmetic and lifecycle conditions.
The bound includes the fixed storage and three trees. It excludes channels, closures, result payloads, temporary vectors, and allocator overhead.
The pinned iterator dispatch proves that taking k ready identities allocates exactly 16*k bytes. Production takes at most one identity per activation.
Retiring a waiter with one or two registered phases can allocate a separate 64-byte vector. The take vector and retirement vector can overlap.
Root selected conditional accounting, not a new refusal rule for every registrant. C1 must enforce its own final waiter retirement through a guard.
The guard must retire phase history before the owner permit is released. It must not release charges for payloads that Core still owns.
The owner must restore an unconsumed completion batch before new registrations use that capacity. All wake clones must retain the shared wake charge.
These requirements do not certify non-C1 callers, production memory policy, release-profile layouts, or the full scheduler accounting gate.

Root selected the complete consumer lifecycle and its first real-daemon regression as the next bounded verification step.
The test may precede full transport accounting. Its handoff must identify unwired accounting; functional success cannot close that requirement.
The deterministic Core gate also blocks Status, because Status submits its own Core request. That test must not require Status completion while Core is held.
The test will use ListPackages to check sibling progress during the gate and Status after release. Matched-artifact Status responsiveness remains a separate requirement.
The regression will use real socket dispatch through `serve_daemon_inner`, beside the existing owner-loop tests. No helper may pump the tested coordination requests.
The draft adds terminal-owner storage to the shared wake. The old fixed-size measurement does not apply to that changed type; the tree derivation remains conditional.
Core drain removes selected envelopes and marks them Delivered before returning the result (`engine/routed_envelope.rs:126-171` at Core `b9e989b`).
Acknowledge changes a separate delivery-state record. Discarding an admitted drain result can therefore lose those envelopes; terminal disposal is not lossless delivery.
The terminal test proves collection and disposal only. C1 preserves existing drain semantics and prohibits automatic replay; durable redelivery needs a separate contract decision.

Review found that the draft holds shared Host permits while coordination waits for Core. Eight such calls can exhaust the eight shared Host slots.
That design can refuse unrelated Host work and the shutdown response. Reserving disposal capacity before ingress was therefore too restrictive for C1.
Root selected retained owner rows across Core waits, with Host reservation only when delivery or disposal is ready. No new capacity partition or policy value is selected.
The existing bounded Host-capacity drain must resume waiting C1 rows by waiter identity. Terminal disposal retains its payload until Host capacity becomes available.
A regression must hold eight coordination calls in Core and verify a Host-only sibling operation. The corrected lifecycle must pass review before functional execution.
Root also selected retention of a consumed completion in its original row when C1 scheduling fails. A C1 fault must not suppress shared Core notifications.
The first no-run library build exited 101 after 33.115 seconds, without a deadline expiry or test executable. No test ran.
Four test callers still invoked the old boxed request directly; another test match omitted the new internal-completion variant.
Root verified the raw errors and all 22 frozen source/config hashes in `/private/tmp/c1-consumer-build-20260910-1`.
The implementer will correct those compile errors with the Host-capacity and scheduler changes. A second build requires review of the new handoff.
Root reviewed draft 4 and authorized one second no-run build while Claude reviewed the lifecycle correction.
That build passed with exit zero after 58.962 seconds. No deadline fired, and no source file changed.
Root verified all 22 frozen source/config hashes in `/private/tmp/c1-consumer-build-20260910-2`.
The library test executable has SHA256 `a4052098d78f061c11660a12b2afde894b09c6a737dfb020d4e319e3837ade9a`.
No functional test ran in that build. Lifecycle review must precede the three proposed exact tests; callback accounting remains open.
Claude accepted draft 4's lifecycle correction. Root then authorized the three exact tests, with a separate 30-second deadline for each.
The first test failed with `nonincreasing_request_id`: the fixture sent ListPackages ID 7 after callback IDs 41 and 42.
It exited 101 with one failed test and 995 filtered tests. No deadline fired. The eight-call and terminal tests did not run.
This protocol setup failure does not establish a C1 defect. Raw evidence remains in `/private/tmp/c1-consumer-functional-20260910-1`.
Root authorized increasing fixture IDs and an explicit ListPackages response-kind assertion. Production code and deadlines remain unchanged.
Root and Claude accepted the test-only correction in draft 5. The third no-run build passed after 63.078 seconds.
Root verified the 22 source/config hashes and executable SHA256 `89997e0fe92ec4011122c3f5e9ae138e187135e320d6c8ad393f0ee96f381df0`.
All three exact tests passed separately: two acknowledgements, eight Core-waiting calls with a Host-only sibling, and staggered terminal completion.
Each run executed one test, filtered 995 tests, and exited zero without a deadline. Source and executable hashes remained unchanged.
Raw evidence is `/private/tmp/c1-consumer-functional-20260910-2`; the build evidence is `/private/tmp/c1-consumer-build-20260910-3`.
These results establish the exercised daemon and terminal paths, not callback accounting, packaged delivery, or full C1 acceptance.
Root assigned the remaining focused lifecycle tests next: capacity wake recovery, abandonment, Core refusal, poison, and scheduling failure.
The fixture failure exposed a review omission: both reviews missed request ordering. Future socket-fixture reviews must check the complete request sequence.

Root and Claude accepted draft 6's eight focused lifecycle tests and two test-only hooks.
The fourth no-run build passed after 63.588 seconds. Root verified all 23 source/config hashes and the executable hash.
The executable SHA256 is `f14b8697161f851313418af844ba1cda4377fdf989bf4af892cd17c45321c04c`.
All eight tests passed once each, with exit zero, one executed test, 1,003 filtered tests, and no deadline expiry.
Raw evidence is `/private/tmp/c1-consumer-focused-20260910-1`; the build evidence is `/private/tmp/c1-consumer-build-20260910-4`.
The tests cover Owner and Host capacity recovery, queued and admitted abandonment, Core full/stopped refusal, queue poison, and C1 scheduling failure.
Execution counters and separate drop probes prove the tested no-replay and disposal behavior. Admitted Drain abandonment preserves the documented consumed-envelope loss.
The poison test deliberately catches a panic. Its stderr records that injection; the test then verifies retained ingress and Host disposal.
The focused fixtures do not bind the doorbell. The earlier socket tests cover that path.
The scheduler test proves shared notification access, an unrelated collectable ticket, and terminal retirement. It does not prove progress under global scheduler exhaustion.
Claude independently verified the raw results and provenance. The tests provide no repeated-run reliability or allocation-size evidence.
Registration refusal, DeliverySubmission, and UnexpectedCompletion remain untested. Callback accounting remains open.
Root selected acknowledgement accounting next, using the existing interfaces and proofs. Production memory budgets and new measurement execution remain separately gated.

The acknowledgement handoff identified retained standard-library wait storage on Core's persistent plugin workers.
Root and Claude verified the existing source and measurement evidence. Callback completion, channel disposal, and runtime stop do not end that storage lifetime.
A join returning either success or a worker-panic result establishes thread exit. Core owns that receipt; Hub owns the memory policy.
The proposed interface reserves once per worker before thread creation and covers all blocking callback kinds on that worker.
Some existing panic paths detach workers without joining them. A charge paired with a plain JoinHandle can therefore release too early during unwinding.
The proposed failure policy retains the reservation until process exit if Core loses the join handle. Root requests Jason's approval before selecting that policy or assigning Core changes.
The bounded review is `/private/tmp/claude-callback-review.OFb8pb/c1-worker-context-lifetime-review.md`.
Independent Hub work continues: aggregate callback admission and a disjoint charge-split interface in `lua_memory.rs`, with test-only limits.
No new worker attachment, production budget, measurement, or Core source change is authorized at this checkpoint.
Root and Claude accepted the independent `reserve_callback_total` and `LuaCallbackCharge::split` interface.
The interface checks the per-callback quota before shared capacity. Splitting transfers disjoint reserved bytes without changing shared usage.
Its no-run build passed after 49.447 seconds. Root verified all 23 source/config hashes and the executable hash.
The executable SHA256 is `73145f2098dcbceaf1f4465ff2e4e23fa3020bd9f9a8ffd2a35119a3fc696058`.
All five exact interface tests passed, each with one executed test, 1,008 filtered tests, exit zero, and no deadline expiry.
Evidence is `/private/tmp/c1-charge-transfer-build-20260910-1` and `/private/tmp/c1-charge-transfer-tests-20260910-1`.
These tests use synthetic charge amounts. They do not establish allocation sizes, callback storage coverage, or production budgets.
The worker-retention policy request remains unanswered. The interface does not implement or authorize the proposed Core attachment.
Jason subsequently approved the Core interface and retention policy, provided the design remains event driven.
Core may release a worker reservation after that worker's join returns. A lost join handle retains its reservation until process exit.
The implementation must not add polling, periodic scans, timers, or an Owner-thread wait. Existing Host-side joins supply the completion receipt.
Hub supplies opaque pre-funded reservations. Core owns their worker attachment and release ordering, without Lua policy or production budget values.
Root will assign one Core writer and preserve the existing Hub writer. Publication, installation, and numerical limits remain outside this approval.
Root and Claude accepted the shared-storage admission interface and the revised acknowledgement ownership design.
The interface reserves instance-owned storage against shared capacity without applying the per-callback quota.
Its build passed in 62.542 seconds. Both exact tests passed with one executed test each, 1,010 filtered tests, and no deadline expiry.
Root verified all 23 source/config hashes and executable SHA256 `7a94be48a59b66a3cc1e076f96e6f67388b16264db4ac10044d261e1c178e03c`.
Evidence is `/private/tmp/c1-shared-storage-build-20260910-1` and `/private/tmp/c1-shared-storage-tests-20260910-1`.
These tests establish synthetic admission and charge lifetime, not allocation sizes or connected callback accounting.
The revised design requires reload to admit new worker resources before removing the old registration. Capacity refusal preserves the old registration.
The Core source review accepted the guarded join records. Formatting and focused verification remain pending; Hub has not connected that API.
Worker resources do not yet account for the outer resource Box, input vector, join-record vector, or executor Arc. Their ownership remains open.
The formatted Core source subsequently passed its build and all nine exact resource tests.
Root verified all 360 source hashes and executable SHA256 `8b9e69585865cc8a937bdef2f185f76df979ad598699ae09cf7ee78d3f8b044f`.
Each test executed once with 112 filtered tests, exit zero, and no deadline expiry. Deliberate panic output remains preserved.
Evidence is `/private/tmp/core-worker-reservations-build-20260910-1` and `/private/tmp/core-worker-reservations-tests-20260910-1`.
The tests cover exact-join release, thread-local destruction ordering, count refusal, replacement, unload, engine destruction, and unjoined failure paths.
Exceptional tests prove retention after runtime destruction, not completed detached-thread exit. The joined test separately proves thread-local destruction ordering.
This verifies the scoped Core interface. Hub integration, attachment metadata ownership, changed allocation sizes, and full callback accounting remain open.

## Historical verified starting points

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
