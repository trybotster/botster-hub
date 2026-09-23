# Foundation integration and final acceptance

Status: spawn checkpoint `b7864ff9` and recovery checkpoint `d45c90da` are pushed. Full foundation acceptance remains open.

## Current handoff — September 22

This section supersedes the earlier status and assignment statements below.

### Latest checkpoint and verification results

The worktree correction is committed and pushed as `d45c90da09304c9928ef106c7ada58225c5c450d` over `5b030dd2`.
This result supersedes the earlier worktree failure and pending implementation statements below.
Automatic review initially denied the correction. The writer reverted partial changes after each denial.
Jason then directly approved the specific transition and its stated risk in the implementer's conversation.
The writer resumed through normal tool review. No deployment or deletion of existing user worktrees was authorized.

The correction serializes attempts through Core release and transfers the original creation right after successful reuse.
Two admitted requests for the same branch can now both succeed: one creates the worktree, and the other reuses it.
The renamed regression checks this intentional API change. It does not permit an arbitrary success-or-conflict result.
The reviewer verified the five-file patch against the live source and checked the raw run records.
All 18 invocations passed; they cover 17 distinct tests, including the original worktree failure and terminal disposal.
The library-test artifact SHA256 is `116efb8d91da13661fb1877e6826b93d29e8893994125102df603853b1da3d65`.
The source patch SHA256 is `882219bd0432653174f1443eb169777d902205315c77364ddfa0729b5eba9a05`.
The focused log SHA256 is `2173c13161a51f6f8769ec5b0909521783f60653478162f4277e4079ca2ce6e3`.
Evidence is in `/Users/jasonconigliari/botster-evidence/managed-recovery-20260922/`.
The candidate binaries still come from clean `5b030dd2`; these results do not verify a packaged daemon containing the correction.
The full suite, repeated stability, and negative controls remain unproved by this run.

Writer 001b now removes only the inert suppression mechanism from `runtime.rs`, `managed_git.rs`, `host_executor.rs`, and `managed_git_worktrees.rs`.
Reviewer 001e must verify that removal preserves identity checks, submitted rollback exclusion, creation rights, and Core release gating.
No removal build has started. Integration, complete asynchronous spawning, crash/restart proof, and matched-client verification remain open.

The following update supersedes the earlier recovery publication and test statements in this section.

- Recovery checkpoint `2212710f760eaa148588f3f2a50edc719d75071a` contains the reviewed journal and effect-retention changes. Checkpoint `5b030dd26c4cf32a8850224582d6642f6306f1e3` adds four reviewed test-file corrections. Both checkpoints are pushed.
- The configured workspace run used `./test.sh --locked --offline -- --test-threads=2`, `BOTSTER_ENV=test`, approved socket access, and a matched candidate. The Hub library reported 1185 passes and 14 failures. Cargo stopped before the other five workspace members ran. The raw log SHA256 is `80f9e467fbd9d79382c383308a7ce27c33878c0dab87adfe58c0cbb0ccec463b`.
- Thirteen failures stopped at an obsolete worker fixture. One test expected a retry after unresolved intent. Writer 001b corrected these test prerequisites in `sessions.rs` and `owner_loop.rs`. Reviewer 001e accepted the test-only patch. The patch remains uncommitted and frozen.
- The corrected artifact SHA256 is `784837bc274008db7b67a3f3ab76c801664201ab3579ca4387d57b9f4dd669f5`. Focused selectors 1–6 passed. Selector 7, `ensure_worktree_and_spawn_reuse_after_undelivered_keeps_the_created_worktree`, failed at the directory-existence assertion. Hub reported reuse, but the reported directory was absent. Selectors 8–14 did not run.
- The focused log is `/private/tmp/botster-recovery-targeted-tests-20260922.log`. Its SHA256 is `638cf08ad4fab43f8c1750a06cd0bd3170b8ff5a3423208c952627545c3bda0d`. This failure reaches the worktree-preservation invariant. It is not a candidate setup failure.
- Root checked the test and cleanup source. The test waits for a directory and an empty input queue; it does not establish queued cleanup. A later cleanup enqueue removes suppression recorded by cancellation. Writer 001b must propose a deterministic regression and an ownership correction. Reviewer 001e must check cancellation before enqueue, after enqueue, and after rollback submission. No production edit or new run is assigned yet. Baseline attribution remains unverified.
- A separate interface defect remains open: `file_commit_error` maps distinct recovery outcomes to `hub_state_commit_failed`. A later correction must preserve typed outcomes without parsing display text.
- The compiler slot is free. Startup-path policy, complete daemon spawning, crash/restart proof, remaining workspace tests, strict verification, integration, and matched-client verification remain open. No deployment or production acceptance is claimed.

Earlier checkpoint details follow.

### Worktree correction assignment

Root selected serialization of attempts for the same worktree, with transfer of the original creation right.
Later requests remain in the existing bounded queue until the active attempt reaches its terminal state.
The terminal event must wake the queue. No polling or new queue is approved.
The original creation right remains distinct from each attempt's session cleanup and reservation release.
Early refusal must not change that right. Git rollback must wait for all relevant Core releases.
The existing exclusion for submitted rollback remains in force.

Writer 001b may implement after reviewer 001e accepts this lifecycle.
The assigned recovery files are `runtime.rs`, `daemon/control/managed_git.rs`, and the session tests.
An `owner_loop.rs` change requires a demonstrated missing terminal wake.
The writer must preserve the frozen fixture patch and recovery publication logic.
The four regressions cover delayed completion, failed reuse, submitted rollback, and refusal before admission.
No build, Host recheck, durable protocol change, integration merge, or deployment is assigned by this step.

- Spawn checkpoint `b7864ff947e22fe48b49eb27c286543fabfffa61` contains all 28 reviewed code/test files. Writer 001b reports a successful non-force push to `delivery/async-spawn-20260921`. Local evidence remains untracked. No deployment occurred.
- The complete spawn patch matches the tested tree. The earlier partial file proposal omitted required dependencies and was rejected. Review established that the shared Lua changes belong to this spawn work; no separate safety checkpoint is required.
- The exact final-output parity test passed on artifact `138b00a4163cd9433534a0c285ffc67b36bcc2c9e0846170a79f4162aa870151`. A direct rerun recorded the command and actual numeric exit 0 after the first run omitted those records. Full daemon spawning remains unproved; the production path still returns `Unavailable` pending the startup-path decision.
- Recovery's combined journal and repository-effect repair passed source review and compilation. All six focused tests passed on artifact `403f7bc79c91e7a25b1f20b1b4b02a55ff2710643710f25c8bd83910522831b4`. Root checked all six result lines and the artifact hash. These include actual `HostMutationContinuation::poll` regressions for both uncertainty variants.
- Recovery retains live external-effect evidence and the owner permit in the existing preallocated cell. Durable terminal disposal does not imply permission to release an active operation's permit. The repair preserves receipts across uncertain outcomes and distinguishes repository uncertainty from state publication.
- Earlier repository tests failed before the intended assertions because the fixture lacked an initial state file and used a noncanonical fault-injection key. Reviewed test corrections resolved those setup problems. The failed logs remain preserved.
- Recovery's exact 10-file checkpoint is accepted for conditional commit/push after the full library suite result and a six-test rerun with complete invocation records. The source freeze is tracked diff `21cf43463c77d0acac7fb0d67b441a33511403daad6a13b71cb8778bc60fd720` plus journal `f91dee202f76903b4d18f93e2549c435b47a01a3b409281d25b649d67316274a`. No publication is yet confirmed.
- Remaining gates include full daemon spawning, real daemon interruption/restart verification, reconciliation policy, integration, and matched-client verification. No negative control or strict gate has run. No production acceptance is claimed.

### Earlier handoff details

The results above supersede build-slot and publication statements in this subsection.

- Recovery: the append-only intent/completion protocol passed design review. Implementation is active. Unresolved intent prevents restart writes. Automatic rollback, evidence deletion, migration, and deployment remain excluded.
- Recovery: checkpoint `4bd98717` is pushed. Root verified its four focused tests. These tests establish live retention, not crash recovery.
- Recovery: the new journal source passed one no-run build with Rust 1.97.0, jobs 2, incremental disabled, and locked/offline dependencies. No tests ran. Reviewer 001e accepted tracked diff `b56302f7cf41caa39cbefdc46e08ab94ea7ee08620bcad60b55c05915017dbc7` and untracked journal hash `47a281e8dec94fb10dceb28e652570bf85c9e362e4a28abde679de23f4ef80ad` before compilation.
- Root verified diagnostic log hash `7e831171e16afff06fe092c568ab205d05dd45bb8ee7e67880c63535aeb15477` at `/private/tmp/botster-recovery-file-diagnostic-20260922.log`. Root verified artifact hash `11211add575dcb643efc994c3042097b7cab624223522775cbbde357a314f45f`. The build reported 83 library warnings and 42 test-library warnings, including 27 duplicates.
- Recovery writer 001d reports a direct user restriction against editing `managed_git.rs`. This restriction takes precedence over Root's earlier assignment. Its two response sites still need the existing `write.cause().client_error()` mapping. The file owner must be resolved before this integration change.
- Spawn: six focused tests passed on artifact `638f7fb0c6c1d62b4f2eb7d541d915b0bca9e20df0eb01a3c91bef05a02c9191`. They cover scratch reuse, queue ordering, two failure paths, and two charged-loader paths. They do not prove successful daemon spawning.
- Spawn: reviewer 0018 accepted the subsequent allowance bundle, operation wrapper, Core binding, and final Host failure lifetime correction. Writer 001b now holds the compiler slot for one diagnostic no-run build. Recovery released that slot after its successful build.
- Startup-captured cwd and executable paths remain an unanswered user decision. No startup-path policy is silently selected.
- Next decisive checks: complete spawn materialization and exercise two real daemon session IDs; exercise the recovery journal across crash/restart boundaries. Repository-effect ordering and the managed-git mappings remain open. No integration merge, activation, or production acceptance is claimed.
- Follow-up: assess typed charged-payload transfers after the spawn checkpoint. Repeated tuple lifetime defects justify this assessment, but no new abstraction is approved by this entry.

## Earlier checkpoint — September 22

This checkpoint supersedes older assignment and build-status statements below.

### Latest implementation decisions

- The user approved the durable recovery extension on September 22: record intent before state publication, preserve evidence across exit or crash, and refuse writes after restart until explicit reconciliation when intent remains unresolved. Automatic rollback, unresolved-evidence deletion, and deployment remain excluded. This supersedes the pending-approval statements below.
- Writer 001d and reviewer 001e now own the bounded protocol design and independent review. The proposal must cover startup, normal writes, related repo-file/worktree effects, terminal handoff, admission, synchronization failures, and crash points. Implementation follows protocol review; no particular protocol is accepted yet.
- Recovery compilation passed at source diff `beed3cef96928f516d178f53d3266ed9917aa9b6c022d20e9d719cc23466e8e1`. Root directly ran all four focused tests against artifact `5a11f733c58f8f2a2bd7152cc4cf7eb98e9b4bc7105d46d4f4b17c042fac7e97`; all passed. An initial sandboxed attempt failed during setup, before assertions. Approved-access runs passed. Root granted the exact reviewed 14-file partial checkpoint commit and push; publication confirmation is pending.
- Spawn's charged parser now has source acceptance with input and parser charges retained concurrently. No additional input charge or formula change is needed. The Host connection and failure-delivery changes remain under review/build verification. Startup-path policy remains undecided.
- Recovery checkpoint `4bd987172a702fbfd71b480ee49ae75d96317dc5` is committed and pushed on `delivery/durable-recovery-20260921`. The writer reports a clean worktree and exactly the 14 reviewed files. Reviewer 001e will verify committed content. No merge or activation occurred.
- The pre-admitted cell addresses live retention only. Startup error disposal, terminal disposal, and restart refusal share one unresolved durable-ownership blocker. The approved protocol design phase addresses that blocker; no protocol implementation is approved yet.
- The existing in-document ledger covers session and worktree phases. A post-failure record cannot establish its own durability. A precommitted intent remains a candidate, not an accepted protocol. A digest match proves visible content, not successful synchronization.
- Spawn writer 001b reports that runtime-only spawning now refuses before enqueue or effects. Legacy queued requests receive the same refusal. Source verification remains pending. The Lua handoff, Host connection, and owner receipt lifecycle remain incomplete.
- The single spawn queue remains selected. Review confirmed one pop per owner activation, not a byte or time bound. The reviewer withdrew the bare-channel loss claim after tracing the conversion receipt in the payload. Its owner-side ticket still requires connection.

- Recovery: `3bc264a8` adds sticky write quarantine. All 12 state-directory tests passed. Reviewer 001e accepted the evidence. This proves the storage primitive, not runtime activation.
- Spawn: the parser oracle passed all 12 fixtures. Artifact SHA256 is `8506fcf5b977d968085179b51de02f7d8a26962640c456f245781c735ac6d7c1`. Evidence is in the spawn worktree at `target/parser-oracle-20260922-v2`. This is fixture evidence, not a bound for all inputs.
- Recovery writer 001d and reviewer 001e completed source review of retained authority and the inspect migration. The four-file API diff is `b623f605c57fc0b120c8b8b655c85bf8bb03dba8b5a386f11a134ee62ed2fa16`. The two-file inspect diff is `3d1146d61019350b8c107df1de1b2d1aef4af255a9e84207d167bc45c11e5d3b`. These changes are unbuilt and uncommitted. Runtime, daemon, Host, and test callers must migrate with the API.
- Inspect now uses the existing daemon transport. Source review accepted the live-daemon and offline tests. The found-session output lacks an executed assertion. The new source uses Hub projection rows rather than the old Core listing.
- Spawn writer 001b continues the real producer and consumer path. Reviewer 0018 checks that path. The capacity enumeration closed at `2 * OWNER_BUDGET_CAPACITY + 5`. Production-path verification remains open.
- Root verified that `split_fixed` permanently reduces the parent ceiling. Temporary scratch must not prevent later valid projection admission. Root directed a scoped scratch lifetime under the original callback parent. A generic join based only on account identity is not accepted.
- Root assigned recovery caller migration to writer 001d and reviewer 001e in their recovery worktree. They own startup, publication, persistence callers, and exact test helpers. Spawn regions and `host_executor` remain with writer 001b. Root will integrate both changes. The breaking API must not publish alone.
- The recovery scope includes persistence callers and typed-result handling in `daemon/control/host_work.rs` and `managed_git.rs`. Root verified their mutation constructors and result consumers. Uncertain writes must retain their state through these cleanup paths without rollback.
- The compiler slot is free. No new build is assigned. Startup-captured paths, final CLI handling of unresolved state, and restart reconciliation remain undecided. Earlier permission denials remain in force.

Recovery evidence: `/Users/jasonconigliari/botster-evidence/durable-recovery-20260921/quarantine-v1/`.
The next decisive checks are a reachable spawn lifecycle and the coordinated recovery caller migration.
Final matched-client verification, production activation, and full foundation acceptance remain open.

## Historical orchestration ledger — September 21

This section preserves the earlier status and assignment record. The current handoff above takes precedence.
Root owns this section. Root updates it at assignments, accepted checkpoints, blockers, user decisions, and handoffs.
Agent acknowledgments do not require new entries. Raw evidence stays in persistent evidence directories.
Each accepted checkpoint must identify its source, review, executed checks, evidence location, and remaining limits.
Root commits and pushes reviewed checkpoints and log updates. Installation and runtime replacement remain unauthorized.

### Delivery contract and priorities

### Decision handoff — September 21, resumed assignments

### Continuation — September 22

Reviewer 0018 accepted the combined lifecycle source contract at report hash `93fe2dbcc49067d96f6aa7caae29bdd6c602fa8b6623fa0cf09eecd74cab3a23`.
Root released writer 001b to implement the bounded capacity and lifecycle changes. This is design acceptance, not implementation acceptance.
The scope includes typed daemon and plugin rows, local transport or Lua conversion receipts, and exact cleanup during terminal drain.
The conditional file scope now includes `src/data_plane/driver.rs` and `src/daemon/owner_loop.rs` for the derived capacity and regressions.
Ordinary plugin spawns must reserve an existing owner permit before work. They can receive a typed capacity refusal under client load.
This operation limit does not replace per-plugin memory accounting. The existing operation limit remains unchanged.
The report update adds this admission rule and the `RetainedUnconfirmed` regression. Reviewer 0018 will check that small delta during implementation.
The next gate is a combined source freeze and test-plan review. No build is granted. Parser activation and final unresolved CLI disposition remain open.

The reviewer accepted the compile-repair increment after independent reconstruction. No rebuild has run.
The writer implemented the runtime-only refusal before materialization and made `WaiterId` required. These changes remain untested.
The writer also kept client-owned reservations out of the shared retained-reservation store. Full daemon ownership remains incomplete.
Root assigned the local transport receipt changes to writer 001b, subject to the complete lifecycle review.
Success means completion at the existing local transport write boundary, not channel acceptance or remote application receipt.
The owner must retain cleanup authority through receipt loss, refused sends, partial writes, and terminal disposal.
The plugin cleanup helper starts detached shutdown and retracts context without confirmed release. It does not satisfy the retained-operation contract.

The capacity audit found five background registration slots outside ordinary owner permits.
The existing bound, `2 * OWNER_BUDGET_CAPACITY`, does not include these slots. Connection capacity does not reserve unused permits.
Root selected `2 * OWNER_BUDGET_CAPACITY + background_slots` for final review. The proposed background bound is five.
This choice preserves ordinary request admission. It does not authorize an arbitrary limit increase or a second queue.
Before implementation, reviewer 0018 must check every production registration owner and the registered, ready, and phase-counter storage lifetimes.
The audit establishes an insufficient capacity guarantee, not an executed exhaustion case. No capacity edit or build is accepted yet.
Root rejected a speculative completion-wake retry because the required wake was not proved.
Root also rejected an alleged charge leak: successful `grow(bytes)` makes the immediate `split_fixed(bytes)` failure unreachable under exclusive access.
The next milestone is one reviewed lifecycle contract, followed by a combined source freeze and focused production-path verification.

The user approved Root's recommendation to require the daemon control owner for session-type spawning.
Runtime-only calls must reject before materialization or Core work. Synchronous callers may wait on a daemon response off the owner thread.
Writer 001b owns the bounded implementation. Reviewer 0018 owns independent lifecycle review. Both received the decision.
The daemon must register the operation before effects or handle escape. The client spawn method must require a `WaiterId`.
The implementation must not transfer cleanup authority from an already-started public pending handle.
Review must include plugin fulfillment, caller timeout, handle destruction, terminal disposal, and exact context retirement.
Unresolved cleanup must retain its identity, payload, charge, and owner permit. This decision does not select final CLI disposition.
Startup-captured paths remain undecided. Earlier build and export denials remain unchanged.

The resumed diagnostic build failed with five compiler errors and two warnings. No tests ran and no artifact was produced.
The failed source freeze and diagnostic records remain in the writer's local `.spawn-review/` directory.
The compile repair remains separate from the lifecycle repair. The revised patch hash is `cf01c5c22c8b2b5052ba032e6cf45caf429b689047f7875d0ba6db846e49056f`.
The reviewer found that the Host result pool and callback memory pool are separate. The writer replaced zero Host result cost with the existing full reserved capacity.
The final increment review remains pending. Root has not granted a rebuild.
Full asynchronous spawn remains incomplete. Production activation, allocation accounting, and lifecycle verification remain open.

RESUMED at the user's explicit request. The user manually selected GPT-6 Sol for the implementer. Root will preserve this choice.
Root checked the live agent inventory and the spawn worktree: HEAD is `a8884a5f`, with no tracked changes and only local `.spawn-review/` untracked.
Writer 001b and reviewer 0018 resume the real daemon consumer and exact context retirement. Other agents remain idle until an actionable assignment exists.
The next slice includes owner capacity followed by Host capacity before dequeue, capacity wake routing, and atomic migration of pre-reservation context publication and raw alias cleanup.
Reuse the 17 passing focused tests as checkpoint evidence. Full asynchronous spawn remains incomplete. Freeze the new source for review before assigning a build.
Root retains commit/push coordination. Prior permission denials and unresolved stop/restart decisions remain open. No new build, export, or deployment is authorized by resumption.
The earlier pause preserved all worktrees and evidence. No agents or worktrees were deleted.

Partial checkpoint `a8884a5f24878b027e0f73e3ed92addc6be914f4` preserves the exact seven-file v5 source. Root verified its scope and pushed it to `origin/delivery/async-spawn-20260921` without force.
The commit distinguishes tested tracker/collector behavior, partially exercised owner binding, and compiled-only inactive implementation. It does not claim full spawn acceptance.
Reviewer 0018 accepted the execution evidence and confirmed identical warning multisets between v4 and v5. Local evidence remains untracked; external export is still blocked.
Root instructed writer 001b to resume the authorized daemon consumer and exact context retirement. No new build, integration merge, deployment, or unresolved-stop policy is granted.

The repaired v5 run passed its build and all 17 exact cases. Root read all 17 result lines and recomputed artifact SHA256 `c5b1ca9e142fb164e33b86f88925022b3be59f7a873a5a357f549dddb30d4b34`.
The writer reports 1,015 unchanged source hashes and 108 unchanged warning records relative to v4. Independent evidence review remains pending.
Source freeze: `02bb017cb7bd558b53f5c82ebeb82f6271179980eb389b82f9c89695b7113daf`. Only the test fixture changed after v4; the failed v4 run remains preserved.
The compiler slot is free. Root requested a commit partition that preserves the tested source without presenting inactive production work as complete.
The next implementation priority remains the actual daemon consumer and exact context retirement. These passing cases do not close daemon activation, full accounting, or unresolved-stop policy.

The writer reports that the v4 build passed in 99.85 seconds. Artifact SHA256 is `0b4621757573bb7ff8b0928273c89e06b29e05df35f5c96c66081e437154b92e`; inventory contains 1,254 tests.
Cases 1–6 each passed once, including live reservation lookup and definitive release. Independent execution review remains pending.
Case 7 failed during fixture setup before terminal polling. Cases 8–17 did not run. The writer stopped and released the compiler slot.
Root checked the failure log and registration code: a waiter cannot register a second batch while its earlier phases remain registered.
Root granted a test-only fixture repair: register both tracker phases together. A three-phase batch can test exact isolation, but cannot establish concurrent independent production registrations.
Preserve the failed run. No rebuild is granted before fixture review. This review missed the helper's registration precondition; future fixture review must trace helper preconditions before accepting test coverage.

Reviewer 0018 accepted combined source `f3649d356f5f2924df4892bbf539cc5f5335ef4e90c0631c6374a5364977173d` and exact-phase increment `2392bd0d701b64389ddb3fe7e342cce39e3fa4e00e2c7e103b47288b2cd7b07d`.
Root assigned writer 001b the sole compiler slot for `owner-tracker-v4`: one library-test build, nine new exact cases, and eight existing exact regressions.
The run must use Rust 1.97.0, two Cargo jobs, incremental compilation disabled, locked/offline dependencies, the original artifact, and the crate directory.
Source edits remain paused. Local evidence must retain source/artifact hashes, effective configuration, exact counts, and diagnostics. Stop at the first failure or permission denial; no repair or retry is granted.
This is a verification grant, not a successful run. Full daemon activation, terminal-loop integration, context retirement, and complete allocation accounting remain open.

Reviewer 0018 accepted tracker v2 at source scope: `ee6a2b188b26c77f4af8527e3e5c46ae23bb1b7acddbcd82114522d7b6f11b45`. No tests have run.
The writer will freeze the tracker repair and exact-phase cleanup change together. The existing v2 artifacts remain unchanged review history.
Root verified that queued tickets already retain their exact identity and wake handle. The reviewer withdrew the contrary claim. No new identity storage is needed.
The bounded source grant collects at most the current tracker's two exact ready phases. Unready registrations and unrelated phases must remain intact.
The next verification plan uses the original Cargo artifact from the crate directory, records effective build configuration, and checks unchanged source hashes.
No compiler slot is assigned until the incremental source review and combined freeze are ready. Type-size output does not close the remaining allocation-accounting gaps.

Source inspection reopened terminal cleanup progression: the existing collector removes one ready phase, while an operation has begin and completion phases.
If both phases are ready, the draft can leave completion registered and return Pending before the terminal loop parks. This is a source finding, not an executed regression.
Writer 001b and reviewer 0018 must define bounded collection of the current operation's exact phases. Disposal stays unwired. Tests must cover simultaneous readiness, staggered readiness, and unrelated-phase isolation.
The projection module is included, but its projection function has no production caller. The reviewer withdrew the production-use claim and warning prediction.

The tracker repair passed independent source review at patch `22015209f8a254c260dc9fc2f434fda19d61e9bacf4188bb89c9a22ce49f1102`.
The tracker now retains the accepted operation ID after completion loss. Three Reserve consumers capture this ID after polling. Capture cancellation keeps its current-pending semantics.
The live loss regression and actual layout checks remain open. Root granted a test-only driver injection for one selected owner Reserve operation; no build is assigned.
Production draft `d3348951e1aff7d1e208d0a6588d5094579b8d734fcd14aac5f8fcbf8e2a08ef` is under review. It has no daemon consumer or activation. Context retirement and disposal wiring remain incomplete.
Automatic approval review denied external evidence export twice. Existing local frozen artifacts support review. Direct user approval is required before export; no alternate export route is authorized.
The recovery pair withdrew the proposed equivalence between process exit and normal release. Process exit skips cleanup for pending launches, and a launched worker can lack a registry record.
The source investigation is closed. Normal same-owner cleanup can continue. Final unresolved CLI disposition still requires a user decision; no worker-survival or restart-safety claim is accepted.

Root resumed the authorized implementation and review assignments after checking live agent status.
The reviewed Core pin is integrated and pushed as `f128195b`. Downstream compilation remains unverified.
Spawn writer 001b remains at `ecb9eb02` with changes in runtime.rs, runtime/session_spawn.rs, session_types.rs, and new lua_runtime/spawn_input.rs.
The writer reports shared stage construction, retained variable allowance, and charged input projection. None of this production work is accepted or activated yet.
The next source checkpoint defines terminal cleanup transitions before disposal wiring. Reviewer 0018 checks the actual ownership and field order.
Recovery reviewers found that existing terminal completion handling can retain a row and finish cleanup before Core stops.
Unresolved rows need explicit ownership. Charges retain their Arc account, but tickets become unusable after Core stops; a result value alone does not preserve a live runtime.
The recovery pair will specify the exceptional-stop caller boundary. No new restart protocol or automatic stop policy is approved.
The completed safety agent 0017 is absent from the live inventory. Its shared integration worktree and evidence remain intact.
No compiler slot is assigned. The denied full-workspace command still requires direct user approval; the request to continue does not override that denial.

Core published cleanup `ab1a7cee` and reservation identity `891e220295427fd93991638d7c62ba40fa25d4ae` on the recovery branch. Main remains `053148f6`.
The full-workspace command did not start: automatic approval review rejected the tool-delivered grant and requires direct user authorization.
Root requested that approval. No retry, alternative tool, native build, or dependency-download bypass is authorized. The compiler slot is free.

Root granted the coherent ordinary spawn implementation to Hub writer 001b in the reviewed production boundary.
Reuse the existing pending bit, shared doorbell, Core stage, Host executor, and exact-loss reply mechanism. No parallel spawn engine or uncharged activation is allowed.
Reviewer 0018 checks owner registration, loss signaling, field/drop order, saturation, both observation orders, all context writers, and the real Lua two-ID test.
Allocation proofs remain acceptance blockers where missing. Source implementation can advance independently of the blocked Core workspace run.

Hub pin patch `1e9daf3e` passed independent source review. Ten files contain exactly 24 old-to-new revision substitutions: 12 manifest pins, six lock sources, six active literals.
All five Core fixture files are byte-identical across revisions. Historical reports and evidence remain unchanged; future-report source tracks the new pin.
Root authorized a separate commit of only the frozen pin diff. Downstream compilation has not run and is not claimed.

Core reviewer 001a accepted cleanup execution and the scoped reservation-identity checkpoint.
Strict focused all-target clippy passed without warnings or unfulfilled lint expectations. Fresh resource/client/route-observer tests passed 21/12/5.
Identity acceptance reuses the unchanged 19 admission tests and seven doctests, including the intended E0308 rejection. No downstream build is claimed.
The separate support build without default features emitted 19 Core unused/dead-code warnings. That configuration was not a strict lint gate.
Root authorized separate cleanup and identity commits, followed by a push of `delivery/core-recovery-20260921` only. Main, tags, and force pushes are excluded.
Root authorized initialization of the existing Ghostty gitlink `eb72ec61304ea256be1d86ed8fa961c84e43ecbd` only.
Native dependency downloads and full-workspace builds require the next scoped plan. Existing Zig 0.16 is available; no installation is assigned.
Hub must keep Cargo unchanged until Root supplies the reviewed published Core SHA. Its production-boundary review continues at clean source `64bcb3af`.
No compiler slot is currently assigned. Full workspace, downstream compilation, allocation-order evidence, and complete spawn acceptance remain open.

Hub fixed-deduction source `64bcb3af` is integrated as `e79dc3fb`. The single-file patch matches reviewed `1e186b89`.
Independent review accepted six exact tests and the 53/9 regression groups against the 1,242-test artifact. Counts overlap.
Artifact: `b51b9e3283d5551371aacea17165dc28c7cbdc974dba19928a94318ba9c4814d`. All 1,014 source hashes remained unchanged.
Warnings remain at 83 records. One unused-method record now includes `split_fixed`; no other diagnostic changed.
This closes the fixed-deduction primitive checkpoint only. The actual daemon spawn boundary is under review; no activation or fresh integration build is claimed.
Core cleanup `097492af` passed source review. Root granted `core-lint-cleanup-v1` for strict focused lint and fresh 21/12/5 test groups.
Core owns the sole compiler slot. Its cleanup and identity commits remain gated on verification.

Core baseline `053148f6` reproduced the obsolete paste fixture failure at the same assertion with the correct working directory.
The reviewed test-only replacement uses scheme-2 unsafe paste, exact routed identity, wake-driven completion, and typed zero byte counts.
Its exact case and all 13 client integration tests passed. Reviewer 001a accepted artifact `614e624a` and the raw execution records.
Combined focused behavior evidence covers 77 disjoint passing cases across unchanged library/session source and the repaired client test.
The fixture is committed separately as `d4f92ccb`. Identity patch `3a634ee5` remains frozen and uncommitted.
The non-strict focused lint inventory completed with 53 diagnostics. It is not strict acceptance.
Root authorized one four-file cleanup for those diagnostics. Patch `097492af` is frozen for reviewer 001a; no build or cleanup commit is granted yet.
The cleanup preserves StepFailure and uses five narrow lint expectations for intentional rich failure diagnostics. Runtime policy remains unchanged.

Hub fixed-deduction patch `1e186b89` passed source review, including derived lease/oracle layout checks.
Root assigned the sole compiler slot as `fixed-deduction-v1`: fresh library artifact, inventory 1,242, six exact tests, memory group 53, acknowledgement group nine.
The test counts overlap. Preserve source/artifact hashes and compare warning records against 83; stop on any failure or mismatch.
The Hub pair separately prepares the actual ordinary daemon path, using existing exact-loss reply machinery rather than a new notifier.
Registration before handle escape, proposed field/drop order, and both completion/receipt observation orders still need implementation proof.

Core behavior verification currently has 76 passing cases and one unresolved client integration failure.
Behavior v1 used the wrong working directory for a source-reading test. V2 used the crate directory and passed all 12 session integration tests.
The remaining paste test sends scheme-1 frames and expects JSON results. Current source rejects that input before admission and emits binary results.
The old `session_not_writable` premise was retired. These are source findings; baseline execution has not yet established historical failure.
Root granted one exact baseline test at `053148f6` in `/private/tmp/core-paste-baseline-zop7pofa`, with correct cwd and unchanged source.
Writer 0019 owns the compiler slot. Preserve the actual failure and provenance; do not retry or edit the test under this grant.
Reviewer 001a will identify a current rejection premise that preserves operation identity, zero writes, and a bound route. No test repair is authorized yet.
The non-strict lint inventory remains pending; the earlier conditional grant did not run after the client failure.

Hub's actual producer creates a reply endpoint before Host materialization. The funded helper is test-only, but the unfunded production channel already crosses that boundary.
Splitting the existing charge seals later growth. Root reopened the affected accounting design instead of weakening sealed descendants.
The Hub pair must compare a shared operation allowance with a fixed-charge deduction from the remaining growth ceiling across all ownership and cancellation paths.
No extra independent callback allowance, blanket input-limit prepayment, or implementation change is authorized by this design review.

Hub charge-phase source `b1af5eae` is integrated as `39e04f28`. The two-file patch hash matches accepted `0b34c8ae` exactly.
Independent review accepted all eleven exact tests and the 47/9 regression groups against the 1,236-test artifact; these counts overlap.
All source manifests held. Warnings changed from 81 to 83 solely for the dormant growth API, with no removals.
Tests executed the evidence copy, not the Cargo output path. The reviewer corrected this claim against actual command records; hashes match.
This closes primitive verification only. No fresh integration build, context activation, or complete asynchronous spawn acceptance is claimed.
The Hub pair now traces reply-channel charge splitting before enqueue against later materialization growth. Sealed descendants must remain sealed.

Core focused v4 passed formatting but stopped at six test-support lint errors. No planned behavior test ran in v4.
Root granted the separate `core-identity-behavior-v1` run for the same 77 cases at frozen `6d5308d5` plus identity `3a634ee5`.
The next lint step is one non-strict focused inventory, after a separate grant. Strict acceptance remains required; StepFailure stays unchanged pending that inventory.

Core lint repair is committed as `6d5308d5`. Reviewer 001a verified the exact three files and accepted hashes; identity diff `3a634ee5` remains unchanged.
Root assigned the sole compiler slot as `core-identity-focused-v4` after Hub released it.
The grant covers formatting, strict focused clippy, and fresh Core library and two integration test artifacts.
Require listed and executed counts of 21 resource, 12 client, and 19 admission library tests, plus 12 session and 13 client integration tests.
Execute artifacts in place and verify their hashes before and after. Reuse unchanged v2 doctest evidence; stop on the first failure.
The full workspace gate, direct box-deallocation check, and second-paste rejection execution remain open.
Hub reports all charge-phase selections passed with 1,236 inventory entries and unchanged source manifests. Independent execution review is pending.
Artifact: `3fc1287792d5b1e9b04feb8ca8b64b6e4fc0fa14e7303867c5c7275fc040964d`.
The writer reports warnings increased from 81 to 83, solely for the unused growth error and growth/shrink methods. The reviewer must verify that comparison.

Core reviewer 001a accepted three-file lint patch `c4d3cf92` against live source and its before/after hashes.
Root authorized its separate normal commit, staging only those three files. Identity patch `3a634ee5` must remain unchanged and uncommitted.
The unbox body and release sequence are byte-unchanged. Existing tests do not directly observe outer-box deallocation; that evidence gap remains open.
Core will prepare exact test targets and positive counts before its next grant. Integration tests must not be mistaken for library module tests.
Hub owns the compiler slot for charge-phase verification. The writer reported build process 32466 running; no test result is accepted yet.

Root granted Hub writer 001b the sole compiler slot for `charge-phase-v1`.
The plan requires one fresh library artifact, 1,236 inventory entries, eleven exact tests, and regression groups of 47 and nine tests.
Group counts overlap the exact tests. Record artifact identity, unchanged source manifests, and warning differences; stop on a failure or mismatch.
This run does not establish allocation measurements, context migration, or daemon-path acceptance.
Core writer 0019 may prepare the three-file lint repair while Hub verifies. Reviewer 001a must accept the frozen diff before compilation or commit.
Preserve the existing Box-taking unbox helper with a narrow explained `clippy::boxed_local` allowance. Do not change the trait solely to avoid this lint.
The function boundary preserves outer allocation release before resource destruction. Existing allocation-order evidence still needs identification.
The other repairs preserve the empty-queue return, exact Result type, and existing `>= 1` predicate as a compile-time assertion.
The identity patch remains frozen. No Core compiler slot, identity commit, or new allocator harness is granted.

Core focused v3 passed formatting, then failed strict clippy with four diagnostics in three unchanged files.
The diagnostics concern question-mark syntax, a constant assertion, the resource unbox helper, and a redundant result wrapper.
No identity diagnostic appeared, but the failed run does not pass the Core gate. These failures have no executed baseline comparison yet.
Source manifests, HEAD `4afa8e52`, and identity patch `3a634ee5` remained unchanged. Writer 0019 released the compiler slot.
Root assigned writer 0019 and reviewer 001a a bounded source review of those four diagnostics. No repair or rerun is authorized yet.
The unbox helper must preserve outer-box deallocation before the supplied resource drops. A syntax-only repair must not change that contract.
Hub writer 001b will submit the prepared charge-phase test plan for the next compiler grant. No compiler slot is currently assigned.
Raw Core evidence: `evidence/core-identity-focused-v3` in the Core writer worktree. Full workspace and identity acceptance remain open.

Core v2 passed 19 admission tests and seven doctests, including the intended E0308 identity rejection.
Strict workspace clippy stopped in protocol code before it checked Core. A separate baseline run reproduced that lifetime diagnostic and the unused-mut warning.
Reviewer 001a accepted the two-file lint repair, committed as `4afa8e52` after formatting commit `f8e0b759`.
The uncommitted identity patch remains `3a634ee5`. Root granted the sole compiler slot as `core-identity-focused-v3`.
Run pinned formatting, then strict all-target clippy for `botster-core` and `botster-terminal-protocol`. Stop on the first failure.
The verified dependency graph excludes Ghostty from this focused run. Full workspace verification remains open because the pinned Ghostty submodule is uninitialized.
Zig 0.16.0 is available. No submodule initialization, dependency update, or identity commit is authorized by this grant.
Core evidence is under the writer worktree's `evidence/core-session-reservation-identity-v2`, `core-identity-baseline-diagnosis`, and `core-identity-lint-repair` directories.

Hub reviewer 0018 accepted charge-phase patch `0b34c8ae` in report 36. The patch changes exactly two files at base `076fa923`.
Only total admission opens growth. Every successful split seals both outputs, including zero splits; failed splits preserve state.
Shrink preserves state, and lease sharing seals growth. Existing layout formulas derive the changed charge size automatically.
The reviewer checked all eleven new test bodies. Formatting and diff checks passed with all 1,014 source hashes unchanged.
No charge-phase test has executed. Hub writer 001b will prepare exact selectors and regression counts while Core holds the compiler slot.
Evidence: `s1-async-spawn-20260921-sess-001b/charge-phase-v1-verification` and reviewer report `36-charge-phase-v1-source-verdict.md`.
This primitive does not close funded context migration or the real daemon spawn path. Recovery remains held on restart policy and protocol review.

Core v1 verification stopped at a reproduced baseline-only format failure. No compilation ran in v1; failed evidence remains preserved.
The reviewed formatting-only repair is committed as `f8e0b759` on the Core writer branch. Reviewer 001a confirmed identity patch `3a634ee5` is unchanged.
Root's conditional `core-session-reservation-identity-v2` compiler grant is now active. Run the full pinned verification sequence from formatting onward.
Hub reviewer 0018 accepted the charge-primitive contract at its scope. Root authorized implementation in `lua_memory.rs` and focused tests, not caller activation.
Split descendants must remain sealed; explicit shrink preserves state; sharing seals; growth checks the complete unsplit charge and aggregate delta before mutation.
The pair must agree zero-split semantics and audit allocation layout. A size_of-derived formula adjusts automatically; only proved stale assumptions justify extra edits.
Hub may edit and freeze source while Core verifies, but it has no compiler slot. Context ownership and recovery restart acceptance remain incomplete.

Core reviewer 001a accepted identity patch `3a634ee5d65a07deac0d1947ffa5d038bd03f7049794e31bf2319593b4098380` for verification.
Root granted writer 0019 compiler slot `core-session-reservation-identity-v1` after checking no Rust build was active.
Run pinned Rust 1.97 format/diff checks first, then fresh admission tests, doctests with actual E0308 rejection evidence, and the documented clippy gate.
Preserve full source/artifact provenance, positive selected counts, and warning comparison. Stop on unexpected failure before editing or rerunning.
Earlier formatting used an older rustfmt and does not satisfy the pinned gate. No Core commit or Hub dependency update is authorized yet.
Hub charge review identified proposed growth on the existing charge type. Root requires sealed descendants, explicit shrink semantics, and complete live-byte conservation before approval.
No new Hub charge API or context source is authorized. Core verification proceeds independently of these design checks.

Core owner 0019 proved an opaque detached scope/generation identity can preserve reservation equality without retaining its Arc graph.
Root authorized Core-only identity type, accessor, reexports, and focused tests at baseline `053148f6`, with independent reviewer 001a.
No new counter, serialization, numeric getters, or release capability is allowed. Real reservation ownership remains in spawn/cleanup stages.
The token is process-local and does not prove current reservation liveness or launch success. No Core build or Hub dependency update is granted yet.
Spawn pair continues the context producer/read charge trace using the proposed detached identity. No context source grant exists.
Writer proposed a phase allowance in `context-parent-transfer-trace.md`; reviewer 0018 must first check existing account primitives and complete live-byte conservation.
That shared-account API remains unapproved. No independent helper-removal or dormant context-sizing slice is assigned.
Core and Hub pairs can progress independently. Recovery protocol changes remain held pending restart policy and review.

Selector source `076fa923` is integrated as `106e412a`; the single-file cherry-pick completed without conflicts.
Verification remains scoped to the reviewed source branch. No fresh whole-integration build or final delivery acceptance is claimed.
Writer 001b is tracing the approved context caller/identity boundary. Reviewer 0018 independently checks its ownership requirements.
Context and acquisition source remain unchanged. No compiler slot is assigned. Orchestration continues beyond this checkpoint.

Reviewer 0018 verified selector execution in verdict 28. Root accepted the checkpoint and authorized its normal single-file commit.
Five new exact tests and two existing target tests passed once each. Selection and catalog parity groups passed five each; counts overlap.
Artifact `b172f82d6791826c59599868c4b2002db8bdd86a52f077d4924ef9385822996c` contains 1,225 tests; all 1,014 source hashes held.
Warnings fell from 83 to 81 solely because the borrowed selector is now used. No performance claim follows. Integration awaits source SHA.
Root assigned the pair the approved post-reservation context boundary next: all producers, exact-attempt cleanup, both aliases, retained reads, and persistent charges.
They must identify actual dependencies rather than block this independent work on unresolved libc acquisition scope. No context source or build grant yet.
No compiler slot is assigned. Recovery remains held on restart policy and protocol review, not on the completed selector checkpoint.

Reviewer 0018 accepted selector freeze `51b1bd05` in verdict 27 after checking all three callers, distinct filters, diagnostic order, and five parity tests.
Root granted writer 001b compiler slot `source-selection-borrowed-v1` after confirming no Rust build was active.
The grant covers one fresh artifact, five exact new tests, and relevant existing selection/target/catalog parity tests with positive inventory-matched counts.
Use Rust 1.97, two jobs, disabled incremental compilation, locked/offline dependencies, and unchanged target configuration. Preserve all 1,014 hashes and warning comparison.
Repeated predicate formatting is a disclosed cost; no net allocation-traffic or performance improvement is measured. Stop on failure before edits or reruns.
Recovery pair confirmed a joint crash-evidence packet and holds further protocol variants pending restart policy and independent protocol review.
The proposed slot still does not establish sticky-quarantine persistence. No recovery source or schema change is authorized.

Recovery restart analysis found that existing RecoveryRecord cannot represent generic state mutation without a new schema/protocol.
A bounded pending-slot candidate remains unapproved. Its clear rename can survive despite failed directory sync, so startup can see no marker after live quarantine.
Confirmed candidate-data durability does not prove successful marker clearing or preserve quarantine across restart. The pair must not claim unconditional restart safety.
Runtime base revision resets on load and is not durable identity. Persisted attempt/host identity and its candidate relationship need proof.
Successive C1/C2 file replacements do not retain both prior documents. Root requested an exact crash-boundary evidence inventory, not another protocol variant.
Jason's conservative restart-refusal decision remains pending. No schema, recovery source, marker protocol, or compiler grant is authorized.
The pair proposes moving the existing shared-view budget before startup mutation and transferring that same budget into publication; source implementation remains held.
Spawn selector source is frozen at `51b1bd056d91aae366fa2484e5ede2a03e29a22ce0da9cb53313e109ea1bfdaf` for whole-source review.
Spawn acquisition now requires error headroom before I/O. Rust-visible I/O bounds have source evidence, but libc scope is not settled by counter visibility.

Root authorized spawn writer 001b to simplify source selection in `src/session_types.rs` only, including parity tests.
The change reuses borrowed selection, migrates all three owned callers, and removes full-vector clones and owned sorting without changing selection semantics.
Reviewer 0018 must check the frozen patch, iterator-signature change, all callers, diagnostic peer order, target eligibility, and qualified loser behavior before compilation.
Recovery pair jointly selected terminal outcome handling (C). Ordinary successful drain prevents late state writers; panic/unwind bypasses that drain and remains unproved.
The pair found post-rename path-clone and long-path CString allocations. The coordinated plan must remove or pre-fund them without adding a path limit.
Descriptor-relative checks must preserve pathname-replacement detection. No source grant for recovery or compiler slot is assigned.
The existing terminal wait and process-driver final disposition remain open; reporting an unresolved outcome does not prove retention or restart safety.

Recovery pair corrected the lifecycle premise: successful tracked `serve_daemon` drain receives completions before stop; terminal disposal can discard typed outcomes.
Root selected interception before opaque disposal, charged terminal evidence, and an owned unresolved outcome returned by explicit stop as the API direction.
Report known synced commits even when the reply is lost. Published sync uncertainty must retain candidate, prior view, and authority; printing is not reconciliation.
The pair prepares one coordinated patch plan. No new lifecycle parent, automatic shutdown, or unrelated terminal scheduling change is assigned.
Direct-drop reachability, startup exceptions, charged transfer, and restart treatment must remain explicit. No source or build grant has been issued.
Spawn writer completed exact Rust 1.97 I/O source analysis at compiler commit `2d8144b7880597b6e6d3dfd63a9a9efae3f533d3`.
Its revised growth proposal retains one parent during acquisition and splits final storage only on return. Reviewer 0018 must verify live-overlap coverage.
Darwin libc internal allocation behavior remains unproved; no exclusion is inferred. Evidence: writer `rust197-io-allocation-proof.md` and pinned source manifest.
Reviewer 0018 also found a possible existing borrowed-selector replacement for whole-vector clones and sort scratch; parity review precedes source changes.

Recovery writer 001d found no safe startup-only activation: existing Host mutations still construct path-only writers outside retained authority.
Root declined another dormant startup slice and assigned the shared authority/outcome lifecycle as the next recovery task.
The writer must compare two concrete designs for authority transfer, late completion retention, retirement, capacity funding, and restart evidence.
Reviewer 001e independently checks actual daemon/process owners and the late-worker failure path. No owner waits, polling, automatic rollback, or retry is authorized.
The design must separate in-process retention from durable restart reconciliation and identify only genuine product choices. No edits or builds are assigned yet.
Evidence: `durable-recovery-20260921/retained-startup-boundary-9.md` under `/Users/jasonconigliari/botster-evidence/`.
Spawn writer 001b is investigating exact Rust 1.97 I/O allocation and finite-parent buffer growth; reviewer 0018 investigates collection allocation correspondence.
Both implementation/review pairs have active assignments. No compiler slot is assigned.

Jason approved retained authority for File startup saves: "Yes—require retained authority".
Jason also removed backward-compatibility constraints globally: "No worry about backwards compatibility or old callers with anything. None of this is used yet".
Do not add legacy shims or preserve unsafe unbound File saves. Migrate current repository callers when changing an interface.
The specific File-save decision leaves custom stores unchanged. Surface any necessary wider store-contract change rather than assuming it.
Compatibility relief does not waive accounting, error ownership, intended product behavior, or the no-owner-wait requirement.
Root resumed recovery writer 001d and reviewer 001e on the smallest coherent retained-authority startup checkpoint and current-caller migration.
They must separate startup ownership from unresolved late-worker outcome retention and restart treatment. No production quarantine activation or build is assigned.
Root also sent the compatibility decision to the spawn pair so they can remove compatibility-only obstacles without duplicating semantic implementations.

Jason approved the context visibility decision: "Yes—publish after reservation".
Spawn context may become visible only after Core reserves the session ID, before process launch. The previous timing question is resolved.
Failed reservation must not publish or replace context. Cleanup still needs exact attempt/generation ownership for both aliases and must preserve retained reads.
Persistent context charges and cleanup implementation remain engineering requirements. This decision does not waive them or authorize an unspecified mechanism.
Root sent the decision to writer 001b and reviewer 0018. Their current acquisition and collection investigations continue without overlapping context edits.
The separate retained-authority File-save compatibility question remains unanswered.

Jason challenged Root's stop after the error-sizing checkpoint. Root stopped too early; checkpoint completion did not end the authorized orchestration task.
Root resumed writer 001b on a concrete funded full-definition acquisition contract, including ownership, I/O errors, and file-change behavior.
Reviewer 0018 independently traces pinned collect, clone, and stable-sort allocation behavior on actual selection paths, then reviews the acquisition contract.
Both assignments are bounded source investigations. They must return concrete interfaces or precise failed operations, not another broad blocker inventory.
Root inspected the ordinary and catalog readers directly. The ordinary reader uses `fs::read`; catalog acquisition admits metadata-sized storage and probes one extra byte.
Their diagnostic and growth behavior differs. No silent semantic replacement, source edit, or runtime activation is authorized by these investigations.
Root requested Jason's pending decisions on post-reservation context publication and retained-authority File saves without stopping independent engineering work.
No compiler slot is assigned. Root must continue from assignment results instead of treating an integrated checkpoint as task completion.

Construction-error sizing source `6bd7bf75` is integrated as `60f864b6`. The two-file cherry-pick completed without conflicts.
Independent verification remains scoped to the source branch. No fresh whole-integration build or production acceptance is claimed.
The writer is idle after the accepted checkpoint. No compiler slot or further implementation assignment is active.
Remaining construction gates include funded repository acquisition, collection allocation correspondence, and environment path acquisition.
The broader stack, context, Core transfer, recovery, and ordinary daemon-consumer gates remain open. This checkpoint does not waive them.

Reviewer 0018 verified construction-error sizing execution in verdict 23. Root accepted the checkpoint and authorized its normal two-file commit.
Six new exact tests and the ErrorImpl guard each passed once. The enclosing group passed seven tests; the counts overlap.
The fresh artifact contains 1,220 tests. All 1,014 source hashes matched, format and diff checks passed, and 83 warnings remained unchanged.
Artifact SHA256: `b50b865bd900b8043002132b732541beea849d4a3bac5f8730febe2749a17bfa`.
This closes only the covered error-sizing prerequisite. Funded construction, excluded I/O errors, environment paths, and source-selection collections remain open.
Evidence: `construction-error-sizing-v1-result.md` and its verification directory in the existing writer evidence directory.
Integration awaits the source commit. No compiler slot is assigned and no new source work is authorized.

Reviewer 0018 accepted construction-error sizing source in verdict 22. Patch SHA256: `b52968dc9263bf4b47cf8f4640365bd9a7696c4fae5f1a7822b8d59f54efe4d0`.
Exactly two files changed. The parser arithmetic is unchanged; concrete construction candidates include wrapper overlap without Serde-specific terms.
Root granted writer 001b compiler slot `construction-error-sizing-v1` after checking that no Rust build was active.
The grant covers one fresh library artifact, six exact new tests, their enclosing error group, and the ErrorImpl layout guard.
Use Rust 1.97.0, two jobs, disabled incremental compilation, locked/offline dependencies, and unchanged target configuration.
Preserve all 1,014 source hashes, raw results, artifact provenance, and warning comparison. Stop on any mismatch or failure before edits or reruns.
Reviewer 0018 must verify execution evidence before commit acceptance. The checkpoint does not fund construction, cover I/O errors, or activate a permit.

Root authorized a narrow construction-error sizing checkpoint after inspecting the existing parser formatting arithmetic and reviewer verdict 21.
Writer 001b owns `materialization_error.rs`; `bounded_catalog.rs` may only expose `validation_error_text_bytes` if its covered family is verified.
The checkpoint shares checked formatting arithmetic without changing parser bounds. Construction errors must not inherit Serde boxes or position terms.
It must enumerate concrete error families, wrapper overlap, overflow refusal, and exclusions. I/O coverage and funded construction remain unresolved.
No generic formatter, runtime caller change, reader extraction, message change, or unnecessary candidate accumulator is authorized.
Reviewer 0018 must review the frozen source and proposed tests before a compiler grant. No compiler slot is assigned.
The reader reuse check confirmed partial mechanisms only: catalog projection loses required fields, local Budget is not Lua funding, and snapshot semantics differ.
Evidence: writer `repository-reader-reuse-check.md` and reviewer `21-reuse-verdict.md` in the existing evidence directories.

Reviewer 0018 accepted the trait JSON derivation in source: maximum encoded length 4,193 bytes and conservative buffer overlap 16,772 bytes.
These are allocation-request bounds, not measured allocation peaks or executed tests. No complete materialization permit follows.
Construction-error formatting still needs demonstrated reuse of the existing bound. Source selection still lacks funded acquisition and collect/sort correspondence.
Root assigned one narrow read-only interface check: trace repository acquisition to existing funded-reader code and identify the reusable formatting-bound helper.
The writer must distinguish an unwired mechanism from a missing contract. Reviewer 0018 checks those findings before any implementation decision.
Environment path acquisition remains unresolved. No code changes or compiler slot are authorized.
Evidence: writer `three-input-terms-derivation.md` and reviewer `19-three-terms-verdict.md` in the existing evidence directories.

Reviewer 0018 withdrew the startup-caching solution and the claim that a complete bound follows from resolving paths.
Four construction prerequisites remain unproved: environment path acquisition, formatting growth, selection collections, and construction errors.
These are technical questions, not impossibility results or a request to relax accounting. Independent increments are not the proved sole alternative.
Root assigned writer 001b a concrete read-only derivation of the three input-derived terms: formatting, selection collections, and construction errors.
The output must contain formulas, exact constructor correspondence, allocation overlaps, failure candidates, and proposed decisive tests, or the failed premise.
Reviewer 0018 will review that proof. Environment paths, Core transport, context policy, and stacks are outside this bounded derivation.
No source edits or builds are assigned. Evidence for the corrected prerequisites: `hub-async-spawn-review-20260921-fable/18-unresolved-prerequisites.md`.

The complete allocation table now reuses one admitted parent and disjoint splits. The writer withdrew the proposed new aggregate owner and Lua memory API.
Reviewer 0018 proposed a two-file construction boundary, but Root withheld implementation authorization because its prerequisites remain unproved.
Moving `current_dir` or `current_exe` to startup does not establish admission before their first allocation. Caching also needs a behavior proof.
Root found no `set_current_dir` or `chdir` call in this worktree's `src` and `crates`; that search alone does not authorize changed path semantics.
Resolving path acquisition does not by itself prove the complete selection, formatting, and error bound before construction.
The reviewer must return the corrected prerequisite list. No further broad design pass, caching, source edits, or builds are assigned.
Evidence: `complete-materialization-ownership-table.md` in the writer evidence directory and `17-minimal-first-boundary.md` in the reviewer evidence directory.

Root assigned an independent technical pass on complete materialization allocations and enforcement of the per-callback ceiling.
Writer 001b must map actual allocations, existing charges, overlapping lifetimes, refusal ownership, and final release through Core copies.
The pass must determine whether splitting one admitted parent charge can enforce the ceiling before proposing another mechanism.
Reviewer 0018 must verify the table and minimal private interface. No source edits or builds are authorized yet.
The reviewer confirmed detached workers can outlive executor destruction. Root rejected the recommendation to bypass stack accounting for activation.
A requested stack size, recursion-depth limit, or worker-body exit signal does not alone prove mapped-stack ownership and release.
Technical allocation work can proceed independently. Runtime activation remains gated; no stack exclusion or new limit is approved.

Writer 001b completed the read-only materialization/stack pass. Reviewer 0018 must verify its premises before implementation resumes.
The parser model does not itself cover source resolution, context, metadata, and Core request copies. Existing coverage for those allocations requires review.
The writer also reports that separate callback reservations lack a shared per-callback ceiling. The reviewer must check existing ownership mechanisms before adding one.
Host workers receive no account and can survive executor destruction through disposal permits. No explicit stack size or stack account policy was found.
A guard inside a worker does not alone prove platform stack-release timing. Requested stack size and actual allocated storage must remain distinct.
Root requested the smallest evidenced contract or user decision, not an invented quota, exclusion, or accounting mechanism.
Evidence: `s1-async-spawn-20260921-sess-001b/materialization-stack-contract-pass.md` under `/Users/jasonconigliari/botster-evidence/`.
No source edits or builds followed this pass. Consumer activation remains held.

Materialization source `f7ea2c67` is integrated as `71fee4e0` after independent source and execution review. The checkpoint remains dormant.
No whole-integration build or production acceptance follows from the source-branch tests. The cherry-pick completed without conflicts.
The next consumer proposal exposes unresolved charged-ingress, materialization, worker-stack, context, and recovery dependencies.
Root assigned writer 001b and reviewer 0018 one bounded read-only pass on the charged materialization interface and worker-stack ownership.
They must identify actual thread lifetime, account ownership, reservation and release points, and existing policy values before proposing implementation.
No stack size, quota, exclusion, consumer activation, source edits, or builds are authorized by this pass.
Evidence: `s1-async-spawn-20260921-sess-001b/next-production-path-proposal.md` under `/Users/jasonconigliari/botster-evidence/`.
Context timing and recovery lifetime remain separate unresolved gates. No compiler slot is assigned.

Reviewer 0018 verified the v2 raw evidence in `hub-async-spawn-review-20260921-fable/14-v2-execution-verdict.md`.
Root accepted the dormant nine-file checkpoint and authorized its normal commit. Integration awaits the writer's commit SHA.
The reviewer recomputed the artifact hash and checked all 26 test results against the 1,214-test inventory.
The specific 256-definition refusal concern is resolved at model scope. No quota change or capacity-policy decision is needed for that fixture.
This does not establish observed allocation peaks, general capacity acceptance, or absence of conservative refusal on other inputs.
Root requested one bounded proposal for the next production-path step. No further source edits or builds are authorized by that request.
Worker-stack ownership, funded ingress, the ordinary daemon consumer, full-channel wake recovery, and the real two-identifier milestone remain open.

The writer reports that materialization v2 verification passed. Reviewer 0018 now owns the raw-evidence review; checkpoint acceptance remains pending.
All 19 exact selections passed one test each. Seven groups passed 23, 4, 4, 4, 6, 5, and 1 tests; these counts overlap.
The recorder reports unchanged 1,014 source hashes and 83 warnings. The artifact contains 1,214 tests.
Artifact SHA256: `a0de6db05e008c65e4a601dc38b8de79f5e687608420139dd56c60f73df87768`.
The corpus input was 2,666,149 bytes. The model bound was 207,351 bytes; their sum was 2,873,500 bytes against the unchanged 8,388,608-byte quota.
These are model values, not observed allocation peaks. Separate output and scratch totals were not collected. Production capacity acceptance remains open.
Evidence: `s1-async-spawn-20260921-sess-001b/materialization-definition-release-v2-result.md` and its verification directory under `/Users/jasonconigliari/botster-evidence/`.
The writer released the compiler slot. Source remains frozen and uncommitted until independent execution review.

Reviewer 0018 accepted the complete v2 source in `hub-async-spawn-review-20260921-fable/13-v2-model-verdict.md`.
Root assigned compiler slot `materialization-definition-release-v2` to writer 001b after checking that no Rust build process was active.
The grant covers one fresh library test artifact, focused checks, affected parser/accounting groups, and the ErrorImpl layout guard.
Use Rust 1.97.0, two jobs, disabled incremental compilation, locked/offline dependencies, and the existing target configuration.
The writer must preserve source hashes, artifact provenance, raw results, positive test counts, and warning comparisons for independent review.
Stop on source mismatch, compilation failure, or an unexpected test failure. No source checkpoint acceptance or runtime activation follows from this grant.
The corpus check establishes model quantities only. Observed allocations, production capacity acceptance, and worker-stack ownership remain open.

Materialization v2 is frozen at patch SHA256 `4ebd6f595333129e306d756539a91df9c2dc9709aa86b0931910a5877ab4f924`.
The writer reports 1,014 source hashes and passing format preflight. Only three files differ from v1; the other 1,011 hashes match.
Root assigned the complete revised model to reviewer 0018. No compilation or test execution has occurred for v2.
The proof separates earlier typed failures from later typed prefixes. Counting success does not establish typed success.
The preserved maximum and typed-error bound cover earlier failures. The pinned destruction chain protects later prefixes after the release.
The proposed 256-definition check compares model bounds only. It does not measure allocations or establish production capacity acceptance.
Evidence: `s1-async-spawn-20260921-sess-001b/materialization-definition-release-v2-proof.md` and adjacent patch and manifest under `/Users/jasonconigliari/botster-evidence/`.

The reviewer accepted materialization model `0e1a4c50` in source. Its 1,014-file manifest and passing format preflight remain preserved.
No build or runtime proof follows from this review. The 256-definition candidate may expose conservative capacity refusal.
Root authorized `materialization-definition-release-v2` within the same nine files after an independent review of the pinned ownership chain.
The writer must release only completed-definition temporary storage. The release must exclude parent-vector growth and remain unreachable on child failure.
The model must retain output, decoder scratch, prior owners, the recorded maximum, and the separate typed-error bound.
The reviewer must check the complete revised model before a compiler grant. No quota change, permit activation, or stack-accounting exclusion is approved.
Evidence: `hub-async-spawn-review-20260921-fable/11-materialization-model-verdict.md` and `12-completed-definition-release-endorsed.md` under `/Users/jasonconigliari/botster-evidence/`.
The writer's `completed-definition-release-audit.md` is under `s1-async-spawn-20260921-sess-001b/` in that evidence directory.
Recovery adapter work is held. Returned authority (B2) is selected, but unbound public File saves need Jason's compatibility decision.
HostExecutor drops closed-delivery completions. A detached writer can finish after daemon stop returns; no surviving outcome reporter is established.
The reviewer withdrew both the assumed shutdown/status consumer and authority-slot retention claim. A log is not retained recovery evidence.
The sticky write pause remains approved, but late-worker evidence ownership and restart reconciliation are unresolved activation gates.
Recovery source stays clean until a concrete lifecycle contract is selected. No compiler slot is currently assigned.

Jason approved conservative affected-store write pause after a visible write whose disk synchronization fails: "Sure, we can be conservative".
Retain the unresolved candidate, prior state, and write authority. Report a clear typed error and refuse further affected-store persistence.
Do not start effects that require the unavailable persistence step. Unrelated reads, sessions, and terminal transport continue.
No automatic rollback, retry, clearing after a later sync, or shutdown is authorized. Writes can remain paused until reconciliation is separately selected.
This policy approval does not approve every adapter mechanism. The writer and reviewer must settle exact capability and failure-bundle ownership before shared edits.
Path restoration does not automatically clear the pause. A path-current check alone cannot enforce this policy.

Storage registration `7d2e18e2` is integrated as `896af154` after independent source and raw-evidence acceptance.
Nine macOS storage tests and fifteen recovery regressions passed. Removing lock and link checks caused the intended failures; restored controls passed.
Baseline and restored binaries match `72ef6c8820ac205339f507e16d38f4ee8d65c4eb52eacf4aaf8b458d8972069e`.
Evidence: `/Users/jasonconigliari/botster-evidence/durable-recovery-20260921/storage-registration-v1/`.
The macOS build newly compiles pinned rustix 1.1.4 with std/alloc/fs; dependency versions and checksums did not change.
The storage module has no production adapter. Linux, pre-rename race, drive-cache durability, crash recovery, and strict lint remain open.
Recovery next prepares the exact production adapter and ownership-lifetime handoff plan before edits. No compiler slot is currently assigned.
Spawn implements the reviewed materialization model source boundary; its only extra catalog change is helper visibility for the pinned error-size bound.

Remaining receipt tests `28b3afca` are integrated as `92cedcca` after independent source and execution review.
Nine exact tests passed once each; enclosing groups passed 10, 6, and 4 tests, reported separately because counts overlap.
Artifact: `63fffb6dc6599c65a904b4456491562027d95572167ca548efc37158199c8017`; all 1,013 hashes held; 83 warnings unchanged.
Evidence: `/Users/jasonconigliari/botster-evidence/s1-async-spawn-20260921-sess-001b/receipt-remaining-nine-v1-verification/`.
All twelve specified receipt/reply cases now have executed evidence. The surface remains dormant; immediate receive does not cover worker wait-context storage.
The ordinary consumer, full-channel wake recovery, funded ingress, typed materialization, and real daemon milestone remain open.
Spawn next prepares one bounded materialization plan before edits. Recovery owns compiler slot `storage-registration-v1`.
Storage verification covers macOS operations, nine storage tests, fifteen recovery regressions, and reviewed lock/link controls.
The ineffective rename control is withdrawn. Linux execution and rename-race execution are not claimed. No production storage activation is authorized.

Classifier `b904be36` is integrated as `6e4a51e6` after independent verification of 15 baseline tests, four intended negative failures, and four restored passes.
Restored and baseline artifacts match: `ac6c198dcbf17753210aa55965b51ce12e63fabadab2107dff17120411d5c08b`.
Evidence: `/Users/jasonconigliari/botster-evidence/durable-recovery-20260921/classifier-v1/`; 1,007-file manifests restored exactly; 107 warning diagnostics per build.
This changes only restart classification. Recovery consumers, durable reuse, storage activation, and strict lint remain open.
Recovery now prepares the reviewed storage module registration and exact pinned rustix dependency edge as a separate checkpoint.
No production store replacement, protocol application, or schema change is assigned in that checkpoint.
Spawn owns compiler slot `receipt-remaining-nine-v1` for the accepted remaining-nine test patch `9893d46c`.
The source removes an unused refusal classification while preserving returned ownership. Worker blocking-receive storage remains unwaived.

Receipt tests `8d4ca50a` are integrated as `b2832401` after independent raw-evidence review.
Three exact tests passed once each. Enclosing groups passed 3 and 8 tests; those groups include the exact tests and are not additive.
The tests establish receipt loss, refused mint preserving phase identity, and quota conversion reporting Abandoned through actual entry paths.
Artifact SHA256: `fd62ab6759c7a47c36497a8ac3ca4ef619c341da9d0aac925fe9e1233d6be464`; 1,013 source hashes held; 83 warnings.
Evidence: `/Users/jasonconigliari/botster-evidence/s1-async-spawn-20260921-sess-001b/receipt-first-three-v2-verification/`.
No mutation control ran. Full daemon behavior remains open. Spawn prepares the remaining nine meaningful cases together, without a build grant.
Recovery owns compiler slot `classifier-v2-20260921` for the corrected classifier, negative control, and exact restored controls.
Read-only formatting checks no longer require the compiler slot. Writers format owned drafts before source review and freeze.
The six-case worktree refusal proposal is held because existing managed reuse suppresses earlier rollback; durable authority needs full lifecycle review.
Recovery withdrew its unsafe-release claim after checking Core retention guards. Discarded retained-release results remain a separate suspected ownership leak.

Spawn freeze-v2 checkpoint `7d920f69` is integrated as `161030b8` after independent source and raw-evidence acceptance.
Formatting, fresh library test compilation, and 12 selected groups passed: 62 tests, zero failures. All 1,013 source hashes held.
Artifact SHA256: `4087d3011c6432b5d058f7261c7d9d397024a73b8aa9013a917b31fb4eaee545`. Warning comparison stayed at 83.
Evidence: `/Users/jasonconigliari/botster-evidence/s1-async-spawn-20260921-sess-001b/applied-freeze-v2-verification/`.
Receipt/reply/wrapper are compiled only, dormant, and behaviorally untested. Ordinary wake is live, but its daemon consumer remains absent.
The next source checkpoint adds 13 receipt/reply/wrapper behavioral cases. No new compiler slot is granted; the previous slot is released.
Integration contains additional checkpoints beyond the tested branch; no fresh whole-integration verification is claimed.
Recovery will separately correct successor-bearing receipt classification without changing schema or transitions.
Root selected preparation of coordinated protocol 9-to-10 changes, preserving exact matching and supported custom sockets.
Shared protocol application, other-repository edits, client rollout, installation, and deployment remain held.

Latest verification: freeze v1 stopped at formatting only. Root inspected the single whitespace correction and granted conditional freeze-v2 verification.
No compilation/test result is recorded yet. The writer must preserve failed logs and prove only that correction changed the manifest.
Recovery review 1 accepted the directory-lock premise conditionally, with target execution checks still required.
Storage must not report a post-rename committed write as a precommit failure. Apple directory durability and fresh-state NotFound handling need correction.
The inspection protocol proposal is held: version/generated client compatibility and exact registry reads remain unresolved.
Writer and reviewer must reconcile default-socket claims with the earlier custom-socket trace before removing endpoint discovery.
The next spawn checkpoint needs behavioral tests for receipt, reply, and conversion paths; current managed tests do not cover those paths.
Root rejected the reviewer's proposed exclusion for std blocking-receive Context storage. Worker-lifetime accounting remains required and open.
Only the previously approved private mlua reference-storage exclusion stands. A source comment cannot create another exclusion.

Current spawn gate: Root froze new source application after independent review identified excessive overlapping unverified surfaces.
Reviewer accepted exact freeze `d4f368e4` plus 1,013-file manifest in `03-frozen-baseline-verdict.md`.
Root granted spawn the sole compiler slot `freeze-v1-20260921`: formatting check, one offline locked library test build, and selected regressions.
Each test run must pass at least one test, fail none, and match its artifact inventory count. All source hashes must remain unchanged.
Stop on failure or drift; the grant does not permit source repair or another build. Recovery remains source-only during this slot.
This supersedes older no-slot statements below. No build result is available yet. The ordinary daemon consumer remains absent.
The writer must report exact HEAD, all tracked/untracked changes, safety import status, source hashes, and a bounded verification plan.
The reviewer must examine that exact combined baseline before Root grants the single compiler slot.
The applied wake changes belong in this freeze; do not describe it as replay plus receipt/reply only.
Unapplied counting, timeline, cleanup, client-context, and ingress proposals are parked. Resume one connected surface after baseline verification.
This gate supersedes earlier permissions to apply independent spawn changes. Preserve all existing drafts; no rollback or deletion is requested.
Independent reviewer accepted the original replay-only evidence: 29 tests passed, artifact/source hashes matched, and warning counts matched.
Verdict: `/Users/jasonconigliari/botster-evidence/hub-async-spawn-review-20260921-fable/01-frozen-replay-evidence-verdict.md`.
That evidence does not verify the current combined source. Root's sequencing mistake allowed source drafts to accumulate before combined compilation.
Replacement rule: review and verify one frozen connected baseline before applying another overlapping surface.

Jason approved one writer per state directory. While the daemon owns the directory, `run-one` refuses immediately with a clear ownership error.
`inspect` reads through the daemon. Without the daemon, standalone persistence and fresh-state creation remain unchanged.
The recovery pair owns the implementation and independent review. The pair must identify additional CLI file ownership before edits.
Verification must cover every writer, socket configurations, path aliases, lock lifetime, daemon failure, and stale snapshot prevention.
This decision does not authorize live migration, installation, or runtime replacement.

Jason selected per-plugin memory budgets. Standalone per-plugin accounts are approved in principle with existing limits.
Do not assume a lifecycle instance contains exactly one plugin. Trace plugin identity and account lifetime before wiring.
Whether Hub-hosted plugins also replace the shared Hub account remains an unanswered clarification. Do not change that policy yet.

The live terminal audit found all eight sessions at prompts; session activity labels did not establish useful work.
The spawn reviewer had repeated API safeguard errors. Root assigned the available Hub Fable reviewer to the pending independent review.
The spawn writer must correct the unescaped-string excess reservation while preserving frozen replay source and evidence.
The next acceptance target remains real daemon spawn, conversion acknowledgement, exact-generation cleanup, and a responsive sibling request.
The recovery writer acknowledged and traced actual CLI/store callers. Root assigned the relevant `src/main.rs` command regions.
Shared hooks in `runtime.rs`, `daemon.rs`, and `host_mutations.rs` require an explicit handoff before edits.
The spawn writer resumed source work. Both assigned reviewers currently show provider usage limits with automatic continuation at 5:50 PM Pacific.
Review is pending, not active or accepted. Writers may prepare source and ownership traces; builds and integration remain gated.
Recovery found path-only store authority in HostPrepare/HostPackageRestore, including rollback.
The separate handoff proposal must cover producers in `daemon/control/host_work.rs` and `managed_git.rs`.
Current saved transports and daemon status do not prove which daemon owns the requested state directory.
Recovery must propose the smallest live-owner validation contract before adding endpoint metadata or a generation mechanism.
Review must cover stale metadata, crash/restart, response validation, path aliases, and unavailable owners. No second-writer fallback is permitted.
Spawn is checking existing identity contracts for reuse. Shared file edits remain held for an explicit handoff.
Spawn found no reusable directory-owner proof in the reviewed status, installation, or endpoint interfaces.
Recovery identified a storage premise gap: path-based writes can diverge from a retained directory descriptor after replacement.
Root authorized a separate dependency proposal using already-locked `rustix` 1.1.4 for safe descriptor-relative operations.
No manifest application, version update, or portability/safety acceptance follows. Review must cover the complete persistence lifecycle.
Directory locking is advisory. It cannot exclude legacy writers that do not acquire the lock.
Recovery must state that compatibility boundary and propose rollout prerequisites; no process termination, permission change, or migration is authorized.
Known endpoint rejection cannot prove absence of every legacy writer. This remains a release prerequisite, not a block on source preparation.
Spawn traced the ordinary Lua queue to the synchronous consumer without an ordinary daemon owner wake.
The writer must prepare one connected daemon continuation and reuse existing notifications where suitable, with exact-generation cleanup.
Spawn's allocation timeline report identifies lost event order across existing visitors and the decoder adapter.
Root requested independent review of that replacement boundary before dependent edits. Counting/typed semantic divergence remains unresolved.
A sound conservative bound can suffice; exact minimum peak is not a new requirement. Additional capacity refusals still require acceptance.
Receipt/reply patches are applied but dormant and uncommitted. Their combined review and verification remain pending.
Recovery prepared separate contract, dependency, and protocol proposals plus an unregistered descriptor-relative storage draft.
Its five test definitions have not run. Shared-file and manifest application remain held for storage premise review.
No compiler slot is granted. Root retains integration and publication ownership. Completed Core and safety implementation work stays stopped.

### Delivery scope

Deliver usable Workspaces rendering, asynchronous coordination and spawn, responsive siblings, and charged ownership through shutdown.
Core owns generic execution. Hub owns admission, correlation, supervision, recovery policy, and its persistence document.
Host workers perform filesystem and repository work. Lua composes product behavior. Web and TUI own client behavior and presentation.
Preserve no owner waits, no polling for progress, exact operation identity, existing limits, and retained context reads.
Do not remove synchronous public callers until their supported replacement is verified.
Do not change retention, capacity, exceptional-stop policy, or execution isolation without the applicable decision.

The primary implementation milestone is a real daemon Lua tool spawning two explicit IDs while a sibling request remains responsive.
Verify command, environment, working directory, context reads, conversion acknowledgement, and abandonment at each effect boundary.
Do not substitute synchronous helper pumping for this production path.
Durable recovery proceeds in parallel on independent files. Recovery policy gates final integration, not all spawn implementation.
Safety repairs continue on a separate track. A demonstrated unsafe boundary blocks only the code that crosses it.
Lint cleanup is not the primary work. Full accounting, client behavior, integration, and performance requirements remain in scope.

### Verified checkpoints

Dormant adapter `c2cfb2b4` is integrated as `cd755e3b`. Fable verified compilation, 26 passing tests, and copied artifact provenance.
Only six files had explicit per-command hashes. Per-command Git status also showed the three omitted committed parser modules unchanged.
The capture limitation is recorded; no concrete discrepancy requires a rerun. Future runs must validate the complete requested first-party hash set.
RawValue capture was withdrawn: capture can allocate workspace and fail before returning a malformed value's span.
The replacement adapter advances a token cursor through existing schema seeds. It is not a public parser or an active materialization permit.
Numeric/ignored failure parity, counting-pass funding, one combined allocation timeline, error/output ownership, and production wiring remain open.
The next source work connects those obligations at the repository materialization caller. No compiler slot is assigned.

Raw-Content prefix checkpoint `79cbb51c` is integrated as `df63bce9`; Fable verified 15 passing tests and exact artifact/source provenance.
Scratch-model checkpoint `7b3786a6` is integrated as `63c11d02`; Fable verified 20 passing tests, including the reserve(4) counterexample.
These are dormant terms, not complete materialization accounting. Scanner completeness, tagged/outer failure retention, and total capacity remain open.
The next implementation connects the actual repository decoder caller to one charged materialization permit. Delivery drafts remain separate.
Ignored nesting is not bounded by the parser recursion limit. Fund actual stack growth without imposing a new nesting limit.
Separate deserializer passes do not coexist; use their maximum workspace requirement rather than summing them.
No compiler slot is assigned. The latest integrated combination still needs a combined verification run.

Dormant schema checkpoint `37ed78ff` is integrated as `708cf0b1`. Fable verified the four-file scope, artifact provenance, and 13 passing tests.
Formatting and library/test compilation passed. Matching warning sets are unchanged apart from an existing diagnostic's shifted line.
This establishes dormant schema traversal only. Production wiring, scratch, failure-prefix retention, and the combined live maximum remain open.
The earlier schema patch accidentally removed production code during restoration from truncated output. Review rejected it before any build.
Committed source stayed intact. The writer restored the full file and verified its exact registration-only diff; Fable reviewed every replacement hunk.
Future restores must use complete source and an exact diff-scope check. Rejected evidence remains preserved.
Failure-prefix work is a separate source-review draft. No compiler slot is currently assigned.

Counter repair `d5dca026` is committed after Fable verified the combined build, two exact checks, and the 12-test state-owner group.
This closes arithmetic underflow. No old-arithmetic run executed; that comparison remains source reasoning.
Recovery lint follow-up `25fd8145` is integrated as `ddaaee34`. Thirteen recovery tests passed; strict lint reports 131 errors, not a clean pass.
Parser probe `31466c16` is integrated as `bad5eb2d`. Fable verified all eight fixtures and source/artifact provenance.
The latest integration includes these two additional checkpoints but has not yet been rebuilt after their integration.
Dormant parser checkpoint `2ed7fb15` is integrated as `f951fbd4`. Fable verified formatting, library check, test compilation, and nine tests.
The four-file checkpoint covers Content arithmetic/traversal, tagged-field counting, and charged Hub error formatting. None is a production caller yet.
The test build emitted 83 warning records representing 59 unique diagnostics, unchanged from its matched baseline apart from one shifted line.
The newer outer-schema draft was excluded from this build. Full parser accounting and capacity behavior remain unproved.
Spawn released the compiler slot. No compiler slot is currently assigned.

Current implementation: full-definition parser accounting and separate spawn receipt/reply/Host integration remain in progress.
The dormant parser terms have bounded test evidence. Full traversal, scratch, combined output/error overlap, and disposal funding remain open.
Root selected the existing complete decoder with conservative source-derived error funding, not a second error parser.
Additional capacity refusals require concrete accounting evidence and an explicit acceptance decision.
Worker resource helpers `9885c8e4` are reviewed, pushed, and integrated as `871c061a`. Five tests and restored controls passed.
Strict lint reports 135 errors on that helper branch, including four new unused-helper diagnostics. Full integration lint is unmeasured.
Allocation sizing remains source-derived; an independent oracle and actual worker-join lifetime checks remain open.
Jason selected per-plugin budgets. Constructor/load wiring must establish plugin-to-account ownership before implementation.
Standalone per-plugin accounts may use existing limits. The Hub-hosted account policy remains unresolved.

Iterator repair `6921e27d` is committed after Fable verified 28 passing tests on the combined spawn/safety source.
The former Error-key crash now returns the same typed error as the value path. General execution isolation remains open.
Recovery `bc0d5940` is pushed on its delivery branch and integrated as `c055fa31` after Fable's final verdict.
Both isolated lifecycle tests passed with producer-built, hash-verified Hub and Core worker artifacts. The earlier setup failures remain preserved.
The counter run built combined spawn/safety/recovery source. Later integrated lint, probe, and worker-helper changes still need a combined build.
No installation or live schema migration is authorized.

Spawn checkpoint `a223937e` is integrated as `32e16744`. Fable accepted its four-file scope and eight selected tests.
The matched oracle establishes the inflight slot change from 584 to 600 bytes with matching charges at capacities 1, 2, 4, and 8.
Four Lua sizing rows vary within unchanged binaries. Their cause remains open; exclude them from the matched comparison.
This checkpoint does not establish daemon behavior, full S1, or strict lint. Integration preserved both uncommitted safety files byte-for-byte.
Fable verified 13 recovery tests, 16 persistence tests, and 5 heap tests passed on freeze 2.
All three ablations reached intended failures. Restored controls passed, eight source hashes matched, and the restored binary matched the original candidate.
Strict lint failed with 133 library errors. No comparison against the historical 114 count is established.
The recovery commit remains an isolated verification checkpoint, not final acceptance.
Two initial lifecycle checks failed during setup because candidate binary variables were missing. Their later configured runs passed.
The corrected conversion receipt has source approval only. Its helper has no production caller; the patch remains separate and unapplied.
Root approved a spawn-specific funded reply pair, not another pending queue. Response strings, errors, and worker wait-storage ownership remain accounting gates.

| Checkpoint | Status and evidence | Limits |
| --- | --- | --- |
| Core `053148f6` | Merged and pushed to Core main. Accepted D1(a) source stands. Seven saved binaries independently match their manifest. | Historical 33-run acceptance survives in the plan; original logs are missing. The manifest alone does not bind the binaries to a source revision. |
| Hub `27c9b089` | Pushed. PumpState source review accepted. Formatting, library check, and test compilation passed. Strict lint exited 101 with 114 library errors. | No behavioral test ran. Library-test lint remains unproved. Evidence: `/Users/jasonconigliari/botster-evidence/pump-default-20260921-sess-0017`. |
| Hub `9cc101cb` | Pushed. Spawn-family cleanup-wake repair accepted. Three exact tests passed; restoring only the old acceptance rule made all three fail at intended readiness assertions. Restored source passed again. | Establishes the tested lost-wake repair, not full asynchronous spawn or inevitable permanent noncompletion. No new lint run. Evidence: `/Users/jasonconigliari/botster-evidence/spawn-phase-gap-20260921-sess-0017`. |

### Active work and ownership

| Track | Owner and source boundary | Next deliverable and decisive check | Status |
| --- | --- | --- | --- |
| Lua safety | Existing Hub Codex/Fable pair. | Completed iterator and counter checkpoints; handoff before overlapping edits. | Both fixes are verified, committed, and pushed. General isolation remains open. |
| S1/S2 spawn and conversion | Codex/Fable pair on `delivery/async-spawn-20260921`. Owns runtime and ordinary spawn integration. | Compile the reviewed parser terms; complete full accounting and the real daemon lifecycle. | Extraction, receipt factory, and parser probe are verified. Receipt/reply and Host integration remain separate source work. No full S1 acceptance. |
| R1 recovery and worker accounting | Codex/Fable pair on `delivery/durable-recovery-20260921`. Owns assigned recovery, persistence, lifecycle, and layout files. | Resolve approved policy gates, wire production ownership, and verify allocation sizes and actual joins. | Isolated recovery, lint follow-up, and worker helpers are verified checkpoints. Runtime recovery and worker load wiring remain incomplete. |
| Core support | Existing Core pair. Read-only reports in its persistent worktree. | No new Core prerequisite found. Reuse the reservation API; do not repeat D1(a). | Assigned source reviews complete. No compiler ownership. |

The earlier safety/spawn scheduling uncertainty is preserved in their evidence. Both runs completed; no timing or exclusive-resource claim follows.
The dormant-parser verification is complete. Root must grant the next build explicitly.
Root grants later slots explicitly. A reserved slot is not evidence of an active process or completed check.
Only one Botster compiler runs at a time: Rust 1.97.0, two build jobs, incremental compilation disabled.
Spawn and recovery use separate persistent worktrees based on the reviewed integration source.
Separate worktrees do not permit conflicting ownership. Root approves shared-file patches and merges them in dependency order.
Recovery must request integration hooks from the spawn owner; it must not independently rewrite `runtime.rs` or the daemon owner loop.
The spawn pair must wait for the safety handoff before editing the owned JSON walker or hook regions.
That boundary does not block independent daemon consumer, receipt, or recovery implementation.

Root assigned the crate-private local receipt factory in `data_plane/driver.rs` to the spawn pair, subject to its exact lifecycle review.
Root assigned minimal recovery fields/defaults in `persistence.rs` and module registration in `lib.rs` to the recovery pair.
The recovery pair also owns the exact recovery-field accounting changes in `hub_state_heap.rs`.
Recovery must reuse the single existing Hub state document/store and verify old-snapshot compatibility.
The existing serialization-before-charge boundary remains a G1 production-activation blocker, not permission to create another store.
Recovery worker helpers are integrated without constructor/load wiring. Hub account scope remains pending; no uncharged fallback is approved.

Current feature sessions:

- Spawn writer: `sess-1790026283-001b-831bec162af5cabf5a3b42375a529a08`; reviewer: `sess-1790026295-001c-058661451ae4bf5cf9e371481df5be04`.
- Recovery writer: `sess-1790026323-001d-e6d217aea97a81912f0f72631d9dcee9`; reviewer: `sess-1790026335-001e-2200a15351f039615bc70ba8075e41fd`.
- Safety writer: `sess-1790019339-0017-0db7d72fffa856605f6fbe4bf77f9236`; reviewer: `sess-1790019351-0018-71bfe4dbd98d67e5a198db16765a1660`.

Spawn worktree: `/Users/jasonconigliari/botster-sessions/botster-hub-async-spawn-20260921`.
Recovery worktree: `/Users/jasonconigliari/botster-sessions/botster-hub-durable-recovery-20260921`.
Both branches start at documentation checkpoint `8438b4ae`, whose source is `9cc101cb`.

### Current decisions and blockers

- S1 input uses staged admission, not a full 8 MiB reservation across the asynchronous wait. Whole-allowance prepayment remains unapproved.
- Original input charges must move with queued requests and survive caller timeout. Queue, materialization, context, and result charges remain separate obligations.
- Lua handle conversion can occur before a Rust callback body. The entry allowance and reentrancy proof remain open; body-only charging does not close them.
- Repository loading must preserve full definitions and environment data. The existing 4 MiB file ceiling is not permission to narrow accepted configurations.
- The catalog's file-plus-scratch 16 MiB formula is conservative, not a proved minimum. A smaller full-definition bound remains unverified.
- Independent source review disproved the proposed `3*max(decoded_string,nesting,8)` bound: serde's Unicode reserve request can exceed decoded output length. Tagged Content, malformed input, duplicate entries, and variable diagnostics also need funding. Root selected isolated pinned-parser allocation probes before a replacement formula; no production parser change or compatibility reduction is approved.
- Root assigned the parser probe to the spawn writer. Reuse the existing `core_ticket_allocations` recorder; add no allocator or unsafe code. Include truncated Unicode, duplicate environment keys, and tagged sequence cases. Fable reviews source and raw evidence. Preserve the frozen lifecycle source during its matched comparison. Fixture results cannot establish a universal bound.
- Exact-generation context ownership must preserve retained reads after session removal. Bare session IDs do not authorize replacement-generation cleanup.
- No new Core interface is required by the reviewed S1 trace. Accepted-ID retention is a separate fault-contract candidate, not a proved normal reservation leak.
- R1 operator resolution, retention, recovery capacity, state-clone/serialization capacity, and exceptional-stop policy remain unselected. No silent eviction or new numeric limit is authorized.
- Recovery schema compatibility is an integration gate. Verify that old readers/writers reject incompatible records; a version bump alone does not prove protection. No live migration is authorized.
- Root authorized isolated schema-3-to-4 loader/normalization implementation after Fable verifies old-source rejection. Test all load/save paths, field preservation, unsupported-version refusal, and unchanged original bytes after failed writes. Production migration remains unauthorized.
- Proposed durable IDs use a persisted host-scoped sequence, not runtime-local WaiterId. Overflow, restore, concurrent writes, and non-reuse require review.
- The instruction counter has a source-confirmed wraparound path after caught exhaustion errors. Saturation repairs arithmetic, not general catchable-error or native-stall containment.
- Fable accepted the isolated Error-key reproduction. The baseline build passed; both the minimal iterator and production walker modes exited with SIGABRT. Artifact provenance and controls were verified. This proves the pinned mechanism, not its frequency in real plugins. Root authorized the reviewed source-only `Table::for_each` repair. The repaired walker still needs successful execution and error-parity checks. Do not run a crash candidate inside the user's active runtime.
- Spawn reports two lifecycle tests and six local receipt tests passed. Matched baseline/candidate allocation runs also passed. Pending slots remain 328 bytes; inflight slots grow from 584 to 600 bytes. Reported charges match allocations at capacities 1, 2, 4, and 8. The writer verified restoration of all 1004 candidate file hashes before building. Fable still must review raw evidence; full asynchronous spawn acceptance remains open.
- Spawn stage admission must charge request/channel and phase-registration storage; waiter identity alone is not an accounting proof. The spawn pair owns this bounded interface repair.
- Spawn premise review accepted independent stage-machine extraction, ingress/owner integration, charged materialization interface, and receipt lifecycle. Implementation may proceed without a new planning gate.
- Root rejected an uncharged repository-parser fallback for production activation. Isolated accounted fixtures may verify lifecycle components, but do not close full configured-target S1 acceptance.
- Recovery premise review accepted isolated schema/store implementation. Last-writer-wins persistence and restored-snapshot sequence reuse remain explicit identity/durability gates until actual supported writer exclusion is verified.
- Recovery review found `inspect` and `run-one` can load/save state without the daemon's socket-owner lock. Concurrent saves can lose newer records and sequence advances. Root accepted this as an R1 activation blocker. The repair must cover one persisted document across socket configurations and preserve supported CLI behavior.
- The proposed read-only `inspect`/`run-one` startup mode is not approved. It changes persistent behavior, including fresh-state creation. A startup proof token alone does not establish exclusion for every later store write. Keep that policy/API decision separate from isolated recovery implementation.
- Jason approved immediate concurrent `run-one` refusal and daemon-backed `inspect`, with standalone persistence unchanged. Implementation and ownership verification remain open.
- Inconsistent-ledger startup refusal needs an explicit diagnostic/operator recovery contract before activation. Adjacent lock-file aliasing and unlink/recreation behavior also remain unproved; inode-keyed flock alone does not establish document identity.
- `client_api.rs` bare-ID context removal is a shared integration surface. Exact-generation context changes must cover that caller before acceptance.
- The claimed list/show wrapped-error producer was disproved: exported Lua wrappers validate arguments. No wrapper migration is approved on that premise.
- Shared Publish/Drain key allocation before reservation is recorded separately. The spawn-only input proposal does not repair those callers.

### Open acceptance requirements

| Requirement | Current disposition |
| --- | --- |
| M1/M2/A1 accounting | Still open for complete callback, retained state, context, transport, worker, and disposal ownership. Only the specified private mlua `ref_free` storage is excluded. |
| C1 coordination | Earlier functional evidence exists. Complete accounting and matched current-artifact acceptance remain open. |
| S1/S2 lifecycle | Ordinary daemon consumer, conversion acknowledgement, exact-generation cleanup, managed/direct parity, and real daemon proof remain open. |
| R1 recovery | Durable records, restart ownership, event-driven drain, and approved recovery policy remain open. |
| I1 isolation | Existing finalizer-registration restriction is not general cancellation or native-stall containment. No new process boundary is approved. |
| Unresolved failures | Event-owner assertion, Host-permit accounting assertion, original request ID 7, and reused-worktree assertion remain unresolved. No fresh diagnosis follows from lint repairs. |
| Strict checks | Last measured library lint count is 114 at the PumpState checkpoint; no full strict pass or library-test lint pass is established. |
| V1/P1 delivery | Fresh-checkout dependency resolution, matched Web/TUI/plugin/daemon behavior, remaining input/reconnect cases, and optimized performance/presentation evidence remain open. Reuse valid historical evidence only at its recorded scope. |

### Evidence and recovery record

Design inputs are under `/Users/jasonconigliari/botster-evidence/s1-design-20260921-sess-0017`.
Recovery freeze 2 has patch SHA256 `dd7433801d53516bfe491ec90474d9b3dce605e83d6c5dfedbac5ee9e2dbd53a`.
Its source verdict is `/Users/jasonconigliari/botster-evidence/r1-recovery-review-20260921-sess-001e/implementation-review-3-freeze2.md`.
This verdict permits the evidence run. It does not establish compilation, test success, or production recovery.
Review findings are under `/Users/jasonconigliari/botster-evidence/hub-pump-default-review-20260921`.
Core source reports are under `/Users/jasonconigliari/botster-sessions/botster-core-recovery-20260921/evidence`.
The old temporary integration worktree is unusable. Temporary verification logs and the R1 revision-6 design/review are unavailable.
Do not use a historical file path as proof that its contents survived. Preserve new reports and raw logs outside temporary and build directories.
Current integration worktree: `/Users/jasonconigliari/botster-sessions/botster-hub-foundation-resume-20260921`.
Current integration branch: `integration/foundation-resume-20260921`.

Root must integrate reviewed work, update this ledger, and request exact policy decisions before their implementation is blocked.
Root must not substitute message forwarding or repeated premise reviews for a concrete implementation assignment.

## Active repair: plugin rendering on September 9

### September 14 isolated integration candidate

Root assembled the reviewed callback checkpoint `255665ee` with baseline assets `a4d8641a`, ownership tests `825e2a63`, and doctor timeouts `fdca8b47`.
The merge candidate is `393509954d6f0e757ccaf7a6185b7716e7bdd5f4`, before this documentation update.
Conflict resolution preserves the reviewed callback source and all three added ownership-matrix entries. Main and the writer worktrees remain unchanged.
This is a local candidate, not final acceptance, publication, or installation. Combined source review, asset verification, and bounded test gates remain required.
The strict lint baseline, original refusal-ID-7 failure, and labelled Host-permit flake remain explicit. No failing gate is waived.

Claude verified preservation against actual local main `9e1ba428`, including its finalizer guard and superseded acknowledgement charge holder.
Combined revision `7df1e11d` passed asset checks, memory build contracts, artifact generation, ten ownership tests, two handshake tests, and the unchanged incompatible-doctor fixture.
The workspace run stopped after 1,140 Hub library tests passed and two failed. Later workspace tests did not run.
The failures were the labelled Host-permit assertion and an unclassified event-owner `waiting_for_owner` assertion. Eight acknowledgement tests and seven sandbox tests passed.
Strict lint stopped at four reproduced client baseline errors. The original request-ID-7 failure did not recur and remains unresolved.
Formatting-only commit `bf850a4c` passed review and formatting checks. Focused handshake, collection-capacity, and coordination-lifecycle suites passed 2, 16, and 11 tests respectively.
The allocation oracle passed and matched historical `e026bfff` scenarios. This is not a same-revision before-and-after control.
Ten isolated event-owner runs passed on a recovered binary. Its original executed path survives, but no contemporaneous pre-rebuild hash exists.
These runs do not resolve the full-suite failure.
Evidence is in `/tmp/hub-7df1e11d-combined-verify`, `/tmp/hub-bf850a4c-fmt-suites`, and `/tmp/hub-7df1e11d-event-owner-10`.
Full acceptance, publication, and installation remain open. No failing gate is waived.

The bounded event-owner diagnostic phase ended inconclusive. Source supports wall-clock exhaustion before dispatch as a candidate, not a confirmed cause.
Test-only diagnostics remain in a separate worktree. The interrupted v3 run and invalid v4 setup run remain labelled and preserved.
A corrected sibling bundle removed fourteen missing-worker errors, but six WebRTC fixtures rejected the external worker path.
The target test passed without a diagnostic record. A separate missing-reused-worktree assertion remains unclassified; it is not accepted as another setup failure.
No further diagnostic run is authorized. Evidence remains under `/tmp/hub-event-owner-diag-run` and `/tmp/hub-event-owner-v4-sibling-bundle`.
Independent lint work may add four reviewed item-level allowances while preserving inline storage and the public counter method. No behavior change is authorized.

### September 15 restart and lint repair

Root confirmed that the integration tree survived at clean checkpoint `98af5558d352187fde81c4b78881c930ed35f7b0` before this documentation update.
The Hub writer and Core writer reported no active compiler process. Root did not restart accepted checks.
Core remains clean at `053148f6e8e63c3c38b2cbabd54a6e4e8211143c`.

The client strict lint check now passes for all targets. The accepted helper repair removed all thirteen introduced Hub library diagnostics.
The Hub library retains 157 baseline diagnostics. Baseline origin does not waive the strict lint requirement.
Hub library-test lint coverage remains unproved because the library fails first.
Normal, `test-internals`, and `allocation-oracle` library checks passed for the helper repair.
The repaired library test artifact passed 16 collection-capacity tests, 11 coordination-lifecycle tests, and 15 Lua JSON tests.
Both affected allocation oracle harnesses passed. These checks establish the helper repair, not full foundation acceptance.
Evidence remains in `/tmp/hub-helper-visibility-verify` and `/tmp/hub-clippy-matched-arms`.

The Hub writer owns the next repair: remove seven redundant borrow operators in `src/lua_runtime.rs` and `src/packages.rs`.
Root checked the argument types and callee signatures. Claude reviews the exact diff before Root authorizes verification.
The new strict lint run reported 150 Hub library errors and exited with 101. It reported no `needless_borrow` or `vec_init_then_push` diagnostics.
Formatting and the ordinary library check passed. The library test build passed, followed by five package-refresh tests with 1,137 filtered tests.
Root inspected the source diff and raw evidence in `/tmp/hub-needless-borrow-verify`. Root also confirmed the copied binary hashes.
The diff removes seven borrow operators and includes formatter reflow with one removed trailing comma. No other semantic change appears.
Claude accepted the diff and raw evidence in message `msg_plugin-w_1789456096_b40c96`.
Root authorized a local commit of the two source files only. The writer must exclude this plan update.
The strict lint requirement remains open. The next repair requires a separate reviewed proposal from the retained diagnostics.
The writer committed the repair as `7f3592a86eeb59984a6eb08ee2fa487559b8f894`, with parent `98af5558d352187fde81c4b78881c930ed35f7b0`.
Root verified the two changed source paths. Only this plan remained dirty after the commit.
The next proposal replaces four `usize` multiplication expressions in `src/session_types/bounded_catalog.rs` with equivalent `saturating_mul` calls.
Root checked the expressions and preserved admission checks. Claude must accept the proposal and exact diff before the writer runs bounded verification.
Claude accepted the exact arithmetic diff in message `msg_plugin-w_1789456499_b40d93` before verification.
The new strict lint run reported 146 Hub library errors and exited with 101. The three repaired lint categories remained absent.
Formatting, the ordinary library check, and the library test build passed. The copied binary listed and passed all eight catalog tests, with 1,134 filtered tests.
Root inspected the raw evidence and confirmed both binary hashes in `/tmp/hub-saturating-mul-verify`.
The repair preserves computed values, including overflow cases. It establishes no new overflow-safety guarantee.
Claude accepted the arithmetic evidence in message `msg_plugin-w_1789456722_cdfec1`.
Root authorized a local commit of `src/session_types/bounded_catalog.rs` only, excluding this plan.
Full acceptance and Hub library-test lint coverage remain open. The writer will propose the next repair from the retained 146 diagnostics.
The arithmetic repair is committed as `fb39340ad09c913594dad9102a83a13d1254e466`, with parent `7f3592a86eeb59984a6eb08ee2fa487559b8f894`.
Root verified that only the catalog source file entered the commit. Only this plan remained dirty.
The next proposal prefixes five unused binding names with underscores across five daemon files. Calls, parameter types, and drop scopes must remain unchanged.
Root found that the proposed test filters named production functions rather than tests. The writer must provide verified test selectors before authorization.
Claude raised a possible retirement defect behind the unused `daemon` parameter in `retire_route_owner`.
The claim cites historical adapter-binding behavior and is not verified against the current source.
Root paused all five edits and requested bounded, independent traces of the retirement consumer and adapter-binding mutation owner.
An empty match arm or unused parameter alone does not prove missing cleanup. No lifecycle change is authorized.
Claude withdrew the retirement defect claim in message `msg_plugin-w_1789457011_40d01c`; he held no failing evidence at the current revision.
His current trace identifies `AttachedStream::close_adapter` as the adapter-binding mutation owner, separate from the cited empty match arm.
Root selected the original five named-binding renames. Signatures, statements, calls, and binding scopes must remain unchanged.
This decision does not establish full retirement correctness. It rejects an unsupported blocker to a behavior-preserving repair.
The writer may prepare the exact diff for review. He must supply corrected test selectors before Root authorizes verification.
Claude accepted the five-underscore diff and four corrected test selectors in message `msg_plugin-w_1789457219_a02774`.
Root independently inspected the diff and authorized bounded verification: formatting, strict lint, library compilation, and the four selected tests.
The writer must confirm each full selector against the copied binary's inventory. The `host_work` rename has source and compilation coverage only.
The new strict lint run reported 141 Hub library errors and exited with 101. It reported no unused-variable diagnostics for the library.
Formatting, the library check, and the library test build passed. Each of the four exact test runs listed one test and passed one test.
Each run filtered 1,141 tests. Root inspected the raw results and confirmed both binary hashes in `/tmp/hub-unused-variables-verify`.
Claude accepted the five-rename evidence in message `msg_plugin-w_1789457478_82b881`.
Root authorized one local commit of the five source files, excluding this plan. The writer will propose the next group from the retained 141 diagnostics.
These results do not establish complete retirement correctness or Hub library-test lint coverage.
The five renames are committed as `c0635cf0f5dff460f3ab9f4ba9b00217a339f9c7`, with parent `fb39340ad09c913594dad9102a83a13d1254e466`.
Root verified the five source paths and remaining plan-only changes.
The next proposal removes redundant rest patterns from four unit variants in `request_must_finish`, in `src/daemon/control/pending.rs`.
Root checked all four enum declarations and the predicate. Fielded variants and classification must remain unchanged.
Claude must accept the premise and exact diff before bounded formatting, lint, and compilation checks. No behavioral test claim follows from those checks.
Claude accepted the exact unit-pattern diff in message `msg_plugin-w_1789457861_112578`. Root independently confirmed the four changes.
The strict lint run reported 137 Hub library errors and exited with 101. The four unit-pattern diagnostics were absent.
Formatting, the ordinary library check, and the library test build passed. No behavioral test ran for this equivalent pattern spelling.
Root inspected the commands, source identity, and raw lint result in `/tmp/hub-unit-pattern-verify`.
Claude accepted the unit-pattern evidence in message `msg_plugin-w_1789458074_a3191a`.
Root authorized a local commit of `src/daemon/control/pending.rs` only, excluding this plan, and requested the next reviewed proposal.
The strict lint requirement and full acceptance remain open. The confirmed library diagnostic count is 137.
The unit-pattern repair is committed as `71ef39c27b063d2ce2e58c6dd2174ee4a1da184e`, with parent `c0635cf0f5dff460f3ab9f4ba9b00217a339f9c7`.
Root verified its single source path and remaining plan-only changes.
The next proposal replaces three clones of the derived `Copy` type `LeaseIdentity` with copies in `package_event_router.rs` and `runtime.rs`.
Root confirmed the type and operands. The runtime helper is not gated by `cfg(test)` at its declaration; test usage is a separate claim.
Root corrected the test selector to include `package_event_router::tests::`. The writer must list and execute exactly one selected test.
Claude must accept the premise and diff before bounded verification. The runtime helper sites receive source and compilation coverage only.
Claude accepted the exact clone diff in message `msg_plugin-w_1789458442_dbe062`. Root independently inspected all three changes.
The strict lint run reported 134 Hub library errors and exited with 101. The three selected clone diagnostics were absent.
Formatting, the ordinary library check, and the library test build passed. The exact reader test listed one test and passed one, with 1,141 filtered tests.
Root inspected the raw results and confirmed both binary hashes in `/tmp/hub-clone-on-copy-verify`.
The runtime helper sites retain source and compilation coverage only.
Claude accepted the clone evidence in message `msg_plugin-w_1789458649_3ef386`.
Root authorized a local commit of the two source files only, excluding this plan, and requested the next proposal from the retained 134 diagnostics.
The clone repair is committed as `20a9443e5df34f379c84bd37fcec601438830e3b`, with parent `71ef39c27b063d2ce2e58c6dd2174ee4a1da184e`.
Root verified the two source paths and remaining plan-only changes.
The next proposal changes four suppression-helper arguments from mutable references to shared references in `src/daemon/control/sessions.rs`.
Both helpers already accept shared references. Root checked their signatures and call sites; helper bodies and call order must remain unchanged.
Claude must accept the premise and diff before bounded checks and one source-order regression test. That test does not prove runtime suppression behavior.
Claude accepted the exact borrow diff in message `msg_plugin-w_1789458931_28a61c`. Root independently inspected the four changes and preserved call order.
The first pair is in `handle_runtime`, not `handle_session_remove`. The second pair is in `handle_shutdown_session`.
The strict lint run reported 130 Hub library errors and exited with 101. The four unnecessary-mutable-borrow diagnostics were absent.
Formatting, the library check, and the library test build passed. The source-order test listed one test and passed one, with 1,141 filtered tests.
Root inspected raw evidence and confirmed both binary hashes in `/tmp/hub-unnecessary-mut-verify`.
The test guards source order only; it does not prove runtime suppression.
Claude accepted the borrow evidence in message `msg_plugin-w_1789459149_c50bdf`.
Root authorized a local commit of `src/daemon/control/sessions.rs` only, excluding this plan, and requested the next proposal from the retained 130 diagnostics.
The borrow repair is committed as `74a5e529f9cabefc3982b90a2c351d763ec59f52`, with parent `20a9443e5df34f379c84bd37fcec601438830e3b`.
Root verified the single source path and remaining plan-only changes.
The next proposal replaces three `let Some(...)` statements whose else branches return `None` with `?`, in three `Option`-returning functions.
Root checked the exact bodies and return types in `entities.rs`, `plugins.rs`, and `runtime.rs`.
Claude must accept the premise and diff before bounded checks and two exact plugin-routing tests. The entities and runtime sites retain source and compilation coverage only.
Claude accepted the exact `?` diff in message `msg_plugin-w_1789459558_206df7`. Root independently inspected the three rewrites.
The strict lint run reported 127 Hub library errors and exited with 101. One `question_mark` diagnostic remains in unchanged `host_family.rs`.
Formatting, the library check, and the library test build passed. Both exact plugin-routing tests listed and passed one test each, with 1,141 filtered tests per run.
Root inspected raw results and confirmed both binary hashes in `/tmp/hub-question-mark-verify`.
The two tests cover plugin routing only. The other two sites retain source and compilation coverage.
Claude accepted the `?` evidence in message `msg_plugin-w_1789459808_931e85`.
Root authorized a local commit of the three source files only, excluding this plan, and requested the next proposal from the retained 127 diagnostics.
The three `?` rewrites are committed as `6fb1126a5a98e4c0677f5c99f1ff206c46dcfe1d`, with parent `74a5e529f9cabefc3982b90a2c351d763ec59f52`.
Root verified the three source paths and remaining plan-only changes.
The next proposal removes two bare function-level `#[must_use]` attributes. Root identified both diagnostics as `clippy::double_must_use`.
Claude reviews the return types and ignored-result diagnostics, including the tuple returned by `EventPlaneReplaceError::into_parts`.
Root authorized a standalone four-case compiler probe before attribute edits. Evidence is retained in `/tmp/hub-double-must-use-probe`.
The pinned compiler warns for ignored direct and tuple-contained `Result` values without function attributes.
With the function attributes, it emits additional function warnings. Removal preserves the type warning; it does not replace that warning or preserve identical diagnostics.
Root inspected the probe sources, commands, diagnostics, and exact two-attribute diff. Claude reviews both before repository verification.
Claude accepted the attribute diff and probe in message `msg_plugin-w_1789460281_8eee19` before repository verification.
The strict lint run reported 125 Hub library errors and exited with 101. Both `double_must_use` diagnostics were absent.
Formatting, the library check, and the library test build passed. Both exact regression tests listed and passed one test each, with 1,141 filtered tests per run.
Root inspected raw results and confirmed both binary hashes in `/tmp/hub-double-must-use-verify`.
The standalone probe remains separate evidence for duplicate-warning removal.
Claude accepted the attribute evidence in message `msg_plugin-w_1789460514_66c7a0`.
Root authorized a local commit of the two source files only, excluding this plan, and requested the next proposal from the retained 125 diagnostics.
The attribute repair is committed as `a74e062819758e84472a431679e750499c285676`, with parent `6fb1126a5a98e4c0677f5c99f1ff206c46dcfe1d`.
Root verified the two source paths and remaining plan-only changes.
The next proposal removes two returned local bindings in `entities.rs` and `bounded_catalog.rs`.
Root inspected both sites. Claude reviews concrete types and temporary drop behavior under edition 2024 before edits.
The outer parser binding, error conversion, reservations, and releases must remain unchanged. The confirmed library diagnostic count remains 125.
Claude accepted the exact return-binding diff in message `msg_plugin-w_1789460873_966e1d`.
The ordinary library check ran first and passed. Formatting and the library test build also passed.
The strict lint run reported 123 Hub library errors and exited with 101. Both `let_and_return` diagnostics were absent.
Both exact regressions listed and passed one test each, with 1,141 filtered tests per run.
Root inspected raw evidence and confirmed both binary hashes in `/tmp/hub-let-and-return-verify`.
Compilation establishes borrow validity, not drop order. The named deserializer still drops at inner-block exit before error conversion and budget releases.
Its scratch vector deallocation remains observable to accounting. The absence of an explicit `Drop` implementation does not prove unobservable destruction.
Claude accepted the return-binding evidence in message `msg_plugin-w_1789461150_6e5523`; the parser-site conditional is closed at this scope.
Root authorized a local commit of the two source files only, excluding this plan, and requested the next proposal from the retained 123 diagnostics.
The return-binding repair is committed as `01f6014b626d42b30daa1af79e2a672ee57f4622`, with parent `a74e062819758e84472a431679e750499c285676`.
Root verified the two source paths and remaining plan-only changes.
The next proposal removes four unused import names and gates the `WebrtcTerminalAdmission` import under `cfg(test)`.
The `AdmissionState` import remains available in production. Claude reviews descendant visibility and conditional compilation before edits.
After premise and diff acceptance, bounded formatting, lint, library, and library-test compilation checks are authorized. No behavioral test claim follows.
Claude accepted the exact import diff in message `msg_plugin-w_1789461520_ae5d92`. Root independently inspected all five files.
The strict lint run reported 118 Hub library errors and exited with 101. The five unused-import diagnostics were absent.
Formatting, the ordinary library check, and the library test build passed, including the test-only `WebrtcTerminalAdmission` import.
Root inspected commands, source identity, and raw outputs in `/tmp/hub-unused-imports-verify`. No behavioral test ran for this group.
Claude accepted the import evidence in message `msg_plugin-w_1789461718_a0e96b`.
Root authorized a local commit of the five source files only, excluding this plan, and requested the next proposal from the retained 118 diagnostics.
The import repair is committed as `d172a2e9b9bb52275ec808387c5a34b0994468e4`, with parent `01f6014b626d42b30daa1af79e2a672ee57f4622`.
Root verified the five source paths and remaining plan-only changes.
The next proposal replaces a duplicate lookup and insert with the `BTreeMap` entry API in `PackageEntityFamilyState::admit_retained`.
Claude reviews entry ownership and accounting before edits. Occupied entries must remain unchanged while the duplicate input moves to `discarded`.
The vacant branch must preserve insertion, the high-water update, and resync rearming in that order. No performance improvement is claimed.
Claude initially cited a different `pending_by_seq` field. Root corrected the receiver before authorizing edits.
The actual field is `PackageEntityFamilyState::pending_by_seq: BTreeMap<u64, PackageEntityMutation>`, not the leased-mutation map.
Claude corrected the trace in message `msg_plugin-w_1789462043_452ea0` and withdrew unsupported allocation and performance claims.
Root authorized the exact entry rewrite and diff review. The library check must run first after diff acceptance; a borrow error stops work without restructuring.
If compilation passes, bounded lint and compilation checks plus the exact duplicate regression are authorized. The commit remains held for evidence review.
Claude accepted the exact entry diff in message `msg_plugin-w_1789462247_69c181`. Root independently inspected both branches.
The library check ran first and passed without restructuring. Formatting and the library test build also passed.
The strict lint run reported 117 Hub library errors and exited with 101. The `map_entry` diagnostic was absent.
The exact duplicate-entry test listed and passed one test. Root inspected raw evidence and confirmed binary hashes in `/tmp/hub-map-entry-verify`.
No allocation or performance improvement is claimed.
Claude accepted the entry evidence in message `msg_plugin-w_1789462511_db4e27`, including the test's assertion that the first pending body remains intact.
Root authorized a local commit of `src/package_entity_fanout.rs` only, excluding this plan, and requested the next proposal from the retained 117 diagnostics.
The entry repair is committed as `2985d4685eafb630c2ddf09dd4c1e98b4da9ae2b`, with parent `d172a2e9b9bb52275ec808387c5a34b0994468e4`.
Root verified its single source path and remaining plan-only changes.
The next proposal replaces one closure returning an empty string literal with an eager empty-string default in `SourceRef::eligible`.
Root inspected the expression. Claude must accept the premise and diff before bounded formatting, lint, and compilation checks.
No behavioral test coverage is claimed. Callers establish reachability only.
Claude accepted the exact empty-default diff in message `msg_plugin-w_1789462801_8c0478`. Root independently inspected the expression.
The strict lint run reported 116 Hub library errors and exited with 101. The unnecessary-lazy-evaluation diagnostic was absent.
Formatting, the ordinary library check, and the library test build passed. Root inspected raw evidence and source identity in `/tmp/hub-unwrap-or-verify`.
Claude accepted the empty-default evidence in message `msg_plugin-w_1789462997_00c2ac`.
Root authorized a local commit of `src/session_types/bounded_catalog.rs` only, excluding this plan, and requested the next proposal from the retained 116 diagnostics.
The empty-default repair is committed as `41680c145ff686a02c2ee4ef5f8c9be3e36c83e4`, with parent `2985d4685eafb630c2ddf09dd4c1e98b4da9ae2b`.
Root verified the single source path and remaining plan-only changes.
The next proposal passes `repo_rollback_bytes` directly to `map_or` instead of a forwarding closure in `host_mutations.rs`.
Root inspected the expression and callee signature. Claude must accept the premise and diff before bounded formatting, lint, and compilation checks.
Logical-byte accounting and capacity checks must remain unchanged. No behavioral test coverage is claimed.
Claude accepted the forwarding-closure diff in message `msg_plugin-w_1789463336_9428aa`. Root independently inspected it.
The strict lint run reported 115 Hub library errors and exited with 101. Formatting, the library check, and the library test build passed.
Root inspected raw evidence in `/tmp/hub-redundant-closure-verify`. No behavioral test ran. The source commit remains held for final evidence review.

### September 15 publication and writer handoff

Jason authorized committing and pushing reviewed checkpoints along the way, plus captures of important findings.
Root pushed accepted commit `41680c145ff686a02c2ee4ef5f8c9be3e36c83e4` to `origin/integration/callback-baselines-20260914` and verified the remote ref.
This is checkpoint publication, not full acceptance, a main-branch merge, a release, or installation.
Jason authorized replacing the Grok implementation writer with Codex/Astra when convenient. Claude remains the independent reviewer.
Grok finished the current verification and reported no active compiler. He will start no further repair group before the handoff.
Root captured important decisions and corrections in the vault inbox as `2026-09-15-botster-reviewed-checkpoints-and-compiler-evidence.md`.
The capture passed frontmatter and wiki-link validation. It is raw inbox material, not completed atomic-note processing.
The strict lint requirement and full acceptance remain open.
Root owns this plan update. The writer must preserve the separate diagnostic worktree.

The event-owner assertion, original request-ID-7 failure, Host-permit failure, and missing-reused-worktree assertion remain unresolved.
R1 revision 6 remains a design proposal. The G1 account for state cloning and serialization still needs an approved capacity.
The timer audit is complete. Root restored the historical I1 proposal from agent message `msg_plugin-w_1789455811_3114e6` after its file became unavailable.
The restored proposal retains its historical source baselines. It does not supersede Jason's later approval of the tested `__gc` registration restriction.
The process boundary remains unapproved. General cancellation and native-stall containment remain outside that tested restriction.
All original accounting, lifecycle, client, and performance requirements remain in scope. Publication and installation remain separate permission gates.

### Current execution: complete the plan

#### September 11 approved accounting exclusion

Jason approved excluding mlua's private retained Rust reference storage, `ExtraData.ref_free: Vec<c_int>`, from the memory-accounting guarantee.
Its actual capacity, baseline growth, and growth overlap are not exposed through mlua's public API.
Record this storage as excluded, not funded. Live reference allocations, conversion scratch, collection capacity, and Lua allocator storage remain in scope.
This approval does not authorize unsafe access, a dependency fork, or a new memory limit.
Jason separately approved shared capacity errors for hooks, Publish, and Drain, with raised errors and message text preserved but the `runtime error: ` prefix removed.
Hooks use pre-funded shared `ExternalError`. Publish and Drain raise a precreated Lua string through the existing trusted callback wrapper; their errors already lack that prefix.
Do not route Publish or Drain through `ExternalError`: the wrapper's current `to_string` branch would allocate again.
Initial Rust and Lua storage remain charged to their actual owners. Implementation and verification remain open.
The detailed capture is `/Users/jasonconigliari/knowledge/inbox/2026-09-11-botster-excludes-private-mlua-reference-storage.md`.

Jason requested implementation of all plan items after identifying repeated accounting checkpoints without requirement closure.
This section supersedes earlier per-format, per-build, and per-test freezes for the assignments below.
The existing limits and architectural constraints remain unchanged. Publication and installation still require separate authority.

| Work | Owner | Next deliverable |
| --- | --- | --- |
| M1/M2/C1 accounting | Existing Hub writer, with Claude review | Connected pre-allocation admission through daemon execution and final disposal, including state, frame, cache, error, transport, and worker storage. |
| D1(a) spawn prerequisite | Existing Core writer, with Claude review | Generic prelaunch reservation, reserved launch, definitive release, and exclusion across conflicting spawn and adoption paths. |
| S1/S2/R1 lifecycle | Hub writer after the shared interfaces | Ordinary and managed spawn with conversion acknowledgement, confirmed cleanup, and durable recovery ownership. Unselected recovery policies remain explicit gates. |
| Dependency integration and V1 | Root | One dependency identity, integrated source, and matched daemon and client evidence. |
| A1/P1 | Root | Remaining accounting repairs and optimized measurements against the existing acceptance criteria. |

Writers may implement, format, test, and fix within these contracts without separate approval for each routine step.
The Hub writer owns the current compiler window. Root coordinates any Core build to preserve one compiler and at most two Cargo jobs.
Review must evaluate the complete affected lifecycle. A passing helper test does not close a production requirement.
Root connected the local Hub candidate to reviewed Core `6a9320393b6407a33230816508c9a7e700bfaee6` across all workspace manifests.
The lockfile changes only six Core source identities. Local resolution does not prove publication or fresh remote resolution.
The process correction is to assign complete requirements and retain valid evidence, instead of stopping after each mechanical verification step.
The reviewer identified recursive Lua conversion as a separate depth and Rust-stack risk. Root requested a 128-container limit; Jason has not selected it yet.
Independent implementation continues. Instruction-hook error storage will use source-derived, VM-proportional funding under the approved shared Rust account.
Root approved moving event and handler table composition to trusted Lua, while preserving indexed-access behavior and protecting against global rebinding.
Generic `Value` conversion can clone boxed Lua errors before callback admission. Trusted wrappers or pre-funded state storage must cover that path.
The Core reservation implementation also covers built-in local-runtime clones. Its API must preserve external runtime compatibility and bounded reservation ownership.
The first callback build stopped at a stale fixture revision guard that Root missed during dependency integration.
Root updated the guard, fixture provenance constant, and exact test expectation to the selected Core pin. The Core fixture files are unchanged.
Trusted argument validation must preserve the Lua null sentinel. Lua reports both full userdata and light userdata as `userdata`.
The corrected build passed, followed by fifteen focused tests for callback errors, argument conversion, registration parity, root correlation, sandbox behavior, and load refusal.
Root read the raw outputs and differential registration tests, then verified output hashes, recorded before/after identities, and executable hashes.
Evidence is `/private/tmp/c1-lua-callback-error-tests-20260910-1`. This closes the tested behavior comparisons, not complete memory accounting.
Implementation continues through the remaining accounting segments without a new mechanical approval gate.
Final review requires integer and null type-name tests. Non-null userdata keeps Lua's `userdata` diagnostic name; no unsupported classification invariant is added.
Root rejected the reported fallback-clone allocation defect: pinned `String` and `ValueRef` clones share their existing Arc and reference index.
The review must distinguish reference creation from reference cloning. The separate reference-creation failure path remains in scope.

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
The policy/account slice subsequently passed independent source review, including the corrected refusal test and pinned formatting.
Its frozen source is `/private/tmp/c1-lua-policy-account-review-20260910-3`; the complete patch SHA256 is `a54c5ad32737cc70200a3e92a97f09191b4b8a1a0508f8e084e454b08858c8f9`.
The aggregate limits apply per `HubRuntime`, not per process. Public Host API construction now uses the runtime factory; the runtime error enum gains Config.
Root authorized one 300-second no-run build for library tests and the two affected external test targets. Test execution still requires artifact verification.
The first build failed in the new refusal test: `current_package_generation` returns `Result`, but the test called `is_some`.
Root and the reviewer missed that return type. The failed build is preserved in `/private/tmp/c1-lua-policy-account-build-20260910-1`.
The corrected setup must require `Ok(value)` with `value > 0`, because the lookup returns `Ok(0)` for an absent generation.
Root authorized only that assertion correction, pinned formatting, and one identical bounded rebuild. No test has run for this slice.
The reviewer then found that the fixture has no event contract or subscription, so its generation stays zero even during replacement.
Root paused any rebuild not yet started and required one real event fixture before accepting the generation-preservation test.
This corrects coverage of an existing acceptance requirement. It does not add a production mechanism or weaken the requirement to source-only evidence.
Build 2 had already started and passed for revision 5. No tests ran against its incomplete fixture.
Revision 7 installs a real event subscription and checks nonzero generation, unchanged generation and subscription after both refusals, and replacement on retry.
Root verified the six frozen source hashes and authorized one identical bounded build for revision 7. Test execution remains separately gated.
Build 3 passed in 51.994 seconds with no deadline expiry. Root and the reviewer independently verified 990 source inputs and four executable hashes.
The library test executable SHA256 is `18d5c251ffbd4d646e4bcad03c3fc74bfe493d0ede35671ec0e9832770cdda2d`.
The JSON stream reports 153 warning events and no errors. Evidence is `/private/tmp/c1-lua-policy-account-build-20260910-3`.
Root authorized eleven exact tests, once each: six new unit tests, four external configuration tests, and the existing external Lua reload test.
Each test has a 60-second deadline. Execution must stop at the first failure or identity change; these tests do not certify full callback accounting.
The six unit tests and four external configuration tests passed. Root inspected every raw result and verified unchanged before/after identities.
The external reload test failed before behavior because its required candidate-path environment was absent. That setup failure is preserved separately.
Root authorized one reload-only retry with the existing verified candidate fixture in `/private/tmp/botster-plugin-render-candidate.QqYTjY`.
The retry uses the current integration-test executable. It checks library reload behavior, not matched delivery of the current Hub binary.
The retry passed: one test passed, 47 were filtered, exit zero, no deadline expiry, and empty stderr.
Root read its raw output and verified unchanged source, executable, and fixture identities in `/private/tmp/c1-lua-policy-account-reload-tests-20260910-2`.
All eleven selected behaviors now have passing results. The earlier setup failure remains recorded; no broad suite or matched-delivery claim follows.
This closes the bounded policy/account wiring check. Callback-frame, worker, shared-storage, and full acknowledgement accounting remain open.
The next ownership proposal is `/private/tmp/c1-callback-frame-ownership-plan-20260910-1/plan.md`.
It places argument and returned-handle funding in the Lua state owner, admitted through shared Rust storage before Lua construction.
Core worker metadata remains a separate segment. Escaped Lua owners, construction failure, teardown, and the disjoint conversion allocation inventory require source review.
No state-storage byte value, nesting multiplier, hidden unsafe return hook, or live accounting connection is authorized by this proposal.
Independent review accepted the proposed owner and acknowledgement partition, but did not prove escaped-owner or teardown safety.
The Hub writer will trace those ownership edges. The reviewer will inventory argument, return, and re-entry handles across installed callback families.
The acknowledgement handle count alone cannot size the whole state. The inventory must distinguish fixed counts from payload-dependent counts.
Any later nesting formula must include the pinned overflow path recorded by Q4, not only its normal-depth limit.
The ownership trace found no production strong Lua escape. Temporary byte borrows and weak-handle upgrades remain enclosed by synchronous runtime use.
It found a constructor gap: mlua can panic during partial initialization before creating its owning RawLua Arc, while an ordinary earlier charge releases.
The trace is `/private/tmp/c1-lua-owner-edge-review-20260910-1/trace.md`. This is source-path evidence, not an executed failure.
The writer will specify a private owner active before construction, with release after confirmed cleanup and retention when cleanup completion is uncertain.
That contract must protect the existing VM charge as well as future Rust-state funding. Normal-path policy tests remain valid; unwind accounting is not closed.
The callback inventory found `events.emit` copies its Rust String argument before body admission. A fixed handle allowance cannot cover those plugin-selected bytes.
That argument path needs separate bounded admission with preserved coercion and error behavior; no change to it is authorized in the state-owner contract task.
Approximate re-entry handle counts are not sizing inputs. The reviewer will refine those counts and the outer Rust-to-Lua entry frames.
Instruction-hook error allocation remains an open, separate input. The inventory is `/private/tmp/claude-callback-review.OFb8pb/c1-f1-callback-frame-inventory.md`.
The concrete owner contract is `/private/tmp/c1-lua-state-owner-contract-20260910-1/contract.md`.
It arms a private charge guard before `Lua::new_with`, transfers Lua into that owner, and releases only after confirmed destruction.
Its VM-only slice can address the existing unwind gap without selecting Rust-state bytes. Future Rust funding must extend the same owner with a mandatory charge.
Root checked the pinned constructor's returned-error boundary. Independent contract review precedes source changes; F1 work remains preserved.
Independent review accepted the VM-only contract. Root authorized implementation in `src/lua_runtime.rs` and its direct sandbox fixtures, with focused test source only.
The suggested weak-upgrade check was withdrawn: a failed upgrade does not prove that another thread's destructor has completed.
The implementation must preserve the traced private ownership boundary. A future escaping strong Lua owner must reopen that proof.
Single-panic retention and double-panic process abort remain distinct. No formatting, build, or test execution is authorized for this new slice yet.
The VM-owner source and eight focused test bodies subsequently passed independent review. Pinned formatting changed layout only.
Root verified both changed files and five unchanged policy files in `/private/tmp/c1-lua-vm-owner-review-20260910-2`.
Root authorized one 300-second no-run build of the library and two external test targets, covering production and test constructor forms.
Test execution remains gated on artifact verification. The test hooks establish owner ordering, not cleanup of mlua's partial state or a real finalizer panic.
The VM-owner build passed in 65.410 seconds. Both constructor forms compiled; the build reported 153 warning events and no errors.
Root verified 990 source inputs and four executable hashes in `/private/tmp/c1-lua-vm-owner-build-20260910-1`.
Root authorized thirteen exact tests: eight owner tests, one sandbox test, three Lua completion tests, and the existing load/reload refusal test.
Each test runs once with a 60-second deadline and stops the sequence on failure or identity change.
All thirteen tests passed once each. Every process exited zero; no deadline fired.
Root read all raw outputs and verified their hashes, 990 live source inputs, and four live executable hashes.
The evidence is `/private/tmp/c1-lua-vm-owner-tests-20260910-1`. Five tests printed their expected injected panics without test failures.
This verifies the exercised VM charge lifecycle and retained regressions. Complete Rust-state and callback accounting remain open; nothing was published or installed.
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

## September 15 cleanup checkpoint

Jason requested integration of valid work, followed by deletion of unused agents and worktrees.
Root fast-forwarded Core main from `b9e989be` to `053148f6` and pushed main successfully.
The Core delivery branch is also pushed. The recorded D1(a) review accepted the committed source after 33 exact passing test runs.
This integration does not close Hub acceptance or authorize installation.

Root deleted five unused Botster sessions: Hub Grok B2, Core Grok B3, spawn Codex A9, spawn Claude AA, and timer audit B1.
Root removed the spawn-lifecycle, event-driven-timers, callback-accounting, and Core worker-reservations worktrees.
The three Hub worktrees had no unique commits outside the pushed integration history.
The callback worktree had an uncommitted diagnostic patch. Root preserved that patch but did not merge it.
Claude confirmed that no reviewer process required the callback worktree.

Preserved files are at `/Users/jasonconigliari/botster-evidence/botster-cleanup-20260915.onGSh6`.
They include seven hash-matched Core binaries, the original hash manifest, the callback diagnostic patch, and private configuration from both worktrees.
The directory has mode 700. Private configuration files have mode 600 and are not committed.
The callback patch hash is `7010858245cef8e3fa59bdada89d35a67a2e6a3199cba61ada4ce81258c0fdab`.
Historical spawn-plan and worker-metadata evidence directories were already empty. Cleanup did not recover those missing records.

Hub source checkpoint `e3cc644e` is accepted and pushed. Hub library strict lint still reports 115 errors and exits 101.
Library-test lint, the event-owner failure, Host-permit accounting, request ID 7, and the reused-worktree assertion remain open.
R1 remains a design proposal. G1 still needs a capacity decision.
The active integration worktree remains. Other historical worktrees require a separate inventory before deletion.
The next bounded source proposal is Astra's `PumpState` default derivation, with Claude as independent reviewer.

## September 21 crash recovery

Jason requested replacement Codex implementers and Fable reviewers after the Botster crash.
The former temporary integration directory no longer contains a usable Git worktree.
The PumpState verification directory is empty. Its interrupted checks are not accepted as current evidence.
Root restored Hub checkpoint `03eaf5c2` on `integration/foundation-resume-20260921`.
The persistent worktree is `/Users/jasonconigliari/botster-sessions/botster-hub-foundation-resume-20260921`.
Root restored Core checkpoint `053148f6` on `delivery/core-recovery-20260921`.
The persistent Core worktree is `/Users/jasonconigliari/botster-sessions/botster-core-recovery-20260921`.
Core main already contains this implementation. Root did not repeat or undo that integration.

The Hub Codex/Fable pair resumes the reviewed PumpState change and reconstructs missing verification evidence.
The Hub writer has the sole Botster compiler slot, with two build jobs and incremental compilation disabled.
The Core Codex/Fable pair first verifies saved artifact hashes and the reservation handoff from source.
The Core pair must identify a concrete Core-owned gap before proposing additional implementation.
Both pairs must keep evidence outside temporary storage and preserve all existing acceptance limits.
Root retains integration and publication ownership. This recovery does not authorize installation or runtime replacement.

## September 23 reviewed corrections and candidate verification

The Host diagnostic reproduced one outstanding permit at the original frame-completion boundary.
The entity-resync scan held that permit. Three additional owner turns retired the work in this run.
The final test correction checks the original frames first, then requires Host work to retire within two seconds.
It no longer requires global Host idleness at the frame-completion boundary.

The WebRTC correction preserves the typed usage-query error.
Only `ErrDataChannelClosed` enters the existing bounded close-event wait. Other query errors remain `UsageFailed`.
The correction preserves send-error precedence, conservative byte accounting, and the active frame.
The original real-peer assertion remains unchanged.
If the close event never arrives, the existing wait still returns `SendFailed`.

Fable accepted both source diffs and independently checked the raw focused evidence.
Seven exact focused tests passed, one invocation each. The build and all seven invocations exited zero.
This evidence does not establish repeat stability or full workspace acceptance.
The focused log SHA-256 is `5bf67961ffe2133abcb906b8e6797ae8559e3e216e2ecb6391e10392bb16e35c`.

Agent 001b committed and pushed both corrections on `delivery/durable-recovery-20260921`:

- `1be7eb62a91b6842609c75d77d7b54c14481fb18`: Host-permit test correction.
- `ebc60c12a33fb85bc5558c6b15afc14398914348`: WebRTC usage-query correction.

The fresh candidate build from clean tip `ebc60c12` exited zero.
Its manifest SHA-256 is `4986cd5c991b4c3a0b35e1b3c083880e409bba4fbd9d98bf932a67d17ec6871f`.
Agent 001b started one full workspace run with the fresh candidate in live session `88927`.
The raw log is `/Users/jasonconigliari/botster-evidence/managed-recovery-20260922/reviewed-ebc60c12-full-suite.log`.
The adjacent `reviewed-ebc60c12-full-suite-run.md` records the command and environment.
The full result is recorded in the September 23 current checkpoint below. No installation or runtime replacement occurred.

## Completion gate

### September 23 verification update

Recovery checkpoint `b3545c33` removes the unused cleanup suppression mechanism.
Its 17 distinct focused tests passed. This does not establish full acceptance.
The first workspace run passed 1,295 tests across completed binaries, then stopped in the lifecycle suite after a fixture hung.
That interrupted run exited 130. Later test binaries did not complete.

Checkpoint `00f5d8e3` corrects three incompatible-daemon fixtures to use the framed protocol and bounded waits.
The three focused fixture tests passed. Production source did not change in this checkpoint.
Both checkpoints are pushed on `delivery/durable-recovery-20260921`.

The next workspace run used `./test.sh --locked --offline -- --test-threads=2`.
The Hub library reported 1,200 passed and two failed. The run exited 101 before later test binaries ran.
The packaged candidate came from `b3545c33`; the test source came from `00f5d8e3`.
The raw log is `/Users/jasonconigliari/botster-evidence/managed-recovery-20260922/fixture-correction-full-suite-00f5d8e3.log`.
Its SHA-256 is `cc6a5b0fdcb545f76c5034a144ee223b191ee9fec8cb2d8d37a84f6bd9a9e6e6`.

- Agent 001b owns the Host-permit diagnostic for `prepared_snapshots_and_fanout_progress_when_host_capacity_is_full`.
  The assertion observed one outstanding permit instead of zero.
  Root reviewed the test-only diagnostic and authorized one build and one focused run.
  A zero initial count makes that diagnostic inconclusive. Production behavior remains unchanged.
- Agent 001e owns the read-only WebRTC analysis for `remote_closed_subscription_keeps_host_sibling_live`.
  The test observed `usage_failed` instead of `remote_close`.
  Source analysis identifies a possible channel-removal window between a successful send and the usage query.
  The failing run does not record that ordering. The original assertion remains required.
  The next action is a bounded typed-error proposal and a deterministic regression design.

Neither failure has a verified baseline attribution. Neither failure is classified as pre-existing or fixed.
Async-spawn integration, ordinary spawn materialization, recovery acceptance, and final production-path verification remain open.
No installation or runtime replacement is authorized.

This phase completes only when integration, required product behavior, architecture findings, and measured acceptance are resolved or explicitly returned for a user decision.
The final report separates completed work, evidence, known limits, and remaining product choices.
Neither a green test count nor clean source formatting is a substitute for this gate.

### September 23 current checkpoint: lifecycle failures and subscription retirement

This section supersedes the pending-run and active-diagnostic statements above. It does not change the completion gate.

The workspace run at `ebc60c12` exited 101. Completed binaries reported 1,204 library passes, 30 main passes, 10 ownership passes,
four external-options passes, 14 capability passes, and 35 client-API passes.
The lifecycle binary reported 135 passed, 183 failed, and one ignored. Later binaries did not run.
The raw log SHA-256 is `d7d9c5a0d22f793052400a3a0d6f56b13358e3306e9dfe24d3d0ae33b570a28c`.
Of the failures, 163 reported the same harness taint after worker identity capture failed.
These are not 163 independently established product defects. The other 20 failures remain grouped by behavior.

Two reviewed, test-only checkpoints are pushed on `delivery/durable-recovery-20260921`:

- `1e1b1876` validates cleanup identity against the exact candidate and attributes cleanup taint to the originating test.
  Four exact focused tests passed. This does not authorize cleanup of old diagnostic workers.
- `073500d0` corrects retained-authority fixtures and copies the matching recovery journal in the CLI fixture.
  Four exact focused tests passed. Production authority checks remain unchanged.

The evidence directory is `/Users/jasonconigliari/botster-evidence/managed-recovery-20260922`.
The records are `validated-candidate-harness-run.md` and `authority-fixture-run.md`.
No full workspace run has passed after these corrections.

Agent 001b finished the uncompiled G2 subscription-retirement diff in the recovery worktree.
Agent 001e owns independent source review. The final diff SHA-256 is
`3bccfbe4aa2302222e7e22bb3d59bdb278c0c314185626dc02cb88c88702fb96`.
The earlier hash was superseded after the sibling cleanup test retained the retired subscription ID.
Root sent the corrected hash to the reviewer. The writer must keep this diff unchanged during review.
The review checks provider-loss delivery, retained terminal intent, capacity notifications, disconnect accounting, and outstanding publications.
The sibling test uses one cleanup guard and two owner rows. It does not use a live Unix socket.
The initial G2 review exposed a worker-send race. Clearing a publication flag cannot revoke a worker that already passed its send check.
The correction retains terminal intent until the exact running Host job produces its completion receipt.
The owner rejects new delivery arms and uses the existing completion route to wake subscriber delivery.
Cancellation does not release a running job's retirement fence.

The first compile failed with E0502 in WebRTC signaling. No tests ran in that attempt.
The implementer moved the shared notification-handle clone before the mutable runtime borrow.
The retry compiled the library and lifecycle targets successfully. Nine exact library tests passed, one invocation each.
The reviewer checked the source, raw logs, and artifact hashes. This evidence does not establish repeat stability.
The tests cover a real Host submission and completion route, a malformed receipt, and a stopped-executor rejection.
Ready-state delivery and retry after a Full rejection remain untested.
The lifecycle target compiled but did not run. The live sibling test remains unverified.

Checkpoint `15e7fa0f08a0568945e799113f259ddc214d4c2b` contains the 12 reviewed files and is pushed on the recovery branch.
The staged diff SHA-256 was `daa40dfc968ee01903e6507e4cbd2b0d029cf4b6df65c206057f29095e4eafe1`.
The implementer reported a clean worktree after the commit and started a fresh candidate build.
The initial candidate passed the two corrected package tests but failed the same-connection sibling test.
The diagnostic confirmed that CreateSessionType succeeded before the sibling read timed out.
Source review identified a missing SubscriberDelivery notification after a committed session-type generation change.
Checkpoint `f1510e358455d171b69224b28f48e87e44453d34` adds that notification only when the generation changes.
The checkpoint is reviewed and pushed. It also corrects test setup that left startup notifications set.
Five isolated notification tests passed after the corrections. The two capacity tests clear all startup maintenance bits.
Earlier notification claims that relied on initial readiness are superseded by these isolated results.
The earlier single-turn failure's exact scheduling cause was not measured.

The fresh candidate at `g2-candidate-f1510e35-20260923` passed all three selected lifecycle tests:
provider reconnect, provider removal with a terminal Error, and sibling delivery on the same Unix connection.
Each invocation executed one test and exited zero. Root and the reviewer inspected the raw evidence.
The sibling result verifies new delivery after the neighboring subscription retires on that same connection.
These selected results do not establish repeat stability, a full-suite pass, or installation readiness.
The candidate manifest names Hub `f1510e35` and Core `053148f6`.
The candidate README records commands, artifact hashes, and raw log hashes.
The stale-provider resynchronization test failed on this candidate. The advanced-subscriber ordering test did not run.
A test-only diagnostic confirmed the initial snapshot at sequence zero and both publication responses.
The seed returned `accepted` with `last_accepted_seq=1`. The gap returned `resync_scheduled` with `high_water_seq=20`.
The unchanged 20-second condition then failed with `attempts=40, degraded=0`.
The exact invocation reported zero passed, one failed, and 321 filtered tests in 27.93 seconds. It exited 101.
The diagnostic diff SHA-256 is `5fd5c84485965b03e8d5afa867456e8c01e808270c605fb04b7f3d33131f4934`.
The test binary SHA-256 is `4768793fff734f6684c5db64916c59b692b884a5aaf6dba78a9c627ee73bd3e8`.
The candidate directory contains `pressure-discriminator.log`, with SHA-256
`7bae3e36ac447babc40a6b1c5b1f423f0e8630b5c278f4a52ba03cc03223b844`.
Root inspected the raw failure. The reviewer checked the log and the diagnostic response fields.
Agent 001b must trace retry-state transitions before proposing a correction. Agent 001e must review that trace independently.
The aggregate attempt counter does not establish which per-family reset path ran.
No additional wake, changed limit, or production correction is accepted from this result alone.

The subsequent source trace found that every below-floor snapshot called `rearm_resync`, including a resynchronization response.
That call reset the per-family attempt count. The aggregate counter continued to increase.
Checkpoint `10b139418dfbce07b253eace44c52a6d562e0d3d` preserves attempts for stale Resync snapshots and retains rearm for stale new Subscribe snapshots.
The correction preserves the generation guard and refreshes returned progress after rearm. It changes no timeout, limit, or wake mechanism.
The first compile failed on a missing mutable borrow. The mechanical correction compiled successfully.
Three exact library tests passed, one invocation each. The reviewer accepted the source and raw evidence at that scope.
The checkpoint contains only the three reviewed files and is pushed. The worktree was clean for the candidate build.
Root and the reviewer verified the fresh candidate manifest and binary hashes in `g2-candidate-10b13941-20260923`.
The pressure lifecycle test passed against this candidate: one passed, zero failed, 321 filtered, 18.18 seconds.
Its raw log SHA-256 is `e9c8f2ce8b2f403e1a21af5407cc5ec90c70fb36e22b6f7addd9862206ae0866`.
The advanced-subscriber lifecycle test then failed: zero passed, one failed, 321 filtered, 16.25 seconds.
Its raw log SHA-256 is `de996f555b40a1dcaa21196e2f0ba8d78c26b042614f1cf5bfde4ffae080f62d`.
The failure reports a 10-second entity-frame timeout at `common.rs:1024`, with `probe=unconfirmed`.
Root inspected both raw logs. This timeout does not identify which frame was missing or prove an ordering defect.
Agent 001b owns the bounded wait diagnosis. Agent 001e checks the runtime evidence independently.
The pressure result closes only this focused scenario. Full acceptance remains open.

The subscriber diagnostic then confirmed all setup responses and initial snapshots. The final wait received one additional stale snapshot.
It reported one resync attempt and zero degradations. Source review found that snapshot convergence could clear the subscriber retry need.
Moving MarkResync after draining alone would reset attempts and could create unbounded retries. The accepted correction preserves the existing cycle instead.
Checkpoint `8d9cc48e29df8fadfca5b77d45457b662928042d` retains a per-pass catch-up observation and preserves retry state across snapshot processing.
The existing delivery owner serializes target registration and traversal. The correction adds no queue, limit, or owner-side subscriber scan.
Exact target checks exclude terminating and replaced subscriptions. Real publish behavior remains unchanged.
The correction retains resync charges while needed and tests release on convergence and degradation.
The reviewed six-file diff compiled. Eleven exact library tests passed, one invocation each. The commit is pushed.
Root and the reviewer verified the clean candidate at `g2-candidate-8d9cc48e-20260923` against Hub `8d9cc48e` and Core `053148f6`.
Three exact lifecycle tests then passed, each with one passed, zero failed, and 322 filtered tests:

- Subscriber catch-up: 9.49 seconds; raw log SHA-256 `44e3b6dfd765d4f15b1382bf86a5d1b2207e3791892c3a822972c9862394e7a1`.
- Stagnant provider without a family gap: 15.61 seconds; SHA-256 `3eb9d432286f5db96c98e30bb4e19938ef9b27e0a7938f05ef5eec975c7ddbba`.
- Original pressure case: 15.13 seconds; SHA-256 `7dade79b79063f9023c516dd6e0242a2b669c6f260ed1bbdd290bf18a6f62354`.

Root inspected the raw result lines and recorded commands. The reviewer verified the runtime evidence.
The stagnant test checks eight new attempts, one new degradation, stable counters for three seconds, and no rollback or Error for A.
These focused passes do not establish repeat stability or full acceptance. Strict lint and the full suite remain open.
The same test binary failed against the prior `10b13941` candidate at the intended final catch-up assertion.
All setup checks passed. The final wait received one additional stale snapshot, with one attempt and zero degradations.
The negative-control log SHA-256 is `02662db744a4415d394dc101389c127aa5679c048c107f01879b7589925ba833`.
The reviewer verified this comparison and closed the candidate-isolation caveat.
The candidate build reported warnings; attribution remains unverified. No installation or running-daemon replacement occurred.
The old candidate must not supply runtime evidence for this production change.
The evidence packet is `g2-implementation-review.md` in the evidence directory above.
The retry compile log SHA-256 is `cb24e49cc131bfca7358e736c9b85e905f8bf03dad73502551d0829562b8ff60`.
The focused log SHA-256 is `57b2450b711ec3c28a11592eecc26ab01df810335697c2aa96def692b56275f0`.

The G3/G4 correction changes four test files only. It updates stale protocol, conformance, and CLI schema expectations.
It preserves feature refusal checks, the protocol 8 rejection fixture, and legacy schema 3 reader support.
The reviewed diff SHA-256 is `a68a014095da71a756656c711a6688e5f07aec7f6a90ee3f15bc260147ac84b2`.
Eight exact client tests and five exact lifecycle tests passed. Each invocation executed one test.
The lifecycle tests used the unchanged production candidate from `8d9cc48e`.
The reviewer verified all fifteen log hashes, both test binaries, and the current selectors.
Root read the evidence packet and checked the raw result lines. Root accepted this focused checkpoint.
The packet is `managed-recovery-20260922/g3-g4-test-only-fixture-run.md` under `/Users/jasonconigliari/botster-evidence`.
Its SHA-256 is `5b70f7fdf8c3507b2ad685063c116d088e39413f603b341a59a84168875a1bed`.
Agent 001b committed and pushed `abe4ceb6f3ca4604304a437a7546b82e1dd19d7e` with only the four reviewed test files.
The writer reported that HEAD equals the remote branch and the worktree is clean.
Root accepted the G5 premise after reading the response builder, continuation, and admission paths.
The response builder replaces package operation labels with `host_execution`. The configure failure log confirms that value.
The install and enable tests fail at their operation assertions, but their logs do not print the actual value.
Agent 001b now owns the bounded correction in `host_work.rs` and diagnostic test assertions.
The continuation retains the static label from the typed request before the request moves into the Host command.
Both admission calls and all `finish_error` calls must receive that label. Unclassified and non-package labels remain unchanged.
Restoration transport errors remain unchanged. `PreparedMutation`, submission errors, and commit-outcome mapping remain outside this correction.
Root and reviewer 001e accepted G5 diff `5e518a82934abaedfb72ba83ae7ec2c203a258392adea6da163fe0ef46a3a7bb` at HEAD `abe4ceb6`.
The two-file patch includes classifier checks, later-phase error checks, and retained admission checks for package and non-package requests.
The package admission test checks `configure` after the second poll, with the request ID and both diagnostic copies.
Root granted agent 001b the sole compiler slot for compilation and four serial, exact library tests.
The first build failed on three errors in new tests: two private constructor calls and one invalid mutable borrow.
The writer applied only mechanical test fixes. The accepted diff is now `7b705ab55224d73168ab742365094f8245bb199b8f53f0b4e7597343fd14b107`.
The retry build passed. All four exact library tests passed, each with one test executed and 1223 filtered tests.
Root checked the raw result lines. Reviewer 001e verified the unchanged diff, six log hashes, and both test binaries.
The retry log SHA-256 is `d75e86c55e04982601f91cf1661d5ce2eaabe0b4308060fccca8241b9f7ebde7`.
Evidence remains in `/Users/jasonconigliari/botster-evidence/managed-recovery-20260922/g5-operation-label`.
The reviewed correction is committed and pushed as `609dbb0e46b515e6a846c136ce39cb29a10159fa`.
Root and reviewer 001e verified the fresh matched candidate. The first lifecycle test passed its install-operation check.
It then failed the local-path exclusion assertion. The other two lifecycle selectors did not run.
A test-only diagnostic confirmed that the Host message included the manifest path through `PackageRegistryError.package_name`.
This is a newly reached acceptance failure. Its historical origin remains unverified.
The Host formatter bypassed the existing `package_error_display_name` policy in `daemon_projection.rs`.
Root and reviewer 001e accepted bounded reuse of that helper, with its refresh exception unchanged.
The three-file diff is `098a518feecf7669b42bc32325681e87099297e6af40a7de6d6ccfb3e9c1f099`.
Its library build passed. The new Host policy test and the existing projection test each executed one test and passed.
Root checked both result logs. Reviewer 001e verified the focused evidence.
The reviewed display correction is committed and pushed as `db3dc263e9ceac8b8747564b86eaa5ac1f4cae08`.
Root and reviewer 001e verified its clean candidate, manifest, and artifact hashes.
All three exact lifecycle selectors passed against that candidate, each executing one test with 322 filtered tests.
The local CLI test passed in 8.40 seconds. The plugin namespace test passed in 5.63 seconds.
The package contract matrix passed in 10.72 seconds. Root checked all three raw result logs and the recorded commands.
Evidence resides in `/Users/jasonconigliari/botster-evidence/managed-recovery-20260922/g5-candidate-db3dc263-20260923`.
Independent lifecycle evidence review is pending. These passes do not establish full acceptance.
Root released the compiler slot. Agent 001b next audits the G6 source guard without edits.
Reason-payload sanitization is outside this correction. The separate CLI Package Display site remains an unverified follow-up candidate.
The formatting check failed. Baseline attribution remains unverified; no strict formatting acceptance is claimed.
The build reported 45 warnings, including 30 duplicates. Attribution and strict lint remain open.

The remaining failure groups cover operation labels,
the terminal-path source guard, shutdown during update checks, chunk reassembly, and deterministic WebRTC failure evidence.
The selected WebRTC test design must prove an incomplete response and read the record through the real owner consumer.
CLI failure-format coverage remains open. The test design does not close that requirement.

Full delivery still requires async-spawn integration, ordinary spawn materialization, recovery verification, strict lint,
the full workspace suite, and verification with matched artifacts and real clients.
No installation, deployment, or running-daemon replacement occurred.
