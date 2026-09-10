# Foundation integration and final acceptance

Status: source publication complete on September 9, 2026. Full foundation acceptance remains open.

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
- Sol implements. Claude reviews independently.
- Each repository has one writer in its canonical worktree.
- Verify the actual spawn location before editing. A prompt does not set the working directory.
- Each phase ends with a scoped commit. Preserve unrelated changes and historical evidence.
- Use one compiler or runtime window, at most two Cargo jobs, and `CARGO_INCREMENTAL=0`.
- Keep validation proportional to changed behavior. Do not repeat live tests for documentation-only changes.
- Do not treat source-string guards, log sizes, or absent errors as proof of behavior.
- Record exact commands, exit status, revisions, dependencies, artifacts, and remaining limits.
- Remove completed agents after their handoff and evidence preservation.

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
