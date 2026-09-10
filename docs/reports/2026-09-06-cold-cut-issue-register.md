# Cold-cut issue register

## Scope and status rules

This register records findings reported during the Core, Hub, Web, and TUI cold cut. It is not a complete audit clearance.

The active implementation plan remains in the canonical Hub worktree:
`docs/plans/2026-09-06-north-star-cold-cut-implementation.md`.

Agent reports are evidence of reported work, not independent verification. Compilation does not prove runtime behavior. No replacement tests have run at this checkpoint.

An issue closes only after its fix commit, reviewer result, and relevant verification result are recorded. A prepared or compiled test is not a passing test.

## Confirmed Core findings

The reviewer inspected committed Core `a931125`, relative to `bf6e7d9`. Paths and line numbers below refer to `a931125` under `crates/`.

Core reports source fixes and regression tests for all nine findings. The fix commit, compiler results, and re-review remain pending.

| ID | Defect and required invariant | Source evidence | Owner |
| --- | --- | --- | --- |
| C1 | Queue overflow can discard an exit event or an unsent resync transition. Pressure must preserve terminal completion and a valid transition, or explicitly close the route. | `botster-core/src/engine/client_worker.rs:945`, terminal enqueue paths at 728 and 752 | Core |
| C2 | Completed input results release admission capacity before delivery and can accumulate beyond queue limits. Retained results and local rejection results must remain bounded. | `botster-core/src/engine/client_worker.rs:1688`, overflow at 971, completion at 1103 | Core |
| C3 | Concurrent readbacks drain and discard each other's replies. Each request must retain its correlated response until completion or cancellation. | `botster-core-daemon/src/daemon.rs:2252`, 2296; `botster-core/src/runtime/worker_process.rs:690`, 720 | Core |
| C4 | Cancelling Spawn abandons reconciliation while launch continues. Cleanup ownership and concurrency accounting must remain until the child is collected and stopped/reaped. | `botster-core-daemon/src/daemon.rs:1639`; `botster-core/src/runtime/worker_process.rs:744` | Core |
| C5 | Worker results and mode messages omit the header required by the parent decoder. Both ends must use one exact representation. | `botster-core-daemon/src/bin/botster-session-worker.rs:386` and `publish_modes_if_changed`; `botster-core/src/runtime/worker_process.rs:2501` | Core |
| C6 | Host captures fail a terminal-subscription ownership check. Host captures must use host-operation ownership and complete cancellation explicitly. | `botster-core/src/engine/botster.rs:1689`, 1602, 1880 | Core |
| C7 | Old capture pages can receive the new resync epoch. Each capture must retain its attachment and epoch identity through every page enqueue. | `botster-core/src/engine/botster.rs:149`, 1646, 1809; `botster-core/src/engine/client_worker.rs:783`, 925 | Core |
| C8 | Owner release removes completed captures but leaves pending captures. Disconnect must release pending, active, and completed work and reject late reopening. | `botster-core-daemon/src/daemon.rs:1722`, 1754, 2343 | Core |
| C9 | An object that cannot fit can evict valid history before rejection. Check unconditional refusal conditions before eviction. | `botster-core-daemon/src/daemon.rs:2576`, 2582, 2596; policy construction at 181 | Core |

## Contract and implementation issues

### Web findings through 92d6cf3

The reviewer inspected committed source through `92d6cf3`. References below use `ccc115a`. Web must verify and correct these findings on its newer fixture checkpoint `877e589`.

- W1: `src/botster/terminalInputCapture.ts:77` captures local search key and paste events. Restrict PTY input capture to the terminal input surface.
- W2: `src/botster/hubTerminalDataPlane.ts:262` allocates paste payloads before an unbounded waiter list. Waiting payloads escape the declared byte limit. Reserve capacity before waiting.
- W3: the same paste waiters survive attachment reset and can reorder with later keys. Use one ordered queue and cancel unsent operations from the lost attachment.
- W4: `src/botster/webrtcDaemonClient.ts:750` retains encrypted messages in an unbounded promise chain before decoding. Bound admission before asynchronous work.
- W5: crypto calls at 2832 and 2853 use an always-copying `toArrayBuffer` helper at 3071. Avoid unnecessary JavaScript copies while preserving correct view boundaries.

These are static findings. No Web runtime tests have run at this checkpoint.

### TUI focused runtime check at bf783ca

TUI selected 13 exact tests. Eight passed; the ninth failed; four were not run. Execution stopped at the first failure.

T1: `app::tests::input_results_are_correlated_by_operation_on_every_epoch` failed because resync hydration clears the local attached marker. Input-result handling then drops a valid result and strands its operation. The same marker also blocks sending frames released by the input window during resync.

Required correction: distinguish the trusted attachment identity from hydration state. Preserve input correlation and sending during resync, without admitting input for a lost or unconfirmed attachment. A focused fix and rerun are assigned.

TUI fixed T1 at `9c49973`. The implementer reports a clean workspace all-target check and 15 exact focused tests passed: the original 13 plus resync queue release and lost/replaced attachment regressions. Each test ran serially under a 60-second process alarm. No daemon, PTY, live harness, or full suite ran. Source re-review remains distinct from this focused test result.

### Hub findings at 2fe19aa

These are confirmed static-review findings. Hub has the corrections assigned. Neither has source clearance or runtime verification.

- H1: `src/data_plane/driver.rs` falls back from `try_send` to blocking `send` under the admission mutex. Queue saturation can block owner progress and stop coordination. Admission must remain nonblocking and bounded.
- H2: `sessions.rs:781` admits a deferred attach before recording connection ownership. Cleanup in `unix/connection.rs:713` can miss it. The continuation can then register through a dying mux in `unix/adapter.rs:246`. Track pending ownership at admission and reject stale completion before mutation; clean up late success by exact attachment identity.
- H3: `unix/connection.rs:769` checks the old attachment during Core cleanup but drops that identity from the result. The continuation at 825 can close a replacement by route key alone. Carry identity through completion and validate before owner-side mutation.
- H4: `listener.rs:64` unlinks the flock file before releasing its descriptor. Contenders can lock different inodes for the same pathname. Keep the lockfile persistent and release the descriptor without unlinking it.

These decisions are agreed. Their implementation still needs review and focused runtime verification.

| ID | Issue | Required end state |
| --- | --- | --- |
| A1 | Hub fixed the attachment generation while Core changed it on resync. | Keep attachment generation fixed. Carry a separate `stream_epoch` in routing metadata. |
| A2 | A proposed per-route epoch inside the shared body risked per-client copies. | Keep one immutable output body. Store route-specific metadata in a small queue descriptor. |
| A3 | A stale resync transition could restore old state. | Require matching `from_epoch`, different `to_epoch`, and matching envelope epoch. Fence the current attachment first. |
| A4 | Applying the display epoch filter to input results could strand operations. | Correlate results within the attachment regardless of display epoch. Complete each operation once. Do not replay input. |
| A5 | Clients disagreed about missing snapshot state. | Before READY, capture failure closes the route. After READY, incomplete history ends assembly without discarding the valid screen. |
| A6 | Input framing differed between Core and Hub proposals. | Use the same routing header in both directions. Input uses epoch zero, ignored for admission. |
| A7 | TUI restored separate entity sockets because an old Hub restriction remained. | Use one Unix connection with correlated subscription acknowledgements and entity delivery. |
| A8 | Parked pre-attach frames could create independent route budgets. | Charge retained frames to the aggregate connection limit of 256 items / 8 MiB. Release them on failure or replacement. |
| A9 | Snapshot APIs required a page copy from shared data. | Parse borrowed snapshot slices where the parser consumes them within the call. |
| A10 | Generated schemas and dependency pins described different source states. | Use exact committed artifacts and consistent Core-family pins. Generate canonical lockfiles through Cargo. |

Always-active worker Ghostty, including zero attached clients, remains a required invariant. The parent must not duplicate the server parser. These requirements are not yet runtime-verified.

## Verification and delivery issues

| ID | Issue | Control |
| --- | --- | --- |
| V1 | Production crates compiled while obsolete test sources still used deleted APIs. | Remove obsolete contract tests. Preserve and update useful invariant tests. Compile intended targets before running focused tests. |
| V2 | A cancellation trial could reach attached before cancellation. | Mark that trial inconclusive for pending-attach cancellation. Do not count it as proof of that invariant. |
| V3 | Paste accounting initially used an unverified byte formula. | Assert the exact count only for the specified successful outcome and encoder mode. Partial and unknown outcomes need separate assertions. |
| V4 | GHOSTSNP fixtures came from an older package. | Verify that their binary format remains current and record provenance. Do not infer validity from a wrapper-version change. |
| V5 | TUI's binary check temporarily excluded a dev dependency. | Label that check as limited. Restore the intended manifest and repeat the full target check. |
| V6 | Hand-edited lockfiles and unpublished revisions blocked resolution. | Validate Cargo-generated lockfiles against exact committed objects with canonical source identities. Do not publish merely to bypass local resolution. |
| V7 | Agents previously worked outside their assigned worktrees and triggered permission requests. | Use one canonical worktree and one writer per repository. Start replacement agents in that worktree. Preserve handoffs and clean phase commits. |
| V8 | Repeated messages and build-window handoffs can obscure actual progress. | Record exact commits, commands, active processes, results, and window release. Do not equate agent activity with completed work. |

## Current verification limits

- Core `a931125`: seven production crates compile, according to the implementer. The nine confirmed defects above prevent clearance.
- Hub `c64b3cc`: client and test-support targets pass locked compiler checks, according to the implementer. Main Hub transport work remains unfinished and unchecked.
- Web `92d6cf3`: type checks and lint pass, according to the implementer. Runtime checks and review completion remain pending.
- TUI: binary-only compilation passed before the final dependency updates. The intended full target check remains pending at `877f3ed`.
- Performance has not been measured for the replacement stack. No fastest-multiplexer claim is established.
- The Web reviewer has not yet delivered a final report. Hub and TUI still require consolidated review.

## Closure record

### Hub bounded source clearance at 4a22d71

The reviewer cleared the assigned Hub ownership paths on `4a22d716f7831fc227c0e15c276a13b056d103b6`. Corrections cover H1–H4, tracker retirement, cleanup permits, departing-owner identity, route reservation provenance, duplicate closes, and sibling attribution. Namespace admission and stored-owner checks also received source review.

The implementer reports a clean locked workspace all-target check. No Hub runtime tests have run for this candidate. This is bounded source clearance, not a complete audit or runtime acceptance.

For each issue, add the fix commit, reviewer decision, verification command and result, and remaining limits. Keep unresolved issues explicit. Do not remove a finding merely because the code changed.

### Re-review of bdd8993

The reviewer compared committed `bdd8993` with `a931125`. The reviewer did not run builds or tests.

- C1, C2, C3, C5, C6, and C9: source corrections confirmed for the reported paths. Runtime verification remains open.
- C4: still open. A launch can enter the receiver after its final empty check but before receiver destruction. The queued child then lacks cleanup ownership.
- C7: still open. A new attachment resets its capture fence. The capture request and page enqueue do not retain the complete attachment identity. Stale work can match the replacement fence.
- C8: still open. Cancellation removes capture work but retains cancellation IDs whose only removal path requires a result that will no longer arrive.

The implementer reports that production crates compile and Core library test targets compile. Daemon test compilation still fails on four obsolete API calls. Worker regression compilation is unconfirmed. No test ran.

Required regression improvements include actual worker producer helpers, concurrent readbacks, spawn destruction, attachment replacement, queued stale captures, host capture ownership, and owner release.

### Re-review of c941e00 with cleanup 67bc05d

- C4: source correction confirmed. A drop guard retains launch cleanup ownership through channel destruction and installation failure. Actual child termination and reaping remain untested.
- C7: still open. Stale pages are rejected, but a stale resync request can cancel a replacement capture before the request is validated. Validate before any destructive state change.
- C8: still open. Tombstones are removed, but a full control queue can reject barrier cancellation after active ownership is discarded. Retain cleanup responsibility until cancellation is accepted or the worker stops.
- The queued-capture cancellation test does not create queued work. Its absent runtime session causes immediate failure. Correct the setup before treating it as coverage.

The implementer reports that production and test targets compile at `67bc05d`. No tests ran.

### Source clearance at 280dea4

The reviewer confirmed C7's pre-mutation identity checks at `5eb19c8`. C8 required further corrections through `e377533` and `280dea4`.

At `280dea4`, all three control-writer failure paths explicitly end the link. Socket shutdown works despite retained clones. Stdio failure drops the owned input pipe. Host failure handling uses actual cleanup, not a flag-only transition.

All C1–C9 now have source corrections confirmed for the assigned paths. This is not a complete architecture review or runtime clearance.

The implementer reports clean workspace production and test-target compilation at `280dea4`. No tests ran. Actual child cleanup, barrier release, prepare failure, and stdio behavior retain runtime verification limits. Writer-deadline tests need an outer process timeout.
