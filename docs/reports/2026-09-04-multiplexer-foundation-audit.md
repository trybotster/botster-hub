# Botster multiplexer foundation audit

Date: 2026-09-04. Scope: local source and consumed dependency revisions, with focused executable checks. This is an audit, not an implementation change or a certification of the running installation.

## Judgment

The repository split is broadly sensible. The foundation still has several places where **responsibility is separated but progress is not**. A slow worker can occupy the common terminal pump; the TUI performs blocking requests on its rendering thread; the browser serializes input behind acknowledgements. Those are multiplexer problems, independent of plugin quality.

The largest architectural debt is the two server-side terminal models. Worker and daemon both parse the output and retain Ghostty history, but own different aspects of terminal behavior. This adds CPU and memory cost and makes ordering, resize, query replies, snapshots, and recovery harder to reason about.

I would keep the current repositories and fix these ownership and scheduling contracts. Another broad extraction campaign would not address the demonstrated failures. The next milestone should be a measured, continuously working multiplexer: interactive terminals remain responsive while other sessions spawn, resize, flood output, disconnect, and exit.

## Evidence and scope

| Repository | Audited working-tree HEAD |
| --- | --- |
| botster-core | `93acae3` |
| botster-hub | `bb1a330` |
| botster-web | `9e18b10` |
| botster-tui | `3b84d57` |
| botster-tui-kit | `7940306` |
| botster-project-pipelines | `643c4d7` |
| botster-workspaces | `e267871`, with pre-existing plugin/test edits |

Hub consumes Core `48a4370`, not Core HEAD. TUI consumes Core `7eafa47`, Hub client `baeb04d`, and Kit `7940306`. Web consumes terminal-protocol `0.3.0`, UI contract `0.3.3`, and hub-test-support `0.1.43`. A newer sibling checkout does not automatically repair a consumer. Different pins alone do not prove incompatibility.

Pending integration: Hub commit `e50e0f0` updates the Core dependencies to `93acae3` on branch `project-pipelines/ticket_1787600679_990088`. This update is not merged into the audited main revision. At the follow-up check, that branch had advanced to `7164c38`. Tests must record the selected Hub commit and its locked Core revision. Do not treat the pending update as merged evidence.

Three Astra reviewers independently inspected runtime, clients, and plugin/platform primitives. The main review recovered intent from the vault and checked integration evidence. Relevant vault decisions include “botster is a local rust runtime that can optionally federate with cloud,” “botster Hub Rust stays a trusted host kernel,” and “Hub extraction must reduce ownership rather than only split files.” They informed the review; current code determined the findings.

Evidence labels below distinguish executable reproductions, direct source findings, and performance hypotheses. There was no fresh release-build throughput, latency, or RSS campaign across the complete stack. Existing debug binaries and existing browser dist were used only where explicitly noted. No live user sessions or pipeline records were changed.

## 1. Critical: blocking work still runs on the shared terminal pump

Hub's `DataPlaneDriver` owns one CoreDaemon thread. Its loop pumps terminal wakes and then executes up to 64 arbitrary Core request closures. `CoreDaemonHandle::call` waits synchronously for completion with no deadline. A bounded queue bounds the number of requests, not the time any one request occupies the pump.

More seriously, Core's `pump_woken` calls `complete_pending_terminal_resize`. That function loops over pending resizes and calls `wait_for_resize_applied`, which sleeps and pumps the selected worker until acknowledgement or the configured timeout. The resize wait reuses the input timeout `DEFAULT_MODE_GATED_INPUT_TIMEOUT`, which defaults to five seconds. While it waits, a later wake from another session cannot be processed by this common pump.

The code first processes the sessions already present in the batch. That helps same-batch progress but does not protect an unrelated keystroke or PTY response arriving after the wait begins. The existing `stalled_resize_acknowledgment_does_not_block_a_later_named_sibling` test injects both sessions before the same pump, leaving precisely this temporal gap.

Session spawn uses the same synchronous bridge. Worker startup includes process launch, socket readiness, handshake waits, and registry writes. Consequently a new or unhealthy session can interfere with existing terminals even though the data-plane thread is separate from Hub's owner thread.

Evidence: direct production call path; resize wait is present in Hub's pinned Core `48a4370` as well as Core HEAD. Relevant locations:

- Hub `src/data_plane/driver.rs:59`, `:260`, `:300`; `src/runtime.rs:3525`.
- Core `crates/botster-core-daemon/src/daemon.rs:1274` (`:1263` at Hub's pin).
- Core `crates/botster-core/src/engine/managed_session_runtime.rs:162`.
- Core `crates/botster-core/src/runtime/worker_process.rs:1170`, `:1450`, `:2190`.

**Repair:** make resize acknowledgement an explicit pending operation completed by a matching wake or deadline. Keep the pump nonblocking. Perform spawn/readiness and slow persistence preparation outside that pump, then commit a short result through its owner. Preserve ordering and generation checks. A timeout around the caller alone would not free the blocked pump.

**Acceptance:** stall session A's resize acknowledgement, then inject input into B after A has entered the wait. B must deliver echo and accept input before A recovers or times out. Repeat for slow spawn, snapshot hydration, and teardown. The test must distinguish later arrivals from work already in the first batch.

## 2. High: the TUI's event loop defeats an otherwise wake-driven backend

The TUI polls Hub, refreshes its terminal projection, draws, and then waits up to 100 ms for a Crossterm keyboard/mouse event. Terminal output cannot interrupt that wait. With no user input, this imposes roughly a 10 Hz update cadence and up to almost 100 ms of scheduling delay before work costs are included. This is a source-derived bound, not a measured FPS claim.

Normal Hub requests explicitly remove the socket read timeout and block until a response arrives. The rendering/input thread calls that path directly for actions and input. A slow response freezes painting and application-level quit handling as well as the next request.

The TUI also projects the complete Ghostty viewport after individual output frames and again immediately before drawing. The consumed Core implementation walks the grid and allocates a cell vector and grapheme strings. Painting walks the grid again and clones a string per cell. N output frames between draws can cause N+1 full projections. Ratatui's eventual terminal diff does not eliminate that preceding work.

Evidence: direct source, including the actually consumed Core `7eafa47` implementation:

- TUI `crates/botster-tui/src/app.rs:1158`, `:1174`, `:1179`, `:5107`, `:9475`.
- TUI `crates/botster-tui/src/projection_paint.rs:36`, `:63`.
- Core `crates/botster-terminal-ghostty/src/client.rs:594`, `:658` at `7eafa47`.

**Repair:** have socket readiness, input, timers, and shutdown wake the client event loop. Resolve requests outside rendering with bounded completion channels and deadlines. Mark projections dirty as output is applied, then project once per scheduled paint. Borrow symbols during paint. Introduce damage-based row projection only if measurement justifies the additional API.

## 3. High: two server Ghostty models split terminal authority and duplicate work

Core configures both the worker's Ghostty and the parent's Ghostty factory with the same default 10,000,000-byte scrollback budget. The worker parses each output chunk, then the parent parses it again before fanout.

The copies have real responsibilities: the worker supplies mode tokens and attach snapshots; the parent serves readback and injects PTY query replies. A worker comment explicitly suppresses its own replies because the parent dual shadow supplies them. Removing the parent without moving those APIs would break behavior.

This is distinct from legitimate client-side terminal models. Web and TUI need local rendering state. The concern is two complete server parsers and histories for the same session, with split authority over terminal behavior.

Evidence:

- Core `crates/botster-core-daemon/src/daemon.rs:60`, `:517`, `:3440`.
- Core `crates/botster-core-daemon/src/bin/botster-session-worker.rs:518`.
- Core `crates/botster-core/src/engine/managed_session_runtime.rs:1543`, `:2241`.

The default is a page-allocation budget, not fixed upfront RSS or a total memory ceiling. Two instances permit twice that scrollback allocation budget, plus screens, parser state, queues, snapshots, and clients. CPU and actual RSS amplification remain unmeasured.

**Repair direction:** make the surviving session worker the terminal authority for parsing, query responses, modes, and consistent readback/snapshot capture. Keep CoreDaemon's session ownership, routing, admission-independent mechanics, and bounded metadata projections. Measure the current two-parser path before migration and verify replies, resize, attach, and restart against the same authoritative state afterward.

## 4. High: exited-session history has no aggregate memory policy

On natural exit, Core freezes final screen text, a full serialized snapshot, modes, and colors in `retained_terminal`. The parent session/Ghostty object also remains. Explicit `remove_session` releases both; no automatic Hub count, byte, or age policy was found.

This makes retained memory depend on cumulative session history, not just the number of active terminals. Retention can be a valid product feature, but it needs an explicit budget and eviction/archive behavior. Here the retained snapshot is RAM-only: restart loses ended-session readback, as an integration test explicitly expects.

Additionally, `resolve_readback` clones the entire retained structure, including `snapshot.bytes: Vec<u8>`, even when the caller needs only mode flags or screen text. Small readback operations can therefore copy a large history snapshot.

Evidence: Core `crates/botster-core-daemon/src/daemon.rs:498`, `:2033`, `:2430`, `:2521`; `crates/botster-core/src/contract/terminal_screen.rs:81`; ended-readback restart expectation in `crates/botster-core-daemon/tests/daemon_integration_test.rs:4801`.

**Repair:** Hub chooses a finite retained-history policy; Core exposes accounting and explicit eviction mechanisms. Share immutable retained snapshot buffers or return only the requested field. Measure filled-live, exited, removed, and restarted states separately.

## 5. High: Web input is stop-and-wait, and mounted large paste misses the paste protocol

Mounted Restty input selects the mode-gated path. A single Promise chain waits for each `InputResult` before transmitting the next event. A controlled reproduction queued `a`, `b`, `c`: only `a` was sent before its acknowledgement; acknowledging it released only `b`.

Thus ordinary key/mouse event throughput is coupled to acknowledgement round trips. Mode freshness and ordered retry are valid requirements. They do not remove the need for bounded buffering, batching, and a considered in-flight window. The current code also defers resize behind the pending mode-gated operation.

A separate correctness gap appears on paste. Restty submits formatted clipboard text as one semantic input. The mode-gated encoder permits at most 65,519 bytes; the large-paste transaction path exists only in raw `writeInput`. Running the actual method with the installed encoder and a 70,000-byte semantic input produced `PayloadTooLarge`, with zero frames sent. The transport catches the error and records it only through its diagnostic callback.

The live 70 KB paste proof directly opens a stream and calls `encodePaste`, bypassing the mounted clipboard path. The protocol works in that test while the user path does not exercise it.

Evidence:

- Web `src/botster/botsterTerminalPtyTransport.ts:79`, `:97`.
- Web `src/botster/hubTerminalDataPlane.ts:164`, `:194`, `:274`, `:297`, `:618`.
- Vendored Restty `src/vendor/restty/chunk-xwdkhsew.js:56549`.
- Web `scripts/live-packaged-protocol-harness.mjs:7886`.

**Repair:** preserve paste as a semantic operation through the renderer boundary and route it through the transaction protocol. Surface failure to the user. Design bounded input batching/windowing while retaining mode-dependent re-encoding order. Verify through actual keyboard/clipboard interaction, including cancellation and a mode transition during queued input.

## 6. High-confidence waste: production Web output runs test instrumentation

Each rendered chunk is base64-encoded into a DOM data attribute, then encoded again for a harness event. With no harness present, the condition `!harness?.suppressRendererWriteTelemetry` still evaluates true; the downstream recorder checks too late to avoid constructing its argument.

An executable reproduction invoking the actual render observer without a harness fed one 65,536-byte chunk. It recorded two base64 calls processing 131,072 input bytes and an 87,384-character DOM attribute. This is diagnostic work on the production terminal path.

Evidence: Web `src/botster/resttyRenderer.ts:82`, `:509`; `src/botster/hubTerminalDataPlane.ts:1345`.

**Repair:** install the observer only when diagnostic collection is explicitly enabled. Keep terminal payloads out of DOM attributes. This is a small, low-risk performance repair with a direct oracle; it does not need a platform redesign.

## 7. High: session identity can collide in persistence

Core's registry maps every character outside ASCII letters, numbers, dash, and underscore to underscore. Distinct public IDs therefore share a filename. Loading does not check whether the record's session ID matches the requested ID.

A temporary harness included the actual registry source and saved `audit:a`, then `audit_a`. Loading `audit:a` returned `audit_a`, and the registry contained one row. Hub accepts caller-supplied session IDs through its spawn path. This implementation is unchanged between Hub's Core pin and Core HEAD.

Evidence: Core `crates/botster-core-daemon/src/registry.rs:154`, `:165`, `:242`; Hub `src/client_api.rs:1988`.

**Repair:** use an injective filename encoding or a collision-resistant key with stored identity verification. Define and enforce public ID validation consistently. Test punctuation and Unicode identities through spawn, list, restart/adoption, and removal. Existing records need an explicit reconciliation strategy; changing the encoding alone could strand them.

## 8. High: transport recovery is less durable than session recovery

Web schedules one reconnect after transport closure. If that connect fails, the catch records a harness diagnostic but does not schedule another retry. A controlled source reproduction observed one connection attempt and zero retry timers. A restart that takes longer than that immediate attempt can leave an idle page disconnected until another request or reload.

Startup also serially awaits package/navigation/catalog subscriptions before its later `session` pull. A controlled hook reproduction rejecting package navigation never reached that pull.
Optional product surfaces can therefore prevent the later pull and its load-status completion.

Implementation follow-up: the production transport starts its session subscription during `hub.subscribe()`, before that later `session` pull (`hubTransport.ts:280–290`).
The controlled hook result establishes that optional failure blocks the later pull and its load-status completion, not that production never starts session discovery.
The startup repair must preserve this distinction and test the actual transport. Optional failures must not report a false connection failure.

Hub's existing-socket probe has a related liveness hole: it performs a blocking Hello/ACK without read/write deadlines. An isolated dummy listener accepted Hello and withheld ACK; the existing debug Hub stayed blocked until the audit stopped its child. Missing-socket rebinding is explicitly a no-op. These are different from PTY survival: a worker can survive while clients cannot reach it.

Evidence: Web `src/botster/webrtcDaemonClient.ts:1841`, `:1902`; `src/app/useProductionHubConnection.ts:142`; Hub `src/transport/unix/listener.rs:125`, `:159`.

**Repair:** explicit bounded reconnect/backoff with visible status and cancellation; independent session hydration; deadline-bound socket probing and authoritative daemon ownership. Repair socket-path recovery without guessing that a failed handshake grants permission to remove a live listener.

## 9. Runtime scaling still includes whole-population scans and repeated encoding

Wake-driven does not yet mean work proportional to the sessions that woke. CoreDaemon groups session IDs, then filters the full route batch and searches the full ingress list for every session: O(S × R). ClientWorker separately scans all live owners for each named ingress session. Maintaining a session-to-owner index and grouping each batch once would preserve targeted dispatch while avoiding those scans. Evidence: Core `crates/botster-core-daemon/src/daemon.rs:1224`; `crates/botster-core/src/engine/client_worker.rs:735`.

Terminal framing also does avoidable work. Raw output is copied per subscriber and base64-encoded into the terminal event. That event is serialized, parsed back into a JSON Value for TerminalFrame, and serialized again for transport. WebRTC serializes to determine a budget size and again to install its slot; slot reads clone the bytes. Unix base64-wraps the already encoded terminal JSON again, approaching 16/9 (about 1.78×) raw payload size before metadata.

Evidence: Core `crates/botster-core/src/engine/subscription_multiplexer.rs:251`; `crates/botster-terminal-protocol-client/src/events.rs:282`, `:379`; `crates/botster-terminal-protocol/src/frame.rs:22`; Hub `src/transport/webrtc/adapter.rs:118`, `src/transport/shared/adapter_slot.rs:151`, `:264`, and `crates/botster-hub-client/src/lib.rs:3216`.

These are static buffer-pass and wire-expansion findings, not a measured throughput loss. Preserve the opaque protocol boundary, but represent an encoded frame as shared immutable bytes with a cached length. Remove redundant parsing/serialization first; consider a wire-format migration only with measured benefit and explicit compatibility handling.

### Hub primitives

Two examples undermine the claim that all background work is bounded:

- With a session-type subscriber, the owner rebuilds the complete type catalog and reads enabled repositories' configuration files before starting the delivery-budget timer. The unchanged-generation check occurs after this work. Cache by generation and refresh changed sources outside the owner thread. Evidence: `src/subscription/entity.rs:540`, `:568`, `:416`; `src/session_types.rs:1164`.
- Plugin-store `get(key)` reads and JSON-decodes every record in the namespace. Prefix listing also scans and decodes before filtering. An ordinary list-then-get collection loader becomes quadratic. All namespaces share a backend mutex, and batch commits recreate a namespace using hardlinks. This is platform infrastructure debt, even though plugins are the immediate callers. Use direct keyed reads, indexed/ranged retrieval, and an appropriate transactional store. Evidence: `src/capabilities.rs:949`, `:1040`, `:1132`, `:1195`, `:1245`.

These are source-derived complexity and blocking findings; no real-store latency was measured. Plugin execution already releases the outer capability mutex before running prepared work, which is a useful boundary to preserve. Do not infer that every plugin read directly blocks terminal fanout.

## 10. The integration and performance proofs do not yet establish the product claim

There are substantial real-runtime tests, bounded adapter queues, lifecycle accounting, explicit overflow/resync behavior, and teardown checks. Those are valuable assets. However, several tests demonstrate lower-level mechanisms while bypassing the timing or user path that matters: same-batch resize isolation and direct-stream paste are concrete examples above.

The Hub's shared Web+TUI coordinator has also drifted. Its embedded producer omits `botster-web-production-alt-exit`, which the current Web driver requires. Executing that producer emitted only the fallthrough echo, without the alternate-screen exit sequence or required marker. Its integration report's final resume explicitly says the authentic combined coordinator was not rerun before direct merge.

Evidence: `script/prove-north-star-shared-session:50`; Web `scripts/live-packaged-protocol-harness.mjs:8160`; Hub `docs/reports/prove-the-terminal-transport-north-star-across-core-hub-web-and-tui-implement.md:223`.

Seven-repository acceptance tooling does exist in `script/test-production-package-runtime`. The missing piece is keeping a supported coordinate set continuously exercised: checked-in Hub CI runs workspace gates; Web's checked-in workflow is a manual baseline campaign; no checked-in automatic cross-repository client/product lane was found.

Performance evidence must be described precisely. The retained Ubuntu idle campaign measured 0–10 ms Hub CPU over five seconds across 20 repetitions on older pinned revisions. That is positive evidence for that idle topology, not current interactive latency or scaling. The Web report named “capture the debug-runtime terminal regression baseline” explicitly says it did not capture a performance baseline. No complete two-arm observation JSON was found in the inspected reports.

**Repair:** maintain one known-good stack manifest and run the real first-party user paths continuously. Give the harness producer one source. Preserve targeted conformance tests, but require the mounted client and delayed-arrival isolation oracles where those are the product claim. Benchmark the current product directly even when authenticated legacy comparison is unavailable; label it as a current-product baseline, not a legacy comparison.

## Responsibilities I would preserve and tighten

| Component | Responsibility | Required performance property |
| --- | --- | --- |
| Session worker | PTY/process survival; one authoritative terminal parser, modes, replies, snapshot/readback | A slow session does not block another worker or the shared pump |
| Core/CoreDaemon | Generic session lifecycle, input transactions, fair fanout, bounded routing and recovery mechanics | Nonblocking pump; per-session/client budgets and explicit completion state |
| Hub | Policy, admission, credentials, packages, retention policy, transport adapters | Filesystem/network/plugin work cannot occupy terminal progress; short owner turns |
| Web/TUI transports | Protocol adaptation, correlation, bounded input, reconnect, attachment generations | No blocking UI calls; retries and queue limits remain explicit |
| Web/TUI renderers | Local terminal projection, selection/viewport, paint scheduling | Work follows changes and paint demand, not every packet or idle tick |
| TUI Kit | Generic layout, widgets, focus, input mechanics | No second Botster session/terminal authority |
| Plugins | Workflow and product semantics | Cannot be prerequisites for basic terminal liveness |

A single owner is useful when its operations are short and nonblocking. More threads are not the objective. Likewise, local client terminal state is expected; duplicate server authority is the part that needs consolidation.

## Delivery order

1. Repair shared-pump liveness and registry identity. Record the exact Hub and Core revisions for each proof. Account for the pending `e50e0f0` update to Core `93acae3`; recheck main before selecting the integration revision.
2. Repair client scheduling and user paths: TUI wake/request architecture and paint coalescing; Web instrumentation, clipboard transactions, reconnect, then bounded input windowing.
3. Establish current-product performance numbers and a continuously exercised compatible stack. Do this while the preceding work lands, not after another decomposition project.
4. Consolidate server terminal authority and introduce bounded retained history, preserving readback/query-response behavior. Measure the before/after cost.
5. Address catalog and plugin-store scaling under the same owner-turn and isolation rules. Keep plugin workflow cleanup separate from the multiplexer milestone.

Avoid reopening the repository split, moving functions merely to reduce file size, adding another generic scheduler facade, or rerunning the complete integration campaign after every tiny repair. Each owner should prove its changed contract; the supported stack then proves composition continuously.

## Minimum multiplexer performance matrix

Use optimized builds and record exact Hub, Core, worker, clients, renderer, and Kit revisions. Include total process-tree cost, not Hub CPU alone. Agree numeric budgets on named hardware before using them as release gates.

| Workload | Measurements and oracle |
| --- | --- |
| 1 / 10 / 50 quiet sessions | Hub+worker+client CPU, RSS, thread count, wakeups; no repeated full-grid projection without changes |
| One output flood plus interactive sibling | Key-to-PTY and key-to-paint p50/p95/p99; sibling echo progresses during flood |
| Fast and stalled clients on the same session | Fast-client latency; bounded slow-client queue; explicit disconnect/resync |
| Resize A with worker acknowledgement withheld; inject B later | B echo/input progress before A timeout; correct final dimensions on A |
| Slow session spawn while an existing session is active | Existing terminal latency and control responsiveness throughout startup |
| Full-history attach/reconnect during output | Time to first useful paint, input readiness, bytes copied/sent, peak RSS, live-byte ordering |
| Real 70 KB+ clipboard paste | Exact received content; correct bracketed-paste behavior; cancellation and visible errors |
| Repeated filled sessions then exit/remove | Memory growth per state, retained-history accounting, actual reclamation |
| Hub restart longer than first reconnect attempt | Same surviving session reappears without page reload or manual traffic |

## Secondary plugin observations

These were investigated before scope was sharpened and should not drive the foundation roadmap. Controlled Lua reproductions found stale session reuse on step revisit/retry, activation after cancellation, a finite record ceiling without public run archival, workspace context that does not create membership, and a Plan gate that defaults to passed without artifact evidence. Fixable workflow issues; the runtime store's quadratic read path is the foundation concern extracted from them.

## Validation performed

- Core registry collision reproduced using actual registry source in a temporary Rust harness; focused terminal-protocol checks passed (16 tests).
- An attempted isolated resize-timing reproduction did not advance its attachments beyond `attaching` with the existing build artifacts. It is inconclusive and supplies no latency measurement. The shared-pump finding rests on the verified production call path and the same-batch test gap, not timing from that attempt.
- Web source-based controlled reproductions covered input sequencing, oversized semantic paste, production telemetry cost, reconnect, and startup subscription failure.
- Web `npm test` passed with two React `act` warnings. Browser runtime smoke passed against existing dist, not a newly rebuilt source artifact.
- Hub coordinator producer mismatch reproduced by extracting and executing its checked-in shell producer.
- Hub socket startup hang reproduced with an isolated listener and existing debug binary; only audit-owned processes were stopped.
- Plugin package/UI/runtime checks and workspace runtime checks passed where run; the full plugin script stopped at a sandbox-denied cutover socket operation.
- Source audits covered ownership, actual consumer pins, histories, queues, CI entrypoints, and existing performance evidence. Static cost models are not presented as measured end-to-end regressions.

No production source, pins, package installation, vault content, or user data was changed by this audit. This report is the only added repository artifact.
