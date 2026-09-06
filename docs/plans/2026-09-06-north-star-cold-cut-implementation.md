# North-star cold cut: implementation contract and assignments

Status: Root-approved. Date: 2026-09-06. Author: Fable architect; rulings by root.
This is the single live implementation plan. The root draft
`2026-09-06-north-star-cold-cut.md` in the root Hub checkout is background.
This plan supersedes the pin, merge-order, and separate-worktree sequencing in the
September 4 plans and the delivery ledger. Those files remain historical evidence.

User north star, binding: the highest practical multiplexer performance with the
fewest PTY-to-client interactions and the least overhead. Every hop in the hot
path has a stated allocation, copy, and encode budget (section 7). Numbers in
this plan are design requirements, not measured claims. Measurement happens after
the coherent stack passes the user-path verification (section 9).

Verified starting identities (read-only, `git rev-parse HEAD`, clean status):

| Layer | Canonical worktree | Commit |
| --- | --- | --- |
| Core and worker | `/Users/jasonconigliari/botster-sessions/botster-core-foundation-stale-mode` | `bf6e7d996bca2786ad4142c870a13c57a490e241` |
| Hub and Hub client | `/Users/jasonconigliari/botster-sessions/trybotster-botster-hub-project-pipelines-ticket_1787600679_990088-rc1` | `02fa0a7a4c32f8666827e578ee4a28fc3b1b98ca` |
| Web | `/private/tmp/botster-web-foundation.sm0cZt/web` | `45a6eb696a7e472ade870d482d08c6f0bf1e37c0` |
| TUI | `/Users/jasonconigliari/botster-sessions/git-github.com-trybotster-botster-tui-project-pipelines-ticket_1788460430_647093` | `88cc23e2b4cef4178aeb48e95ec9a5549ba15ebe` |

Rules that apply to every assignment:

- One writer per repository. Each writer starts in the worktree it edits.
- Phase A is a schema checkpoint commit: the schema crates and generated
  artifacts compile. The whole workspace need not compile while callers are
  being replaced. No checked-in stubs, fallbacks, or placeholder paths.
- Phase B is the complete implementation commit. A source checkpoint is not a release.
- No fallback protocol, no compatibility branch, no migration scaffold, no dual path.
- Old tests that specify a deleted contract are deleted with that contract.
- Compilation, formatting, and type checks are implementation tools. Replacement
  tests start only when the four producers and consumers form one coherent stack.
- Terminal phase state, queues, semantic input accounting, and byte routing stay
  in Core. Hub owns admission, concrete adapters, retention policy, and host work.
  Lua owns product composition.
- No periodic poll loops anywhere. No payload telemetry unless the operator enables it.

## 1. Ownership rulings

### 1.1 True duplicate server terminal authority (delete)

Core keeps two complete Ghostty instances per worker-backed session. The worker
(`crates/botster-core-daemon/src/bin/botster-session-worker.rs:107`) owns modes,
mode-gated input, and GHOSTSNP export. The parent
(`crates/botster-core/src/engine/managed_session_runtime.rs:2450`,
`TerminalScreenEngine<T>::record_output`) re-parses every output chunk, serves
`read_screen` and `capture_terminal_state`, and injects OSC `write_pty` replies
(`managed_session_runtime.rs:1728`). The worker suppresses its own replies because
the parent does them (`botster-session-worker.rs:518`).

Ruling: the worker is the only server parser. For worker-backed sessions the
parent `TerminalScreenEngine`, the parent terminal backend factory, parent
`write_pty` injection, and the parent Ghostty scrollback budget are deleted.

### 1.2 Legitimate terminal state (keep)

- `DefaultBotsterEngine` under `local-runtime` runs in-process sessions. Its
  terminal is the session owner, not a shadow. It keeps `TerminalScreenEngine`.
- Web (Restty) and TUI (`GhosttyTerminal` projection) keep local rendering models.
- Hub keeps no terminal model and no terminal phase state. Hub adapters stay content-blind.

### 1.3 Generic Core embedder APIs (keep, make nonblocking)

The `CoreDaemon` facade stays the embedder API. Every operation that touches a
worker, the filesystem, or a slow path becomes a Core pending operation completed
by a wake (section 3.6). Nothing on the shared pump waits.

## 2. Contract table

| Contract | Schema owner and file | Producers | Consumers | Finite limits | Replaces |
| --- | --- | --- | --- | --- | --- |
| Host-control v9 envelope | Hub, `crates/botster-hub-client/src/lib.rs` | Web, TUI, Hub CLI, updater, MCP, smoke, test-support | Hub daemon control | 32 outstanding per connection; 1 MiB request frame; 20-byte decimal `request_id`, monotonic | v8 framing, FIFO response matching |
| Terminal stream scheme 2 (binary) | Core, `crates/botster-terminal-protocol/src/{frame.rs,route.rs,codec.rs}`; generated TS in `botster-terminal-protocol-client/src/typescript.rs` | Core fanout | Hub (opaque), Web, TUI | route id 1024 bytes; body by egress budget | JSON `TerminalFrame`, base64 `terminal_output`, JSON snapshot bodies |
| Terminal input scheme 2 (binary) | Core, `crates/botster-terminal-protocol/src/input_frame.rs`, `botster-terminal-protocol-client/src/input.rs` | Web, TUI | Hub (header only), Core `ClientWorker`, worker | 65 535-byte body; 32 operations / 2 MiB per session; 128 / 8 MiB per client | Scheme 1, `ModeGatedInput`, freshness tokens |
| Worker session protocol | Core, `crates/botster-core/src/contract/session_protocol.rs` | Worker, Core parent | Worker, Core parent | 128 MiB frame ceiling (existing); pending 30 resizes, 8 readbacks per session | `FRAME_MODE_GATED_*` family, parent query replies |
| Core pending operations | Core, `crates/botster-core-daemon/src/daemon.rs` | Core | Hub | 4 spawns per daemon; 8 readbacks per session; 4 captures per client | Synchronous engine calls on the pump; unbounded `CoreDaemonHandle::call` |
| Retained ended history | Core mechanism, Hub policy | Core | Hub, clients (through readback) | 16 MiB per object; 64 MiB / 200 sessions aggregate | Unbounded `retained_terminal`, full-struct clone |
| Keyed transactional store | Core `botster-core::storage`; Hub namespaces and grants | Hub capabilities | Lua `plugin_db` | 512 B key; 1 MiB value; 256 ops per batch; range 1 000 items / 4 MiB | `LocalPluginStoreBackend` |
| Hub owner turn | Hub | Hub | Hub | 2 ms, 64 messages, or 256 KiB, whichever first | Owner-turn catalog rebuild, blocking `call` |
| Unix socket ownership | Hub, `src/transport/unix/listener.rs` | Hub | Hub | nonblocking `flock` | Blocking Hello probe, no-op rebind |
| Client loops | Web and TUI | Web, TUI | none | 256 items / 8 MiB pending; 32 in-flight input operations | 100 ms poll loop, stop-and-wait input |

## 3. Exact contracts

### 3.1 Host-control v9 (Hub client crate)

```rust
pub const PROTOCOL_VERSION: u16 = 9;             // any other version is rejected at Hello; no negotiation
pub const MAX_REQUEST_ID_BYTES: usize = 20;      // canonical positive decimal u64, no leading zeros
pub const MAX_OUTSTANDING_REQUESTS: usize = 32;
pub const MAX_CONTROL_REQUEST_BYTES: usize = 1 << 20;
pub const MAX_CONTROL_RESPONSE_BYTES: usize = 1 << 20;   // large data is paged, never oversized
pub const SNAPSHOT_PAGE_BYTES: usize = 256 * 1024;

#[derive(Serialize, Deserialize)]
#[serde(tag = "frame", rename_all = "snake_case")]
pub enum ClientFrame { Request { request_id: String, request: DaemonRequest } }

#[derive(Serialize, Deserialize)]
#[serde(tag = "frame", rename_all = "snake_case")]
pub enum ServerFrame {
    Response { request_id: String, response: DaemonResponse },
    Event { event: DaemonEvent },
}
```

Rules:

- `request_id` is a decimal u64 string, strictly increasing for the connection
  lifetime. The server keeps only `last_request_id`; there is no seen-id set.
- Hub may complete requests out of order. Clients key pending work by
  `(connection_generation, request_id)`.
- Events on one subscription stay ordered relative to each other. No other
  ordering across frames is promised.
- `DaemonOperatorError.request_id` equals the envelope `request_id`.
- A valid 33rd outstanding request receives a correlated error response
  `too_many_requests`. That connection's terminal streams are unaffected.
- A malformed frame, a frame whose declared length exceeds
  `MAX_CONTROL_REQUEST_BYTES`, a nonincreasing or non-decimal `request_id`, or an
  unknown `frame` tag closes the offending connection with typed close reason
  `protocol_error { code }`. Hub cannot echo an id it has not decoded. Closing
  one connection never affects another connection's sessions or routes.
- Large readback is paged. `DaemonRequest::CaptureSnapshot` returns
  `DaemonCaptureSnapshot { capture_id: String, total_bytes: u64, page_bytes: u32,
  pages: u32, unavailable: Option<HistoryUnavailableReason> }`.
  `DaemonRequest::ReadSnapshotPage { session_id, capture_id, page: u32 }` returns
  `DaemonSnapshotPage { capture_id, page, payload_base64 }` of at most
  `SNAPSHOT_PAGE_BYTES`. A capture lives 60 s or until the connection closes; at
  most 4 open captures per connection. `DaemonReadScreen.text` and
  `DaemonModeFlags` fit one response; both gain
  `unavailable: Option<HistoryUnavailableReason>`.
- No Hub-side mirror of terminal-stream events. `terminal_modes`, `input_result`,
  and `history_unavailable` travel only on the terminal stream. `DaemonEvent`
  gains no new terminal variants.
- WebRTC control delivery keeps its chunk header (`message_id`, `generation`)
  for reassembly only. Response payloads carry `request_id`.
- Unknown or rejected `connection_generation`, subscription generation, or
  attachment generation completions are discarded for that key only. They never
  cancel sibling requests, subscriptions, or sessions.
- The generated `crates/botster-hub-client/generated/daemon-protocol.ts` and
  `hub-test-support` fixtures are regenerated by the Hub writer in Phase A.

### 3.2 Terminal stream scheme 2 (Core-owned binary)

Terminal data never passes through JSON or base64. Core owns the framing and
generates both the Rust and TypeScript codecs.

Shared session-event body (one immutable buffer per session event, shared by all
subscribers of that session):

```
TerminalBody = [u8 scheme = 2][u8 kind][u16 LE flags][u32 LE body_len][body]
kind 1 OUTPUT            body = raw PTY bytes
kind 2 SNAPSHOT_READY    body = raw GHOSTSNP bytes (worker export, ready phase)
kind 3 SNAPSHOT_HISTORY  body = raw GHOSTSNP bytes (history phase)
kind 4 SNAPSHOT_FINISH   body = empty
kind 5 PROCESS_EXIT      body = [u8 has_code][i32 LE code]
kind 6 MODES             body = [u32 LE mode_bits][u16 LE rows][u16 LE cols]
```

Personalized body (one buffer per route, low rate):

```
kind 16 ATTACH_STATE        body = [u8 state]           1 attaching, 2 attached, 3 detached, 4 failed
kind 17 INPUT_RESULT        body = [u64 LE operation_id][u8 outcome][u8 has_accepted][u64 LE accepted_payload_bytes][u8 has_written][u64 LE written_pty_bytes][u32 LE mode_bits][u16 LE detail_len][detail UTF-8]
kind 18 HISTORY_UNAVAILABLE body = [u8 reason]          1 evicted, 2 restart, 3 oversize, 4 capture_failed
kind 19 ROUTE_RESYNC        body = empty                 egress overflow: route restarts at SNAPSHOT_READY
```

Core-owned routed envelope, in-process:

```rust
pub struct TerminalFrame { kind: TerminalKind, bytes: Arc<[u8]> }   // bytes = complete TerminalBody
pub struct RoutedTerminalFrame { pub route: RouteId, pub generation: u64, pub frame: TerminalFrame }
pub struct RouteId(Arc<str>);   // subscription id: UTF-8, 1..=1024 bytes, no control characters; validated in Core
```

Session ids keep full Unicode; the registry identity digest already covers them.
Route validation lives in `botster-terminal-protocol/src/route.rs`.

Hub Unix container (adapter writes with `writev`: header buffer plus body slice):

```
UnixTerminalContainer = [u32 LE total_len][u8 container = 2][u16 LE route_len][route UTF-8][u64 LE generation][TerminalBody]
```

WebRTC: the existing chunk header (`LOCAL_WEBRTC_DELIVERY_CHUNK_VERSION` stays 2)
carries route and generation once per message; chunk payloads are slices of the
shared `TerminalBody`. No re-serialization for chunking or budget accounting;
the budget is `bytes.len()`.

Hub obtains route and generation from adapter identity, never from the body.
Hub never inspects `TerminalBody` beyond copying `total_len`.

`TerminalAdapter::try_write(&mut self, frame: &RoutedTerminalFrame)` replaces
the `&TerminalFrame` signature. `close`, `pressure`, `try_read`, the one-slot
rule, hard-close abandonment, partial-envelope completion, and the
`MIN_ADAPTER_INGRESS_BUFFER_FRAMES = 64` contract stay unchanged.

Ordering per route: ATTACH_STATE attached, MODES, SNAPSHOT_READY, live OUTPUT
interleaved with SNAPSHOT_HISTORY, SNAPSHOT_FINISH, then OUTPUT, PROCESS_EXIT last.
Egress overflow (64 frames or 4 MiB queued on one route) emits ROUTE_RESYNC and
restarts that route from a fresh SNAPSHOT_READY; bytes are never dropped and the
client decoder state is reset by ROUTE_RESYNC, never continued.

### 3.3 Terminal input scheme 2 (Core-owned binary)

```
InputFrame = [u8 scheme = 2][u8 kind][u16 BE body_len][u64 BE operation_id][body]    // 12-byte header
kind 1 RAW_BYTES     body = bytes                                    explicit, never mode-encoded
kind 2 KEY           body = [u8 action][u16 key][u16 mods][u16 consumed_mods][u8 composing][u32 unshifted_codepoint][utf8 text to end]
kind 3 MOUSE         body = [u8 action][u8 has_button][u8 button][u16 mods][u16 col][u16 row][u32 x_px][u32 y_px]
kind 4 FOCUS         body = [u8 focused]
kind 5 RESIZE        body = [u16 rows][u16 cols][u32 width_px][u32 height_px]
kind 6 PASTE_BEGIN   body = [u32 total_len][u8 allow_unsafe]
kind 7 PASTE_CHUNK   body = [u32 index][bytes]
kind 8 PASTE_COMMIT  body = empty
kind 9 PASTE_ABORT   body = empty
```

Constants (`input_frame.rs`, mirrored in the generated TS):

```rust
pub const TERMINAL_INPUT_SCHEME_VERSION: u8 = 2;
pub const INPUT_HEADER_BYTES: usize = 12;
pub const MAX_TERMINAL_INPUT_BODY_BYTES: u16 = 65_535;
pub const MAX_PASTE_BYTES: usize = 1_048_576;
pub const MAX_PASTE_CHUNKS: usize = MAX_PASTE_BYTES.div_ceil(MAX_PASTE_CHUNK_DATA_BYTES);
pub const MAX_INPUT_OPERATIONS_PER_SESSION: usize = 32;
pub const MAX_RETAINED_INPUT_BYTES_PER_SESSION: usize = 2 * MAX_PASTE_BYTES;
pub const MAX_ASSEMBLING_PASTES_PER_SUBSCRIPTION: usize = 1;
pub const MAX_INPUT_OPERATIONS_PER_CLIENT: usize = 128;
pub const MAX_RETAINED_INPUT_BYTES_PER_CLIENT: usize = 8 * MAX_PASTE_BYTES;
pub const MAX_ENCODED_INPUT_BYTES: usize = MAX_PASTE_BYTES + 64;
```

Operation ids: u64, chosen by the client, strictly increasing within one
subscription generation, starting at 1 after each attach. Core rejects a
nonincreasing id with `INPUT_RESULT { outcome: rejected_protocol }` for that frame
and keeps only `last_operation_id` per route. `PASTE_CHUNK`, `PASTE_COMMIT`, and
`PASTE_ABORT` carry the `operation_id` of the active paste for that route. In JS
JSON (host control only) u64 ids serialize as decimal strings.

Key and mouse enums are Core-owned generated protocol values:
`botster-terminal-protocol/src/keys.rs` defines `TerminalKey` (explicit `u16`
values) with a generated table of W3C UI Events `code` names, and `TerminalMouseButton`,
`TerminalMods` bit flags. Core has no Crossterm dependency. The
`botster-terminal-ghostty` crate maps `TerminalKey` and `TerminalMouseButton` to
`GhosttyKey` and `GhosttyMouseButton` through explicit `match` arms, not by
numeric order. Key text is bounded only by the frame body; IME composition is
carried by `composing` and `utf8`.

Encoder inputs: `RESIZE` carries cell and pixel geometry so the worker sets
`GHOSTTY_MOUSE_ENCODER_OPT_SIZE`; `MOUSE` carries cell and pixel position so SGR,
SGR-pixels, URXVT, UTF, and X formats all encode; wheel is `MOUSE` press with
button 4..=7; `FOCUS` uses `ghostty_focus_encode`. Nothing supported today is dropped.

Admission and accounting are Core `ClientWorker` responsibilities keyed by
`ClientId`, route, and session: 32 operations and 2 MiB retained per session,
128 operations and 8 MiB per client, one assembling paste per route. A frame
that exceeds a bound is answered `INPUT_RESULT { outcome: rejected_lane_full }`
for that operation only; the route stays attached. `PASTE_BEGIN` with
`total_len > MAX_PASTE_BYTES` is answered `rejected_too_large`. Hub enforces only
the 12-byte header, `body_len`, the adapter ingress buffer, and transport byte
bounds; Hub never parses results or retires semantic counters.

Typed client commands (`input.rs`, `TerminalInputCommand`) mirror the kinds above
one to one with `operation_id: u64` on every variant.

### 3.4 Input outcomes (terminal schema)

```rust
#[repr(u8)]
pub enum InputOutcome {
    Written = 1,               // accepted_payload_bytes and written_pty_bytes both known
    PartialWrite = 2,          // PTY write failed after positive progress; written_pty_bytes known
    WriteFailed = 3,           // zero PTY progress; detail carries the error
    Cancelled = 4,             // cancel honoured; written_pty_bytes reports progress so far
    RejectedNotWritable = 5,   // PTY closed or session exiting; zero progress
    RejectedTooLarge = 6,
    RejectedUnsafePaste = 7,   // ghostty_paste_is_safe false and allow_unsafe not set
    RejectedLaneFull = 8,
    RejectedProtocol = 9,      // nonincreasing id, unknown paste operation, malformed body
    SessionEnded = 10,
    OutcomeUnknown = 11,       // set by Core parent when the worker link fails after admission
}
```

`accepted_payload_bytes` is the client payload the worker admitted; `written_pty_bytes`
is the byte count written to the PTY including encoder-added bytes such as
bracketed-paste markers. Both are `Option<u64>`; unknown is encoded as absent,
never as zero. No layer retries an operation whose outcome is `PartialWrite`,
`WriteFailed`, `Cancelled`, or `OutcomeUnknown`.

Worker execution: admit atomically into the bounded lane; borrow the terminal only
for the encode call; append the encoded bytes to an ordered per-session encoded
buffer; progress PTY writes nonblocking from the worker loop while output
parsing continues; report exactly one result per operation. `FRAME_INPUT_CANCEL`
abandons unwritten remainder and reports `Cancelled` with the written count.

### 3.5 Worker session protocol (Core)

```rust
// deleted
FRAME_MODE_GATED_PTY_INPUT (0x19), FRAME_MODE_GATED_PTY_INPUT_RESULT (0x1a), FRAME_MODE_GATED_CANCEL (0x1b)
// added
pub const FRAME_INPUT_OPERATION: u8 = 0x1d;   // parent -> worker: [u64 route_generation][InputFrame]
pub const FRAME_INPUT_RESULT: u8 = 0x1e;      // worker -> parent: INPUT_RESULT body plus route_generation
pub const FRAME_INPUT_CANCEL: u8 = 0x1f;      // parent -> worker: [u64 route_generation][u64 operation_id]
pub const FRAME_MODES_CHANGED: u8 = 0x20;     // worker -> parent: MODES body (spontaneous)
pub const FRAME_FINAL_STATE: u8 = 0x21;       // worker -> parent, after last PTY output, before FRAME_PROCESS_EXITED
```

`FRAME_PTY_OUTPUT` payloads are the raw PTY bytes and become the shared OUTPUT
body without re-encoding. `FRAME_GET_SCREEN`, `FRAME_GET_MODE_FLAGS`, and
`FRAME_GET_SNAPSHOT` stay as correlated worker RPCs; the parent no longer answers
any of them itself. Output forwarding never waits on these RPCs, on plugin
work, on per-frame acknowledgement, or on a second parser.

```rust
pub struct WorkerFinalState {
    pub screen_text: String,
    pub snapshot: Option<Vec<u8>>,     // GHOSTSNP; None when export fails, reason in `error`
    pub mode_bits: u32, pub rows: u16, pub cols: u16,
    pub color_profile: TerminalColorProfile,
    pub error: Option<String>,
}
```

Natural-exit ordering is preserved: final PTY bytes, `FRAME_FINAL_STATE`,
`FRAME_PROCESS_EXITED`. The parent retains the final state, then emits
PROCESS_EXIT on every route after the last OUTPUT and only after the adapter
reports Ready. Hard close abandons unsent frames without replay.

### 3.6 Core pending operations and progress

```rust
pub struct PendingOperationId(pub u64);
pub enum CoreOperation {
    Spawn(SpawnSessionRequest), Adopt(SessionId), ShutdownSession(SessionId), RemoveSession(SessionId),
    ReadScreen(ReadScreenRequest), ReadModeFlags(ReadModeFlagsRequest), CaptureSnapshot(CaptureSnapshotRequest),
    Resize { session_id: SessionId, rows: u16, cols: u16 }, CancelInput { route: RouteId, generation: u64, operation_id: u64 },
}
pub enum CoreCompletion { /* one variant per CoreOperation with id and typed Result */ }
impl CoreDaemon {
    pub fn begin(&mut self, op: CoreOperation, deadline: Instant) -> Result<PendingOperationId, CoreDaemonError>;
    pub fn cancel(&mut self, id: PendingOperationId) -> bool;
    pub fn take_completions(&mut self) -> Vec<CoreCompletion>;
    pub fn read_snapshot_page(&mut self, capture: &CaptureId, page: u32) -> Result<Arc<[u8]>, CoreDaemonError>; // in-memory, bounded
}
```

`pump_woken` reconciles worker replies, resize acknowledgements, and expired
deadlines, then queues completions and one embedder wake. It never sleeps or
waits on a worker, the filesystem, or the network. Worker launch, socket
readiness, and handshake run on Core's `worker_process` threads; `begin(Spawn)`
returns before launch completes. Limits: 4 pending spawns per daemon, 8 pending
readbacks per session, 4 open captures per client; beyond them `begin` returns
`CoreDaemonError::PendingLimit`. Expiry fails only the named operation.

Deleted: synchronous `read_screen`, `read_mode_flags`, `capture_snapshot`, and
`spawn` waits; `drain_runtime_for_readback`; `wait_for_resize_applied` remnants;
`latest_mode_for`; `map_gated_result`; `mode_gated_pty_input`;
`ModeGatedInputOutcome`; `last_mode_freshness`; `resolve_readback` full clone.

Routing scale: `CoreDaemon` and `ClientWorker` keep a session-to-routes index
updated on bind and unbind; batch processing iterates woke sessions and their
routes only. Fairness: each pump turn services every woke route once in wake
order; a route with a Full adapter yields without blocking siblings.

Hub bridge: `CoreDaemonHandle::call` is deleted. `CoreDaemonHandle::begin(op,
deadline) -> PendingOperationId` and a `CoreCompletion` receiver are the only
cross-owner path. The Hub owner reads Core state through its own projection
(`daemon_projection.rs`, `session_projection.rs`), updated from completions and
`DataPlaneProgress`; those reads are synchronous in-memory reads of Hub state.
Hub-owned blocking work (filesystem, package catalog, store, network) runs on a
Hub pool and never enters the Core thread. The Hub owner turn is cooperative:
2 ms, 64 messages, or 256 KiB processed, whichever comes first, then yield. The
budget is scheduling policy, not a latency guarantee.

### 3.7 Retained ended history

```rust
pub struct RetainedTerminal {
    pub screen_text: Arc<str>, pub snapshot: Option<Arc<[u8]>>,
    pub mode_bits: u32, pub rows: u16, pub cols: u16, pub color_profile: TerminalColorProfile,
    pub exited_at: u64, pub bytes: usize,   // screen_text + snapshot + 256-byte metadata allowance
}
pub struct RetentionPolicy { pub max_object_bytes: usize, pub max_total_bytes: usize, pub max_sessions: usize }
impl CoreDaemon {
    pub fn set_retention_policy(&mut self, policy: RetentionPolicy);
    pub fn retention_accounting(&self) -> RetentionAccounting;   // total_bytes, sessions, evictions
    pub fn evict_retained(&mut self, session_id: &SessionId) -> bool;
}
```

Hub defaults (`HubConfig`): `max_object_bytes = 16 MiB`, `max_total_bytes = 64 MiB`,
`max_sessions = 200`. Eviction is oldest `exited_at` first, applied on insert. An
object above the cap is not stored; the registry keeps the exit record and exit
code, and readback returns `history_unavailable { oversize }`. Readback returns
the requested field through `Arc` clones only.

Restart semantics:

- Live sessions: the worker survives Hub restart. Adoption re-attaches from the
  worker's own snapshot; nothing replays through a parent parser.
- Ended sessions: retained history is Core RAM only. After Hub restart the
  registry exit record remains and readback returns `history_unavailable { restart }`.
  No persisted ended history is promised.
- Live worker Ghostty page budget stays 10 MB with one instance per session.

### 3.8 Keyed transactional store

```rust
pub struct Namespace(String);   // ^[a-z0-9_.-]{1,64}$; one redb table per namespace
pub trait KeyedStore {
    fn get(&self, ns: &Namespace, key: &[u8]) -> Result<Option<Bytes>, StoreError>;
    fn range(&self, ns: &Namespace, prefix: &[u8], after: Option<&[u8]>, max_items: usize, max_bytes: usize) -> Result<RangePage, StoreError>;
    fn batch(&self, ns: &Namespace, ops: &[StoreOp]) -> Result<(), StoreError>;   // atomic
}
pub const MAX_KEY_BYTES: usize = 512; pub const MAX_VALUE_BYTES: usize = 1 << 20;
pub const MAX_BATCH_OPS: usize = 256; pub const MAX_RANGE_ITEMS: usize = 1_000; pub const MAX_RANGE_BYTES: usize = 4 << 20;
```

Backend: `redb`. It is not in the local cargo registry; the Core writer reports a
dependency blocker if the fetch or Rust 1.97 build fails. No automatic fallback.
Hub `capabilities.rs` keeps namespace, grant, and quota policy over this trait.
`LocalPluginStoreBackend` is deleted with no old-format read path.

### 3.9 Hub owner work and socket ownership

- Session-type catalog: refreshed on the Hub pool when the package registry or
  session-type generation changes; publishes `Arc<CatalogSnapshot { generation, entities }>`.
  The owner compares generations and delivers; it never reads package files.
- Unix socket: Hub holds an `flock`ed `<socket>.owner` file for its lifetime,
  acquired nonblocking at start. Failure means another live Hub owns the path;
  start fails with `AlreadyRunning`. The Hello probe is deleted. A stale socket
  is removed only after the lock is held. `rebind_missing_socket_path` re-creates
  the listener while the lock is held.

## 4. Per-layer assignments

### 4.1 Core and worker (writer: Fable Core)

Phase A, compiling schema crates and generated artifacts:

- `contract/session_protocol.rs`: 3.5 constants and payloads.
- `botster-terminal-protocol/src/{frame.rs, route.rs, codec.rs, input_frame.rs, keys.rs}`: 3.2, 3.3, 3.4.
- `botster-terminal-protocol-client/src/{input.rs, events.rs, typescript.rs}`: typed
  commands and decoders; generated TS binary codecs and key tables.
- `botster-terminal-ghostty/src/sys.rs`: bindings for `ghostty_key_event_*`,
  `ghostty_key_encoder_*`, `ghostty_mouse_event_*`, `ghostty_mouse_encoder_*`,
  `ghostty_focus_encode`, `ghostty_paste_is_safe`, `ghostty_paste_encode`; safe
  wrappers `GhosttyTerminal::{encode_key, encode_mouse, encode_focus, encode_paste}`;
  explicit `TerminalKey` and button maps.
- `daemon.rs`: `CoreOperation`, `CoreCompletion`, `PendingOperationId`, `RetentionPolicy`.
- `contract/terminal_adapter.rs`: `try_write(&RoutedTerminalFrame)`.
- `botster-core::storage` trait and constants (3.8).

Phase B:

- Worker: 3.4 execution model, 3.5 frames, `write_pty` reply injection, `FRAME_FINAL_STATE`.
- Parent: delete the shadow (1.1), synchronous readback, mode-gated engine path,
  `last_mode_freshness`; implement pending operations, owner index, `take_completions`.
- Fanout: one `TerminalBody` `Arc<[u8]>` per session event; `RoutedTerminalFrame`
  per route with `Arc<str>` route; ROUTE_RESYNC on overflow.
- `ClientWorker`: 3.3 admission and accounting by `ClientId`; paste assembly;
  `OutcomeUnknown` on worker link failure.
- Retention (3.7); store over `redb` (3.8).
- Delete the diagnostic config fields and registry legacy probe listed in 4.5.
- Docs: delete `docs/ghostty-shadow-terminal-architecture.md` and
  `docs/architecture/ghostty-shadow-terminal-adapter.md`; rewrite
  `terminal-protocol.md`, `terminal-adapter.md`, `durable-session-worker-protocol.md`,
  `core-daemon.md`, `client-worker-terminal-egress.md`, `ghostty-only-terminal-runtime-authority.md`.
- Tests: delete mode-gated, freshness, parent-shadow, synchronous-readback,
  JSON-frame, and legacy-probe tests. Keep wake, adapter-close, natural-exit,
  hard-close, registry identity, and paste tests, rewritten to scheme 2.

### 4.2 Hub and Hub client (writer: Fable Hub, this worktree)

Phase A: v9 envelope and constants; `CaptureSnapshot` paging DTOs; regenerated
`daemon-protocol.ts`; regenerated `hub-test-support` fixtures for v9 and scheme 2;
Core dependency rev at Core Phase A. The client crate and test-support compile.

Phase B:

- `src/daemon/control/*`, `owner_loop.rs`, `connection.rs`: `ClientFrame` decode,
  monotonic ids, 32-limit correlated error, protocol-error close, out-of-order
  completion, 2 ms / 64 / 256 KiB owner turn.
- `src/transport/webrtc/{control_channel.rs, delivery.rs}`: `request_id` in
  response payloads; delete kind-based matching helpers.
- `src/data_plane/driver.rs`, `runtime.rs`: delete `CoreDaemonHandle::call`;
  `begin` plus completion receiver; convert all 39 `call` sites; owner reads only
  its projection.
- `src/transport/shared/adapter_slot.rs`, `unix/{mux_write.rs, adapter.rs, host_write_order.rs}`,
  `webrtc/{adapter.rs, subscription_channel.rs}`: `RoutedTerminalFrame`, Unix
  container with `writev`, WebRTC slices of shared bytes, budget by `len()`.
- `src/transport/shared/ingress.rs`: 12-byte input header validation, opaque forward.
- `src/config.rs`, `runtime.rs`: `RetentionPolicy` defaults, accounting in `DaemonStatus`.
- `src/capabilities.rs`: `plugin_db` over `KeyedStore`; delete `LocalPluginStoreBackend`.
- `src/subscription/entity.rs`, `session_types.rs`: off-owner catalog refresh.
- `src/transport/unix/listener.rs`: `flock` owner file, delete probe, implement rebind.
- `src/update.rs`: identity-checked registry reads; `IncompatibleWorkers` startup
  error; no termination, no deletion.
- Consumers in 4.6 (CLI, updater, MCP, smoke, operator console, scripts, fixtures).
- Delete the diagnostic knobs in 4.5.
- Docs: rewrite `docs/client-protocol.md` (v9, limits, paging, retention, restart
  semantics, scheme 2 containers); update `docs/lua-plugin-abi.md` (store);
  mark September 4 plans historical in `docs/plans/README` note.
- Tests: delete v8 framing, FIFO matching, JSON-frame adapter, catalog-in-owner,
  store-scan, Hello-probe, and old-worker termination tests. Keep lifecycle,
  close-contract, WebRTC reservation, and adapter tests on scheme 2.

### 4.3 Web (writer: Fable Web)

Phase A: regenerated protocol imports (host v9 and terminal scheme 2 codecs from
Core), `hubTransport.ts` request map keyed by `request_id`, typed input encoders
in `terminalInputMessage.ts` (`KeyboardEvent.code` to `TerminalKey` by generated
name table, pointer to MOUSE with cell and pixel geometry, FOCUS, RESIZE with
pixel size, paste). Typecheck passes for these modules.

Phase B:

- `webrtcDaemonClient.ts`: delete `pendingMatchesResponse` and FIFO `pendingRequests`;
  map by `request_id`; monotonic ids; close on protocol error.
- `hubTerminalDataPlane.ts`: binary scheme 2 decode from `ArrayBuffer` without
  base64; in-flight window of 32 operations per route in order, no
  acknowledgement serialization; delete `stale_mode` retry, freshness handling,
  and `map_gated_result` mirrors; surface every `INPUT_RESULT` outcome through
  `TerminalInputOutcome`; ROUTE_RESYNC resets the decoder and re-primes Restty.
- `resttyRenderer.ts`, `TerminalViewHost.tsx`, `mouseMode.ts`,
  `mountScopedWheelReencoder.ts`: intercept keyboard, pointer, wheel, and focus at
  the container as paste already is; Restty encoders are render-only; apply
  MODES for local mouse capture; clipboard paste through PASTE_* frames.
- Reconnect: one cancellable retry loop with capped exponential delay, 250 ms to
  8 s, while the user's connection intent exists; visible status; session pull
  independent of optional subscriptions.
- Telemetry: render observer installed only when diagnostics are enabled; no
  DOM attribute payloads.
- Paint: apply frames in order, dirty flag, one paint per animation frame.
- `scripts/live-packaged-protocol-harness.mjs` and `src/App.test.mjs` shims: v9 and scheme 2.
- Delete v8 and JSON-frame fixtures and stale-mode scenarios; keep paste,
  resize, reconnect, and attach-ordering scenarios.

### 4.4 TUI (writer: Fable TUI)

Phase A: dependency revs (Core Phase A; Hub Phase A commit rev for
`botster-hub-client` and `botster-hub-test-support`), `crossterm` `event-stream`
feature, `futures-lite`, and the `HubIo` owner signature:

```rust
pub struct HubIo { /* socket reader, writer, EventStream task, deadlines */ }
pub enum AppWake {
    Input(crossterm::event::Event), Terminal(RoutedTerminalFrame),
    Completed { request_id: u64, result: Result<DaemonResponse, RequestError> },
    Event(DaemonEvent), Deadline, Shutdown,
}
impl HubIo {
    pub fn submit(&self, request: DaemonRequest, deadline: Instant) -> u64;
    pub fn cancel(&self, request_id: u64);
    pub fn shutdown(self, bound: Duration);
}
```

Phase B:

- `app.rs::run_loop`: one `mpsc::Receiver<AppWake>`; wait with `recv_timeout` to
  the earliest absolute deadline only; no periodic poll.
- Input: `crossterm::event::EventStream` on the I/O thread under
  `futures_lite::future::block_on` with `or`-select against the channel; drop of
  the stream wakes its internal wait thread (verify on this pin before relying
  on it for bounded shutdown). TUI maps Crossterm keys and mouse to `TerminalKey`
  and MOUSE in TUI code; Core stays Crossterm-free.
- Delete `request_and_apply`, pre-loop `try_connect`, `set_read_timeout(None)`
  blocking reads, `mode_gated_input_required`, `forward_mode_gated_input`, reprobe logic.
- Input window 32 operations per route; paste through PASTE_* frames; explicit
  RAW_BYTES only where the TUI intends raw bytes.
- Projection: dirty flag; one `project_viewport` per paint; `projection_paint.rs`
  borrows symbols.
- Bounded storage: 256 items / 8 MiB pending events; a terminal route overflow
  detaches and re-attaches that route only.
- `acceptance.rs` and first-party plugin tests on v9 and scheme 2; delete tests
  for deleted paths.
- TUI Kit: no change; confirm no Botster session state was added.

### 4.5 Diagnostic machinery and legacy scaffolding removal

Rule: production paths carry no environment-driven test behavior. Fault
injection that a legitimate end-state test needs moves to `#[cfg(test)]` code or
to `botster-core-test-support` / `botster-hub-test-support` fakes behind traits.
Strict typed errors stay.

Hub, delete from production code and their readers: all 35 `BOTSTER_HUB_TEST_*`
environment knobs present at `02fa0a7` (`..._RESERVED_CHANNEL_RECEIPT`,
`..._INGRESS_ADMISSION_OBSERVATION`, `..._DATA_PLANE_OBSERVATION`,
`..._PAUSE_DATA_PLANE`, `..._PARK_DATA_PLANE_TURN`, `..._DATA_PLANE_WATCHDOG_MS`,
`..._FORCE_ADAPTER_WOULD_BLOCK*`, `..._FORCE_CLOSE_WORK_OVERFLOW`,
`..._STALL_UNIX_EVENT_FLUSH`, `..._UNIX_WAKE_OBSERVATION`, `..._EXTRA_CHANNEL_*`,
`..._CORE_SHUTDOWN_MARKER`, `..._DATA_PLANE_STOP_MARKER`, `..._FAIL_RUNTIME_DRAIN_*`,
`..._FAIL_SNAPSHOT_HISTORY_AFTER_READY`, `..._INCOMPATIBLE_DAEMON`,
`..._UPDATE_SOURCE_ROOT`, `..._WORKER_EGRESS_CAPACITY`, `..._CLIENT_EVENT_QUEUE_MAX`,
`..._LIFECYCLE_JOURNAL_CAPACITY`, `..._RESERVATION_EXPIRES_IN_SECONDS`,
`..._EVENT_HANDLER_HOLD_MS`, `..._EVENT_INVOCATION_TIMEOUT_MS`,
`..._LOCAL_RUNTIME_READINESS_BUDGET_MS`, `..._DISABLE_ONE_SHOT_CLAIM`,
`..._DROP_JOURNAL_WAKES`, `..._HOLD_JOURNAL_PULL`, `..._CLOSE_LOCAL_WEBRTC_OPERATION`,
`..._CLEAR_ADAPTER_WOULD_BLOCK_AFTER_REJECTION`, `..._FORCE_SHUTDOWN_CLASSIFY_STOPPING_FOR`);
`adapter_slot.rs::forced_would_block`; `webrtc/adapter.rs::test_forced_would_block`;
`driver.rs::{pause_data_plane, park_stopped_turn_for_test, observe_for_test}` and
the watchdog env override; `capabilities.rs::batch_test_hook`; reserved-channel
receipt recording in `webrtc/subscription_channel.rs`; the admission receipt hook.
Capacities those knobs overrode become constants from section 6 or `HubConfig`
fields. `src/update.rs::terminate_incompatible_sessions` and old-worker recovery
are deleted; startup fails with `HubStartError::IncompatibleWorkers { sessions }`
and deletes nothing.

Core, delete: `CoreDaemonConfig` fields `test_fail_retain_final_terminal_state_for`,
`test_fail_runtime_drain_for`, `test_fail_runtime_drain_message`,
`test_fail_snapshot_history_after_ready`, `test_force_shutdown_watchdog_for`,
`test_mode_for`; `worker_process` `test_fail_pty_writes` and its worker CLI flag;
`registry.rs::{reject_legacy_record, legacy_record_filename}` and their tests.
Keep `SessionRegistryError::UnsupportedFormat` and identity checks on read,
overwrite, and remove; an unsupported record stays on disk and yields that error.
`#[cfg(test)]` counters in `CoreDaemon` stay because they compile out.

### 4.6 Every consumer of the changed framing and input schema

Host-control v9:

| Consumer | Owner | Change |
| --- | --- | --- |
| `src/main.rs` CLI commands (status, sessions, packages, doctor, smoke, attach, spawn) | Hub | `DaemonConnection::request` with monotonic ids |
| `src/update.rs` updater connection | Hub | v9; identity-checked registry; no termination |
| `src/local_webrtc_smoke.rs` | Hub | v9 chunks with `request_id` |
| `src/mcp.rs` | Hub | v9 |
| `src/operator_console.rs` | Hub | request completions keyed by id |
| `crates/botster-hub-client` `request`, `request_with_requirement`, `stream_attach`, `typescript.rs` | Hub | envelope, id generation, 32-limit client guard |
| `crates/botster-hub-test-support` `isolated_hub.rs`, `lib.rs`, `conformance_data.rs`; `packages/hub-test-support/test.mjs` | Hub | fixtures for v9 and scheme 2 |
| `script/prove-north-star-shared-session`, `script/test-production-package-runtime`, `examples/`, `fixtures/` | Hub | one producer source, v9 frames |
| Web `hubTransport.ts`, `webrtcDaemonClient.ts`, `scripts/live-packaged-protocol-harness.mjs`, `src/App.test.mjs` | Web | id map, regenerated protocol |
| TUI `app.rs`, `acceptance.rs` | TUI | `HubIo`, v9 fixtures |

Terminal scheme 2 and input scheme 2:

| Consumer | Owner | Change |
| --- | --- | --- |
| `botster-terminal-protocol-client/src/{input.rs, events.rs, typescript.rs}` | Core | binary codecs, key tables, TS artifact |
| `botster-core/src/engine/{client_worker.rs, subscription_multiplexer.rs, managed_session_runtime.rs}` | Core | shared bodies, routed frames, admission |
| worker binary | Core | decode, encode, results, final state |
| `botster-core-dev`, Core `examples/`, `botster-core-test-support` | Core | scheme 2 senders and fakes |
| Hub `src/transport/shared/{ingress.rs, adapter_slot.rs}`, `unix/*`, `webrtc/*`, `crates/botster-hub-client` stream helpers | Hub | containers, header validation, opaque forward |
| Web `terminalInputMessage.ts`, `hubTerminalDataPlane.ts`, `botsterTerminalPtyTransport.ts`, `mouseMode.ts`, `mountScopedWheelReencoder.ts`, `terminalGrid.ts` | Web | binary decode, typed events, delete re-encoders |
| TUI `app.rs` input and terminal paths, `projection_paint.rs` | TUI | typed events, binary decode |

## 5. Integration ordering

1. Root records this document as binding. Writers start in parallel.
2. Core Phase A commit. Hub Phase A commit (depends on Core Phase A rev). Web and
   TUI Phase A commits (depend on Hub Phase A rev and artifacts).
3. All four Phase B implementations proceed in parallel. Contract changes go to
   the owning writer and root; this document is amended, never forked.
4. Codex reviews each layer's Phase B as one consolidated change.
5. With four Phase B commits, root records the exact revision set and authorizes
   the first replacement stack build and the user-path verification in section 9.

## 6. Finite limits, one table

| Limit | Value | Outcome when reached |
| --- | --- | --- |
| Outstanding host requests per connection | 32 | correlated `too_many_requests` response |
| Host request frame / response frame | 1 MiB / 1 MiB | close connection / page data |
| `request_id` | decimal u64, 20 bytes, monotonic | close on invalid or nonincreasing |
| Snapshot page / open captures per connection | 256 KiB / 4 | next page request / `PendingLimit` |
| Input operations per session / retained bytes | 32 / 2 MiB | `rejected_lane_full` for that operation |
| Input operations per client / retained bytes | 128 / 8 MiB | `rejected_lane_full` |
| Paste per operation / assembling per route | 1 MiB / 1 | `rejected_too_large` / `rejected_lane_full` |
| Encoded input per operation | 1 MiB + 64 B | `rejected_too_large` |
| Input operation id | u64, monotonic per route generation | `rejected_protocol` |
| Route id | 1..=1024 UTF-8 bytes | attach rejected |
| Egress per route | 64 frames / 4 MiB | ROUTE_RESYNC, fresh SNAPSHOT_READY |
| Pending resizes per session | 30 (existing) | park owner, resume on acknowledgement |
| Pending readbacks per session / spawns per daemon | 8 / 4 | `PendingLimit` |
| Retained ended object | 16 MiB | not stored, `history_unavailable{oversize}` |
| Retained ended total / sessions | 64 MiB / 200 | evict oldest exit |
| Live worker Ghostty page budget | 10 MB | Ghostty scrollback saturation (existing) |
| Store key / value / batch / range | 512 B / 1 MiB / 256 ops / 1 000 items, 4 MiB | typed `StoreError`, next page |
| Client pending events | 256 items / 8 MiB | detach and re-attach that route |
| Client in-flight input operations | 32 per route | queue locally to 2 MiB, then reject to user |
| Hub owner turn | 2 ms, 64 messages, or 256 KiB | yield |
| Socket owner lock | nonblocking `flock` | `AlreadyRunning` |
| Web reconnect delay | 250 ms to 8 s, capped exponential | keep retrying while intent exists; cancel available |

## 7. Hot path and per-hop budget

```
PTY read (worker)
  │ read into reusable 64 KiB buffer; Ghostty parse in place
  ▼
FRAME_PTY_OUTPUT over worker socket  ...................... copy 1: kernel socket (unavoidable IPC)
  │ Core reader thread reads into a fresh Vec sized to the frame; no re-encode
  ▼
Core fanout: TerminalBody = Arc<[u8]> built once (header + raw bytes) .... copy 2: header prepend, one allocation
  │ RoutedTerminalFrame { route: Arc<str>, generation, frame } per route: no payload copy
  ▼
Hub adapter try_write ............................................ zero copy: slot holds the Arc
  ├─ Unix: writev([container header], [TerminalBody]) ............ copy 3: kernel socket (unavoidable)
  └─ WebRTC: chunk = header + slice of TerminalBody .............. copy 3: encryption/SCTP output (unavoidable)
  ▼
Client decode: header parse, raw bytes fed to renderer ........... copy 4: renderer's own write (client-owned)
```

| Hop | Allocations per output chunk | Copies | Encode work |
| --- | --- | --- | --- |
| Worker PTY read and parse | 0 (reused buffer) | 0 | Ghostty parse only |
| Worker to Core socket | 1 (frame Vec) | 1 kernel | length-prefixed header |
| Core `TerminalBody` | 1 (`Arc<[u8]>`) | 1 memcpy into the body | 8-byte header |
| Per route | 1 small (`RoutedTerminalFrame`, `Arc` clone) | 0 | none |
| Hub adapter slot | 0 | 0 | none |
| Unix container | 0 (stack header) | 1 kernel (`writev`) | 15-byte header plus route |
| WebRTC | per chunk header only | 1 into encrypted output | none beyond transport |
| Client | decoder view, no base64 | 1 into renderer | none |

Forbidden in the hot path: JSON, base64, per-subscriber payload copies,
readback RPC waits, snapshot waits, plugin invocations, per-chunk
acknowledgements, timer-driven polling, payload telemetry when disabled.
Batching is event-driven: one pump turn coalesces all woke routes; per-route
fairness is one frame per route per turn when adapters are Ready.

## 8. Conflicts and root rulings recorded

1. `redb` is not in the local registry. Core reports an actual blocker if it
   fails to resolve or build; no automatic fallback.
2. Crossterm 0.29 `event-stream` is present. TUI confirms drop-wakes-thread on
   this pin before relying on it for bounded shutdown.
3. `subscription_id` leaves the terminal body; route and generation travel in the
   Core routed envelope and the Hub containers. Fixtures change accordingly.
4. TUI pins `botster-hub-client` and `botster-hub-test-support` to the Hub
   canonical worktree commit until root publishes.
5. Restty must expose container-level keyboard and pointer events before its own
   encoder runs. Web already does this for paste; if Restty swallows keydown
   first, Web intercepts at the pane app level and reports to root.
6. Per-connection input accounting is Core `ClientWorker` state keyed by
   `ClientId`; Hub enforces transport bounds only.

## 9. Verification after the coherent stack exists

First-party user paths on one recorded revision set: spawn, attach, key input,
mouse, focus, resize with pixel geometry, 70 KB+ paste, output flood with a
responsive sibling, stalled client with a fast client, natural exit with final
bytes then PROCESS_EXIT, hard close abandonment, reconnect, Hub restart with
live-worker adoption, ended-session readback after restart returning
`history_unavailable{restart}`, and eviction at the retention limit.

Then, separately, an optimized-build benchmark of whole process-tree CPU and RSS,
output throughput, and interactive sibling key-to-paint p50, p95, and p99 on
named hardware. No performance claim is made before those numbers exist, and no
"fastest" claim is made without a comparable measurement.

## 10. Out of scope

Workflow plugin redesign, Project Pipelines behavior, cloud federation, and any
change to `botster-ui-contract` 0.3.3.
