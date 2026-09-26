# Plugin platform design

Status: revision 3 for review (2026-09-26). Owner: plugin-platform Hub writer.
Revision 1 (`4e6f5aa2`) and revision 2 (`0bc5c83e`) were rejected; section 17
maps every finding to its answer.

Scope: the Lua-facing platform that first-party plugins need, starting with
the cutover plugins messaging, orchestrator, and project-pipelines.
**PRODUCT DECISION** marks a choice for the user. **USER DECISION** records
a choice the user already made. Everything else is a design proposal for
the reviewer.

The monorepo plugin API is evidence of what plugins need. It is not a design
to copy. This is a cold cut: no compatibility with the monorepo API and no
compatibility shims for the current modular ABI.

## 1. User decisions that bind this design

1. Plugins reach processes, sockets, and the filesystem only through
   Hub-brokered capabilities.
2. **Isolation target: one OS process per plugin.** The plugin API is
   host-agnostic: every plugin-to-Hub interaction is an asynchronous,
   serializable message, so the same plugin code runs on today's hardened
   thread host and on the process host. Core builds the policy-free worker
   process and IPC; the Hub owns the worker binary, the Lua runtime, the OS
   sandbox profile, the limits, the restart policy, and the grants.
3. **Storage: document collections** with declared indexes, bounded index
   queries, atomic batches, and change feeds. SQL is only a possible later
   opt-in capability.
4. **UI: entities plus declarative views** over a small fixed component set.
   Plugins never run code in clients. Sandboxed Web bundles are only a
   possible later escape hatch.

## 2. Survey summary

Facts from the current Hub (HEAD `f1674920`):

1. **One VM per plugin, serialized.** Each package has one
   `LuaPluginRuntime` with one `Mutex<LuaState>` (`src/lua_runtime.rs:1773`).
   Core runs two executor threads per plugin; both serialize on that mutex.
2. **The owner loop never blocks on plugins.** Admission is `try_admit`; a
   queued request parks as `ControlStep::pending_in(ReadyClass::PluginCompletion)`;
   the Core completion notifier wakes the owner; a bounded `CompletionDrain`
   slice routes the result.
3. **Helpers block the plugin worker.** `coordination.*` and
   `entity_publish` wait up to 1 s for the owner; `session_types.spawn` and
   `ensure_worktree_and_spawn` wait up to 30 s; `plugin_db.*` performs
   synchronous redb I/O. A handler in a 30 s wait blocks every other
   invocation of that plugin.
4. **No asynchronous delivery into Lua exists.** Core's
   `CapabilityRuntimeRequest.callback` is always `None`; capability events
   are never drained into Lua in production; no production code fires timers.
5. **Grants are global.** `default_hub_capability_grants`
   (`src/capabilities.rs:1852`) is one set that names `project-pipelines` and
   `botster-workspaces` literally. Handlers register with
   `required_capability: None`.
6. **Error conventions are inconsistent**: raise, `{ ok = false }`, or
   `{ status = ... }`, depending on the helper.
7. **The sandbox leaked ambient authority** through the base library. Slice 0
   closes it (section 14).
8. **Parsed but inert handler kinds**: `command`, `hook`, `timer`,
   `session_action`.
9. **Missing**: logging, JSON, clock, modules, HTTP from Lua, file watching,
   package secrets, session list/inspect/close/metadata, MCP prompts and
   proxy, notifications, repo detection, command gates.
10. **Core refuses unregistered handler ids** at admission ("plugin handler
    is not registered"), so every delivery must target a handler registered
    at load.

Cutover plugin needs:

| Need | messaging | orchestrator | project-pipelines |
| --- | --- | --- | --- |
| MCP tools with caller identity | 3 | 10 | 63 |
| Session list / lookup by label / inspect | yes | yes | |
| Session spawn (managed worktree) | | yes | yes (exists) |
| Session close / metadata (label, task) | | yes | legacy |
| Terminal read / input write | | yes | |
| Inbox post / receive + doorbell | yes | yes | legacy |
| Timers | | yes (60 s prune) | legacy (30 s reconcile) |
| Storage with atomic batch | | | yes (KV today) |
| Indexed queries | | | full N+1 reload per call today |
| Views, actions, entity families | | | yes (UiNode trees today) |
| Event emit / subscribe | | | yes / legacy |
| Package dependency check | | | yes (Hub lacks it) |
| Command gate, push, MCP prompts, fs | | | legacy |
| Multi-hub discovery and RPC | | yes | |
| Logging / JSON | yes | yes | |

## 3. Principles

1. **No ambient authority.** A plugin VM holds pure computation only. Every
   effect outside the VM is a message to the Hub.
2. **Capabilities are declared, granted per package, and checked per call**
   against the package's admitted set (section 13). No global grant list.
3. **Capabilities name outcomes, not mechanisms.** Plugins select
   Hub-admitted objects by id (target, worktree, session, collection, gate).
   Plugins never supply commands or host paths.
4. **Every host interaction is an asynchronous message.** Nothing blocks the
   owner loop; no host I/O blocks the plugin VM; nothing polls.
5. **Plugins write sequential code.** Handlers are suspendable: an
   asynchronous call suspends the handler and a delivery resumes it
   (section 4).
6. **Every resource is bounded by existing accounting.** New numbers are
   **PRODUCT DECISION**s.
7. **One plugin cannot starve siblings**: per-plugin reservations, per-plugin
   queues, and rotation between plugins under the owner turn budget.
8. **Reload-safe by generation.** Suspended handlers, callbacks, and
   reservations belong to one generation. A new generation cancels them.

## 4. Execution model

### 4.1 Messages

A plugin is a message-driven actor. The same messages exist on both hosts.

| Direction | Message | Meaning |
| --- | --- | --- |
| Hub to plugin | `Invoke{PluginInvocationRequest}` | run a handler, deliver a result, or deliver an event |
| Hub to plugin | `Cancel{request_id}` | cancel one invocation |
| plugin to Hub | `InvocationResult{PluginInvocationResult}` | the handler finished or suspended |
| plugin to Hub | `HostCall{call_id, request_id, max_result_bytes, body}` | ask the Hub to do one operation |
| plugin to Hub | `Log{body}` | one structured log record |

On the process host these are Core IPC frames (section 12). On the thread
host they are in-process calls with the same types and the same accounting.
`body` fields are Hub types that Core carries as opaque, byte-accounted
bytes.

### 4.2 Suspendable handlers

Every handler invocation runs as a Lua coroutine. An asynchronous
`botster.*` helper does four things, in this order:

1. It takes one slot and `max_result_bytes` from the plugin's delivery pool
   (section 5). If the pool cannot fit the call, the helper returns
   `backpressured` at once and does not suspend.
2. It records `call_id -> coroutine` in the VM's call table.
3. It sends a `HostCall` with that `call_id`.
4. It yields. The invocation returns `InvocationResult{ suspended }` and the
   VM is free for other invocations.

When the Hub completes the call, it sends `Invoke` to the reserved handler
`botster:resume` with `{ call_id, root_request_id, result }`. The runtime
resumes the coroutine, and the helper returns `result` to the plugin code.

**Ordering inside the VM.** The VM runs one invocation at a time on both
hosts: the thread host holds the VM mutex, and the process host runs
`Invoke`s one at a time. A resume `Invoke` can therefore start only after the
invocation that issued the call has yielded and returned. A completion can
never reach a coroutine that has not suspended yet. The resume trampoline
removes the call-table entry before it resumes, so a duplicate completion
finds no entry and is dropped and counted. An unknown `call_id` (from a
cancelled or retired call) is dropped the same way.

**Ordering at the Hub (the call ledger).** Outside the VM, the Hub can see
results in either order: on the thread host, one executor can publish the
final result of a resume before the other executor publishes the
`suspended` result of the invocation that issued the call. The Hub keeps a
per-plugin ledger entry for each request-response invocation, keyed by its
`root_request_id`, which every resume carries:

| Event | Ledger state before | Action |
| --- | --- | --- |
| `suspended` for the root | running | mark suspended |
| `suspended` for the root | final result held | answer the waiter with the held result; remove the entry |
| final result for the root | running or suspended | answer the waiter; remove the entry |
| final result for the root | (arrives before `suspended`) | hold it; mark "final held" |
| deadline or cancel | any | answer the waiter with `timed_out` or `cancelled`; keep a tombstone until the root's last outstanding call completes, then remove it |

Sequential calls in one handler produce one `suspended` result per resume;
only the root's first `suspended` and its final result change the ledger.
Resumes of Background handlers need no ledger entry. The ledger holds at most
one entry per admitted request-response invocation, so the existing
request-response admission bounds it.

**Refusal after the yield.** A Hub-side refusal (for example
`capability_denied`) is a completion like any other and uses the call's
delivery slot, which the plugin took in step 1 before it yielded. So a
refusal always reaches the suspended handler.

```lua
call = function(request)
  local ticket = botster.capabilities.store.get({ collection = "tickets", id = request.payload.id })
  if not ticket.ok then return ticket end
  local page = botster.capabilities.store.query({
    collection = "runs", index = "by_ticket", equals = { ticket.value.doc.id }, limit = 20,
  })
  return { ok = true, value = { ticket = ticket.value.doc, runs = page.value.docs } }
end
```

Rules:

1. **Request-response handlers** (MCP tools, view actions, entity
   snapshots) may suspend. The Hub keeps the original waiter parked, keyed by
   the suspended call. The final `InvocationResult` of the last resume
   answers the waiter. The original deadline covers the whole handler.
2. **Interleaving.** While a handler is suspended, other invocations of the
   same plugin may run. Plugin state can change across a suspension point,
   exactly as across an `await`.
3. **Yield boundaries.** A coroutine cannot yield across a host (C) frame,
   for example inside a `table.sort` comparator or a `string.gsub` callback.
   An asynchronous helper called there returns
   `{ ok = false, error = { kind = "invalid_request", message = "cannot suspend here" } }`.
   Lua 5.4 `pcall` is yieldable, and the sandbox wrapper keeps it yieldable.
4. **Cancellation.** Unload, reload, and deadline expiry resume every
   affected suspended handler with
   `{ ok = false, error = { kind = "cancelled" } }` so its cleanup code runs
   under a fresh instruction budget. A handler that suspends again after a
   cancellation receives `cancelled` at once. When the generation is already
   gone (process killed or VM replaced), the handler's memory is released
   with the VM; nothing leaks.
5. **Bound.** Every suspended handler holds exactly one delivery reservation
   (section 5). The per-plugin reservation count therefore bounds the number
   of suspended handlers. This design adds no separate number.
6. **Accounting.** A suspended coroutine's stack and locals stay in the
   plugin VM, so the per-VM memory limit charges them. On the process host
   the OS limits of the worker also apply.
7. **Instruction budget.** Each `Invoke`, including each resume, runs under
   a fresh instruction budget, as today. A handler cannot progress without a
   delivery, and every delivery is admitted, so resumes are bounded work.

### 4.3 Streams

Watches, repeating timers, change feeds, and event subscriptions deliver many
results. They register a callback; each event is a new handler invocation
(`Invoke` of the family's reserved handler), not a resume. The callback runs
as a suspendable handler too.

```lua
local watch = botster.capabilities.store.watch({ collection = "questions" }, function(event)
  -- event.value = { op = "put" | "delete" | "resync", id = "...", doc = {...} }
end)
watch.value:cancel()
```

A stream has three separate lifetimes, each with its own bound:

| Lifetime | What it holds | Charged to | Released when |
| --- | --- | --- | --- |
| Setup call | the call that arms the stream | one pool debit, like any call | the setup result is admitted |
| Armed resource | the timer, watch, or feed itself | one unit of the per-plugin capability operation capacity (128); a recursive inotify watch charges one unit per watched directory | `cancel()`, unload, or generation retirement |
| Pending event | one undelivered event | nothing until delivery; then one pool debit at admission | the event's invocation is admitted |

- **At most one pending event per stream.** A stream never queues a second
  event. While an event is pending, new occurrences are folded into it:
  timers count `missed`, watches mark `overflow` for that watch, and change
  feeds mark `resync`. So an armed stream holds at most one event's bytes, and
  the pending event is built only when the pool has room for it.
- **`cancel()`** removes the callback inside the VM at once, so no new event
  invocation of that stream runs. The Hub then disarms the resource and drops
  its pending event. A callback invocation that is already suspended is an
  independent handler: it keeps its own calls and completes normally. Plugin
  code that must stop in-flight work checks its own state after each resume.

### 4.4 Local helpers

`botster.json`, `botster.clock`, and `botster.log` run inside the VM and
send no host call (`log` sends a fire-and-forget `Log` message). They return
results directly and never suspend.

## 5. Delivery, reservations, and accounting

This section is the contract with the event-driven writer (Hub delivery
path) and the Core process-host writer (engine and IPC; premise
`docs/plans/plugin-process-host.md` on `origin/delivery/plugin-process-host-20260926`).

### 5.1 The delivery pool

1. **Engine reservation.** At load, the Hub asks the Core engine for a
   standing delivery pool for the plugin's generation: a number of slots,
   request bytes (Background queue), and completion bytes (completion store).
   The engine takes them out of the plugin's own Background capacity. The
   pool invariant is: plugin Background capacity = delivery pool + ordinary
   Background work (events). Ordinary work can never occupy pool capacity.
2. **Admission from the pool** never refuses for capacity while the pool has
   room. It debits one slot and the delivery's actual bytes. The bytes return
   to the pool when the engine drains the job's completion, or when the job
   fails or is cancelled. A retired generation's pool is released as a whole.
3. **Credits are pool room.** The plugin side keeps an exact mirror of the
   pool's free room. A host call first debits one slot and its
   `max_result_bytes` from that room (section 4.2 step 1). If the room cannot
   fit the call, the call returns `backpressured` at once and does not
   suspend. On the process host, the parent grants credits only from the
   pool's free room, and a `HostCall` that exceeds its credit is a protocol
   violation that kills the worker.
4. **Guaranteed delivery.** Because the plugin debits delivery capacity
   before it sends and yields, every outcome of an accepted call (success,
   Hub refusal, timeout, cancellation) has capacity for its `Invoke`.
5. **Exactly once.** Every accepted call produces exactly one completion.
   The Hub's call table removes the call when it publishes the completion;
   the VM's call table drops duplicates and unknown ids (section 4.2).
6. **Cancellation keeps its slot.** A cancelled call keeps its pool debit
   until its `cancelled` completion is admitted or the generation retires.
7. **One terminal per call.** For every accepted call the Hub produces
   exactly one terminal: a completion admitted from the pool, or
   `release_call(call_id)` for a call it will never answer (for example
   because its generation retired). The pool unit returns to the plugin as
   `Credit{call_id, bytes}` exactly once: when that completion is drained,
   fails, or is cancelled, or on `release_call`. Request bodies and log
   records use their own conserved credits (section 5.2), so every
   plugin-to-Hub frame is credit-bounded and the parent reader never blocks.
8. **Numbers.** **PRODUCT DECISION (pool size)**: Option A (recommended):
   reuse existing per-plugin numbers: slots = the capability operation
   capacity (128), request bytes plus completion bytes = the plugin's
   Background queue byte capacity (1 MiB) and completion reservation bytes as
   the engine already configures them. Option B: dedicated values.

### 5.2 Charges outside the pool

| Allocation | Charged to | Bound |
| --- | --- | --- |
| `HostCall` request body (store documents, HTTP body) | credit request bytes (process host: parent ingress accounting) | the call's declared request size, checked before the parent reads the body |
| Decoded request in the Hub | Hub callback account, before decoding | per-callback ceiling (8 MiB) and the call's request size |
| Producer buffer (HTTP body being read, file read, query page being built) | Hub callback account, before each growth step | the call's `max_result_bytes`; a producer stops and fails the call at that size |
| Encoded completion | the call's pool debit | `max_result_bytes` (encoded size, including field names and headers) |

- **Serialization overlap.** While the Hub encodes a completion, the
  producer buffer and the encoded bytes both exist. The producer buffer is
  released immediately after encoding. So a plugin's peak is at most twice its
  in-flight `max_result_bytes`, and the pool bounds that sum.
- **Shared callback account.** The callback account is Hub-wide (64 MiB).
  Each plugin's share of it is bounded by its own in-flight calls, which the
  pool bounds (twice the pool bytes at the peak). A plugin therefore cannot
  consume the account beyond its pool-derived share, and sibling plugins keep
  theirs.

### 5.3 Owner delivery

Completions are published to a per-plugin pending queue, and the producer
calls an owner-installed notifier (one control message, one maintenance
wake). The owner's delivery slice rotates across plugins with pending
completions under the existing turn budget (items, bytes, time) and admits
each completion from its plugin's pool. Pool admission cannot refuse for
capacity, so the slice never parks a completion.

## 6. The calling convention

1. **Table first.** Every `botster.*` function takes one table. A function
   with no fields accepts an omitted table: `botster.clock.now()` equals
   `botster.clock.now({})`. A stream takes its callback as the second and
   last argument.
2. **Results.** Every `botster.*` function returns
   `{ ok = true, value = ... }` or
   `{ ok = false, error = { kind, message, retryable, ... } }`.
   Asynchronous one-shot functions return this result after resuming.
   Stream functions return `{ ok = true, value = <handle> }`.
3. **Constants are not calls.** `botster.json.null` is a constant.
4. **Raising.** Helpers raise only when the VM cannot continue: the VM memory
   limit, the instruction budget, or a host allocation refusal.
5. **`require(name)`** is the one exception: it is Lua's standard module
   function, not a `botster.*` operation (section 7.4).
6. **Namespaces.** `botster.<area>` holds functions that need no grant
   (`register`, `log`, `json`, `clock`, `events`). `botster.capabilities.<area>`
   holds every function that needs a manifest grant.

Error kinds (one closed set):

| kind | meaning | retryable |
| --- | --- | --- |
| `invalid_request` | argument shape or value refused | no |
| `capability_denied` | the package lacks the grant, or the object is not the plugin's | no |
| `not_found` | the named object does not exist | no |
| `conflict` | revision conflict or already exists | no |
| `quota_exceeded` | a plugin-owned quota is full | no |
| `backpressured` | a bounded queue or reservation is full now | yes |
| `timed_out` | the operation's deadline expired | yes |
| `cancelled` | the plugin or the Hub cancelled the operation | no |
| `unavailable` | the target runtime is stopped or not configured | yes |
| `failed` | the operation ran and failed; `detail` explains | no |

Core `CapabilityRuntimeErrorKind` maps onto it: `Backpressured` to
`backpressured`; `CapabilityDenied` to `capability_denied`;
`OperationNotFound`, `ResourceNotFound`, `StoreNotFound` to `not_found`;
`TimedOut`, `Cancelled`, `InvalidRequest` to the same names;
`RuntimeStopped` to `unavailable`; `RevisionConflict` to `conflict`;
`QuotaExceeded` to `quota_exceeded`; `PatchFailed` and `BackendFailed` to
`failed`. Event-plane `rejected_*` statuses map to `invalid_request` or
`capability_denied`; `shed_*` to `backpressured`.

Handler signature: every handler receives one `request` table with
`payload`, `caller` (section 9.4), and `origin`. A handler returns a result
table; for MCP tools `ok = false` becomes an MCP error result.

## 7. Area 1: basics

### 7.1 Logging

```lua
botster.log.info({ message = "ticket advanced", fields = { ticket_id = id } })
-- debug, info, warn, error; returns { ok = true } or { ok = false, error = { kind = "backpressured" } }
```

- Grant: none. Local; fire-and-forget `Log` message.
- The Hub writes records with package, generation, and level to the Hub log
  and to a per-plugin ring that the `get_plugin_logs` operator tool reads.
  The worker keeps a bounded log queue; when it is full, the record is
  dropped and a dropped counter travels with the next accepted record.
- **PRODUCT DECISION (log limits)**: record size, rate, queue size, and ring
  size are new numbers. Option A (recommended): reuse the event-plane
  policy values (64 KiB per record, 100 records/s, burst 200) and the
  per-plugin event capacity (256) for the queue and the ring. Option B:
  dedicated values.

### 7.2 JSON

```lua
botster.json.encode({ value = { a = 1 } })                 -- { ok, value = "<text>" }
botster.json.encode({ value = {}, arrays = "empty" })       -- empty tables encode as []
botster.json.decode({ text = body })                        -- { ok, value = <table> }
botster.json.null                                           -- constant
```

- Grant: none. Local. Uses the memory-bounded converter in
  `src/lua_runtime/lua_json.rs`.

### 7.3 Clock

```lua
botster.clock.now()        -- { ok, value = <Unix epoch milliseconds> }
botster.clock.monotonic()  -- { ok, value = <milliseconds since the worker started> }
```

- Grant: none. Local.

### 7.4 Modules: `require`

```lua
local store = require("lib.store")   -- the package file lua/lib/store.lua
```

- Grant: none.
- At load, the Hub reads the package's module tree (`lua/**.lua`) and sends
  it in the load message as in-memory text. `require` serves only that set.
  After load the VM has no filesystem access, which the sandboxed worker
  process requires anyway.
- The Hub walks the module tree from the package root without following
  symlinks and refuses non-UTF-8 names, `..` components, and files larger
  than the per-callback ceiling.
- **Bounded staging, charged before allocation.** The Hub reserves the
  staging budget from the callback account before it reads anything: the
  per-VM memory limit (16 MiB) is the ceiling for the whole module set. For
  each file it reserves the file's size from that budget, then reads with the
  existing bounded reader, which refuses a file that grows past its reserved
  size. A set that exceeds the budget fails the load before the VM exists.
  The staged text moves into the load message (process host) or the new VM
  (thread host), and the staging charge is released only after the VM's own
  memory accounting holds the text.
- Modules compile with the text-only `load` (section 14).
- A reload sends the module set again, so there is no stale cache.

### 7.5 Timers

```lua
local once = botster.capabilities.timer.after({ ms = 5000 }, function(event) end)
local tick = botster.capabilities.timer.every({ ms = 60000 }, function(event)
  -- event.value = { sequence = n, missed = m }
end)
tick.value:cancel()
```

- Grant: `{ surface = "timers", scope = "callbacks" }`.
- The Hub keeps plugin timers in one deadline heap. The owner loop's next
  deadline includes the earliest plugin timer: one scheduled wake at the
  exact expiry, no periodic tick.
- **Fair expiry service.** The delivery slice services expired timers in
  rotation across plugins under the owner turn budget. When the budget ends
  with expired timers left, the remaining timers stay due. Due work is ready
  work, so the owner schedules another slice at once; this is a ready queue,
  not a timer.
- **Missed expiries.** At service time the Hub computes
  `missed = floor((now - due) / interval)` and the next due time in constant
  time, whatever the gap. A repeating timer whose previous fire is still
  undelivered does not deliver again; its next delivery carries the missed
  count. So a short interval cannot fill any queue, and no minimum interval
  is needed.
- An armed timer is one armed resource; each fire is one pending event
  (section 4.3).
- **PRODUCT DECISION (timer marker)**: the rewrite rules have no category
  for plugin-requested schedules. Option A: add
  `// timer: plugin-schedule — <which plugin API>` for the one Hub site that
  arms plugin deadlines. Option B: classify it as `deadline`.
- Plugins that used timers to poll (orchestrator's prune, project-pipelines'
  reconcile) port to events.

## 8. Area 2: outside world

### 8.1 HTTP

```lua
local response = botster.capabilities.http.request({
  method = "POST",
  url = "https://api.telegram.org/sendMessage",
  headers = { { name = "content-type", value = "application/json" },
              { name = "authorization", secret = "bot_token" } },
  body = encoded.value,
  max_bytes = 65536,
  timeout_ms = 10000,
})
-- response.value = { status = 200, headers = { ... }, body = "..." }
```

- Grant: `{ surface = "network", scope = "<origin>" }`. The request origin
  must equal a granted origin.
- Transport: Core `HttpCapabilityRuntime` with Hub `RealHttpTransport`.
  Redirects are not followed; a 3xx response returns to the plugin as is.
- **Pre-admission ceiling.** The helper computes `max_result_bytes` before
  the call, from fixed numbers only: the envelope overhead, plus the
  response header ceiling (the Core HTTP header count limit of 64 times the
  Core header-size limit), plus `max_bytes` of body. The Core response limit
  (4 MiB) caps `max_bytes`. The transport reads headers and body into a
  producer buffer charged per growth step (section 5.2) and fails the call
  with `failed` / `detail = "response_too_large"` the moment either part
  passes its ceiling; it never truncates.
- **PRODUCT DECISION (network policy)**: today the Hub allows loopback only,
  GET and POST only, and denies credential headers. Option A (recommended):
  manifest-declared remote origins, enabled by the operator; credentials
  only through secret-bound headers (section 8.3). Option B: plugins may
  read secrets and set credential headers themselves. Option C: loopback
  only.

### 8.2 Filesystem roots: watch, read, list, stat

```lua
botster.capabilities.fs.watch({ root = "vault", path = "inbox", recursive = true }, function(event)
  -- event.value = { path = "inbox/a.md", change = "created" | "modified" | "removed" | "overflow" }
end)
local file = botster.capabilities.fs.read({ root = "vault", path = "inbox/a.md", max_bytes = 262144 })
local listing = botster.capabilities.fs.list({ root = "vault", path = "inbox" })
```

- Grant: `{ surface = "filesystem", scope = "<root name>" }`. A root is
  `data` (the plugin's own directory under the Hub data directory) or a name
  the manifest binds to an operator-set configuration field. The operator,
  not the plugin, chooses the host directory.
- **Path grammar.** `path` is a relative path of non-empty UTF-8 components
  separated by `/`. The Hub refuses absolute paths, `.` and `..` components,
  empty components, and NUL bytes before any I/O.
- **Authority.** The Hub opens each root once, at grant time, as a
  directory descriptor and records the root's device and inode. Every
  operation resolves relative to that descriptor; no operation resolves a
  path string from `/`.
- **Linux enforcement.** `openat2(root_fd, path, RESOLVE_BENEATH |
  RESOLVE_NO_SYMLINKS | RESOLVE_NO_MAGICLINKS)`. The kernel guarantees that
  the whole resolution stays beneath the root, including under concurrent
  renames: it fails with `EXDEV` or `EAGAIN` instead of escaping. The Hub
  maps both to `capability_denied` and does not retry.
- **macOS enforcement.** macOS has no `openat2`. The Hub opens with
  `openat(root_fd, path, O_NOFOLLOW_ANY)`, which refuses a symlink in any
  component. A component walk alone does not stop a concurrent rename that
  moves an intermediate directory outside the root, so after the open the
  Hub verifies containment of the opened object: it reads the object's
  current path with `fcntl(F_GETPATH)` and requires the root's canonical path
  as a prefix, and it requires the root's device and inode to be unchanged.
  If the check fails, the Hub closes the descriptor without reading and
  returns `capability_denied`. The Hub reads only through the verified
  descriptor, so what it returns was beneath the root when verified.
- **Adversarial test (slice 6).** A helper thread in the test renames an
  intermediate directory between a position inside the root and a position
  outside it, where the outside copy holds a marker file. The plugin reads
  and lists the path while the renames run. No response may ever contain the
  marker. The ablation removes the containment check (macOS) or
  `RESOLVE_BENEATH` (Linux), and the test must then observe the marker.
- **Listings** report entries with their kind (`file`, `directory`,
  `symlink`, `other`) and never follow symlinks. The entry count is bounded
  by the existing filesystem grant limit (1024 entries); more entries return
  `truncated = true` and a cursor.
- **Reads** are bounded by `max_bytes` (capped by the existing 1 MiB grant
  limit) and charged to the call's reservation.
- **Watches** use the OS event source (FSEvents on macOS, inotify on Linux);
  there is no polling watcher. An event carries only a root-relative path and
  a change kind, never file data. **Identity rule:** the Hub reports an event
  only if its path lies under the root's canonical path and the root's device
  and inode are unchanged; otherwise it drops the event. If the root itself
  is renamed or removed, the watch ends with `unavailable`. Any later read of
  an event's path goes through the enforcement above, so an event can never
  lead to data outside the root.
- **Watch accounting.** A watch is one armed resource; a recursive inotify
  watch charges one armed-resource unit per watched directory, and a watch
  that would exceed the plugin's remaining capacity fails with
  `quota_exceeded`. A watch holds at most one pending event (section 4.3);
  further changes fold into it, and a fold that loses information marks it
  `overflow` so the plugin rescans with `list`.

### 8.3 Package secrets

```lua
botster.capabilities.secrets.read({ name = "bot_token" })               -- { ok, value = "<secret>" }
botster.capabilities.secrets.write({ name = "refresh_token", value = t }) -- { ok = true }
```

- Secrets are package configuration fields of type `secret`. The manifest
  lists, per field, the origins it may be sent to:
  `{ "name": "bot_token", "type": "secret", "origins": ["https://api.telegram.org"] }`.
- Two separate grants:
  - `{ surface = "secrets", scope = "use" }`: the plugin may name its secret
    fields in HTTP headers (`{ name = ..., secret = "<field>" }`). The Hub
    substitutes the value at send time only when the request origin is in
    that field's `origins`. The plugin never sees the value.
  - `{ surface = "secrets", scope = "read" }`: `secrets.read` returns
    plaintext, for plugins that must place a secret in a URL or a body. The
    origin guarantee does not hold for a package with this grant; the
    operator sees that at enable time.
- `secrets.write` needs `read`. Both calls are asynchronous (the credential
  store is the OS keychain in production).
- Another package's secrets are unreachable: names resolve only inside the
  calling package.

## 9. Area 3: agents and sessions

### 9.1 Session reads

```lua
local sessions = botster.capabilities.sessions.list({ owner = "self" })   -- or owner = "any"
local session = botster.capabilities.sessions.get({ session_id = id })
-- rows: { session_id, label, title, agent_name, status, lifecycle, target_id, worktree_id,
--         branch, workspace, owner_plugin, created_at, metadata }
```

- Grant: `{ surface = "session_actions", scope = "session_read" }`; `owner =
  "any"` needs `session_read:any`.
- Rows are the Hub session projection (the `/session` entity family). Live
  state stays on `botster.events.on({ owner = "hub", name = "session_family" }, fn)`.

### 9.2 Session actions

```lua
botster.capabilities.sessions.close({ session_id = id })
botster.capabilities.sessions.update({ session_id = id, label = "Review", metadata = { step = "verify" } })
botster.capabilities.sessions.send_input({ session_id = id, text = "continue\r" })
botster.capabilities.sessions.read_screen({ session_id = id })
botster.capabilities.sessions.notify({ session_id = id, title = "New message", hint = "receive_messages" })
```

- Grants on the `session_actions` surface: `session_close`,
  `session_update`, `session_input`, `session_screen`, `session_notify`.
- By default a plugin acts only on sessions it spawned. **PRODUCT DECISION
  (cross-plugin control)**: Option A (recommended): a `:any` scope suffix
  (for example `session_close:any`) that the operator approves at enable.
  Option B: all session actions apply to every session.
- `update` writes `label`, `title`, and the plugin's own metadata namespace
  (`metadata[<package>]`). The Hub adds a control request for it.

### 9.3 Messaging between agents

```lua
botster.capabilities.messages.post({ to_session = id, type = "message", payload = body })
botster.capabilities.messages.receive({ session_id = request.caller.session_id })
```

- Grant: `{ surface = "session_actions", scope = "session_message" }`.
- Mechanism: the Core routed-envelope primitive with session targets (the
  mechanism of today's native MCP message tools).
- **PRODUCT DECISION (message tools)**: Option A (recommended): the
  messaging plugin owns the MCP tool surface, label lookup, envelope format,
  and doorbell text, and the Rust `NativeHubToolProvider` message tools are
  deleted. Option B: keep the native tools; ship no messaging plugin.

### 9.4 Caller identity

Handlers receive `request.caller = { session_id = ... }` for MCP calls.
Today `mcp-serve` reads the caller from `BOTSTER_SESSION_UUID`, which any
local process can set. **PRODUCT DECISION (caller authentication)**:
Option A (recommended): the Hub injects a per-session caller token into the
session context at spawn; `mcp-serve` presents it; the Hub maps it to the
session. Option B: treat the variable as advisory and document the gap.

### 9.5 Lifecycle and terminal output

- Lifecycle: the existing session-family stream (snapshot plus deltas),
  subscribed with `botster.events.on`. Terminal deltas gain `exit_code`.
- **PRODUCT DECISION (output hooks)**: the Hub must not relay terminal
  bytes. Option A (recommended): semantic terminal events from Core (bell,
  OSC 9/777 notifications, title change, idle/active) in the session-family
  stream, plus `read_screen` on demand. Option B: a sampled raw-output
  stream, which puts the Hub on the byte path.

### 9.6 Multi-hub

**PRODUCT DECISION (federation)**: Option A (recommended for cutover): local
hub only; `list_hubs` returns the local hub. Option B: a federation
capability now.

## 10. Area 4: MCP

```lua
return botster.register({
  tools = { { name = "tickets.create", description = "...", input_schema = { ... },
              handler = "create", call = function(request) end } },
  prompts = { { name = "tickets.plan", description = "...",
                arguments = { { name = "ticket_id", required = true } },
                handler = "plan", call = function(request) end } },
})
```

- Grant: `{ surface = "mcp" }`. Prompts use the Core `McpPrompt` handler
  kind; `mcp-serve` adds `prompts/list` and `prompts/get`.
- **PRODUCT DECISION (proxy)**: Option A (recommended): declarative
  `mcp_servers` in the manifest; the Hub owns the MCP client (over a network
  grant, or a supervised stdio runnable entrypoint), re-exposes tools as
  `<package>.<tool>`, and dispatches auth failures to the plugin's
  `McpProxyAuthError` handler. Option B: plugin-code proxies over HTTP.

## 11. Area 5: storage (USER DECISION: document collections)

### 11.1 Declaration

The manifest declares collections and indexes, so the Hub admits them with
the package:

```json
"storage": {
  "collections": {
    "runs": { "indexes": { "by_ticket": ["ticket_id", "created_seq"], "by_status": ["status"] } },
    "run_steps": { "indexes": { "by_run": ["run_id", "position"],
                                "by_session": ["agent_session_uuid", "status"] } }
  }
}
```

An index is an ordered list of top-level document fields. Changing an index
declaration triggers a Hub-run rebuild of that index at enable time, charged
to the package.

### 11.2 Operations

```lua
local store = botster.capabilities.store
store.get({ collection = "runs", id = "run_1" })
-- { ok, value = { doc = {...}, revision = 3 } }  or  error.kind = "not_found"
store.put({ collection = "runs", id = "run_1", doc = run, expected_revision = 3 })
store.delete({ collection = "runs", id = "run_1", expected_revision = 4 })
store.batch({ ops = {
  { op = "put", collection = "runs", id = "run_2", doc = run2, expected_revision = 0 },
  { op = "delete", collection = "run_steps", id = "step_9", expected_revision = 2 },
} })
store.query({ collection = "run_steps", index = "by_run", equals = { "run_1" },
              range = { from = { 0 }, to = { 99 } }, order = "asc", limit = 50,
              max_bytes = 131072, cursor = nil })
-- { ok, value = { docs = { ... }, cursor = "..." | nil } }
store.watch({ collection = "run_steps", index = "by_session", equals = { sid } }, function(event) end)
```

- Grant: `{ surface = "plugin_db", scope = "<own package name>" }`.
- Every operation is an asynchronous host call (section 4).
- **Queries always use an index, an item limit, and a byte limit.** `equals`
  binds a prefix of the index fields; `range` bounds the next field. `limit`
  (items) is required and at most the Core range page (1000 items).
  `max_bytes` is optional; its default and its cap are the room the plugin's
  pool can give one call. The helper sets `max_result_bytes` from
  `max_bytes` before the call. The store stops a page at whichever limit it
  reaches first and returns `cursor` for the next page, so a page never
  exceeds the admitted reservation. A document larger than the remaining page
  room ends the page before it; a document larger than the whole reservation
  fails with `failed` / `detail = "document_exceeds_page"`. There is no
  filter expression, so the scanned rows equal the returned rows.
- **Atomic batches** validate every revision and index change, then commit
  as one Core batch (at most 256 operations, the existing Core ceiling).
  Index entries are written in the same transaction as the documents.
- **Change feeds.** After a commit, the store publishes one change event per
  affected document to matching watches of the same plugin: collection
  watches, or index-range watches whose range the old or the new index key
  touches. A watch whose undelivered events reach the per-plugin event
  capacity is reset: the Hub discards its queue and delivers
  `{ op = "resync" }`; the plugin re-queries.
- **Entity projection.** The manifest may bind a collection to a package
  entity family with a field allowlist
  (`"entities": { "project-pipelines.run": { "collection": "runs", "fields": [...] } }`).
  The Hub then serves snapshots from the collection and live changes from its
  change feed, with no plugin code. Plugins can still publish computed
  entities with `botster.entity_publish`.
- Layout on the existing redb keyed store: documents under
  `c/<collection>/d/<id>`, index entries under
  `c/<collection>/i/<index>/<order-preserving key>/<id>`.
- Quotas: `max_record_bytes` (64 KiB) per document and `max_plugin_bytes`
  (4 MiB) per package, including index entries. **PRODUCT DECISION (key
  quota)**: `max_plugin_keys` (1024) today counts every key. Index entries
  multiply keys; project-pipelines already sits near 1024. Option A
  (recommended): count documents only against `max_plugin_keys`, and bound
  index entries through `max_plugin_bytes`. Option B: count every key and
  raise the number.
- Later, as a separate opt-in capability: SQLite per plugin, bounded by a
  progress-handler work budget and a per-connection heap limit, off the
  owner. Not in this plan.

### 11.3 project-pipelines mapping

project-pipelines (modular repo, v0.4.0) is on the modular ABI today. It
stores 19 families as KV records under `v4/<family>/<id>` and reloads the
whole namespace on every tool call. The mapping:

| Collection | Indexes |
| --- | --- |
| `projects` | `by_name` (name) |
| `project_targets` | `by_project` (project_id) |
| `tickets` | `by_project` (project_id, position); `by_status` (status, position) |
| `ticket_dependencies` | `by_ticket` (ticket_id); `by_depends_on` (depends_on_id) |
| `pipeline_definitions` | `by_name` (name) |
| `runs` | `by_ticket` (ticket_id, created_seq); `by_status` (status) |
| `run_steps` | `by_run` (run_id, position); `by_session` (agent_session_uuid, status) |
| `gate_results` | `by_run_step` (run_step_id) |
| `reviews` | `by_run_step` (run_step_id) |
| `findings` | `by_review` (review_id); `by_status` (status) |
| `artifacts` | `by_run_step` (run_step_id) |
| `checklists` | `by_owner` (scope, owner_id) |
| `checklist_items` | `by_checklist` (checklist_id, position) |
| `questions` | `by_status` (status, created_seq); `by_run` (run_id) |
| `answers` | `by_question` (question_id) |
| `question_orchestrators` | `by_scope` (scope) |
| `pr_links` | `by_ticket` (ticket_id) |
| `events` | `by_seq` (seq) (the 256-entry audit ring) |
| `session_requests` | `by_status` (status); `by_run_step` (run_step_id) |
| `advance_requests` | `by_run` (run_id) |
| `meta` | none (id counters, entity sequences) |

`by_session` reproduces the legacy SQL index
`run_steps(agent_session_uuid, status)`. The CAS retry loop becomes one
`batch` with `expected_revision` per document. The entity families that
project-pipelines serves today become manifest entity projections.

## 12. Area 6: UI (USER DECISION: entities plus declarative views)

- Plugins publish typed entities (collection projections, or
  `botster.entity_publish` for computed ones) through the existing entity
  subscription pipeline.
- Plugins declare views in the manifest over entity families, from a fixed
  component set: `list`, `table`, `detail`, `form`, `actions`, and
  `status_badge`. A view names its entity family, its fields or columns, its
  empty state, and its actions. An action names a plugin `ui_action` handler
  and its input form.

```json
"views": [{
  "id": "project-pipelines.runs",
  "component": "table",
  "entity": "project-pipelines.run",
  "columns": [ { "field": "title" }, { "field": "status", "component": "status_badge" } ],
  "row_actions": [ { "id": "advance", "label": "Advance", "handler": "advance_run" } ],
  "empty": { "title": "No runs" }
}]
```

- Web and TUI both render from the same declaration. Plugins never run code
  in clients. The declaration schema lives in `botster-ui-contract` (a
  sibling crate in this repository); the Hub admits and projects it; the Web
  and TUI renderers are work in their repositories.
- `surface_route` render handlers and plugin-built `UiNode` trees are
  replaced by view declarations (cold cut). Navigation keeps targeting
  admitted views.
- Ownership of the replacement:
  - **This repository (plugin-platform writer):** the view declaration
    schema and its validation in `crates/botster-ui-contract`; manifest
    parsing and admission (every view names an admitted entity family of the
    same package, every field exists in the family's projection allowlist,
    every action names a registered `ui_action` handler); client projection
    of admitted views; action routing to the plugin handler with the
    action's validated input; removal of `surface_route` rendering.
  - **Web and TUI repositories (their writers, scheduled by the
    orchestrator):** rendering each component from the declaration over the
    existing entity subscription, and sending actions. This plan does not
    change other repositories.
- Live updates use the existing entity pipeline: a collection projection or
  `botster.entity_publish` changes an entity, and every client showing a
  view over that family receives the change. Views never re-render through
  plugin code.
- Later escape hatch (not now): sandboxed Web-only plugin bundles.

## 13. Area 7: Hub-brokered actions

### 13.1 Repo detection

```lua
botster.capabilities.repo.detect({ target_id = id })
-- { ok, value = { is_git = true, repo = "owner/name", branch = "main", head = "<sha>" } }
```

- Grant: `{ surface = "session_actions", scope = "repo_read" }`. Input is a
  target id or worktree id, never a path.

### 13.2 Command gates

**PRODUCT DECISION (gate source)**: Option A (recommended): a gate is a
declared session type with `lifecycle = "task"` and `interaction =
"noninteractive"`, from the repo or the package like any session type.
`botster.capabilities.gates.run({ session_type_id = id, worktree_id = wt })`
spawns it and returns `{ exit_code, duration_ms, output_tail }` when the
session exits. This reuses spawn admission, supervision, output capture, and
exit events. Option B: a separate gate registry with its own runner.

### 13.3 Worktree lifecycle

```lua
botster.capabilities.worktrees.ensure({ target_id = id, branch = "feature/x" })
botster.capabilities.worktrees.remove({ worktree_id = id })
```

- Grant: `{ surface = "session_actions", scope = "worktree_manage" }`.
- Mechanism: the existing managed-git path. `remove` refuses dirty worktrees
  and worktrees the plugin did not create.

### 13.4 Notifications

**PRODUCT DECISION (channel)**: Option A (recommended): client notices,
extending the existing manifest `events.notices`, through
`botster.capabilities.notify({ title = "...", body = "...", severity = "warning", session_id = id })`.
Option B: OS or mobile push. Option C: external channels through plugin HTTP.

### 13.5 Package status

```lua
botster.capabilities.packages.status({ name = "github" })
-- { ok, value = { installed, enabled, configured, capabilities = { ... } } }
```

- Grant: none beyond the package's own admission. Replaces
  `provider_dependencies.check`.

## 14. Slice 0: sandbox hardening

| Global | Decision | Reason |
| --- | --- | --- |
| `dofile`, `loadfile` | removed | read host files with daemon privileges |
| `print`, `warn` | removed | write daemon stdio; `botster.log` replaces them |
| `string.dump` | removed | produces bytecode |
| `load` | text only (`mode` must be nil or `"t"`) | binary chunks can break VM memory safety |
| `collectgarbage` | `"count"` only | other options stop or retune the collector that enforces the VM memory limit |
| `pcall`, `xpcall` | re-raise the shared budget error once the budget is exhausted | the budget hook raises an ordinary error that a protected loop could absorb forever |
| `setmetatable` | refuses `__gc` | existing guard |
| string metatable | protected | shared by every string in the VM |
| `os`, `io`, `package`, `require`, `debug` | absent | libraries not opened; cleared defensively |
| `assert`, `error`, `getmetatable`, `ipairs`, `next`, `pairs`, `rawequal`, `rawget`, `rawlen`, `rawset`, `select`, `tonumber`, `tostring`, `type`, `_G`, `_VERSION` | kept | VM-local only |

The process host adds an OS sandbox around the whole worker (section 15).

## 15. Process host contract (with the Core process-host writer)

Core builds the mechanism; the Hub builds the worker binary (Core worker
library plus the Hub Lua runtime; mlua stays out of Core) and chooses every
policy value.

Lifecycle:

1. Spawn. The parent sets rlimits and the process group before exec (values
   from the Hub). The worker starts with a minimal environment and no
   inherited descriptors except the IPC channel and the readiness pipe.
2. `Bootstrap{sandbox}`: the child library calls the Hub-supplied
   `apply_sandbox` hook (Seatbelt on macOS, Landlock plus seccomp on Linux)
   before any plugin code exists in the process.
3. `Ready`, then `Load{sources, config}` with the module set (section 7.4)
   and the host-call credits (section 5).
4. `Loaded{registration}`: the Hub validates the registration exactly as the
   thread host does today.

Messages: section 4.1. Deliveries into the plugin are ordinary `Invoke`s of
reserved handlers through `PluginWorkerEngine`; Core adds no separate result
or event message.

Engine API (Core premise revision 2, `4e6b0db`, section 5):
`engine.try_reserve_delivery_pool(plugin_key, slots, request_bytes,
completion_bytes_per_slot)` at load returns a `DeliveryPool` bound to the
current generation, with `accept_call(call_id, max_result_bytes)`,
`admit_result(call_id, request)`, and `release_call(call_id)`. On the thread
host, the Hub's in-process host port calls the same methods, so both hosts
share one accounting path. Log frames carry `dropped_since_last`.

Flow control: the standing delivery pool and its credits (section 5.1),
separate conserved credits for host-call request bodies and for log records
(section 5.2), and a dropped-record counter for logs. A host call without a
credit, or with an unknown or duplicate `call_id`, is a protocol violation:
Core kills the worker. Every delivery into the plugin, including a Hub
refusal, is an ordinary `Invoke` admitted from the pool, so it runs under
engine admission and deadline supervision.

Kill and crash: on process exit (the kqueue/pidfd exit watch, never a timer),
Core fails every in-flight `Invoke` of that generation with `WorkerCrashed`
or `WorkerKilled{Deadline | Budget | ProtocolViolation | Requested}`, drops
queued host calls, releases reservations, and emits `PluginProcessExited`.
The Hub resumes nothing in a dead generation; it releases the generation's
resources and applies its restart policy.

Deadline: the engine cancels the invocation, Core sends `Cancel`, and after a
Hub-supplied grace (a `// timer: deadline`) Core kills the process group.

**PRODUCT DECISIONS (process host policy)**: Core has no defaults; the Hub
supplies every number (Core premise section 11): startup deadline (spawn
through `Loaded`), shutdown deadline, cancel grace, `max_frame_bytes`,
ingress bytes, log credits (count and bytes), stderr tail bytes, the pool
(slots, request bytes, completion bytes per slot), the memory cap (enforced
on macOS by a counting global allocator in the Hub worker binary, a
mechanism the orchestrator accepted), the restart policy, and rlimits. Option A
(recommended): memory limit = the existing per-VM limit (16 MiB) plus a
measured runtime overhead; restart once per crash with a
`// timer: backoff`, then quarantine until the operator re-enables. Option B:
dedicated values.

The same messages run on the thread host, so the cutover plugins can start
there and move without API changes.

## 16. Implementation slices and acceptance

Every slice lands with a real daemon test that loads a real Lua fixture
package through `EnablePackageLocalPath` and drives it through daemon
requests. Tests wait on events (daemon responses, subscription frames,
session exit events), never on sleeps. Each guard has its own ablation: the
guard is removed and the named assertion must fail at that assertion, not at
setup.

| Slice | Content | Depends on | Acceptance cases (each with its ablation) |
| --- | --- | --- | --- |
| 0 | Sandbox (section 14) | none | Host-file `dofile`/`loadfile` fail; bytecode `load` fails; `string.dump`, `print` absent; `collectgarbage("collect")` refused; a `pcall` spin fails at the budget; text `load` works. Per-guard control VM probes. |
| 1 | Per-package grants (section 17 item 5); result convention for existing helpers; ABI doc rewrite | none | A package without `plugin_db` gets `capability_denied` while a granted sibling succeeds; the literal grant list is gone (ablation: restore it, the denial test fails). |
| 2 | `log`, `json`, `clock`, `require` | log limits decision | `require` of a sibling module works; a symlinked or `..` module fails the load; log records reach `get_plugin_logs` with package and level; a flooding plugin drops records and reports the count without blocking. |
| 3 | Suspendable handlers, delivery pool, credits, call ledger, delivery path, timers | reservation caps and timer marker decisions; event-driven writer's delivery path | An MCP tool that suspends on a timer returns its final value to the caller; a saturated plugin (all reservations held) gets `backpressured` while a sibling plugin's timer still fires and its tool still answers; `cancel()` stops a repeating timer (next fire never runs); reload resumes a suspended handler with `cancelled` and a stale-generation completion is dropped and counted; accounting returns to the baseline after completion, cancel, and reload; a host call completed before the issuing invocation's `suspended` result is published still answers the MCP caller (a test gate holds the issuing executor after the yield, before publication); a Hub refusal after the yield resumes the handler with the typed error; a pool-exhausted plugin's next call returns `backpressured` without suspending. |
| 4 | Storage collections (section 11) | key quota decision | Index query returns exactly the bound range with a limit; a batch with one stale revision changes nothing; a watch receives put and delete events after commit; a flooded watch receives `resync`; a collection projection serves a snapshot and a live change to an entity subscriber. |
| 4b | Declarative views (section 12): schema, admission, projection, action routing; `surface_route` removed | none (user decision made); Web/TUI renderer work scheduled by the orchestrator | A manifest view over an undeclared family, an unknown field, or an unregistered action fails enable; an admitted view is projected to a daemon client; an action reaches the plugin handler with validated input and returns its result; a collection change reaches a client subscribed to the view's family; ablation: removing the field-allowlist check lets the invalid manifest enable. |
| 5 | Sessions, messaging, caller identity (section 9) | cross-plugin control, message tools, caller auth decisions | A plugin cannot close a session it does not own without `:any`; post/receive round trip between two sessions; a forged caller is refused (option A). |
| 6 | HTTP, secrets, filesystem roots (section 8) | network policy decision | A non-granted origin is denied; a secret-bound header is sent only to its origin; a symlink swapped in during a read is refused; `..` is refused before I/O; watch overflow delivers one `overflow`. |
| 7 | MCP prompts and proxy; repo, gates, worktrees, notifications, package status | proxy, gate, channel decisions | Per the chosen options. |
| 8 | Process host integration (section 15) | Core process host; process policy decisions | A crashing plugin leaves siblings serving; a spinning plugin is killed at the deadline; the same fixtures pass on both hosts. |
| 9 | Ports: messaging, orchestrator, project-pipelines | slices 1 to 5, 4b, and 7; Web/TUI view renderers for project-pipelines | Live proofs of each plugin's tool surface; for project-pipelines, its views render in Web and TUI from the declarations, and a ticket change appears in both without a reload. |

## 17. Answers to review findings

### 17.1 Revision 1

1. **Admission accounting** (reviewer 1): section 5 now reserves one delivery
   slot and `max_result_bytes` per accepted call at acceptance, sizes the
   encoded envelope, transfers the charge atomically at admission, releases on
   every terminal outcome, and keeps every structure per plugin.
2. **Storage and secrets I/O** (reviewer 2): every store and secret
   operation is now an asynchronous host call (sections 8.3 and 11). Local
   helpers are limited to in-VM work (section 4.4).
3. **Filesystem confinement** (reviewer 3): section 8.2 specifies the path
   grammar, descriptor-based `openat` walks with `O_NOFOLLOW`, listing and
   read bounds, watcher state, and overflow recovery.
4. **Secret references** (reviewer 4): `use` and `read` are separate grants;
   each secret field binds its origins; redirects are not followed
   (section 8.3).
5. **Calling convention** (reviewer 5): section 6 defines one convention;
   every example follows it; `require` is the single documented exception.
   `events.on` becomes `botster.events.on({ owner, name }, fn)`.
   Per-package grants: the manifest declares; enable-time admission records
   `admitted_capabilities`; every call checks the calling package's admitted
   set; `default_hub_capability_grants` and its literal names are deleted;
   every handler carries its `required_capability`, so Core also checks.
6. **Decision dependencies and timer fairness** (reviewer 6): section 16
   lists each slice's decision dependencies; section 7.5 defines fair expiry
   service and constant-time missed-expiry handling.
7. **Acceptance and ablations** (reviewer 7): section 16 lists decisive
   cases per slice with event-based completion signals and per-guard
   ablations.
8. **Found in revision**: callbacks alone cannot return an asynchronous
   result to a request-response handler, because the handler has already
   returned. Section 4 adds suspendable handlers.

### 17.2 Revision 2

1. **Early completion and the suspension handshake**: section 4.2 orders the
   steps (pool debit, call table, send, yield), shows why a resume cannot
   precede the yield inside the VM, and adds the Hub call ledger for either
   order of `suspended` and final results, duplicate and unknown completions,
   cancellation tombstones, and sequential calls. A Hub refusal after the
   yield uses the slot debited before the yield.
2. **Core capacity ownership**: section 5.1 replaces per-call Hub
   reservations with a standing engine delivery pool (agreed with the Core
   process-host writer): the engine holds the queue slots, request bytes, and
   completion bytes; admission from the pool cannot refuse for capacity;
   ordinary Background work cannot use pool capacity; cancellation keeps its
   slot until its delivery transfers or the generation retires; every call
   has exactly one terminal. Section 5.2 charges producer buffers, request
   bodies, serialization overlap, and the shared callback account.
3. **Stream lifetimes**: section 4.3 separates the setup call, the armed
   resource, and the pending event, bounds each, charges recursive watcher
   directories, and states that `cancel()` stops new event invocations but
   does not cancel an already suspended callback invocation.
4. **Bounds before allocation**: module staging reserves before reading
   (7.4); HTTP derives a pre-admission ceiling from fixed header limits and
   `max_bytes` and fails rather than truncates (8.1); queries page by items
   and bytes inside the admitted reservation (11.2).
5. **UI replacement**: section 12 assigns schema, admission, projection,
   action routing, and `surface_route` removal to this repository and
   rendering to the Web and TUI writers; section 16 adds slice 4b with
   acceptance and ablation, and slice 9 requires the rendered views.
6. **Filesystem authority**: section 8.2 uses `openat2` with
   `RESOLVE_BENEATH` on Linux and `O_NOFOLLOW_ANY` plus a post-open
   `F_GETPATH` and root-identity check on macOS, adds the watch identity
   rule, and specifies an adversarial rename test with its ablation.

## 18. Product decisions (summary)

1. Delivery pool size (5.1).
2. Log limits (7.1).
3. Timer marker (7.5).
4. Network policy (8.1).
5. Cross-plugin session control (9.2).
6. Message tools ownership (9.3).
7. Caller authentication (9.4).
8. Terminal-output hooks (9.5).
9. Federation (9.6).
10. MCP proxy (10).
11. Storage key quota (11.2).
12. Gate source (13.2).
13. Notification channel (13.4).
14. Process host policy: kill grace, restart policy, OS limits (15).
