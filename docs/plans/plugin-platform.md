# Plugin platform design

Status: revision 8 for review (2026-09-26). Owner: plugin-platform Hub writer.
Revisions 1 (`4e6f5aa2`), 2 (`0bc5c83e`), 3 (`affb2368`), 4 (`781ebb57`),
5 (`8e02f13e`), 6 (`dd3b5512`), and 7 (`c32f5c18`) were rejected;
section 17 maps every finding to its answer.

Scope: the Lua-facing platform that first-party plugins need, starting with
the cutover plugins messaging, orchestrator, and project-pipelines.
**USER DECISION** records a choice the user made. On 2026-09-26 the user
approved every product decision of revision 3 as recommended (Option A);
the alternatives stay in the text as the record of what was considered. Everything else is a design proposal for
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
   decisions for the user.
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

**Ordering at the Hub (the chain ledger).** Outside the VM, the Hub can see
publications in either order: on the thread host, one executor can publish
the final result of a resume before the other executor publishes the
`suspended` result of the invocation that issued the call. The ledger
therefore never depends on order. It counts.

A *chain* is one request-response invocation (the root) plus every resume
invocation of its handler. Every resume carries the root's
`root_request_id`, which the Hub copies from its call table when it builds
the resume. The Hub keeps one ledger entry per chain:

- `open_invocations`: incremented when the Hub admits an invocation of the
  chain (the root or a resume); decremented when that invocation publishes
  its result, whatever the result is (`suspended`, final, or failed).
- `open_calls`: incremented when the Hub accepts a host call issued by the
  chain; decremented at the call's single terminal (section 5.1).
- `answered`: false until the waiter receives exactly one answer.

Events and their only effects:

| Event | Effect |
| --- | --- |
| an invocation of the chain publishes its marker (`suspended` or `done`) | `open_invocations -= 1` |
| the chain's Reply arrives | if not `answered`: answer the waiter with the Reply body, set `answered`; in every case call `release_call` and mark the Reply released |
| an invocation of the chain fails (handler error, worker stopped) | `open_invocations -= 1`; if not `answered`: answer with the failure, set `answered` |
| deadline expiry or caller cancellation | if not `answered`: answer `timed_out` or `cancelled`, set `answered`; cancel the chain's open calls (section 4.2 rule 4) |
| a host call of the chain reaches its terminal | `open_calls -= 1` |

**Where the result travels.** Invocations publish only markers (section 5.0).
A request-response handler's return value, whether it suspended or not, is
sent by the runtime as a Reply (a credited message, section 15) before the
invocation publishes its `done` marker. The Hub may see the Reply and the
markers in any order; the counters below do not depend on it.

**Chain slots and reply credits are two things.**

- A *chain slot* is a Hub reservation: each plugin has 2. The Hub takes one
  when it admits a request-response root and holds it in the ledger entry
  until the entry is removed. A 3rd concurrent root is refused with
  `backpressured`.
- A *reply credit* is Core's ingress credit: each plugin has 2 (1 MiB body
  each). The runtime consumes one only when it actually sends a Reply; Core
  returns it at `release_call`. Because an entry, and so its slot, is removed
  only after its Reply (if any) has been released, at most one Reply per open
  slot can be in flight, and the 2 credits always suffice.
- Every marker carries `reply_sent: true | false`. The runtime sets it; the
  Hub uses it to know whether a Reply is still owed to the release path.

Terminal protocol for each case (each gives exactly one answer and releases
everything):

| Case | Reply | Waiter answer | Release |
| --- | --- | --- | --- |
| Handler returns a result within 1 MiB | sent (`reply_sent = true`) | the Reply body | `release_call` on the Reply; entry removed when the marker has also drained |
| Handler raises an error | not sent (`reply_sent = false`) | the failure | entry removed when the failure marker drains; no credit was consumed |
| Handler returns more than 1 MiB | not sent; the runtime checks the size before sending | `failed` / `response_too_large` | as for an error |
| Deadline or caller cancellation before the Reply | the handler may still send one later (`reply_sent = true` on its marker) | `timed_out` or `cancelled`, at once | a late Reply finds the entry already answered: the Hub calls `release_call` and discards it; the entry (a tombstone) is removed when the marker drains and, if `reply_sent`, the Reply has been released |
| A Reply for an unknown or removed entry | (protocol bug) | none | `release_call`, discard, count; on the process host Core treats an unknown `call_id` as a protocol violation |
| Generation retirement (unload, reload, kill, crash) | any in flight is dropped by Core with the generation | every unanswered waiter gets `cancelled` (unload, reload) or `unavailable` (kill, crash) | Core releases the generation's credits; the Hub drops the generation's entries and slots |

Real-daemon proofs (slice 3): two failing handlers followed by a successful
call on the same plugin; a cancellation before the Reply racing with a late
Reply; an oversized return; generation retirement with a suspended chain.
Each asserts exactly one answer, both slots free, and both credits returned.

The entry is removed when `answered` is true, both counters are zero, and a
Reply that a marker reported as sent has been released. Removing the entry
frees the chain slot.
Each event changes a disjoint field, and "answer" happens at most once, so
every interleaving of `suspended`, final, deadline, and late publications
gives the same outcome: the first of final, failure, or deadline answers,
and later publications only drain the counters. A late final result after a
deadline is counted and discarded. Resumes of Background handlers use the
same counters without a waiter. The request-response admission bound limits
the number of entries.

Test gates for slice 3: a gate that holds the issuing executor after the
yield and before publication proves the "final before `suspended`" order; a
gate that holds the resume proves the other order; a handler with three
sequential calls proves repeated suspensions; a deadline between two
suspensions proves the late-final path. Each case asserts one answer and a
removed entry.

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
   its chain. The chain's Reply answers the waiter (section 4.2, "Where the
   result travels"); every `InvocationResult` is only a marker. The original
   deadline covers the whole handler.
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
  -- event.value = { collection = "questions", changed = true }; re-query the indexes
end)
watch.value:cancel()
```

A stream has three separate lifetimes, each with its own bound:

| Lifetime | What it holds | Charged to | Released when |
| --- | --- | --- | --- |
| Setup call | the call that arms the stream | one pool unit, like any host call | the setup result's completion is drained (section 5.1) |
| Armed resource | the timer, watch, or feed, plus its one pending-event record | one unit of the per-plugin capability operation capacity (128), plus the record's fixed size from the plugin's share (section 5.2); a recursive inotify watch charges one unit per watched directory | `cancel()`, unload, or generation retirement |
| Delivered event | one event invocation | ordinary Background capacity of the plugin's Core queue (never the pool) | Core drains the invocation's completion |

- **Stream events never use the delivery pool.** The pool belongs only to
  the plugin's own host calls (section 5.1), so a stream event and a host
  call can never spend the same capacity. Stream events are admitted with
  ordinary `try_admit` into the plugin's Background capacity.
- **One pending-event record per stream, charged at arm time.** Arming a
  stream reserves one fixed-size record before the resource exists: a timer
  record holds `sequence` and `missed`; a watch record holds a change kind,
  an `overflow` flag, and one path buffer of the OS path maximum (`PATH_MAX`,
  an OS fact, not a policy number); a store-watch record holds only its
  collection and a `changed` flag (section 11.2). New occurrences fold into
  the record (timers count `missed`; a watch that sees a second path sets
  `overflow`; a store watch stays `changed`). No occurrence allocates.
- **Capacity wake.** When `try_admit` returns `Backpressured`, the record
  stays pending. The next completion that the plugin's own worker publishes
  (the existing `PluginCompletionPublished` notifier) frees Background
  capacity and re-arms the delivery slice for that plugin, which admits its
  pending records. No new source event and no timer is needed.
- **`cancel()`** removes the callback inside the VM at once, so no new event
  invocation of that stream runs. The Hub then disarms the resource and
  frees its record. A callback invocation that is already suspended is an
  independent handler: it keeps its own calls and completes normally. Plugin
  code that must stop in-flight work checks its own state after each resume.

### 4.4 Local helpers

`botster.json`, `botster.clock`, and `botster.log` run inside the VM and
send no host call (`log` sends a fire-and-forget `Log` message). They return
results directly and never suspend.

### 4.5 Publication cost (REQUIREMENT, orchestrator 2026-09-27)

Today one `botster.entity_publish` makes about 8 cross-thread wakes, one
after another:
- Lua to owner (`EntityPublishProgress`);
- owner to Host and back for Admit, Advance, and Reply (6 wakes);
- Host to Lua (an mpsc reply).

Only one publication is in flight at a time, so the cost is scheduling
latency, not CPU. A handler that publishes 512 times does not fit the 1 s
event deadline on a loaded host (evidence:
`botster-evidence/core-pin-549b3f6-20260926/core-per-publication-20260926.md`).

This is wrong for the platform. A publication therefore is NOT a host call
(section 4.2): it does not suspend the handler and has no per-publication
result round trip.

**What `entity_publish` does.** `botster.entity_publish(spec)` runs in the VM:
1. It validates the frame locally: shape, the plugin's declared entity family,
   and the size bound.
2. It takes one unit of publication credit (see "Credit" below).
3. It appends a `Publish` message to the plugin's ordered outbound stream,
   the same stream that carries `HostCall`, `Log`, and `InvocationResult`
   (section 4.1). On the process host this stream is the single IPC writer,
   which keeps push order. On the thread host it is a per-plugin FIFO.
4. It returns at once, without suspending:
   - `{ ok = true, value = { seq = <n> } }`, where `seq` is the plugin's
     local publication sequence;
   - `invalid_request` or `capability_denied` for a local validation failure;
   - `backpressured` when no credit is left. Nothing is sent in that case.

**Order.** The Hub applies one plugin's publications in stream order, one at
a time, through the existing entity pipeline. Different plugins do not wait
for each other.

**Where apply errors go.** A publication can still fail when the Hub applies
it (for example a stale sequence). The Hub does not report that error back to
the Lua call, which has already returned. It records the first apply failure
against the invocation that sent the publication (its `request_id`). The
invocation's final result carries it (see "Barrier" below). Every apply
failure is also counted, and logged to the plugin's log ring (section 7.1).

**Barrier: the handler's final result.** The Hub routes an invocation's final
result (`done`, a handler error, or a timeout) only after it has applied every
publication that the invocation sent before that result.
- On the process host this needs no extra round trip: the publications come
  before the `InvocationResult` in the same ordered stream.
- On the thread host the Hub keeps, per invocation, the number of
  publications sent and the number applied. It holds the completion in the
  drain until the two are equal. This is an O(1) check at routing, with no
  wait and no poll.
- If any publication of the invocation failed to apply, the Hub turns a `done`
  result into `{ ok = false, error = { kind = "failed",
  detail = { publication_seq, reason } } }`. The Hub routes the handler's own
  error or timeout unchanged; the publication failure is added to `detail`.

**Barrier inside a handler.** `botster.entity_flush()` is one host call
(section 4.2). It suspends once and resumes when every publication the plugin
sent before it has been applied. It returns `{ ok = true, value = { applied
= <n> } }`, or the first apply failure since the previous flush. A handler
that needs to read back its own publications calls it once, not once per
publication.

**Credit.** Publication credit bounds the unapplied publications of one
plugin. A unit returns when the Hub applies the publication, or discards it.
- Existing bounds stay: the global `PUBLICATION_CAPACITY` (256 retained
  publications, `lua_runtime/entity_publish.rs`) and the entity publish bytes
  counted in the callback account (section 5.0).
- A per-plugin split of that capacity would be a new number. It is an open
  decision for the user (section 18). Until then, every plugin draws from the
  existing global bound, and exhaustion returns `backpressured`.

**Cancellation.** Cancelling an invocation does not recall publications it
already sent. The Hub still applies them in order, and then routes the
`cancelled` result behind them.

**Generation cleanup.** On unload or reload, the Hub discards the old
generation's unapplied publications, counts them, and returns their credit.
A stale-generation `Publish` that arrives later is dropped by the same
generation check as a stale completion (section 4.2).

**Cost.** A handler that publishes N times makes N `Publish` sends, 0
suspensions, and 0 per-publication round trips, plus its one final result
(and one round trip for each `entity_flush` it calls).

**Deterministic test (slice 3 acceptance).** The test counts the whole path
for one handler that publishes 512 times and then returns. Test-only counters
record:
- Lua suspensions and resumptions of the handler;
- host calls and their result deliveries;
- owner wakes and Host executor handoffs caused by the handler.

Assertions:
- 0 suspensions and 0 host calls;
- Host handoffs and owner wakes grow by at most a constant that does not
  depend on N (compared between N = 1 and N = 512);
- `family_seq == 512`, and the final result is routed after the last
  publication is applied.

A second case calls `entity_flush` once and asserts exactly 1 suspension.
Ablation: route each publication through a host call. The test then counts
512 suspensions and fails at that assertion. The test measures no time.

## 5. Delivery, reservations, and accounting

This section is the contract with the event-driven writer (Hub delivery
path) and the Core process-host writer (engine and IPC; premise
`docs/plans/plugin-process-host.md` on `origin/delivery/plugin-process-host-20260926`).

### 5.0 The complete allocation (USER DECISION, 2026-09-26)

Sized for the 8-plugin maximum (128 MiB total / 16 MiB per-VM Lua limits; a
known ceiling to revisit with the process-per-plugin host). "Existing" means
an unchanged number from the current code; "approved" means a number the user
approved for this plan.

| Resource (owner) | Scope | Allocation | Numbers |
| --- | --- | --- | --- |
| Background queue (Core engine) | per plugin, existing 256 slots / 1 MiB | host-call pool | 128 slots / 512 KiB (approved) |
| | | ordinary Background work (stream events, package events, session-family frames) | 128 slots / 512 KiB (approved) |
| Request-response queue (Core engine) | per plugin, existing | request-response roots; at most 2 open chains per plugin (a 3rd call is `backpressured`) | existing queue numbers; 2 chains (approved) |
| Completion store entries (Core engine) | global, per-plugin share reserved at load | 128 host-call results + 2 chain roots + 32 ordinary = 162 per plugin | global 1296 entries (approved; was 256) |
| Completion reservation bytes (Core engine) | global | 4 KiB per entry (the existing Background allowance) | 1296 x 4 KiB = 5.2 MiB (approved; was 8 MiB) |
| Completion queue bytes (Core engine) | global | markers only | 32 MiB (existing, unchanged) |
| Reply credits (Core ingress on the process host; Hub callback account) | per plugin | the final result of a request-response chain | 2 x 1 MiB (approved) |
| Request-body credit (Core ingress; then the Hub retained charge) | per plugin | host-call request bodies until backend disposal | 512 KiB (approved) |
| Result producer and encoding (Hub callback account) | per plugin | producer buffer plus encoded bytes during overlap | 2 x 512 KiB (derived from the pool) |
| Stream records and ledger charges (Hub callback account) | per plugin | one record per armed stream; one charge per open chain | about 160 KiB (derived: 128 records of fixed size plus `PATH_MAX`) |
| Lifecycle maps (Hub callback account) | per plugin | the plugin's rows in the Hub lifecycle maps (`descriptors`, `event_handlers`, `loaded`), sized from the admitted manifest and registration at load and charged in the retained share. Unfunded today: `plugin_worker_metadata_bytes` has only a test caller (`prepare_worker_resources`). | derived from the manifest (no new number) |
| Log ring (Hub callback account) | per plugin, charged as records arrive (not reserved at load) | `botster.log` records (section 7.1) | up to 256 records / 512 KiB (approved); 8 x 512 KiB = 4 MiB at most, taken from the remainder below |
| Module staging (Hub callback account) | global, temporary | ONE 16 MiB staging permit; package loads stage one at a time and release the permit when the VM holds the text | 16 MiB (approved) |
| Hub callback account total | global | 8 x about 3.7 MiB retained shares + 16 MiB staging = about 45.6 MiB; the rest (about 18 MiB) stays for existing users (for example up to 8 MiB of entity publishes in flight) and the log rings (at most 4 MiB) | 64 MiB (existing, unchanged) |

Load reserves the plugin's whole retained share (queue split, completion
entries and bytes, reply and request credits, callback share) in one
all-or-nothing step, then waits for the staging permit. If any part cannot be
reserved, the load fails with `quota_exceeded` and nothing stays reserved.

**Reload headroom (orchestrator decision, 2026-09-27).** Each loaded plugin
holds its full 16 MiB per-VM charge against the 128 MiB total from load to
unload (`LuaPluginRuntime::new_named` calls `reserve_vm`). A reload builds the
new VM before it drops the old one, so for a moment it holds two charges.
- The load maximum stays 8.
- The steady state that keeps reload working is 7 plugins plus 1 reload slot.
  The Hub does not reserve that slot.
- A reload while 8 plugins are loaded is refused with a typed error that names
  the cause: the VM budget is full during reload; unload another plugin or
  retry.

Required proofs (slice 3):
- 8 plugins load, and an ordinary event is still delivered while all 8 hold
  their full shares.
- The early-final ordering test (section 4.2).
- A 3rd concurrent request-response call to one plugin is `backpressured`
  while a sibling answers.
- A 9th plugin's load is refused with `quota_exceeded`.
- A reload while 8 are loaded gets the typed reload refusal. A reload while 7
  are loaded succeeds.
- The lifecycle-map charge is taken at load and returned at unload (ablation:
  skip the charge, and the accounting assertion fails).

### 5.1 The delivery pool (host calls only)

1. **Engine reservation.** At load, the Hub reserves a standing delivery
   pool from the Core engine for the plugin's generation
   (`try_reserve_delivery_pool(plugin_key, slots, request_bytes,
   completion_bytes_per_slot)`). The engine takes it out of the plugin's own
   Background capacity. Invariant: plugin Background capacity = delivery pool
   + ordinary Background capacity. Only host-call results use the pool; stream
   events and package events use ordinary capacity (section 4.3).
2. **Admission from the pool** (`admit_result`) never refuses for capacity.
3. **Credits are pool room, owned by the plugin alone.** The plugin side
   mirrors the pool's free room exactly. A host call debits one slot and its
   `max_result_bytes` before it is sent (section 4.2 step 1). If the room
   cannot fit the call, the call returns `backpressured` at once and does not
   suspend. On the process host, a `HostCall` beyond its credit, or with an
   unknown or duplicate `call_id`, is a protocol violation that kills the
   worker.
4. **Guaranteed delivery.** Every outcome of an accepted call (success, Hub
   refusal, timeout, cancellation) has pool capacity for its `Invoke`.
5. **One terminal and one release point per call.** The Hub gives every
   accepted call exactly one terminal: a result through `admit_result`, or
   `release_call(call_id)`. The pool unit returns to the plugin as
   `Credit{call_id, bytes}` at exactly one point: when Core drains the
   completion of the result invocation (or that invocation fails or is
   cancelled), or at `release_call`. A cancelled call keeps its unit until
   its `cancelled` result's completion is drained or the generation retires.
6. **USER DECISION (pool size, amended 2026-09-26, Option A):** the plugin's
   existing Background queue byte capacity (1 MiB) is split. The host-call
   pool gets 512 KiB of request and result bytes and 128 slots (the
   capability operation capacity); ordinary Background work (stream events,
   package events, session-family frames) keeps a guaranteed 512 KiB. The
   total does not change. A single host-call result larger than 512 KiB fails
   with `{ kind = "failed", detail = "response_too_large" }`; large downloads
   need a future streaming or file capability, not in this plan.
7. **Completion store holds markers only** (USER DECISION, 2026-09-26; it
   supersedes the earlier 1040-entry decision). Every invocation, whether a
   root, a resume, or ordinary Background work, completes with a marker of at
   most 4 KiB. Result payloads travel as credited messages (section 5.2),
   never in the completion store. The full allocation is in section 5.0.

### 5.2 Memory outside the pool, and the per-plugin share

| Allocation | Charged to | Held until |
| --- | --- | --- |
| `HostCall` frame bytes at parent ingress (process host) | Core's ingress credit, debited by the child before send | the Hub dequeues the call (Core contract, unchanged) |
| Decoded request in the Hub, until the backend is done with it | a retained backend charge from the plugin share, sized by the existing bounded JSON accounting walker (representation overhead included) | the backend finishes, fails, or cancels the call, or the generation retires |
| Producer buffer (HTTP body, file read, query page) | the call's result allowance inside the plugin share, before each growth step | encoding finishes |
| Encoded completion | the call's pool unit | the result's completion is drained |
| Stream pending-event records | the plugin share, at arm time | the stream is disarmed |
| Chain ledger entries (section 4.2) | a fixed ledger charge from the plugin share, at root admission | the entry is removed |
| Module staging (section 7.4) | the one global 16 MiB staging permit, never a plugin share | the VM's own accounting holds the text; then the next load may stage |
| Final request-response result (a Reply, section 4.2) | one of the plugin's 2 reply credits (1 MiB each): Core ingress on the process host, then the Hub callback share | the Hub has answered the waiter |

- **Two credits, two lifetimes.** Core's ingress credit covers the frame
  while it waits in the parent's ingress queue and returns when the Hub
  dequeues the call (Core contract). At that dequeue, the Hub takes a
  retained backend charge from the plugin's share **before** it acknowledges
  the dequeue, so the bytes are always charged to one of the two. If the
  share cannot cover the decoded request, the Hub answers the call with a
  `backpressured` result through `admit_result` (its pool unit guarantees the
  delivery) and decodes nothing. The retained charge has one release point:
  the backend's terminal for the call (success, failure, cancellation) or
  generation retirement. So repeated calls cannot accumulate decoded bodies
  beyond the share.
- **Serialization overlap.** While a completion is encoded, its producer
  buffer and its encoded bytes both exist, so the result part of the share is
  twice the pool bytes.
- **Per-plugin share, reserved at load.** Each plugin's share is the sum of
  its bounds: twice the pool bytes, the request credit bytes, its stream
  records (at most the operation capacity times the largest record), its
  ledger charges (at most its request-response admission bound times the
  fixed ledger charge), and its reply credits. Module staging is not part of
  the share; it uses the global staging permit. At load, the Hub reserves the
  whole share from the Hub-wide callback account. A plugin that cannot get
  its share fails to load with `quota_exceeded`. Because every plugin's share
  is reserved up front, one plugin can never take a sibling's share; the
  Hub-wide account bound alone would not guarantee that.
- **Chains are bounded.** Every live chain holds either an open invocation
  (Core queue bounded) or an open host call (a pool unit), plus its ledger
  charge. Background chains use the same counters and the same charge.

### 5.3 Owner delivery

Host-call completions are published to a per-plugin pending queue, and the
producer calls an owner-installed notifier (one control message, one
maintenance wake). The owner's delivery slice rotates across plugins under
the existing turn budget (items, bytes, time). It admits host-call results
from the plugin's pool (never refused for capacity) and stream records with
ordinary `try_admit` (parked on `Backpressured` until the capacity wake of
section 4.3).

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
- **USER DECISION (log limits), Option A approved 2026-09-26**: record size, rate, queue size, and ring
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
- **USER DECISION (timer marker), Option A approved 2026-09-26**: the rewrite rules have no category
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
- **One total response allowance.** `max_bytes` is required and is the
  allowance for the whole encoded response: status, every header name and
  value, the body, and the result envelope. The helper sets
  `max_result_bytes = max_bytes`; it must fit the free pool room (at most
  512 KiB), or the call returns `backpressured` at once. There is no default,
  so no new number exists. The transport counts encoded bytes as it reads
  the status line, each header, and each body chunk into a producer buffer
  charged per growth step (section 5.2), and fails the call with `failed` /
  `detail = "response_too_large"` the moment the running total passes
  `max_bytes`; it never truncates. The Core header limits (64 headers, 128
  name bytes, 8192 value bytes) still apply per header, but they are not
  reserved up front.
- **Slice 6 proofs.** A small response under its allowance succeeds; a
  response whose headers alone pass the allowance fails with
  `response_too_large`; a response whose body passes it fails the same way.
- **USER DECISION (network policy), Option A approved 2026-09-26**: today the Hub allows loopback only,
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
- By default a plugin acts only on sessions it spawned. **USER DECISION (cross-plugin control), Option A approved 2026-09-26**: Option A (recommended): a `:any` scope suffix
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
- **USER DECISION (message tools), Option A approved 2026-09-26**: Option A (recommended): the
  messaging plugin owns the MCP tool surface, label lookup, envelope format,
  and doorbell text, and the Rust `NativeHubToolProvider` message tools are
  deleted. Option B: keep the native tools; ship no messaging plugin.

### 9.4 Caller identity

Handlers receive `request.caller = { session_id = ... }` for MCP calls.
Today `mcp-serve` reads the caller from `BOTSTER_SESSION_UUID`, which any
local process can set. **USER DECISION (caller authentication), Option A approved 2026-09-26**:
Option A (recommended): the Hub injects a per-session caller token into the
session context at spawn; `mcp-serve` presents it; the Hub maps it to the
session. Option B: treat the variable as advisory and document the gap.

### 9.5 Lifecycle and terminal output

- Lifecycle: the existing session-family stream (snapshot plus deltas),
  subscribed with `botster.events.on`. Terminal deltas gain `exit_code`.
- **USER DECISION (output hooks), Option A approved 2026-09-26**: the Hub must not relay terminal
  bytes. Option A (recommended): semantic terminal events from Core (bell,
  OSC 9/777 notifications, title change, idle/active) in the session-family
  stream, plus `read_screen` on demand. Option B: a sampled raw-output
  stream, which puts the Hub on the byte path.

### 9.6 Multi-hub

**USER DECISION (federation), Option A approved 2026-09-26**: Option A (recommended for cutover): local
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
- **USER DECISION (proxy), Option A approved 2026-09-26**: Option A (recommended): declarative
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
- **Store watches are bounded resync notifications.** A watch on a
  collection delivers `{ collection, changed = true }` after one or more
  commits touched the collection; further commits fold into the one pending
  record (section 4.3). The plugin then re-queries the indexes it cares
  about. There is no durable change log, no cursor, and no tombstone, so a
  watch holds no store data and costs nothing after it is cancelled. Plugins
  that need the exact set of changes keep their own `updated_seq` field and
  an index on it.
- **Entity projection.** The manifest may bind a collection to a package
  entity family with a field allowlist
  (`"entities": { "project-pipelines.run": { "collection": "runs", "fields": [...] } }`).
  The Hub then serves snapshots from the collection and live changes from
  each commit's own diff, which the commit path hands to the projection
  synchronously, with no plugin code and no change log. Plugins can still publish computed
  entities with `botster.entity_publish`.
- Layout on the existing redb keyed store: documents under
  `c/<collection>/d/<id>`, index entries under
  `c/<collection>/i/<index>/<order-preserving key>/<id>`.
- Quotas: `max_record_bytes` (64 KiB) per document and `max_plugin_bytes`
  (4 MiB) per package, including index entries. **USER DECISION (key quota), Option A approved 2026-09-26**: `max_plugin_keys` (1024) today counts every key. Index entries
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

**USER DECISION (gate source), Option A approved 2026-09-26**: Option A (recommended): a gate is a
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

**USER DECISION (channel), Option A approved 2026-09-26**: Option A (recommended): client notices,
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
share one accounting path.

The completion store carries markers only (section 5.0). A request-response
chain's final result is a **Reply**: a fire-and-forget `HostCall` kind with
its own conserved ingress credit class (2 credits of 1 MiB body per plugin),
returned at `release_call`; the Hub's terminal for a Reply is `release_call`,
with no result `Invoke`. Core validates at spawn that `max_frame_bytes` is at
least the Reply body allowance plus the envelope overhead (`InvalidConfig`
otherwise). Core premise section 5.1 at `e54ce21` records the same contract.
The pool and the ordinary completion entries are reserved in one atomic
multi-spec reservation at load. Log frames carry `dropped_since_last`.

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

**USER DECISION (process host policy), 2026-09-26**: memory cap = the per-VM
limit (16 MiB) plus a measured runtime overhead; restart once after a crash
with a `// timer: backoff`, then quarantine until the operator re-enables;
every other number below is measured in slice 8 and proposed to the user
before use. Core has no defaults; the Hub
supplies every number (Core premise section 11): startup deadline (spawn
through `Loaded`), shutdown deadline, cancel grace, `max_frame_bytes`,
ingress bytes, log credits (count and bytes), stderr tail bytes, the pool
(slots, request bytes, completion bytes per slot), the memory cap (enforced
on macOS by a counting global allocator in the Hub worker binary, a
mechanism the orchestrator accepted), the restart policy, and rlimits. The
pool size is already decided (section 5.1), so the pool is not measured
first.

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
| 3 | Suspendable handlers, delivery pool, credits, call ledger, delivery path, timers | reservation caps and timer marker decisions; event-driven writer's delivery path | An MCP tool that suspends on a timer returns its final value to the caller; a saturated plugin (all reservations held) gets `backpressured` while a sibling plugin's timer still fires and its tool still answers; `cancel()` stops a repeating timer (next fire never runs); reload resumes a suspended handler with `cancelled` and a stale-generation completion is dropped and counted; accounting returns to the baseline after completion, cancel, and reload; a host call completed before the issuing invocation's `suspended` result is published still answers the MCP caller (a test gate holds the issuing executor after the yield, before publication); a Hub refusal after the yield resumes the handler with the typed error; a pool-exhausted plugin's next call returns `backpressured` without suspending; a timer fires into an otherwise idle plugin while its pool is fully free, and again while every pool slot is in use (the ordinary 512 KiB carries both); a host-call result above 512 KiB fails with `response_too_large`; 8 plugins load and each uses its full completion share concurrently; one plugin with both request-response chains suspended does not delay a sibling's MCP call; its 3rd concurrent call is `backpressured`; a 9th plugin's load is refused with `quota_exceeded`; a timer record parked on `Backpressured` is delivered after the plugin's next completion, with no new timer fire (ablation: remove the completion-notifier re-arm, the record stays parked); a plugin whose share cannot be reserved fails to load with `quota_exceeded` while a loaded sibling keeps answering; decoded request bytes stay charged until the backend disposes them (ablation: return the credit at dequeue, the accounting assertion fails); a handler that publishes 512 entities suspends 0 times, makes 0 host calls, and its Host handoffs and owner wakes grow by at most a constant from N = 1 to N = 512; its final result is routed after its last publication applies; one `entity_flush` suspends exactly once (section 4.5; ablation: route each publication through a host call, 512 suspensions are counted and the assertion fails). |
| 4 | Storage collections (section 11) | key quota decision | Index query returns exactly the bound range with a limit; a batch with one stale revision changes nothing; two watches on one collection each receive one notification for a burst of commits and one batch touching many documents; cancelling one watch leaves the other notified (ablation: remove folding, the second commit queues a second event); a collection projection delivers a delete from the commit diff to an entity subscriber; a collection projection serves a snapshot and a live change to an entity subscriber. |
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

### 17.3 Revision 3

1. **Ledger transitions**: section 4.2 replaces the state table with
   order-free counters (`open_invocations`, `open_calls`, `answered`); every
   publication, including a late one after a deadline, only drains counters;
   test gates cover both publication orders, sequential suspensions, and the
   late-final path.
2. **Event-credit ownership**: the pool belongs to host calls only; stream
   events use ordinary Background capacity with a capacity wake
   (sections 4.3, 5.1, 5.3). The Core premise states the same.
3. **Retained event charges and one release point**: each armed stream
   reserves one fixed-size record at arm time, folds occurrences into it, and
   is delivered through ordinary admission; change feeds carry only a
   sequence and the plugin pulls changes (sections 4.3, 11.2). A pool unit
   has one release point: the drain of its result's completion (5.1).
4. **Request memory**: the request credit holds decoded requests until the
   backend disposes them; the per-plugin share (results twice, requests,
   stream records, ledger charges, staging) is reserved from the Hub-wide
   account at load, which guarantees sibling shares (5.2). Ledger entries
   carry a retained charge and are bounded per plugin.
5. **User decisions**: every product decision is now a recorded user
   decision (Option A).

### 17.4 Revision 4

1. **Pool against ordinary capacity**: the user amended decision 1 to split
   the 1 MiB Background byte capacity 512 KiB / 512 KiB (section 5.1); slice
   3 proves a timer fire into an idle plugin with the pool fully free and
   fully used.
2. **Request credit ownership**: Core's ingress credit keeps its
   return-at-dequeue lifetime; the Hub takes a separate retained backend
   charge at dequeue, before acknowledging it, with one release point
   (section 5.2).
3. **Change-feed cursors**: durable replay is dropped. Store watches are
   bounded resync notifications with no cursor or tombstone; entity
   projections read each commit's diff in the commit path (section 11.2);
   slice 4 tests two watches, folding across a many-document batch, and
   cancellation.

### 17.5 Revision 5

1. **HTTP allowance**: `max_bytes` is now the required total allowance for
   the whole encoded response (status, headers, body, envelope), counted
   while reading; header limits are enforced per header but never reserved up
   front (section 8.1). Slice 6 proves a small response succeeds and
   oversized headers or bodies fail with `response_too_large`.
2. **Completion store**: the user decided the allocation (Option A): each
   plugin's share (128 x 4 KiB pool completions plus 2 request-response
   chains x 1 MiB) is reserved at load; the global store grows to 1040 entries
   and 20 MiB of reservation bytes inside the unchanged 32 MiB ceiling; a
   load that cannot reserve its share fails with `quota_exceeded`
   (section 5.1 item 7, slice 3 proofs).

### 17.6 Revision 6

The user approved one complete allocation (section 5.0), which supersedes the
revision-6 completion-store numbers.

1. **Ordinary completion entries**: 32 per plugin are part of the 162-entry
   share; slice 3 proves an ordinary event at 8-plugin saturation.
2. **Resume ownership**: the completion store carries only markers, so each
   invocation owns exactly one small entry for its own marker; the final
   result is a Reply with its own credit, held from root admission to
   `release_call` (section 4.2). No entry holds two publications, and the
   early-final ordering needs no handoff.
3. **Staging and the callback account**: staging is one global 16 MiB permit
   with serialized loads, not part of any plugin share; the table in section
   5.0 fits both the completion store and the 64 MiB callback account.

### 17.7 Revision 7

The Reply lifecycle now separates the Hub's chain slot (reserved at root
admission, freed at ledger-entry removal) from Core's reply credit (consumed
at send, returned at `release_call`), which matches Core `e54ce21`. Markers
carry `reply_sent`, so an entry is removed only after its sent Reply is
released. Section 4.2 gives one terminal protocol for sent, never sent,
oversized, cancelled, late, unknown, and retired Replies, with real-daemon
proofs. The stale rule that the final `InvocationResult` answers the waiter
is replaced.


## 18. User decisions (summary, all approved as Option A on 2026-09-26)

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

Open (not yet decided by the user):

15. Per-plugin publication credit (4.5): whether to split the existing global
    `PUBLICATION_CAPACITY` (256) into per-plugin credit, and the split value.
    Until the user decides, plugins draw from the global bound.
