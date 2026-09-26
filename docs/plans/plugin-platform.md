# Plugin platform design

Status: draft for review (2026-09-26). Owner: plugin-platform Hub writer.
Scope: the Lua-facing platform that first-party plugins need, starting with
the cutover plugins messaging, orchestrator, and project-pipelines.
Decisions marked **PRODUCT DECISION** need the user. Everything else is a
design proposal for the reviewer.

The monorepo plugin API is evidence of what plugins need. It is not a design
to copy. This is a cold cut: no compatibility with the monorepo API and no
compatibility shims for the current modular ABI.

## 1. Survey summary

Facts from the current Hub (HEAD `f1674920`) that drive this design:

1. **One VM per plugin, serialized.** Each package has one
   `LuaPluginRuntime` with one `Mutex<LuaState>` (`src/lua_runtime.rs:1773`).
   Core runs two executor threads per plugin (one reserved for
   request-response work), and both serialize on that mutex.
2. **The owner loop never blocks on plugins.** Owner-side admission is
   `try_admit`; a queued request parks as
   `ControlStep::pending_in(ReadyClass::PluginCompletion)`; the Core completion
   notifier wakes the owner, and a bounded `CompletionDrain` slice routes the
   result. This is the park/resume path this design reuses.
3. **Helpers block the plugin worker.** Every current helper is synchronous
   and runs on the worker thread that holds the VM mutex:
   `coordination.*` and `entity_publish` wait up to 1 s for the owner,
   `session_types.spawn` and `ensure_worktree_and_spawn` wait up to 30 s, and
   `plugin_db.*` does synchronous redb I/O. A handler in a 30 s wait blocks
   every other invocation of that plugin, including request-response work.
4. **No asynchronous delivery into Lua exists.** Core's
   `CapabilityRuntimeRequest.callback: Option<PluginHandlerRef>` exists but is
   always `None`. Timer, HTTP, WebSocket, and filesystem events collect in a
   per-plugin queue that production never drains into Lua. No production code
   fires timers (`drain_events_at` is called only by tests).
5. **Grants are global, not per package.** `HubCapabilityRuntime` checks one
   set, `default_hub_capability_grants` (`src/capabilities.rs:1852`), which
   names `project-pipelines` and `botster-workspaces` literally. Package
   manifests are checked at enable time but the runtime never reads them.
   Every handler registers with `required_capability: None`, so Core's
   per-handler check never runs.
6. **Error conventions are inconsistent.** Helpers raise Lua errors
   (`session_types.*`, `coordination.*`), return `{ ok = false, ... }`
   (`plugin_db.batch`, `worktrees.show`), or return `{ status = ... }`
   (`events.emit`, `entity_publish`).
7. **The sandbox leaked ambient authority.** mlua always opens the base
   library, so `dofile`, `loadfile`, binary `load`, `string.dump`, `print`,
   `warn`, and full `collectgarbage` were reachable, and `pcall` could absorb
   the instruction-budget error forever. Slice 0 fixes this (section 12).
8. **Parsed but inert.** The handler kinds `command`, `hook`, `timer`, and
   `session_action` are parsed at registration and never dispatched.
9. **Missing entirely.** Logging, JSON, clock, module loading, HTTP from Lua,
   file watching (`Watch` returns `InvalidRequest`), package secrets, session
   list/inspect/close/metadata, MCP prompts, MCP proxy, push notifications,
   repo detection, and command gates.

Cutover plugin needs (full inventory in the survey notes):

| Need | messaging | orchestrator | project-pipelines |
| --- | --- | --- | --- |
| MCP tools with caller identity | 3 | 10 | 63 |
| Session list / lookup by label / inspect | yes | yes | |
| Session spawn (with managed worktree) | | yes | yes (exists) |
| Session close / metadata (label, task) | | yes | legacy |
| Terminal read (snapshot) / input write | | yes | |
| Inbox post / receive + doorbell | yes | yes | legacy |
| Timers (repeating, cancel) | | yes | legacy |
| KV store + atomic batch | | | yes (exists) |
| Structured query | | | N+1 full load today |
| Surfaces, UI actions, entity providers | | | yes (exists) |
| Event emit / subscribe | | | yes / legacy |
| Provider dependency check | | | yes (Hub lacks it) |
| Command gate, push, MCP prompts, fs | | | legacy |
| Multi-hub discovery and RPC | | yes | |
| Logging / JSON | yes | yes | |

project-pipelines (modular repo `botster-project-pipelines`, v0.4.0) is
already on the modular ABI. It calls `botster.capabilities.provider_dependencies.check`,
which the Hub does not implement.

## 2. Principles

1. **No ambient authority.** A plugin VM holds pure computation only: the
   table, string, math, and utf8 libraries plus the narrowed base library.
   Every effect outside the VM is a Hub-brokered call.
2. **Capabilities are declared, granted per package, and checked per call.**
   The package manifest declares each capability. Hub admission records the
   granted set in the package record. Every privileged call checks the
   calling package's granted set, which is the same set that admission
   recorded. There is no global grant list.
3. **Capabilities name outcomes, not mechanisms.** "Run the repo's command
   gate" is a capability; "run a shell command" is not. "Detect the repo of a
   target" is a capability; "exec git" is not. Plugins select Hub-admitted
   objects by id (target id, worktree id, session id, gate id). Plugins do not
   supply paths, commands, or process arguments.
4. **Nothing blocks the owner loop, and I/O does not block the plugin
   worker.** Every operation whose completion depends on external I/O or on
   another process is asynchronous with a callback. The callback arrives
   through the existing admission and park/resume path. No polling anywhere.
5. **Every resource is bounded by existing accounting.** Per-VM memory, the
   callback account, the per-plugin worker queue, and the per-plugin
   capability operation and event capacities bound every new resource. This
   design adds no numeric limit. Where a new limit seems necessary, the
   design marks it **PRODUCT DECISION**.
6. **One plugin cannot starve siblings.** Each plugin has its own queues.
   Owner-side delivery rotates between plugins and runs under the existing
   turn budget.
7. **Reload-safe by generation.** Callbacks live inside the plugin VM. A
   reload replaces the VM, cancels the old generation's resources, and drops
   stale deliveries.
8. **One calling convention** (section 3).

## 3. The calling convention

### 3.1 Namespaces

- `botster.<area>` holds runtime basics that every plugin receives with no
  grant: `register`, `log`, `json`, `clock`, `entity_publish`, and `events`.
- `botster.capabilities.<area>` holds every operation that needs a manifest
  grant. Hub checks the grant on every call.
- `require` is a Hub-provided module loader over the package's own files
  (section 4.4).

This keeps the current split. `botster.capabilities.plugin_db` and
`botster.capabilities.session_types` keep their names.

### 3.2 Arguments

Every function takes one table. Positional arguments are not used, except
the callback, which is always the last argument of an asynchronous call.

```lua
local result = botster.capabilities.plugin_db.get({ key = "tickets/1" })
local handle = botster.capabilities.http.request({ url = url }, function(result) end)
```

### 3.3 Results

Every call returns a result table with an explicit discriminant:

```lua
{ ok = true, value = <any> }
{ ok = false, error = { kind = "<error kind>", message = "<text>", retryable = <bool>, ... } }
```

Rules:

- Operational failures return `ok = false`. They never raise.
- Invalid arguments also return `ok = false` with `kind = "invalid_request"`.
  One discriminant covers every failure, so plugin code needs no `pcall`.
- A helper raises only when the VM itself cannot continue: the VM memory
  limit, the instruction budget, or a host allocation refusal. A plugin
  cannot catch these meaningfully (the sandbox re-raises budget errors).
- Area-specific failure data goes in extra `error` fields, for example
  `error.mutation_index` for `plugin_db.batch`.

### 3.4 Error kinds

One closed set for all areas:

| kind | meaning | retryable |
| --- | --- | --- |
| `invalid_request` | argument shape or value refused | no |
| `capability_denied` | the package lacks the grant, or the object is not the plugin's | no |
| `not_found` | the named object does not exist | no |
| `conflict` | revision conflict or already exists | no |
| `quota_exceeded` | a plugin-owned quota is full (store keys, bytes) | no |
| `backpressured` | a bounded queue is full now; try again later | yes |
| `timed_out` | the operation's deadline expired | yes |
| `cancelled` | the plugin or the Hub cancelled the operation | no |
| `unavailable` | the target runtime is stopped, disconnected, or not configured | yes |
| `failed` | the operation ran and failed; `error.detail` explains | no |

Core `CapabilityRuntimeErrorKind` maps onto this set: `Backpressured` to
`backpressured`; `CapabilityDenied` to `capability_denied`;
`OperationNotFound`, `ResourceNotFound`, and `StoreNotFound` to `not_found`;
`TimedOut`, `Cancelled`, and `InvalidRequest` to the same names;
`RuntimeStopped` to `unavailable`; `RevisionConflict` to `conflict`;
`QuotaExceeded` to `quota_exceeded`; `PatchFailed` and `BackendFailed` to
`failed`. Event-plane statuses map the same way: `rejected_*` to
`invalid_request` or `capability_denied`, `shed_*` to `backpressured`.

### 3.5 Asynchronous calls and handles

An asynchronous call returns a result immediately:

```lua
local started = botster.capabilities.http.request({ url = url }, function(result)
  if not result.ok then
    botster.log.warn({ message = "fetch failed", error = result.error })
    return
  end
  handle_body(result.value.body)
end)
if not started.ok then
  -- refused at admission; the callback will never run
end
local handle = started.value
handle:cancel()
```

Rules:

1. If admission refuses the call, the result is `ok = false` and the
   callback never runs.
2. If admission accepts the call, `value` is a handle. A one-shot operation
   runs its callback exactly once, unless the plugin cancels it first. A
   stream (watch, repeating timer) runs its callback once per event.
3. `handle:cancel()` is authoritative inside the VM: it removes the callback
   at once, so the callback never runs again, whatever is in flight. The Hub
   then releases the resource asynchronously.
4. A handle is userdata with `cancel()`, `active()`, and a read-only `id`.
   Dropping a handle does not cancel the operation.
5. A callback runs as an ordinary Background invocation of the plugin. It can
   call every helper, emit events, and start further operations.

### 3.6 Synchronous classes

A helper may stay synchronous only in two classes:

- **Local**: the work is bounded and needs no other thread's progress. For
  example `json`, `clock`, `log`, `config.get`, and `plugin_db` (local redb;
  see section 8).
- **Owner admission round trip**: the helper needs one owner-thread
  admission decision and not the completion of external work. For example
  `entity_publish` and `coordination.*`. The wait is a marked
  `// timer: deadline` whose expiry returns `timed_out`.

Everything else is asynchronous. `session_types.spawn` and
`ensure_worktree_and_spawn` move from a 30 s blocking wait to callbacks.

## 4. Area 1: basics

### 4.1 Structured logging

```lua
botster.log.info({ message = "ticket advanced", ticket_id = id, step = step })
-- levels: debug, info, warn, error
```

- Grant: none.
- Records carry the package name, the generation, and the level. Hub writes
  them to the Hub log and keeps a bounded per-plugin ring that the
  `get_plugin_logs` operator surface reads.
- Accounting: a record is JSON-encoded under the per-callback byte ceiling.
  Each plugin gets its own ring, so one plugin cannot evict another's logs.
- **PRODUCT DECISION (limits)**: the ring size and a record rate limit are
  new numbers. Option A (recommended): reuse the event-plane policy values,
  64 KiB per record and 100 records/s with a burst of 200, and keep the last
  256 records per plugin (the existing per-plugin event capacity). Option B:
  new dedicated values.
- Replaces: `print` (removed in slice 0) and monorepo `log.*`.

### 4.2 JSON

```lua
local encoded = botster.json.encode({ value = { a = 1 } })   -- { ok, value = "<text>" }
local decoded = botster.json.decode({ text = body })          -- { ok, value = <table> }
local list = botster.json.array({})                           -- encodes as []
local nothing = botster.json.null
```

- Grant: none. Local.
- Uses the existing memory-bounded conversion in `src/lua_runtime/lua_json.rs`.
- `json.array` marks a table as an array, which fixes the empty-table
  ambiguity that entity frames work around today.
- Replaces: monorepo `json.*`.

### 4.3 Clock

```lua
botster.clock.now()        -- { ok, value = <integer Unix epoch milliseconds> }
botster.clock.monotonic()  -- { ok, value = <integer milliseconds since Hub start> }
```

- Grant: none. Local.
- Replaces: `os.time`, `os.clock`, `os.date`.

### 4.4 Multi-file plugins: `require`

```lua
local store = require("lib.store")   -- loads <package>/lua/lib/store.lua
```

- Grant: none.
- At package load, Hub reads every `.lua` file below the package's module
  root (`lua/`) into the new VM, as text. `require` serves modules from that
  in-memory set only. After load there is no filesystem access.
- Paths are canonicalized against the package root. `..`, absolute paths,
  and symlinks that leave the root fail the load.
- Accounting: module sources are Lua strings in the plugin VM, so the per-VM
  memory limit bounds them. Each file read uses the existing bounded read
  with the per-callback ceiling.
- Modules compile with the text-only `load` (section 12).
- A reload re-reads the modules into the new VM, so there is no stale cache.
- Replaces: monorepo `require` and the single-file limit.

### 4.5 Timers

```lua
local once = botster.capabilities.timer.after({ ms = 5000 }, function(result) end)
local tick = botster.capabilities.timer.every({ ms = 60000 }, function(result)
  -- result.value = { sequence = n, missed = m }
end)
tick.value:cancel()
```

- Grant: `{ surface = "timers", scope = "callbacks" }` (existing Core scope).
- Hub keeps plugin timers in one deadline heap. The owner loop's next
  deadline includes the earliest plugin timer, so there is one scheduled wake
  at the exact expiry and no periodic tick.
- A repeating timer coalesces: if the previous fire is still undelivered, the
  next fire increments `missed` instead of queueing. So a short interval
  cannot fill the plugin's queue, and no minimum interval is needed.
- Resources: each timer counts against the existing per-plugin capability
  operation capacity (128).
- Timer marker: the rewrite rules have no category for plugin-requested
  schedules. **PRODUCT DECISION (orchestrator ruling)**: add
  `// timer: plugin-schedule — <which plugin API>` for the one Hub site that
  arms plugin deadlines, or classify it as `deadline`. Plugins that used
  timers to poll (orchestrator's 60 s prune, project-pipelines' 30 s
  reconcile) must port to events instead; the platform does not endorse
  polling.
- Replaces: `timer_once` (colliding ids, never fires), the inert `timer`
  handler kind, and monorepo `timer.*`.

## 5. Area 2: outside world

### 5.1 HTTP

```lua
botster.capabilities.http.request({
  method = "POST",
  url = "https://api.telegram.org/sendMessage",
  headers = { ["content-type"] = "application/json",
              authorization = botster.capabilities.secrets.ref({ name = "bot_token" }) },
  body = encoded,
  timeout_ms = 10000,
}, function(result)
  -- result.value = { status = 200, headers = {...}, body = "<text or bytes>" }
end)
```

- Grant: `{ surface = "network", scope = "<origin>" }`, for example
  `https://api.telegram.org`. The request origin must equal a granted origin.
- Transport: the existing Core `HttpCapabilityRuntime` and Hub
  `RealHttpTransport`, submitted with a callback.
- Accounting: in-flight requests count against the per-plugin capability
  operation capacity. The response body is charged to the plugin's callback
  account until delivery. The deliverable response size is the smaller of the
  Core response limit (4 MiB) and the plugin's Background queue byte capacity
  (1 MiB); this is derived from existing limits, not a new one.
- Errors: `capability_denied` (origin not granted), `timed_out`, `cancelled`,
  `failed` (with `detail.status` for transport errors).
- **PRODUCT DECISION (network policy)**: today the Hub allows loopback only,
  GET and POST only, and denies `authorization` and `cookie` headers.
  Plugins such as telegram and github need remote origins and credentials.
  - Option A (recommended): remote origins are allowed only when the manifest
    declares them and the operator enables the package. Credentials enter
    only through secret references (`secrets.ref`), which Hub substitutes at
    send time for granted origins. The plugin never sees the secret value,
    so it cannot send it elsewhere.
  - Option B: the same origin grants, but the plugin reads secrets and sets
    headers itself. Simpler, but a plugin can exfiltrate its secret to any
    granted origin.
  - Option C: keep loopback only; remote access goes through supervised
    runnable entrypoints.
- Replaces: monorepo `http.request` and the blocking `http.post`.

### 5.2 File watching and scoped reads

```lua
botster.capabilities.fs.watch({ root = "vault", path = "inbox", recursive = true },
  function(result)
    -- result.value = { path = "inbox/a.md", change = "created" | "modified" | "removed" | "overflow" }
  end)
botster.capabilities.fs.read({ root = "vault", path = "inbox/a.md" }, function(result) end)
botster.capabilities.fs.list({ root = "vault", path = "inbox" }, function(result) end)
```

- Grant: `{ surface = "filesystem", scope = "<root name>" }`. A root is
  either `data` (the plugin's own directory under the Hub data directory) or
  a name bound to an operator-set package configuration field. The manifest
  declares the root and its configuration field; the operator sets the path.
  The plugin never supplies an absolute path.
- Mechanism: Core's `WatchCapabilityRequest` and `FilesystemCapabilityRequest`
  contracts, with an OS event source (FSEvents on macOS, inotify on Linux).
  There is no polling watcher. Events are coalesced; queue overflow reports
  `overflow` so the plugin rescans.
- Accounting: each watch is a resource in the per-plugin operation capacity.
  Event delivery uses the per-plugin event capacity (256).
- Replaces: monorepo `watch.directory` and `fs.*`.

### 5.3 Package secrets

```lua
botster.capabilities.secrets.get({ name = "bot_token" })      -- { ok, value = "<secret>" }
botster.capabilities.secrets.set({ name = "refresh_token", value = token })
botster.capabilities.secrets.ref({ name = "bot_token" })       -- opaque userdata for http
```

- Grant: `{ surface = "secrets", scope = "own" }`. A package can name only
  secrets that its manifest declares as `secret` configuration fields.
  Another package's secrets are unreachable.
- Storage: the Hub credential provider (OS keychain in production), keyed by
  package name and field name. The package configuration shows these fields
  as set or unset, never the value.
- `get` is local and synchronous. `set` writes the credential store
  asynchronously with a callback, because keychain writes can block.
- Replaces: monorepo `secrets.get/set` and the write-only config marker.

## 6. Area 3: agents and sessions

### 6.1 Session reads

```lua
botster.capabilities.sessions.list({ owner = "self" })   -- or owner = "any"
botster.capabilities.sessions.get({ session_id = id })
-- rows: { session_id, label, title, agent_name, status, lifecycle, target_id,
--         worktree_id, branch, workspace, owner_plugin, created_at, metadata }
```

- Grant: `{ surface = "session_actions", scope = "session_read" }`.
- Mechanism: an owner admission round trip that reads the Hub session
  projection (the same rows as the `/session` entity family). Plugins that
  need live state keep using `events.on("hub", "session_family")`.
- Accounting: the result is bounded by the existing session list response
  shape and charged as callback bytes.

### 6.2 Session actions

```lua
botster.capabilities.sessions.close({ session_id = id }, function(result) end)
botster.capabilities.sessions.update({ session_id = id, label = "Review", metadata = { step = "verify" } })
botster.capabilities.sessions.send_input({ session_id = id, text = "continue\r" })
botster.capabilities.sessions.read_screen({ session_id = id }, function(result) end)
botster.capabilities.sessions.notify({ session_id = id, title = "New message", hint = "..." })
```

- Grants (all on the existing `session_actions` surface): `session_close`,
  `session_update`, `session_input`, `session_screen`, `session_notify`.
- Ownership: by default a plugin may act only on sessions it spawned
  (`owner_plugin` equals its package). **PRODUCT DECISION (cross-plugin
  control)**: orchestrator-style plugins act on any session. Option A
  (recommended): an extra scope suffix `:any` (for example
  `session_close:any`) that the operator approves at enable. Option B: all
  session actions apply to any session.
- `update` writes `label`, `title`, and the plugin's own metadata namespace
  (`metadata[<package>]`) only. Hub needs a new control request for this.
- `close` is asynchronous because shutdown completes later.
- `read_screen` uses the existing `ReadScreen` daemon request.
- Replaces: `Hub:delete_agent`, `Hub:update_session`, `Hub:get_pty_snapshot`,
  `Hub:send_message`, `Hub:notify`.

### 6.3 Messaging between agents

```lua
botster.capabilities.messages.post({ to_session = id, type = "message", payload = body, reply_to = nil })
botster.capabilities.messages.receive({ session_id = caller })   -- drains the caller's inbox
```

- Grant: `{ surface = "session_actions", scope = "session_message" }`.
- Mechanism: the existing Core routed-envelope primitive
  (`botster.coordination`) with session targets. This is the mechanism the
  native Hub MCP tools already use.
- **PRODUCT DECISION (who owns the message tools)**: today the Hub's Rust
  `NativeHubToolProvider` implements `post_message`, `receive_messages`,
  `ack_message`, and `notify_session`. Option A (recommended): move the tool
  surface, label lookup, envelope format, and doorbell text into the
  messaging plugin over `messages.*` and `sessions.notify`, and delete the
  native tools (cold cut). This keeps product policy in Lua. Option B: keep
  the native tools and ship no messaging plugin.

### 6.4 Caller identity in MCP handlers

MCP tool handlers receive `request.caller = { session_id = ... }`.

**Risk**: today `mcp-serve` takes the caller from the `BOTSTER_SESSION_UUID`
environment variable, which any local process can set. Access decisions
(for example "drain only your own inbox") need a proof of identity.
**PRODUCT DECISION (caller authentication)**: Option A (recommended): Hub
injects a per-session caller token into the session context at spawn;
`mcp-serve` presents it; Hub maps it to the session. Option B: accept the
environment variable as advisory and document the trust gap.

### 6.5 Lifecycle and terminal-output hooks

- Lifecycle: the existing `events.on("hub", "session_family", fn)` stream
  (snapshot plus deltas). It is already bounded, backpressured, and
  gap-recovering. The platform adds `exit_code` to terminal deltas, which
  command gates need (section 10.2).
- **PRODUCT DECISION (output hooks)**: the Hub must not relay terminal bytes
  (the data plane bypasses the Hub). Option A (recommended): semantic
  terminal events from Core (bell, OSC 9/777 notifications, title change,
  idle/active transitions) delivered through the session-family stream, plus
  `read_screen` on demand. Option B: a sampled raw-output stream with byte
  budgets; this puts the Hub on the byte path and is not recommended.
- Replaces: `hooks.on("agent_created" | "agent_lifecycle" | "pty_output" | ...)`.

### 6.6 Multi-hub

**PRODUCT DECISION (federation)**: the orchestrator plugin serves
`list_hubs` and remote agent control through hub discovery and hub-to-hub
RPC. Option A (recommended for cutover): local hub only; `list_hubs` returns
the local hub. Option B: design a federation capability now.

## 7. Area 4: MCP

### 7.1 Tools and prompts

```lua
return botster.register({
  tools = {{ name = "tickets.create", description = "...", input_schema = {...},
             handler = "create", call = function(request) end }},
  prompts = {{ name = "tickets.plan", description = "...",
               arguments = {{ name = "ticket_id", required = true }},
               handler = "plan", call = function(request) return { messages = {...} } end }},
})
```

- Grant: `{ surface = "mcp" }`.
- Prompts use the existing Core `McpPrompt` handler and descriptor kinds.
  `mcp-serve` adds `prompts/list` and `prompts/get` next to the tool methods.
- Tool results use the same result convention: `ok = false` becomes an MCP
  error result with the error kind in the content.

### 7.2 MCP proxy

**PRODUCT DECISION (proxy shape)**:

- Option A (recommended): declarative. The manifest declares
  `mcp_servers = [{ id, url | runnable_entrypoint, auth = { secret = "<field>" } }]`.
  Hub owns the MCP client, connects through the network grant or supervises
  a stdio server as a runnable entrypoint, and re-exposes the tools as
  `<package>.<tool>`. The plugin may register an `McpProxyAuthError` handler
  to refresh credentials. Protocol mechanics stay in the trusted kernel.
- Option B: a plugin-code proxy over `http.request`. Every plugin then
  re-implements MCP framing.

## 8. Area 5: storage

`botster.capabilities.plugin_db` keeps its operations (`get`, `set`,
`patch`, `delete`, `list`, `batch`) and moves to the result convention.

- Grant: `{ surface = "plugin_db", scope = "<own package name>" }`, read from
  the package's admitted set (fixes the global list).
- It stays synchronous (local class): redb operations are bounded and need
  no other thread. The batch stays atomic.

**PRODUCT DECISION (structured query)**. project-pipelines today reloads
its whole namespace on every tool call (a list plus one get per key).

- Option A (recommended now): KV plus a VM-resident cache. The plugin VM
  persists between invocations, and the plugin is the only writer of its
  namespace, so it can load once per generation and update its cache on its
  own commits. No new Hub API.
- Option B: KV plus declared secondary indexes. The registration declares
  indexes on JSON fields per key prefix; Hub maintains index entries inside
  the same atomic batch; `plugin_db.query({ prefix, where, order_by, limit,
  cursor })` runs bounded range scans.
- Option C: SQL (SQLite per plugin). Most expressive, but it needs query
  cost bounding, migrations, and a second storage engine.

## 9. Area 6: UI

**PRODUCT DECISION (UI model)**:

- Option A (recommended): keep the current model. `surface_route` handlers
  return `botster-ui-contract` `UiNode` trees, lists bind to package entity
  families, and `entity_publish` sends live changes. Add a pure-Lua builder
  module, shipped by Hub and loaded with `require("botster.ui")`, that
  produces the same trees with less boilerplate. Homepage widgets use the
  existing `dashboard_widget` surface kind. No new Rust.
- Option B: a retained-mode builder with Hub-side diffing. More ergonomic
  for complex screens, but it moves render state into the Hub.
- Option C: plugin web assets in iframes. The contract has an `iframe` node,
  but Hub serves no package assets; this needs an asset-serving capability.

## 10. Area 7: Hub-brokered actions

### 10.1 Repo detection

```lua
botster.capabilities.repo.detect({ target_id = id }, function(result)
  -- result.value = { is_git = true, repo = "owner/name", branch = "main", head = "<sha>" }
end)
```

- Grant: `{ surface = "session_actions", scope = "repo_read" }`.
- Input is a target id or worktree id, never a path. Hub reads `.git`
  directly where it can and runs `git` through its managed-git runner where
  it must.

### 10.2 Command gates

**PRODUCT DECISION (gate source)**. A gate must be a Hub-admitted command,
not a plugin-supplied string.

- Option A (recommended): a gate is a declared session type with
  `lifecycle = "task"` and `interaction = "noninteractive"`, defined by the
  repo or the package like any session type.
  `botster.capabilities.gates.run({ session_type_id, worktree_id }, cb)`
  spawns it in the worktree and calls back with
  `{ exit_code, duration_ms, output_tail }` when the session exits. This
  reuses spawn admission, process supervision, output capture, and exit
  events.
- Option B: a separate gate registry (for example `.botster/gates.json`)
  with its own runner.

### 10.3 Worktree lifecycle

```lua
botster.capabilities.worktrees.ensure({ target_id = id, branch = "feature/x" }, function(result) end)
botster.capabilities.worktrees.remove({ worktree_id = id }, function(result) end)
```

- Grant: `{ surface = "session_actions", scope = "worktree_manage" }`.
- Mechanism: the existing managed-git path (`prepare_managed_worktree`,
  rollback, reconcile). `remove` refuses a dirty worktree and refuses
  worktrees that the plugin did not create. `ensure_worktree_and_spawn` stays
  as the combined operation.

### 10.4 Push notifications

**PRODUCT DECISION (notification channel)**:

- Option A (recommended): client notices. Extend the existing manifest
  `events.notices` so `botster.capabilities.notify({ title, body, severity,
  session_id })` shows a notice in connected Web and TUI clients.
- Option B: OS or mobile push, which needs a push service and keys.
- Option C: external channels through plugin HTTP (telegram).

### 10.5 Provider and package dependencies

```lua
botster.capabilities.packages.status({ name = "github" })
-- { ok, value = { installed, enabled, configured, capabilities = {...} } }
```

- Grant: none beyond `mcp` or `surfaces`; status is read-only.
- Replaces: `provider_dependencies.check`, which project-pipelines calls and
  the Hub lacks.

## 11. Asynchronous delivery contract

The event-driven Hub writer builds this path. The Lua API above depends on
exactly these properties.

1. **Registration.** At load, Hub registers one delivery handler per
   capability family (`Http`, `Watch`, `Timer`, and `Event` for the rest)
   for every Lua plugin, with `required_capability` set to the family grant,
   so Core's handler check also applies. Core refuses unregistered handler
   ids, so per-callback handler ids are not possible.
2. **Callback storage.** The Lua closure stays in the plugin VM, in a table
   keyed by a callback id. Hub never holds a closure. The capability request
   carries `(plugin key, generation, callback id)`.
3. **Completion to owner.** A capability worker publishes a completion into
   a per-plugin pending queue and calls a notifier that the owner installs,
   like `install_plugin_completion_notifier`. The notifier sends one control
   message that sets a maintenance wake. No polling.
4. **Delivery slice.** The owner's slice rotates across plugins with pending
   completions under the existing turn budget (items, bytes, time). It may
   batch several completions of one plugin into one Background invocation.
   It calls `try_admit`.
   - `Queued`: the completions leave the pending queue.
   - `Backpressured`: the completions stay parked. The plugin's next worker
     completion (the existing `PluginCompletionPublished` notifier) re-arms
     the wake. No timer and no retry loop.
5. **Generation.** A completion whose generation differs from the plugin's
   current generation is dropped and counted. The Lua trampoline also drops
   a callback id it does not know (for example after `cancel()`).
6. **Accounting.** The completion payload is charged to the plugin's callback
   account from production until admission, when ownership moves to the Core
   queue, or until it is dropped.
7. **Timers.** Plugin timers live in a deadline heap that feeds the owner's
   next deadline. An expired timer enqueues through the same pending path.
8. **Cleanup.** Unload and reload call `cleanup_plugin_capabilities`, which
   cancels in-flight operations, releases resources, drops pending
   completions, and releases their charges. Reload already calls it
   (`src/runtime/package_effect.rs`).

Invariants: the owner never blocks; nothing polls; one completion causes at
most one callback; each plugin's pending memory is bounded; a stalled plugin
parks only its own completions.

**PRODUCT DECISION (per-plugin callback share)**: the callback account is
Hub-wide (64 MiB, 8 MiB per callback). One plugin can fill it with parked
completions and starve siblings. Option A (recommended): cap each plugin's
parked completion bytes at its own Background queue byte capacity (1 MiB, an
existing number) and refuse new operations with `backpressured` above it.
Option B: a new explicit per-plugin share of the callback account.

## 12. Slice 0: sandbox hardening (landed first)

Base globals after slice 0:

| Global | Decision | Reason |
| --- | --- | --- |
| `dofile`, `loadfile` | removed | read host files with daemon privileges |
| `print`, `warn` | removed | write daemon stdio; `botster.log` replaces them |
| `string.dump` | removed | produces bytecode |
| `load` | text only (`mode` must be nil or `"t"`) | binary chunks can break VM memory safety |
| `collectgarbage` | `"count"` only | other options stop or retune the collector that enforces the VM memory limit |
| `pcall`, `xpcall` | re-raise once the instruction budget is exhausted | the budget hook raises an ordinary error that a protected loop could absorb forever |
| `setmetatable` | refuses `__gc` | finalizers run outside invocation accounting (existing guard) |
| string metatable | protected (`__metatable = false`) | shared by every string in the VM |
| `os`, `io`, `package`, `require`, `debug` | absent | libraries not opened; the sandbox clears them defensively |
| `assert`, `error`, `getmetatable`, `ipairs`, `next`, `pairs`, `rawequal`, `rawget`, `rawlen`, `rawset`, `select`, `tonumber`, `tostring`, `type`, `_G`, `_VERSION` | kept | VM-local only |

Slice 4.4 later installs a Hub `require` over the package's own modules.

## 13. Isolation and grants: one source of truth

1. The manifest declares capabilities (`{ surface, scope }`).
2. Enable-time admission checks them against the host profile and records
   `admitted_capabilities` in the package record (exists).
3. The runtime check reads the calling package's `admitted_capabilities`,
   not a global set. `default_hub_capability_grants` and its literal package
   names are deleted.
4. Every registered handler carries its `required_capability`, so Core also
   checks package metadata at admission.
5. project-pipelines must then declare every capability it uses (`config`
   reads need none; `session_actions` scopes, `plugin_db`, `mcp`, and
   `surfaces` are declared).

## 14. Implementation slices

Each slice is a small reviewed vertical slice with a real daemon test and a
real Lua plugin.

0. Sandbox hardening (section 12). In review.
1. Per-package grants (section 13) and the result convention for existing
   helpers (section 3). Rewrite `docs/lua-plugin-abi.md`.
2. Basics: `log`, `json`, `clock`, `require` (section 4.1 to 4.4).
3. Delivery path integration (section 11, built by the event-driven writer)
   plus timers (section 4.5) as its first client.
4. Session reads and actions, messaging, caller identity (section 6).
5. HTTP and secrets (section 5.1, 5.3); file watch and reads (section 5.2).
6. MCP prompts and proxy (section 7).
7. Repo detection, gates, worktrees, notifications, package status
   (section 10).
8. Ports: messaging, then orchestrator, then project-pipelines, each with a
   live proof.

Slices 4 to 7 depend on the product decisions above. Slices 1 to 3 do not.

## 15. Product decisions (summary)

1. Logging limits (4.1).
2. Plugin timer marker category (4.5).
3. Network policy and secret references (5.1).
4. Cross-plugin session control (6.2).
5. Ownership of the message tools (6.3).
6. MCP caller authentication (6.4).
7. Terminal-output hooks (6.5).
8. Multi-hub federation (6.6).
9. MCP proxy shape (7.2).
10. Structured query (8).
11. UI model (9).
12. Command gate source (10.2).
13. Notification channel (10.4).
14. Per-plugin callback share (11).
