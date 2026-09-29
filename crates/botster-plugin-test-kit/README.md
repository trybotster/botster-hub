# botster-plugin-test-kit

Test a Botster Lua plugin against the **real** Hub plugin runtime. The kit loads
your package into a real Hub daemon and drives it one settled step at a time.
There is no hand-written fake of the plugin API, so a plugin that calls a removed
API fails in your test, as it fails in the Hub.

## Install

Install the binary at a pinned Hub commit, then run your specs:

```sh
cargo install --locked --git https://github.com/trybotster/botster-hub \
  --rev <hub-commit> botster-plugin-test-kit
botster-plugin-test --plugin . test/*_spec.lua
```

`--plugin <dir>` is the plugin checkout. `t:load(".")` resolves relative to it,
and `require("helper")` finds Lua files under it. The binary exits non-zero when
any test fails and prints one TAP line per test.

## An example spec

The file `specs/readme_example_spec.lua` in this crate is this example, and the
crate's tests run it.

```lua
local kit = require("botster.test")

kit.test("a note reaches its downstream handler and an ended session leaves", function(t)
  local p = t:load("kit-fixture")                      -- real admission and load

  local noted = p:call_tool("kit-fixture.note", { key = "first" })
  t:eq(noted.ok, true)
  t:eq(p:db_get("noted").items[1], "first")            -- plugin_db state
  t:match(p:emitted_events(), { { name = "kit-fixture.noted", payload = { key = "first" } } })

  t:sessions_baseline({ kit.session({ id = "sess-a", state = "running" }) })
  t:session_upsert(kit.session({ id = "sess-a", state = "exited", code = 0 }))
  t:eq(p:db_get("family").items[4].lifecycle_class, "ended")

  local refused = p:call_tool("kit-fixture.read", { key = "x" }, { caller = { session_id = "sess-a" } })
  t:eq(refused.error.kind, "unsupported_by_kit")
  t:eq(refused.error.gate, "G1")
end)
```

## How a step works

Every verb that changes the Hub is one **step**. A step ends only when all of
its work is done: the request has its reply, every plugin invocation it caused
has completed (including handlers of events that your plugin emits), and no
owner work is ready. A chain that never ends fails with `not_settled` after the
step deadline. Nothing sleeps, and the kit polls nothing.

The Hub in the kit runs on a **logical clock**. It stands still until the spec
calls `t:advance(ms)`. `botster.clock.now` and `botster.clock.monotonic` inside
the plugin read that clock.

## Spec API (in-process mode, the default)

`kit = require("botster.test")`

| Verb | What it does |
| --- | --- |
| `kit.test(name, fn(t))` | Register a test. Each test gets a fresh Hub. |
| `kit.session{ id, state, code?, reason? }` | Describe a session. `state` is `starting`, `running`, `stopping`, `exited`, or `failed`. |
| `t:load(path)` | Load a package directory. A load error fails the test. Returns `p`. |
| `t:try_load(path)` | Like `load`, but returns `nil, { kind, message }` on a load error. |
| `t:sessions_baseline(list)` | Supply one complete session baseline, as Core would. |
| `t:session_upsert(session)` / `t:session_remove(id)` | Supply one journal change. The production consumers produce the `session_family` frames. |
| `t:advance(ms)` | Move the clock. Returns the timers that became due, in deadline order. |
| `t:request(table)` | Send a `DaemonRequest` (a table with a `type` tag) through the production path. |
| `t:receive_routed(session_id)` / `t:ack_routed(session_id, envelope_id)` | Receive routed envelopes as a target, and acknowledge them. |
| `t:settle()` | Run the owner until it is idle. |
| `t:eq(a, b)` | Deep equality. Integers compare exactly. |
| `t:match(actual, expected)` | Subset match: each expected field is present and matches. |
| `t:ok(value, message?)` | Truthy check. |

`p` is a loaded plugin:

| Verb | What it does |
| --- | --- |
| `p:call_tool(name, args, opts?)` | Returns `{ ok, error = { kind, message }, result, response }`. `ok` is false only when the Hub refuses the call. The tool's own return value is `result`. |
| `p:db_get(key)` / `p:db()` | plugin_db payloads. |
| `p:entities(type)` | Published entity frames of a type. The first call subscribes as a client does. Call it before the action to see every frame. A refused subscription raises. |
| `p:emitted_events()` | Plugin-audience events that this plugin emitted, through the real event router. |
| `p:routed(session_id)` | Routed envelopes that Core holds for a session. Reading does not acknowledge them. |
| `p:logs()` / `p:tools()` | Structured logs of the plugin, and the tool descriptors that the Hub lists. |

Load a producer package before a consumer that subscribes to its events. The Hub
refuses a subscription to an event that no loaded package has declared.

## What the kit refuses, and why

The kit does not fake what the Hub cannot do yet. It refuses with the typed error
`unsupported_by_kit` and names the blocked acceptance gate:

| Gate | Refused | Waits for |
| --- | --- | --- |
| G1 | `p:call_tool(..., { caller = ... })` and `{ token = ... }` | The verified-caller model (platform slice 5). |
| G2 | Lua timer callbacks (`advance` reports fired timers but runs no callback) | Platform slice 3. |
| G3 | `t:double(...)` for HTTP, filesystem, and process backends | Platform slice 6. |
| G4 | `p:views()` | Platform slice 4b. |
| G5 | `p:posts()` (posts and doorbells as Hub requests) | Platform slice 5. |
| G6 | Running the same specs on the process host | Platform slice 8. |

## Real-daemon mode

```sh
export BOTSTER_HUB_BIN=... BOTSTER_SESSION_WORKER_BIN=... BOTSTER_CANDIDATE_MANIFEST=...
botster-plugin-test --e2e --plugin . test/e2e/*_spec.lua
botster-plugin-test --conformance
```

`--e2e` starts a real `botster-hub` process in an isolated data directory and
drives it only through its socket, as a client does. The driver has `t:load`,
`t:try_load`, `t:request`, `t:eq`, `t:match`, `t:ok`, and `p:call_tool`,
`p:tools`, `p:logs`. Every other verb raises `unsupported_by_kit`: a real daemon
runs on the real clock and gives a client no plugin_db access. The Hub's own
gate builds the three candidate binaries (`script/build-dev-artifacts`).

`--conformance` runs the Hub's plugin contract matrix conformance on a real
daemon. It checks the Hub with the bundled matrix package, not your plugin.

## Rust API

The crate exports `KitHub` and `KitOptions` for Rust tests. The verbs above
exist there under the same names (`enable_package`, `call_tool`, `advance`,
`sessions_baseline`, `plugin_db`, `emitted_events`, `routed`, ...). A test that
must hold a handler open uses `hold_handler`; the hold is process-wide, so put
such a test in its own test target.

## Limits today

- The in-process kit starts no session worker. Spawning a real session needs `--e2e`.
- There is no backend double below Hub policy yet (gate G3).
- The kit checks the plugin's behaviour, not its Lua source: a bad global in a
  path that no test reaches is not found.
