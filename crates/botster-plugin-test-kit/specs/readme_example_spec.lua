-- The example in the README. runner_test runs this file, so it stays true.
local kit = require("botster.test")

kit.test("a note reaches its downstream handler and an ended session leaves", function(t)
  local p = t:load("kit-fixture")                      -- real admission and load

  -- call a tool; the step ends only when the whole event chain has run
  local noted = p:call_tool("kit-fixture.note", { key = "first" })
  t:eq(noted.ok, true)
  t:eq(p:db_get("noted").items[1], "first")            -- plugin_db state
  t:match(p:emitted_events(), { { name = "kit-fixture.noted", payload = { key = "first" } } })

  -- deliver Hub session events through the production path
  t:sessions_baseline({ kit.session({ id = "sess-a", state = "running" }) })
  t:session_upsert(kit.session({ id = "sess-a", state = "exited", code = 0 }))
  t:eq(p:db_get("family").items[4].lifecycle_class, "ended")

  -- a caller identity is not supported yet: a typed refusal names the gate
  local refused = p:call_tool("kit-fixture.read", { key = "x" }, { caller = { session_id = "sess-a" } })
  t:eq(refused.error.kind, "unsupported_by_kit")
  t:eq(refused.error.gate, "G1")
end)
