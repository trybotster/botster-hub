-- Real-daemon specs for the in-repo fixtures. Run with:
--   botster-plugin-test --e2e --plugin fixtures specs/e2e_spec.lua
local kit = require("botster.test")

kit.test("a plugin loads into a real daemon and serves a tool over the socket", function(t)
  local p = t:load("kit-fixture")
  local stored = p:call_tool("kit-fixture.remember", { key = "alpha", value = "one" })
  t:eq(stored.ok, true)
  t:eq(stored.result.stored, "alpha")
  t:match(p:logs(), { { level = "info", message = "remembered" } })
  local found = false
  for _, tool in ipairs(p:tools()) do
    if tool.name == "kit-fixture.remember" then found = true end
  end
  t:ok(found, "the tool is listed by the daemon")
end)

kit.test("a plugin that calls the removed events.on fails to load in a real daemon", function(t)
  local plugin, err = t:try_load("removed-events-on")
  t:eq(plugin, nil)
  t:ok(tostring(err.message):find("events", 1, true), err.message)
end)

kit.test("verbs that a client cannot serve are refused by name", function(t)
  local p = t:load("kit-fixture")
  t:eq(pcall(function() return t:advance(1) end), false)
  t:eq(pcall(function() return p:db_get("alpha") end), false)
  local refused = p:call_tool("kit-fixture.remember", { key = "k", value = "v" }, { caller = {} })
  t:eq(refused.error.kind, "unsupported_by_kit")
end)

kit.test("a Hub request goes over the real socket", function(t)
  t:eq(t:request({ type = "plugin_mcp_list_tools" }).ok, true)
end)
