-- botster-workspaces: an ended Hub session leaves its workspace.
--
-- A port of the session_family pruning test in botster-workspaces'
-- test/plugin_runtime_test.lua (lines 827-923 at 7ec8fac). That test ran
-- against a hand-written fake Hub API that still defined the removed global
-- `events.on`, so it passed while the real Hub refused to load the plugin.
-- This spec loads the real package into the real Hub plugin runtime.
--
--   botster-plugin-test --plugin <botster-workspaces checkout> \
--     specs/workspaces_pruning_spec.lua
local kit = require("botster.test")

local FAMILY = "botster-workspaces.membership"
local SESSION = "66666666-6666-4666-8666-666666666666"
local BYSTANDER = "77777777-7777-4777-8777-777777777777"

local function refs(p, workspace_id)
  local shown = p:call_tool("botster_workspaces.show", { id = workspace_id })
  return shown.result.workspace.session_refs
end

kit.test("an ended session leaves its workspace and publishes the removal", function(t)
  local p = t:load(".")
  local created = p:call_tool("botster_workspaces.create", { name = "alpha" })
  t:eq(created.ok, true)
  t:eq(created.result.ok, true, "the create tool succeeds")
  local workspace_id = created.result.workspace.id

  t:sessions_baseline({
    kit.session({ id = SESSION, state = "running" }),
    kit.session({ id = BYSTANDER, state = "running" }),
  })
  p:entities(FAMILY) -- watch the membership family from here on

  for _, id in ipairs({ SESSION, BYSTANDER }) do
    local added = p:call_tool("botster_workspaces.add_session", {
      workspace_id = workspace_id,
      session_id = id,
    })
    t:eq(added.result.ok, true, "session joins the workspace")
  end
  t:eq(#refs(p, workspace_id), 2)
  t:ok(p:db_get("membership:" .. SESSION) ~= nil, "membership is stored")

  -- A running session keeps its membership.
  t:session_upsert(kit.session({ id = SESSION, state = "running" }))
  t:eq(#refs(p, workspace_id), 2, "a current session keeps its workspace reference")

  local removals_before = 0
  for _, frame in ipairs(p:entities(FAMILY)) do
    if frame.type == "entity_remove" then
      removals_before = removals_before + 1
    end
  end

  -- The session ends: the plugin prunes its membership and the reference.
  t:session_upsert(kit.session({ id = SESSION, state = "exited", code = 0 }))
  t:eq(p:db_get("membership:" .. SESSION), nil, "the ended session's membership key is deleted")
  t:eq(#refs(p, workspace_id), 1, "the ended session leaves the workspace")
  t:ok(p:db_get("membership:" .. BYSTANDER) ~= nil, "another session keeps its membership")

  local removals = {}
  for _, frame in ipairs(p:entities(FAMILY)) do
    if frame.type == "entity_remove" then
      removals[#removals + 1] = frame
    end
  end
  t:eq(#removals, removals_before + 1, "the ended session publishes one membership removal")
  t:eq(removals[#removals].id, SESSION)

  -- Removing the row does not confirm that the session ended: keep the rest.
  t:session_remove(BYSTANDER)
  t:ok(p:db_get("membership:" .. BYSTANDER) ~= nil, "a removed row keeps its membership")
  t:eq(#refs(p, workspace_id), 1)
end)
