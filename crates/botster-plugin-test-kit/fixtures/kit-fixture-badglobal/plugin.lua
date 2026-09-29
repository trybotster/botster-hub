-- Plugin test kit fixture: `log` is not a global of the sandbox. The call is
-- in a branch that no tool call reaches, so every runtime spec passes.
local function prune(id)
  if id == "never" then
    log.warn({ message = "unreachable" })
  end
end

return botster.register({
  tools = {
    {
      name = "kit-fixture-badglobal.noop",
      description = "Does nothing that fails.",
      input_schema = { type = "object" },
      handler = "noop",
      call = function()
        prune("x")
        return { ok = true }
      end,
    },
  },
})
