local received = {}

events.on("event-plane-producer", "sample.ready", function(event)
  received[#received + 1] = event
end)

-- Second handler for the same event. The original accumulator above is
-- unchanged. This body stays empty of busy-loops (the Lua instruction budget
-- would abort a long loop before the 1000 ms invocation timeout).
events.on("event-plane-producer", "sample.ready", function(event)
  return { slow = true, token = event.token }
end)

events.on("hub", "worktree_created", function(event)
  local n = 0
  while n < 8000000 do
    n = n + 1
  end
  return { delayed = true, worktree_id = event.worktree_id }
end)

return botster.register({
  tools = {
    {
      name = "event_plane.last_received",
      description = "Return events received from the producer.",
      input_schema = {
        type = "object",
        additionalProperties = false,
      },
      handler = "last_received",
      call = function()
        return { count = #received, last = received[#received] }
      end,
    },
  },
})
