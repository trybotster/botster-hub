local family = "event-plane-cycle.probe"
local handler_status = "none"
local provider_status = "none"

events.on("event-plane-producer", "sample.ready", function(event)
  botster.entity_publish({
    type = "entity_upsert",
    entity_type = family,
    snapshot_seq = 32,
    id = "gap",
    entity = { id = "gap", token = event.token or "live" },
  })
  local result = events.emit("cycle.probe", { ok = true, token = "handler" })
  handler_status = result.status
end)

return botster.register({
  tools = {
    {
      name = "event_plane.cycle_status",
      description = "Return causal-scope emit statuses from the event handler and later provider.",
      input_schema = { type = "object", additionalProperties = false },
      handler = "cycle_status",
      call = function()
        return {
          handler_status = handler_status,
          provider_status = provider_status,
        }
      end,
    },
  },
  handlers = {
    {
      id = "probe",
      kind = "entity_provider",
      descriptor_id = family,
      descriptor = { entity_type = family, id_field = "id" },
      call = function()
        local result = events.emit("cycle.probe", { ok = true, token = "provider" })
        provider_status = result.status
        return {
          type = "entity_snapshot",
          entity_type = family,
          snapshot_seq = 32,
          items = { { id = "gap", token = "provider" } },
        }
      end,
    },
  },
})
