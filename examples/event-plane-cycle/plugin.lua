local function emit_status(result)
  if result.ok then return result.value.status end
  return result.error.detail.status
end

local family = "event-plane-cycle.probe"
local handler_status = "none"
local provider_status = "none"

botster.events.on({ owner = "event-plane-producer", name = "sample.ready" }, function(event)
  botster.entity_publish({
    type = "entity_upsert",
    entity_type = family,
    snapshot_seq = 32,
    id = "gap",
    entity = { id = "gap", token = event.token or "live" },
  })
  handler_status = emit_status(
    botster.events.emit({ name = "cycle.probe", payload = { ok = true, token = "handler" } })
  )
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
        provider_status = emit_status(
          botster.events.emit({ name = "cycle.probe", payload = { ok = true, token = "provider" } })
        )
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
