-- Plugin test kit fixture: a consumer of kit-fixture's plugin-audience event.
-- It records each delivery in plugin_db. It may load before the producer.
local plugin_db = botster.capabilities.plugin_db

botster.events.on({ owner = "kit-fixture", name = "kit-fixture.noted" }, function(event)
  local current = plugin_db.get({ key = "seen" })
  local items = {}
  if current.record then
    items = current.record.payload.items
  end
  items[#items + 1] = event.key
  plugin_db.set({ key = "seen", schema_version = 1, payload = { items = items } })
  return { ok = true }
end)

return botster.register({
  tools = {
    {
      name = "kit-fixture-consumer.seen",
      description = "The keys this consumer has received.",
      input_schema = { type = "object" },
      handler = "seen",
      call = function()
        local current = plugin_db.get({ key = "seen" })
        if current.record then
          return { items = current.record.payload.items }
        end
        return { items = botster.json.decode({ text = "[]" }).value }
      end,
    },
  },
})
