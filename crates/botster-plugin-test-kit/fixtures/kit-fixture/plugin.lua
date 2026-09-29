-- Plugin test kit fixture. Each output is observable through the kit.
local plugin_db = botster.capabilities.plugin_db

local function append(key, item)
  local current = plugin_db.get({ key = key })
  local items = {}
  if current.record then
    items = current.record.payload.items
  end
  items[#items + 1] = item
  plugin_db.set({ key = key, schema_version = 1, payload = { items = items } })
end

-- A downstream handler: it runs only after the emitting tool's event is
-- delivered by the Hub's package event router.
botster.events.on({ owner = "kit-fixture", name = "kit-fixture.noted" }, function(event)
  append("noted", event.key)
  return { ok = true }
end)

-- Session lifecycle frames, in delivery order.
botster.events.on({ owner = "hub", name = "session_family" }, function(frame)
  local item = { type = frame.type }
  if frame.type == "entity_upsert" then
    item.id = frame.id
    item.lifecycle_class = frame.entity.lifecycle_class
  elseif frame.type == "entity_remove" then
    item.id = frame.id
  elseif frame.type == "snapshot_chunk" then
    item.ids = {}
    for _, entity in ipairs(frame.items) do
      item.ids[#item.ids + 1] = entity.session_uuid .. ":" .. entity.lifecycle_class
    end
  end
  append("family", item)
  return { ok = true }
end)

local published = 0

return botster.register({
  handlers = {
    {
      id = "items",
      kind = "entity_provider",
      descriptor_id = "kit-fixture.item",
      descriptor = { entity_type = "kit-fixture.item", id_field = "id" },
      call = function()
        return { type = "entity_snapshot", entity_type = "kit-fixture.item", snapshot_seq = 0, items = {} }
      end,
    },
  },
  tools = {
    {
      name = "kit-fixture.publish",
      description = "Publish one kit-fixture.item entity.",
      input_schema = {
        type = "object",
        properties = { id = { type = "string" }, label = { type = "string" } },
        required = { "id", "label" },
      },
      handler = "publish",
      call = function(request)
        published = published + 1
        return botster.entity_publish({
          type = "entity_upsert",
          entity_type = "kit-fixture.item",
          snapshot_seq = published,
          id = request.id,
          entity = { id = request.id, label = request.label },
        })
      end,
    },
    {
      name = "kit-fixture.remember",
      description = "Store one value in plugin_db.",
      input_schema = {
        type = "object",
        properties = { key = { type = "string" }, value = { type = "string" } },
        required = { "key", "value" },
      },
      handler = "remember",
      call = function(request)
        plugin_db.set({ key = request.key, schema_version = 1, payload = { value = request.value } })
        botster.log.info({ message = "remembered", fields = { key = request.key } })
        return { stored = request.key }
      end,
    },
    {
      name = "kit-fixture.whoami",
      description = "Return the caller the Hub set for this call.",
      input_schema = { type = "object" },
      handler = "whoami",
      -- The Hub passes the caller in the second argument, as `request.caller`.
      call = function(_, request)
        return { caller = request.caller }
      end,
    },
    {
      name = "kit-fixture.log_many",
      description = "Write many log records.",
      input_schema = {
        type = "object",
        properties = { count = { type = "integer" } },
        required = { "count" },
      },
      handler = "log_many",
      call = function(request)
        for index = 1, request.count do
          botster.log.info({ message = "record", fields = { index = index } })
        end
        return { logged = request.count }
      end,
    },
    {
      name = "kit-fixture.note",
      description = "Emit kit-fixture.noted for one key.",
      input_schema = {
        type = "object",
        properties = { key = { type = "string" } },
        required = { "key" },
      },
      handler = "note",
      call = function(request)
        local emitted = botster.events.emit({ name = "kit-fixture.noted", payload = { key = request.key } })
        return { emitted = emitted.ok }
      end,
    },
    {
      name = "kit-fixture.emit_undeclared",
      description = "Emit an event the manifest does not declare.",
      input_schema = { type = "object" },
      handler = "emit_undeclared",
      call = function()
        return botster.events.emit({ name = "kit-fixture.undeclared", payload = {} })
      end,
    },
    {
      name = "kit-fixture.route",
      description = "Publish one routed envelope to a target.",
      input_schema = { type = "object" },
      handler = "route",
      call = function(args)
        return botster.coordination.publish({
          id = args.envelope_id,
          target = args.target,
          body = args.body,
          content_type = "text/plain",
          created_at = 1,
        })
      end,
    },
    {
      name = "kit-fixture.read",
      description = "Read one plugin_db record.",
      input_schema = {
        type = "object",
        properties = { key = { type = "string" } },
        required = { "key" },
      },
      handler = "read",
      call = function(request)
        local result = plugin_db.get({ key = request.key })
        if result.record then
          return { payload = result.record.payload }
        end
        return { missing = true }
      end,
    },
  },
})
