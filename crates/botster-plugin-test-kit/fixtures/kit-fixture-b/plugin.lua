-- Plugin test kit fixture: a second producer of plugin-audience events.
return botster.register({
  tools = {
    {
      name = "kit-fixture-b.note",
      description = "Emit kit-fixture-b.noted for one key.",
      input_schema = {
        type = "object",
        properties = { key = { type = "string" } },
        required = { "key" },
      },
      handler = "note",
      call = function(request)
        local emitted = botster.events.emit({ name = "kit-fixture-b.noted", payload = { key = request.key } })
        return { emitted = emitted.ok }
      end,
    },
  },
})
