return botster.register({
  tools = {
    {
      name = "event_plane.emit_ready",
      description = "Emit the declared sample.ready event.",
      input_schema = {
        type = "object",
        additionalProperties = false,
        properties = {
          token = { type = "string" },
        },
      },
      handler = "emit_ready",
      call = function(args)
        return events.emit("sample.ready", {
          ok = true,
          token = args.token or "live",
        })
      end,
    },
  },
})
