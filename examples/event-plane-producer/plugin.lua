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
          subject = { type = "string" },
          notice = { type = "string" },
        },
      },
      handler = "emit_ready",
      call = function(args)
        local payload = {
          ok = true,
          token = args.token or "live",
        }
        if args.subject ~= nil then
          payload.subject = args.subject
        end
        if args.notice ~= nil then
          payload.notice = args.notice
        end
        return events.emit("sample.ready", payload)
      end,
    },
  },
})
