local PAYLOAD_PAD = string.rep("x", 4096)

local function emit_one(token, subject, notice, pad)
  local payload = {
    ok = true,
    token = token or "live",
  }
  if subject ~= nil then
    payload.subject = subject
  end
  if notice ~= nil then
    payload.notice = notice
  end
  if pad ~= nil then
    payload.pad = pad
  end
  return events.emit("sample.ready", payload)
end

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
        return emit_one(args.token, args.subject, args.notice, nil)
      end,
    },
    {
      name = "event_plane.emit_burst",
      description = "Emit a bounded burst of sample.ready events for saturation.",
      input_schema = {
        type = "object",
        additionalProperties = false,
        properties = {
          count = { type = "integer" },
          prefix = { type = "string" },
        },
      },
      handler = "emit_burst",
      call = function(args)
        local count = args.count or 25
        if count < 1 then
          count = 1
        end
        if count > 25 then
          count = 25
        end
        local prefix = args.prefix or "burst"
        local last = nil
        for i = 1, count do
          last = emit_one(prefix .. "-" .. tostring(i), nil, nil, PAYLOAD_PAD)
        end
        return { count = count, last = last }
      end,
    },
  },
})
