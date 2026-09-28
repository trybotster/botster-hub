-- Plugin test kit fixture: one tool arms a Hub timer, one reads the clock.
return botster.register({
  tools = {
    {
      name = "kit-fixture-timer.arm",
      description = "Arm a one-shot Hub timer.",
      input_schema = {
        type = "object",
        properties = { delay_ms = { type = "integer" } },
        required = { "delay_ms" },
      },
      handler = "arm",
      call = function(request)
        return botster.capabilities.timer_once(request.delay_ms)
      end,
    },
    {
      name = "kit-fixture-timer.clock",
      description = "Read botster.clock.",
      input_schema = { type = "object" },
      handler = "clock",
      call = function()
        return { now = botster.clock.now().value, monotonic = botster.clock.monotonic().value }
      end,
    },
  },
})
