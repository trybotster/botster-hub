-- Local-only synthetic package fixture for the botster-hub dogfood proof.
return botster.register({
  tools = {
    {
      name = "dogfood.synthetic.echo",
      description = "Echo test input and prove a hub capability primitive.",
      input_schema = {
        type = "object",
        properties = {
          message = { type = "string" },
        },
        additionalProperties = false,
      },
      handler = "echo",
      call = function(args)
        local timer = botster.capabilities.timer_once(1)
        return {
          message = args.message or "empty",
          capability = timer,
          ambient = {
            os = os == nil,
            io = io == nil,
            package = package == nil,
          },
        }
      end,
    },
  },
})
