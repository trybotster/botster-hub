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
        local config = botster.capabilities.config.get()
        local cross_package_ok, cross_package_value = pcall(function()
          return botster.capabilities.config.get("other.package")
        end)
        return {
          message = args.message or "empty",
          capability = timer,
          config = config,
          cross_package_config_attempt = {
            ok = cross_package_ok,
            value = tostring(cross_package_value),
          },
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
