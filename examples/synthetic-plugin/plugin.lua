-- Local-only synthetic package fixture for the botster-hub runtime proof.
botster.events.on({ owner = "hub", name = "worktree_created" }, function(event)
  return {
    observed = "worktree_created",
    worktree_id = event.worktree_id,
    target_id = event.target_id,
  }
end)

return botster.register({
  tools = {
    {
      name = "runtime.synthetic.echo",
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
        local config = botster.capabilities.config.get().value
        local cross_package = botster.capabilities.config.get("other.package")
        local cross_package_ok = cross_package.ok
        local cross_package_value = cross_package.error and cross_package.error.kind
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
