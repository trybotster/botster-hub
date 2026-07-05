-- Canonical package/plugin contract matrix fixture for hub and client conformance.

local PACKAGE = "botster.plugin-contract-matrix"

local function action_result(arguments, state, extra)
  local result = {
    request_id = arguments.request_id or "plugin-contract-matrix-action",
    surface_id = "contract.app",
    action_id = "contract.action",
    node_id = "contract-app-action",
    state = state,
  }
  for key, value in pairs(extra or {}) do
    result[key] = value
  end
  return result
end

local function app_surface(_arguments)
  return {
    type = "panel",
    id = "contract-app-panel",
    props = {
      title = "Plugin Contract Matrix",
    },
    children = {
      {
        type = "text",
        id = "contract-app-summary",
        props = {
          text = "UiNode payload delivered through plugin_surface_render.",
        },
      },
      {
        type = "button",
        id = "contract-app-action",
        props = {
          label = "Run contract action",
          action = "contract.action",
        },
      },
    },
  }
end

local function empty_surface(_arguments)
  return {
    type = "panel",
    id = "contract-empty-panel",
    props = {
      title = "No contract rows",
    },
    children = {
      {
        type = "text",
        id = "contract-empty-message",
        props = {
          text = "No fixture rows are available.",
        },
      },
    },
  }
end

local function blocked_surface(_arguments)
  error("contract matrix blocked render")
end

local function settings_surface(_arguments)
  local config = botster.capabilities.config.get()
  local endpoint = config.values.endpoint or {}
  local mode = config.values.mode or {}
  local token = config.values.api_token or {}
  local text = string.format(
    "endpoint=%s mode=%s api_token_state=%s",
    endpoint.value or "",
    mode.value or "",
    token.state or ""
  )
  return {
    type = "panel",
    id = "contract-settings-panel",
    props = {
      title = "Contract Settings",
    },
    children = {
      {
        type = "text",
        id = "contract-settings-summary",
        props = {
          text = text,
        },
      },
    },
  }
end

local function contract_action(arguments)
  if arguments.fail == true then
    return action_result(arguments, "error", {
      error = "contract action failed by request",
      form_errors = { "contract action failed by request" },
    })
  end
  return action_result(arguments, "accepted", {
    normalized_values = {
      message = arguments.message or "ok",
    },
    payload = {
      package_name = PACKAGE,
      message = "contract action accepted",
    },
  })
end

return botster.register({
  handlers = {
    {
      id = "contract_app_surface",
      kind = "surface_route",
      descriptor_id = "contract.app",
      descriptor = {
        title = "Contract App",
        surface_id = "contract.app",
      },
      call = app_surface,
    },
    {
      id = "contract_empty_surface",
      kind = "surface_route",
      descriptor_id = "contract.empty",
      descriptor = {
        title = "Contract Empty State",
        surface_id = "contract.empty",
      },
      call = empty_surface,
    },
    {
      id = "contract_blocked_surface",
      kind = "surface_route",
      descriptor_id = "contract.blocked",
      descriptor = {
        title = "Contract Blocked Surface",
        surface_id = "contract.blocked",
      },
      call = blocked_surface,
    },
    {
      id = "contract_settings_surface",
      kind = "surface_route",
      descriptor_id = "contract.settings",
      descriptor = {
        title = "Contract Settings",
        surface_id = "contract.settings",
      },
      call = settings_surface,
    },
    {
      id = "contract_action",
      kind = "ui_action",
      descriptor_id = "contract.action",
      descriptor = {
        action_id = "contract.action",
        surface_id = "contract.app",
      },
      call = contract_action,
    },
  },
})
