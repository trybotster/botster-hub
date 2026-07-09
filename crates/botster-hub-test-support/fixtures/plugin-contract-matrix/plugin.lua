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
      density = "regular",
      variant = "plain",
    },
    children = {
      {
        type = "toolbar",
        id = "contract-app-toolbar",
        props = {
          label = "Contract actions",
          density = "compact",
          variant = "plain",
        },
        slots = {
          actions = {
            {
              type = "button",
              id = "contract-app-action",
              props = {
                label = "Run contract action",
                action = {
                  id = "contract.action",
                },
              },
            },
          },
        },
      },
      {
        type = "form",
        id = "contract-app-form",
        props = {
          action = {
            id = "contract.action",
          },
        },
        children = {
          {
            type = "text_input",
            id = "contract-app-message",
            props = {
              name = "message",
              label = "Message",
              required = true,
            },
          },
          {
            type = "button",
            id = "contract-app-submit",
            props = {
              label = "Submit contract action",
              action = {
                id = "contract.action",
              },
            },
          },
        },
      },
      {
        type = "metric_grid",
        id = "contract-app-metrics",
        props = {
          density = "compact",
          variant = "subtle",
          compact = true,
        },
        children = {
          {
            type = "metric",
            id = "contract-app-render-metric",
            props = {
              label = "Render path",
              value = "validated",
              caption = "plugin_surface_render",
              tone = "success",
              status = "healthy",
            },
          },
        },
      },
      {
        type = "section",
        id = "contract-app-section",
        props = {
          title = "Application primitives",
          description = "Renderer-neutral UiNode application surface.",
        },
        children = {
          {
            type = "status_badge",
            id = "contract-app-status",
            props = {
              label = "Validated",
              status = "supported",
              tone = "success",
            },
          },
          {
            type = "table",
            id = "contract-app-table",
            props = {
              columns = {
                {
                  id = "primitive",
                  label = "Primitive",
                },
                "status",
              },
              rows = {
                {
                  id = "contract-app-row-toolbar",
                  cells = {
                    primitive = "toolbar",
                    status = "supported",
                  },
                },
              },
              empty_state = {
                type = "empty_state",
                id = "contract-app-table-empty",
                props = {
                  title = "No primitives",
                  description = "The fixture did not publish primitive rows.",
                },
              },
            },
          },
          {
            type = "empty_state",
            id = "contract-app-empty-state",
            props = {
              title = "No pending contracts",
              description = "All required application primitives validated.",
            },
          },
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

local function invalid_body_surface(_arguments)
  return {
    type = "not_a_uinode_kind",
    id = "contract-invalid-body-panel",
  }
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
  if arguments.field_error == true then
    return action_result(arguments, "error", {
      error = "message is required",
      field_errors = {
        ["contract-app-message"] = { "Message is required" },
      },
      form_errors = { "Message is required" },
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
      id = "contract_invalid_body_surface",
      kind = "surface_route",
      descriptor_id = "contract.invalid_body",
      descriptor = {
        title = "Contract Invalid Body",
        surface_id = "contract.invalid_body",
      },
      call = invalid_body_surface,
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
