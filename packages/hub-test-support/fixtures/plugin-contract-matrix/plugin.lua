-- Canonical package/plugin contract matrix fixture for hub and client conformance.

local PACKAGE = "botster.plugin-contract-matrix"
local ENTITY_OWNER = "bns1_626f74737465722e706c7567696e2d636f6e74726163742d6d6174726978"
local ENTITY_FAMILY = ENTITY_OWNER .. ".run"

local function action_result(arguments, state, extra)
  local result = {
    request_id = arguments.request_id,
    surface_id = arguments.surface_id,
    action_id = arguments.action_id,
    node_id = arguments.node_id,
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
              id = "contract-app-open",
              props = {
                label = "Open contract dialog",
                action = {
                  id = "contract.action",
                  payload = {
                    operation = "open",
                  },
                },
              },
            },
            {
              type = "button",
              id = "contract-app-toggle",
              props = {
                label = "Toggle contract state",
                action = {
                  id = "contract.action",
                  payload = {
                    operation = "toggle",
                  },
                },
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
      {
        ["$kind"] = "presentation_if",
        predicate = {
          kind = "present",
          key = "contract-dialog",
        },
        node = {
          type = "dialog",
          id = "contract-dialog",
          props = {
            title = "Contract dialog",
            presentation = "auto",
          },
          slots = {
            body = {
              {
                type = "text",
                id = "contract-dialog-body",
                props = {
                  text = "Dialog visibility follows scoped presentation state.",
                },
              },
              {
                type = "form",
                id = "contract-app-form",
                props = {
                  action = {
                    id = "contract.action",
                    payload = {
                      operation = "submit",
                    },
                  },
                  submit_label = "Submit contract action",
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
                        payload = {
                          operation = "submit",
                        },
                      },
                    },
                  },
                },
              },
            },
          },
        },
      },
      {
        ["$kind"] = "presentation_if",
        predicate = {
          kind = "truthy",
          key = "contract-toggle",
        },
        node = {
          type = "text",
          id = "contract-toggle-state",
          props = {
            text = "Contract toggle active",
          },
        },
      },
      {
        ["$kind"] = "presentation_if",
        predicate = {
          kind = "equals",
          key = "selected-workspace",
          value = "workspace-alpha",
        },
        node = {
          type = "text",
          id = "contract-selected-workspace",
          props = {
            text = "Workspace alpha selected",
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

local function session_lifecycle_surface(arguments)
  local requested = arguments.session_uuids or {}
  if type(requested) ~= "table" then
    error("session_uuids must be an array")
  end
  if #requested > 16 then
    error("session_uuids exceeds the 16-reference fixture limit")
  end

  local children = {}
  for index, session_uuid in ipairs(requested) do
    if type(session_uuid) ~= "string" or session_uuid == "" then
      error("session_uuids entries must be non-empty strings")
    end
    table.insert(children, {
      ["$kind"] = "bind_list",
      source = "/session",
      where = {
        session_uuid = session_uuid,
      },
      item_template = {
        type = "text",
        id = string.format("contract-session-%d-lifecycle", index),
        props = {
          text = {
            ["$bind"] = "@/lifecycle_class",
          },
        },
      },
      empty_template = {
        type = "text",
        id = string.format("contract-session-%d-unavailable", index),
        props = {
          text = "Session unavailable",
        },
      },
    })
  end

  table.insert(children, {
    ["$kind"] = "bind_list",
    source = "/session",
    where = {
      lifecycle_class = "current",
    },
    item_template = {
      type = "inline",
      id = {
        ["$bind"] = "@/session_uuid",
      },
      children = {
        {
          type = "button",
          id = {
            ["$kind"] = "bind_list_descendant_id",
            key = "spawn",
          },
          props = {
            label = {
              ["$bind"] = "@/lifecycle_class",
            },
            action = {
              id = "contract.action",
              payload = {
                operation = "spawn",
                session_uuid = {
                  ["$bind"] = "@/session_uuid",
                },
              },
            },
          },
        },
        {
          type = "button",
          id = {
            ["$kind"] = "bind_list_descendant_id",
            key = "rename",
          },
          props = {
            label = "Rename session",
            action = {
              id = "contract.action",
              payload = {
                operation = "rename",
                session_uuid = {
                  ["$bind"] = "@/session_uuid",
                },
              },
            },
          },
        },
        {
          type = "button",
          id = {
            ["$kind"] = "bind_list_descendant_id",
            key = "remove",
          },
          props = {
            label = "Remove session",
            action = {
              id = "contract.action",
              payload = {
                operation = "remove",
                session_uuid = {
                  ["$bind"] = "@/session_uuid",
                },
              },
            },
          },
        },
      },
    },
  })

  return {
    type = "panel",
    id = "contract-session-lifecycle-panel",
    props = {
      title = "Session lifecycle projection",
    },
    children = children,
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
  local payload = arguments.payload or {}
  local values = arguments.values or {}
  if payload.fail == true then
    return action_result(arguments, "error", {
      error = "contract action failed by request",
      form_errors = { "contract action failed by request" },
    })
  end
  if payload.identity_mismatch == true then
    local result = action_result(arguments, "accepted")
    result.request_id = "mismatched-request"
    return result
  end
  if payload.invalid_replacement == true then
    return action_result(arguments, "accepted", {
      replacement = {
        type = "form",
        id = "contract-invalid-replacement",
        props = {
          action = {
            id = "contract.action",
          },
        },
      },
    })
  end
  if payload.operation == "open" then
    return action_result(arguments, "accepted", {
      presentation = {
        {
          kind = "set",
          key = "contract-dialog",
          value = true,
        },
        {
          kind = "set",
          key = "selected-workspace",
          value = "workspace-alpha",
        },
      },
    })
  end
  if payload.operation == "toggle" then
    return action_result(arguments, "accepted", {
      presentation = {
        {
          kind = "toggle",
          key = "contract-toggle",
        },
      },
    })
  end
  if payload.operation == "spawn" or payload.operation == "rename" or payload.operation == "remove" then
    return action_result(arguments, "accepted", {
      payload = {
        operation = payload.operation,
        session_uuid = payload.session_uuid,
      },
    })
  end
  local message = values.message
  if payload.operation == "submit" and (message == nil or message:match("^%s*$")) then
    return action_result(arguments, "rejected", {
      error = "message is required",
      field_errors = {
        ["contract-app-message"] = { "Message is required" },
      },
      form_errors = { "Message is required" },
    })
  end
  return action_result(arguments, "accepted", {
    normalized_values = {
      message = values.message or "ok",
    },
    payload = {
      package_name = PACKAGE,
      message = "contract action accepted",
    },
    presentation = {
      {
        kind = "clear",
        key = "contract-dialog",
      },
    },
    replacement = {
      type = "text",
      id = "contract-action-replacement",
      props = {
        text = "Contract action accepted",
      },
    },
  })
end

local function package_entity_surface(_arguments)
  return {
    type = "panel",
    id = "contract-entities-panel",
    children = {
      {
        ["$kind"] = "bind_list",
        source = "/" .. ENTITY_FAMILY,
        item_template = {
          type = "text",
          id = { ["$bind"] = "@/id" },
          props = { text = { ["$bind"] = "@/status" } },
        },
      },
    },
  }
end

local function package_entity_snapshot(_arguments)
  local config = botster.capabilities.config.get()
  local mode = config.values.mode or {}
  local generation = 1
  if mode.value == "read" then
    generation = 2
  end
  return {
    type = "entity_snapshot",
    entity_type = ENTITY_FAMILY,
    snapshot_seq = generation,
    items = {
      {
        id = "contract-run-1",
        status = "generation-" .. generation,
        package_name = PACKAGE,
      },
    },
  }
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
      id = "contract_session_lifecycle_surface",
      kind = "surface_route",
      descriptor_id = "contract.sessions",
      descriptor = {
        title = "Session Lifecycle Projection",
        surface_id = "contract.sessions",
      },
      call = session_lifecycle_surface,
    },
    {
      id = "contract_package_entity_surface",
      kind = "surface_route",
      descriptor_id = "contract.entities",
      descriptor = {
        title = "Package Entity Projection",
        surface_id = "contract.entities",
      },
      call = package_entity_surface,
    },
    {
      id = "contract_package_entity_provider",
      kind = "entity_provider",
      descriptor_id = ENTITY_FAMILY,
      descriptor = {
        entity_type = ENTITY_FAMILY,
        id_field = "id",
      },
      call = package_entity_snapshot,
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
