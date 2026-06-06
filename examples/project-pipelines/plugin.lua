-- Project Pipelines local plugin entrypoint.

local PLUGIN = "project-pipelines"
local STATE_KEY = "state"

local function empty_schema()
  return {
    type = "object",
    properties = {},
    additionalProperties = false,
  }
end

local function default_state()
  return {
    tickets = {},
    runs = {},
    gates = {},
    events = {},
    next_ticket = 0,
    next_run = 0,
    next_step = 0,
  }
end

local function load_state()
  local ok, result = pcall(botster.capabilities.plugin_db.get, { key = STATE_KEY })
  if not ok then
    return default_state()
  end
  return result.record.payload
end

local function persist_state(state)
  return pcall(botster.capabilities.plugin_db.set, {
    key = STATE_KEY,
    schema_version = 1,
    payload = state,
  })
end

local function persist_failed(error)
  return {
    ok = false,
    error = {
      code = "persist_failed",
      message = "failed to persist Project Pipelines state: " .. tostring(error),
    },
  }
end

local function save_or_error(state)
  local ok, result = persist_state(state)
  if not ok then
    return persist_failed(result)
  end
  return nil
end

local function string_arg(arguments, key)
  local value = arguments[key]
  if type(value) == "string" then
    return value
  end
  return nil
end

local function missing_arg(key)
  return {
    ok = false,
    error = {
      code = "missing_argument",
      message = "missing required argument: " .. key,
    },
  }
end

local function not_found(kind, id)
  return {
    ok = false,
    error = {
      code = "not_found",
      message = kind .. " not found: " .. id,
    },
  }
end

local function push_event(state, kind, payload)
  table.insert(state.events, {
    kind = kind,
    payload = payload,
    created_at = 0,
  })
end

local function create(arguments)
  local state = load_state()
  local title = string_arg(arguments, "title") or "Untitled local ticket"
  local pipeline_id = string_arg(arguments, "pipeline_id") or "local_pipeline"
  state.next_ticket = state.next_ticket + 1
  local ticket = {
    id = "ticket_local_" .. state.next_ticket,
    title = title,
    status = "open",
    pipeline_id = pipeline_id,
    created_at = 0,
  }
  push_event(state, "ticket.created", { ticket_id = ticket.id })
  table.insert(state.tickets, ticket)
  local error = save_or_error(state)
  if error then
    return error
  end
  return { ok = true, ticket = ticket }
end

local function list()
  local state = load_state()
  return {
    ok = true,
    tickets = state.tickets,
    runs = state.runs,
    gates = state.gates,
    events = state.events,
  }
end

local function update(arguments)
  local state = load_state()
  local ticket_id = string_arg(arguments, "ticket_id")
  if not ticket_id then
    return missing_arg("ticket_id")
  end
  local ticket = nil
  for _, candidate in ipairs(state.tickets) do
    if candidate.id == ticket_id then
      ticket = candidate
      break
    end
  end
  if not ticket then
    return not_found("ticket", ticket_id)
  end
  ticket.title = string_arg(arguments, "title") or ticket.title
  ticket.status = string_arg(arguments, "status") or ticket.status
  push_event(state, "ticket.updated", { ticket_id = ticket.id })
  local error = save_or_error(state)
  if error then
    return error
  end
  return { ok = true, ticket = ticket }
end

local function coordination_for(ticket_id, run_number, target_id, worktree, agent_name)
  local request_id = "project-pipelines:" .. ticket_id .. ":" .. run_number
  local target = { type = "plugin", plugin_key = PLUGIN }
  local envelope_id = "project-pipelines-run:" .. request_id
  local extension = {
    request_id = request_id,
    target_id = target_id,
    assigned_worktree = worktree,
    owner_plugin = PLUGIN,
    agent_name = agent_name,
  }
  local published = botster.coordination.publish({
    id = envelope_id,
    target = target,
    content_type = "application/vnd.botster.project-pipelines.start+json",
    body = "project-pipelines-run-start",
    extension = extension,
    created_at = 0,
  })
  local drained = botster.coordination.drain({ target = target, limit = 1 })
  local envelope = drained.envelopes[1]
  local primitive = envelope.payload.extension
  local acknowledged = botster.coordination.acknowledge({
    target = target,
    envelope_id = envelope.id,
  })
  return {
    target_id = primitive.target_id,
    assigned_worktree = primitive.assigned_worktree,
    request_id = primitive.request_id,
    owner_plugin = primitive.owner_plugin,
    agent_name = primitive.agent_name,
    -- This constrained local start records primitive-backed coordination before
    -- any agent session is spawned, so there is no real session UUID to attach.
    session_uuid = nil,
    envelope_id = envelope.id,
    publish_delivery_status = published.deliveries[1].status,
    drain_cursor = envelope.cursor,
    ack_delivery_status = acknowledged.state.status,
  }
end

local function start(arguments)
  local state = load_state()
  local ticket_id = string_arg(arguments, "ticket_id")
  if not ticket_id then
    return missing_arg("ticket_id")
  end
  local exists = false
  for _, ticket in ipairs(state.tickets) do
    if ticket.id == ticket_id then
      exists = true
      break
    end
  end
  if not exists then
    return not_found("ticket", ticket_id)
  end
  local target_id = string_arg(arguments, "target_id")
  if not target_id then
    return missing_arg("target_id")
  end
  local worktree = string_arg(arguments, "worktree")
  if not worktree then
    return missing_arg("worktree")
  end
  local agent_name = string_arg(arguments, "agent_name") or "codex"
  state.next_run = state.next_run + 1
  state.next_step = state.next_step + 1
  local coordination = coordination_for(ticket_id, state.next_run, target_id, worktree, agent_name)
  local run = {
    id = "run_local_" .. state.next_run,
    ticket_id = ticket_id,
    status = "active",
    current_step_id = "step_local_" .. state.next_step,
    coordination = coordination,
  }
  push_event(state, "run.started", {
    run_id = run.id,
    request_id = coordination.request_id,
    target_id = coordination.target_id,
    assigned_worktree = coordination.assigned_worktree,
    owner_plugin = coordination.owner_plugin,
    envelope_id = coordination.envelope_id,
    ack_delivery_status = coordination.ack_delivery_status,
  })
  table.insert(state.runs, run)
  local error = save_or_error(state)
  if error then
    return error
  end
  return { ok = true, run = run }
end

local function submit_gate(arguments)
  local state = load_state()
  local run_id = string_arg(arguments, "run_id")
  if not run_id then
    return missing_arg("run_id")
  end
  local exists = false
  for _, run in ipairs(state.runs) do
    if run.id == run_id then
      exists = true
      break
    end
  end
  if not exists then
    return not_found("run", run_id)
  end
  local gate_id = string_arg(arguments, "gate_id")
  if not gate_id then
    return missing_arg("gate_id")
  end
  local status = string_arg(arguments, "status")
  if not status then
    return missing_arg("status")
  end
  local gate = {
    run_id = run_id,
    gate_id = gate_id,
    status = status,
    summary = string_arg(arguments, "summary"),
    evidence = arguments.evidence or {},
    created_at = 0,
  }
  push_event(state, "gate.submitted", { run_id = run_id, gate_id = gate_id, status = status })
  table.insert(state.gates, gate)
  local error = save_or_error(state)
  if error then
    return error
  end
  return { ok = true, gate = gate }
end

local function request_step_advance(arguments)
  local state = load_state()
  local run_id = string_arg(arguments, "run_id")
  if not run_id then
    return missing_arg("run_id")
  end
  local run = nil
  for _, candidate in ipairs(state.runs) do
    if candidate.id == run_id then
      run = candidate
      break
    end
  end
  if not run then
    return not_found("run", run_id)
  end
  run.status = "ready_for_review"
  push_event(state, "step.advance_requested", {
    run_id = run_id,
    summary = string_arg(arguments, "summary"),
  })
  local error = save_or_error(state)
  if error then
    return error
  end
  return { ok = true, run = run }
end

return botster.register({
  tools = {
    {
      name = "project_pipelines.create",
      description = "Create a constrained local Project Pipelines ticket.",
      input_schema = {
        type = "object",
        properties = {
          title = { type = "string" },
          pipeline_id = { type = "string" },
        },
        required = { "title" },
        additionalProperties = false,
      },
      handler = "create",
      call = create,
    },
    {
      name = "project_pipelines.list",
      description = "List constrained local Project Pipelines records.",
      input_schema = empty_schema(),
      handler = "list",
      call = list,
    },
    {
      name = "project_pipelines.update",
      description = "Update a constrained local Project Pipelines ticket.",
      input_schema = {
        type = "object",
        properties = {
          ticket_id = { type = "string" },
          title = { type = "string" },
          status = { type = "string" },
        },
        required = { "ticket_id" },
        additionalProperties = false,
      },
      handler = "update",
      call = update,
    },
    {
      name = "project_pipelines.start",
      description = "Start a constrained local Project Pipelines run.",
      input_schema = {
        type = "object",
        properties = {
          ticket_id = { type = "string" },
          target_id = { type = "string" },
          worktree = { type = "string" },
          agent_name = { type = "string" },
        },
        required = { "ticket_id", "target_id", "worktree" },
        additionalProperties = false,
      },
      handler = "start",
      call = start,
    },
    {
      name = "project_pipelines.current_context",
      description = "Return constrained local Project Pipelines context.",
      input_schema = empty_schema(),
      handler = "current_context",
      call = list,
    },
    {
      name = "project_pipelines.submit_gate",
      description = "Record gate evidence for a constrained local run.",
      input_schema = {
        type = "object",
        properties = {
          run_id = { type = "string" },
          gate_id = { type = "string" },
          status = { type = "string" },
          summary = { type = "string" },
          evidence = { type = "object" },
        },
        required = { "run_id", "gate_id", "status" },
        additionalProperties = false,
      },
      handler = "submit_gate",
      call = submit_gate,
    },
    {
      name = "project_pipelines.request_step_advance",
      description = "Advance a constrained local run step.",
      input_schema = {
        type = "object",
        properties = {
          run_id = { type = "string" },
          summary = { type = "string" },
        },
        required = { "run_id" },
        additionalProperties = false,
      },
      handler = "request_step_advance",
      call = request_step_advance,
    },
  },
})
