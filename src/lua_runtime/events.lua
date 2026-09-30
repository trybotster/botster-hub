-- botster.events: the table-first, result-shaped event API over the Hub's
-- registration shim (`on`) and event-plane ingress (`emit`).
local raw = ...
local raw_on, raw_emit = raw.on, raw.emit
local pcall, error, type = pcall, error, type

-- Event-plane statuses mapped to the platform's error kinds; any other
-- rejection is an invalid request. The exact status stays in `detail`.
local kinds = {
    rejected_undeclared = "capability_denied",
    rejected_foreign = "capability_denied",
    rejected_audience = "capability_denied",
    shed_full = "backpressured",
    shed_busy = "backpressured",
}

local function failure(status)
    local kind = kinds[status] or "invalid_request"
    return {
        ok = false,
        error = {
            kind = kind,
            message = "event " .. status,
            retryable = kind == "backpressured",
            detail = { status = status },
        },
    }
end

local events = {}

-- botster.events.on({ owner = <string>, name = <string> }, handler)
function events.on(spec, handler)
    if type(spec) ~= "table" then
        return failure("rejected_invalid")
    end
    local ok, message = pcall(raw_on, spec.owner, spec.name, handler)
    if ok then
        return { ok = true }
    end
    if message == "rejected_invalid" or message == "rejected_wildcard" then
        return failure(message)
    end
    -- An error raised by the plugin's own code (for example a metamethod)
    -- stays the plugin's error.
    error(message, 0)
end

-- botster.events.emit({ name = <string>, payload = <any> })
function events.emit(spec)
    if type(spec) ~= "table" or type(spec.name) ~= "string" then
        return failure("rejected_invalid")
    end
    local result = raw_emit(spec.name, spec.payload)
    if result.status == "accepted" then
        return { ok = true, value = { status = "accepted" } }
    end
    return failure(result.status)
end

return events
