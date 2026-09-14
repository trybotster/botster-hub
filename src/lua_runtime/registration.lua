local globals, null = ...
local value_type, raise, protected = type, error, pcall
local math_type, raw_equal = math.type, rawequal
local raw_length, raw_get = rawlen, rawget
local string_find, string_byte, stringify = string.find, string.byte, tostring
local utf8_length, utf8_codes = utf8.len, utf8.codes

local function type_name(value)
    local kind = value_type(value)
    if kind == 'number' then
        return math_type(value) == 'integer' and 'integer' or 'number'
    end
    if kind == 'userdata' and raw_equal(value, null) then return 'lightuserdata' end
    -- Lua reports non-null userdata without inspecting Rust error values.
    return kind
end

local function require_type(value, expected, argument)
    if value_type(value) ~= expected then
        local message = 'error converting Lua ' .. type_name(value) .. ' to ' .. expected
        if argument then message = 'bad argument #' .. argument .. ': ' .. message end
        raise(message, 0)
    end
    return value
end

local function require_utf8(value, target)
    local length, index = utf8_length(value)
    if length then return value end
    local first = string_byte(value, index)
    local width = first >= 0xc2 and first <= 0xdf and 2
        or first >= 0xe0 and first <= 0xef and 3
        or first >= 0xf0 and first <= 0xf4 and 4
        or 1
    local invalid = 1
    if width > 1 then
        for offset = 1, width - 1 do
            local byte = string_byte(value, index + offset)
            if not byte then invalid = nil; break end
            if byte < 0x80 or byte > 0xbf
                or offset == 1 and (first == 0xe0 and byte < 0xa0
                    or first == 0xed and byte > 0x9f
                    or first == 0xf0 and byte < 0x90
                    or first == 0xf4 and byte > 0x8f) then
                invalid = offset
                break
            end
        end
    end
    local detail
    if invalid then
        detail = 'invalid utf-8 sequence of ' .. invalid .. ' bytes from index ' .. (index - 1)
    else
        detail = 'incomplete utf-8 byte sequence from index ' .. (index - 1)
    end
    raise('error converting Lua string to ' .. target .. ' (' .. detail .. ')', 0)
end

local function string_value(value)
    local kind = value_type(value)
    if kind == 'number' then value = stringify(value)
    elseif kind ~= 'string' then
        raise('error converting Lua ' .. type_name(value) .. ' to String (expected string or number)', 0)
    end
    return require_utf8(value, 'String')
end

-- These are the Unicode White_Space characters used by Rust str::trim.
local function blank(value)
    for _, code in utf8_codes(value) do
        if not (code >= 0x09 and code <= 0x0d or code == 0x20
            or code == 0x85 or code == 0xa0 or code == 0x1680
            or code >= 0x2000 and code <= 0x200a or code == 0x2028
            or code == 0x2029 or code == 0x202f or code == 0x205f or code == 0x3000) then
            return false
        end
    end
    return true
end

local function table_field(value, key)
    local ok, field = protected(function() return value[key] end)
    if ok and value_type(field) == 'table' then return field end
end

local function sequence(value)
    local index = 0
    return function()
        index = index + 1
        local item = raw_get(value, index)
        if item == nil then return end
        return require_type(item, 'table')
    end
end

local function on(owner, name, handler)
    if value_type(owner) ~= 'string' or value_type(name) ~= 'string'
        or value_type(handler) ~= 'function' then
        raise('rejected_invalid', 0)
    end
    require_utf8(owner, '&str')
    require_utf8(name, '&str')
    if blank(owner) or blank(name) or string_find(owner, '*', 1, true)
        or string_find(name, '*', 1, true) or string_find(owner, '?', 1, true)
        or string_find(name, '?', 1, true) then
        raise('rejected_wildcard', 0)
    end
    local registration = require_type(globals.__botster_registration, 'table')
    local handler_table = table_field(registration, 'handlers')
    if not handler_table then
        handler_table = {}
        registration.handlers = handler_table
    end
    local handler_id = 'event:' .. owner .. ':' .. name .. ':' .. (raw_length(handler_table) + 1)
    local handlers = require_type(globals.__botster_handlers, 'table')
    handlers[handler_id] = handler
    local entry = { id = handler_id, kind = 'event', event_owner = owner, event = name }
    handler_table[raw_length(handler_table) + 1] = entry
end

local function register(registration)
    require_type(registration, 'table', 1)
    local handlers = require_type(globals.__botster_handlers, 'table')
    local pending = require_type(globals.__botster_registration, 'table')
    local pending_handlers = table_field(pending, 'handlers')
    if pending_handlers then
        local custom_handlers = table_field(registration, 'handlers')
        if not custom_handlers then
            custom_handlers = {}
            registration.handlers = custom_handlers
        end
        local index = raw_length(custom_handlers)
        for pending_handler in sequence(pending_handlers) do
            index = index + 1
            custom_handlers[index] = pending_handler
        end
    end
    local tools = table_field(registration, 'tools')
    if tools then
        for tool in sequence(tools) do
            local handler_id = string_value(tool.handler)
            local handler = require_type(tool.call, 'function')
            handlers[handler_id] = handler
        end
    end
    local custom_handlers = table_field(registration, 'handlers')
    if custom_handlers then
        for custom_handler in sequence(custom_handlers) do
            local handler_id = string_value(custom_handler.id)
            local ok, handler = protected(function()
                return require_type(custom_handler.call, 'function')
            end)
            if ok then handlers[handler_id] = handler
            else
                local ok, kind = protected(function() return string_value(custom_handler.kind) end)
                if ok and kind == 'entity_provider' then
                    raise('entity provider declarations require a call handler', 0)
                end
            end
        end
    end
    globals.__botster_registration = registration
    return registration
end

return on, register
