#!/usr/bin/env tarantool

local http_router = require("http.router")
local http_server = require("http.server")
local json = require("json")
local log = require("log")

local httpd = http_server.new("127.0.0.1", 8080, {
    log_requests = true,
    log_errors = true
})
local router = http_router.new()

box.cfg{
    listen = 3301,
    log_level = 5,
    log = "logs/server_log.log"
}


box.once("initialize", function()
    box.schema.space.create("kv_storage")
    box.space.kv_storage:format({
        {name = "key", type = "string"},
        {name = "value"}
    })
    box.space.kv_storage:create_index("primary", {
        type = "hash",
        parts = {"key"}
    })
end)


function get_value(req)
    local key = req:stash("key")
    local value = box.space.kv_storage:select(key)
    if next(value) == nil then
        local res_err = req:render({json = { error = "Not found"}})
        res_err.status = 404
        return res_err
    end 
    local res_success = req:render({json = { value = value[1]['value']}})
    res_success.status = 200
    return res_success
end


function insert_value(req)
    local decode_status, json_data = pcall(function()
        return req:json()
    end)

    if json_data == nil or json_data.key == nil or type(json_data.key) ~= 'string' 
                                        or json_data.value == nil or not decode_status then
        local res_error = req:render({json = { error = "Bad request"}})
        res_error.status = 400
        return res_error
    end
    local insert_status = pcall(function()
        box.space.kv_storage:insert{json_data.key, json_data.value}
    end)

    if not insert_status then
        local res_error = req:render({json = { error = "Conflict"}})
        res_error.status = 409
        return res_error
    end

    local res_success = req:render({json = { status = "Created"}})
    res_success.status = 201
    return res_success
end


function update_value(req)
    local key = req:stash("key")
    local status, json_data = pcall(function()
        return req:json()
    end)

    if json_data == nil or json_data.value == nil or not status then
        local res_error = req:render({json = { error = "Bad request"}})
        res_error.status = 400
        return res_error
    end

    local res = box.space.kv_storage:update({key}, {{"=", 2, json_data.value}})
    if res == nil then
        local res_error = req:render({json = { error = "Not found"}})
        res_error.status = 404
        return res_error
    end
    local res_success = req:render({json = { status = "Updated"}})
    res_success.status = 200
    return res_success
end


function delete_value(req)
    local key = req:stash("key")
    
    local res = box.space.kv_storage:delete{key}
    if res == nil then 
        local res_error = req:render({json = { error = "Not found"}})
        res_error.status = 404
        return res_error
    end

    local res_success = req:render({json = { status = "Deleted"}})
    res_success.status = 200
    return res_success
end


router:route({path = "/kv/:key", method = "GET"}, get_value)
router:route({path = "/kv", method = "POST"}, insert_value)
router:route({path = "/kv/:key", method = "PUT"}, update_value)
router:route({path = "/kv/:key", method = "DELETE"}, delete_value)


httpd:set_router(router)
httpd:start()


