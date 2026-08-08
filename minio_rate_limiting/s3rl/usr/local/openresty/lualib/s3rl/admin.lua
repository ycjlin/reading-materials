-- =============================================================================
-- /usr/local/openresty/lualib/s3rl/admin.lua
--   GET  /-/s3rl/config          目前生效設定
--   PUT  /-/s3rl/config          原子寫入新設定（tmp + rename），5s 內全 worker 生效
--   POST /-/s3rl/reload          強制重讀
--   GET  /-/s3rl/policy?bucket=&op=   查某 bucket 實際套到的 policy
--   GET  /-/s3rl/metrics         Prometheus
--   POST /-/s3rl/metrics/reset
-- =============================================================================

local cjson   = require "cjson.safe"
local config  = require "s3rl.config"
local metrics = require "s3rl.metrics"

local _M = { _VERSION = "1.0.0" }

local function say(status, tbl)
    ngx.status = status
    ngx.header["Content-Type"] = "application/json"
    ngx.say(cjson.encode(tbl))
    return ngx.exit(status)
end

local function write_atomic(path, data)
    local tmp = path .. ".tmp." .. ngx.worker.pid()
    local f, err = io.open(tmp, "w")
    if not f then return nil, err end
    f:write(data); f:close()
    local ok, rerr = os.rename(tmp, path)
    if not ok then os.remove(tmp); return nil, rerr end
    return true
end

function _M.route()
    local uri    = ngx.var.uri
    local method = ngx.req.get_method()
    local path   = uri:gsub("^/%-/s3rl", "")

    if path == "/config" and method == "GET" then
        return say(200, config.get() or {})

    elseif path == "/config" and method == "PUT" then
        ngx.req.read_body()
        local body = ngx.req.get_body_data()
        if not body then return say(400, { error = "empty body (檢查 client_body_in_file_only)" }) end
        local t = cjson.decode(body)
        if type(t) ~= "table" then return say(400, { error = "invalid JSON" }) end

        local ok, err = write_atomic(config.path(), body)
        if not ok then return say(500, { error = "write failed: " .. tostring(err) }) end
        config.load()
        return say(200, { ok = true, version = (config.get() or {}).version })

    elseif path == "/reload" and method == "POST" then
        local ok = config.load()
        return say(ok and 200 or 500, { ok = ok, version = (config.get() or {}).version })

    elseif path == "/policy" and method == "GET" then
        local a = ngx.req.get_uri_args()
        if not a.bucket then return say(400, { error = "bucket required" }) end
        local ops = a.op and { a.op } or { "list", "read", "write", "delete", "meta", "other" }
        local out = {}
        for _, op in ipairs(ops) do
            out[op] = config.policy(a.bucket, op) or "unlimited"
        end
        return say(200, { bucket = a.bucket, effective = out })

    elseif path == "/metrics" and method == "GET" then
        ngx.header["Content-Type"] = "text/plain; version=0.0.4"
        ngx.say(metrics.prometheus())
        return ngx.exit(200)

    elseif path == "/metrics/reset" and method == "POST" then
        metrics.reset()
        return say(200, { ok = true })
    end

    return say(404, { error = "not found", path = path })
end

return _M
