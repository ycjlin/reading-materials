-- =============================================================================
-- /usr/local/openresty/lualib/s3rl/handler.lua
-- access_by_lua / log_by_lua 入口
-- =============================================================================

local config   = require "s3rl.config"
local parser   = require "s3rl.parser"
local limiter  = require "s3rl.limiter"
local response = require "s3rl.response"
local metrics  = require "s3rl.metrics"

local _M = { _VERSION = "1.0.0" }

local sub, upper, fmt = string.sub, string.upper, string.format
local ngx_var = ngx.var

-- 這些路徑不做 bucket 限流
local SKIP = { "/minio/", "/-/s3rl/", "/health" }

local function should_skip(uri)
    for i = 1, #SKIP do
        local p = SKIP[i]
        if sub(uri, 1, #p) == p then return true end
    end
    return false
end

local function reqid()
    local r = ngx_var.request_id
    return r and upper(sub(r, 1, 16)) or "0000000000000000"
end

---------------------------------------------------------------------------
function _M.access()
    local cfg = config.get()
    if not cfg or not cfg.enabled then return end

    local uri = ngx_var.uri
    if should_skip(uri) then return end

    local host = ngx_var.http_host or ngx_var.host
    local bucket, key = parser.bucket_and_key(host, uri)
    if not bucket or bucket == "" then return end   -- ListBuckets 等 service-level

    local args   = ngx.req.get_uri_args(32)
    local method = ngx.req.get_method()
    local op     = parser.classify(method, bucket, key, args)
    local ak     = parser.access_key(ngx_var.http_authorization, args)

    local rid = reqid()
    ngx_var.s3rl_bucket = bucket
    ngx_var.s3rl_op     = op
    ngx_var.s3rl_ak     = ak or "-"
    ngx_var.s3rl_reqid  = rid

    -- 白名單（備份服務、DR replication 之類不能被限）
    if ak and config.is_exempt(ak) then
        ngx_var.s3rl_action = "exempt"
        return
    end

    local p = config.policy(bucket, op)
    if not p then
        ngx_var.s3rl_action = "off"
        return
    end

    -- egress 頻寬節流（bytes/s，nginx 原生 limit_rate）
    if p.limit_rate and p.limit_rate > 0 then
        ngx_var.limit_rate = p.limit_rate
    end

    -- 限流計數的 scope key
    local scope = bucket .. "|" .. op
    if p.per_access_key and ak then
        scope = scope .. "|" .. ak
    end

    local ctx = ngx.ctx
    local action, delay, reason = limiter.check(scope, p, ctx)

    if ctx.s3rl_quota_remaining then
        ngx.header["x-s3rl-quota-remaining"] = ctx.s3rl_quota_remaining
    end

    if action == "reject" then
        local outcome = "reject:" .. (reason or "?")
        ngx_var.s3rl_action = outcome
        metrics.incr(bucket, op, outcome)

        local msg
        if reason == "quota" then
            msg = fmt("Request quota exceeded for bucket '%s'.", bucket)
        elseif reason == "conn" then
            msg = fmt("Too many concurrent '%s' requests on bucket '%s'.", op, bucket)
        else
            msg = fmt("Please reduce your '%s' request rate on bucket '%s'.", op, bucket)
        end

        return response.error({
            status      = cfg.reject_status,
            code        = (reason == "quota") and "SlowDown" or "SlowDown",
            message     = msg,
            bucket      = bucket,
            retry_after = cfg.retry_after,
            request_id  = rid,
            headers     = { ["x-s3rl-reason"] = reason, ["x-s3rl-op"] = op },
        })
    end

    if action == "delay" then
        ngx_var.s3rl_action = "delay"
        ngx_var.s3rl_delay  = fmt("%.4f", delay)
        metrics.incr(bucket, op, "delay")
        ngx.sleep(delay)                       -- 平滑節流，不讓 client 進重試風暴
        return
    end

    metrics.incr(bucket, op, "pass")
end

---------------------------------------------------------------------------
function _M.log()
    limiter.leaving(ngx.ctx)                   -- 一定要還 conn slot
end

return _M
