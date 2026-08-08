-- =============================================================================
-- /usr/local/openresty/lualib/s3rl/limiter.lua
-- 三層限制串接：rate(leaky bucket) -> quota(fixed window) -> conn(concurrency)
-- 任一層 reject 時，把前面已 commit 的層 rollback 掉
-- =============================================================================

local limit_req   = require "resty.limit.req"
local limit_conn  = require "resty.limit.conn"
local limit_count = require "resty.limit.count"

local _M = { _VERSION = "1.0.0" }

local ngx_log, ERR = ngx.log, ngx.ERR

local DICT_REQ, DICT_CONN, DICT_COUNT = "s3rl_req", "s3rl_conn", "s3rl_count"

-- 依參數快取 limiter 物件（物件只存設定，狀態在 shared dict）
local c_req, c_conn, c_cnt = {}, {}, {}

local function req_limiter(rps, burst)
    local k = rps .. "/" .. burst
    local o = c_req[k]
    if not o then
        local err
        o, err = limit_req.new(DICT_REQ, rps, burst)
        if not o then ngx_log(ERR, "[s3rl] limit_req.new: ", err); return nil end
        c_req[k] = o
    end
    return o
end

local function conn_limiter(conn, burst, est)
    local k = conn .. "/" .. burst .. "/" .. est
    local o = c_conn[k]
    if not o then
        local err
        o, err = limit_conn.new(DICT_CONN, conn, burst, est)
        if not o then ngx_log(ERR, "[s3rl] limit_conn.new: ", err); return nil end
        c_conn[k] = o
    end
    return o
end

local function count_limiter(count, window)
    local k = count .. "/" .. window
    local o = c_cnt[k]
    if not o then
        local err
        o, err = limit_count.new(DICT_COUNT, count, window)
        if not o then ngx_log(ERR, "[s3rl] limit_count.new: ", err); return nil end
        c_cnt[k] = o
    end
    return o
end

---------------------------------------------------------------------------
--- @return action "pass"|"delay"|"reject", delay(number), reason(string|nil)
---------------------------------------------------------------------------
function _M.check(key, p, ctx)
    local total = 0
    local rl, ql               -- 已 commit、需要 rollback 的 limiter

    local function rollback()
        if rl then rl:uncommit(key); rl = nil end
        if ql then ql:uncommit(key); ql = nil end
    end

    -- 1) rate：leaky bucket，超出 burst 直接 rejected
    if p.rps and p.rps > 0 then
        local l = req_limiter(p.rps, p.burst or p.rps)
        if l then
            local delay, err = l:incoming(key, true)
            if not delay then
                if err == "rejected" then return "reject", 0, "rate" end
                ngx_log(ERR, "[s3rl] limit_req: ", err)     -- shared dict 滿 -> fail open
            else
                rl = l
                total = total + delay
            end
        end
    end

    -- 2) quota：固定視窗總量（例如某 bucket 每天 5,000 萬次 API）
    local q = p.quota
    if q and q.count and q.count > 0 then
        local l = count_limiter(q.count, q.window or 86400)
        if l then
            local remaining, reset = l:incoming(key, true)
            if not remaining then
                if reset == "rejected" then
                    rollback()
                    return "reject", 0, "quota"
                end
                ngx_log(ERR, "[s3rl] limit_count: ", reset)
            else
                ql = l
                ctx.s3rl_quota_remaining = remaining
                ctx.s3rl_quota_reset     = reset
            end
        end
    end

    -- 3) conn：同時在途請求數（擋住 list 打爆 metadata 層最有效的一層）
    if p.conn and p.conn > 0 then
        local l = conn_limiter(p.conn, p.conn_burst or p.conn, p.conn_est or 0.05)
        if l then
            local delay, err = l:incoming(key, true)
            if not delay then
                if err == "rejected" then
                    rollback()
                    return "reject", 0, "conn"
                end
                ngx_log(ERR, "[s3rl] limit_conn: ", err)
            else
                ctx.s3rl_conn_lim = l
                ctx.s3rl_conn_key = key
                total = total + delay
            end
        end
    end

    -- 需要等太久就不要拖著連線，直接回 SlowDown 讓 SDK 退避重試
    local maxd = p.max_delay or 0.2
    if total > maxd then
        _M.leaving(ctx)                -- 釋放剛拿到的 conn slot
        rollback()
        return "reject", total, "delay_exceeded"
    end
    if total > 0 then return "delay", total, nil end
    return "pass", 0, nil
end

--- log phase 必須呼叫，否則 conn 計數會漏掉不還
function _M.leaving(ctx)
    local l = ctx.s3rl_conn_lim
    if l then
        l:leaving(ctx.s3rl_conn_key, tonumber(ngx.var.request_time))
        ctx.s3rl_conn_lim = nil
    end
end

return _M
