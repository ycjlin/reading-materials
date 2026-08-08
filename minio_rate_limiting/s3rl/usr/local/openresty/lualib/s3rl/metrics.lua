-- =============================================================================
-- /usr/local/openresty/lualib/s3rl/metrics.lua
-- 計數器放 shared dict。注意 cardinality：per-bucket 只在「有被限流」時才記，
-- 否則 5000 buckets x 6 ops x 3 outcomes = 90k series，Prometheus 會很痛苦。
-- =============================================================================

local _M = { _VERSION = "1.0.0" }

local dict = ngx.shared.s3rl_stats
local fmt, concat = string.format, table.concat

local function bump(k)
    if not dict then return end
    local ok = dict:incr(k, 1, 0)
    if not ok then dict:set(k, 1) end
end

--- outcome: "pass" | "delay" | "reject:<reason>"
function _M.incr(bucket, op, outcome)
    bump("op|" .. op .. "|" .. outcome)                 -- 低基數，永遠記
    if outcome ~= "pass" then
        bump("bk|" .. bucket .. "|" .. op .. "|" .. outcome)  -- 只記被限流的 bucket
    end
end

function _M.reset()
    if dict then dict:flush_all(); dict:flush_expired() end
end

function _M.prometheus()
    local out = {
        "# HELP s3rl_requests_total S3 requests by operation class and rate-limit outcome",
        "# TYPE s3rl_requests_total counter",
    }
    local keys = dict:get_keys(0)
    local bucket_lines = {}
    for i = 1, #keys do
        local k = keys[i]
        local v = dict:get(k) or 0
        local scope, rest = k:match("^(%a%a)|(.+)$")
        if scope == "op" then
            local op, outcome = rest:match("^([^|]+)|(.+)$")
            out[#out + 1] = fmt('s3rl_requests_total{op="%s",outcome="%s"} %d', op, outcome, v)
        elseif scope == "bk" then
            local b, op, outcome = rest:match("^(.-)|([^|]+)|(.+)$")
            bucket_lines[#bucket_lines + 1] =
                fmt('s3rl_limited_total{bucket="%s",op="%s",outcome="%s"} %d', b, op, outcome, v)
        end
    end
    if #bucket_lines > 0 then
        out[#out + 1] = "# HELP s3rl_limited_total Throttled/rejected requests by bucket"
        out[#out + 1] = "# TYPE s3rl_limited_total counter"
        for i = 1, #bucket_lines do out[#out + 1] = bucket_lines[i] end
    end

    -- shared dict 用量，用來判斷要不要調大 lua_shared_dict
    for _, name in ipairs({ "s3rl_req", "s3rl_conn", "s3rl_count", "s3rl_stats" }) do
        local d = ngx.shared[name]
        if d then
            out[#out + 1] = fmt('s3rl_shdict_free_bytes{dict="%s"} %d', name, d:free_space())
            out[#out + 1] = fmt('s3rl_shdict_capacity_bytes{dict="%s"} %d', name, d:capacity())
        end
    end

    return concat(out, "\n") .. "\n"
end

return _M
