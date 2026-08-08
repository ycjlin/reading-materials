-- =============================================================================
-- /usr/local/openresty/lualib/s3rl/redis_bucket.lua
-- 【選用】多台 OpenResty 時的「全域」token bucket。
--
-- lua_shared_dict 只在單機共享，N 台 OpenResty = 實際限額 x N。
-- 兩種解法：
--   (A) 簡單：把設定值除以 N（推薦先用這個，零額外延遲、零額外故障點）
--   (B) 精準：用這支 Redis token bucket（+0.2~0.5ms，Redis 掛掉要 fail-open）
--
-- 用法（在 handler.lua 的 limiter.check 之前插入）：
--   local rb = require "s3rl.redis_bucket"
--   local allowed, wait = rb.take("s3rl:" .. scope, p.rps, p.burst, 1)
-- =============================================================================

local redis = require "resty.redis"

local _M = { _VERSION = "1.0.0" }

_M.host       = "10.20.0.30"
_M.port       = 6379
_M.password   = nil
_M.db         = 0
_M.timeout    = 100      -- ms，寧可 fail-open 也不要拖垮 S3 資料面
_M.pool_size  = 256
_M.pool_idle  = 60000    -- ms
_M.fail_open  = true

-- KEYS[1]=key  ARGV: rate, burst, now(sec, 浮點), cost, ttl
local SCRIPT = [[
local key   = KEYS[1]
local rate  = tonumber(ARGV[1])
local burst = tonumber(ARGV[2])
local now   = tonumber(ARGV[3])
local cost  = tonumber(ARGV[4])
local ttl   = tonumber(ARGV[5])
local d  = redis.call('HMGET', key, 'ts', 'tk')
local ts = tonumber(d[1]) or now
local tk = tonumber(d[2]) or burst
local delta = now - ts
if delta < 0 then delta = 0 end
tk = math.min(burst, tk + delta * rate)
local allowed, wait = 0, 0
if tk >= cost then
  tk = tk - cost
  allowed = 1
else
  wait = (cost - tk) / rate
end
redis.call('HMSET', key, 'ts', now, 'tk', tk)
redis.call('EXPIRE', key, ttl)
return { allowed, tostring(wait) }
]]

local SHA = nil

local function connect()
    local r = redis:new()
    r:set_timeouts(_M.timeout, _M.timeout, _M.timeout)
    local ok, err = r:connect(_M.host, _M.port)
    if not ok then return nil, err end
    if r:get_reused_times() == 0 then
        if _M.password then
            local ok2, e2 = r:auth(_M.password)
            if not ok2 then r:close(); return nil, e2 end
        end
        if _M.db and _M.db ~= 0 then r:select(_M.db) end
    end
    return r
end

local function release(r)
    r:set_keepalive(_M.pool_idle, _M.pool_size)
end

--- @return allowed(boolean), wait(number seconds)
function _M.take(key, rate, burst, cost)
    if not rate or rate <= 0 then return true, 0 end

    local r, err = connect()
    if not r then
        ngx.log(ngx.ERR, "[s3rl] redis connect: ", err)
        return _M.fail_open, 0
    end

    local now  = ngx.now()
    local ttl  = math.ceil(burst / rate) + 10
    local res, rerr

    if SHA then
        res, rerr = r:evalsha(SHA, 1, key, rate, burst, now, cost or 1, ttl)
    end
    if not res then
        if rerr and not rerr:find("NOSCRIPT", 1, true) and SHA then
            ngx.log(ngx.ERR, "[s3rl] redis evalsha: ", rerr)
        end
        res, rerr = r:eval(SCRIPT, 1, key, rate, burst, now, cost or 1, ttl)
        if res then
            local sha = r:script("LOAD", SCRIPT)
            if sha and sha ~= ngx.null then SHA = sha end
        end
    end

    release(r)

    if not res then
        ngx.log(ngx.ERR, "[s3rl] redis eval: ", tostring(rerr))
        return _M.fail_open, 0
    end
    return tonumber(res[1]) == 1, tonumber(res[2]) or 0
end

return _M
