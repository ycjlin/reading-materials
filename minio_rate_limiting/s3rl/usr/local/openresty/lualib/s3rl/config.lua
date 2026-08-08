-- =============================================================================
-- /usr/local/openresty/lualib/s3rl/config.lua
-- 設定載入 + 熱抽換 + policy 解析（exact bucket > prefix rule > default）
-- =============================================================================

local cjson    = require "cjson.safe"
local lrucache = require "resty.lrucache"
local parser   = require "s3rl.parser"

local _M = { _VERSION = "1.0.0" }

local ngx_log, ERR, WARN, INFO = ngx.log, ngx.ERR, ngx.WARN, ngx.INFO
local sub, sort = string.sub, table.sort

local opts      = {}
local snapshot  = nil     -- 目前生效的設定（每個 worker 一份）
local resolved  = nil     -- lrucache: "bucket|op" -> policy | false
local last_raw  = nil

-- 出廠預設值（fab 環境保守起步，list 一定要壓）
local FALLBACK = {
    list   = { rps =   50, burst =  100, conn =   32, max_delay = 0.50 },
    read   = { rps = 3000, burst = 6000, conn = 2048, max_delay = 0.20 },
    write  = { rps =  800, burst = 1600, conn = 1024, max_delay = 0.20 },
    delete = { rps =  300, burst =  600, conn =  256, max_delay = 0.20 },
    meta   = { rps = 3000, burst = 6000, conn = 1024, max_delay = 0.20 },
    other  = { rps =  200, burst =  200, conn =   64, max_delay = 0.20 },
}

local OPS = { "list", "read", "write", "delete", "meta", "other" }

---------------------------------------------------------------------------
local function read_file(path)
    local f, err = io.open(path, "r")
    if not f then return nil, err end
    local d = f:read("*a")
    f:close()
    return d
end

local function merge(base, over)
    if type(over) ~= "table" then return base end
    local r = {}
    for k, v in pairs(base) do r[k] = v end
    for k, v in pairs(over) do
        if v ~= nil and v ~= cjson.null then r[k] = v end
    end
    return r
end

local function normalize(t)
    local cfg = {
        version       = t.version or 0,
        enabled       = t.enabled ~= false,
        reject_status = tonumber(t.reject_status) or 503,   -- AWS SDK 對 503 SlowDown 會自動退避
        retry_after   = tonumber(t.retry_after)   or 1,
        defaults      = {},
        buckets       = t.buckets or {},
        prefix_rules  = t.prefix_rules or {},
        exempt_ak     = {},
    }
    local d = t.defaults or {}
    for i = 1, #OPS do
        local op = OPS[i]
        cfg.defaults[op] = merge(FALLBACK[op], d[op])
    end
    for _, ak in ipairs(t.exempt_access_keys or {}) do
        cfg.exempt_ak[ak] = true
    end
    -- 長 prefix 優先
    sort(cfg.prefix_rules, function(a, b)
        return #(a.prefix or "") > #(b.prefix or "")
    end)
    return cfg
end

---------------------------------------------------------------------------
function _M.load()
    local raw, err = read_file(opts.path)
    if not raw then
        ngx_log(ERR, "[s3rl] cannot read ", opts.path, ": ", tostring(err))
        return false
    end
    if raw == last_raw then return true end          -- 沒變就不動

    local t = cjson.decode(raw)
    if type(t) ~= "table" then
        ngx_log(ERR, "[s3rl] invalid JSON: ", opts.path, " (keep old config)")
        return false
    end

    local ok, res = pcall(normalize, t)
    if not ok then
        ngx_log(ERR, "[s3rl] normalize failed: ", res, " (keep old config)")
        return false
    end

    snapshot = res
    last_raw = raw
    resolved = lrucache.new(50000)
    ngx_log(INFO, "[s3rl] config reloaded version=", snapshot.version,
                  " buckets=", #OPS)
    return true
end

function _M.init(o)
    opts      = o or {}
    opts.path = opts.path or "/etc/openresty/s3rl/limits.json"
    parser.init(opts.vhost_domains)
    if not _M.load() then
        ngx_log(WARN, "[s3rl] falling back to built-in defaults")
        snapshot = normalize({})
        resolved = lrucache.new(50000)
    end
end

function _M.start_watcher(interval)
    local ok, err = ngx.timer.every(interval or 5, function(premature)
        if premature then return end
        _M.load()
    end)
    if not ok then ngx_log(ERR, "[s3rl] watcher start failed: ", err) end
end

---------------------------------------------------------------------------
function _M.get()          return snapshot end
function _M.path()         return opts.path end
function _M.is_exempt(ak)  return snapshot and ak and snapshot.exempt_ak[ak] == true end

--- 解析 (bucket, op) 的有效 policy；回傳 nil 表示「不限流」
function _M.policy(bucket, op)
    local cfg = snapshot
    if not cfg then return nil end

    local ck = bucket .. "|" .. op
    local hit = resolved:get(ck)
    if hit ~= nil then
        if hit == false then return nil end
        return hit
    end

    local eff = cfg.defaults[op] or cfg.defaults.other

    -- prefix rule（例如 fab12- / eda- / scratch-）
    for i = 1, #cfg.prefix_rules do
        local r  = cfg.prefix_rules[i]
        local pre = r.prefix or ""
        if sub(bucket, 1, #pre) == pre then
            if r.enabled == false then
                resolved:set(ck, false); return nil
            end
            eff = merge(eff, r["*"])
            eff = merge(eff, r[op])
            break
        end
    end

    -- exact bucket 覆寫（最高優先）
    local b = cfg.buckets[bucket]
    if b then
        if b.enabled == false then
            resolved:set(ck, false); return nil
        end
        eff = merge(eff, b["*"])
        eff = merge(eff, b[op])
    end

    resolved:set(ck, eff)
    return eff
end

return _M
