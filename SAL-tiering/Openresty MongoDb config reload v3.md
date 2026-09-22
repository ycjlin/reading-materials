Openresty 被包在k8s pod 裡面，我們可以從外面下指令改變裡面的狀態，進而以事件觸發openresty 有所動作嗎？
我不想重啟。我想用Mango DB來存config Map, 當D B的內容有所更動的時候，我想要事件觸發Openresty從D B裡面讀取最新的值，更新他的config map，不需要restart 重啟服務
Config map is json

可以，而且不用 reload。關鍵是把 config 從「nginx 指令層」搬到「Lua 的 shared memory 層」——只要 config 是 Lua 在 runtime 讀的資料，就能隨時換掉；只有寫在 nginx.conf 裡的 directive 才需要 reload。

## 架構

```
MongoDB (source of truth)
   │  change stream
   ▼
config-watcher (sidecar，跟 OpenResty 同一個 pod)
   │  POST 127.0.0.1:8081/admin/config
   ▼
OpenResty: lua_shared_dict  ──►  各 worker 的 local cache (parsed table)
                                      ▲
                                      └── 每個 request 只比對 version
```

**為什麼 sidecar 放在同一個 pod**：多 replica 的 fan-out 問題自動消失（不用去列舉 pod IP 逐一 push），而且 admin port 可以只綁 `127.0.0.1`，Service 完全不暴露它。代價是 N 個 replica 就開 N 條 change stream，replica 數量到上百再考慮改成「中央 watcher + Redis pub/sub，每個 pod 自己 subscribe」。

另外 **push 一定要搭一條 pull 路徑**：新 pod 冷啟動、watcher 斷線漏訊息、Mongo 短暫不可用，都得靠定期對帳補回來。

## 雙層 cache

`lua_shared_dict` 是所有 worker 共用，但每個 request 都 `cjson.decode` 一次會很痛。做法是 shdict 存 raw JSON + 一個單調遞增的 `version`，每個 worker 自己留一份 parsed table，熱路徑只做一次 `SHM:get("ver")`（微秒等級）比對。

**gw/config.lua**

```lua
local cjson = require "cjson.safe"
local http  = require "resty.http"

local SHM    = ngx.shared.gw_config
local K_BODY, K_VER = "body", "ver"
local PULL_URL   = os.getenv("CONFIG_PULL_URL") or "http://127.0.0.1:8090/config"
local CACHE_FILE = "/var/run/gw/last-known-good.json"

local _M = {}
local worker_cache = { ver = -1, data = nil }

local function apply(ver, raw)
    local data, err = cjson.decode(raw)
    if type(data) ~= "table" then
        return nil, "bad json: " .. tostring(err)
    end
    -- 這裡放 schema 驗證，壞 config 一定要在這關擋掉

    local cur = SHM:get(K_VER)
    if cur and ver <= cur then
        return true, "stale, ignored"      -- 防亂序 push 蓋掉新版
    end

    local ok, serr = SHM:set(K_BODY, raw)  -- shdict 滿了會失敗，一定要檢查
    if not ok then return nil, serr end
    SHM:set(K_VER, ver)

    local f = io.open(CACHE_FILE, "w")     -- blocking 但頻率低，可接受
    if f then f:write(ver .. "\n" .. raw); f:close() end

    ngx.log(ngx.NOTICE, "config applied, version=", ver)
    return true
end

-- 熱路徑：只有版本變了才重新 decode
function _M.get()
    local ver = SHM:get(K_VER)
    if not ver then return nil end
    if ver ~= worker_cache.ver then
        local raw  = SHM:get(K_BODY)
        local data = raw and cjson.decode(raw)
        if data then
            worker_cache.data, worker_cache.ver = data, ver
        end
    end
    return worker_cache.data, worker_cache.ver
end

-- sidecar 偵測到 Mongo 變更後打進來
function _M.push()
    if ngx.req.get_method() ~= "POST" then return ngx.exit(405) end
    ngx.req.read_body()
    local raw = ngx.req.get_body_data()
    if not raw then
        local path = ngx.req.get_body_file()   -- body 大會落地成檔案
        if path then local f = io.open(path); raw = f:read("*a"); f:close() end
    end
    local ver = tonumber(ngx.req.get_headers()["X-Config-Version"])
    if not raw or not ver then return ngx.exit(400) end

    local ok, err = apply(ver, raw)
    if not ok then
        ngx.log(ngx.ERR, "config rejected: ", err)
        ngx.status = 400
        return ngx.say(err)
    end
    ngx.say("ok ", SHM:get(K_VER))
end

local function pull(premature)
    if premature then return end
    local httpc = http.new()
    httpc:set_timeout(3000)
    local res = httpc:request_uri(PULL_URL, { method = "GET" })
    if not res or res.status ~= 200 then return end
    local doc = cjson.decode(res.body)          -- { version = N, payload = {...} }
    if doc and doc.version then
        apply(doc.version, cjson.encode(doc.payload))
    end
end

function _M.init_worker()
    if ngx.worker.id() ~= 0 then return end     -- 只讓一個 worker 去拉

    local f = io.open(CACHE_FILE, "r")          -- Mongo 掛了也有東西可跑
    if f then
        local all = f:read("*a"); f:close()
        local v, raw = all:match("^(%d+)\n(.*)$")
        if v then apply(tonumber(v), raw) end
    end

    ngx.timer.at(0, pull)                       -- init_worker 不能開 cosocket
    ngx.timer.every(30, pull)                   -- 定期對帳
end

return _M
```

**nginx.conf**

```nginx
lua_shared_dict gw_config 5m;
init_by_lua_block        { require "resty.core" }
init_worker_by_lua_block { require("gw.config").init_worker() }

server {
    listen 127.0.0.1:8081;                  # 只給 sidecar，Service 不要暴露
    location = /admin/config {
        content_by_lua_block { require("gw.config").push() }
    }
}

server {
    listen 8080;
    location / {
        access_by_lua_block {
            local cfg = require("gw.config").get()
            if not cfg then return ngx.exit(503) end
            -- cfg.routes / cfg.rate_limits / ...
        }
        proxy_pass http://backend;
    }
}
```

**watcher sidecar**（順便在 8090 開一個 `GET /config` 給 pull 路徑用，OpenResty 就完全不用碰 Mongo）

```python
col = MongoClient(os.environ["MONGO_URI"])["gw"]["config"]

def push(doc):
    requests.post("http://127.0.0.1:8081/admin/config",
                  data=json.dumps(doc["payload"]),
                  headers={"Content-Type": "application/json",
                           "X-Config-Version": str(doc["version"])},
                  timeout=3).raise_for_status()

push(col.find_one({"_id": "gateway"}))

token = None
while True:
    try:
        with col.watch(pipeline=[{"$match": {"documentKey._id": "gateway"}}],
                       full_document="updateLookup", resume_after=token) as s:
            for change in s:
                token = s.resume_token
                if change.get("fullDocument"): push(change["fullDocument"])
    except Exception as e:
        print("watch error:", e, flush=True); time.sleep(2)
```

## 幾個會踩到的點

**Change stream 需要 replica set**，standalone MongoDB 不支援。另外 `resume_after` 一定要存，不然 watcher 重啟會漏掉中間的變更（pull 對帳就是為了兜這個）。

**不要用 lua-resty-mongol 直連 Mongo**。那套驅動很舊、wire protocol 支援不完整、也沒有 change stream。讓 sidecar 講 Mongo，OpenResty 只講 HTTP。

**`init_worker_by_lua` 階段不能開 cosocket**，所以第一次拉 config 一定要丟進 `ngx.timer.at(0, ...)`，不能直接呼叫。

**壞的 config 會同時毒死所有 pod**。`apply()` 的 schema 驗證是最後一道防線，正式環境建議在 Mongo doc 加個 `canary: ["pod-xxx"]` 欄位，先推一個 pod 觀察再全量。

**有些東西 Lua 救不了**，還是得 reload：`listen` port、`worker_processes`、靜態 `location` block、`log_format`。upstream server list 要改成 `balancer_by_lua_block` + `ngx.balancer.set_current_peer` 從 shdict 讀才能動態；TLS 憑證可以用 `ssl_certificate_by_lua_block` 動態換。順帶一提，`nginx -s reload` 是 graceful 的，不會斷連線，跟 restart 不一樣——真的遇到只能靠 directive 的設定時，這是可接受的退路。

**效能還想再壓**的話，把 `SHM:get(K_VER)` 換成 `lua-resty-events`（Kong 在用的那套）做 worker 間廣播，worker 只在收到事件時才重讀，熱路徑連 shdict lookup 都省掉。不過以你的量級，先跑 benchmark 再決定要不要加這層複雜度。

需要的話我可以把這幾份整理成完整的檔案（含 k8s Deployment YAML 跟 NetworkPolicy）。
