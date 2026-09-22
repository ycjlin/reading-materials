problem: Openresty 被包在k8s pod 裡面，我們可以從外面下指令改變裡面的狀態，進而以事件觸發openresty 有所動作嗎？
我不想重啟。我想用Mango DB來存config Map, 當D B的內容有所更動的時候，我想要事件觸發Openresty從D B裡面讀取最新的值，更新他的config map，不需要restart 重啟服務
Config map is json; sidecar 用javascript實作

用 JavaScript 寫 sidecar 最適合，MongoDB 官方的 Node driver 對 Change Stream 支援最完整。

給你一個 production ready 的版本，不用 polling，DB 一變就打 OpenResty。

### 1. Sidecar 程式碼

只需要一個 dependency: `mongodb`

`package.json`
```json
{
  "name": "openresty-watcher",
  "type": "module",
  "dependencies": {
    "mongodb": "^6.8.0"
  }
}
```

`watcher.js`
```javascript
import { MongoClient } from 'mongodb';

const MONGO_URI = process.env.MONGO_URI; // mongodb://.../?replicaSet=rs0
const DB_NAME = process.env.DB_NAME || 'mydb';
const COLL_NAME = process.env.COLL_NAME || 'configs';
const CONFIG_ID = process.env.CONFIG_ID || 'gateway-config';
const RELOAD_URL = process.env.RELOAD_URL || 'http://127.0.0.1:9111/_internal/reload';
const INTERNAL_TOKEN = process.env.INTERNAL_TOKEN || 'change-me';

let resumeToken = null;

async function notifyOpenResty(newDoc) {
  // 策略 B：直接把最新 json 推給 OpenResty，OpenResty 不用連 DB
  // 如果你想用策略 A：只通知，OpenResty自己去抓，就把 body 拿掉
  const payload = newDoc.data; // 你的 json 欄位

  for (let i = 0; i < 3; i++) {
    try {
      const res = await fetch(RELOAD_URL, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'X-Internal-Token': INTERNAL_TOKEN
        },
        body: JSON.stringify(payload)
      });
      if (res.ok) {
        console.log(`[watcher] reload ok: ${await res.text()}`);
        return;
      }
      console.error(`[watcher] reload failed ${res.status}`);
    } catch (e) {
      console.error(`[watcher] fetch error: ${e.message}`);
    }
    await new Promise(r => setTimeout(r, 1000 * (i + 1)));
  }
}

async function start() {
  const client = new MongoClient(MONGO_URI);
  await client.connect();
  console.log('[watcher] mongo connected');

  const coll = client.db(DB_NAME).collection(COLL_NAME);

  // 啟動時先推一次，確保同步
  const current = await coll.findOne({ _id: CONFIG_ID });
  if (current) await notifyOpenResty(current);

  const pipeline = [
    { $match: { 'fullDocument._id': CONFIG_ID } }
  ];

  const watch = () => {
    const opts = { fullDocument: 'updateLookup' };
    if (resumeToken) opts.resumeAfter = resumeToken;

    const stream = coll.watch(pipeline, opts);
    console.log('[watcher] watching change stream...');

    stream.on('change', async (change) => {
      resumeToken = change._id; // 存起來，斷線重連用
      console.log(`[watcher] db changed: ${change.operationType}`);
      await notifyOpenResty(change.fullDocument);
    });

    stream.on('error', async (err) => {
      console.error('[watcher] stream error', err);
      stream.close();
      await new Promise(r => setTimeout(r, 5000));
      watch(); // 自動重連
    });
  };

  watch();

  process.on('SIGTERM', async () => {
    await client.close();
    process.exit(0);
  });
}

start().catch(console.error);
```

重點：
1. `fullDocument: 'updateLookup'` 這樣 update 時也會拿到完整 json
2. `resumeToken` 存起來，Pod 網路閃斷不會漏事件
3. 啟動時先 `findOne` 推一次，避免 watcher 比 OpenResty 晚啟動

### 2. OpenResty 端要改成收 POST

這樣 OpenResty 完全不用裝 mongo driver。

```nginx
server {
    listen 127.0.0.1:9111;

    location /_internal/reload {
        allow 127.0.0.1;
        deny all;

        content_by_lua_block {
            if ngx.var.http_x_internal_token ~= os.getenv("INTERNAL_TOKEN") then
                ngx.exit(403)
            end

            ngx.req.read_body()
            local body = ngx.req.get_body_data()
            if not body then
                ngx.status = 400
                ngx.say("empty body")
                return
            end

            -- 直接寫入 shared dict，所有 worker 立刻可見
            local ok, err = ngx.shared.my_config:set("gateway_config", body)
            if not ok then
                ngx.status = 500
                ngx.say(err)
                return
            end
            ngx.shared.my_config:set("config_version", ngx.now())
            ngx.say("updated")
        }
    }
}
```

你業務那邊就這樣讀：

```lua
local dict = ngx.shared.my_config
local cfg_json = dict:get("gateway_config")
local cfg = require("cjson").decode(cfg_json)
```

### 3. Dockerfile

`Dockerfile.watcher`
```dockerfile
FROM node:20-alpine
WORKDIR /app
COPY package.json package-lock.json*./
RUN npm ci --only=production
COPY watcher.js./
CMD ["node", "watcher.js"]
```

### 4. K8s Deployment 片段

同一個 Pod，共享 `localhost`

```yaml
spec:
  containers:
  - name: openresty
    image: your-openresty:1.0
    ports:
    - containerPort: 80
    env:
    - name: INTERNAL_TOKEN
      valueFrom:
        secretKeyRef: { name: app-secret, key: token }
  - name: watcher
    image: your-watcher:1.0
    resources:
      requests: { cpu: "20m", memory: "64Mi" }
      limits: { cpu: "100m", memory: "128Mi" }
    env:
    - name: MONGO_URI
      valueFrom:
        secretKeyRef: { name: app-secret, key: mongo-uri }
    - name: RELOAD_URL
      value: "http://127.0.0.1:9111/_internal/reload"
    - name: INTERNAL_TOKEN
      valueFrom:
        secretKeyRef: { name: app-secret, key: token }
```

這樣流程就完成了：

你 `db.configs.updateOne()` -> Node 的 `change` 事件觸發 -> `fetch 127.0.0.1:9111` -> `shared_dict` 更新 -> 請求立刻走新邏輯，全程無 restart。

要我幫你把 debounce 防抖也加上去嗎？如果你短時間內連續寫 DB 10 次，現在會打 10 次，我可以幫你合併成 1 次。
