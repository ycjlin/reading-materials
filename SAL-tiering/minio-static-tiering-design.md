# MinIO Static Tiering Service — 詳細設計

**Status:** Proposed
**Scope:** 多 MinIO cluster 的 user-declared placement（static tiering），含 OpenResty 設定零重啟熱更新
**Data path:** `Client → OpenResty → SAL → HAProxy → MinIO cluster(hot / warm / cold)`

> ### ⚠ 已被 v2 修訂部分取代
> 若採納「placement 一旦設定即不可更改」的約束，請併讀 **`minio-tiering-v2-immutable-placement.md`**。
> 該修訂取代本文件的 **§4.3、§8.2（fallback）、§10（整節）、§13（部分）、§16、§17 #1**，
> 並刪除 `read_fallback`、mover 公開入口、`default_tier` 隱含 fallback、以及 catalog 作為讀取路徑權威的角色。
> **特別注意**：v2 §1 說明了「rule append-only」為何**不足以**保證 placement 不可變（嵌套插入陷阱）。

---

## 0. 設計摘要（TL;DR）

| 主題 | 決策 |
|---|---|
| Tiering 語意 | **Write-time placement**，不是 lifecycle transition。物件一旦落在某 cluster 就不會自己搬 |
| 使用者介面 | `(bucket, prefix) → tier` rule，**longest-prefix wins**，prefix 強制以 `/` 結尾 |
| Single source of truth | Control Plane（etcd / Postgres）產生 **versioned + signed policy bundle** |
| OpenResty 熱更新 | `lua_shared_dict` 存 raw bundle + 單調遞增 version；worker 以 version 為 cache key 重建 per-worker trie。**不需要 reload，更不需要 restart** |
| 一致性關鍵 | **Two-phase activation（`effective_at` 排程生效）+ 節點 version 回報**，消除節點間 policy skew 造成的誤路由 |
| 職責切分 | OpenResty 做「便宜的 rule lookup」，SAL 做「跨 cluster 的 S3 語意」（LIST merge、MPU pinning、bucket fan-out、cross-tier copy） |
| LIST 跨 tier | Prefix rule 會把 keyspace 切成**有序不重疊區間**，因此用 **interval sequential scan**（每頁通常只打 1 個 cluster），不是 k-way heap merge |
| 最大風險 | Rule 事後改指向 → 同一 key 在兩個 cluster 各有一份。v1 用 state machine + `read_fallback` + mover + reconciler；**v2 改以 immutable placement 結構性消除** |

---

## 1. 目標與非目標

### 1.1 功能需求

1. 多個獨立 MinIO cluster，以介質分層：`hot`(NVMe)、`warm`(SATA SSD)、`cold`(HDD)。
2. 使用者以 `bucket + prefix` 宣告該範圍的資料要落在哪個 tier；系統不自動搬移。
3. 對 client 完全是**單一 S3 endpoint、單一 bucket namespace**。Client 不知道底下有幾個 cluster。
4. 支援 `PUT / GET / HEAD / DELETE / ListObjectsV2 / ListObjectVersions / Multipart / CopyObject / DeleteObjects`。
5. 新增或修改 tiering rule 後，**OpenResty 不需 restart、不需 `nginx -s reload`**，於數秒內全節點一致生效。

### 1.2 非功能需求

| 項目 | 目標 |
|---|---|
| Routing overhead（OpenResty 決策） | p99 < 200 µs，記憶體 O(rules) |
| Policy 傳播延遲 | p99 < 10 s（poll 模式）；可選 etcd watch 降至 < 1 s |
| 可用性 | Control Plane 掛掉不影響 data path（last-known-good + 落地檔） |
| 誤路由容忍度 | **0**。誤路由 = 寫錯 cluster = 之後讀不到，屬資料面事故 |
| Rule 規模 | 設計到 10⁵ rules；fab 實務上預期 10³ 量級 |
| 稽核 | 每次 rule 變更可追溯到 requester + CAB ticket + 生效時間 + 各節點確認 version |

### 1.3 非目標

- **不做**自動冷熱判斷、access-time based demotion（那是 dynamic tiering，另案）。
- **不做**跨 tier 的 strong-consistency 交易（例如「同時原子性寫兩個 cluster」）。
- **不做** cluster 間 replication／DR。Tier 之間不是副本關係。
- **不接管** MinIO 內部 EC、healing、scanner 行為，只做參數建議。

---

## 2. 為什麼不用 MinIO 原生 ILM Transition Tier

MinIO 有 `mc ilm tier add` + lifecycle transition，可以把物件 transition 到 remote tier。這裡刻意不用，理由：

| 面向 | MinIO ILM transition | 本設計（write-time placement） |
|---|---|---|
| Metadata 位置 | 永遠留在 source cluster 的 `xl.meta` | 完全在目標 cluster |
| 讀取路徑 | 必須經過 source cluster proxy 回來 | 直達目標 cluster |
| Source cluster 是否能縮容 | **不能**。SSD cluster 永遠背著全量 metadata 與 LIST 負擔 | 能。各 tier 獨立擴縮 |
| 使用者可控性 | 由 lifecycle rule（天數）決定，非使用者意圖 | 使用者以 prefix 明確宣告 |
| 對你現有 LIST 瓶頸 | **惡化**。5 B objects 的 metadata 全壓在 hot cluster | 改善。metadata 按 tier 分散 |
| 失效隔離 | source cluster 掛掉 → cold 資料也讀不到 | cold cluster 掛掉只影響 cold |

結論：既然需求本來就是「user 自行決定」，就不需要 ILM 的自動性，而 ILM 帶來的 metadata 集中是你已知的痛點。**放棄 ILM，改在 gateway 層做 placement routing。**

---

## 3. 整體架構

```
                     ┌──────────────────────────────────────────┐
                     │  Tiering Control Plane (3 replicas)      │
   使用者 / 平台 API ─▶│  · REST: PUT/GET bucket placement        │
                     │  · 驗證 + 影響評估 + CAB 稽核             │
                     │  · 產生 versioned & signed bundle         │
                     │  · 收集各節點 policy version heartbeat    │
                     └───────┬──────────────────────┬───────────┘
                             │ GET /v1/placement/bundle (ETag)
                             │ (poll 3s，或 etcd watch)
        ┌────────────────────┴──────┐        ┌─────┴──────────────────┐
        ▼                           ▼        ▼                        ▼
┌───────────────┐          ┌───────────────┐  ┌───────────────┐ ┌──────────┐
│  OpenResty #1 │   ...    │  OpenResty #N │  │    SAL #1..M  │ │  Mover   │
│ ─ TLS         │          │               │  │ ─ S3 語意層    │ │ (搬遷工) │
│ ─ authz 前置   │          │ lua_shared_   │  │ ─ LIST merge  │ └──────────┘
│ ─ bucket/tier │          │ dict          │  │ ─ MPU pinning │
│   rate limit  │          │ + per-worker  │  │ ─ bucket 扇出  │
│ ─ rule lookup │          │   trie cache  │  │ ─ cross-tier  │
│   → X-Sal-*   │          └───────────────┘  │   copy        │
└───────┬───────┘                             └───────┬───────┘
        └──────────────────────────────────────────────┘
                             │  X-Sal-Tier: hot|warm|cold
                             ▼
                     ┌───────────────┐
                     │    HAProxy    │  use_backend by req.hdr(x-sal-tier)
                     └───┬───┬───┬───┘
             ┌───────────┘   │   └───────────┐
             ▼               ▼               ▼
     ┌──────────────┐ ┌──────────────┐ ┌──────────────┐
     │ MinIO hot    │ │ MinIO warm   │ │ MinIO cold   │
     │ NVMe, EC:2   │ │ SSD, EC:3    │ │ HDD, EC:4    │
     └──────────────┘ └──────────────┘ └──────────────┘
                             │
                             ▼  (async event)
                     ┌────────────────────────┐
                     │  SAL Catalog (你既有)   │
                     │  object → tier, size…  │
                     │  加速 LIST、reconcile   │
                     └────────────────────────┘
```

### 3.1 為什麼要有兩層（OpenResty + SAL）

不是重複。切分依據是「這個請求能不能只靠 URI 決定」：

| 請求類別 | 決策位置 | 理由 |
|---|---|---|
| `GET/HEAD/PUT/DELETE` 單一 object | **OpenResty 決 tier**，SAL 僅驗證後透傳 | 一次 trie lookup，無跨 cluster 語意 |
| `ListObjectsV2` / `ListObjectVersions` | **SAL** | 可能橫跨多 tier，需區間掃描與 token 編碼 |
| `CreateMultipartUpload` 及後續 part 操作 | **SAL** | uploadId 需 pin 到 tier |
| Bucket 層級（Create/Delete/Head/Versioning/Tagging/Policy） | **SAL** | 需扇出到所有相關 tier |
| `CopyObject` / `UploadPartCopy` | **SAL** | 跨 tier 需 read-then-write |
| `DeleteObjects` (batch) | **SAL** | 需按 tier 拆分再合併結果 |

OpenResty 在 `access_by_lua` 階段分類，設 `X-Sal-Mode: direct|coordinate`。SAL 對 `direct` 走極短路徑（驗證 + reverse proxy），對 `coordinate` 走完整語意處理。

---

## 4. Placement Policy 資料模型

### 4.1 Bundle Schema

Control Plane 對外只暴露一個 immutable、versioned 的 bundle：

```jsonc
{
  "version": 41207,                       // 單調遞增；data plane 拒絕回退
  "generated_at": "2026-08-11T09:14:22Z",
  "min_client_schema": 1,
  "default_tier": "warm",                 // 全域預設（兜底）

  "clusters": [
    { "id": "hot",  "media": "nvme", "haproxy_backend": "be_minio_hot",
      "min_object_size": 0,        "max_object_size": null, "writable": true },
    { "id": "warm", "media": "ssd",  "haproxy_backend": "be_minio_warm",
      "min_object_size": 0,        "max_object_size": null, "writable": true },
    { "id": "cold", "media": "hdd",  "haproxy_backend": "be_minio_cold",
      "min_object_size": 1048576,  "max_object_size": null, "writable": true }
  ],

  "bucket_defaults": { "fab-lot": "cold", "eda-scratch": "hot" },

  "rules": [
    { "id": "r-0912", "bucket": "fab-lot", "prefix": "raw/",
      "tier": "cold", "effective_at": 1754900000, "state": "active",
      "read_fallback": [], "ticket": "CAB-24817", "owner": "yield-eng" },

    { "id": "r-0913", "bucket": "fab-lot", "prefix": "raw/2026/",
      "tier": "warm", "effective_at": 1754900000, "state": "active",
      "read_fallback": [], "ticket": "CAB-24817", "owner": "yield-eng" },

    { "id": "r-0977", "bucket": "fab-lot", "prefix": "raw/2026/wk32/",
      "tier": "hot",  "effective_at": 1754986500, "state": "staged",
      "read_fallback": ["warm"], "ticket": "CAB-24993", "owner": "yield-eng" }
  ],

  "checksum": "sha256:9f2c…",
  "signature": "ed25519:MEUCIQ…"          // 選用：CP 私鑰簽章，data plane 驗簽
}
```

要點：

- `effective_at` 是 **未來的 wall-clock 秒**。這是解決節點間 skew 的關鍵（§6.3）。
- 同一 `(bucket, prefix)` 可以有多筆 rule，`effective_at` 不同 → 形成該節點的時間序列版本。Data plane 取「`effective_at ≤ now` 中最新的一筆」。
- `read_fallback` 讓遷移期間的讀取可以回退查舊 tier。
- `min_object_size` 掛在 cluster 上：cold tier 拒收小檔（LOSF 保護，§12.3）。

### 4.2 匹配語意

```
owner(bucket, key, now) =
    argmax_len { rule.prefix : rule.prefix 是 key 的 prefix
                             ∧ rule.bucket = bucket
                             ∧ rule.effective_at ≤ now }
    → 若無命中，取 bucket_defaults[bucket]
    → 若無，取 default_tier
```

**Longest-prefix wins**，且「還沒生效的 deeper rule 必須讓位給 shallower 的生效 rule」——這點在實作 trie walk 時要注意（見 §7.5）。

### 4.3 硬性約束（設計上刻意加的限制）

| 約束 | 理由 |
|---|---|
| **rule.prefix 必須以 `/` 結尾，或為空字串** | 避免 `logs` 意外命中 `logs2/`；讓 trie 退化為 segment trie（快、無回溯）；讓 LIST 的區間邊界落在 delimiter 上 |
| **rule 一旦 active 且該 prefix 下已有物件，不得直接改 tier** | 直接改 = 舊物件讀不到。必須走 §10 的 state machine |
| **同一 bucket 內 rule 數上限**（建議 512） | 控制 LIST 的區間數；超過通常意味著使用者該改用不同 bucket |
| **不允許 rule prefix 互相「部分重疊但非嵌套」** | 以 `/` 結尾即天然滿足：兩個 prefix 只可能是「無關」或「嚴格嵌套」 |
| `default_tier` 與 `bucket_defaults` 不可為 non-writable cluster | 否則新 bucket 第一次寫入就失敗 |

---

## 5. 元件職責

| 元件 | 職責 | 不做什麼 |
|---|---|---|
| **Control Plane** | rule CRUD、驗證、影響評估、bundle 產生與簽章、節點 version 收集、稽核 | 不在 data path 上 |
| **OpenResty** | TLS、bucket/tier 雙維度 rate limit、URI 解析、rule lookup、標注 `X-Sal-*`、log-phase 發 catalog event | 不 buffer body、不改 signed header、不做 S3 語意 |
| **SAL** | 驗證 OpenResty 決策、跨 cluster S3 語意、LIST 區間掃描與 token、MPU pinning、bucket 扇出、cross-tier copy | 不做 LB、不做健康檢查 |
| **HAProxy** | 依 `X-Sal-Tier` 選 backend、cluster 內 LB、MinIO 健康檢查、長連線與大 body timeout | 不做 routing 決策 |
| **MinIO cluster** | 物件儲存、EC、heal | 不知道 tiering 存在 |
| **SAL Catalog** | `(bucket, key, version) → tier, size, etag, mtime`；加速 LIST；reconcile 來源 | Phase 1 不在讀取關鍵路徑上 |
| **Mover** | rule 改指向後的背景搬遷 + 驗證 + 清理 | 不自動觸發，由 CP 排程 |

---

## 6. Control Plane

### 6.1 API

```
GET    /v1/placement/bundle                      # data plane 拉取；支援 If-None-Match
GET    /v1/buckets/{bucket}/placement            # 使用者查看
PUT    /v1/buckets/{bucket}/placement            # 全量覆寫（帶 If-Match: <etag> 做 CAS）
POST   /v1/buckets/{bucket}/placement:validate   # dry-run：回傳影響評估，不落地
GET    /v1/placement/rollout/{version}           # 各節點是否已收到此 version
GET    /v1/placement/audit?bucket=&since=        # 稽核查詢
POST   /v1/migrations                            # 建立搬遷任務（rule 改指向時）
GET    /v1/migrations/{id}
```

`PUT` body：

```jsonc
{
  "default_tier": "cold",
  "rules": [
    { "prefix": "raw/",           "tier": "cold" },
    { "prefix": "raw/2026/",      "tier": "warm" },
    { "prefix": "raw/2026/wk32/", "tier": "hot"  }
  ],
  "ticket": "CAB-24993",
  "reason": "wk32 進 SPC 分析，需要 SSD 讀取延遲",
  "activation_delay_sec": 30        // 可選，預設 30
}
```

`:validate` 回應（這是 self-service 的安全閥）：

```jsonc
{
  "ok": false,
  "version_if_applied": 41208,
  "effective_at": "2026-08-11T09:15:00Z",
  "diff": [
    { "prefix": "raw/2026/wk32/", "from": "warm", "to": "hot", "kind": "repoint" }
  ],
  "impact": {
    "objects_affected": 8412774,          // 來自 SAL Catalog
    "bytes_affected": 191234567890,
    "requires_migration": true,
    "target_tier_free_bytes": 88000000000,
    "target_tier_headroom_after": -1.0    // 負值 → 拒絕
  },
  "errors": ["target tier 'hot' 容量不足：需 178 GiB，可用 82 GiB"]
}
```

### 6.2 驗證規則（`PUT` 一律先跑）

1. Schema / tier 存在性 / prefix 以 `/` 結尾 / rule 數上限。
2. **Repoint 檢測**：與現行 active rule 比對；若 `to != from` 且 catalog 顯示該 prefix 下 `objects > 0` → 標記 `requires_migration`，且必須帶 `POST /v1/migrations` 才允許提交。
3. **容量前置檢查**：目標 tier 剩餘容量 ≥ 影響量 × 1.2，否則拒絕。
4. **配額檢查**：`owner` 團隊在該 tier 的配額（§15）。
5. **衝突檢查**：該 bucket 上是否有進行中的 migration 覆蓋重疊 prefix → 拒絕。
6. **審批**：若 `to` 是 hot 且影響量 > 閾值（例如 10 TiB），要求額外簽核。

### 6.3 Two-Phase Activation — 這是熱更新能安全的前提

**問題**：N 台 OpenResty 各自 poll，不可能同一瞬間拿到新 bundle。若 rule 改指向，在 skew 窗口內同一個 key 的兩個 PUT 會落到兩個 cluster。這不是「暫時不一致」，這是**資料事故**。

**做法**：

```
t0   CP 收到 PUT，驗證通過，寫入 store，version = V，rules 標 state=staged，
     effective_at = t0 + Δ   (Δ = activation_delay_sec，預設 30s ≥ 4× poll interval)

t0+ε 各 data plane 節點 poll 到 version V。
     節點「收下」但因 effective_at > now，lookup 仍回舊 tier。
     節點在下次 heartbeat 回報 X-Policy-Ver: V。

t0+δ CP 觀察到所有已註冊節點（OpenResty + SAL）都回報 ≥ V。
     若在 effective_at 前未達成 → CP 自動把該 rule 的 effective_at 往後延一輪，
     並發告警；連續 3 輪失敗 → 標記 rollout_failed 並回滾到 V-1。

t0+Δ 所有節點的本地時鐘同時跨過 effective_at → 同一瞬間切換。
     CP 將 rules state 由 staged 改 active（下一個 bundle version）。
```

依賴：**NTP**（fab 環境本來就有，chrony 偏移 < 50 ms 即足夠）。以 `Δ = 30 s` 對比 50 ms 偏移，安全邊界 600×。

好處：
- 熱更新變成「排程生效」，skew 窗口從「poll interval」壓到「時鐘偏移」。
- 天然產生 CAB 要的證據：*「變更於 09:15:00 生效，變更前 N 個節點皆已確認收到 version 41208」*。
- 也給了 abort 窗口：`effective_at` 之前可以直接 `DELETE` 掉這個 staged rule。

### 6.4 稽核

每筆變更寫 append-only audit record：`{version, actor, ticket, reason, diff, impact_snapshot, effective_at, node_ack_list, applied_at}`。存 Postgres + 匯出到既有 log 平台。

---

## 7. OpenResty 設定熱更新實作（核心需求）

### 7.1 為什麼不能只靠 `nginx -s reload`

`reload` 其實是 graceful 的（舊 worker 處理完現有連線才退出），並非「重啟」。但仍不適合當成設定變更機制：

| 問題 | 說明 |
|---|---|
| Worker 世代堆積 | 大 PUT 可能跑 15 分鐘，舊 worker 不退；頻繁 reload → shutting-down worker 累積，記憶體翻倍 |
| Shared dict 保留但 LRU cache 全清 | rate limit 狀態尚可，但每次 reload 都要重新暖機 |
| 長連線斷開 | keepalive 連線被回收，client 端看到 connection reset 重試 |
| 變更耦合 | 設定檔改動需要 config management 推送 + 語法驗證，做不到「秒級、可回滾、有 version」 |

**原則：把「程式（nginx.conf + Lua 模組）」與「資料（placement policy）」徹底分離。** 程式變更走正常發版流程（可接受 reload）；policy 變更走 data plane 熱更新，永不 reload。

> 附帶提醒：不要用 `lua_code_cache off` 來達成「Lua 也熱更新」。它會讓每個請求重新載入模組，效能掉一個數量級，且 upvalue 狀態全失，只適合開發環境。

### 7.2 資料流

```
CP  ──HTTP GET (If-None-Match)──▶  worker 0 的 ngx.timer.every(3s)
                                        │
                                        │ 驗 checksum/signature、驗 version 單調遞增
                                        ▼
                       lua_shared_dict "placement"
                         bundle:raw:<V>   = <json string>   (TTL 1h)
                         bundle:version   = V
                         bundle:etag      = "..."
                         bundle:fetched_at= <epoch>
                                        │
                       ┌────────────────┼────────────────┐
                       ▼                ▼                ▼
                  worker 1          worker 2   …     worker K
              _router (upvalue)  _router          _router
              每個請求：讀 bundle:version（O(1) 微秒級）
              若 != _router.version → 解析 raw、重建 segment trie、原子換指標
                                        │
                                        ▼
                       落地 /var/lib/openresty/placement/bundle.json
                       （atomic rename；供 process 冷啟動 seed）
```

關鍵取捨說明：

- **`lua_shared_dict` 只能存 string / number / boolean，不能存 Lua table**，所以 trie 一定是 per-worker 建。用「shared dict 存 raw + version stamp，worker 用 upvalue cache」是唯一乾淨的做法。
- 每個請求只多一次 `dict:get("bundle:version")`（約 0.3–1 µs，含 shm lock）。重建只在 version 變化時發生，一個 worker 一次。
- **只讓 worker 0 拉取**，避免 K 個 worker × N 個節點打爆 CP。worker 0 若被 nginx 重生，`init_worker_by_lua` 會重新執行、重新選舉。

### 7.3 `nginx.conf`

```nginx
worker_processes auto;
worker_rlimit_nofile 200000;

env SAL_CP_URL;
env SAL_NODE_ID;

http {
    lua_package_path "/etc/openresty/lua/?.lua;;";

    lua_shared_dict placement   32m;   # policy bundle（多世代）
    lua_shared_dict rl_bucket   64m;   # 既有 bucket rate limit
    lua_shared_dict rl_tier      8m;   # 新增：per-tier 入場控制
    lua_shared_dict evtbuf      64m;   # log-phase catalog event ring buffer
    lua_shared_dict prom        16m;

    # ── S3-behind-nginx 的必要開關（見 §7.8）───────────────
    merge_slashes off;                 # S3 key 允許 "//"，預設 collapse 會破壞 key 與 SigV4
    underscores_in_headers on;
    client_max_body_size 0;            # 不限制物件大小
    client_body_timeout 300s;
    proxy_request_buffering off;       # 絕對不要 buffer PUT body
    proxy_buffering off;               # 也不要 buffer GET response
    proxy_http_version 1.1;
    proxy_read_timeout 900s;
    proxy_send_timeout 900s;
    ignore_invalid_headers off;

    init_by_lua_block {
        -- init_by_lua 不能開 cosocket，只能從磁碟 seed
        require("placement").seed_from_disk()
        require("router")        -- 預載模組，避開首請求抖動
        require("route")
    }

    init_worker_by_lua_block {
        require("placement").start()      -- worker 0 起 timer
        require("catalog_emit").start()   -- 所有 worker：定期 flush event
    }

    upstream sal {
        server sal-1.fab.local:8080 max_fails=2 fail_timeout=5s;
        server sal-2.fab.local:8080 max_fails=2 fail_timeout=5s;
        server sal-3.fab.local:8080 max_fails=2 fail_timeout=5s;
        keepalive 512;
        keepalive_requests 100000;
        keepalive_timeout 75s;
    }

    server {
        listen 443 ssl reuseport backlog=32768;
        server_name s3.fab.local *.s3.fab.local;

        # 必須先宣告，Lua 才能寫入
        set $sal_tier       "";
        set $sal_rule_id    "";
        set $sal_policy_ver "";
        set $sal_mode       "";
        set $sal_fallback   "";
        set $sal_bucket     "";

        location = /healthz {
            access_log off;
            content_by_lua_block { require("route").healthz() }
        }

        location / {
            access_by_lua_block { require("route").run() }
            log_by_lua_block    { require("catalog_emit").on_log() }

            proxy_set_header Host              $http_host;   # SigV4 簽 host，不可改
            proxy_set_header Connection        "";
            proxy_set_header X-Request-Id      $request_id;
            proxy_set_header X-Real-IP         $remote_addr;
            proxy_set_header X-Sal-Tier        $sal_tier;
            proxy_set_header X-Sal-Rule-Id     $sal_rule_id;
            proxy_set_header X-Sal-Policy-Ver  $sal_policy_ver;
            proxy_set_header X-Sal-Mode        $sal_mode;
            proxy_set_header X-Sal-Fallback    $sal_fallback;

            # 注意：結尾不可加 "/" 或任何 URI。
            # 一旦帶 URI，nginx 會改用 normalize/decode 後的 $uri，SigV4 立刻壞掉。
            proxy_pass http://sal;
        }
    }
}
```

### 7.4 `placement.lua` — bundle poller

```lua
local http  = require "resty.http"
local cjson = require "cjson.safe"

local _M   = {}
local DICT = ngx.shared.placement

local DISK    = "/var/lib/openresty/placement/bundle.json"
local CP_URL  = os.getenv("SAL_CP_URL")  or "http://tiering-cp.fab.local/v1/placement/bundle"
local NODE_ID = os.getenv("SAL_NODE_ID") or (ngx.config.prefix() or "unknown")
local POLL    = 3
local RAW_TTL = 3600

------------------------------------------------------------------
local function validate(raw)
    local b = cjson.decode(raw)
    if type(b) ~= "table"       then return nil, "not json object" end
    if type(b.version) ~= "number" or b.version <= 0
                                then return nil, "bad version"     end
    if type(b.rules)   ~= "table"  then return nil, "no rules"     end
    if type(b.clusters)~= "table"  then return nil, "no clusters"  end
    if not b.default_tier          then return nil, "no default"   end
    -- checksum / Ed25519 簽章驗證（CP 有簽時強制）
    local ok, err = require("bundle_verify").check(raw, b)
    if not ok then return nil, "verify failed: " .. tostring(err) end
    return b
end

-- 先寫 versioned raw，最後才 bump version：
-- 讀端先讀 version 再讀 raw:<version>，故永不會看到 version 指向不存在的 raw
local function install(raw, b)
    local ok, err = DICT:set("bundle:raw:" .. b.version, raw, RAW_TTL)
    if not ok then return nil, "shm full: " .. tostring(err) end
    DICT:set("bundle:version", b.version)
    DICT:set("bundle:installed_at", ngx.time())
    return true
end

local function write_disk(raw)
    -- 小檔（典型 < 1 MiB）的阻塞 IO，於 timer 中執行，可接受。
    -- 若 bundle 成長到數 MiB，改用 sidecar agent 落地（見 §7.9 選項 B）。
    local tmp = DISK .. ".tmp"
    local f = io.open(tmp, "wb")
    if not f then return end
    f:write(raw); f:close()
    os.rename(tmp, DISK)                -- 同 filesystem，atomic
end

------------------------------------------------------------------
function _M.seed_from_disk()
    local f = io.open(DISK, "rb")
    if not f then
        ngx.log(ngx.WARN, "placement: no on-disk bundle, waiting for CP")
        return
    end
    local raw = f:read("*a"); f:close()
    local b, err = validate(raw)
    if not b then
        ngx.log(ngx.ERR, "placement: on-disk bundle invalid: ", err)
        return
    end
    install(raw, b)
    ngx.log(ngx.NOTICE, "placement: seeded from disk version=", b.version)
end

local function poll(premature)
    if premature or ngx.worker.exiting() then return end

    local cur = DICT:get("bundle:version") or 0
    local httpc = http.new()
    httpc:set_timeout(2000)

    local res, err = httpc:request_uri(CP_URL, {
        method  = "GET",
        headers = {
            ["If-None-Match"]  = DICT:get("bundle:etag"),
            ["X-Node-Id"]      = NODE_ID,        -- CP 用來做 rollout ack
            ["X-Node-Role"]    = "openresty",
            ["X-Policy-Ver"]   = tostring(cur),
            ["Accept-Encoding"]= "gzip",
        },
    })

    if not res then
        DICT:incr("stat:fetch_err", 1, 0)
        ngx.log(ngx.ERR, "placement: fetch failed: ", err)
        return
    end

    DICT:set("bundle:last_contact", ngx.time())

    if res.status == 304 then return end
    if res.status ~= 200 then
        DICT:incr("stat:fetch_err", 1, 0)
        ngx.log(ngx.ERR, "placement: CP status ", res.status)
        return
    end

    local b, verr = validate(res.body)
    if not b then
        DICT:incr("stat:verify_err", 1, 0)
        ngx.log(ngx.ERR, "placement: reject bundle: ", verr)   -- 保留舊的，不動
        return
    end
    if b.version < cur then
        ngx.log(ngx.ERR, "placement: refusing rollback ", b.version, " < ", cur)
        return
    end
    if b.version == cur then
        DICT:set("bundle:etag", res.headers["ETag"])
        return
    end

    local ok, ierr = install(res.body, b)
    if not ok then
        DICT:incr("stat:install_err", 1, 0)
        ngx.log(ngx.ERR, "placement: install failed: ", ierr)
        return
    end
    DICT:set("bundle:etag", res.headers["ETag"])
    write_disk(res.body)
    ngx.log(ngx.NOTICE, "placement: installed version=", b.version,
                        " rules=", #b.rules)
end

function _M.start()
    if ngx.worker.id() ~= 0 then return end     -- 單一 fetcher
    ngx.timer.at(0, poll)
    local ok, err = ngx.timer.every(POLL, poll)
    if not ok then ngx.log(ngx.ERR, "placement: timer.every failed: ", err) end
end

function _M.staleness()
    local t = DICT:get("bundle:last_contact") or 0
    return ngx.time() - t
end

return _M
```

### 7.5 `router.lua` — segment trie、longest-prefix、`effective_at`

```lua
local _M = {}

-- node = { seg = { [segment] = node }, vers = { {at=, tier=, rid=, fb=}, ... } }
-- vers 依 at 遞減排序 → lookup 時第一個 at<=now 就是當前生效版本
local function new_node() return { seg = {} } end

local function sort_all(node)
    if node.vers then
        table.sort(node.vers, function(a, b) return a.at > b.at end)
    end
    for _, c in pairs(node.seg) do sort_all(c) end
end

function _M.build(b)
    local roots = {}
    for i = 1, #b.rules do
        local r = b.rules[i]
        if r.state ~= "deleted" then
            local root = roots[r.bucket]
            if not root then root = new_node(); roots[r.bucket] = root end
            local node = root
            if r.prefix and r.prefix ~= "" then
                for seg in string.gmatch(r.prefix, "([^/]+)") do
                    local nxt = node.seg[seg]
                    if not nxt then nxt = new_node(); node.seg[seg] = nxt end
                    node = nxt
                end
            end
            node.vers = node.vers or {}
            node.vers[#node.vers + 1] = {
                at   = r.effective_at or 0,
                tier = r.tier,
                rid  = r.id,
                fb   = r.read_fallback,
            }
        end
    end
    for _, root in pairs(roots) do sort_all(root) end

    local cl = {}
    for _, c in ipairs(b.clusters) do cl[c.id] = c end

    return {
        version   = b.version,
        roots     = roots,
        bdefault  = b.bucket_defaults or {},
        gdefault  = b.default_tier,
        clusters  = cl,
        nrules    = #b.rules,
    }
end

local function effective(node, now)
    local vs = node.vers
    if not vs then return nil end
    for i = 1, #vs do
        if vs[i].at <= now then return vs[i] end
    end
    return nil
end

-- 回傳 tier, rule_id, read_fallback
-- 重點：deeper 但尚未生效的 rule 必須讓位給 shallower 已生效的 rule，
--       所以是「一路往下走，記住最深的『已生效』命中」，而不是取最深節點。
function _M.resolve(R, bucket, key, now)
    local root = R.roots[bucket]
    local hit
    if root then
        hit = effective(root, now)               -- prefix="" 的 bucket 級 rule
        local node, from = root, 1
        while true do
            local p = string.find(key, "/", from, true)
            if not p then break end              -- 只在 "/" 邊界匹配
            node = node.seg[string.sub(key, from, p - 1)]
            if not node then break end
            local e = effective(node, now)
            if e then hit = e end
            from = p + 1
        end
    end
    if hit then return hit.tier, hit.rid, hit.fb end

    local bd = R.bdefault[bucket]
    if bd then return bd, "bucket-default", nil end
    return R.gdefault, "global-default", nil
end

-- 給 SAL 用：列出 LIST prefix 所涵蓋的有序區間（見 §8.3）
function _M.intervals(R, bucket, prefix, now) --[[ ... ]] end

return _M
```

複雜度：`resolve` 是 `O(key 的 segment 數)`，無回溯、無 regex、無 table 配置（除了 `string.sub` 的短字串）。10⁵ rules 下實測仍在數十 µs 內。

### 7.6 `route.lua` — access phase

```lua
local cjson    = require "cjson.safe"
local router_m = require "router"
local place    = require "placement"

local _M   = {}
local DICT = ngx.shared.placement

local BASE_DOMAIN = "s3.fab.local"
local _router                                   -- per-worker upvalue cache

------------------------------------------------------------------
local function get_router()
    local ver = DICT:get("bundle:version")
    if not ver then return nil, "no_policy" end
    if _router and _router.version == ver then return _router end

    local raw = DICT:get("bundle:raw:" .. ver)
    if not raw then
        -- raw 被 LRU 淘汰：沿用舊 router，不要 fail
        if _router then return _router end
        return nil, "raw_missing"
    end
    local b = cjson.decode(raw)
    if not b then
        if _router then return _router end
        return nil, "decode_failed"
    end
    _router = router_m.build(b)
    return _router
end

------------------------------------------------------------------
local function parse_target()
    local host = (ngx.var.http_host or ""):gsub(":%d+$", "")
    local uri  = ngx.var.uri                     -- 已 percent-decode，未 collapse slash

    -- virtual-hosted style: <bucket>.s3.fab.local/<key>
    local vb = host:match("^([^%.]+)%.s3%.fab%.local$")
    if vb then return vb, uri:sub(2) end

    -- path style: /<bucket>/<key...>
    local b, k = uri:match("^/([^/]+)(.*)$")
    if not b then return nil, nil end
    return b, (k or ""):sub(2)                   -- 去掉分隔的 "/"
end

-- 需要 SAL 做跨 cluster 協調的請求
local function needs_coordination(method, key, args)
    if key == "" then return true end                        -- 所有 bucket 級操作
    if args.uploads  ~= nil then return true end             -- Create/ListMultipartUploads
    if args.uploadId ~= nil then return true end             -- UploadPart/Complete/Abort/ListParts
    if args.delete   ~= nil and method == "POST" then return true end  -- DeleteObjects
    if ngx.var.http_x_amz_copy_source then return true end   -- Copy / UploadPartCopy
    return false
end

------------------------------------------------------------------
function _M.run()
    local R, err = get_router()
    if not R then
        ngx.log(ngx.ERR, "route: no usable policy: ", err)
        ngx.header["x-amz-request-id"] = ngx.var.request_id
        return ngx.exit(503)          -- fail closed：沒有 policy 絕不猜 tier
    end

    local bucket, key = parse_target()
    if not bucket then return end     -- ListBuckets 等，交給 SAL

    local method = ngx.req.get_method()
    local args   = ngx.req.get_uri_args(64)    -- 只讀 query string，不碰 body

    ngx.var.sal_bucket     = bucket
    ngx.var.sal_policy_ver = tostring(R.version)

    if needs_coordination(method, key, args) then
        ngx.var.sal_mode = "coordinate"
        ngx.var.sal_tier = ""
    else
        local tier, rid, fb = router_m.resolve(R, bucket, key, ngx.time())
        ngx.var.sal_mode    = "direct"
        ngx.var.sal_tier    = tier
        ngx.var.sal_rule_id = rid
        if fb and #fb > 0 then
            ngx.var.sal_fallback = table.concat(fb, ",")
        end

        -- rate limit：bucket 維度（既有）+ tier 維度（新增，保護 HDD）
        local ok = require("ratelimit").check(bucket, tier, method)
        if not ok then
            ngx.header["Retry-After"] = "1"
            return ngx.exit(503)
        end
    end

    require("metrics").inc_route(ngx.var.sal_tier, ngx.var.sal_mode)
end

function _M.healthz()
    local stale = place.staleness()
    local ver   = DICT:get("bundle:version") or 0
    local body  = cjson.encode{ policy_version = ver, policy_stale_sec = stale }
    if ver == 0 or stale > 300 then
        ngx.status = 503
    else
        ngx.status = 200
    end
    ngx.header["Content-Type"] = "application/json"
    ngx.say(body)
end

return _M
```

**Fail-closed 的取捨**：完全沒有 policy 時回 503，而不是套用 `default_tier`。理由 —— 猜錯 tier 的代價（物件寫到錯的 cluster，之後永久讀不到）遠高於短暫 503。反過來，policy「舊」時不 fail：因為 §6.3 的 `effective_at` 已保證舊 policy 仍是自洽且正確的。

### 7.7 Log phase：發 catalog event

```lua
-- catalog_emit.lua（節錄）
local EVT = ngx.shared.evtbuf

function _M.on_log()
    local m = ngx.req.get_method()
    if m ~= "PUT" and m ~= "POST" and m ~= "DELETE" then return end
    if ngx.status >= 300 then return end

    -- log_by_lua 不能用 cosocket，因此只寫 shared dict ring buffer
    local seq = EVT:incr("seq", 1, 0)
    EVT:set("e:" .. seq, ngx.var.sal_bucket .. "\t" .. (ngx.var.sal_tier or "")
            .. "\t" .. ngx.var.uri .. "\t" .. m .. "\t" .. ngx.status
            .. "\t" .. (ngx.var.upstream_bytes_received or 0), 300)
end

function _M.start()
    ngx.timer.every(1, function(premature)
        if premature then return end
        -- 批次讀出 → lua-resty-kafka producer（timer 中可用 cosocket）
    end)
end
```

若後續採用 §16 Phase 3 的「OpenResty 直連 HAProxy」快路徑，這個 log-phase event 就是 catalog 的唯一寫入來源，屆時需提升為 at-least-once（本地 spool file + ack）。

### 7.8 S3-behind-nginx 地雷清單

這些是 gateway 前置 S3 時最常踩、且症狀難查（多半表現為隨機 `SignatureDoesNotMatch`）的問題：

| 地雷 | 症狀 | 處置 |
|---|---|---|
| `merge_slashes on`（預設） | key 含 `a//b` 時 404 或簽章錯 | `merge_slashes off;` |
| `proxy_pass http://sal/;`（帶 URI） | nginx 改用 normalize+decode 的 `$uri`，含 `%20`、`+`、中文的 key 簽章全錯 | `proxy_pass http://sal;`（不帶 URI），且該 location 內不用 `rewrite ... break` |
| 改寫 `Host` | SigV4 的 `SignedHeaders` 一定含 `host` | `proxy_set_header Host $http_host;`；HAProxy 也不可改 |
| `proxy_request_buffering on`（預設） | 大 PUT 先落 nginx 暫存檔，磁碟爆、延遲飆高 | `off` |
| 動到 body | `aws-chunked` / `STREAMING-AWS4-HMAC-SHA256-PAYLOAD` 的 chunk signature 失效 | 絕不呼叫 `ngx.req.read_body()`；不加任何 body filter |
| 剝掉 `Content-Encoding: aws-chunked` | 同上 | 不要用 gzip/gunzip filter 在 S3 location |
| `client_max_body_size` 預設 1 m | 稍大的 PUT 直接 413 | `0`（不限） |
| Presigned URL 的 query 被改 | `X-Amz-Signature` 對不上 | 不做 `rewrite`、不排序/正規化 args |
| `ngx.var.uri` vs `$request_uri` 混用 | 用前者比對 rule 正確（已 decode），但轉發必須用原始 raw | 比對用 `ngx.var.uri`，轉發交給無 URI 的 `proxy_pass` |
| 加 header 破壞簽章？ | **不會**。只要新增的 header 不在 client 的 `SignedHeaders` 裡即安全 | `X-Sal-*` 命名保留給內部，並在 OpenResty 入口把 client 送來的同名 header 清掉（防偽造） |

> 最後一項很重要：**必須在入口清除 client 端傳來的 `X-Sal-*`**，否則使用者可以自己帶 `X-Sal-Tier: hot` 繞過 placement policy。實作上在 `access_by_lua` 用 `ngx.req.clear_header()` 清除，或在 SAL 端只信任來自 OpenResty 網段 + mTLS 的 header。

### 7.9 Policy 傳播的三個選項

| 選項 | 傳播延遲 | 複雜度 | 適用 |
|---|---|---|---|
| **A. nginx 內 poller**（上述） | 3–6 s | 低。無新元件 | **建議起點**。搭配 §6.3 後延遲不影響正確性 |
| **B. Sidecar agent + 本地檔** | 3–6 s | 中。多一個 Go agent | bundle > 幾 MiB，或想把驗簽/落地移出 nginx；agent 可與 SAL 節點共用 |
| **C. `lua-resty-etcd` watch** | < 1 s | 高。data plane 直連 etcd，需 etcd ACL/TLS/連線數規劃 | 需要秒內生效（例如緊急封鎖某 prefix）。APISIX 即此模式 |

三者對 §7.5 之後的程式完全相同——只換「誰把 raw 寫進 shared dict」。建議 Phase 1 用 A，把 B/C 留成可插拔。

---

## 8. Request Path 逐項語意

### 8.1 分類與處理摘要

| S3 操作 | 模式 | 處理 |
|---|---|---|
| `PutObject` / `GetObject` / `HeadObject` / `DeleteObject` | direct | 依 `X-Sal-Tier` 透傳；有 `X-Sal-Fallback` 時 404 回退重試 |
| `GetObject` with `Range` | direct | 同上，Range header 原樣透傳 |
| `ListObjectsV2` / `ListObjects` / `ListObjectVersions` | coordinate | §8.3 區間順序掃描 |
| `CreateMultipartUpload` | coordinate | 決 tier → 後端建 MPU → uploadId 編碼 tier |
| `UploadPart` / `CompleteMPU` / `AbortMPU` / `ListParts` | coordinate | 解碼 uploadId 取 tier，透傳 |
| `ListMultipartUploads` | coordinate | 扇出所有相關 tier + 合併 |
| `CreateBucket` / `DeleteBucket` / `HeadBucket` | coordinate | 扇出，§8.5 |
| `ListBuckets` | coordinate | 由 catalog 或 tier 聯集回答 |
| `Put/GetBucketVersioning|Tagging|Policy|ObjectLock` | coordinate | 扇出寫、單點讀 + 一致性檢查 |
| `CopyObject` / `UploadPartCopy` | coordinate | 同 tier → 原生 server-side copy；跨 tier → §8.6 |
| `DeleteObjects` (batch) | coordinate | 按 tier 分桶扇出、按原順序合併結果 |
| `GetBucketLocation` / `GetBucketAcl` | coordinate | SAL 直接合成回應 |

### 8.2 單物件讀寫

**寫入（`PutObject`）**

```
1. OpenResty resolve → tier T，設 X-Sal-Tier: T
2. SAL 驗證：以本地 policy 重算 tier
     · 若一致 → 透傳
     · 若不一致（node 間 version skew，理論上被 §6.3 消除）
       → 以 SAL 的結果為準，metric sal_policy_skew_total++，WARN log
3. SAL 檢查 clusters[T].min_object_size
     · Content-Length < min → 400 InvalidRequest
       "objects smaller than 1 MiB are not accepted on tier 'cold'"
     · 若 policy 設 small_object_redirect=true，改導到 fallback tier（warm）
       並在 catalog 標記實際 tier ≠ rule tier
4. HAProxy → MinIO(T)。SAL 不 buffer body，全程 stream
5. 成功後 SAL 送 catalog event {bucket,key,version_id,tier,size,etag,mtime}
```

**讀取（`GetObject` / `HeadObject`）**

```
1. resolve → tier T，透傳
2. 若 200/206 → 完成（絕大多數情形，零額外成本）
3. 若 404 且 X-Sal-Fallback = [F1, F2]（僅遷移中的 prefix 才有）
     → 依序重試 F1、F2（HEAD 先探再 GET，避免重複傳輸）
     → 命中則回應加 X-Sal-Served-From: F1 供監控
     → 全部 miss → 404
4. 若 catalog 已為權威（Phase 2）→ 跳過 fallback 猜測，直接查 catalog 取 tier
```

`read_fallback` 的成本必須有上限：**最多 2 個 fallback tier，且只有 `state=migrating` 的 rule 才允許帶**。遷移完成後 CP 必須把它清掉，這是 mover 的收尾步驟。

### 8.3 LIST — 區間順序掃描（本設計的核心演算法）

#### 問題

`ListObjectsV2(prefix=P)` 的結果可能橫跨多個 tier，且必須：
(a) 全域 lexicographic 有序；(b) `NextContinuationToken` 可跨頁續傳；(c) 絕不因某 tier 故障而**靜默截斷**（S3 client 無法察覺缺 key）。

#### 關鍵觀察

因為所有 rule prefix 都以 `/` 結尾，rule 集合對 key space 形成一個**嵌套結構**，而嵌套結構在 lexicographic 序上恰好誘導出一組**有序、不重疊、連續的區間**，每個區間單一 tier 擁有。

因此**不需要 k-way heap merge**，只要「依序掃過區間」即可。這也意味著：**每一頁通常只打 1 個 cluster**，扇出放大幾乎為零。

#### 範例

```
bucket = fab-lot，rules：
  ""                → cold   (bucket default)
  "raw/2026/"       → warm
  "raw/2026/wk32/"  → hot
  "wip/"            → hot

LIST(prefix="raw/") 的區間分解（⊤ 表示「大於所有以該 prefix 開頭的字串」的最小上界）：

  #  區間                                     owner   後端查詢
  ─────────────────────────────────────────────────────────────────────────────
  1  [ raw/            , raw/2026/       )    cold    prefix=raw/,           start-after=raw/
  2  [ raw/2026/       , raw/2026/wk32/  )    warm    prefix=raw/2026/,      start-after=raw/2026/
  3  [ raw/2026/wk32/  , raw/2026/wk32/⊤ )    hot     prefix=raw/2026/wk32/   ← 完整子樹，無多讀
  4  ( raw/2026/wk32/⊤ , raw/2026/⊤      )    warm    prefix=raw/2026/,      start-after=raw/2026/wk32/⊤
  5  ( raw/2026/⊤      , raw/⊤           )    cold    prefix=raw/,           start-after=raw/2026/⊤
```

實務上 `X⊤` 用 `X` 後接一個高位元組（`X .. "\xFF"` 重複至 key 長度上限，或直接用 `X .. string.rep("\xff", 8)`）近似即可；更嚴謹的做法是把最後一個字元 +1（`raw/2026/` → `raw/2026\x30`），但因為 rule prefix 一律以 `/`(0x2F) 結尾，`X` 去尾斜線後 +1 為 `X[:-1] .. "\x30"`，是精確上界。

#### 演算法

```
function list(bucket, P, delimiter, max_keys, token):
    R  ← router snapshot（若 token 帶舊 version，取該 version 的 snapshot，保留最近 8 代）
    IV ← intervals(R, bucket, P, now)      # 有序區間清單，由 trie DFS 產生

    i  ← token.i or 1
    k  ← token.k or nil                    # 上次回傳的最後一個 key
    cp ← token.cp or nil                   # 上次回傳的最後一個 CommonPrefix
    out_keys, out_cps ← [], []

    while i ≤ len(IV) and len(out_keys) + len(out_cps) < max_keys:
        iv ← IV[i]
        start ← max(iv.lower, k_successor(k))     # k 已回傳過，需 exclusive
        remain ← max_keys - len(out_keys) - len(out_cps)

        resp ← backend[iv.tier].ListObjectsV2(
                   bucket    = bucket,
                   prefix    = iv.query_prefix,   # 完整子樹時用子樹 prefix，否則用父 prefix
                   delimiter = delimiter,
                   start_after = start,
                   max_keys  = remain + 1)        # +1 用來判定是否還有更多

        for entry in resp（已依序）:
            if entry.key ≥ iv.upper: break        # 越界 → 換下一個區間
            if len(out_keys)+len(out_cps) ≥ max_keys: 
                truncated ← true; break
            if entry is CommonPrefix:
                if cp is not None and entry.prefix ≤ cp: continue   # 跨區間去重
                out_cps.append(entry); cp ← entry.prefix
            else:
                out_keys.append(entry); k ← entry.key

        if truncated: break
        if resp 已耗盡此區間: i ← i + 1; k ← nil

    if truncated or i ≤ len(IV):
        next_token ← b64(json{v: R.version, i: i, k: k, cp: cp})

    return { Contents: out_keys, CommonPrefixes: out_cps,
             IsTruncated: next_token ≠ nil, NextContinuationToken: next_token }
```

#### 設計細節

| 議題 | 處置 |
|---|---|
| **Continuation token 穩定性** | Token 帶 `v`（policy version）。SAL 保留最近 8 代 router snapshot，續頁時用 token 裡的版本重算區間，保證分頁期間區間切分不變。8 代 × 幾 MB 記憶體，可忽略 |
| **Token 過舊** | 若 token 的 `v` 已被淘汰 → 回 `InvalidArgument` 並在訊息說明需重新開始列舉。這比靜默錯結果好。實務上分頁 session 是秒級，不會遇到 |
| **`delimiter` 處理** | **不自行實作**，原樣下推給各後端 MinIO，只在 SAL 合併 + 去重 `CommonPrefixes`。因為區間有序，跨區間去重只需記住「上次回傳的最後一個 CommonPrefix」 |
| **跨區間重複的 CommonPrefix** | 例：`P=""`、`D="/"`、rule `raw/2026/`→warm，則 `raw/` 會被 cold 與 warm 兩個區間各產生一次。上述 `cp ≤ last_cp` 判斷即可去重（因為有序，一定連續出現） |
| **多讀成本** | 只有「非完整子樹」的區間會多讀（最多 `max_keys` 個越界 key 被丟棄）。可用「gap 區間的最長共同 prefix」收窄 query prefix 進一步優化 |
| **後端故障不可靜默** | 任一區間所屬 tier 回錯 → **整個 LIST 回 503 `SlowDown`/`ServiceUnavailable`**，絕不回部分結果 + `IsTruncated: false` |
| **`ListObjectVersions`** | 同演算法，比較 key 改為 `(key, version_id)` 二元序；各 tier 的 version_id 獨立，不需全域排序 |
| **`ListObjectVersions` 的 `key-marker`+`version-id-marker`** | 一併編進 token |

#### 更好的路：由 Catalog 服務 LIST

你已經為 5 B objects 的 LIST 退化建了 SAL Catalog。在有 catalog 的前提下，**LIST 應該直接由 catalog 回答**：

| | 區間掃描 | Catalog 查詢 |
|---|---|---|
| 一致性 | Strong（讀後端當下狀態） | Eventual（取決於 event pipeline lag，典型 < 1 s） |
| 延遲 | 取決於後端；HDD 上深 prefix 仍慢 | 穩定，索引查詢 |
| 跨 tier | 需區間邏輯 | 天然統一（catalog 本來就跨 cluster） |
| 後端負載 | 直接壓 MinIO metadata | 零 |
| `delimiter` | 下推給 MinIO | 需自行實作 `GROUP BY` / prefix rollup |

**建議策略**：
- 預設由 catalog 服務 LIST（快、不壓 MinIO、跨 tier 天然統一）。
- 保留 `x-sal-consistency: strong` header（或特定 IAM principal）走區間掃描，供對帳、備份掃描、審計等需要即時真值的場景。
- Catalog lag 超過閾值時，SAL 自動降級為區間掃描，並在回應加 `x-sal-list-source: backend`。

區間掃描因此不是白做——它是 catalog 的 fallback、也是 reconciler 的真值來源。

### 8.4 Multipart Upload

Tier 在 `CreateMultipartUpload` 就能決定（key 已知），問題是後續 `UploadPart` 必須回到同一個 cluster。

**做法：把 tier 編進 uploadId（stateless，優於外部 mapping）**

```
CreateMultipartUpload:
  tier ← resolve(bucket, key)
  backend_upload_id ← MinIO(tier).CreateMultipartUpload(...)
  sal_upload_id ← base64url( "1|" + tier + "|" + backend_upload_id )
  → 回給 client sal_upload_id

UploadPart / CompleteMPU / AbortMPU / ListParts:
  (ver, tier, backend_id) ← decode(uploadId)
  若 decode 失敗 → 404 NoSuchUpload
  改寫 query 的 uploadId 為 backend_id，轉發到 MinIO(tier)
```

注意事項：

| 議題 | 處置 |
|---|---|
| uploadId 是 opaque | S3 規格允許任意字串，SDK 一律當黑盒；安全 |
| **改寫 query 會破壞 SigV4** | uploadId 在 canonical query string 內。因此 MPU **必須走 re-sign 模式**（§9），不能 pass-through。這是 MPU 唯一需要重簽的原因 |
| 若不想 re-sign | 替代方案：SAL 把 `sal_upload_id ← backend_upload_id`（不改寫），另在 Redis/catalog 存 `upload_id → tier` 映射，TTL = MPU 最長生命週期（建議 7 天，與 MinIO 的 MPU 清理策略對齊）。犧牲 statelessness 換取 pass-through。**建議採此方案**，因為它避免了 SAL 持有使用者密鑰 |
| `ListMultipartUploads` | 依 §8.3 區間分解，扇出到涉及的 tier 並合併（按 `(key, uploadId)` 排序） |
| MPU 期間 rule 生效切換 | Create 時已釘住 tier，切換不影響進行中的 MPU。Complete 後 catalog 記錄的 tier 可能 ≠ 當前 rule tier → reconciler 標記為 misplaced，交由 mover 處理 |
| `Complete` 的原子性 | MinIO 端原生保證；SAL 只在 Complete 成功後才發 catalog event |

### 8.5 Bucket 層級操作

Bucket namespace 必須全域統一。策略：**lazy fan-out + catalog 為 namespace 真值**。

| 操作 | 行為 |
|---|---|
| `CreateBucket` | 先寫 catalog（`state=creating`）→ 在**所有 writable cluster** 上建 bucket（idempotent，`BucketAlreadyOwnedByYou` 視為成功）→ catalog 標 `active`。任一 cluster 失敗 → 回 500 並留下 `creating` 狀態供 reconciler 補齊 |
| 為何是「所有 cluster」而非「rule 涉及的 cluster」 | 因為 rule 可以事後新增。若只在部分 cluster 建 bucket，新增 rule 後第一次寫入會撞 `NoSuchBucket`。全建的成本只是每 cluster 一個空目錄 |
| `DeleteBucket` | 對**每個** cluster 檢查為空（`ListObjectsV2(max-keys=1)`），任一非空 → 409 `BucketNotEmpty`。全空 → 逐一刪除 → catalog 標 `deleted`。同時要求 CP 移除該 bucket 的所有 rule |
| `HeadBucket` | 由 catalog 回答（單點查詢，快）。catalog 不可用時降級為查任一 cluster |
| `ListBuckets` | 由 catalog 回答。降級時取所有 cluster 的**聯集** |
| `PutBucketVersioning` / `ObjectLock` / `Tagging` / `Policy` / `Lifecycle` | **扇出寫所有 cluster**，全成功才回 200；部分失敗 → 500 + 記錄不一致，由 reconciler 收斂。讀取時比對各 cluster 是否一致，不一致則回 500 並告警（不可回一個「看起來對」的值） |
| `PutBucketNotification` | 各 cluster 獨立配置；SAL 需把 event target 統一指向同一 queue，並在 event payload 補上 tier 欄位 |
| `PutBucketLifecycle` | **限制**：只允許 expiration 規則，**禁止 transition**（transition 會與本設計的 placement 語意衝突）。SAL 主動拒絕含 `Transition` 的 lifecycle 配置 |

### 8.6 跨 tier `CopyObject`

```
src_tier ← resolve(src_bucket, src_key)
dst_tier ← resolve(dst_bucket, dst_key)

if src_tier == dst_tier:
    → 原樣透傳，MinIO 原生 server-side copy（零資料經過 SAL）

else:
    size ← HEAD(src)
    if size ≤ COPY_INLINE_MAX (建議 5 GiB，並確認 SAL 有足夠頻寬預算):
        stream GET(src_tier) → PUT(dst_tier)，保留 metadata / tags / content-type
        （metadata-directive=COPY 時複製，REPLACE 時用 request 的）
    else:
        → 501 NotImplemented
          "cross-tier copy of objects larger than 5 GiB must be performed via
           the migration API: POST /v1/migrations"
```

理由：`CopyObject` 是同步 API，S3 SDK 的 timeout 通常 < 15 min。跨 cluster 搬 100 GiB 必然超時，client 端重試又會造成重複傳輸。與其做一個不可靠的同步大複製，不如**明確拒絕並導向非同步的 migration API**。在 fab 的受控工作負載下，這個限制可接受，且必須寫進使用者文件。

`UploadPartCopy` 跨 tier 亦無法原生完成（MinIO 不能從別的 cluster 讀）。處理：SAL 以 Range GET 讀取來源對應區段，再以 `UploadPart` 上傳。有大小上限（單 part ≤ 5 GiB）故實務可行。

### 8.7 `DeleteObjects`（batch）

```
1. 解析 XML body（這是少數需要讀 body 的操作，SAL 端讀，OpenResty 端不讀）
2. 按 tier 分桶：{ tier → [keys] }
3. 並行扇出（每 tier 最多 1000 keys/請求，S3 上限）
4. 合併：Deleted[] 與 Error[] 依 **request 中原始順序** 回填
5. 任一 tier 整體失敗 → 該 tier 的所有 key 標為 Error{Code: InternalError}，
   而不是讓整個請求失敗（S3 語意是 per-key 結果）
6. Quiet mode（<Quiet>true</Quiet>）照 S3 語意只回 Error[]
```

---

## 9. SigV4 與 IAM

### 9.1 兩種模式

| | **Pass-through（透傳）** | **Re-sign（重簽）** |
|---|---|---|
| 做法 | 不動 signed 內容，client 的簽章直接由 MinIO 驗證 | SAL 驗證 client 簽章，再用後端憑證重新簽發 |
| 前提 | 所有 cluster 上有**相同的 access key / secret**；`Host` 不變；URI/query/body 不變 | SAL 能取得 client secret（或走 STS），SAL 持有各 cluster 的服務帳號 |
| 優點 | SAL 不接觸密鑰；零 crypto 成本；不需 buffer body | 可任意改寫 URI/query/header；可做細緻 policy |
| 缺點 | 不能改寫任何 signed 元素（uploadId 在 query 內 → MPU 受限） | SAL 成為密鑰關鍵元件；需要讀 body 算 payload hash（大物件不可行，需用 `UNSIGNED-PAYLOAD`） |

### 9.2 建議：混合

```
資料路徑（GET/PUT/HEAD/DELETE/LIST/bucket ops）  →  Pass-through
SAL 自行合成的請求（LIST 區間掃描的子請求、
   bucket 扇出、cross-tier copy、reconcile、mover） →  SAL 服務帳號自簽
MPU                                              →  Pass-through + Redis 存 uploadId→tier 映射
                                                     （避免改寫 query，見 §8.4）
```

### 9.3 身分同步

Pass-through 要求各 cluster 有相同身分。三種做法：

| 做法 | 說明 | 建議 |
|---|---|---|
| **共用外部 IdP** | 各 cluster 都配 `MINIO_IDENTITY_LDAP_*` 指向同一 AD/LDAP，或 `MINIO_IDENTITY_OPENID_*` 指向同一 OIDC。身分與 policy 自動一致 | **首選**。fab 幾乎必有 AD |
| **IAM 同步器** | 一個 controller 讀「來源 cluster」的 `mc admin user/policy/svcacct` 狀態，同步到其他 cluster；以來源為單一真值 | 次選。要處理 service account 的 secret 無法讀出的問題（需由同步器代為建立） |
| 各自管理 | 不可行。使用者在 A cluster 有 key，在 B cluster 沒有 → 隨機 403 | ✗ |

另外，**bucket policy 也必須同步**（policy 綁在 bucket 上，各 cluster 獨立儲存）。這歸 §8.5 的扇出寫處理。

### 9.4 防偽造

`X-Sal-*` header 是內部信任邊界。必須：

1. OpenResty 入口 `ngx.req.clear_header("X-Sal-Tier")` 等，清掉 client 傳來的同名 header。
2. OpenResty → SAL 走 mTLS，SAL 只接受來自已知 client cert 的 `X-Sal-*`。
3. SAL 一律**重算一次** tier 並以自己的結果為準（OpenResty 的值僅作 skew 監控）。這使得偽造 header 完全無效。

---

## 10. Rule 變更與資料搬遷

> **⚠ 本節在 v2（immutable placement）下整節取代。** 見 `minio-tiering-v2-immutable-placement.md` §3（刪除清單）
> 與 §5.4/§5.5（簡化後的狀態機與 freeze 協定）。以下內容僅在允許事後改變 placement 時適用；
> §10.3 的 Mover 與 §10.4 的 Reconciler 在 v2 仍保留，但角色分別降為 admin break-glass 與純斷言監控。

### 10.1 為什麼這是最大的風險

Static tiering 的隱含合約是：**「讀取時重算 rule，必須得到寫入時的同一個 tier。」** 只要 rule 事後改指向，這個合約就破了：

- 舊物件仍在 old tier，但讀取被導向 new tier → **404**。
- 更糟：同一個 key 在 old tier 有 v1、new tier 有 v2 → **兩份資料、兩份 metadata**，`GET` 拿到哪個取決於當時的 rule → **靜默資料錯誤**。

所以「改 rule」不是配置變更，是**資料遷移**。

### 10.2 Rule state machine

```
                  ┌──────────┐
   PUT placement  │  DRAFT   │  驗證：schema / 容量 / 配額 / 衝突
                  └────┬─────┘
                       │ 若 diff 為「新增（該 prefix 下無物件）」
        ┌──────────────┴──────────────┐
        │                             │ 若 diff 為「改指向（有物件）」
        ▼                             ▼
  ┌───────────┐                 ┌──────────────┐
  │  STAGED   │  effective_at   │  需 migration │  必須先 POST /v1/migrations
  │ (排程生效) │                 │    ticket     │
  └─────┬─────┘                 └───────┬───────┘
        │ 全節點 ack + 時鐘到達            │
        ▼                               ▼
  ┌───────────┐                 ┌──────────────────────────────────┐
  │  ACTIVE   │                 │  MIGRATING                       │
  │ 乾淨狀態   │                 │  · new 寫入 → new tier            │
  └───────────┘                 │  · 讀取：new → 404 → read_fallback │
                                │  · mover 背景複製 old → new       │
                                └───────┬──────────────────────────┘
                                        │ 驗證通過（數量 + 抽樣 checksum）
                                        ▼
                                ┌──────────────────┐
                                │  DRAINING        │  刪除 old tier 上的副本
                                └───────┬──────────┘
                                        │ 清空且 catalog 一致
                                        ▼
                                ┌──────────────────┐
                                │  ACTIVE          │  清掉 read_fallback
                                └──────────────────┘
```

### 10.3 Mover

獨立的 Go worker（可重用你既有的 `mdchurn` / benchmark harness 基礎）：

```
1. 從 catalog 取得 (bucket, prefix) 下的物件清單，按 key 切成 range shard
   （不從 MinIO LIST 取，避免壓 metadata；catalog 本來就是為此建的）
2. 每 shard：GET(old) → PUT(new)，保留 metadata/tags/storage-class
   · 帶版本的 bucket：逐 version 複製，維持時間順序
   · 大物件用 MPU
   · 驗證：ETag（非 MPU 物件）或 size + 自算 SHA-256（MPU 物件 ETag 不可比）
3. 速率控制：
   · 全域 token bucket（MiB/s + objects/s 雙維度）
   · 尊重 OpenResty 的 per-tier rate limit（mover 走同一條 data path，
     用專屬 IAM principal，配額與使用者流量隔離）
   · HDD 來源：限制併發（建議 ≤ 32），避免 seek thrash 影響線上讀取
4. 進度：以 key range 為 checkpoint 寫回 CP，可中斷續傳
5. 完成條件：catalog 中該 prefix 下 old tier 物件數 = 0，且抽樣驗證 100% 通過
6. 收尾：CP 發新 bundle，清掉該 rule 的 read_fallback（此步驟前絕不可清）
```

### 10.4 Reconciler

持續執行的對帳工作（低優先度、可搶佔）：

| 檢查 | 動作 |
|---|---|
| Catalog 中 `object.tier ≠ resolve(rule)` | 標記 misplaced；若非遷移中 → 告警（表示曾發生誤路由或 MPU 跨越切換） |
| 同一 `(bucket, key, version_id)` 在多個 tier 都存在 | **最嚴重**。以 mtime 較新者為準，較舊者移入 quarantine bucket（不直接刪），發 P1 告警 |
| Catalog 有紀錄但 tier 上不存在 | 補刪 catalog 紀錄；若數量異常 → 告警 |
| Tier 上存在但 catalog 無紀錄 | 回填 catalog（event pipeline 掉訊息） |
| Bucket 在部分 cluster 缺失 | 補建 |
| Bucket 層級設定（versioning/lock/policy）跨 cluster 不一致 | 以 catalog 記錄的期望值收斂，或告警要求人工介入 |

Reconciler 掃描節奏：熱 prefix 每小時，全量每週（依 5 B objects 規模，全量掃描需分片並用 catalog 而非 LIST）。

---

## 11. HAProxy

```haproxy
global
    maxconn 200000
    nbthread 16
    tune.bufsize 32768
    tune.h2.initial-window-size 1048576

defaults
    mode http
    option http-keep-alive
    option forwardfor                   # X-Forwarded-For 未被簽章，安全
    timeout connect  3s
    timeout client   900s               # 大 PUT
    timeout server   900s
    timeout tunnel   3600s
    timeout http-keep-alive 60s
    timeout queue    30s
    retry-on conn-failure empty-response
    retries 2

frontend fe_s3
    bind 0.0.0.0:9000
    # 不要動 Host header（SigV4）
    # 依 SAL 標注的 tier 選 backend
    acl tier_hot  req.hdr(x-sal-tier) -m str hot
    acl tier_warm req.hdr(x-sal-tier) -m str warm
    acl tier_cold req.hdr(x-sal-tier) -m str cold
    use_backend be_minio_hot  if tier_hot
    use_backend be_minio_warm if tier_warm
    use_backend be_minio_cold if tier_cold
    # 未標注 → 明確拒絕，不要落到 default（避免猜錯 cluster）
    http-request deny deny_status 400 unless tier_hot or tier_warm or tier_cold

backend be_minio_hot
    balance leastconn                   # 請求大小差異大，優於 roundrobin
    option httpchk GET /minio/health/live
    http-check expect status 200
    server h1 minio-hot-1:9000 check inter 2s fall 3 rise 2 maxconn 2048
    server h2 minio-hot-2:9000 check inter 2s fall 3 rise 2 maxconn 2048
    server h3 minio-hot-3:9000 check inter 2s fall 3 rise 2 maxconn 2048
    server h4 minio-hot-4:9000 check inter 2s fall 3 rise 2 maxconn 2048

backend be_minio_cold
    balance leastconn
    option httpchk GET /minio/health/live
    http-check expect status 200
    # HDD：刻意壓低 maxconn。超出即在 HAProxy 排隊，
    # 好過讓 MinIO 內部與磁碟佇列崩塌（HDD 上尾延遲會指數惡化）
    server c1 minio-cold-1:9000 check inter 5s fall 3 rise 2 maxconn 256
    server c2 minio-cold-2:9000 check inter 5s fall 3 rise 2 maxconn 256
    # …
```

| 決策 | 理由 |
|---|---|
| 用 header 選 backend，而非讓 SAL 直連節點 | SAL 不需知道 MinIO 節點清單；擴縮容只改 HAProxy |
| `balance leastconn` | 物件大小分布長尾，roundrobin 會把大請求疊到同一節點 |
| `/minio/health/live` 做 health check | 節點層存活。`/minio/health/cluster` 是 cluster 層 quorum，適合放到監控而非 LB（否則一個 cluster 問題會讓全部節點同時下線） |
| cold backend `maxconn` 顯著低於 hot | HDD 併發保護。搭配 §7.6 的 per-tier rate limit 形成兩層入場控制 |
| `http-request deny unless tier_*` | fail closed。缺 header 時寧可 400，不可猜 |
| 不設 `default_backend` | 同上 |

---

## 12. MinIO 各 tier 調校

### 12.1 Erasure coding

| Tier | 介質 | 建議 set size / EC | 有效容量比 | 理由 |
|---|---|---|---|---|
| hot | NVMe | 8 drives, `EC:2` | 75% | 低重建放大、低尾延遲 |
| warm | SATA SSD | 12 drives, `EC:3` | 75% | 平衡 |
| cold | HDD | 16 drives, `EC:4` | 75% | 較寬條帶提高單次 IO 效率；HDD 重建慢，需較高容錯 |

以 `MINIO_STORAGE_CLASS_STANDARD=EC:N` 設定。注意：**EC 設定在 pool 建立後不可改**，需在部署前定案。

### 12.2 環境變數建議

```bash
# ── cold (HDD) ─────────────────────────────────────────
MINIO_STORAGE_CLASS_STANDARD=EC:4
MINIO_STORAGE_CLASS_INLINE_BLOCK=256KiB   # 小檔直接內嵌 xl.meta，省一次 IO
MINIO_SCANNER_SPEED=slowest               # 保護 HDD IOPS 給前台
MINIO_HEAL_DRIVE_WORKERS=1
MINIO_API_REQUESTS_MAX=1200               # 搭配 HAProxy maxconn 形成兩層限流
MINIO_API_REQUESTS_DEADLINE=2m

# ── hot (NVMe) ─────────────────────────────────────────
MINIO_STORAGE_CLASS_STANDARD=EC:2
MINIO_SCANNER_SPEED=default
MINIO_API_REQUESTS_MAX=0                  # 不限，靠 OpenResty 限流
```

OS 層（cold tier）：XFS + `noatime,nodiratime`、`mq-deadline` 排程器、`read_ahead_kb` 調大（HDD 上循序讀有利）、`vm.dirty_ratio` 調低避免寫入尖峰。

### 12.3 Cold tier 的小檔問題（LOSF）

HDD 上一個 1 KiB 物件與一個 1 MiB 物件的成本幾乎相同（都是一次 seek）。5 B objects 若平均 10 KiB，光 metadata IO 就會吃光 HDD。

三個層次的處置，建議全上：

1. **策略層（最有效）**：`clusters[cold].min_object_size = 1 MiB`。SAL 在 `PutObject` 直接拒絕更小的物件並給出明確錯誤訊息。使用者被迫在應用端打包（tar/parquet/zip），這對 fab 的 wafer map、log bundle 場景本來就是正確做法。
2. **自動重導**：`small_object_redirect=true` 時，小於門檻的物件改寫入 warm，catalog 記錄實際 tier。使用者無感，但 warm 容量規劃需納入。
3. **inline 內嵌**：`MINIO_STORAGE_CLASS_INLINE_BLOCK=256KiB`，讓小物件資料直接進 `xl.meta`，把「metadata IO + data IO」合併成一次。

若上述仍不足，才考慮你先前評估的 **packing gateway**（SAL 之下加一層把小物件聚合成 blob + 自維護 offset index）。但那會讓 SAL 從「routing 層」變成「儲存格式層」，複雜度躍升一個量級 —— 建議先用策略層擋住，把 packing 留作 Phase 4 的獨立提案。

---

## 13. 失效模式與降級

| 故障 | 影響 | 降級行為 | 復原 |
|---|---|---|---|
| **Control Plane 全掛** | 無法新增/修改 rule | Data path 完全不受影響（last-known-good in shm + 落地檔）。`policy_stale_sec` 攀升觸發告警 | CP 恢復後自動續傳 |
| **Bundle 內容損毀 / 驗簽失敗** | 無 | 拒絕安裝，保留舊版本，`stat:verify_err` 告警 | CP 重新產生 |
| **OpenResty 節點時鐘偏移 > Δ** | 該節點提前/延後切換 rule | Heartbeat 帶本地時間，CP 偵測偏移 > 5 s 即告警並暫停 staged activation | 修 NTP |
| **某節點 policy 落後** | §6.3 保證不會誤路由（因為舊 policy 自洽）；但新 rule 對該節點不生效 | CP 在 ack 未達 100% 前不讓 rule 生效 | 自動延後或人工介入 |
| **完全沒有 policy（冷啟動 + 磁碟無檔 + CP 掛）** | 該節點無法服務 | **503 fail closed**，`/healthz` 回 503 → LB 摘除該節點 | 從 peer 節點複製落地檔，或等 CP |
| **SAL 節點掛** | 該節點連線中斷 | OpenResty upstream `max_fails` 摘除；client 重試 | 無狀態，重啟即可 |
| **某 tier cluster 掛** | 只影響該 tier 的資料 | 單物件操作回 503；**LIST 若涉及該 tier → 整體 503，絕不回部分結果** | 該 cluster 恢復 |
| **Cold cluster 過載** | 尾延遲惡化 | OpenResty per-tier 限流 + HAProxy `maxconn` 排隊 + `Retry-After` | 自動 |
| **Catalog lag 過大** | LIST 結果偏舊 | 自動降級為區間掃描，回應加 `x-sal-list-source: backend` | pipeline 恢復 |
| **誤路由（bug）造成物件寫錯 cluster** | 該物件讀不到 | Catalog 記錄「實際 tier」（來自寫入回應），讀取以 catalog 為準即可自癒；reconciler 標記並排入 mover | Phase 2 後基本免疫 |
| **同 key 兩 tier 各一份** | 靜默資料錯誤 | Reconciler P1 告警，新者為準，舊者移 quarantine（不刪） | 人工確認 |

### 13.1 Fail-open vs fail-closed 的原則

| 情境 | 選擇 | 理由 |
|---|---|---|
| 沒有 policy | **Closed**（503） | 猜錯 tier = 資料寫錯位置，代價不可逆 |
| Policy 舊但有效 | **Open** | 舊 policy 自洽正確，只是不含最新 rule |
| Tier 不在 bundle 的 clusters 清單 | **Closed**（500） | 設定錯誤 |
| Catalog 不可用 | **Open**（降級） | Catalog 是加速層，非正確性依賴（Phase 1） |
| LIST 部分 tier 失敗 | **Closed**（503） | 部分結果無法被 client 察覺，比報錯更危險 |

---

## 14. 可觀測性與 SLO

### 14.1 必備指標

```
# Policy 傳播（熱更新的健康度 —— 這組是本設計的命脈）
sal_policy_version{node,role}                   gauge   各節點目前版本
sal_policy_target_version                       gauge   CP 目前版本
sal_policy_stale_seconds{node}                  gauge   距上次成功接觸 CP
sal_policy_rebuild_total{node}                  counter worker 重建 trie 次數
sal_policy_fetch_errors_total{node,reason}      counter
sal_policy_verify_errors_total{node}            counter
sal_policy_skew_total{decided_by}               counter OpenResty 與 SAL 決策不一致
sal_policy_rollout_ack_ratio{version}           gauge   已 ack 節點 / 總節點

# Routing
sal_route_total{tier,mode,rule_id}              counter  ※ 絕不以 key 或 prefix 當 label
sal_route_default_total{bucket}                 counter  落到 default 的比例（過高 → 使用者沒設 rule）
sal_route_latency_seconds{phase}                histogram

# 資料面
sal_request_duration_seconds{tier,op,status}    histogram
sal_request_bytes{tier,op,direction}            histogram
sal_fallback_hits_total{from_tier,to_tier}      counter  遷移期回退命中
sal_small_object_rejected_total{tier,bucket}    counter
sal_cross_tier_copy_total{src,dst,result}       counter

# LIST
sal_list_intervals_per_request                  histogram  區間數（放大程度）
sal_list_backend_calls_per_page                 histogram  應接近 1
sal_list_source_total{source}                   counter    catalog / backend
sal_list_token_expired_total                    counter

# 對帳
sal_reconcile_misplaced_objects                 gauge
sal_reconcile_duplicate_objects                 gauge   ※ 應恆為 0，非 0 即 P1
sal_migration_progress_ratio{migration_id}      gauge
```

### 14.2 告警

| 告警 | 條件 | 級別 |
|---|---|---|
| Duplicate object across tiers | `sal_reconcile_duplicate_objects > 0` | **P1** |
| Policy skew | `sal_policy_skew_total` 5 分鐘增量 > 0 | **P1** |
| Policy 傳播卡住 | `sal_policy_target_version - min(sal_policy_version) > 0` 持續 > 120 s | P2 |
| Policy stale | `sal_policy_stale_seconds > 300` | P2 |
| Rollout 未達 100% ack | `sal_policy_rollout_ack_ratio < 1` 且已接近 `effective_at` | P2 |
| Default route 比例異常 | `sal_route_default_total` 佔比 > 20% | P3（使用者教育） |
| Tier 容量 | 任一 tier 使用率 > 80% | P2 |
| LIST 放大 | `sal_list_backend_calls_per_page` p99 > 4 | P3（rule 設計過於碎片化） |

### 14.3 SLO

| SLO | 目標 |
|---|---|
| S3 API 可用性（非 cold tier） | 99.95% / 月 |
| `GetObject` p99（hot） | < 25 ms（1 MiB） |
| `GetObject` p99（cold） | < 400 ms（1 MiB，含排隊） |
| `ListObjectsV2` p99（catalog 路徑） | < 150 ms（1000 keys） |
| Policy 變更端到端生效時間 | < 60 s（含 30 s activation delay） |
| 誤路由事件 | 0 / 年 |

---

## 15. 容量、配額、Chargeback

Placement 由使用者自決，若無配額，所有人都會把資料塞進 hot tier。

```jsonc
// CP 內部的配額模型
{
  "team": "yield-eng",
  "quotas": { "hot": "20TiB", "warm": "200TiB", "cold": "2PiB" },
  "usage":  { "hot": "18.4TiB", "warm": "142TiB", "cold": "1.1PiB" },  // 來自 catalog 彙總
  "policy": {
    "on_exceed": "reject_rule_change",   // 不阻擋既有寫入，只阻擋新增 hot rule
    "hot_rule_ttl_days": 90              // hot rule 自動到期，需主動續期
  }
}
```

兩個關鍵機制：

1. **`hot_rule_ttl_days`** —— 指向 hot tier 的 rule 帶到期日。到期前 14 天通知 owner；到期後自動轉為建立一個「hot → warm」的 migration 提案（需 owner 確認）。這解決了 static tiering 最常見的長期問題：**沒人回來把資料降級**。
2. **Chargeback 報表** —— 由 catalog 按 `(team, bucket, prefix, tier)` 彙總 bytes × 單價，月度出帳。這是讓使用者自我約束最有效的手段，比技術限制更管用。

---

## 16. 分階段上線

### Phase 1 — 基本 placement routing（4–6 週）

**範圍**：CP + bundle 分發 + OpenResty 熱更新 + SAL direct/coordinate + HAProxy + hot/cold 兩個 cluster。
**限制**：Rule **append-only**；一旦某 prefix 下有物件，不允許改指向。LIST 走區間掃描。
**驗收**：
- [ ] 新增 rule 後 60 s 內全節點生效，`nginx -s reload` 次數 = 0
- [ ] 混沌測試：poll 期間殺 CP、殺 worker 0、灌損毀 bundle、時鐘偏移 → 皆不誤路由
- [ ] 對照測試：對同一組 key，經 SAL 的 LIST 結果與直連各 cluster 手動合併的結果**逐 key 相同**（含 delimiter、分頁、versions）
- [ ] SigV4 相容性矩陣：aws-cli v2、boto3、mc、s3fs、rclone、presigned URL、含特殊字元（空格、`+`、`%`、`//`、UTF-8）的 key、`aws-chunked` 上傳
- [ ] `X-Sal-Tier` 偽造測試：client 自帶 header 無效

### Phase 2 — Catalog 整合與遷移能力（6–8 週）

- Catalog 記錄「實際 tier」，成為讀取路徑的權威（消除 rule 改指向的風險）
- LIST 改由 catalog 服務，區間掃描降為 `consistency=strong` 與 reconciler 用
- Rule state machine 完整化 + Mover + Reconciler
- 配額與 chargeback

### Phase 3 — 效能與延遲最佳化（4 週）

- OpenResty 對 `direct` 模式**直連 HAProxy**，繞過 SAL（省一跳，約 0.3–1 ms + 一份連線資源）。此時 catalog event 唯一來源變成 OpenResty 的 `log_by_lua`，需升級為 at-least-once（本地 spool + ack）
- etcd watch 取代 poll（policy 生效 < 1 s）
- Per-tier adaptive admission control（依 MinIO 回壓動態調整）
- Warm tier 上線

### Phase 4 — 選項（另案評估）

- Cold tier packing gateway（小檔聚合）
- Dynamic tiering（依 access pattern 自動建議 rule，仍需人工核准）

---

## 17. 已知限制與風險

| # | 限制 / 風險 | 嚴重度 | 緩解 |
|---|---|---|---|
| 1 | Rule 改指向需資料遷移，不是純配置變更 | **高** | **v2 已結構性消除**：placement 不可變 → 無 re-point。見 v2 §2。若不採 v2，則靠 §10 state machine + `:validate` 影響評估 + catalog 為權威 |
| 2 | 跨 tier `CopyObject` > 5 GiB 不支援 | 中 | 明確 501 + 導向 migration API；寫入使用者文件 |
| 3 | LIST 跨 tier 有放大；rule 過碎會退化 | 中 | 每 bucket rule 上限 512；`sal_list_intervals_per_request` 監控；Phase 2 後由 catalog 服務 |
| 4 | Bucket 層級設定需跨 cluster 一致，是分散式狀態 | 中 | 扇出寫 + reconciler；讀取不一致時 fail（不猜） |
| 5 | Pass-through SigV4 要求 IAM 跨 cluster 同步 | 中 | 共用 LDAP/OIDC；否則需 IAM 同步器 |
| 6 | `effective_at` 依賴 NTP | 低 | Δ=30 s vs 偏移 < 50 ms，安全邊界 600×；偏移 > 5 s 告警並暫停 activation |
| 7 | SAL 是新的關鍵路徑元件 | 中 | 無狀態、N+2、health check、Phase 3 的 direct 模式可繞過 |
| 8 | Cold tier 小檔會殺死 HDD | **高** | §12.3 三層處置，以 `min_object_size` 為主 |
| 9 | 使用者可能全塞 hot tier | 中 | §15 配額 + `hot_rule_ttl_days` + chargeback |
| 10 | MinIO CE 授權與版本策略（AGPL、CE 功能收斂） | **高**（非技術） | 本設計刻意不依賴 MinIO 的 ILM/tiering/replication 等進階功能，只用核心 S3 + EC，因此後端可替換為 Ceph RGW / SeaweedFS / RustFS 而不動 SAL 以上任何一層。這是本架構最重要的策略性收益 |

> 第 10 點值得單獨強調：**把 placement 邏輯放在 gateway 而非依賴 MinIO 的 tiering 功能，等於把「後端可替換性」設計進了架構裡。** 只要後端說 S3、支援 EC，就能當作一個 tier 掛進來。日後若某個 tier 要換成 Ceph RGW 或 SeaweedFS，只是新增一個 `clusters[]` 條目 + 一個 HAProxy backend + 一次 migration。

---

## 附錄 A — ADR 摘要

### ADR-1：Placement routing 於 gateway，而非 MinIO ILM transition

**Decision**：在 SAL 層做 write-time placement。
**Alternatives**：(a) MinIO ILM remote tier；(b) 各 tier 給不同 endpoint，由應用端自己選。
**Rationale**：(a) 把 metadata 全壓在 source cluster，直接惡化既有的 5 B objects LIST 問題，且 source cluster 無法縮容；(b) 破壞單一 namespace，且 rule 變更要改所有應用端。
**Consequences**：+ 後端可替換、metadata 分散、各 tier 獨立擴縮；− 需自己實作 LIST 合併與 MPU pinning；− SAL 成為新的關鍵元件。

### ADR-2：Policy 分發用「poll + 排程生效」，而非 push-only

**Decision**：HTTP poll（3 s，ETag）取得 versioned bundle + `effective_at` 排程生效 + 節點 ack。
**Alternatives**：(a) CP 主動 push 到各節點；(b) etcd watch；(c) 直接 `nginx -s reload`。
**Rationale**：Push 的難點是「確認全節點收到」與節點清單維護；poll 天然自我修復（節點重啟自己來拿）。而**正確性不靠傳播速度，靠排程生效**——這使 3 s poll 完全夠用。`reload` 有 worker 世代堆積與長連線斷開問題。
**Consequences**：+ 節點無狀態、CP 無需知道節點清單、自我修復；+ 排程生效讓 skew 窗口從秒級壓到毫秒級；− 生效延遲 30 s（可調，但這是刻意的安全緩衝）；− CP 需維護 ack 表以判斷 rollout 完成。
**Revisit when**：需要「秒內封鎖某 prefix」的安全需求出現 → 切 etcd watch（§7.9 選項 C），程式不變。

### ADR-3：SigV4 pass-through 為主，SAL 自簽為輔

**Decision**：資料路徑 pass-through；SAL 合成的請求用服務帳號自簽；MPU 用 Redis 映射避免改寫 query。
**Alternatives**：全面 re-sign。
**Rationale**：Re-sign 需要 SAL 能取得使用者 secret（安全面難以接受），且對大物件必須算 payload hash 或退化成 `UNSIGNED-PAYLOAD`。
**Consequences**：+ SAL 不持有使用者密鑰、零 crypto 成本、不需 buffer body；− 各 cluster 必須有相同身分（需共用 IdP 或 IAM 同步器）；− 不能改寫任何 signed 元素，故 MPU 需外部映射狀態。

---

## 附錄 B — 使用者文件要寫進去的三句話

1. **Tiering 是寫入時決定的，不會自己搬。** 你今天把 `raw/2026/` 設成 cold，明天改成 hot，昨天寫進去的資料**不會**跟著跑——必須開 migration。
2. **Cold tier 不收小於 1 MiB 的物件。** 請在應用端打包（tar / parquet）。這不是限制，是 HDD 的物理現實。
3. **改設定後約 30–60 秒生效**，你會在 `:validate` 看到精確的生效時間。不需要通知平台團隊、不需要維護窗口。
