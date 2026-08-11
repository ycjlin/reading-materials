# 設計修訂 v2 — Immutable Placement

**Supersedes:** v1 設計文件的 §4.3（約束）、§8.2（fallback）、§10（Rule 變更與資料搬遷）、§13（部分）、§16（階段規劃）、§17（風險 #1）
**新增約束:** 使用者一旦設定 `(bucket, prefix) → cluster`，即不可更改

---

## 0. 這個約束買到什麼、代價是什麼

| | v1（可變 placement） | v2（immutable placement） |
|---|---|---|
| 正確性保證方式 | **靠對帳維持**：catalog 記錄實際位置 + reconciler 修補 | **結構性保證**：可從 rule set 本身證明 |
| Rule state machine | 6 個狀態，含 MIGRATING / DRAINING | 3 個狀態（+1 個 freeze 過渡態） |
| `read_fallback` | 必要 | **刪除** |
| Mover | 使用者功能 | **降為 admin break-glass 工具** |
| Catalog 在讀取路徑 | Phase 2 起為權威（正確性依賴） | **完全離開讀取路徑**，只做 LIST 加速 / 帳務 / inventory |
| 同 key 兩份 tier | 需 reconciler 偵測修補 | **結構上不可能**，非 0 即代表有 bug |
| Policy skew 的後果 | 誤路由（不可逆資料事故） | 短暫 403（可自癒，benign） |
| 單物件讀取 | 可能需 404 → fallback 重試 | 恆為單一跳，零重試 |
| SAL 是否必經 | 是 | **絕大多數請求可在 OpenResty 決策後直達 HAProxy** |
| 代價 | — | **誤設無法補救**；namespace 規劃壓力前移；失去「大範圍 cold + 小範圍 hot 例外」的表達力 |

淨評估：**這個約束值得付。** 它把系統從「靠對帳維持不變量」改成「靠結構保證不變量」，砍掉整個遷移子系統，同時把最嚴重的失效模式（靜默資料錯誤）降級為 benign 的暫時性錯誤。代價集中在使用者體驗，而 UX 代價可以用工具（validate gate、prefix template、tier-in-prefix 慣例）補回大半。

---

## 1. 先釐清一件事：「Rule 不可更改」≠「Placement 不可變」

這是整份修訂最重要的一段。

如果只把約束實作成「rule 一旦建立就不能修改，只能新增」（append-only），**不變量仍然是破的**：

```
t1  使用者宣告：  raw/  → cold
t2  寫入物件：    raw/2026/wk32/lot001.tar     → 落在 cold ✓
t3  使用者「新增」rule： raw/2026/wk32/ → hot      ← 這是「新增」，不是「修改」！
t4  讀取 raw/2026/wk32/lot001.tar
    → resolve 命中更深的 rule → 導向 hot → 404
```

**在 trie 裡插入一個更深的節點，在語意上等同於把該子樹底下所有既存物件 re-point。** 「不修改既有 rule」完全防不住這件事。

所以約束必須定義在**行為**上，而不是在 rule 這個資料列上：

> **不變量 I（Placement Determinism）**
> 對任一 `(bucket, key)`，若曾在時刻 `t_w` 成功寫入，則對所有 `t > t_w`：
> `resolve(bucket, key, t) = resolve(bucket, key, t_w)`

換句話說：**任何 policy 變更都不得改變任何已寫入 key 的解析結果。**

---

## 2. 執行模型

### 2.1 三條規則

要讓不變量 I 成立，需要三件事同時成立：

| 規則 | 內容 | 防的是什麼 |
|---|---|---|
| **R1 — 不可嵌套** | 任兩條 rule 的 prefix 不得互為 prefix。新 rule 的 prefix 既不能是既有 prefix 的前綴，也不能被既有 prefix 前綴 | §1 的嵌套插入陷阱 |
| **R2 — 先宣告後寫入** | 寫入的 key 必須落在某條已生效 rule 的子樹內；否則拒絕。**不存在隱含的 default tier fallback** | 「未宣告區域先有資料，事後宣告 = re-point」 |
| **R3 — 首次寫入即封印** | Rule 在「子樹仍為空」之前可自由刪除；一旦該子樹有物件，rule 即封印（不可改 tier、不可刪除） | 誤設在無資料階段仍可救；有資料後鎖死 |

R1 + R2 合起來，就讓不變量 I 成為**只需檢查 rule set 本身**即可保證的性質 —— 不需要查任何物件是否存在，不需要 catalog，不需要對帳。

證明草稿：

```
由 R1，rule prefix 集合互斥 → 任一 key 至多落在一條 rule 的子樹內。
由 R2，key 能被寫入 ⟺ 寫入時存在唯一一條 rule R 使 key ∈ subtree(R)。
由 R1，任何後續新增的 rule R' 之 prefix 與 R 互斥
      → subtree(R') ∩ subtree(R) = ∅ → key ∉ subtree(R')。
由 R3，R 本身不可改 tier、不可刪除（因 subtree(R) 非空）。
⟹ resolve(key) 恆為 R.tier。∎
```

### 2.2 Bucket placement mode（建立時決定，不可更改）

R2 意味著必須有個明確的「宣告」動作。把它綁在 bucket 上，分三種模式：

| Mode | 語意 | 適用 |
|---|---|---|
| **`single`** | 整個 bucket 一個 tier。等價於唯一一條 `prefix=""` 的 rule。因 R1，此模式下不得再有任何其他 rule | 最常見。單一用途的 bucket |
| **`partitioned`** | 無 root rule；一組互斥的 prefix rule。可事後**新增**互斥 rule；寫入未宣告區域 → 拒絕 | 需要在同一 namespace 內混放不同 tier |
| **`templated`** | `partitioned` + 一個 prefix template 與自動預宣告器（§5.2） | 時間分區資料（fab 的週/日批次） |

`single` ⇄ `partitioned` **不可互轉**。想轉 → 建新 bucket。這聽起來嚴格，但它讓「placement 是 bucket 的型別」這件事可以在一句話裡講完。

### 2.3 允許的操作矩陣

| 操作 | `single` | `partitioned` / `templated` |
|---|---|---|
| 建立 bucket 時指定 tier / 初始 rule 集 | ✔ | ✔ |
| 新增互斥的 rule | ✘（R1 禁止，root rule 已覆蓋全部） | ✔ |
| 新增嵌套的 rule | ✘ | ✘（R1） |
| 修改既有 rule 的 tier | ✘ | ✘ |
| 刪除 rule（子樹為空，走 freeze 協定） | ✘（等同刪 bucket） | ✔ |
| 刪除 rule（子樹非空） | ✘ | ✘（R3） |
| 寫入未宣告區域 | n/a（全部已宣告） | ✘ → `403 NoPlacementRule` |

### 2.4 R1 的兩種強度（重要的設計取捨）

R1 寫成「結構性禁止嵌套」是最強的形式，但它犧牲了表達力（無法做「`raw/` 全放 cold，但 `raw/2026/wk32/` 例外放 hot」）。較弱的替代是「允許嵌套，但要求該子樹為空」：

| | **S1：結構性禁止嵌套（建議預設）** | **S2：允許嵌套但子樹須為空** |
|---|---|---|
| 判定依據 | 只看 rule set，O(log n) | 需查該子樹是否有物件 |
| 資料面依賴 | 無 | catalog 或一次跨 tier LIST 探測 |
| 正確性 | 可證明（§2.1） | 依賴 catalog 正確性 **且** 無競態 |
| 競態 | 無 | **有**（見下） |
| 表達力 | 低，需前期規劃 | 高 |
| 建議 | 自助服務的預設 | 平台團隊代操作的例外路徑，且必須走 freeze 協定 |

**S2 的競態**：

```
t1  CP 檢查 raw/2026/wk32/ 子樹為空 → 通過
t2  某節點以舊 policy 把 raw/2026/wk32/x 寫入 cold（合法，命中 raw/→cold）
t3  新 rule raw/2026/wk32/ → hot 生效
t4  讀 raw/2026/wk32/x → 導向 hot → 404。資料實質遺失。
```

修法只有一個：**先凍結、再驗空、再發布**（§5.5 的 freeze 協定）。也就是說 S2 並不比 S1 少一個機制，只是多一次驗空。所以：**預設 S1，把 S2 當成需要 CAB 的特殊路徑**。

---

## 3. v1 設計的刪除清單

| v1 元素 | 動作 | 為什麼安全 |
|---|---|---|
| Rule state machine 的 `MIGRATING` / `DRAINING` | **刪除** | 不再有 re-point，就不再有遷移 |
| `read_fallback` 欄位與 `X-Sal-Fallback` header | **刪除** | 讀取永遠只有一個正確 tier |
| §8.2 的「404 → 依序重試 fallback tier」邏輯 | **刪除** | 單物件讀取恆為單一跳 |
| Mover（使用者可觸發的遷移） | **降級**為 admin-only break-glass（§6.4），不出現在自助 API | 正常流程不需要它 |
| `POST /v1/migrations` 公開端點 | **刪除**（改為內部端點 + 需 CAB） | 同上 |
| Reconciler 的「同 key 多 tier → 取新者、舊者入 quarantine」修復邏輯 | **改為純斷言**：`sal_invariant_duplicate_objects` 必須恆為 0，非 0 直接 P1，不自動修復 | 結構上不可能發生；若發生表示有 bug，自動修復會掩蓋 bug |
| Reconciler 的「misplaced → 排入 mover」 | 同上，改為斷言 + 告警 | 同上 |
| Catalog 作為讀取路徑權威（v1 Phase 2 的核心） | **刪除此角色** | Prefix rule 現在永遠正確。Catalog 回歸純加速層 |
| `bucket_defaults` / `default_tier` 的隱含 fallback | **刪除**（R2） | 隱含 fallback 正是「未宣告先寫入」的來源。`single` mode 的 root rule 是**顯式**宣告，不是 fallback |
| v1 §16 Phase 2 的「遷移能力」 | **刪除整個 Phase** | 階段規劃因此縮短約 6–8 週 |

補充說明最後一項：`default_tier` 之所以必須刪掉，是因為它讓任何未宣告的 key 都能寫入。一旦寫入，該區域就有了資料，之後宣告任何覆蓋它的 rule 都是 re-point。**隱含 default 與不變量 I 天生衝突。**

---

## 4. Router 的簡化

R1 讓 resolve 從「longest-prefix match」退化為「找路徑上第一個帶 rule 的節點」——因為互斥保證路徑上至多一個。

```lua
-- router.lua (v2)
-- node = { seg = {}, sorted = {}, rule = { at=, tier=, rid= } or nil }
-- 由 R1 保證：任一根到葉的路徑上，至多一個節點帶 rule

local _M = {}

function _M.resolve(R, bucket, key, now)
    local b = R.buckets[bucket]
    if not b then return nil, "no_bucket_placement" end

    local node, from = b.root, 1
    while node do
        local r = node.rule
        if r then
            if r.at > now then return nil, "not_effective" end   -- staged，尚未生效
            if r.frozen     then return nil, "frozen_prefix"  end -- 刪除協定進行中
            return r.tier, r.rid
        end
        local p = string.find(key, "/", from, true)
        if not p then break end                  -- 只在 "/" 邊界匹配
        node = node.seg[string.sub(key, from, p - 1)]
        from = p + 1
    end
    return nil, "no_rule"                        -- R2：拒絕
end

return _M
```

與 v1 相比：

- 不再需要 `vers` 時間序列陣列（一條 rule 只有一個 tier，永遠）。
- 不再需要「記住最深的已生效命中」的回溯邏輯。
- 不再回傳 `read_fallback`。
- 多了三種明確的拒絕原因，全部 fail closed。

`single` mode 就是 `b.root.rule ~= nil` 的特例，同一段程式直接處理。

### 4.1 拒絕的處理：有界的同步 policy refresh

R2 帶來一個新的 UX 問題：使用者剛宣告 rule，policy 還在傳播，此時寫入會拿到 `403 NoPlacementRule`，看起來像「設定沒生效」的閃斷。

因為這個失效模式現在是 benign 的（拒絕，不是誤路由），可以安全地加一條自癒路徑：

```lua
-- route.lua 片段
local tier, reason = router_m.resolve(R, bucket, key, ngx.time())

if not tier and (reason == "no_rule" or reason == "no_bucket_placement") then
    -- 有界的同步刷新：每節點每秒最多觸發一次，避免 CP 被打爆
    if require("placement").try_refresh_now(1.0) then
        R = get_router()                                  -- 重取（可能已更新）
        tier, reason = router_m.resolve(R, bucket, key, ngx.time())
    end
end

if not tier then
    return s3_error(403, "NoPlacementRule",
        "no placement rule covers this key; declare a prefix rule first")
end
```

`try_refresh_now(min_interval)` 以 shared dict 做節點級 rate limit，命中則同步向 CP 拉一次 bundle（2 s timeout）。效果：宣告後的空窗從「poll 間隔」縮到「一次 CP round-trip」。

> 注意：這條路徑在 v1 是**絕對不能做**的——v1 若 policy 落後，正確行為是沿用舊 policy（自洽），主動刷新反而可能在 skew 窗口中造成誤路由。是 immutability 讓它變安全的。

---

## 5. Control Plane 的變更

### 5.1 Bundle 產生 gate（defense in depth）

不變量不能只在 API 層檢查。CP 在**產生每個 bundle 時**都要重新驗證整個 rule set，違反就拒絕發布：

```
assert_bundle_invariants(bundle):
    for each bucket:
        if mode == "single":
            assert 恰好一條 rule 且 prefix == ""
        else:
            assert 無 prefix == ""
            將所有 prefix 排序，檢查相鄰兩者不互為 prefix   # O(n log n)，即 R1
        assert 所有 prefix 以 "/" 結尾（或 mode==single 的 ""）
        assert 所有 tier ∈ clusters 且 writable
    若任一檢查失敗 → 不發布，保留上一版，觸發 P1 告警
```

這條 gate 的價值在於：即使 API 層有 bug、即使有人直接改資料庫，錯誤的 bundle 也到不了 data plane。

### 5.2 API 變更

```
POST /v1/buckets                      # 建立 bucket，必須指定 mode + 初始 rule 集
POST /v1/buckets/{b}/rules            # 新增互斥 rule（partitioned/templated）
DELETE /v1/buckets/{b}/rules/{id}     # 走 freeze 協定；非空則拒絕
POST /v1/buckets/{b}/rules:validate   # 強制前置；回傳 confirm_token
GET  /v1/buckets/{b}/rules
```

**`PUT /v1/buckets/{b}/placement`（v1 的全量覆寫）刪除。** 全量覆寫的語意在 immutable 模型下無法定義。改為只有「新增單條」與「刪除單條」。

`POST rules` 的兩階段確認（因為決定不可逆，必須讓使用者看見後果）：

```jsonc
// 1) validate
POST /v1/buckets/fab-lot/rules:validate
{ "prefix": "raw/2026/wk32/", "tier": "hot" }

→ 200
{
  "ok": true,
  "immutable_warning": "此設定一旦有資料寫入即無法更改。要改變 tier，唯一途徑是刪除該 prefix 下所有資料後移除規則。",
  "conflicts": [],                          // R1 檢查結果
  "effective_at": "2026-08-11T09:15:10Z",
  "cost_estimate": { "tier": "hot", "usd_per_tib_month": 210 },
  "constraints": { "min_object_size": 0 },
  "confirm_token": "ct_9f2c8a…",            // 5 分鐘有效，綁定確切的 prefix+tier
  "checklist": [
    "raw/2026/wk32/ 之下的所有資料將永久位於 hot tier",
    "此 prefix 不可再細分（R1：不可嵌套）",
    "你的團隊 hot tier 配額剩餘 1.6 TiB"
  ]
}

// 2) commit
POST /v1/buckets/fab-lot/rules
{ "prefix": "raw/2026/wk32/", "tier": "hot",
  "confirm_token": "ct_9f2c8a…", "ticket": "CAB-24993" }

→ 202 Accepted
{ "rule_id": "r-1042", "state": "staged",
  "effective_at": "2026-08-11T09:15:10Z", "retry_after_sec": 12 }
```

`confirm_token` 綁定 `(bucket, prefix, tier)` 的雜湊，防止「validate 一個、commit 另一個」。

**衝突訊息必須具體。** R1 被觸發時，最容易讓使用者困惑，所以錯誤訊息要直接給出解法：

```jsonc
{
  "ok": false,
  "conflicts": [{
    "kind": "nesting",
    "existing_rule": { "id": "r-0912", "prefix": "raw/", "tier": "cold",
                       "objects": 4128773, "sealed": true },
    "message": "prefix 'raw/2026/wk32/' 嵌套於既有規則 'raw/' 之內。不允許嵌套。",
    "remedies": [
      "改用不重疊的 prefix（例如 'hot/2026/wk32/'）",
      "若 raw/ 尚無資料，可先刪除 r-0912 再以更細粒度重新宣告",
      "若確需嵌套例外，請開 CAB 由平台團隊執行 freeze-verify-publish 流程"
    ]
  }]
}
```

### 5.3 Prefix template（`templated` mode）

R1 + R2 的組合對時間分區資料很不友善：使用者每週都要手動宣告一次新 prefix，忘記就寫入失敗。這是最需要工具補救的地方。

```jsonc
POST /v1/buckets/fab-lot/templates
{
  "pattern": "raw/{YYYY}/wk{WW}/",
  "tier_schedule": [                       // 依分區「年齡」決定 tier，於宣告時一次定案
    { "age_weeks": 0,  "tier": "hot"  },   // 當週與未來 4 週 → hot
    { "age_weeks": 4,  "tier": "warm" },   // 4–26 週 → warm
    { "age_weeks": 26, "tier": "cold" }    // 26 週以上 → cold
  ],
  "provision_ahead_weeks": 6,
  "gc_empty_after_weeks": 8                // 過期且為空的 rule 自動刪除（走 freeze 協定）
}
```

關鍵語意：`tier_schedule` 是在**該分區被宣告的那一刻**求值一次，之後那條 rule 就固定了。它**不是** lifecycle transition，資料不會隨時間搬動。這個設計讓「不同年齡的資料落在不同 tier」在 immutable 模型下仍然可行 —— 因為每週的資料是**不同的 prefix**，落在不同 tier 完全不違反不變量 I。

這其實是 immutable placement 最漂亮的用法：**把「時間」編進 key，讓 tiering 由 namespace 的結構自然表達，而不是由狀態變更表達。**

Provisioner 每天執行：
- 宣告未來 `provision_ahead_weeks` 週的 prefix（依 schedule 求 tier）。
- GC 掉過期且為空的 rule（避免 §7 的 LIST plan 過長）。
- 若某週的 rule 因配額不足無法宣告 → 提前 6 週告警，而不是在使用者寫入時才失敗。

### 5.4 Rule 狀態機（大幅簡化）

```
   ┌─────────┐  validate + confirm_token
   │  DRAFT  │
   └────┬────┘
        │ R1/R2/配額/容量 檢查通過
        ▼
   ┌─────────┐  effective_at 排程（Δ 可縮短至 10s，見 §6）
   │ STAGED  │  ← 此時 resolve 回 not_effective（拒絕，非誤路由）
   └────┬────┘
        │ 全節點 ack + 時鐘到達
        ▼
   ┌─────────┐
   │ ACTIVE  │  ── 首次寫入 ──▶ sealed = true（終態，不可逆）
   └────┬────┘
        │ 僅當 sealed == false，且走 freeze 協定
        ▼
   ┌─────────┐        ┌──────────┐
   │ FROZEN  │──驗空──▶│ REMOVED  │
   └────┬────┘  非空   └──────────┘
        │  └──────▶ 回 ACTIVE + 回報錯誤
```

`sealed` 由 SAL 在首次成功寫入時回報給 CP（或由 catalog 的第一筆 event 推導）。這只是**加速判斷**用的 hint——freeze 協定的驗空步驟才是權威。即使 `sealed` flag 遺失，freeze 驗空仍會攔住。

### 5.5 Freeze 協定（唯一需要協調的流程）

刪除 rule（以及 S2 的嵌套插入）必須用它：

```
1. CP 發布 rule.frozen = true，帶 effective_at = now + Δ
   → 生效後所有節點對該 prefix 的寫入回 403 FrozenPrefix；讀取不受影響
2. 等所有節點 ack 該 version 且時鐘跨過 effective_at
3. 驗空：對 owning tier 執行 ListObjectVersions(prefix, max-keys=1)
   （必須用 ListObjectVersions，含 noncurrent version 與 delete marker）
4a. 空  → 發布移除該 rule
4b. 非空 → 解除 frozen，回報 409 PrefixNotEmpty + 實際物件數
```

步驟 1–2 正是防住 §2.4 那個競態的關鍵：**在驗空之前先讓所有節點停止寫入。** 沒有這個步驟，任何「驗空後再改」的流程都是不安全的。

---

## 6. `effective_at` 的角色降級（但不刪除）

v1 裡兩階段生效是**正確性機制**（防誤路由）。v2 裡誤路由已結構性消除，所以它的理由變了：

| 用途 | v1 | v2 |
|---|---|---|
| 防誤路由 | ★ 核心 | 不再需要 |
| Freeze 協定的協調基礎 | 次要 | ★ **核心**（無此則驗空不安全） |
| UX 確定性（給使用者一個明確的「可以開始寫了」時刻） | 次要 | ★ 主要 |
| CAB 稽核證據 | 次要 | ★ 主要 |

因此：

- **Δ 從 30 s 縮短到 10 s**。新增 rule 的失效模式是 benign（403），且有 §4.1 的同步刷新自癒，不需要大的安全邊界。
- **Freeze 的 Δ 維持 30 s**，因為它承擔真正的協調責任，且刪除操作不急。
- **時鐘偏移的要求放寬**：偏移 > 5 s 仍告警，但不再需要暫停 activation（後果從資料事故降為短暫 403）。

---

## 7. LIST 的簡化

R1 讓 v1 §8.3 的「區間切分 + gap 區間 + 多讀丟棄」整套邏輯大幅退化。互斥 prefix 意味著 keyspace 被切成**完整子樹**，沒有 gap。

三種情形：

```
輸入：bucket b, 查詢 prefix P

情形 1  存在 rule Q 使 Q.prefix 是 P 的 prefix（含相等）
        → 該 P 之下所有 key 都屬 Q.tier
        → 單一後端呼叫，delimiter/marker/max-keys 全部原樣下推
        → SAL 幾乎不做事；OpenResty 甚至可直接透傳
        ★ 這是壓倒性多數的情形（應用程式列舉自己宣告的區域）

情形 2  P 之下有 m 條 rule（P 位於宣告層之上）
        → plan = 依字典序的 [Q1, Q2, …, Qm]，每個以 prefix=Qi.prefix 查詢
        → 完整子樹，零多讀；依序串接即為全域有序
        → continuation token = { v, i, k }   (i = plan 索引, k = 最後回傳的 key)

情形 3  以上皆非
        → 該 P 之下不可能有 key（R2）→ 回空結果
```

與 v1 相比消失的複雜度：

| v1 的問題 | v2 |
|---|---|
| Gap 區間需用 `start-after` + 上界判定，會多讀並丟棄 | **消失**。所有 plan entry 都是完整子樹 |
| `X⊤` 上界的字串構造 | **消失**，不需要上界 |
| Nested rule 造成的區間交錯 | **消失**（R1） |
| 區間數可能遠大於 rule 數 | 區間數 = 涉及的 rule 數 |

仍需處理的兩件事：

1. **`CommonPrefixes` 跨 plan entry 去重**（情形 2）。例：`P=""`、`D="/"`、rule `raw/2026/` 與 `raw/2027/` → 兩者都產生 `raw/`。因 plan 依序處理，只需在 token 記住「上次回傳的最後一個 CommonPrefix」，跳過 `≤` 它的即可。
2. **空子樹跳躍的成本**（`templated` mode 的典型情形）。若 bucket 有 300 條 rule 而多數子樹為空，單頁可能要探測數十次後端。處置：
   - 單頁最多探測 `LIST_PROBE_CAP = 16` 個 plan entry，達上限即回傳（可以 `IsTruncated: true` 但 keys 少於 `max-keys`，S3 規格允許）。
   - Template provisioner 的 `gc_empty_after_weeks` 主動刪除過期空 rule，控制 plan 長度。
   - 深層或跨多 rule 的列舉改由 catalog 服務（catalog 仍是解 5 B objects LIST 退化的主力，只是不再承擔跨 tier 正確性）。

### 7.1 副作用：SAL 的協調範圍縮小

情形 1 的 LIST 可以完全在 OpenResty 判定並直接透傳。加上單物件操作也不再需要 fallback 邏輯，v2 下 SAL 的必要職責只剩：

| 仍需 SAL | 原因 |
|---|---|
| 情形 2 的 LIST | 需 plan 掃描與 token 編碼 |
| MPU | uploadId → tier 映射 |
| Bucket 層級扇出 | 多 cluster 一致性 |
| 跨 tier `CopyObject` | read-then-write |
| `DeleteObjects` batch | 按 tier 分桶 |

也就是說 **v1 §16 Phase 3 的「OpenResty 對 direct 模式直連 HAProxy」在 v2 可以提前到 Phase 1 評估** —— 省一跳（約 0.3–1 ms）與一份連線資源。唯一的代價是 catalog event 必須改由 `log_by_lua` + 本地 spool 產生（至少一次語意）。建議仍先讓 SAL 在路徑上跑完 Phase 1，把 bypass 留作已驗證的最佳化選項。

---

## 8. 新的使用者負擔與緩解

Immutability 的代價全部落在使用者身上，必須主動補償。

### 8.1 建議預設慣例：tier-in-prefix

對絕大多數使用者，最好的 placement 設計是**讓 tier 出現在 key 的第一段**：

```
bucket: fab-lot   mode: partitioned
  hot/    → hot
  warm/   → warm
  cold/   → cold
```

好處：

- Rule set 永遠只有 3 條，永遠不需要新增，永遠不會撞 R1。
- 應用程式「選 tier」= 選擇寫進哪個目錄，語意直觀，程式碼裡看得見。
- 搬移資料 = 應用層的 `CopyObject` + `Delete`（SAL 支援跨 tier copy），使用者自己就能做，不需要平台團隊介入、不需要 migration API。**這一點很重要：它把「不可變」的痛點還給使用者自己解，而且解法很簡單。**

這幾乎把 immutable placement 的所有缺點都繞掉了。**應該把它做成 UI 的預設模板**，只有明確需要別種 layout 的人才自訂。

### 8.2 時間分區：用 `templated` mode（§5.3）

### 8.3 誤設的三層防護

因為誤設不可補救，防護必須前置：

| 層 | 機制 |
|---|---|
| 1. 強制 validate | `POST rules` 沒有 `confirm_token` 直接 400。Token 綁定確切的 `(prefix, tier)` |
| 2. 顯式後果清單 | `checklist[]` + `immutable_warning` 必須在 UI 上以不可摺疊的區塊呈現，含成本估算與配額餘額 |
| 3. 高影響審批 | 目標為 hot、或 prefix 位於第一層（`≤ 1` 段，影響面大）、或團隊 hot 配額使用率 > 80% → 需第二人簽核 + CAB ticket |

再加上第 0 層：**`single` mode 的 bucket 建立成本極低**。鼓勵「多開 bucket、每個 bucket 單一 tier」而不是「一個 bucket 內切很多 prefix」。前者幾乎不可能誤設，後者才需要規劃。

### 8.4 Break-glass

真的誤設了、資料又不能刪，唯一出路：

```
POST /internal/v1/migrations        (platform-team only, requires CAB ticket)
  → 復用 v1 §10.3 的 mover
  → 流程：freeze 舊 prefix（停寫）→ 全量複製到新 tier → 驗證
          → 由 CP 以「特權操作」原子替換該 rule 的 tier
          → 解除 freeze → 刪除來源副本
  → 期間該 prefix 唯讀。必須在維護窗口內執行。
```

關鍵：**這條路徑不可自助化。** 一旦開放，使用者就不會認真做前期規劃，整個結構性保證的價值就流失了。Mover 的程式碼要保留（它同時是 §5.5 驗空與 reconciler 的基礎設施），但入口必須是平台團隊。

---

## 9. 不變量監控

v2 把正確性從「對帳維持」改成「結構保證」，因此監控的性質也變了：**從「偵測並修復」變成「斷言恆為真」**。以下指標**必須恆為 0**，非 0 即代表實作有 bug：

```
sal_invariant_nested_rules              gauge   CP 每次產 bundle 自檢；> 0 → 拒發布 + P1
sal_invariant_duplicate_objects         gauge   同 (bucket,key,version) 出現在多個 tier；P1
sal_invariant_orphan_objects            gauge   物件所在 tier ≠ 其 rule 的 tier；P1
sal_invariant_unruled_objects           gauge   存在但不屬任何 rule 子樹的物件；P1
sal_policy_skew_total                   counter OpenResty 與 SAL 決策不一致；P1
```

預期非 0、需觀察趨勢的指標：

```
sal_reject_no_rule_total{bucket}        counter  R2 拒絕。突增 → 使用者忘記宣告，或 template 沒跟上
sal_reject_not_effective_total{bucket}  counter  staged 窗口內的寫入。應在 §4.1 自癒後趨近 0
sal_reject_frozen_total{bucket}         counter  freeze 協定進行中
sal_rule_sealed_total{tier}             counter  封印事件（首次寫入）
sal_rule_seal_ratio{bucket}             gauge    已封印 / 總 rule
sal_template_provision_lag_weeks        gauge    provisioner 落後程度；< 2 週即告警
sal_list_plan_length                    histogram 情形 2 的 plan 長度；p99 > 32 → rule 過碎
sal_list_probe_capped_total             counter  單頁探測達上限的次數
```

`sal_reject_no_rule_total` 特別值得做成使用者可見的儀表板 —— 它直接告訴使用者「你的應用正在往未宣告的區域寫」。

---

## 10. 既有資料如何併入

如果現況已有一個裝著約 5 B objects 的 MinIO cluster，要納入這個模型：

1. 把現有 cluster 註冊為一個 tier（例如 `legacy`，`writable: true`）。
2. 對每個既有 bucket，以 `single` mode 宣告 `prefix="" → legacy`。因為現有資料**全部**在該 cluster，這個宣告與不變量 I 完全相容（`resolve` 對所有既存 key 都回 `legacy`，正是它們的實際位置）。
3. 新的分層需求走**新 bucket**（`partitioned` 或 `templated`，指向 hot/warm/cold）。
4. 既有 bucket 想分層 → 由應用端逐步把資料 copy 到新 bucket 的對應 prefix，切換讀取端點後刪除舊資料。這是應用層的資料生命週期工作，不是平台的遷移工作。

好處：切換當天零資料搬動、零風險，且 `legacy` tier 可以隨著資料被搬走而逐步縮容。

---

## 11. 更新後的風險表

| # | 風險 | 嚴重度 | 緩解 |
|---|---|---|---|
| 1 | **誤設不可補救** | **高** | §8.3 三層防護 + `single` mode 為預設 + tier-in-prefix 模板 + §8.4 break-glass |
| 2 | 只實作成「rule append-only」而漏掉 R1 → 不變量仍破（§1 的陷阱） | **高** | §5.1 的 bundle gate（O(n log n) 檢查）+ 針對嵌套插入的專門測試案例 |
| 3 | R2 造成「宣告前寫入即失敗」的 UX 摩擦 | 中 | §4.1 同步刷新 + §5.3 template provisioner + `sal_reject_no_rule_total` 儀表板 |
| 4 | R1 失去「大範圍 + 小範圍例外」表達力 | 中 | tier-in-prefix 慣例（§8.1）；`templated` mode；S2 作為 CAB 例外路徑 |
| 5 | 使用者為避開限制而濫建 bucket | 低 | 這其實是**期望行為**。需確保 bucket 建立成本低（§8.5 的扇出）與 bucket 數量上限充足 |
| 6 | `templated` mode 的空子樹拖慢跨 rule LIST | 低 | `LIST_PROBE_CAP` + `gc_empty_after_weeks` + catalog 服務 LIST |
| 7 | Freeze 協定期間該 prefix 唯讀 | 低 | 僅用於刪除空 rule 與 break-glass，正常運作不觸發 |
| 8 | Break-glass 被過度使用，架構保證退化 | 中 | 入口限平台團隊 + 必須 CAB + 每季檢視使用次數（趨勢上升即代表前期規劃工具不足，應改工具而非放寬入口） |
| 9 | Cold tier 小檔（v1 §12.3 不變） | 高 | `min_object_size` 在 validate 階段就告知；immutability 讓這個限制更需要前期溝通 |
| 10 | MinIO CE 授權/版本策略（v1 §17 #10 不變） | 高（非技術） | 後端可替換性不受本修訂影響，反而因 catalog 離開讀取路徑而更乾淨 |

---

## 12. ADR-4：Placement 一旦宣告即不可變

**Status:** Proposed
**Deciders:** 儲存平台架構 + 主要使用者團隊（yield-eng / EDA）+ CAB

### Context

v1 允許事後改變 `(bucket, prefix) → tier`，代價是必須引入 rule state machine、`read_fallback`、mover、以及以 catalog 為讀取路徑權威的對帳體系。整個遷移子系統約佔設計複雜度的 40%，且最嚴重的失效模式（同 key 在兩個 cluster 各一份 → 靜默資料錯誤）只能靠對帳偵測，無法結構性排除。

### Decision

採納不變量 I（Placement Determinism），以 R1（不可嵌套）+ R2（先宣告後寫入）+ R3（首次寫入即封印）三條規則結構性保證之。刪除遷移子系統，Mover 降為平台團隊的 break-glass 工具。

### Options Considered

| | **A. 完全可變（v1）** | **B. Append-only rules（天真版）** | **C. Immutable placement（本案）** |
|---|---|---|---|
| 複雜度 | 高 | 中 | **低** |
| 正確性 | 靠對帳 | **仍破**（§1 嵌套陷阱） | 可證明 |
| 使用者彈性 | 高 | 高 | 中（工具可補） |
| Catalog 依賴 | 讀取路徑權威 | 同 A | 僅加速層 |
| 誤設可救 | 可 | 可 | 僅資料為空時 |
| Skew 後果 | 資料事故 | 資料事故 | 短暫 403 |

**選項 B 必須明確排除。** 它是最容易被誤選的方案——「不能改，只能新增」聽起來就是題目要求，但它不成立。

### Trade-off Analysis

核心取捨是**把彈性成本從平台移轉到使用者**。這在本場景是對的，因為：

1. 分層決策本質上是使用者對自己資料的知識（哪批 lot 要跑 SPC 分析），平台無法代為判斷，所以「使用者要想清楚」本來就是必要的。
2. 遷移子系統的複雜度由平台承擔、風險由全體使用者承擔（一次 reconciler bug 影響所有人）；而規劃成本由各使用者自己承擔、風險局限於自己的 bucket。**後者的失效半徑小得多。**
3 . 使用者側的成本可以用工具幾乎完全消除：tier-in-prefix 模板（3 條規則、永不變更）加上 `templated` mode（自動預宣告），已覆蓋預期的絕大多數場景。

反面意見（應記錄）：若日後出現「大量既有資料需要重新分層」的實際需求（例如 fab 產能結構改變、或分析工作負載模式大幅轉移），本決策會使該需求變成一系列平台團隊代操作的 break-glass 任務，成本高於 v1。屆時應**重新評估**，而不是逐步放寬 break-glass 的入口。

### Consequences

**變容易**
- 讀取路徑：單跳、無 fallback、不碰 catalog
- LIST：完整子樹串接，無 gap 區間、無多讀
- 監控：斷言式（指標恆為 0），而非對帳式
- 開發：Phase 2 的遷移能力整個刪除，時程縮短約 6–8 週
- 後端替換：catalog 不在正確性路徑上，換 tier 的後端實作更乾淨

**變困難**
- 使用者必須前期規劃 namespace（需投入文件與模板）
- 「大範圍 cold + 小範圍 hot 例外」需改用不同 layout 表達
- 誤設的補救成本從「開個 migration」變成「平台團隊 + CAB + 維護窗口」

**需要重新檢視的時機**
- `sal_reject_no_rule_total` 長期居高 → R2 對使用者太苛，或 template 工具不足
- Break-glass 使用頻率上升 → 前期規劃工具不足（**修工具，不放寬入口**）
- 出現大規模重新分層的真實需求 → 重新評估 ADR-4

### Action Items

1. [ ] 在 CP 的 bundle 產生流程實作 `assert_bundle_invariants()`，含 R1 的 O(n log n) 檢查
2. [ ] 針對「嵌套插入」寫專門的 negative test（這是最容易漏的路徑）
3. [ ] 刪除 `read_fallback`、`X-Sal-Fallback`、mover 的公開入口、`default_tier` 隱含 fallback
4. [ ] 實作 `single` / `partitioned` / `templated` 三種 bucket mode 與模式不可轉換的檢查
5. [ ] 實作 freeze 協定（rule 刪除與 break-glass 共用）
6. [ ] 實作 `validate` → `confirm_token` → `commit` 兩階段確認
7. [ ] 實作 `try_refresh_now()` 有界同步刷新
8. [ ] 簡化 `router.lua` 為 §4 的版本；簡化 LIST 為 §7 的三情形
9. [ ] 建立 tier-in-prefix 的 UI 預設模板與使用者文件
10. [ ] 建立五個 `sal_invariant_*` 指標與 P1 告警規則
11. [ ] 制定既有 cluster 併入為 `legacy` tier 的切換計畫（§10）

---

## 附錄 — 使用者文件要改寫的三句話

v1 附錄 B 的第 1 句必須整段換掉：

> **1. Placement 是一次性、不可撤回的決定。**
> 你為某個 prefix 選定 tier 之後，只要有任何資料寫入，這個決定就永久固定。想改變 tier 的唯一辦法是：把該 prefix 下的資料全部刪除或複製到別處，移除規則，重新宣告。**請在寫入第一個物件之前想清楚。**
>
> **1a. 不確定就用「一個 bucket 一個 tier」。** 這是預設、也是最不會出錯的做法。需要混放時，建議把 tier 放在 key 的第一段（`hot/`、`warm/`、`cold/`），這樣三條規則就永遠夠用，而且要搬資料時你自己用 `aws s3 cp` 就能做。
>
> **1b. Prefix 規則不能互相包含。** 已經有 `raw/` 之後，就不能再宣告 `raw/2026/`。時間分區請用 template 功能（我們會幫你自動預先宣告未來的分區）。
