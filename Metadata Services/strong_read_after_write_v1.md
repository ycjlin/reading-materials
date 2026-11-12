要在百億級 S3 物件規模下，使用 Redis 作為熱層（Hot Tier）、ClickHouse 作為冷層（Cold Tier） 來實現 Read-After-Write Strong Consistency（寫後讀強一致性），關鍵在於解決 Redis 的按目錄前綴檢索（Listing）、持久化防丟失以及冷熱數據平滑合併。
以下為您設計完整的系統架構圖與落地實作細節。

系統架構圖（Hot-Cold Hybrid Architecture）

                                  ┌─────────────────────────┐
                                  │     Client / App        │
                                  └────────────┬────────────┘
                                               │ 1. Put / Delete / List
                                               ▼
┌────────────────────────────────────────────────────────────────────────────────────────┐
│                            Storage Gateway (Metadata Service)                          │
│                                                                                        │
│  ┌───────────────────────────────────────┐  ┌───────────────────────────────────────┐  │
│  │          Write Coordinator            │  │           Read Merger                 │  │
│  │ 1. Write S3 -> Get ETag / Version     │  │ 1. Parallel Query: Redis + ClickHouse │  │
│  │ 2. Sync Write Redis (ZSET + HASH)     │  │ 2. Merge Sort by object_name          │  │
│  │ 3. Async Push to Kafka                │  │ 3. Override / Filter Deleted Objects  │  │
│  └───────────┬───────────────┬───────────┘  └───────────┬───────────────┬───────────┘  │
└──────────────┼───────────────┼──────────────────────────┼───────────────┼──────────────┘
               │               │                          │               │
      (Sync)   │               │ (Async)          (Query) │               │ (Query)
               ▼               ▼                          ▼               ▼
┌──────────────────────┐ ┌───────────┐        ┌──────────────────────┐ ┌────────────────┐
│    AWS S3 / MinIO    │ │   Kafka   │        │   Redis Cluster      │ │   ClickHouse   │
│  (Object Storage)    │ └─────┬─────┘        │   (Hot Tier: 24h)    │ │  (Cold Tier)   │
└──────────────────────┘       │              │                      │ │                │
                               │              │ • ZSET (Dir Index)   │ │ • Replacing    │
                               │              │ • HASH (Obj Meta)    │ │   MergeTree    │
                               ▼              └──────────────────────┘ └───────▲────────┘
                     ┌───────────────────┐                                     │
                     │  Batch Consumer   │─────────────────────────────────────┘
                     │  (Flink / Vector) │      Batch Insert (Every 2s / 50k rows)
                     └───────────────────┘

核心模組與數據結構設計


1. Redis 熱層數據結構（支援高效目錄 Listing）

Redis 原生不支援 SQL WHERE parent_dir = ... 範圍查詢，因此必須採用 「目錄索引（ZSET） + 物件詳情（HASH）」 的雙重結構，並利用 Redis Cluster 的 Hash Tag {bucket:parent_dir} 確保同一目錄的數據落在同一個分片（Slot）上，避免跨節點查詢。
• 目錄排序索引（Sorted Set）：用於按字典序（Lexicographical Order）分頁列出檔案。
	• Key: dir:{bucket:parent_dir}
	• Score: 固定設為 0（Score 相同時，Redis ZSET 會嚴格按照 Member 的字典序排列）。
	• Member: {object_name}（例如 report-01.pdf）。
	• 查詢指令: 使用 ZRANGEBYLEX dir:{bucket:parent_dir} [last_seen_name + LIMIT 0 1000 實現毫秒級游標分頁。
• 物件元數據詳情（Hash）：儲存狀態（包含刪除標記 Tombstone）與版本號。
	• Key: meta:{bucket:parent_dir}:{object_name}
	• Fields:
		• status: ACTIVE 或 DELETED（刪除時不直接刪 Key，而是寫入 DELETED 標記，確保能覆蓋 ClickHouse 裡的舊紀錄）
		• size: 檔案大小
		• etag: S3 ETag
		• version: 全局單調遞增版本號（如 Snowflake ID 或 S3 VersionId）
		• updated_at: 時間戳

2. 寫入與刪除流程（Write Path）

為保證寫後讀強一致，網關（Storage Gateway）必須遵循嚴格的順序執行：
1. 操作底層儲存：呼叫 S3 PutObject 或 DeleteObject，取得 S3 回傳的成功狀態與 ETag。
2. 同步寫入 Redis（使用 Lua 腳本保證原子性）：
	• 比較傳入的 version 與 Redis 中現有的 version（防止並發亂序覆蓋，實現樂觀鎖）。
	• 若版本較新，更新 meta:{bucket:parent_dir}:{object_name}，並將 {object_name} 加入 dir:{bucket:parent_dir}（即使狀態為 DELETED 也要保留在 ZSET 中，直到 ClickHouse 同步完成）。
	• 設定 TTL（例如 24 小時）。
3. 非同步發送至 Kafka：將變更事件推入 Kafka，隨後立即向客戶端回傳 200 OK。
4. 批次落盤至 ClickHouse：Flink 或 Vector 消費 Kafka，每 2 秒或累積 5 萬筆批次寫入 ClickHouse 的 ReplacingMergeTree 表。

3. 讀取合併流程（Read Path - Listing）

當客戶端請求列出 bucket/a/b/ 下的前 1000 個物件時：
1. 並行查詢雙層：
	• 向 Redis 查詢：透過 ZRANGEBYLEX 取得該目錄下字典序大於 marker 的前 1000 + N 筆熱數據，並用 Pipeline 批次讀取對應的 meta Hash（N 為預留給已刪除檔案的緩衝數量）。
	• 向 ClickHouse 查詢：執行 SELECT object_name, size, etag, is_deleted, version FROM s3_metadata FINAL WHERE bucket = 'bucket' AND parent_dir = 'a/b/' AND object_name > 'marker' ORDER BY object_name LIMIT 1000。
2. 記憶體歸併排序（In-Memory Merge Sort）：
	• 由於兩邊回傳的列表都已按 object_name 嚴格排序，網關層只需執行一次 O(N) 的雙指標歸併（Two-Pointer Merge）。
	• 衝突解決規則：若兩邊出現相同的 object_name，無條件以 Redis 的紀錄為準（因為 Redis 必定包含最近 24 小時內的最新狀態）。
	• 過濾刪除標記（Tombstone Filtering）：若最終勝出的紀錄其狀態為 DELETED（或 is_deleted = 1），則將其從回傳列表中剔除，並補足分頁筆數後回傳給客戶端。

關鍵運維與容錯機制

• Redis 持久化與高可用：因為 Redis 承載了尚未落盤至 ClickHouse 的強一致狀態，必須開啟 AOF（Append-Only File）且設為 appendfsync everysec 或 always，並配置 Redis Cluster 主從自動故障轉移（Failover）。
• 安全淘汰機制（Safe Eviction）：Redis 的 TTL（如 24 小時）必須遠大於 Kafka 到 ClickHouse 的最大同步延遲（通常僅數秒）。同時建議部署一個背景巡檢腳本，確認某個時間點之前的 Kafka Offset 均已成功寫入 ClickHouse 後，才允許淘汰對應的 Redis 數據。
• 大目錄防護（Hot Key / Big Key）：若單一目錄在 24 小時內新增超過數十萬個檔案，單一 ZSET 會變大。建議在業務層規範目錄結構（例如按小時分目錄 logs/2026/11/12/00/），將單一 ZSET 的成員數控制在 10 萬以內。
這套架構已經涵蓋了百億級規模下的強一致性與高效能需求。接下來您希望深入探討哪一個環節的實作細節：
Redis 原子寫入與版本比對的 Lua 腳本範例？網關層雙指標歸併（Merge Sort）與動態補齊分頁的程式碼邏輯？
Ready to create a plan?
Use Canvas to save, edit, and share your planning progress
Get started
