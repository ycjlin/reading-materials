# MinIO Metadata & Access Analytics Service 架構設計（ClickHouse-only，5B Objects）

Oct 17, 2026 · @Jason Lin

## 1. 背景與目標

結論：以 Kafka + ClickHouse 建立單一的 Metadata & Access Analytics Service。同一套 ClickHouse 同時承擔兩件事：S3 listing 加速（單頁 p99 數百 ms 可接受），以及 object 層級的 access pattern 分析，用來驅動 hot ↔ warm / cold 的 tiering 決策。Tiering 分析是核心目標，listing 是共用同一份 metadata 的附加能力。

**現況問題**

- MinIO 沒有集中式 metadata DB，`ListObjects` 需要對 erasure set 內所有 drive 做 directory walk 再 merge sort。成本與「prefix 下的 object 數 × drive 數」成正比，LOSF 使冷 prefix 的 listing 常達秒級。
- 沒有任何地方記錄「哪個 object 最後一次被讀是什麼時候」，tiering 只能依 object 年齡或人工判斷。這會把仍在使用的資料搬到 cold tier，或讓長期無人讀取的資料佔用 hot tier。
- 多個 MinIO cluster 之間沒有統一視圖，無法回答「全公司 hot tier 上有多少 bytes 已 90 天未讀」。

**目標**

| 目標 | 衡量方式 | 目標值 |
| --- | --- | --- |
| Object 層級 access profile | 每個 object 的 last\_access、7 / 30 / 90 天讀取次數與 bytes | 涵蓋 100% object，分析延遲 < 15 min |
| Tiering 決策自動化 | 每日產出 demote / promote 候選清單，交給 SAL Static Tiering 執行 | Mis-tier 率（搬到 cold 後 30 天內被讀）< 2% |
| 冷大 prefix listing 加速 | ListObjectsV2 單頁 latency，與 MinIO baseline 比較 | ≥ 50x；單頁 p99 < 500 ms |
| 全量 enumeration | 1 億 keys 的 prefix 匯出時間 | ≥ 50x，分鐘級 |
| 容量報表 | 任意 bucket / prefix 的 object 數、bytes、tier 分佈 | 秒級查詢 |

**為什麼只用 ClickHouse**

- Object 層級 tiering 需要把 5B 筆 metadata 與數十億筆 access event 做 join 與聚合，這是 ClickHouse 的強項，其他候選（TiKV、Elasticsearch）都做不好。
- Listing 可以接受數百 ms，ClickHouse 以主鍵有序儲存 `(bucket, object_key)`，range read 可以滿足，因此不需要另外維護 TiKV 或 ES。
- 系統只剩 Kafka + ClickHouse 兩種狀態元件，維運面最小。

**非目標**：取代 MinIO 的 data path、提供 strong consistency 的 listing、支援高 QPS（> 1K page/s）的線上 listing 服務、POSIX namespace。

## 2. 需求與假設

系統以「寫入大量、分析為主、listing 為輔」設計。下表的讀寫量為假設值，Phase 0 需以 MinIO audit log 與 OpenResty access log 校正；其中 GET / HEAD peak 直接決定 ClickHouse 的儲存規模。

**Functional requirements**

1. **Metadata 同步**：所有 MinIO cluster 的 object 建立、覆寫、刪除、storage class 變更，以 eventual consistency 同步到 ClickHouse。
2. **Access 收集**：收集所有經過 gateway 的 GET / HEAD / PUT / DELETE，含 bucket、key、bytes、status、principal。
3. **Access profile**：每個 object 的 first / last access、7 / 30 / 90 天讀取次數與 bytes、目前 tier。
4. **Tiering 決策**：依 policy 每日產出 demote（hot → warm → cold）與 promote（cold / warm → hot）候選，支援 dry-run、審核、限流。
5. **執行閉環**：候選交給 SAL Static Tiering 轉成 rule 執行，執行結果回流 ClickHouse，用於衡量 mis-tier 率。
6. **S3-compatible listing**：`ListObjectsV2`（prefix、`delimiter=/`、start-after、continuation-token、max-keys），既有 client 只需換 endpoint。
7. **擴充查詢**：prefix 統計、tier 分佈、條件查詢（size、mtime、tag）、平行 enumeration。

**Non-functional requirements**

| 項目 | 假設 / 目標 |
| --- | --- |
| Object 總數 | 5B，年成長 30–50%，設計上限 20B |
| 平均 key 長度 | 約 120 bytes |
| Mutation 速率（PUT + DELETE） | 平均 20K ops/s，peak 100K ops/s |
| Read 速率（GET + HEAD） | 平均 50K ops/s，peak 200K ops/s（待確認） |
| Listing QPS | 平均 50、peak 500 page/s |
| Listing latency | 單頁 p50 < 150 ms、p99 < 500 ms |
| Metadata freshness | p99 < 60 s；reconciler 保證 24 h 內收斂 |
| Access analytics freshness | p99 < 15 min |
| Tiering 決策週期 | 每日一次，可手動觸發 |
| Raw access event 保留 | 30 天；每日聚合保留 400 天 |
| 可用性 | Listing 99.9%（可 fallback MinIO）；分析 99.5% |
| Durability | ClickHouse 非 source of truth，可由 MinIO + Kafka 重建；access 歷史無法重建，需 2 replica |

**限制與既有技術棧**

- 後端為多個 MinIO cluster，前方已有 OpenResty → HAProxy，access log 與 listing 路由都在這一層處理。
- Tiering 執行沿用 SAL Static Tiering（etcd config store、append-only immutable rules），本系統只產出決策，不直接搬資料。
- MinIO CE 已 archive，設計只依賴 bucket notification 與 S3 API，未來可換 fork 或其他 S3 後端。
- ClickHouse 以自建 on-prem 叢集為前提，使用 ClickHouse Keeper，不依賴 ClickHouse Cloud 功能。

## 3. 高階架構

系統只有 Kafka 與 ClickHouse 兩種狀態元件：MinIO 的 metadata 事件與 gateway 的 access log 都經 Kafka 進入 ClickHouse；Query Service 從 ClickHouse 回答 listing，Tiering Engine 從 ClickHouse 產生搬移決策交給 SAL 執行，執行結果再以事件回流，形成閉環。

&#91;embedded content: 架構 · 資料 I/O、listing、access 收集與 tiering 閉環\]

讀圖方式：上半部是三條入口（資料 I/O 直達 MinIO、listing 進 Query Service、access log 進 Vector）；中段 Kafka 匯流後進入 ClickHouse；下半部是兩條使用 ClickHouse 的路徑，左邊負責正確性（Reconciler、Bootstrap），右邊負責 tiering，最下方的回流線代表 SAL 觸發的 transition 會再以事件更新 metadata。

| 元件 | 職責 | 狀態 |
| --- | --- | --- |
| OpenResty → HAProxy | 路由 listing、輸出 JSON access log | 無 |
| Vector | 解析 access log、排除 internal principal、送 Kafka | Disk buffer |
| Kafka | Metadata 與 access 兩類事件的緩衝與重放 | 保留 3 天 |
| ClickHouse | Metadata 索引、access 聚合、tiering 特徵與決策紀錄 | 主要狀態 |
| Query Service | S3 listing shim、擴充查詢 API | 無 |
| Reconciler / Bootstrap | 初始全量、持續校正、空目錄清理 | 無 |
| Tiering Engine | 每日計算候選、套用安全限制、輸出 rule 與 manifest | 無（結果在 ClickHouse） |
| SAL Static Tiering | 以 immutable rule 執行實際搬移 | etcd |

## 4. ClickHouse 資料模型

資料分成兩個 database：`meta` 存「現在有哪些 object」（可更新，以 ReplacingMergeTree 處理 LWW），`access` 存「誰在何時讀了什麼」（append-only，以 MergeTree 加聚合表處理）。兩邊都以 `(cluster_id, bucket, object_key)` 為排序主軸，tiering 分析可以依主鍵順序 join。

| 表 | Engine | ORDER BY | 用途 | 保留 |
| --- | --- | --- | --- | --- |
| `meta.object_meta` | ReplacingMergeTree(ver, is\_deleted) | cluster\_id, bucket, object\_key | 主表；無 delimiter listing、tiering join | 永久（tombstone 定期清除） |
| `meta.object_by_parent` | ReplacingMergeTree(ver, is\_deleted) | cluster\_id, bucket, parent, object\_key | `delimiter=/` 的檔案部分 | 同上 |
| `meta.dir_children` | ReplacingMergeTree(ver, is\_deleted) | cluster\_id, bucket, parent, child\_prefix | `delimiter=/` 的 CommonPrefixes | 同上 |
| `access.events_raw` | MergeTree | cluster\_id, bucket, ts | 原始讀寫事件、稽核、重算 | 30 天 |
| `access.object_daily` | AggregatingMergeTree | cluster\_id, bucket, object\_key, day | Object 每日讀取次數與 bytes | 400 天 |
| `access.object_profile` | AggregatingMergeTree | cluster\_id, bucket, object\_key | Object 終身 first / last access | 隨 object 存活 |
| `access.prefix_daily` | AggregatingMergeTree | cluster\_id, bucket, prefix, day | Prefix 熱度報表 | 400 天 |
| `tiering.candidates` / `tiering.actions` | MergeTree | run\_id, cluster\_id, bucket, object\_key | 決策輸出與執行紀錄 | 400 天 |

**4.1 `meta.object_meta`（主表）**

```sql
CREATE TABLE meta.object_meta ON CLUSTER mds
(
    cluster_id     LowCardinality(String),
    bucket         LowCardinality(String),
    object_key     String,
    parent         String,                  -- 直屬目錄，'a/b/c.txt' → 'a/b/'
    size           UInt64,
    etag           String,
    mtime          DateTime64(3),
    version_id     String,
    storage_class  LowCardinality(String),  -- 對應 hot / warm / cold tier
    content_type   LowCardinality(String),
    user_tags      Map(String, String),
    ver            UInt64,                  -- 版本：事件時間 / mtime / 觀測時間（見 5.1）
    is_deleted     UInt8,
    ingest_ts      DateTime DEFAULT now()
)
ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/tables/{shard}/meta/object_meta', '{replica}', ver, is_deleted)
PARTITION BY (cluster_id, cityHash64(bucket) % 16)
ORDER BY (cluster_id, bucket, object_key)
SETTINGS index_granularity = 1024;
```

- `ver` 依來源取不同時間（規則見 5.1），同一個 key 只保留最大 `ver` 的列，亂序到達的舊事件自然被覆蓋，等同 LWW。
- `is_deleted = 1` 是 tombstone，`FINAL` 查詢會自動排除；清除策略見第 8 節。
- `index_granularity = 1024`（預設 8192）降低 listing 每頁讀取的列數，代價是 primary index 記憶體增為約 8 倍，5B 列約數 GB，可接受。
- Partition 的用途是讓 `FINAL` 可以依 partition 平行去重（`do_not_merge_across_partitions_select_final = 1`），並限制背景 merge 的範圍；bucket 的過濾主要依靠 ORDER BY 主鍵。

**4.2 Delimiter listing 用的兩張表**

```sql
-- 某目錄下的「檔案」：parent = 'a/b/' 的所有 object
CREATE TABLE meta.object_by_parent ON CLUSTER mds
(
    cluster_id LowCardinality(String), bucket LowCardinality(String),
    parent String, object_key String,
    size UInt64, etag String, mtime DateTime64(3),
    storage_class LowCardinality(String),
    ver UInt64, is_deleted UInt8
)
ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/tables/{shard}/meta/object_by_parent', '{replica}', ver, is_deleted)
PARTITION BY (cluster_id, cityHash64(bucket) % 16)
ORDER BY (cluster_id, bucket, parent, object_key)
SETTINGS index_granularity = 1024;

-- 某目錄下的「子目錄」：parent = 'a/b/' 時，child_prefix = 'a/b/c/'
CREATE TABLE meta.dir_children ON CLUSTER mds
(
    cluster_id LowCardinality(String), bucket LowCardinality(String),
    parent String, child_prefix String,
    ver UInt64, is_deleted UInt8
)
ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/tables/{shard}/meta/dir_children', '{replica}', ver, is_deleted)
ORDER BY (cluster_id, bucket, parent, child_prefix);
```

- 寫入 `object_meta` 的同一批事件，由 materialized view 同時寫入 `object_by_parent`；`dir_children` 則以 `arrayJoin` 展開該 key 的每一層祖先目錄，例如 `a/b/c/d.txt` 產生 `('', 'a/')`、`('a/', 'a/b/')`、`('a/b/', 'a/b/c/')` 三列。
- 目錄只在新增 object 時建立，刪除 object 不會即時移除目錄；空目錄由每日清理 job 標記 tombstone（第 5.5 節）。
- 只支援 `delimiter=/`；其他 delimiter 由 Query Service 直接 fallback 到 MinIO。

**4.3 Access 系列表**

```sql
CREATE TABLE access.events_raw ON CLUSTER mds
(
    ts          DateTime64(3),
    cluster_id  LowCardinality(String),
    bucket      LowCardinality(String),
    object_key  String,
    op          LowCardinality(String),   -- GET / HEAD / PUT / DELETE / LIST
    status      UInt16,
    bytes       UInt64,
    is_range    UInt8,
    principal   LowCardinality(String),
    client_ip   IPv6,
    request_id  String
)
ENGINE = ReplicatedMergeTree(
    '/clickhouse/tables/{shard}/access/events_raw', '{replica}')
PARTITION BY toYYYYMMDD(ts)
ORDER BY (cluster_id, bucket, ts)
TTL toDateTime(ts) + INTERVAL 30 DAY;

CREATE TABLE access.object_daily ON CLUSTER mds
(
    cluster_id  LowCardinality(String),
    bucket      LowCardinality(String),
    object_key  String,
    day         Date,
    reads       SimpleAggregateFunction(sum, UInt64),
    heads       SimpleAggregateFunction(sum, UInt64),
    bytes_read  SimpleAggregateFunction(sum, UInt64),
    last_ts     SimpleAggregateFunction(max, DateTime64(3))
)
ENGINE = ReplicatedAggregatingMergeTree(
    '/clickhouse/tables/{shard}/access/object_daily', '{replica}')
PARTITION BY toYYYYMM(day)
ORDER BY (cluster_id, bucket, object_key, day)
TTL day + INTERVAL 400 DAY;

CREATE TABLE access.object_profile ON CLUSTER mds
(
    cluster_id  LowCardinality(String),
    bucket      LowCardinality(String),
    object_key  String,
    first_ts    SimpleAggregateFunction(min, DateTime64(3)),
    last_ts     SimpleAggregateFunction(max, DateTime64(3)),
    total_reads SimpleAggregateFunction(sum, UInt64),
    total_bytes SimpleAggregateFunction(sum, UInt64)
)
ENGINE = ReplicatedAggregatingMergeTree(
    '/clickhouse/tables/{shard}/access/object_profile', '{replica}')
PARTITION BY (cluster_id, cityHash64(bucket) % 16)
ORDER BY (cluster_id, bucket, object_key);
```

- `events_raw` 以 `(cluster_id, bucket, ts)` 排序，因為 raw event 主要用於時間範圍的稽核與重算；object 層級查詢一律走聚合表。
- `object_daily` 與 `object_profile` 都由 materialized view 從 Kafka 即時累加，只計 `op IN ('GET','HEAD')` 且 `status < 400` 的事件。
- `prefix_daily` 以 `arrayJoin` 展開 depth 1–3 的 prefix，欄位為 reads、bytes\_read、`uniqCombinedState(object_key)`，供 dashboard 與 prefix 層級報表使用。

**4.4 Sharding**

- 所有表以 `cityHash64(cluster_id, bucket)` 作為 Distributed sharding key，同一 bucket 的 metadata 與 access 落在同一個 shard，listing 與 tiering join 都在 shard 內完成，不需跨 shard shuffle。
- 超大 bucket（> 1B objects）會造成 shard 傾斜：改以 `cityHash64(cluster_id, bucket, top_level_prefix)` 分散，Query Service 依 prefix 定位 shard；跨 top-level prefix 的 listing 由 Query Service 做 k-way merge。

## 5. 同步管線

Metadata 與 access 各走一條 Kafka topic，ClickHouse 以 Kafka table engine + materialized view 直接消費，不另寫 indexer 服務。正確性由三層保證：即時 stream 負責 freshness，bootstrap 負責初始全量，reconciler 負責最終收斂；三者都以同一套 `ver` 規則寫入，可以重疊、重放而不出錯。

**5.1 Metadata event**

1. 每個 MinIO cluster 設定 Kafka notification target，訂閱 `s3:ObjectCreated:*`、`s3:ObjectRemoved:*`、`s3:ObjectTransition:*`，並啟用 `queue_dir`，讓 Kafka 不可用時事件暫存在 MinIO 節點本機。Transition 事件在目前使用的 MinIO 版本上是否完整發出，需在 Phase 0 實測。
2. Topic `meta.events.<cluster>`，partition key = hash(bucket, object\_key)。
3. ClickHouse 端：`meta.kafka_events`（Kafka engine）→ MV 正規化 → `meta.object_meta`；`object_meta` 上再掛兩個 MV，寫入 `object_by_parent` 與 `dir_children`。所有寫入（stream、bootstrap、repair）都只進 `object_meta` 這一個入口，衍生表自動跟上。
4. 事件對應規則：

| 事件 | 寫入內容 | `ver` |
| --- | --- | --- |
| `ObjectCreated:*`（Put、Copy、CompleteMultipartUpload） | 完整 metadata，`is_deleted = 0` | 事件時間（`eventTime`，轉 ns） |
| `ObjectTransition:Complete` | 同 key 更新 `storage_class` | 事件時間 |
| `ObjectRemoved:Delete`、`DeleteMarkerCreated` | `is_deleted = 1` | 事件時間 |
| Bootstrap snapshot | 完整 metadata | object 的 `mtime` |
| Reconciler repair（經 HEAD 確認） | 完整 metadata 或 tombstone | HEAD 的觀測時間 |

用事件時間而非到達時間當 `ver`，亂序到達的事件會被正確排序。Bootstrap 用 `mtime` 是刻意偏小：listing 期間若物件剛被刪除，較大的 delete `ver` 會勝出，不會讓已刪物件復活。Repair 用 HEAD 時間，因為 HEAD 是強一致讀，能確定物件在該時間點的狀態。

5. Kafka engine 設定：`kafka_max_block_size = 1048576`、`stream_flush_interval_ms = 7500`，讓每次 flush 是大批次，避免產生過多小 part；`kafka_handle_error_mode = 'stream'`，解析失敗的訊息寫入 `meta.kafka_errors` 表，不阻塞 consumer。

**5.2 Access event**

1. OpenResty 以 JSON 格式寫 access log（含 bucket、key、op、status、bytes、range、principal、request\_id），由 Vector 讀取後送入 Kafka topic `access.events.<site>`。不在 `log_by_lua` 直接送 Kafka，以免 Kafka 抖動影響 request path；Vector 的 disk buffer 可承受 Kafka 短暫中斷。
2. Vector 負責解析 path-style / virtual-host style URL，取出 bucket 與 key，並過濾 `LIST` 以外的管理類請求。
3. **內部流量必須排除**：Reconciler 的 HEAD、tiering 執行、replication、備份等系統 principal 標記為 internal，不計入 `object_daily` / `object_profile`，否則 reconciler 的定期 HEAD 會讓所有 object 看起來都是熱的。Raw event 仍保留，供稽核使用。
4. ClickHouse 端：`access.kafka_events` → MV → `events_raw`、`object_daily`、`object_profile`、`prefix_daily`。
5. 交付語意為 at-least-once，極少量的重複只會讓讀取次數略為偏高，對 tiering 判斷影響可忽略（偏差方向是「較晚 demote」，屬於安全的一側）。

**5.3 Bootstrap（metadata 初始全量）**

1. 先開啟 notification 與 Kafka engine 消費（capture first），記下開始時間 T0。
2. 對每個 bucket 依 top-level prefix 切片，以低優先權平行執行 S3 listing，結果寫成 Parquet 放到 staging bucket，每個 cluster 限流以免影響線上。
3. 以 `INSERT INTO meta.object_meta SELECT … FROM s3('…/staging/<bucket>/*.parquet')` 匯入，ClickHouse 直接平行讀取 Parquet，衍生表由 MV 自動產生。
4. 匯入完成後該 bucket 才開放 listing；T0 之後的 stream 事件以較大的 `ver` 自然覆蓋 snapshot。

**Access 歷史無法 bootstrap**：MinIO 不記錄 access time，`object_profile` 只能從上線那天開始累積。因此 access 收集應最先上線；在累積滿觀測窗口（建議 30 天，cold demote 需 90 天）之前，tiering 只能以 dry-run 模式運作。

**5.4 Late event**

- 正規化 MV 以事件時間判斷：比 now() 早超過 24 h 的 metadata 事件不寫入 `object_meta`，改寫入 `meta.late_events`。
- Reconciler 每小時對 late event 涉及的 key 做 HEAD 確認後再寫入 repair。
- 這個門檻與 tombstone 清除週期（第 8.3 節）配合：tombstone 至少保留 7 天，遠大於 24 h，因此任何被接受的舊事件都還有 tombstone 可以比較。

**5.5 Reconciler 與目錄清理**

- **Range 比對**：將所有 (bucket, prefix range) 排成輪替清單，每個 range 每 24 h 至少比對一次。ClickHouse 端以 `groupBitXor(cityHash64(object_key, etag, size))` 計算 range hash，MinIO 端以 listing 結果計算同樣的 hash，一致則跳過，不一致才逐筆 diff。
- **修正方式**：差異 key 先 HEAD 確認，再以 repair 事件送入 `meta.repair` topic，與 stream 走同一條 ingest 路徑。
- **吞吐**：5B objects / 24 h 約需 58K keys/s 的 MinIO listing 吞吐，分散到各 cluster 的離峰時段；優先處理 late event、`kafka_errors`、近期 drift 率高的 bucket。
- **空目錄清理**：每日 job 從 `object_meta FINAL` 算出仍有存活 object 的目錄，對 `dir_children` 中已無存活 object 的目錄寫入 tombstone，`ver` 設為掃描開始時間減 1 h，避免誤刪掃描期間才剛到達的新 object 所在目錄。
- **核心指標**：drift rate（每百萬 key 的不一致數）。

## 6. Listing Query Service

既有 client 零改動：OpenResty 把 listing request 導向 Query Service，Query Service 把 S3 參數轉成固定模板的 ClickHouse SQL，在 bucket 所屬 shard 的 serving replica 上執行。其餘 S3 request 照舊打到 MinIO。

**6.1 路由與認證**

- OpenResty 判斷 `GET /<bucket>?list-type=2`（及 v1 `ListObjects`），且 bucket 狀態為 `SERVING`、delimiter 為空或 `/` 時，轉發到 Query Service；其他情況直接走 MinIO。
- Query Service 自行驗證 SigV4（沿用 S3 abstraction layer 的驗證模組與 IAM 同步），再檢查 principal 對 bucket 的 `s3:ListBucket` 權限。
- 回傳標準 `ListBucketResult` XML，欄位與 MinIO 一致；額外附上 `x-mds-watermark`（結果反映到的時間點）與 `x-mds-source: clickhouse | minio`。

**6.2 無 delimiter 的 listing**

```sql
SELECT object_key, size, etag, mtime, storage_class
FROM meta.object_meta FINAL
WHERE cluster_id = {cluster:String}
  AND bucket     = {bucket:String}
  AND object_key >= {prefix:String}
  AND object_key <  {prefix_upper:String}   -- prefix 最後一個 byte + 1；prefix 為空則省略
  AND object_key >  {after:String}          -- start-after 或 token 中的 last_key
ORDER BY object_key
LIMIT {max_keys_plus_one:UInt32}
SETTINGS do_not_merge_across_partitions_select_final = 1,
         optimize_read_in_order = 1,
         max_threads = 4,
         max_execution_time = 2;
```

- 用 `>= / <` 的明確範圍取代 `LIKE 'prefix%'`，確保使用主鍵索引。
- 多取 1 筆用來判斷 `IsTruncated`。
- `FINAL` 會在讀取時對同 key 的多個版本去重，成本與該 range 重疊的 active part 數成正比，因此 part 數是 listing latency 的關鍵指標（第 10 節）。`FINAL` 搭配 `optimize_read_in_order` 能否在 LIMIT 後提前結束，需在 Phase 0 以實際版本 benchmark 確認。
- Query Service 直接連到 bucket 所屬 shard（依第 4.4 節的 sharding key 計算），或透過 Distributed 表加 `optimize_skip_unused_shards = 1`，避免 fan-out。

**6.3 `delimiter=/` 的 listing**

以 `prefix = 'a/b/'` 為例，Query Service 平行送出兩個查詢，再依 key 順序 merge 取前 `max_keys` 筆：

```sql
-- Contents：直屬於 a/b/ 的檔案
SELECT object_key, size, etag, mtime, storage_class
FROM meta.object_by_parent FINAL
WHERE cluster_id = {cluster:String} AND bucket = {bucket:String}
  AND parent = 'a/b/' AND object_key > {after:String}
ORDER BY object_key LIMIT {max_keys_plus_one:UInt32};

-- CommonPrefixes：a/b/ 底下的子目錄
SELECT child_prefix
FROM meta.dir_children FINAL
WHERE cluster_id = {cluster:String} AND bucket = {bucket:String}
  AND parent = 'a/b/' AND child_prefix > {after:String}
ORDER BY child_prefix LIMIT {max_keys_plus_one:UInt32};
```

- Prefix 不以 `/` 結尾時（例如 `a/b/fo`），`parent` 取到最後一個 `/` 為止（`a/b/`），再加上 `>= 'a/b/fo' AND < 'a/b/fp'` 的範圍條件。
- 兩個查詢都只讀一個目錄層級，成本與該目錄的直屬項目數相關，與整個子樹大小無關，這是 delimiter listing 能達到 50x 的關鍵。

**6.4 Pagination**

- Continuation token = base64(HMAC-signed `{v, cluster, bucket, prefix, delimiter, last_key, index_epoch}`)，client 視為 opaque。
- 無跨頁 snapshot：每頁讀當下最新狀態，翻頁期間新增的 key 若大於 `last_key` 會出現在後續頁面，這與 S3 原生 listing 的行為一致。
- Bucket 重新 bootstrap 時 `index_epoch` 遞增，舊 token 仍以 `last_key` 繼續，不報錯。

**6.5 Workload 隔離**

Listing 是低延遲小查詢，tiering 分析是大掃描，兩者必須隔離，否則一個 90 天聚合就能讓 listing p99 爆掉。

| 項目 | Listing（serving） | Analytics / Tiering |
| --- | --- | --- |
| 執行位置 | 每個 shard 的 replica 1、2 | 每個 shard 的 replica 3 |
| Settings profile | `max_threads = 4`、`max_execution_time = 2 s`、`max_memory_usage = 2 GB` | `max_threads = 32`、`max_execution_time = 1 h`、`max_memory_usage = 64 GB` |
| 併發上限 | 每節點 200 | 每節點 10（quota） |
| 使用者 | `mds_query` | `mds_analytics`、`mds_tiering` |

Ingest 寫入所有 replica（Replicated 表本身的複製），background merge 會吃到 serving replica 的 IO，需以 `background_pool_size` 與 merge 相關設定限制，並在第 10 節監控。

**6.6 擴充 API（gRPC + REST gateway）**

| Endpoint | 用途 | 實作 |
| --- | --- | --- |
| `PrefixStats` | Prefix 的 object 數、bytes、tier 分佈 | `object_meta FINAL` 聚合，serving profile 加範圍限制 |
| `AccessProfile` | 指定 object 或 prefix 的 last access 與讀取趨勢 | `object_profile`、`object_daily` |
| `Search` | size、mtime、content\_type、tag、storage\_class 條件查詢 | 參數化 SQL，analytics profile |
| `Export` | 大 prefix 全量匯出（類似 S3 Inventory） | `INSERT INTO FUNCTION s3('…/inventory/…', 'Parquet') SELECT …`，結果寫回 MinIO |
| `Watermark` | 各 cluster / bucket 的 freshness | Kafka consumer lag 與 `max(ingest_ts)` |

**6.7 Fallback 與 bypass**

- Circuit breaker：ClickHouse 錯誤率、listing p99 > 2 s、bucket metadata lag > 5 min 時，OpenResty 自動將 listing 導回 MinIO。
- Bucket 為 `BOOTSTRAPPING` 或 `DEGRADED`、delimiter 不是 `/` 時一律走 MinIO。
- Client 可帶 `x-mds-consistency: strong` 強制走 MinIO 原生 listing。
- Query Service 對熱門的首頁查詢（bucket、prefix、delimiter、max\_keys 相同）做 10 s 的本地快取，吸收儀表板類的重複輪詢。

## 7. Access analytics 與 tiering 決策

決策以 object 為單位分析、以「prefix rule 優先、object manifest 補充」的方式執行：每天為 5B 個 object 計算 access 特徵，套用 policy 產生候選，再把同一 prefix 下幾乎全部可搬的候選合併成 prefix rule 交給 SAL Static Tiering。這樣既保留 object 層級的精準度，也避免對數十億個小檔逐一下 transition 指令。

**7.1 Object 特徵（每日計算）**

| 特徵 | 定義 | 來源 |
| --- | --- | --- |
| `size`、`storage_class` | 目前大小與所在 tier | `object_meta` |
| `age_days` | now − mtime | `object_meta` |
| `idle_days` | now − last\_access；從未被讀則為 min(age\_days, 收集天數) | `object_profile` |
| `reads_7d` / `reads_30d` / `reads_90d` | GET + HEAD 次數（排除 internal principal） | `object_daily` |
| `bytes_read_30d` | 讀取 bytes，用於辨識大物件的 range read | `object_daily` |
| `residency_days` | 進入目前 tier 的天數 | `tiering.actions` 與 transition 事件 |

特徵表 `tiering.object_features` 每日以 `INSERT … SELECT` 在 analytics replica 上重建，各 shard 本地執行。三張來源表都以 `(cluster_id, bucket, object_key)` 排序且同 shard，可使用 `join_algorithm = 'full_sorting_merge'`，記憶體用量與資料量無關：

```sql
INSERT INTO tiering.object_features
SELECT
    m.cluster_id, m.bucket, m.object_key, m.size, m.storage_class,
    dateDiff('day', m.mtime, now())                         AS age_days,
    if(p.last_ts IS NULL,
       least(dateDiff('day', m.mtime, now()), {collect_days:UInt32}),
       dateDiff('day', p.last_ts, now()))                   AS idle_days,
    ifNull(d.reads_7d, 0)  AS reads_7d,
    ifNull(d.reads_30d, 0) AS reads_30d,
    ifNull(d.reads_90d, 0) AS reads_90d,
    ifNull(d.bytes_30d, 0) AS bytes_read_30d
FROM
(
    SELECT cluster_id, bucket, object_key, size, storage_class, mtime
    FROM meta.object_meta FINAL
) AS m
LEFT JOIN
(
    SELECT cluster_id, bucket, object_key, max(last_ts) AS last_ts
    FROM access.object_profile
    GROUP BY cluster_id, bucket, object_key
) AS p USING (cluster_id, bucket, object_key)
LEFT JOIN
(
    SELECT cluster_id, bucket, object_key,
           sumIf(reads + heads, day >= today() - 7)  AS reads_7d,
           sumIf(reads + heads, day >= today() - 30) AS reads_30d,
           sum(reads + heads)                        AS reads_90d,
           sumIf(bytes_read, day >= today() - 30)    AS bytes_30d
    FROM access.object_daily
    WHERE day >= today() - 90
    GROUP BY cluster_id, bucket, object_key
) AS d USING (cluster_id, bucket, object_key)
SETTINGS join_algorithm = 'full_sorting_merge', join_use_nulls = 1;
```

- `object_profile` 與 `object_daily` 是 AggregatingMergeTree，背景 merge 前同一 key 可能有多列，讀取時必須 `GROUP BY`（或 `FINAL`）。
- `FINAL` 會自動排除 `is_deleted = 1` 的列，已刪除的 object 不會進入特徵表。

**7.2 Tier 與成本模型**

| Tier | 媒介（假設） | 儲存成本 | 讀取代價 | 單一 object transition 代價 |
| --- | --- | --- | --- | --- |
| Hot | NVMe / SSD MinIO | C\_hot（最高） | 低延遲，無額外成本 | — |
| Warm | HDD MinIO | C\_warm | 延遲較高，吞吐受限 | 一次 GET + PUT + metadata 更新 |
| Cold | 高 parity HDD 或歸檔層 | C\_cold（最低） | 可能需 restore，分鐘級以上 | 同上，restore 另計 |

每個 demote 候選計算淨效益，只有 `net_benefit > margin` 才成為候選：

```latex
\text{net\_benefit} = \text{size} \times (C_{src} - C_{dst}) \times H - \widehat{\text{reads}}_{H} \times P_{dst} - T_{obj}
```

其中 H 為評估期（例如 90 天）、預估讀取次數由 reads\_30d 外推、P\_dst 為目的 tier 每次讀取的代價（延遲換算或 restore 成本）、T\_obj 為單一 object 的 transition 固定成本。C、P、T 各參數由 storage 團隊依實際硬體與 SLA 填入，存在 `tiering.policy` 表中，修改需經審核並留版本紀錄。

**7.3 初始 policy（可調）**

| 動作 | 條件 | 說明 |
| --- | --- | --- |
| Hot → Warm | idle\_days ≥ 30、reads\_30d = 0、age\_days ≥ 30、size ≥ 128 KiB、residency\_days ≥ 14 | 小於 128 KiB 的檔案只能透過 prefix rule 一起搬 |
| Warm → Cold | idle\_days ≥ 90、reads\_90d = 0、size ≥ 1 MiB、residency\_days ≥ 30 | Cold restore 代價高，門檻較嚴 |
| Warm → Hot | reads\_7d ≥ 10，或 bytes\_read\_30d ≥ 5 × size | 頻繁讀取或大量 range read |
| Cold → Hot | reads\_7d ≥ 3 | 每次讀取都需要 restore，少量讀取即值得回升 |
| 排除 | Object Lock / legal hold、tag `tier=pinned`、bucket 不在 allowlist | 永不自動搬移 |

- **遲滯（hysteresis）**：任何搬移後至少停留 `residency_days` 才可再次搬移；demote 的門檻遠高於 promote 的反向門檻，避免同一批資料來回振盪。
- **觀測窗口不足**：access 收集天數未滿 30 天時，所有 demote 一律 dry-run。

**7.4 從候選到執行**

1. **產生候選**：套用 policy 與成本模型，寫入 `tiering.candidates(run_id, cluster_id, bucket, object_key, action, size, net_benefit, reason)`。
2. **Prefix 合併**：對每個 prefix（depth ≤ 4）計算「候選 bytes / prefix 總 bytes」與「候選數 / prefix 總數」，兩者都 ≥ 95% 時合併為一條 prefix rule；不足 5% 的非候選 object 若屬於排除條件則跳過整個 prefix。
3. **Object manifest**：未被合併的候選中，只保留 size ≥ 1 MiB 的 object，以 `INSERT INTO FUNCTION s3(…, 'Parquet')` 輸出 manifest 到 MinIO；小檔若無法合併為 prefix rule，則留待下次評估。
4. **交付 SAL**：Prefix rule 以 append-only immutable rule 寫入 SAL Static Tiering 的 etcd config store；object manifest 交給 SAL executor 批次執行。
5. **執行回流**：每筆實際搬移寫入 `tiering.actions`；MinIO 發出的 `ObjectTransition` 事件更新 `object_meta.storage_class`，形成閉環。

**7.5 安全機制**

- **預設 dry-run**：新 policy 或新 bucket 先以 dry-run 跑至少 2 個週期，比對候選與預期再開啟執行。
- **Blast radius 上限**：每日搬移量 ≤ 來源 tier 容量的 2%、單一 bucket ≤ 20%、單次 run 的 object 數上限，超過者需人工核准。
- **Canary**：新 policy 先套用於指定的 canary bucket，mis-tier 率達標後再擴大。
- **Kill switch**：一個 etcd 開關即可停止所有新 rule 下發；已下發的 rule 由 SAL 依其 immutable 版本機制停用。
- **Rollback**：從 `tiering.actions` 取出某個 `run_id` 的搬移清單，產生反向 promote manifest。

**7.6 效果衡量**

| 指標 | 定義 | 目標 |
| --- | --- | --- |
| Mis-tier 率 | Demote 後 30 天內被讀的 bytes / demote 總 bytes | < 2% |
| Ping-pong 率 | 90 天內被 demote 又 promote 的 object 比例 | < 0.5% |
| Hot tier 命中率 | 讀取 bytes 中由 hot tier 服務的比例 | 依 SLA 設定 |
| 釋放容量 | 每週由 hot 移出的淨 bytes | 依容量規劃 |

每週自動產出報表，作為調整門檻與成本參數的依據。累積半年以上資料後，可評估以讀取預測模型取代固定門檻（第 11 節）。

## 8. 一致性模型

Metadata 提供 bounded-staleness 的 eventual consistency：單一 key 依 `ver` 單調前進、已刪除物件不會復活、差異在 24 h 內由 reconciler 收斂。Access 資料是「近似但偏保守」的統計：可能略多計（重複事件），不會少計經過 gateway 的讀取。

**8.1 保證與不保證**

| 項目 | 是否保證 | 機制 |
| --- | --- | --- |
| 同一 key 不會退回舊狀態 | 是 | ReplacingMergeTree 依 `ver` 保留最新列 |
| 已刪除物件不會因亂序事件復活 | 是 | Tombstone 保留 ≥ 7 天 + late event 門檻 24 h |
| Listing 不會出現重複 key | 是 | Listing 一律 `FINAL` |
| 結果新鮮度可被觀測 | 是 | `x-mds-watermark` |
| 最終收斂 | 是（≤ 24 h） | Reconciler |
| 跨頁一致 snapshot | 否 | 與 S3 原生行為相同 |
| Read-your-writes | 否 | 需要時使用 `x-mds-consistency: strong` |
| Listing 中的物件一定可 GET | 否 | 刪除可能尚未反映 |
| 空目錄立即消失 | 否 | 每日清理，最長約 24 h |
| Access 次數精確 | 否 | At-least-once，可能略多計 |

**8.2 Freshness 與 watermark**

- Metadata lag = MinIO 事件時間到 `object_meta` 可查的時間差，組成為 MinIO notification（秒級）、Kafka、Kafka engine flush（7.5 s）。
- Bucket watermark = 該 cluster topic 所有 partition 中，已寫入 ClickHouse 的最舊未處理事件時間；Query Service 每 10 s 從 consumer lag 與 `max(ingest_ts)` 更新。
- Access lag = gateway log 寫入到聚合表可查的時間差，組成為 Vector 批次、Kafka、flush，目標 p99 < 15 min；tiering 是每日批次，對此延遲不敏感。

**8.3 Tombstone 生命週期**

1. 刪除事件寫入 `is_deleted = 1` 的列，`FINAL` 查詢立即排除。
2. Tombstone 至少保留 7 天，期間用來擋住亂序到達的舊 PUT。由於 late event 門檻是 24 h，任何被接受的事件都一定還有對應的 tombstone 可比較。
3. 每週離峰時段，對每個 partition 執行 `DELETE FROM meta.object_meta WHERE is_deleted = 1 AND ver < {now − 7d}`（lightweight delete），衍生表同樣處理。之所以不用 `OPTIMIZE … FINAL CLEANUP`，是因為它會不分年齡地移除所有 tombstone。
4. `access.object_profile` 中已刪除 object 的列，在同一個週期 job 中以 anti-join 清除，避免無限成長。

**8.4 Access 資料的已知偏差**

| 偏差來源 | 方向 | 影響 | 處理 |
| --- | --- | --- | --- |
| Kafka / Vector 重複送達 | 多計 | 極少量 | 可接受，偏向較晚 demote |
| 不經 gateway 的流量（直連 MinIO） | 少計 | 可能誤判為冷資料 | 網路層禁止直連；或以 MinIO audit webhook 補收 |
| Internal principal 排除不完整 | 多計 | 冷資料被誤判為熱 | Principal 清單納入變更管理，dashboard 顯示 top principals |
| Vector / Kafka 長時間中斷 | 少計 | 中斷期間的讀取遺失 | 監控 gap；中斷期間的資料不納入 idle 判斷（決策延後一個週期） |

## 9. 容量與效能估算

以第 2 節的假設，ClickHouse 每個 replica 約需 12 TB（其中 access raw event 佔 2/3），建議 4 shard × 3 replica 共 12 個節點；單頁 listing p99 估計 300–500 ms。儲存量主要由 GET / HEAD 速率決定，所有數字需以 Phase 0 的實際資料校正。

**9.1 ClickHouse 儲存（每個 replica，壓縮後）**

| 表 | 估算基礎 | 每列 | 5B objects 時 |
| --- | --- | --- | --- |
| `meta.object_meta` | 5B 列；key 排序後前綴相似，壓縮率高 | 60–80 B | \~350 GB |
| `meta.object_by_parent` | 5B 列，欄位較少 | 45–60 B | \~250 GB |
| `meta.dir_children` | 目錄數假設 1–2 億 | 20–30 B | < 10 GB |
| `access.events_raw` | 平均 50K/s ≈ 43 億列/天，保留 30 天 | 55–65 B | \~7.8 TB |
| `access.object_daily` | 每天被讀的不同 object 假設 3 億，保留 400 天 | \~20 B | \~2.4 TB |
| `access.object_profile` | ≤ 5B 列 | \~35 B | ≤ 175 GB |
| `tiering.object_features` | 5B 列，保留最近 2 次 run | \~60 B | \~600 GB |
| 合計 |  |  | **\~12 TB** |

節省空間的選項：`etag` 改存 `FixedString(16)`（binary MD5）；`events_raw` 的 `request_id`、`client_ip` 只保留 7 天（欄位級 TTL）；access 系列表只放在 2 個 replica 上（serving replica 不需要）。

**9.2 叢集配置建議**

| 元件 | 配置 | 說明 |
| --- | --- | --- |
| ClickHouse | 4 shard × 3 replica = 12 節點；每節點 48 core、256 GB RAM、2 × 7.68 TB NVMe | Replica 1、2 服務 listing，replica 3 跑 analytics 與 tiering；容量保留 2 倍給 merge 與成長 |
| ClickHouse Keeper | 3 節點（獨立部署） | Replicated 表的協調，不與 ClickHouse server 共用機器 |
| Kafka | 6 broker；meta topic 每 cluster 64 partitions，access topic 128 partitions | 保留 3 天：平均 \~30 MB/s × 3 天 × 3 replica ≈ 23 TB |
| Vector | 每台 OpenResty 節點一個 agent | Disk buffer 至少 6 h |
| Query Service | 6+ pods，stateless，HPA |  |
| Reconciler / Bootstrap / Tiering Engine | 各 2+ pods | Tiering Engine 為每日 batch，單一 leader |

20B objects 時，metadata 相關表約為 4 倍，shard 數擴到 8；access 相關表與 object 數無直接關係，主要隨讀取速率成長。

**9.3 寫入能力**

- Metadata：peak 100K events/s，經 MV 扇出到 3 張表，即 300K rows/s；access：peak 200K events/s 扇出到 4 張表，即 800K rows/s。分散到 4 個 shard 後，每 shard 約 280K rows/s，在大批次寫入下遠低於 ClickHouse 的上限。
- 主要風險是 part 數而非吞吐：每次 flush 會在每個被觸及的 partition 產生一個 part。`stream_flush_interval_ms = 7500` 加上 16 個 bucket-hash partition，每 shard 每張表約每秒 2 個新 part，需確認 merge 速度能跟上（第 10 節的 part 數告警）。

**9.4 單頁 listing latency 預算（max-keys = 1000）**

| 階段 | p50 | p99 |
| --- | --- | --- |
| OpenResty 路由 + SigV4 驗證 | 3 ms | 5 ms |
| ClickHouse 解析與規劃 | 3 ms | 10 ms |
| 主鍵定位 + 讀 granule（多個 part） | 20–50 ms | 150–300 ms |
| FINAL 去重 + 排序 | 10–30 ms | 50–100 ms |
| XML 序列化 + 網路 | 10 ms | 30 ms |
| 合計 | **50–100 ms** | **250–450 ms** |

Delimiter listing 的兩個查詢平行執行，總時間取較慢者，約多 10–20%。

**9.5 50x 是否成立**

- 冷的大 prefix 與 delimiter listing：MinIO 單頁常為秒級甚至十秒級，ClickHouse 約 100 ms 級，50x 預期可達。
- 全量 enumeration：`Export` 以 ClickHouse 平行掃描主鍵範圍並直接寫 Parquet，1 億 keys 預估 1 分鐘內完成，遠超 50x。
- 熱的小 prefix：MinIO metacache 命中時可能已在 100 ms 以內，ClickHouse 不會更快；這類請求本來就不是瓶頸，驗收不以此為準。

**9.6 每日 tiering 計算**

特徵表重建在每個 shard 上以 `full_sorting_merge` 處理約 12.5 億列 metadata 與對應的 access 聚合，預估 10–30 分鐘；candidates 與 prefix 合併再 5–10 分鐘。全部在 analytics replica 上執行，不影響 listing。

## 10. 可靠性、監控與運維

Metadata 可由 MinIO 重建，故障的最壞結果是 listing 退回 MinIO 原生速度；access 歷史無法重建，是唯一需要真正保護的資料；tiering 決策錯誤的代價最高，因此有最多層的防護。

**10.1 Failure modes**

| 故障 | 影響 | 處理 |
| --- | --- | --- |
| Kafka 不可用 | Metadata 暫存於 MinIO `queue_dir`，access 暫存於 Vector disk buffer；lag 上升 | 恢復後自動補送；listing lag 超標時 fallback；access 中斷期間不納入 idle 判斷 |
| `queue_dir` 或 Vector buffer 滿溢 | Metadata 部分遺失 / access 部分遺失 | Metadata 由 reconciler 補正；access gap 記錄於 `access.gaps` 表，tiering 跳過受影響期間 |
| Kafka engine consumer 卡住（parse error、schema 變更） | 單一 topic lag 上升 | `kafka_handle_error_mode = 'stream'` 隔離壞訊息；schema 以 versioned Protobuf 管理 |
| Part 數過多（merge 跟不上） | Insert 被節流或拒絕（too many parts）；listing `FINAL` 變慢 | 加大 flush 間隔與 batch；調整 merge pool；必要時暫停 bootstrap |
| ClickHouse Keeper 失去多數 | Replicated 表變唯讀，ingest 停止，listing 仍可讀 | Keeper 3 節點跨 rack；恢復後 Kafka 從 offset 續讀，不遺失 |
| 單一 ClickHouse 節點故障 | 該 shard 剩餘 replica 承接 | Query Service 自動切換 replica；替換節點後自動從其他 replica 複製 |
| 整個 shard 失效 | 該 shard 的 bucket listing 退回 MinIO；access 寫入堆積在 Kafka | 3 replica 跨 rack 降低機率；Kafka 保留 3 天提供緩衝 |
| Tiering policy 錯誤 | 大量資料被誤搬，影響應用延遲 | Dry-run、blast radius 上限、canary、kill switch、依 `run_id` rollback |
| 整個站點失效 | 全部不可用 | Metadata 可重建；access 歷史以每日 `BACKUP` 到另一站點的 MinIO 保護 |

**10.2 關鍵指標**

- **Ingest**：各 topic consumer lag、`max(ingest_ts)` 與 now 的差距、`kafka_errors` 數量、每秒寫入列數。
- **Merge 健康度**：每個 partition 的 active part 數、merge 佇列長度、`DelayedInserts` / `RejectedInserts`。
- **Listing**：p50 / p99（分 delimiter 與否、冷熱 prefix）、fallback 比例、`x-mds-source=minio` 比例、每個 serving replica 的併發數。
- **正確性**：Reconciler drift rate、24 h 內完成比對的 range 比例、late event 數量。
- **Access 資料品質**：每小時事件數與歷史同期比較（偵測 gap）、internal principal 佔比、top principals。
- **Tiering**：每日候選數與 bytes、實際執行量、mis-tier 率、ping-pong 率、run 耗時。

**10.3 告警門檻（初始值）**

| 條件 | 等級 |
| --- | --- |
| Metadata lag p99 > 5 min 持續 5 min | Page |
| Access lag > 1 h，或每小時事件數低於歷史同期 50% | Page |
| 任一 partition active part 數 > 300 | Page |
| Drift rate > 1 per million | Ticket；> 100 per million 則 page 並暫停該 bucket 的 listing serving |
| Listing fallback 比例 > 5% 持續 10 min | Ticket |
| 單次 tiering run 的搬移量超過 blast radius 上限 | 自動暫停並 page |
| Mis-tier 率週平均 > 5% | Ticket，檢討 policy |

**10.4 運維操作**

- **Schema 變更**：一律以 `ALTER TABLE … ON CLUSTER` 加欄位（向後相容），Kafka 訊息格式以 versioned Protobuf 演進；不相容的變更以新表 + 雙寫切換。
- **Bucket 管理**：onboarding（bootstrap → `SERVING`）、offboarding、單一 bucket 重建、強制 reconcile 指定 prefix，皆透過 admin API 執行並記錄 audit log。
- **備份**：每日 `BACKUP` access 與 tiering database 到異地 MinIO，保留 30 天；metadata database 不備份（可重建）。
- **升級**：ClickHouse 以 replica 為單位 rolling upgrade，先升 analytics replica，觀察一週再升 serving replica。

## 11. 取捨、風險與需重新檢視的條件

選擇 ClickHouse-only，是以「listing latency 較高且較不穩定」換取「object 層級分析能力」與「只維運一套儲存系統」。這個取捨在 tiering 是核心、listing 可接受數百 ms 的前提下成立；前提改變時需重新評估。

**11.1 方案比較**

| 方案 | Listing | Object 層級 tiering 分析 | 維運 | 結論 |
| --- | --- | --- | --- | --- |
| **ClickHouse-only（選用）** | p99 數百 ms，受 part 數影響 | 原生支援 5B × 數十億事件的 join 與聚合 | Kafka + ClickHouse + Keeper | 符合目前優先順序 |
| TiKV + ClickHouse | p99 數十 ms | 同左 | 多一套 TiKV / PD，團隊不熟 | Listing 好，但維運成本最高 |
| Elasticsearch + ClickHouse | p99 \~100 ms | 同左 | 兩套大型叢集 | Listing 與搜尋較好，硬體成本高 |
| Elasticsearch-only | p99 \~100 ms | 只能做 prefix 粒度，object 層級成本過高 | 一套 | 不符合 object 層級需求 |

**11.2 主要風險與緩解**

| 風險 | 可能性 | 緩解 |
| --- | --- | --- |
| `FINAL` 在 part 數多時讓 listing p99 超過 500 ms | 中 | `index_granularity = 1024`、控制 flush 頻率與 partition 數、part 數告警；Phase 0 先以實際版本 benchmark |
| Background merge 與 listing 搶 IO | 中 | Serving / analytics replica 分離、限制 merge pool、NVMe |
| `dir_children` 的空目錄延遲消失 | 高（設計如此） | 文件化為已知行為；每日清理；必要時 Query Service 對少量結果做存在性檢查 |
| Access 收集遺漏直連流量 | 中 | 網路層禁止繞過 gateway；或補接 MinIO audit webhook |
| 錯誤 policy 造成大量誤搬 | 低，但影響大 | Dry-run、canary、blast radius 上限、kill switch、rollback |
| ClickHouse 版本行為差異（`is_deleted`、lightweight delete、read-in-order + FINAL） | 中 | 鎖定 LTS 版本；所有依賴的行為列入整合測試 |

**11.3 其他取捨**

- **Kafka engine vs 獨立 ingest 服務**：Kafka engine 少一個服務，但錯誤處理與觀測性較弱。若 Phase 1 發現 consumer 卡住難以排查，可改用 ClickHouse Kafka Connect Sink（支援 exactly-once）。
- **Gateway log vs MinIO `s3:ObjectAccessed:*` notification**：Gateway log 不增加 MinIO 負載，也不受 `queue_dir` 容量限制；代價是看不到繞過 gateway 的流量。
- **固定門檻 vs 預測模型**：初期以可解釋的固定門檻上線，方便審核與除錯；累積半年資料後，再評估以讀取預測模型（例如依 prefix、content\_type、age 預測未來 30 天讀取機率）取代。
- **Strong consistency**：需要在 PUT 路徑同步寫入 metadata，會讓 ClickHouse 進入資料寫入的 critical path，本設計刻意不做。

**11.4 需要重新檢視的條件**

- Listing 實測 p99 長期 > 500 ms，或 listing QPS 需求 > 1K page/s：在 ClickHouse 前加一層 serving store（TiKV 或 ES），ClickHouse 回到純分析角色。
- Object 數超過 20B，或讀取速率使 access raw event 超過每日 1 TB：提高 shard 數，並評估對 HEAD 取樣或縮短 raw 保留期。
- 後端改為 SeaweedFS 等自帶 filer metadata store 的系統：listing 可改由其 metadata store 提供，本系統專注於 access analytics 與 tiering。
- 出現跨 region 部署需求：重新設計 shard 放置與 access 資料的匯集方式。

## 12. 分階段落地與驗收

Access 收集最先上線：access 歷史無法回補，越早開始累積，tiering 越早能從 dry-run 進入正式執行。Listing 與 metadata 管線可以與觀測期並行建置。每個階段都有可量化的 exit criteria，未達標不進下一階段。

1. **Phase 0 — Baseline 與技術驗證**
   - 以 warp 與自製 list harness（可延伸既有 poc-storage-bench）量測 MinIO 在熱 / 冷、有無 delimiter、不同 prefix 規模下的 listing latency 與 enumeration 時間。
   - 以 10 億列合成資料在目標 ClickHouse LTS 版本上驗證：`FINAL` listing 的 p50 / p99 與 part 數的關係、`is_deleted` 與 lightweight delete 行為、`full_sorting_merge` join 的耗時。
   - 實測 MinIO `ObjectTransition` 事件、ILM expiration、replication 是否發出 notification。
   - 統計實際 GET / HEAD 速率與 internal principal 清單，校正第 9 節的容量估算。
   - Exit：baseline 數據定案，50x 目標寫成具體數字；ClickHouse listing p99 < 500 ms 在合成資料上成立。
2. **Phase 1 — Access 收集上線（觀測期開始）**
   - 部署 Vector、Kafka access topic、`access.*` 表與 MV，涵蓋所有 gateway。
   - 建立 access 資料品質 dashboard（事件數、gap、internal 佔比）。
   - Exit：連續 7 天無 gap，事件數與 OpenResty 日誌總數差異 < 0.1%。
3. **Phase 2 — Metadata 管線與 pilot bucket**
   - 部署 metadata topic、`meta.*` 表、Reconciler；選一個 1 億級 LOSF bucket 執行 bootstrap 與即時同步。
   - Shadow listing：對同一請求同時查 ClickHouse 與 MinIO，比對結果。
   - Exit：metadata lag p99 < 60 s、drift rate < 1 per million、shadow 差異全部可由 lag 或空目錄解釋。
4. **Phase 3 — Listing shim 上線與全量 bootstrap**
   - OpenResty 路由、SigV4、continuation token、circuit breaker、workload 隔離。
   - 以 1% → 10% → 100% 漸進切流量；其餘 bucket 依序 bootstrap。
   - Exit：冷大 prefix 與 enumeration 達成 50x、listing p99 < 500 ms、fallback < 1%。
5. **Phase 4 — Tiering dry-run → canary → production**
   - Access 累積滿 30 天後開始每日 dry-run，人工檢視候選與 prefix 合併結果。
   - 選定 canary bucket 開啟 hot → warm 實際執行，觀察 30 天 mis-tier 率。
   - Access 累積滿 90 天後開啟 warm → cold；promote 規則同步上線。
   - Exit：mis-tier 率 < 2%、ping-pong 率 < 0.5%、blast radius 機制演練通過。
6. **Phase 5 — 擴展**
   - 全部 bucket 納入 tiering、`Export` / `Search` 等擴充 API、跨 cluster 統一 namespace、versioned bucket 支援。
   - 累積半年資料後，評估以讀取預測模型取代固定門檻。
