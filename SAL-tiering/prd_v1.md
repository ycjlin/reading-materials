我已將系統設計要求整理成結構化 JSON，並寫入文件供你檢查。以下是完整的需求清單（共 10 大類、60+ 子項）：

## 系統設計要求清單

### 1. 功能性需求

*1.1 存取路徑*
- 支援 S3-compatible API (PUT/GET/DELETE/HEAD/List)
- GET/HEAD 請求需觸發 last_access_time 更新（24h 去重）
- 讀取路徑透明：無論 object 在哪個 tier，client 無感知
- 支持 object 版本控制（可選）

*1.2 Tiering 規則*
- 三層默認：Frequent (<30d), IA (30-90d), Archive (>90d)
- 可配置閾值（天數）
- 支持 minimum stay 約束（例如 IA 至少 30d 才能移回）
- 支持手動指定 object 的 tier（bypass 自動規則）

*1.3 遷移功能*
- 背景遷移：不阻塞讀寫
- 原子性：遷移失敗時回滾，不丟失數據
- 支持並行遷移（多 worker）
- 支持斷點續傳（大 object 分塊遷移）

### 2. 性能需求

*2.1 延遲*
- Hot tier: P99 < 10ms (NVMe)
- Warm tier: P99 < 100ms (HDD)
- Cold tier: P99 < 1s (Archive，可接受預熱)

*2.2 吞吐量*
- 單 gateway: 10Gbps+ 聚合帶寬
- 支持水平擴展 gateway 實例

*2.3 Metadata 更新*
- Access event 寫入延遲 < 5ms (Kafka produce)
- Metadata DB batch update 吞吐量 > 100K ops/s

*2.4 Scanner 效率*
- 全量掃描 < 1h (10 億 objects)
- 增量掃描 < 5min (只掃描最近 24h 變化的 object)

### 3. 可靠性需求

*3.1 數據持久性*
- Hot/Warm tier: 99.999999999% (11 個 9)
- Cold tier: 99.9999999999% (12 個 9)
- Erasure coding: RS(8,3) 或 RS(10,4)

*3.2 可用性*
- Hot tier: 99.99% SLA
- Warm tier: 99.9% SLA
- Cold tier: 99% SLA

*3.3 故障恢復*
- 單節點故障：自動重建 < 30min
- Metadata DB: 多 AZ 部署，RPO=0, RTO<1min
- Migration 中斷：重啟 worker 後自動續傳

### 4. 一致性需求

*4.1 Metadata 一致性*
- Metadata 強一致（讀寫後立即可見）
- Migration 完成後，新讀請求必須看到新 location_ptr

*4.2 遷移原子性*
- 遷移過程：source 數據保留直到 target 寫入成功 + metadata 更新
- 讀請求在遷移中：優先讀 source，若 source 已刪則讀 target

### 5. 成本需求

*5.1 監控開銷*
- 每 object 監控成本 < $0.0025/月（參考 S3）
- Metadata 存儲開銷 < 1KB/object

*5.2 遷移成本*
- 遷移流量不計費（內部網絡）
- 避免不必要的遷移（thrashing 控制）

*5.3 存儲效率*
- Erasure coding 開銷 < 1.5x
- 小 object (<128KB) 合併存儲，避免放大

### 6. 可運維性需求

*6.1 監控告警*
- 每 tier 的 object 數量、存儲量、訪問量
- Migration queue 長度、成功率、平均延遲
- Metadata DB 延遲、QPS、錯誤率
- Gateway P99 延遲、錯誤率

*6.2 日誌審計*
- 所有 access event 記錄（object_id, timestamp, client_ip）
- 所有 migration event 記錄（object_id, source_tier, target_tier, status）
- 日誌保留 90d

*6.3 配置管理*
- Tiering 規則熱更新（不需重啟）
- 支持 per-bucket 或 per-prefix 覆蓋規則

### 7. 安全性需求

*7.1 訪問控制*
- 支援 IAM policy / ACL
- 支援 bucket policy 限制 per-tier 訪問
- 支援 VPC endpoint / private link

*7.2 加密*
- 傳輸加密：TLS 1.3
- 存儲加密：SSE-S3 / SSE-KMS / SSE-C
- Metadata 加密：field-level encryption for sensitive fields

*7.3 合規*
- 支援 object lock (WORM)
- 支援 retention policy (不可刪除)
- 支援 audit log 導出

### 8. 擴展性需求

*8.1 水平擴展*
- Gateway 無狀態，可彈性伸縮
- Metadata DB 支持 sharding (按 bucket 或 object_id hash)
- Storage backend 支持動態添加 node

*8.2 容量*
- 支持 100 億+ objects
- 支持 100PB+ 總存儲
- 單 bucket 支持 10 億+ objects

### 9. 兼容性需求

*9.1 S3 API*
- 100% 兼容 S3 REST API (包括 ListObjectsV2, CopyObject, etc.)
- 支援 S3 SDK (AWS SDK, boto3, minio-py, etc.)

*9.2 現有工具*
- 支援 S3 Inventory (生成 object list + tier 信息)
- 支援 S3 Select (對 hot tier object)
- 支援 lifecycle policy 語法（可選）

### 10. 邊界場景處理

*10.1 小 object*
- <128KB object 按 128KB 計費（或合併打包）
- Metadata 開銷優化：使用 compressed index

*10.2 大 object*
- >5GB object 支持 multipart upload
- Multipart 遷移：分塊並行遷移

*10.3 高頻訪問 object*
- 24h 內多次訪問只更新一次 metadata
- 可選 cache 層：Redis cache hot object metadata

*10.4 遷移衝突*
- 同一 object 同時被讀取和遷移：讀優先，遷移重試
- 使用 optimistic lock (version number) 防止 race condition

*

## 建議改進點（基於你的背景）

1. **性能指標量化不足**：目前 P99 延遲目標較保守，可根據你的實際 workload（如 CephFS / MinIO 經驗）調整
2. **缺少成本效益分析**：建議加入 ROI 計算（例如：預期節省多少 % 存儲成本 vs 監控開銷）
3. **缺少測試策略**：建議補充 chaos engineering 測試（如隨機 kill migration worker、network partition 等）
4. **缺少 observability 細節**：建議加入 distributed tracing (OpenTelemetry) 要求
5. **缺少 multi-tenancy 支持**：如果公司內部多團隊使用，需考慮 quota、isolation、billing 等

你可以告訴我哪些部分需要調整或補充，我可以幫你迭代優化。
