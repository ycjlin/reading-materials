# S3 跨集群同 Bucket List Merge - 完整方案 (Mermaid 版)

## 需求
- Cluster A 和 B 都有 my-bucket，已有大量存量，Key 可能重複
- List 要返回並集，去重取最新
- 需要正確處理 delimiter=/ 的 CommonPrefixes

## 架構圖

```mermaid
graph TD
    Client["S3 Client<br/>aws cli / rclone / SDK"]
    Proxy["List Merge Proxy<br/>:9000<br/>Go + aws-sdk-go-v2"]
    ClusterA["Cluster A<br/>MinIO / Ceph<br/>my-bucket (存量)"]
    ClusterB["Cluster B<br/>MinIO / Ceph<br/>my-bucket (存量)"]

    Client -->|GET /my-bucket?list-type=2<br/>prefix=photos/&delimiter=/| Proxy
    Proxy -->|ListObjectsV2<br/>prefix=photos/<br/>delimiter=/<br/>StartAfter=xxx| ClusterA
    Proxy -->|ListObjectsV2<br/>prefix=photos/<br/>delimiter=/<br/>StartAfter=xxx| ClusterB
    ClusterA -->|Contents + CommonPrefixes<br/>1000條/頁| Proxy
    ClusterB -->|Contents + CommonPrefixes<br/>1000條/頁| Proxy
    Proxy -->|Heap Merge 去重 + 全域排序| Client

    style Proxy fill:#2d5a27,stroke:#fff,stroke-width:2px,color:#fff
    style Client fill:#1a1a2e,stroke:#fff,color:#fff
```

```mermaid
flowchart LR
    subgraph BackendA[Cluster A]
        A1[ListObjectsV2<br/>StartAfter]
        A2[Buffer<br/>1000 keys]
    end
    subgraph BackendB[Cluster B]
        B1[ListObjectsV2<br/>StartAfter]
        B2[Buffer<br/>1000 keys]
    end
    subgraph Proxy[Proxy Heap Merge]
        H[Min-Heap<br/>按 Key 排序]
        D{Dedup<br/>同 Key 取最新}
        P{Delimiter?<br/>Prefix 去重}
        R[Result<br/>maxKeys條]
    end
    A2 --> H
    B2 --> H
    H --> D --> P --> R
    R -->|NextToken = base64(lastKey)| Client
```

## 流式 Heap Merge 詳細流程

```mermaid
sequenceDiagram
    participant Client
    participant Proxy
    participant IterA as BackendIter A
    participant S3A as Cluster A S3
    participant IterB as BackendIter B
    participant S3B as Cluster B S3

    Client->>Proxy: List V2 prefix=photos/ delimiter=/ maxKeys=1000
    Proxy->>IterA: loadNextPage(StartAfter="")
    IterA->>S3A: ListObjectsV2 prefix=photos/ delimiter=/ maxKeys=1000
    S3A-->>IterA: Contents + CommonPrefixes (sorted)
    Proxy->>IterB: loadNextPage(StartAfter="")
    IterB->>S3B: ListObjectsV2 prefix=photos/ delimiter=/ maxKeys=1000
    S3B-->>IterB: Contents + CommonPrefixes (sorted)

    Proxy->>Proxy: heap.Push(A.current, B.current)

    loop 直到湊夠 maxKeys
        Proxy->>Proxy: pop 最小 Key=K
        Proxy->>Proxy: 收集堆中所有 Key==K 的項目
        Proxy->>Proxy: 若有 Prefix -> 發 Prefix<br/>否則選 LastModified 最新的 Content
        Proxy->>IterA: advance() 若貢獻了 K
        IterA->>S3A: 若 buffer 空，自動拉下一頁
        Proxy->>IterB: advance() 若貢獻了 K
        IterB->>S3B: 若 buffer 空，自動拉下一頁
        Proxy->>Proxy: push 新的 current 入堆
    end

    Proxy->>Client: XML + NextContinuationToken=base64(lastKey)
    Client->>Proxy: List V2 continuation-token=base64(lastKey)
    Note right of Proxy: 解出 StartAfter=lastKey<br/>所有後端以 StartAfter 續拉<br/>S3 原生 seek，高效
```

## Delimiter 跨頁去重問題解法

```mermaid
graph TD
    Problem["問題：Proxy 自己算 delimiter<br/>發過 a/b/ 後，StartAfter=a/b/<br/>又看到 a/b/c 再算出 a/b/ -> 重複"]
    Solution["解法：讓後端 S3 自己算 delimiter<br/>後端 StartAfter=a/b/ 時<br/>已跳過 a/b/ 下所有 key"]

    Problem --> Solution

    subgraph 錯誤路徑
        E1[a/b/c -> 算出 a/b/ 發出] --> E2[StartAfter=a/b/]
        E2 --> E3[a/b/d -> 又算出 a/b/ -> 重複!]
    end

    subgraph 正確路徑
        C1[後端 List prefix=a/ delimiter=/] --> C2[S3 返回 CommonPrefix a/b/<br/>不返回 a/b/c, a/b/d]
        C2 --> C3[Proxy 發 a/b/]
        C3 --> C4[StartAfter=a/b/ 後端 S3 已跳過 a/b/*<br/>直接返回下一個 a/c/]
    end
```

## 去重邏輯

```mermaid
flowchart TD
    Start[堆頂彈出 Key=K<br/>收集所有 Key==K 的項目] --> CheckPrefix{是否有 Prefix?}
    CheckPrefix -->|是| EmitPrefix[發 Prefix K<br/>符合 S3 delimiter 語義]
    CheckPrefix -->|否| PickLatest[選 LastModified 最新的 Content<br/>滿足存量合併需求 1]

    EmitPrefix --> Advance[推進所有貢獻 K 的 Iter]
    PickLatest --> Advance
    Advance --> Next{堆空或已夠 maxKeys?}
    Next -->|否| Start
    Next -->|是| End[生成 NextToken]
```

## 效能對比

| 項目 | 全量版 | 流式版 (本方案) |
|------|--------|----------------|
| 每次請求 S3 調用 | 拉該 prefix 全量 N/1000 次 | 每後端 1 頁 |
| 10M List 1000 條 | ~10秒 | ~50ms |
| 記憶體 | O(N) | O(maxKeys * backends) |
| Token | base64(lastKey) | base64(lastKey) + 後端按 StartAfter 續拉 |

## 配置

```yaml
addr: :9000
primary: cluster-a
backends:
  - name: cluster-a
    endpoint: http://minio-a:9000
    accessKey: xxx
    secretKey: xxx
    bucket: my-bucket
    usePathStyle: true
  - name: cluster-b
    endpoint: http://minio-b:9000
    accessKey: xxx
    secretKey: xxx
    bucket: my-bucket
    usePathStyle: true
```

## 運行

```bash
go mod init s3-list-merge-proxy
go get github.com/aws/aws-sdk-go-v2/config github.com/aws/aws-sdk-go-v2/credentials github.com/aws/aws-sdk-go-v2/service/s3 gopkg.in/yaml.v3
go run main-streaming.go -config config.yaml
```

```bash
aws --endpoint-url http://localhost:9000 s3 ls s3://my-bucket --recursive
aws --endpoint-url http://localhost:9000 s3 ls s3://my-bucket --prefix photos/ --delimiter /
aws --endpoint-url http://localhost:9000 s3api list-objects-v2 --bucket my-bucket --prefix photos/ --delimiter "/" --max-keys 2
```
