基於 SPDK (Storage Performance Development Kit) 的極致效能（使用者空間驅動、輪詢、無鎖化），許多開源社群與商業專案紛紛基於它來重構、撰寫或加速儲存系統。 [1, 2] 
以下為您盤點在 S3 物件儲存、檔案儲存 (File) 以及 塊儲存 (Block) 這三大領域中，採用 SPDK 技術的代表性開源儲存系統與技術解決方案：
------------------------------
## 1. 塊儲存 (Block Storage) — SPDK 的絕對主場
塊儲存是 SPDK 應用最成熟、原生支援度最高的領域。不論是提供虛擬機使用的磁碟（vhost），或是跨網路的塊儲存（NVMe-oF），SPDK 都有著主導性的地位。 [3, 4] 

* 
* [FastBlock (openEuler 社群)](https://www.openeuler.org/en/blog/20250121-fastblock/20250121-fastblock.html)：
* 技術原理：這是一個專為現代高速 NVMe/RDMA 網路設計的高性能開源分散式塊儲存系統。它徹底拋棄了傳統 Linux 的核心儲存棧與本地檔案系統，在儲存節點（OSD）的底層完全採用 SPDK Blobstore 來管理裸碟（Raw Disks）並儲存 Raft 日誌與資料。
   * 優勢：整個 I/O 從網絡接收（RDMA）到硬碟落盤全程走使用者空間（Kernel Bypass），無鎖、零複製，實現極低的延遲。 [5] 
* [UbiCloud / Simplyblock](https://www.ubicloud.com/blog/building-block-storage-for-cloud-with-spdk-non-replicated)：
* 技術原理：許多現代開源雲端架構（如替代 AWS 彈性塊儲存 EBS 的方案）會基於 SPDK 的 bdev (Block Device Layer) 與 NVMe/TCP Target 來撰寫控制面。例如 UbiCloud 就在其開源雲平台上使用 SPDK 自研了高性能、非複製/複製的雲端塊儲存系統。 [6, 7] 
* [Ceph (透過 Crimson / BlueStore SPDK 加速)](https://github.com/ceph/ceph)：
* 技術原理：Ceph 的新一代後端儲存引擎 BlueStore 支援使用 SPDK 來驅動底層的 NVMe 固態硬碟。此外，Ceph 正在開發的下一代極致效能 OSD 專案 Crimson，更是深度整合了 Seastar 框架與 SPDK，旨在打造完全無鎖、使用者空間運作的分散式塊儲存。
* 

------------------------------
## 2. 檔案儲存 (File Storage)
由於 SPDK 的設計初衷是繞過核心並進行「塊」層級的操作，它本身並沒有類似 Linux Ext4 或 XFS 的傳統複雜檔案系統。但在開源世界中，主要透過以下兩種架構來提供檔案儲存服務： [2, 8] 

* 
* [SPDK vhost-user-fs (加強型 Virtio-FS)](https://www.snia.org/educational-library/introduction-spdk-vhost-fuse-target-accelerate-file-access-vm-and-containers)：
* 技術原理：SPDK 社群官方實作了使用者空間的 vhost FUSE Target。它能讓虛擬機（如 QEMU/KVM）或輕量級容器（如 Kata Containers）直接透過 FUSE 協定，對接到宿主機上的 SPDK Blobfs（SPDK 自帶的輕量級簡易檔案系統）。
   * 優勢：大幅度縮短了虛擬化環境中檔案讀寫的 I/O 路徑，極適合用於高效能雲端原生容器的持久化檔案儲存。 [4] 
* [CurveFS (Netease 網易開源)](https://github.com/opencurve/curve)：
* 技術原理：Curve 是一個分散式儲存系統，其塊儲存（CurveBS）深度使用了 SPDK 進行效能優化。而建構在 CurveBS 之上的 CurveFS 分散式檔案系統，間接繼承了底層 SPDK 帶來的高吞吐量與低延遲特性，常被用於 AI 訓練等大數據檔案儲存場景。
* 

------------------------------
## 3. S3 物件儲存 (Object Storage)
傳統的 S3 物件儲存（例如 MinIO, Ceph RGW）通常是建構在標準的作業系統檔案系統（如 XFS）之上。為了追求速度，開源社群開始出現結合 SPDK 底層的高效 S3 解決方案： [9] 

* 
* [MinIO (結合 SPDK 概念優化或客製化版本)](https://github.com/minio/minio)：
* 技術原理：雖然標準的 MinIO 主要是用 Go 語言編寫並依賴核心 I/O，但在許多企業級的高性能客製化開源分支中（或結合 NVMe-oF 塊設備），會將 MinIO 的硬碟後端指向由 SPDK 所暴露出來的本地高性能 NVMe/TCP 區塊。
* 自研 S3 基礎架構（透過 SPDK Blobstore 實作）：
* 業界（如智慧雲科、各家大廠基礎架構組）普遍的開源實踐是：利用 SPDK Blobstore 作為扁平的、無階層式的底層物件索引管理，並在最前端用 Go 或 C++ 封裝一層 S3 API 解析器（處理 HTTP RESTful 請求、Bucket 與權限驗證）。這能將小檔案物件儲存的寫入延遲降低數倍。 [8, 9] 
* 

------------------------------
## 💡 總結對比表
如果您的專案需要全面涵蓋 S3、File、Block，目前開源界最常見的落地方式與軟體架構如下：

| 儲存類型 | 代表性開源/架構方案 | SPDK 在其中扮演的角色 | 適用場景 |
|---|---|---|---|
| Block | FastBlock, Ceph Crimson, UbiCloud | 直接作為裸碟驅動與本地 I/O 管理（Blobstore） | 虛擬機雲硬碟、高性能 K8s 雲原生塊儲存 |
| File | SPDK vhost-fuse, CurveFS | 透過 Blobfs 提供 FUSE 介面，優化虛擬化檔案系統路徑 | Kata Containers 容器共用檔案、AI 訓練集存取 |
| S3 Object | S3 API Gateway + SPDK Blobstore 後端 | 繞過 VFS，直接將物件以「Blob」形式寫入裸碟，無目錄結構負擔 | 高併發小檔案儲存、海量圖片/影片極速讀寫 |

您目前是希望尋找一個現成、開箱即用的整合型分布式儲存系統（如 Ceph / FastBlock），還是您打算自己基於 SPDK 函式庫去開發一套客製化的儲存產品呢？我可以為您提供更深入的架構設計建議或程式碼範例。

[1] [https://spdk.io](https://spdk.io/cn/)
[2] [https://horeb.top](https://horeb.top/posts/linux-spdk-%E5%AE%9E%E6%88%98%E7%94%A8%E6%88%B7%E6%80%81-nvme-%E9%A9%B1%E5%8A%A8%E4%B8%8E%E9%AB%98%E6%80%A7%E8%83%BD%E5%AD%98%E5%82%A8/)
[3] [https://github.com](https://github.com/spdk/spdk)
[4] [https://www.snia.org](https://www.snia.org/educational-library/introduction-spdk-vhost-fuse-target-accelerate-file-access-vm-and-containers)
[5] [https://www.openeuler.org](https://www.openeuler.org/en/blog/20250121-fastblock/20250121-fastblock.html)
[6] [https://www.ubicloud.com](https://www.ubicloud.com/blog/building-block-storage-for-cloud-with-spdk-non-replicated)
[7] [https://simplyblock.io](https://simplyblock.io/opensource/)
[8] [https://zhuanlan.zhihu.com](https://zhuanlan.zhihu.com/p/646710218)
[9] [https://github.com](https://github.com/UnknownObject777/system-design-notes-zh/blob/main/24.%20S3-like%20Object%20Storage/README.zh-CN.md)
