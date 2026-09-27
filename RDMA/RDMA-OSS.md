在分散式儲存與 AI 運算（如 GPU 叢集）的推動下，透過 RDMA（Remote Direct Memory Access，包含 InfiniBand 與 RoCE） 來實現極致的低延遲與高吞吐量已成為主流。
針對您詢問的 S3 物件儲存、Block 區塊儲存、以及 File 檔案儲存，以下是目前開源領域中最主流、成熟，或最具潛力的 RDMA 儲存解決方案：
------------------------------
## 1. 🪣 S3 Object Storage (物件儲存) 的 RDMA 開源方案
傳統 S3 依賴 HTTP/TCP 堆疊，對於需要餵飽 GPU 的 AI 訓練來說 CPU 開銷極大。目前專注於 S3 over RDMA 的開源項目主要有：

* 
* [Versity Gateway (versitygw)](https://github.com/versity/versitygw/wiki/S3-RDMA) (✅ 強烈推薦)
* 定位：這是一款極度受矚目的開源 S3 閘道器（Gateway）。其專屬分支 vgwrdma 原生支援 S3 over RDMA（基於 NVIDIA 的 cuObject 框架）。
   * 特點：它保持標準 S3 API 的控制面（走 HTTP/TCP），但將物件的資料面（Payload）傳輸完全轉向 RDMA 矩陣（Fabric）。資料可以直接零複製（Zero-copy）注入到運算節點的記憶體或 GPU 記憶體中。 [1, 2, 3, 4, 5, 6] 
* [RustFS](https://github.com/rustfs/rustfs)
* 定位：2026 年新一代針對 AI 與雲端優化的高效能 S3 相容物件儲存（採用 Rust 開發）。
   * 特點：深度整合 NVIDIA Inception 計畫，原生研發 RDMA 與 DPU 網路硬體卸載（如 Erasure Coding 糾刪碼與加密硬體加速），專門用來在小檔案場景下對標並替代傳統物件儲存。 [3, 7, 8] 
* 

------------------------------
## 2. 💽 Block Storage (區塊儲存) 的 RDMA 開源方案
區塊儲存是 RDMA 發展最成熟的領域，標準基本已定型於 NVMe-oF (NVMe over Fabrics)。

* 
* Linux 核心原生 NVMe-oF Target & Initiator (✅ 最成熟)
* 定位：Linux Kernel 原生自帶的核心模組（Linux 核心的國王級標準）。
   * 特點：直接支援 NVMe over RoCE / InfiniBand。您可以將 Linux 伺服器上的裸碟（NVMe SSD）透過核心直接以 RDMA 協定分享出去，客戶端掛載後在系統裡看起來就像本機的 /dev/nvmeXnY，效能幾乎等同於在地硬碟。 [9] 
* SPDK (Storage Performance Development Kit)
* 定位：Intel 主導的開源使用者空間（User-space）儲存框架。
   * 特點：內建極致效能的 NVMe-oF Target（支援 RDMA 傳輸）。它完全繞過 Linux 核心，採用 Poll-mode（輪詢）驅動。如果您想自行建構超高性能的自訂區塊儲存儲存池，SPDK 是最佳基礎底層。
* [DRBD 9](https://linbit.com/blog/rdma-what-it-means-for-data-transfer-replication/) (LINBIT)
* 定位：Linux 平台經典的高可用區塊複製網路儲存。
   * 特點：開源版本（DRBD 9.2+）全面開源了其 RDMA 傳輸核心模組。在超融合（Hyperconverged）架構中，多台節點之間的區塊資料同步複製可以完全走 RDMA，大幅解放 CPU 的複製開銷。 [10] 
* 

------------------------------
## 3. 📂 File Storage (檔案儲存/分散式文件系統) 的 RDMA 開源方案
這類解決方案主要提供 POSIX 接口（掛載成資料夾），且在 HPC（高效能運算）與 AI 訓練中極為常見：

* 
* [JuiceFS](https://juicefs.com/docs/community/comparison/juicefs_vs_3fs/)
* 定位：雲原生分散式檔案系統，底層使用「元數據引擎（如 Redis/TiKV）」+「對象儲存（如 S3）」的雲原生架構。
   * 特點：在網絡層，JuiceFS 提供對 RDMA 的基礎網路支援，讓運算節點在讀取檔案快取與元數據時能享有極低的網路延遲。 [11] 
* [GlusterFS](https://github.com/gluster/glusterfs) & Ceph
* 定位：傳統老牌的分散式儲存老大哥。
   * 特點：
   * GlusterFS：原生內建 rdma 傳輸類型（transport.type=rdma），可以讓它的分散式檔案掛載直接行經 InfiniBand/RoCE 網路。
      * Ceph (CephFS / RBD)：內部網路通訊模組（Async Messenger）雖然實驗性地支援過 RDMA 核心優化，但因維護難度高，目前生產環境依然建議以 100GbE/200GbE TCP 網路為主。若要在 Ceph 享用 RDMA，通常是用外部的 NVMe-oF Gateway 轉接。
   * 3FS (Bytedance Open Source)
* 定位：由字節跳動開源、專為 AI 訓練（如大模型）設計的分散式文件系統。
   * 特點：它採用高度客製化的 RPC over RDMA 理念作為骨幹通訊協議。整個檔案系統從設計之初就是為了壓榨 RoCE 網路與 NVMe 固態硬碟的極限而生，是目前 AI 圈內非常激進且高效的開源 POSIX 檔案儲存方案。 [11] 
* 

------------------------------
## 📊 直觀架構選擇對照表

| 儲存類型 | 推薦開源項目 | 傳輸技術底層 | 適用場景 |
|---|---|---|---|
| Object (S3) | Versity Gateway[](https://github.com/versity/versitygw) / RustFS | cuObject / 原生 RDMA 卸載 | AI 訓練集讀取、大物件零複製（Zero-copy）直達 GPU |
| Block | Linux Kernel (NVMe-oF) / SPDK | NVMe over Fabrics (RDMA) | 資料庫硬碟、虛擬機（KVM）雲硬碟、極致低延遲裸磁碟空間 |
| File | 3FS / JuiceFS | RPC over RDMA / RoCE 網路 | AI 大模型多機多卡訓練、POSIX 共用資料夾掛載 |

請問您目前正在設計的儲存架構，是希望同時滿足這三種儲存需求（打造一個統一的 RDMA 儲存平台），還是正專注於**特定某一類（例如：專門為 GPU 叢集提供 S3 數據加速）**呢？我可以為您針對特定的開源項目提供更深入的部署或整合細節。

[1] [https://infohub.delltechnologies.com](https://infohub.delltechnologies.com/en-us/p/accelerating-ai-workloads-with-rdma-for-s3-compatible-storage-a-game-changer-with-dell-objectscale/)
[2] [https://www.blocksandfiles.com](https://www.blocksandfiles.com/tape/2026/08/04/versity-says-tape-libraries-can-be-a-data-source-for-gpus-doing-ai-work/5282784)
[3] [https://daily.dev](https://daily.dev/posts/this-s3-alternative-is-insanely-lightweight-and-100-open-source--z86y6d6dj)
[4] [https://github.com](https://github.com/versity/versitygw/wiki/S3-RDMA)
[5] [https://www.vastdata.com](https://www.vastdata.com/blog/the-rise-of-s3-rdma)
[6] [https://www.snia.org](https://www.snia.org/sniadeveloper/session/19307)
[7] https://rustfs.com
[8] [https://github.com](https://github.com/rustfs/rustfs)
[9] [https://elements.tv](https://elements.tv/blog/an-introduction-to-rdma-remote-direct-memory-access/)
[10] [https://linbit.com](https://linbit.com/blog/rdma-what-it-means-for-data-transfer-replication/)
[11] [https://juicefs.com](https://juicefs.com/docs/community/comparison/juicefs_vs_3fs/)
