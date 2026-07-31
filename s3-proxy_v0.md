# S3 Abstraction Layer — Design & Implementation Spec

*Extends: Research / S3 Abstraction Layer (David Su, 2026-07-26). This document keeps the original framing but corrects Method 2 and re-scopes the estimate.*

---

## 0. Recommendations (decisions requested)

| # | Decision | Recommendation |
|---|---|---|
| D1 | nginx/HAProxy path rewriting as the mechanism | **Reject.** SigV4 signs the canonical URI *and* the `Host` header. Any rewrite → `403 SignatureDoesNotMatch`. The layer must terminate auth and re-sign. |
| D2 | Build from scratch vs adopt | **Adopt VersityGW** (Go, Apache-2.0, stateless, runtime-loadable backend plugins) and implement a `router` backend. Do **not** pick S3Proxy — its alias middleware maps bucket→bucket only, has no prefix support, and is static-config only. |
| D3 | `mc mv` for relocation | Bounded one-off only: < 10⁶ objects, versioning off, no Object Lock, prefix write-frozen. Never for compliance buckets. |
| D4 | Mapping store | etcd (authoritative, watch + revision) + in-process radix trie, epoch-fenced. |
| D5 | Namespace model | `(virtual bucket, prefix) → [{generation, backend, bucket, prefix}]`. Generations exist so a physical location can never be *forced* to disappear (see §11 Object Lock finding). |
| D5a | Cross-cluster listing algorithm | **Ordered interval map, not k-way heap merge.** Keyspaces across the two clusters are confirmed disjoint, so every key belongs to exactly one cluster and LIST is an ordered interval walk with one backend call in flight. See §7. Requires the disjointness invariant to be actively enforced (§7.4), not assumed. |
| D6 | Effort | 3–4 months / 2 people is correct for **rename-only** scope. With list merge it is ~1.5× light; with a migration engine ~2× light. See §14. |
| D7 | v1 scope cuts | `ListObjectVersions`, `ListMultipartUploads`, and cross-backend `CopyObject` return **501** on merged rules. Lifecycle managed out-of-band per backend. |

---

## 1. Problem statement

The current page conflates three requirements with very different costs. Separate them before estimating.

| ID | Requirement | Nature | Cost driver |
|----|-------------|--------|-------------|
| **R1** | **Namespace virtualization.** `s3://ds-lake/wat/2025/` must appear as `s3://fab-analytics/wat/2025/` with no byte movement. | Control plane | Low. Rule lookup + path rewrite. |
| **R2** | **List merge.** One virtual bucket/prefix backed by ≥2 physical locations, presented as one listing. | Data plane | High. Pagination, ordering, dedupe. |
| **R3** | **Backend migration.** Physically move a dataset (MinIO → SeaweedFS/Ceph) while clients keep one endpoint. | Data plane + orchestration | Highest. Versions, Object Lock, cutover correctness. |

R1 alone is a weekend of routing logic wrapped in three months of S3-protocol correctness work. R2 and R3 are where the schedule actually goes.

### Goals

- Single stable endpoint and virtual namespace for all consumers; physical layout becomes an operational detail.
- Relocation of a virtual prefix in O(1) time, independent of object count.
- No loss of version history, delete markers, retention state, or ETag stability.
- Added latency ≤ 1 ms p50 / ≤ 5 ms p99 on single-generation object operations.

### Non-goals (v1)

Cross-region replication, client-side caching, S3 Select, bucket notifications, cross-backend transactional writes, admin/ops APIs (`mc admin`, MinIO admin API, SeaweedFS filer API — operators keep direct backend access; consumers do not).

---

## 2. Why Method 1 (`mc mv`) is not a relocation strategy

`mc mv --recursive` = server-side `CopyObject` + `DeleteObject` per object, as the page notes. Quantifying why that is disqualifying:

- **Operation cost.** 2 API ops/object plus listing. 10⁹ objects at a sustained 20k ops/s = **27.8 h** of pure API time; at a more realistic 5k ops/s on a busy cluster = **4.6 days**. Wall-clock is unbounded because it is not resumable.
- **IO amplification.** Every byte is re-read through the EC decode path and re-written through encode. On 8+4 EC that is ~1.5× physical write for a logically zero-value operation.
- **Versioning (as the page says, correctly).** Only the current version moves. Non-current versions and delete markers stay under the old prefix, *and* the delete half of `mv` creates a **new delete marker** on the old prefix — so the old prefix is left in a worse state than before, not a clean one.
- **Object Lock.** Objects under COMPLIANCE retention cannot be deleted before expiry. The delete half fails, leaving two live copies and double billing, with no rollback.
- **ETag instability.** For objects originally uploaded via MPU, a server-side copy recomputes the ETag unless `UploadPartCopy` reproduces the exact part boundaries. Anything doing `If-Match`, ETag comparison, or `mc mirror` / `rclone check` afterwards reports a false mismatch across the whole dataset.
- **No consistency point.** A concurrent writer during the move silently loses objects — they land under the old prefix after the lister has passed it.

**Verdict:** acceptable for a one-off migration of a frozen, unversioned, unlocked prefix under ~10⁶ objects. Not a product feature.

---

## 3. Why Method 2 as written does not work

This is the substantive correction to the current page.

SigV4's canonical request is:

```
HTTPMethod \n CanonicalURI \n CanonicalQueryString \n
CanonicalHeaders \n SignedHeaders \n HashedPayload
```

`CanonicalURI` is the URI-encoded request path, and `host` is always in `SignedHeaders`. Therefore:

1. Rewriting `/mybucket/old-prefix/x` → `/mybucket/new-prefix/x` changes `CanonicalURI` → backend returns `403 SignatureDoesNotMatch`.
2. Rewriting the host (virtual-hosted style, `bucket.s3.internal` → `otherbucket.s3.internal`) changes the signed `host` header → same failure.
3. Presigned URLs carry the signature in the query string under the same canonicalization → also break, and **cannot** be re-signed on the user's behalf because the gateway does not hold the user's intent, only their key.

So a path-rewriting reverse proxy cannot serve authenticated S3 traffic. The layer must **verify the caller's SigV4 with a key it holds, rewrite, then re-sign toward the backend with its own credential.** That is a full S3 protocol server. nginx/HAProxy remain useful in front of it as L4/L7 load balancers and nothing more.

Two consequences that drive the rest of the design:

- **The gateway becomes the authorization enforcement point** (§5). It holds a privileged backend credential, so an authz bug is a full-bucket exposure, not a single-object one.
- **"If that layer already exists the cost is close to zero" is only true if the existing layer is an auth-terminating S3 gateway.** If the current S3 abstraction evaluation landed on VersityGW, that is genuinely close to true — the plugin seam is the right insertion point. If it landed on an nginx-based design, the cost is the full build.

---

## 4. Architecture

```
                        ┌──────────────── control plane ────────────────┐
                        │  mapping API (gRPC) ──► etcd (rules, epochs)   │
                        │        ▲                     │ watch            │
                        │        │                     ▼                  │
   clients ── L4 LB ──►─┼─ gateway pod × N  (stateless, Go)              │
   (boto3, mc,          │     · SigV4 verify → rewrite → re-sign          │
    rclone, s5cmd,      │     · radix trie mapping cache (atomic swap)    │
    Spark, internal)    │     · k-way list merge                          │
                        │     · admission control / audit log             │
                        └─────────────┬──────────────┬───────────────────┘
                                      ▼              ▼
                            MinIO (gen1, ro)   SeaweedFS / Ceph RGW (gen2, rw)
                                      ▲              ▲
                        migration engine (mover workers) ──┘
                        reads rules + delete journal; drives copy/verify
```

Backends are reachable **only** from gateway subnets. Consumers never hold backend credentials — otherwise the mapping is advisory and trivially bypassed.

### Request lifecycle (object GET)

1. TLS terminate; parse method, bucket, key, query.
2. Resolve caller → look up access key in credential store (cached, 60 s TTL).
3. Verify SigV4 (or presigned params); enforce ±15 min clock window.
4. Authorize against policy written on the **virtual** namespace. Deny by default.
5. Resolve `(virtual bucket, key)` via longest-prefix match on the trie → rule + epoch. Pin both for the request lifetime.
6. Rewrite to physical `(backend, bucket, key)` for the highest-priority readable generation. Post-rewrite assertion: physical key must still fall inside the tenant's permitted physical scope (confused-deputy guard).
7. Re-sign with that backend's service credential; issue request on a per-backend pooled connection.
8. On 404 with a multi-generation rule: consult the delete journal; if no tombstone, retry the next generation.
9. Stream the body through with a pooled buffer. Emit `x-amz-request-id` from the backend plus `x-pled-gw-request-id` and `x-pled-rule-id` for correlation. Audit-log `(caller, virtual key, physical key, ruleID, epoch, status)`.

---

## 5. Authentication and authorization

The gateway must possess the caller's secret to verify SigV4. Three options:

| Option | Mechanism | Assessment |
|---|---|---|
| A | Gateway owns the IAM surface; users hold gateway-issued keys only. Backend keys are per-backend service accounts held only by the gateway. | **Recommended.** Clean policy story, single enforcement point, no consumer knowledge of physical layout. |
| B | Gateway mirrors backend IAM, fetching/caching user secrets from the backend. | Couples the gateway to backend IAM internals; breaks the moment two backends have different user sets. |
| C | OIDC federation via `AssumeRoleWithWebIdentity`; gateway mints short-lived keys. | Good long-term target for human/CI access. Add in v2 alongside A. |

Secrets live in Vault (or KMS-wrapped in etcd), never in etcd plaintext. Rotate backend service credentials on a schedule; the gateway must tolerate two valid generations of backend key during rotation.

**Presigned URLs.** The gateway issues and validates its own presigned URLs over the virtual namespace. It must never hand a backend-presigned URL to an end user — that leaks the physical layout and grants the scope of a privileged service account.

**Threat model note.** Because the gateway re-signs with a broad backend credential, the blast radius of an authz bug is the whole backend namespace. Mandatory controls: deny-by-default policy evaluation, the §4 step-6 post-rewrite scope assertion, a per-tenant physical prefix allowlist independent of the mapping rules, and an immutable audit log. Book a security review before Phase 2 of rollout, not after.

---

## 6. Namespace mapping model

```yaml
rule:
  id: r-00317
  virtual: { bucket: fab-analytics, prefix: "wat/2025/" }
  targets:
    - { gen: 2, backend: seaweed-a, bucket: pled-cold, prefix: "wat/2025/", mode: rw }
    - { gen: 1, backend: minio-p1,  bucket: ds-lake,   prefix: "wat/2025/", mode: ro }
  read_order: [2, 1]        # newest generation first; fall through on 404
  list: merge               # none | merge
  epoch: 41
  state: ACTIVE             # DRAFT | SHADOW | ACTIVE | DRAINING | RETIRED
```

**Resolution.** Longest-prefix match over `(virtual bucket, key)` in an immutable radix trie, O(len(key)). Rule count is O(10³–10⁴), so the whole map is < 10 MB resident. Ambiguity rule: longest match wins; equal-length matches are a config error rejected at admission.

**Single-generation rules (R1)** are the common case: pure rename, no fallback, no merge, no measurable overhead beyond the rewrite. Optimize for this path.

**Multi-generation rules (R3)** read the newest generation first and fall through on `NoSuchKey`; writes always go to the highest `rw` generation. This needs **whiteouts**, or a delete during the migration window resurrects the gen-1 object on the next read:

| Whiteout approach | Assessment |
|---|---|
| 0-byte `.wh.<key>` marker object in gen 2 | Cheap, but pollutes the namespace and every listing must filter it. Leaks into consumer-visible data. |
| **Delete journal in the control plane**, keyed by `(ruleID, key)`, TTL = migration duration | **Recommended.** Bounded lifetime, no namespace pollution, and the migration engine consumes it as its authoritative delete list. Consulted only on the 404 path, so the hot path is unaffected. |

**Mapping-change atomicity.** Two-phase with epoch fencing:

1. Publish rule at epoch `E+1` in state `SHADOW`.
2. Every gateway acks the new epoch via an etcd lease. Promoter waits for full quorum ack or times out and aborts.
3. Promote to `ACTIVE`. Requests that already resolved at epoch ≤ `E` finish against their pinned generation. Multipart uploads pin their epoch inside the `uploadId` (§8.1) and therefore survive arbitrarily long cutovers.

This gives **per-request consistency, not linearizability across the cutover instant**: a client that PUTs and immediately LISTs across the flip may observe the object under either target. Document this explicitly. Tenants needing more get a write freeze for the ~1 s promotion window.

### Operation semantics by rule type

| Operation | Single generation | Merged / multi-generation |
|---|---|---|
| GET / HEAD | Rewrite, forward | Newest gen first, journal-checked 404 fallthrough |
| PUT / POST | Rewrite, forward | Highest `rw` gen only |
| DELETE | Rewrite, forward | Delete in `rw` gen + write journal tombstone |
| ListObjectsV2 | Rewrite prefix, rewrite response keys | k-way merge (§7) |
| ListObjectVersions | Rewrite, forward | **501 in v1** |
| MPU | uploadId encoded | uploadId encoded with pinned gen + epoch |
| CopyObject | Same backend: server-side. | Cross-backend: **501 in v1**, use migration engine |
| Bucket-level config | Passthrough (read-only) | **501 in v1** |

---

## 7. List merge across clusters — interval map, not heap merge

**Confirmed constraint: the two clusters hold disjoint keyspaces for the shared bucket.** That is a large simplification and it changes the algorithm. Do not build the general k-way heap merge.

`ListObjectsV2` must still return keys in **UTF-8 byte lexicographic order**, honoring `prefix`, `delimiter`, `start-after`, `max-keys` (≤ 1000), `encoding-type`, with an opaque `NextContinuationToken`.

### 7.1 Model the namespace as an ordered set of disjoint key intervals

```
intervals (sorted, non-overlapping, half-open [lo, hi) on the virtual key):
  ["fab12/", "fab13")  -> cluster-a
  ["fab14/", "fab15")  -> cluster-b
  ["fab16/", "fab17")  -> cluster-a
```

Every key belongs to exactly one interval, therefore to exactly one cluster. Consequences:

- **Point operations (GET/PUT/HEAD/DELETE) have zero fan-out.** Interval lookup is a binary search over an O(10²) sorted array, then a single backend call. Even the 404 path costs one call, not two.
- **LIST becomes an ordered interval walk, not a merge.** Find the first interval intersecting `[max(prefix, start-after), prefix+0xFF…)`, drain it, advance to the next intersecting interval. **One backend LIST in flight at a time.** No min-heap, no parallel fan-out, no 2× list load, no cross-target dedupe of keys.
- **Most LIST requests touch exactly one interval.** Any request whose `prefix` is at or below an interval boundary is served by a single cluster with the same cost as today. Only prefixes above a boundary (including the empty prefix) walk more than one interval.

This is strictly better than the heap merge on every axis and is roughly 40% of the implementation cost (see §14).

### 7.2 Continuation token

```
base64url( protobuf {
  uint8   version
  uint64  epoch            // interval-map epoch
  uint16  interval_idx     // which interval the walk is inside
  bytes   backend_token    // that cluster's native ContinuationToken
  bool    at_interval_end
  bytes   hmac_sha256_16   // rotating key, covers all preceding fields
} )
```

Simpler than the general case: one cursor, not one per target. HMAC is still not optional — an unsigned token lets a client forge a backend token, probe physical layout, or pin a retired epoch. A token whose epoch no longer exists must fail `InvalidArgument`; never silently fall back to a newer map, which would skip or duplicate keys mid-pagination.

### 7.3 Two traps that disjointness does **not** remove

**(a) `CommonPrefixes` can still collide.** Object-level disjointness does not imply prefix-level disjointness. If `fab12/2025/` lives on cluster-a and `fab12/2026/` on cluster-b, then a request with `prefix=""` and `delimiter="/"` produces the `CommonPrefix` `fab12/` from *both* intervals. Rule: **dedupe `CommonPrefixes` whenever an interval boundary is deeper than the query's delimiter depth.** Because output is ordered, duplicates are adjacent, so a one-element lookback is sufficient — no set, no global state. Detect the "boundary deeper than delimiter depth" condition at map-admission time and mark the interval, so the hot path only pays for it when it applies.

**(b) Short pages.** When an interval is exhausted mid-page, the walk must continue into the next interval to fill up to `max-keys`. Returning a short page with `IsTruncated=true` is legal S3 and correct clients handle it — but a meaningful minority of internal tooling treats a short page as end-of-listing and silently truncates results. Fill the page. One extra backend call per interval crossing is cheap insurance against a silent-data-loss class of bug.

### 7.4 The disjointness invariant must be enforced, not assumed

The whole design rests on disjointness. If it is ever violated — someone writes the same key to both clusters out of band — the gateway serves one copy and **silently hides the other**, with no error anywhere. Under a no-data-loss mandate that is unacceptable as an untested assumption.

Two controls, both cheap:

1. **Write-path sampling.** On 1% of PUTs, issue a `HeadObject` against the non-owning cluster for the same key. Non-404 → alert, and record to a violations table. Cost at 100k req/s: ~1k extra HEADs/s spread across clusters, negligible.
2. **Background sweeper.** Walk each interval on the *non-owning* cluster (a LIST that should always return empty) on a rolling schedule. Any key found is a violation. Full sweep cost = one full LIST of each cluster per cycle; run it weekly, or continuously at low rate.

Alert loudly, page on any hit, and treat a violation as a correctness incident rather than a warning. This is the single control most likely to matter two years from now.

### 7.5 Fallback: when disjointness is not interval-expressible

If the partition cannot be written as a bounded set of key ranges — for example the split is by hash, or it is disjoint only as an accident of history — then interval lookup fails and you are pushed toward per-key placement state. **Do not go there.** One row per object is the s3-orchestrator failure mode: it recreates the metadata ceiling you are trying to avoid and makes the placement database a Tier-0, no-data-loss dependency.

Options in preference order: (1) reorganize keys once so the partition becomes prefix-expressible; (2) accept a bounded probe — route on the interval map where possible, and for the residual keys probe both clusters in parallel on read, at the cost of 2× HEAD/GET-404 load; (3) only then consider the general heap merge from the previous revision of this document. Establish which case applies before committing to the §14 estimate.

### 7.6 Still 501 in v1

**`ListObjectVersions`** — ordering is key ASC then version by last-modified DESC, with two markers (`NextKeyMarker` + `NextVersionIdMarker`) and delete markers interleaved. The interval walk does extend to it, but the two-marker cursor and version ordering are enough extra surface to defer. Serve it for single-interval requests; 501 when a request would cross an interval boundary. Same for `ListMultipartUploads`.

---

## 8. S3 protocol details the implementation will get wrong

Ordered by how much schedule they cost in practice.

**8.1 Multipart upload.** `uploadId` is backend-opaque and the gateway is stateless, so it must be self-describing:

```
base64url( ver | epoch | ruleID | targetGen | backendUploadId | hmac )
```

Keep the encoded form under ~512 B — the spec sets no limit but clients and downstream databases assume "reasonably short." `AbortMultipartUpload` must be idempotent. `ListMultipartUploads` on merged rules → 501 in v1.

**8.2 ETag stability.** Pass part ETags through unmodified and never repack parts. The MPU final ETag `md5(concat(md5(part_i)))-N` must come from the backend, never be recomputed by the gateway.

**8.3 Chunked encoding and checksums — budget real time for this.** Current AWS SDKs default to CRC32 checksums with `Content-Encoding: aws-chunked` and `x-amz-trailer`, sending `x-amz-content-sha256: STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER`. A gateway that does not parse chunk-signed bodies incrementally produces the classic failure mode: works with `mc`, fails with boto3. Decision: **de-chunk, verify chunk signatures incrementally, forward with `UNSIGNED-PAYLOAD` over TLS plus the client's explicit `x-amz-checksum-*` header** so the backend still verifies integrity end-to-end. This is one HMAC per request rather than one per 64 KiB chunk.

**8.4 Payload hash verification.** If a client sends a real hex `x-amz-content-sha256`, the gateway must hash the entire body to verify. Stream-hash and fail late with a 400 (as S3 does); never buffer to disk. SHA-256 runs ~1.5–2 GB/s per core with SHA-NI, so saturating 25 GbE (~3.1 GB/s) costs ~2 cores in hashing alone. Budget it.

**8.5 CopyObject.** Rewrite `x-amz-copy-source` as well as the destination. Same-backend copies stay server-side. Cross-backend copies cannot be server-side — they become read+write through the gateway (2× network) and need `UploadPartCopy` chunking above 5 GiB. → 501 in v1; that is what the migration engine is for.

**8.6 Conditional requests and ranges.** Pass `If-Match`, `If-None-Match`, `If-Modified-Since`, `If-Unmodified-Since`, `Range`, and `partNumber` through untouched. Never synthesize 304/412 in the gateway — the backend's view is authoritative and divergence here is very hard to debug.

**8.7 Version-ID routing.** Version IDs are backend-generated and valid only within their generation. A raw passthrough silently 404s after a migration. Encode the generation into the version ID surfaced to clients (same envelope pattern as `uploadId`), or restrict version-ID operations to single-generation rules.

**8.8 Bucket-level configuration.** Versioning, lifecycle, policy, replication, notification, tagging, Object Lock, CORS: for virtual buckets these are gateway-owned and need durable storage. VersityGW's `--meta-bucket` is the established pattern — worth noting that without it, ACL/policy/CORS writes are silently discarded and do not survive a restart, which is exactly the kind of quiet failure to catch in review. Lifecycle is the sharpest edge: an expiry rule on a virtual prefix must be translated into per-target rules on physical prefixes and **re-translated on every mapping change**. v1: manage lifecycle out-of-band per backend and return 501 on the virtual bucket. Say so loudly in consumer docs.

**8.9 Error mapping.** Preserve backend error codes verbatim. Critically, never convert a backend 503 into a 500 — SDK retry policies back off on 503 and give up on 500, so this one-line mistake turns a transient backend blip into a consumer-visible outage.

---

## 9. Data-plane implementation notes

- **Runtime.** Go, `net/http` or Fiber (VersityGW uses Fiber). One `http.Transport` per backend, `MaxIdleConnsPerHost ≥ 256`. Set `ForceAttemptHTTP2=false` for S3 backends — HTTP/2 flow-control windows throttle large-object throughput; HTTP/1.1 keep-alive across many connections measures faster.
- **Streaming.** `io.CopyBuffer` with a `sync.Pool` of 64–256 KiB buffers. Never `ReadAll` a body; never spill to disk. TLS on both sides rules out `sendfile`/`splice` — accept the userspace copy.
- **Memory.** Hard-cap in-flight bodies with a semaphore sized `cores × 64`; 256 KiB × 4096 = 1 GiB worst case. Set `GOMEMLIMIT` to ~80% of the container limit and `GOGC=200` to cut GC CPU on high-QPS small-object paths.
- **Timeouts.** Client: header 10 s, idle 65 s (must exceed the LB's 60 s idle or you get phantom resets). Backend: dial 1 s, response-header 5 s, per-request deadline as a function of declared size, not a constant. Retry only idempotent operations, once, on a fresh connection; retry PUT only when a checksum accompanies it.
- **Mapping cache.** Immutable radix trie behind an `atomic.Pointer`, rebuilt on etcd watch, lock-free swap. Staleness policy must be explicit: if the watch is broken > 30 s, continue serving reads from the last known map (fail-open) but reject writes against `DRAINING` rules (fail-closed). Emit a loud metric.
- **Admission control.** Per-tenant token buckets on both QPS and bytes/s, with `503` + `Retry-After` on shed. Without this, one crawler issuing merged LISTs will saturate the backend and the gateway will be blamed.
- **Observability.** Per-rule and per-backend RED metrics; a histogram of generation-fallthrough rate (the leading indicator that a migration is stalled); token-rejection counter; audit log to a separate immutable sink.

---

## 10. Capacity and SLO model

Overhead added by the gateway (targets, to be validated with `warp` + `mdchurn`):

| Path | added p50 | added p99 |
|---|---|---|
| GET/HEAD small, single generation | 0.3–0.8 ms | 3–5 ms |
| PUT 4 KiB | 0.4–1.0 ms | 5–8 ms |
| GET large (throughput) | — | 5–10% throughput loss |
| ListObjectsV2, single generation | 0.2 ms | 2 ms |
| ListObjectsV2, merged k=2 | +1 backend RTT (parallel) | +~0.5 ms per 1000 keys (heap + dedupe) |

**Sizing.** Budget 60–100 µs CPU per small-object request: TLS record processing, header parse, SigV4 verify (5 HMAC-SHA256 derivations ≈ 2–5 µs), re-sign, rewrite, buffer copy. A 16 vCPU node sustains ~25–40k req/s at 70% CPU with TLS session resumption; **plan at 15k req/s per node** for headroom. A 100k req/s LOSF target → 8 nodes + 2 for N+2. These are planning numbers, not measurements — the differential harness in §12 should produce real ones before any capacity commitment.

---

## 11. Migration engine (R3 only)

Per-prefix state machine: `PLAN → SCAN → COPY → VERIFY → SHADOW (dual-read) → CUTOVER → RECONCILE → RETIRE`.

**Version-aware copy.** Enumerate `ListObjectVersions` oldest→newest and replay: PUTs in order, delete markers as DELETEs. This reconstructs version *ordering* on the target, though version IDs necessarily differ — hence §8.7.

**Object Lock — surface this to Legal/Compliance in week 1.** Retention and legal holds must be re-applied via `PutObjectRetention`/`PutObjectLegalHold` after copy. But COMPLIANCE-mode objects **cannot be deleted from the source before retention expires**, by design. So for compliance buckets the source cannot be retired on the migration's schedule; it retires when retention expires, possibly years later. The gateway therefore keeps a permanent read-only gen-1 target. *This is the reason the design uses generations rather than a flat rename* — a rename model cannot express "this data legally has to stay where it is."

**Verify.** Compare size plus ETag where part layout is reproducible; otherwise request `x-amz-checksum-crc32c` on copy and compare that. Full-body SHA-256 on 100% of objects < 1 MiB and a 1% sample above. Record every decision in a durable manifest — the manifest is the audit artifact, not the logs.

**Throughput planning.** For 1 PB / 10⁹ objects: object count dominates. At 20k obj/s theoretical → 13.9 h; realistically 3–5× that with 32 mover workers → 2–3 days per 10⁹ objects. Bytes at 2 GB/s → 5.8 days. Take the max and plan **1–2 weeks per PB including verification**. Rate-limit both source read IOPS and destination write IOPS with leaky buckets; the migration must always lose to production traffic.

Everything must be idempotent and resumable at object granularity, with a final reconciliation pass driven by the delete journal.

---

## 12. Correctness and test strategy

**Highest-ROI item, build it first: differential testing.** Run identical operation sequences against (a) MinIO direct, (b) gateway → MinIO with identity mapping, (c) gateway → MinIO with a non-trivial prefix rule. Assert byte-identical XML responses modulo request IDs. This catches the large majority of protocol bugs for a few weeks of work and pays for itself repeatedly during the SDK long tail.

**SDK compatibility matrix** matters more than conformance suites, because chunked/checksum behaviour differs per SDK and per version: boto3, aws-cli v2, aws-sdk-java v2, aws-sdk-go-v2, aws-sdk-js v3, `mc`, `rclone`, `s5cmd`, plus whatever Spark/Presto connectors consumers actually use. Pin versions in CI and re-run on SDK bumps.

**Conformance suites.** Ceph `s3-tests` and MinIO `mint`, with an explicit allowlist of expected failures matching the §0 D7 scope cuts.

**Property-based interval-walk tests.** Reuse the `mdchurn` deterministic keyspace generator: generate a keyspace, partition it into random disjoint intervals across the two clusters, then assert that full pagination output equals `sorted(union)` for random `(prefix, delimiter, max-keys, start-after)`. Adversarial keys must be in the corpus: `/`, `//`, trailing `/`, UTF-8 multibyte, `%2F`, 1024-byte keys, and keys that differ only after byte 900. Adversarial *intervals* matter just as much: boundaries that fall mid-key, boundaries deeper than the query's delimiter depth (§7.3a), adjacent intervals on the same cluster, single-key intervals, and an interval whose entire content fits inside one page.

**Disjointness violation tests.** Deliberately write the same key to both clusters out of band, then assert that the write-path sampler and the background sweeper both detect it (§7.4). A control that has never been observed firing is not a control.

**Cutover chaos.** Continuous read/write/delete load while flipping epochs at 1 Hz. Assert: no lost object, no resurrected delete, no 5xx, no out-of-order pagination. This is where the design will actually break, so run it before writing the rollout plan.

**Fault injection.** Backend 503, slowloris, TCP reset mid-body; etcd partition; clock skew ±10 min (SigV4's window is 15 min); credential rotation mid-request.

**Acceptance gates before fanout.** Zero conformance regressions against direct MinIO on the supported API subset; p99 added latency < 5 ms; 72 h soak at 1.5× peak with zero integrity errors; a published, consumer-visible list of 501s.

---

## 13. Rollout

| Phase | Content | Exit criterion |
|---|---|---|
| 0 | Shadow: mirror 1% of read traffic, compare responses offline. No client change. | < 0.01% response divergence |
| 1 | Read-only tenants on identity mapping | 2 weeks clean |
| 2 | One tenant with a real rename rule (R1) | Security review passed |
| 3 | One tenant on a merged-list rule (R2) | List load within budget |
| 4 | Fanout | — |

**Kill switch.** Repoint the endpoint DNS / LB target back to MinIO direct. This only works while identity mapping holds, so maintain an escape-hatch table of virtual→physical for *every* rule, so clients can be repointed manually after real renames exist. Keep it in the runbook, not only in etcd.

---

## 14. Revised effort estimate

| Workstream | Person-weeks |
|---|---|
| Gateway skeleton on VersityGW + `router` backend plugin | 3 |
| SigV4 verify/re-sign, credential store, presign | 4 |
| Mapping model + etcd control plane + epoch fencing | 4 |
| Prefix rewrite for object ops (GET/PUT/DELETE/HEAD/Copy) | 3 |
| MPU uploadId encoding + part paths | 3 |
| ListObjectsV2 interval walk + continuation token (§7) | 2 |
| Disjointness invariant sampler + sweeper (§7.4) | 1 |
| Bucket-level API + meta store | 3 |
| Differential + property-based test harness | 4 |
| SDK compatibility long tail (chunked, checksums, trailers) | 5 |
| Observability, admission control, perf tuning | 3 |
| Security review, threat model, audit logging | 2 |
| Docs, runbooks, rollout | 3 |
| Migration engine + Object Lock handling (R3 only) | 8 |

| Scope | Person-weeks | Calendar, 2 people |
|---|---|---|
| **MVP — R1 only** (rename, single interval, no cross-cluster listing) | ~22 | **~3 months** |
| **+ R2 cross-cluster listing over disjoint intervals** | ~29 | **~3.5 months** |
| **+ R3 migration engine and compliance handling** | ~47 | **~6 months** |

Three notes on the original *3–4 months for 2 people (1.5 dev, 1 test, 1 fanout)*:

- **Confirmed disjoint keyspaces bring the estimate back into range.** The interval-walk design (§7) is ~3 PW cheaper than a general k-way merge, so R1+R2 lands at ~3.5 months rather than ~4+. This is contingent on the partition being interval-expressible — if §7.5 applies, add back 4–6 PW.
- The dev/test split is a bigger problem than the total. **1 month of test is not enough**, because S3 SDK compatibility is a long tail, not a phase — every org that has built an S3 gateway measures it in months. Restructure as continuous differential testing from week 3 rather than a test phase at the end.
- R3 remains the item that breaks the schedule. If physical migration is in scope, say so now; it is not a stretch goal on top of R1+R2.

---

## 15. Risks and open questions

| Risk | Impact | Mitigation |
|---|---|---|
| Authz bug with a privileged backend credential | Full-namespace exposure | Post-rewrite scope assertion; per-tenant physical allowlist; security review at Phase 2 |
| SDK incompatibility discovered post-fanout | Consumer outage, credibility loss | SDK matrix in CI from week 3; Phase 0 shadow diffing |
| Merged LIST load saturates the backend | Latency regression for everyone | Admission control; page cache; k ≤ 2 policy |
| Gateway becomes a hard dependency with no viable rollback | Unbounded blast radius | Escape-hatch table; keep identity mapping valid as long as possible |
| Object Lock blocks source retirement | Migration never "finishes"; double cost persists | Generation model; engage Compliance in week 1 |
| Effort under-estimated → scope creep under schedule pressure | Correctness cut instead of scope | D7 scope cuts agreed **in writing** before coding |

**Open questions for consumers (blocking the design):**

1. Does any consumer use **presigned URLs**? (If yes, §5 becomes a v1 requirement, not v2.)
2. Is **Object Lock / retention** in use on any bucket in scope?
3. Largest object count under a single prefix that may be relocated?
4. Does anything call **ListObjectVersions** or **ListMultipartUploads**? (Determines whether D7's 501s are acceptable.)
5. Are **bucket notifications** or **lifecycle rules** in use today, and who owns them?
6. Who owns the IAM surface after this lands — the gateway team or the current MinIO owners?
7. Single region, single failure domain?

---

## Appendix A — API support matrix (v1)

| API | Single generation | Merged |
|---|---|---|
| GetObject, HeadObject, PutObject, DeleteObject, DeleteObjects | ✅ | ✅ |
| ListObjectsV2, ListObjects (v1) | ✅ | ✅ |
| CreateMultipartUpload, UploadPart, CompleteMPU, AbortMPU, ListParts | ✅ | ✅ |
| CopyObject, UploadPartCopy (same backend) | ✅ | ✅ |
| CopyObject (cross backend) | n/a | ❌ 501 |
| ListObjectVersions, GetObject?versionId | ✅ | ❌ 501 |
| ListMultipartUploads | ✅ | ❌ 501 |
| Object/bucket tagging, ACL | ✅ passthrough | ❌ 501 |
| Lifecycle, notification, replication, Object Lock config | ❌ 501 (out-of-band) | ❌ 501 |
| SelectObjectContent | ❌ 501 | ❌ 501 |
| Admin APIs (`mc admin`, MinIO admin, SeaweedFS filer) | not proxied | not proxied |

## Appendix B — Continuation token layout

```
token = base64url(proto)
proto {
  uint8   version          // bump on any layout change; reject unknown
  uint64  epoch            // rule epoch at page 1; mismatch → InvalidArgument
  string  rule_id
  repeated TargetCursor {
    uint8  gen
    oneof  { bytes backend_token; string last_virtual_key }
    bool   exhausted
  }
  bytes   hmac_sha256_16   // over all preceding fields, rotating key
}
```

Invariants: a token is valid only for the `(ruleID, epoch)` it was minted under; every field is covered by the HMAC; `version` is checked before any other parse to keep forward compatibility cheap.
