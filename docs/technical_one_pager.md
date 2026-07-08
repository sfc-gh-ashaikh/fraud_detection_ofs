# Real-Time Fraud Scoring on Snowflake — Technical One-Pager

## Executive Summary

Proof-of-concept demonstrating real-time payment fraud scoring on Snowflake, replacing a proposed AWS-native architecture (Kinesis + DynamoDB + SageMaker + NVIDIA Triton). The solution meets all success criteria: sub-50ms latency, sub-2-second feature freshness, and radically simpler operations.

---

## Architecture Comparison

### Current Proposed Architecture (AWS Native)

```
Card tap → Payment Backend → Kinesis Data Streams → Lambda/Flink processor
                                                         │
                                                         ├── DynamoDB (velocity features)
                                                         ├── S3 (feature snapshots)
                                                         └── Glue ETL (daily recompute)
                                                               │
                                                               ▼
                                                         SageMaker Endpoint
                                                         + NVIDIA Triton
                                                         + TensorRT
                                                               │
                                                               ▼
                                                         fraud-engine-service (EKS)
                                                               │
                                                               ▼
                                                         Visa/Mastercard response
```

**Components to build and operate:** Kinesis (provisioned shards), Lambda/Flink (event processing), DynamoDB (feature store, capacity planning), S3 + Glue (batch features), SageMaker (model hosting), Triton (GPU inference), EKS (orchestration), CloudWatch (monitoring), IAM (cross-service auth), CodePipeline (deployment).

**Known risk:** Engineering team identified real-time path complexity as a blocker — considering batch fallback for MVP (24-hour stale features).

---

### Snowflake Architecture (Proven in POC)

```
Card tap → Payment Backend → SPCS Scoring Container
                                    │
                                    ├── Feature Group query (1 call) → Online Feature Store
                                    │   └── CONTINUOUS aggregation (<300ms freshness)
                                    │
                                    └── XGBoost inference (in-container, ~1ms)
                                          │
                                          ▼
                                    Visa/Mastercard response
```

**Components to operate:** One Snowflake account. The Online Feature Store, model registry, compute pool, and scoring service are all managed within the same platform boundary.

---

## Operational Simplicity

| Dimension | AWS Native (Proposed) | Snowflake (Proven) |
|---|---|---|
| Services to manage | 10+ (Kinesis, Lambda, DynamoDB, S3, Glue, SageMaker, Triton, EKS, CloudWatch, IAM) | 1 account (OFS + SPCS + Model Registry) |
| Network boundaries | Cross-service IAM, VPC peering, security groups per service | Single VPC, internal mesh between all components |
| Credential surface | Per-service IAM roles, DynamoDB access keys, SageMaker endpoint auth | One SNOWFLAKE_TOKEN (auto-injected by platform) |
| Feature freshness pipeline | Kinesis → Lambda → DynamoDB (minutes) | CONTINUOUS stream aggregation (< 300ms) |
| Model deployment | S3 → SageMaker → Triton model repository → EKS rollout | Model Registry → SPCS CREATE SERVICE (one command) |
| Monitoring | CloudWatch + X-Ray + custom dashboards per service | Unified query history + SPCS service logs |
| Scaling | Per-service auto-scaling (shards, read capacity units, endpoints) | SPCS MAX_INSTANCES + OFS scales automatically |
| Blast radius of failure | Each service can fail independently; cascading failure modes | Single platform boundary; one health check |
| Time to production (estimated) | Months (team identified batch fallback for MVP) | Weeks (proven end-to-end in this POC) |

---

## Latency Results (Measured)

**Test setup:** 100 real transactions sampled from `FRAUD_DEMO_DEV.TRANSACTIONS.FRAUD_TRANSACTIONS` (12M row table of synthetic card payment data). Each transaction sent sequentially as a Thredd-format HTTP POST to the SPCS scoring container (`/score` endpoint). The container performs a Feature Group query to OFS over the internal SPCS mesh, computes derived features, and runs XGBoost inference — the identical path a production payment authorization would follow. 10 warm-up requests were sent first to establish connection pooling before measurement began.

### End-to-End Scoring Latency

| Component | p50 | p95 | p99 |
|---|---|---|---|
| OFS Feature Group lookup (1 call, all 5 FVs) | **10.1ms** | 10.9ms | 13.1ms |
| XGBoost inference | **1.0ms** | 1.1ms | 1.1ms |
| **Total end-to-end** | **11.2ms** | **12.3ms** | **14.1ms** |

### Production Path (over PrivateLink)

PrivateLink adds a fixed overhead for the inbound hop from the customer's API Gateway into the SPCS scoring container. Once inside SPCS, all internal communication (Feature Group query → OFS, model inference) stays on the internal mesh with zero additional overhead.

| Path | Estimated Latency |
|---|---|
| POC (internal mesh, as benchmarked) | 11.2ms p50 |
| + PrivateLink same AZ | +1-3ms |
| + PrivateLink cross-AZ | +3-5ms |
| **Realistic production p50** | **~14-16ms** |
| **EHI authorization budget** | **< 50ms** |
| **Headroom (worst case cross-AZ)** | **~34ms** |

*Note: First request after idle adds ~5-10ms for TLS handshake. Mitigated with persistent connection pooling on the caller side (standard practice for payment backends).*

### Comparison to AWS Target

| Metric | AWS (Proposed) | Snowflake (Measured) |
|---|---|---|
| Feature lookup | ~5-10ms (DynamoDB) | **10.1ms** (OFS Feature Group) |
| Model inference | ~10-20ms (Triton GPU) | **1.0ms** (XGBoost CPU) |
| Total p50 | ~25-40ms (estimated) | **11.2ms** (measured) |
| Feature freshness | Minutes (Kinesis batch) | **297ms** (CONTINUOUS) |

---

## Feature Freshness (Measured)

Single transaction ingested → velocity features updated across all 4 entity pipelines:

| Feature View | Time to Update |
|---|---|
| Customer velocity | 266ms |
| Merchant velocity | 274ms |
| DPAN velocity | 286ms |
| IP velocity | 297ms |
| **All pipelines complete** | **297ms** |

**Impact on fraud detection:** Card-testing attacks (5-10 rapid transactions in <30s) are visible from transaction 2 onwards. With batch features (minutes), the entire attack completes undetected.

---

## Success Criteria Summary

| Criteria | Target | Result | Status |
|---|---|---|---|
| End-to-end latency | < 50ms (EHI budget) | **11.2ms p50 internal, ~14-16ms over PrivateLink** | PASS |
| Feature freshness | < 2 seconds | **297ms** (all 4 pipelines) | PASS |
| Operational simplicity | Fewer moving parts | **1 platform vs 10+ services** | PASS |

---

## Platform Details

| Item | Value |
|---|---|
| Snowflake account | JB74519 |
| Region | AWS EU-WEST-1 (Ireland) |
| Compute pool | FRAUD_OFS_CPU_POOL (CPU_X64_XS, 1-2 instances) |
| Online Feature Store | Postgres-backed, CONTINUOUS aggregation |
| Feature views | 4 velocity (stream) + 1 profile (batch daily) |
| Feature Group | FRAUD_SCORING_FG (single-call, all 5 FVs) |
| Model | XGBoost, 46 features, ~80% recall at 0.05% fraud rate |
| Scoring service | FastAPI + persistent connection pool + dedicated ingest thread |

---

## Scaling to 10x Volume (Future-Proofing)

Current POC: ~60 txn/min. Production target at 10x: ~600 txn/min (~10 txn/sec).

### Compute Scaling

| Lever | Current (POC) | At 10x | How to Configure |
|---|---|---|---|
| SPCS instances | 1 (MIN) / 2 (MAX) | 3-5 instances | `ALTER SERVICE ... SET MIN_INSTANCES = 2, MAX_INSTANCES = 5` |
| Uvicorn workers per instance | 2 | 4-8 | Update CMD in Dockerfile: `--workers 8` |
| Compute pool size | CPU_X64_XS | CPU_X64_S or CPU_X64_M | `ALTER COMPUTE POOL ... SET INSTANCE_FAMILY = 'CPU_X64_S'` |
| Effective concurrency | ~4 requests in flight | ~20-40 requests in flight | Workers × instances |

SPCS auto-scales between MIN and MAX instances based on load. No manual intervention at runtime.

### Online Feature Store Scaling

| Lever | Current (POC) | At 10x | Notes |
|---|---|---|---|
| OFS service tier | Default | Default (auto-scales) | OFS is a managed service; scales horizontally without configuration |
| Ingest throughput | ~60 events/min | ~600 events/min | Well within OFS ingest limits (tested to 10K+ events/sec) |
| Entity cardinality | ~100K customers | ~1M+ customers | Point-lookup latency stays flat (indexed by primary key) |
| Feature view count | 5 FVs in 1 Feature Group | Same | Feature Group design is cardinality-independent |

### Network and Connection Scaling

| Lever | Current (POC) | At 10x | How to Configure |
|---|---|---|---|
| Connection pool size | 20 max connections | 50-100 | Update `HTTPAdapter(pool_maxsize=100)` in app.py |
| Ingest queue depth | 1000 | 5000 | Update `queue.Queue(maxsize=5000)` in app.py |
| Ingest thread count | 1 daemon thread | 2-3 threads | Spawn additional `_ingest_worker` threads at startup |
| API Gateway rate limit | N/A (POC) | 1000 req/sec burst | Configure on caller-side AWS API Gateway |

### Model and Feature Scaling

| Scenario | Approach |
|---|---|
| More features (100+) | Add feature views to the Feature Group; single-call pattern still applies |
| Multiple models (A/B testing) | Deploy parallel SPCS services behind a load balancer endpoint; traffic-split at API Gateway |
| Real-time model retraining | Snowflake Model Registry supports versioned promotion; swap model file on stage, restart service |
| Multi-region deployment | Deploy separate SPCS services in each region; OFS replicates via Snowflake replication |

### What Does NOT Need to Change at 10x

- **Feature Group query pattern** — single OFS call, regardless of volume
- **Scoring service architecture** — same `app.py`, same container image
- **Feature freshness** — CONTINUOUS aggregation scales independently of query volume
- **PrivateLink path** — fixed overhead, volume-independent
- **Model** — XGBoost inference at 1ms is CPU-bound per request, not affected by concurrency

### Scaling Runbook (When Ready)

```sql
-- 1. Scale compute pool
ALTER COMPUTE POOL FRAUD_OFS_CPU_POOL SET
  MIN_NODES = 2
  MAX_NODES = 5
  INSTANCE_FAMILY = 'CPU_X64_S';

-- 2. Scale service instances
ALTER SERVICE FRAUD_DEMO_PROD.ML.FRAUD_SCORING_SERVICE SET
  MIN_INSTANCES = 2
  MAX_INSTANCES = 5;
```

```dockerfile
# 3. Increase workers in Dockerfile
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8080", "--workers", "8"]
```

No code changes required to the scoring logic, feature pipeline, or model.

---

## Next Steps

1. **Load test at scale** — 600 txn/min (10x current volume) sustained for 1 hour
2. **Model integration** — Replace synthetic model with production-trained model on real transaction data
3. **Security review** — PCI DSS compliance assessment for the SPCS scoring path
4. **Cost modelling** — Detailed cost comparison at production transaction volumes
5. **Production deployment** — Promotion from POC account to production account with RBAC
