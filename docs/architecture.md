# Architecture: Fraud Detection — Online Feature Store Solution

## End-to-End Data Flow

```
Customer taps card
  │
  ▼
① Transaction lands in Snowflake              (~sub-second, Snowpipe Streaming)
  │
  ├──► FRAUD_TRANSACTIONS table               (persistence, audit, training)
  │
  └──► Online Feature Store REST Ingest API   (real-time velocity update)
              │
              ▼
② Continuous aggregation updates velocity     (< 2 seconds end-to-end)
   features in Postgres-backed online store
  │
  ▼
③ Scoring request arrives
   (payment gateway → API Gateway → PrivateLink → SPCS)
  │
  ▼
④ SPCS container (optimised scoring path):
   ├── ONE SQL join across 4 Online Feature Tables  (~10ms p50)
   │   customer + merchant + DPAN + IP velocity in 1 round-trip
   │   (vs old: 4 concurrent SDK calls = 4 round-trips)
   ├── Customer profile features from in-memory cache (~0ms)
   ├── Inline derived feature computation             (~0.1ms)
   └── XGBoost inference                              (~5ms p50)
  │
  ▼
⑤ Decision: approve / flag / block            (~17ms total p50)
  │
  └──► Prediction logged to INFERENCE_LOG
```

## Feature Architecture

```
FRAUD_TRANSACTIONS table (source of truth)
      │
      ├──► Online Feature Store (stream feature views, CONTINUOUS aggregation)
      │         ├── FRAUD_CUSTOMER_VELOCITY_STREAM   (counts, sums, maxes — 1h/6h/24h/48h/1wk)
      │         ├── FRAUD_MERCHANT_VELOCITY_STREAM   (counts, sums, approx distinct customers)
      │         ├── FRAUD_DPAN_VELOCITY_STREAM        (counts, sums, approx distinct customers)
      │         └── FRAUD_IP_VELOCITY_STREAM          (counts, sums, approx distinct customers)
      │
      ├──► Online Feature Store (batch feature view, daily refresh)
      │         └── FRAUD_CUSTOMER_PROFILE_ONLINE     (lifetime stats, account age, KYC)
      │
      └──► Training dataset generation (window aggregations at training time, no DT needed)
```

## Feature Freshness by Type

| Feature type | Freshness | How computed | Where served |
|---|---|---|---|
| Velocity aggregates (counts, sums, maxes) | < 2 seconds | CONTINUOUS stream aggregation | Online FS REST |
| Approx distinct counts | < 2 seconds | HyperLogLog (~6.5% RSE) | Online FS REST |
| Derived ratios (velocity ratios, concentration) | At scoring time | Inline in SPCS container | N/A |
| Customer profile (lifetime stats) | Daily | DT-backed batch online FV | Online FS REST |

## Warehouse Strategy

```
┌─────────────────────────────────────────────────────────────────┐
│                       COMPUTE RESOURCES                           │
├─────────────────────────────────────────────────────────────────┤
│                                                                   │
│  Data Generation (one-time)                                       │
│  ┌─────────────────────────────────────────┐                     │
│  │ FRAUD_OFS_LOAD_WH                        │                     │
│  │ Standard LARGE (8 credits/hr)            │  12M rows ~3 min   │
│  │ AUTO_SUSPEND = 60s, INITIALLY_SUSPENDED  │  ~0.4 credits total│
│  └─────────────────────────────────────────┘                     │
│                                                                   │
│  Scoring path feature reads (24/7)                               │
│  ┌─────────────────────────────────────────┐                     │
│  │ FRAUD_OFS_SCORE_WH                       │                     │
│  │ Standard XSMALL (0.5 credits/hr)         │  ~10ms lookups      │
│  │ ALWAYS_ON (no AUTO_SUSPEND on hot path)  │  dedicated, no      │
│  │ Dedicated to scoring only                │  training contention │
│  └─────────────────────────────────────────┘                     │
│                                                                   │
│  ML Training (periodic, ~5 min/month)                            │
│  ┌─────────────────────────────────────────┐                     │
│  │ FRAUD_OFS_TRAIN_WH                       │                     │
│  │ Snowpark-Optimized MEDIUM (6 credits/hr) │  256GB dedicated   │
│  │ AUTO_SUSPEND = 60s, INITIALLY_SUSPENDED  │  ~0.5 credits/run  │
│  └─────────────────────────────────────────┘                     │
│                                                                   │
│  Model Serving (24/7)                                            │
│  ┌─────────────────────────────────────────┐                     │
│  │ FRAUD_OFS_CPU_POOL                       │                     │
│  │ CPU_X64_XS (0.06 credits/hr per node)    │  ~5ms inference    │
│  │ MIN=1, MAX=2 nodes                        │                    │
│  └─────────────────────────────────────────┘                     │
│                                                                   │
│  Online Feature Store (24/7)                                     │
│  ┌─────────────────────────────────────────┐                     │
│  │ Managed Postgres (Online Service)        │                     │
│  │ Pricing driven by: entity cardinality,   │  ~10-15ms lookups  │
│  │ feature view count, ingest rate,         │  < 2s freshness    │
│  │ time-window depth                        │                    │
│  │ REST ingest + query endpoints            │                    │
│  └─────────────────────────────────────────┘                     │
│                                                                   │
│  NOTE: No DT warehouse required. The Online Feature Store        │
│  provides Hybrid Table-backed online serving via the Feature      │
│  Store SDK. FRAUD_OFS_SCORE_WH is dedicated to scoring reads.    │
│                                                                   │
└─────────────────────────────────────────────────────────────────┘
```

## Production Deployment

```
Payment Gateway (Zilch backend)
      │
      ▼
AWS API Gateway
(auth, WAF, rate-limiting, CloudTrail)
      │
      ▼  (AWS PrivateLink — no public internet)
      │
SPCS Scoring Container (FRAUD_OFS_CPU_POOL)
      │
      ├── REST GET → Online FS (4 entity velocity lookups, concurrent)
      │               └── Postgres-backed, PrivateLink URL
      ├── Inline derived feature computation
      └── XGBoost.predict(147 features)
      │
      ▼
Fraud probability → approve / flag / block
      │
      └──► Inference log (FRAUD_DEMO_PROD.MONITORING.INFERENCE_LOG)
```

| Requirement | How it's met |
|---|---|
| Feature freshness < 2s | CONTINUOUS stream aggregation in Online FS (measured: ~280ms) |
| End-to-end latency ~17ms p50 | Online FS REST lookup (~12ms) + XGBoost inference (~5ms) via SPCS internal mesh |
| No public exposure | PrivateLink + private SPCS endpoint |
| Compliance (PCI/FCA) | No data leaves Snowflake's network; full inference audit log |
| Scalability | SPCS auto-scales 1-2 nodes; Online FS scales horizontally |
| Model lifecycle | Snowflake ML Registry: versioning, monitoring, rollback |
| Cost | Pricing depends on entity cardinality, feature view count, ingest rate, and time-window depth (no 24/7 DT warehouse) |

### Latency Note: Internal Mesh vs PrivateLink

The benchmarked latencies above are measured over the **SPCS internal service mesh**
(`svc.spcs.internal`) — the scoring container, OFS, and notebook all run within the
same Snowflake SPCS cluster. This is the recommended production topology: the scoring
service runs inside SPCS and communicates with the Online Feature Store over the
internal network.

The only PrivateLink hop in production is the **inbound call** from the customer's
payment gateway (via AWS API Gateway) into the SPCS scoring container:

```
Customer VPC                              Snowflake SPCS cluster
─────────────                             ──────────────────────
API Gateway ──► PrivateLink (+3-8ms) ──► Scoring Container ──► OFS (internal, ~12ms)
                                                             └──► XGBoost (local, ~5ms)
```

| Network path | Added latency | When it applies |
|---|---|---|
| SPCS internal mesh | 0ms (baseline) | OFS lookups, model inference (all inside SPCS) |
| PrivateLink (same AZ) | +3-5ms | Inbound call from API Gateway to SPCS |
| PrivateLink (cross-AZ) | +5-8ms | If caller is in a different AZ |
| TLS handshake | +2-5ms (first req only) | Amortized to ~0ms with connection pooling |

**Production estimate:** ~20-25ms p50 total (17ms internal + 3-8ms PrivateLink inbound).

To minimise PrivateLink overhead:
- Deploy the caller (API Gateway / Lambda) in the **same AZ** as the Snowflake account
- Use **persistent HTTP connections** (connection pooling) to avoid repeated TLS handshakes
- Keep the scoring service **inside SPCS** so OFS lookups stay on the internal mesh

## Model Promotion

```
FRAUD_DEMO_DEV                    FRAUD_DEMO_STAGING            FRAUD_DEMO_PROD
├── TRANSACTIONS (source data)    ├── ML (validated models)      ├── ML (production models)
├── FEATURES (Online FS, profile) ├── FEATURES (clone for test)  ├── SERVING (SPCS endpoint)
├── ML (experiments, models)      └── MONITORING (test monitors) └── MONITORING (live monitors)
└── SERVING (dev endpoint)
```

`log_model()` in DEV → re-register in STAGING (validation) → re-register in PROD (serving).

## Production Deployment Checklist

- [ ] Set `SNOWFLAKE_PAT` in secrets manager (AWS Secrets Manager or equivalent)
- [ ] Configure AWS PrivateLink between customer VPC and Snowflake account
- [ ] Set up API Gateway with WAF rules and API key authentication
- [ ] Keep SPCS endpoint **private** (`ingress_enabled=False` in production)
- [ ] Set `min_instances=1` on compute pool to avoid cold-starts
- [ ] Implement retry logic on REST Ingest API calls (exponential backoff)
- [ ] Verify dual write path reliability — transactions table + ingest API both confirmed
- [ ] Test at 10x volume (600 txn/min) before go-live
- [ ] Verify 48h and 1wk window aggregations under full entity cardinality
- [ ] Configure Model Monitor alerts (NB05) for AUC-PR drift detection
- [ ] Caller (API Gateway / SPCS) in same AWS region as Snowflake account
- [ ] Set up PAT rotation before expiry
- [ ] Confirm Online Feature Store pricing with Snowflake account team (Preview)
