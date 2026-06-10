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
④ SPCS container:
   ├── 4 concurrent Online FS REST lookups    (~10-15ms, entity velocity features)
   ├── Inline derived feature computation     (~1ms)
   └── XGBoost inference                      (~105ms)
  │
  ▼
⑤ Decision: approve / flag / block            (~130ms total p50)
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
│  │ CPU_X64_XS (0.06 credits/hr per node)    │  ~105ms inference  │
│  │ MIN=1, MAX=2 nodes                        │  ~$198/month       │
│  └─────────────────────────────────────────┘                     │
│                                                                   │
│  Online Feature Store (24/7)                                     │
│  ┌─────────────────────────────────────────┐                     │
│  │ Managed Postgres (Online Service)        │                     │
│  │ ~$200-500/month (instance-based)         │  ~10-15ms lookups  │
│  │ REST ingest + query endpoints            │  < 2s freshness    │
│  └─────────────────────────────────────────┘                     │
│                                                                   │
│  NOTE: No DT warehouse required. The Online Feature Store        │
│  replaces the 24/7 DT pipeline entirely for feature serving.     │
│  Total: ~$400-700/month vs $13,388/month with Dynamic Tables.    │
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
| Feature freshness < 2s | CONTINUOUS stream aggregation in Online FS |
| End-to-end latency ~130ms | Online FS REST lookup (~15ms) + XGBoost inference (~105ms) + PrivateLink (~15ms) |
| No public exposure | PrivateLink + private SPCS endpoint |
| Compliance (PCI/FCA) | No data leaves Snowflake's network; full inference audit log |
| Scalability | SPCS auto-scales 1-2 nodes; Online FS scales horizontally |
| Model lifecycle | Snowflake ML Registry: versioning, monitoring, rollback |
| Cost | ~$400-700/month total (no 24/7 DT warehouse) |

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
