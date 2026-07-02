# Snowflake vs NVIDIA/AWS Native: Fraud Detection Architecture
## Customer Engineering Talking Points

> For use in technical sessions with engineering and architecture teams. This document covers the
> pre-feature engineering ingestion path, the payment gateway integration pattern, and a
> step-by-step architectural comparison against an NVIDIA Triton / AWS native stack.

---

## The Core Argument

Their architecture is a **coordination problem at every layer**: data coordination between Glue and
SageMaker, model coordination between training scalers and serving encoders, operational
coordination between Kinesis, DynamoDB, Triton, and the fraud-engine-service. Their own engineers
chose a batch fallback for MVP because the real-time path was too complex to stand up.

Snowflake collapses the coordination surface: one platform, one network boundary, one credential,
one place to monitor. The ~70 second feature freshness (vs 24-hour daily batch) is not a feature we
added — it is what you get when you remove the coordination layers that were causing the staleness.

---

## The Business Problem: Freshness Determines Fraud Hit Rate

Fraud losses are directly tied to how fresh your features are. The most damaging pattern at this
scale is **card testing**: fraudsters make 5-10 rapid transactions in under 30 seconds to validate
stolen credentials. The model's ability to catch these attacks depends entirely on one thing: can it
see the velocity spike in time?

| Feature Freshness | What the Model Sees | Outcome |
|---|---|---|
| 24 hours (daily batch) | Yesterday's activity | Attack completes undetected. All transactions approved. |
| 33-39 seconds (Dynamic Tables, offline) | Near-current activity | Catches slower attacks. Misses bursts completing in < 39s. |
| **~70 seconds (Online Feature Store)** | Near-current activity | **~1,200x improvement over daily batch. Hybrid Table point lookups on scoring path.** |

This is not a model accuracy problem. The same model, same weights, same threshold — only the
feature freshness changes. The Online Feature Store provides ~70 second effective freshness
(`refresh_freq=1 minute` + `target_lag=10 seconds`), compared to 24 hours for daily batch.
Features are served via low-latency Hybrid Table point lookups — no warehouse scan on the
scoring path.

---

## Part 1: How Payments Get from the Gateway to Snowflake

This is the question a technical audience asks first.

### The Integration Pattern

```
Customer taps card
      │
      ▼
Payment Backend  (EKS/ECS service, same AWS region as Snowflake account)
      │
      ├── Thread A  [ASYNC — fire-and-forget, does not block authorization]
      │   Snowpipe Streaming SDK ──► PrivateLink ──► Snowflake
      │   insertRows(channel="fraud_txn_ch_01", [full_transaction_record])
      │   └── Writes to: FRAUD_TRANSACTIONS table (persistence, training, audit)
      │
      │   [Thread B no longer exists — features sync automatically]
      │   FRAUD_TRANSACTIONS ──► Dynamic Table ──► Online Feature Table (Hybrid Table)
      │   Effective lag: ~70 seconds (refresh_freq=1min + target_lag=10s)
      │
      └── Thread C  [SYNC — blocks until fraud score is returned]
          HTTP POST ──► AWS API Gateway ──► PrivateLink ──► SPCS scoring container
          ├── 4 concurrent fs.read_feature_view(StoreType.ONLINE) lookups
          ├── Derived feature computation inline (~1ms)
          └── XGBoost inference                  (~5ms p50)
          Returns: {fraud_probability, decision}  (~20-25ms p50 total)
                │
                ▼
         EHI service response to checkout.com → Thredd
         (< 50ms EHI budget — scoring uses ~17-25ms, leaving ~25-33ms)
```

**The critical sequencing insight**: Thread A does not block the authorization decision. Thread C
reads velocity features computed from prior transactions (already in the online store). Thread A
writes to FRAUD_TRANSACTIONS for persistence, and the DT pipeline syncs these into the online
store in ~70 seconds for future transactions. The authorization decision is never waiting on the
persistence write.

### What Goes Where — and Why the Payloads Differ

| Destination | Payload | Reason |
|---|---|---|
| Snowflake table (Thread A) | Full transaction record — all columns | Persistence, compliance audit, training data |
| OFS REST Ingest (Thread B) | Thin event — 7 fields only | Only fields needed for velocity aggregation. Smaller payload = lower ingest latency. |

The `IS_GBR` field is precomputed by the payment backend (`merchant_country == 'GBR' ? 1.0 : 0.0`)
to avoid string operations inside the OFS aggregation pipeline. This is a deliberate design choice.

### Three Integration Options

**Option 1 — Direct SDK (lowest latency, simplest operationally)**

Snowpipe Streaming has production-grade SDKs for Java, Python, and Go. For JVM-based payment
backends (very common in fintech), the Java SDK provides a persistent channel connection, batched
`insertRows()` calls, and exactly-once delivery semantics with no additional infrastructure:

```java
// Channel stays open for the lifetime of the service — no per-transaction connection overhead
SnowflakeStreamingIngestClient client =
    SnowflakeStreamingIngestClientFactory.builder("fraud_ingest_client").build();

SnowflakeStreamingIngestChannel channel = client.openChannel(
    OpenChannelRequest.builder("FRAUD_TXN_CH_01")
        .setDBName("FRAUD_DEMO_PROD")
        .setSchemaName("TRANSACTIONS")
        .setTableName("FRAUD_TRANSACTIONS")
        .build());

// Per transaction: single non-blocking call
channel.insertRow(transactionRecord, offsetToken);
```

**Option 2 — Kafka Connector (zero code change if Kafka is already in the stack)**

If the payment gateway already publishes to Kafka or Confluent (extremely common in fintech), the
Snowflake Kafka Connector uses Snowpipe Streaming natively under the hood. Snowflake becomes
another Kafka consumer — no code change on the payment backend for the persistence path. The DT
pipeline then automatically syncs new rows into the Online Feature Store.

**Option 3 — SNS/SQS fan-out (for decoupling at peak volume)**

At high transaction volume where backpressure control is needed: publish the transaction event to
SNS → fan out to two SQS queues → two independent Lambda consumers handle Snowpipe Streaming and
OFS ingest independently. Each consumer retries with exponential backoff without blocking the other.

### Authentication on Both Paths

Both endpoints authenticate with the same **Programmatic Access Token (PAT)**:

```
Snowpipe Streaming:   Authorization: Bearer <PAT>
OFS Ingest API:       Authorization: Snowflake Token="<PAT>"
```

The PAT is stored in **AWS Secrets Manager** and injected into the payment backend at runtime. PAT
rotation before expiry is a production requirement — the payment backend must handle rotation
without a service restart, either by polling Secrets Manager on each request or registering a
rotation hook.

Both ingest paths traverse **PrivateLink** — payment data must not cross the public internet from
the point it leaves the payment gateway. This covers PCI DSS compliance from the first network hop.

---

## Part 2: The Pre-Feature Engineering Path — Why It Doesn't Exist

In the AWS-native architecture, "pre-feature engineering" is a distinct pipeline stage: raw
transactions land in S3, Glue jobs transform and encode them, and the output feeds both the
training pipeline and the serving store. This stage exists because the data and the features live
in different systems with different schemas.

In our architecture, **there is no pre-feature engineering stage**. Features are SQL aggregations
over `FRAUD_TRANSACTIONS`, refreshed automatically via Dynamic Tables and synced to an Online
Feature Table (Hybrid Table) for low-latency point lookups.

### How the Online Feature Store Registers Features

The feature semantics are declared once as a Snowpark DataFrame backed by SQL:

```python
customer_velocity_df = session.sql("""
    SELECT
        CUSTOMER_ID,
        COUNT_IF(TRANSACTION_TS >= DATEADD('hour', -1, CURRENT_TIMESTAMP())) AS PURCHASES_NUM_L1H,
        COUNT_IF(TRANSACTION_TS >= DATEADD('hour', -6, CURRENT_TIMESTAMP())) AS PURCHASES_NUM_L6H,
        SUM(CASE WHEN TRANSACTION_TS >= DATEADD('hour', -1, CURRENT_TIMESTAMP())
                 THEN PURCHASE_AMOUNT ELSE 0 END) AS PURCHASES_AMT_L1H,
        APPROX_COUNT_DISTINCT(
            CASE WHEN TRANSACTION_TS >= DATEADD('day', -7, CURRENT_TIMESTAMP())
                 THEN MERCHANT_ID END) AS DISTINCT_MERCHANTS_L1WK
        -- ... all velocity features across 5 time windows
    FROM FRAUD_DEMO_DEV.TRANSACTIONS.FRAUD_TRANSACTIONS
    WHERE TRANSACTION_TS >= DATEADD('day', -7, CURRENT_TIMESTAMP())
    GROUP BY CUSTOMER_ID
""")

customer_velocity_fv = FeatureView(
    name='FRAUD_CUSTOMER_VELOCITY',
    entities=[customer_entity],
    feature_df=customer_velocity_df,
    refresh_freq='1 minute',
    online_config=OnlineConfig(enable=True, target_lag='10 seconds'),
)
```

Snowflake manages the Dynamic Table refresh and the Online Feature Table sync automatically.
You declare **what** you need; the platform handles **how** — and the online Hybrid Table
provides sub-50ms point lookups for the scoring path.

### Feature Coverage

| Entity | Stream feature views | Aggregation method | Freshness |
|---|---|---|---|
| Customer | 65 features — counts, sums, max/min, approx distinct across 1h/6h/24h/48h/1wk | CONTINUOUS | < 2 seconds |
| Merchant | 20 features — counts, sums, approx distinct customers | CONTINUOUS | < 2 seconds |
| Wallet DPAN | 15 features — counts, sums, approx distinct customers | CONTINUOUS | < 2 seconds |
| IP Address | 12 features — counts, sums, approx distinct | CONTINUOUS | < 2 seconds |
| Customer profile | 13 features — lifetime stats, account age | Batch, daily refresh | 24 hours (sufficient) |

An additional ~30 derived ratio features (velocity burst ratios, merchant concentration, amount
deviation) are computed inline in the SPCS scoring container from the base velocity features. No
additional API call required — pure arithmetic at inference time.

---

## Part 3: Step-by-Step Architectural Comparison

### Step 1: Ingestion

**Their stack:** Kinesis / Amazon Data Streams → Lambda consumers → S3 landing zone

The Kinesis topology exists to decouple ingestion from storage and provide replay capability.
This introduces consumer lag, Lambda cold starts, and a separate operational surface.

**Snowflake:** Snowpipe Streaming (direct-to-table, persistent channel) + OFS REST Ingest API

Snowpipe Streaming provides sub-second ingestion with exactly-once semantics, no micro-batch
staging files, and no COPY INTO cycle. Replay is the table itself — immutable, queryable, indexed.
Both writes are async and non-blocking on the authorization path. **The ingestion architecture adds
zero latency to the payment authorization decision.**

---

### Step 2: Data Preparation / Feature Engineering

**Their stack:** AWS Glue jobs for ETL; NVIDIA RAPIDS for GPU-accelerated preprocessing inside
SageMaker training containers

Their diagram explicitly labels "Glue job" as a required pipeline stage. Glue jobs exist because
the training data and the serving data live in different systems with different schemas — they need
to be reconciled by a transformation layer.

**Snowflake:** No ETL. No Glue jobs. The transactions table is the only source of truth.

Training features are generated directly from `FRAUD_TRANSACTIONS` via SQL window aggregations at
training time. Serving features are maintained live by the OFS. There is no pipeline between them
to maintain, no pipeline lag, and no transformation logic to keep in sync across systems.

---

### Step 3: Online Feature Store

**Their stack:** Batch-precomputed feature snapshots loaded into the fraud-engine-service at startup
(their diagram note: *"we do the same transformations at request time using the same
encoders/scalers that were fitted during training and loaded at service startup"*)

This confirms their "online" features are frozen batch snapshots, not a live store. Features are
stale from the moment the service starts.

**Snowflake:** Snowflake Online Feature Store — Postgres-backed, CONTINUOUS stream aggregation,
< 2s freshness measured at ~280ms in benchmarks.

The practical gap: a card-testing burst of 5 transactions in 8 seconds is completely invisible to
a batch snapshot or a 33-39 second refresh cycle. With CONTINUOUS aggregation, the velocity count
updates after each transaction. The model detects the burst from transaction 2 onwards — regardless
of how fast the burst runs.

**Note on `approx_count_distinct`:** Distinct-value features (distinct merchants, DPANs) use
HyperLogLog with ~6.5% RSE at default precision. Training uses exact counts from the transactions
table. This creates a small, consistent training-serving skew — negligible for fraud velocity
signals. Set `precision=14` to reduce RSE to ~0.8% if precision is required.

---

### Step 4: Model Training

**Their stack:** SageMaker training jobs + NVIDIA RAPIDS containers + ECR image management + S3
data staging + IAM role management

SageMaker training requires managing container images in ECR, staging training data in S3,
configuring IAM role chains, and maintaining a separate job lifecycle outside of the data platform.
The RAPIDS preprocessing step exists because features aren't in the right shape in S3.

**Snowflake:** Snowpark-Optimized MEDIUM warehouse (256GB dedicated RAM, 6 credits/hr) + Model
Registry

Training runs directly from `FRAUD_TRANSACTIONS` in Snowflake — no data movement to S3. The
Snowpark-Optimized warehouse provides 256GB dedicated RAM appropriate for XGBoost on 12M rows.
Training completes in ~5 minutes. The warehouse is suspended when not running — no idle billing.

The Model Registry provides version tracking across DEV → STAGING → PROD, rollback to any prior
version, and the `create_service()` call that deploys the registered model directly to SPCS. No
separate deployment pipeline.

---

### Step 5: Training-Serving Consistency — The Hardest Problem in Production ML

**Their stack:** Their diagram's own description of the problem:

> *"NVIDIA's blueprint assumes batch preprocessing (encoding, scaling) but we do the same
> transformations at request time inside fraud-engine-service, using the same encoders/scalers that
> were fitted during training and loaded at service startup."*

This is the **number one operational failure mode in production ML**. The serialized encoder/scaler
chain creates several hard dependencies:

1. S3 artifact paths must stay in sync between the training job and the serving deployment
2. Every model retrain must regenerate and redeploy the preprocessing objects
3. A version mismatch produces **silent model degradation** — the model still runs, it just scores
   on incorrectly transformed features
4. The serving service must load these objects at startup — adding to cold start time and
   introducing an additional failure mode if the S3 read fails

**Snowflake:** Derived features are pure arithmetic on base velocity features from the OFS.

```python
# Computed inline in the SPCS scoring container — no serialization, no artifact management
velocity_ratio_1h = purchases_num_l1h / (purchases_num_l1wk + 1e-9)
spend_burst_6h    = purchases_amt_l6h / (purchases_amt_l1wk + 1e-9)
amount_deviation  = (purchase_amount - avg_purchase_amount) / (avg_purchase_amount + 1e-9)
```

Nothing to serialize. Nothing to load. Nothing to version separately from the model. The feature
transformation is transparent Python math in the scoring container — reviewable, testable, and
impossible to drift out of sync with training.

---

### Step 6: Real-Time Inference / Serving

**Their stack:** NVIDIA Triton Server (TensorRT/ONNX runtime) + fraud-engine-service microservice
on EC2 or EKS

Their diagram's MVP note: *"MVP: we can implement the batch solution, fraud-engine-service query
the batch result generated by NVIDIA Triton."*

This is the most important sentence in their architecture document. **Their real-time path is so
operationally complex that their own team is considering a batch fallback for MVP.** That means
their first production deployment will have 24-hour stale features — the same staleness problem
they designed the architecture to solve.

**Snowflake:** SPCS scoring container, deployed from the Model Registry via `create_service()` on a
`CPU_X64_XS` compute pool.

```
Incoming authorization request
  → 4 concurrent OFS REST lookups over SPCS internal mesh  (~12ms p50)
  → Compute ~30 derived features inline                      (~1ms)
  → XGBoost.predict(147 features)                            (~5ms p50)
  → Return fraud probability + decision                      (~17ms internal / ~20-25ms production)
```

**On NVIDIA Triton and TensorRT:** TensorRT is a GPU graph optimization runtime — it provides
significant speedup for neural network inference (transformer models, CNNs). For XGBoost on 147
features, it adds infrastructure complexity (model repository format, ensemble pipeline config,
gRPC protocol, dynamic batching tuning) with no latency benefit. Tree-based models don't benefit
from GPU execution at this feature count. If their roadmap moves to neural network-based fraud
models, Triton becomes relevant — that's a conversation to have, not dismiss.

**Cold start:** The SPCS compute pool runs with `min_instances=1`, keeping the scoring container
warm at all times. A container crash or deployment rolls over to the warm instance — no cold start
window where transactions are declined due to scoring unavailability.

---

### Step 7: Monitoring and Closed-Loop Retraining

**Their stack:** A separate automation layer — the diagram shows "Automation Service" but the
tooling is unspecified. Monitoring in a multi-service stack typically means separate dashboards for
Kinesis consumer lag, SageMaker endpoint metrics, DynamoDB throttling, and Triton throughput —
none of which tell you directly whether fraud model performance is degrading.

**Snowflake:** Snowflake Model Monitor, configured in NB05, operating directly on the inference log.

```python
monitor = ModelMonitor.create(
    model_version=prod_model,
    baseline_dataset=training_data,
    inference_log_table='FRAUD_DEMO_PROD.MONITORING.INFERENCE_LOG',
    metrics=['AUC_PR', 'PSI'],
    alert_threshold=0.05,   # retrain trigger if AUC-PR drops > 5% from baseline
)
```

Every scored transaction is logged to `INFERENCE_LOG`. When AUC-PR drops below threshold, a
retraining task triggers automatically. The model performance signal lives in the same platform as
the data — no cross-service correlation required.

---

## Platform Comparison Summary

| Architecture Layer | AWS / NVIDIA Stack | Snowflake Stack |
|---|---|---|
| Ingestion | Kinesis + Lambda + S3 | Snowpipe Streaming SDK + OFS REST API |
| Feature engineering | Glue ETL jobs + RAPIDS preprocessing | No ETL — declare features once at registration |
| Online feature store | Batch snapshots loaded at service startup | Snowflake OFS CONTINUOUS aggregation (< 2s) |
| Training | SageMaker + ECR + S3 data staging | Snowpark-Optimized WH + Model Registry |
| Serving | NVIDIA Triton + fraud-engine-service + EKS | SPCS container via `create_service()` |
| Training-serving consistency | Serialized encoders/scalers from S3 — version drift risk | Arithmetic inline in scoring container — nothing to serialize |
| Monitoring | Separate tooling across each service boundary | Snowflake Model Monitor on inference log |
| Compliance boundary | Data crosses multiple AWS service boundaries | Data never leaves Snowflake's network |
| MVP feasibility | Batch fallback required due to real-time complexity | Real-time path is the MVP path |
| Services to operate | 7+ independently monitored services | One platform with managed components |

The "one platform vs seven services" framing is not about counting components — Snowflake has SPCS,
OFS, the Model Registry. The real distinction is **one credential boundary, one network boundary,
one billing relationship, one monitoring surface.** Incidents don't require correlating logs across
five systems. Compliance audits don't require proving data isolation across six service boundaries.
Operational runbooks are not split across three teams.

---

## Anticipated Technical Objections

**"We already have Kinesis in place — is this a rip-and-replace?"**

Not necessarily. If Kafka or Kinesis is already in the event stream, the Snowflake Kafka Connector
consumes directly from the existing topic. The persistence path becomes zero-code. The OFS ingest
call is a small addition to any existing event consumer.

**"What happens when we need to add a new fraud signal — say, device fingerprint?"**

Register a new field in the stream source schema and add a new `Feature` declaration to the
relevant feature view. The scoring container is updated to use the new feature. The transition runs
both versions in parallel during rollout, with zero downtime. This is operationally simpler than
updating a DynamoDB schema, redeploying an aggregation service, and redeploying fraud-engine-service.

**"NVIDIA Triton supports multiple model frameworks — can Snowflake do that?"**

SPCS is a container runtime — any model framework that runs in a Docker container runs in SPCS. The
Model Registry supports XGBoost, scikit-learn, PyTorch, TensorFlow, and custom containers. If the
roadmap includes a neural network fraud model, the SPCS container changes; the rest of the
architecture does not.

**"The Online Feature Store is in Preview — is this production-ready?"**

It is now GA (requires `snowflake-ml-python >= 1.18.0`). Online Feature Tables are backed by
Hybrid Tables. Hybrid table request billing was disabled March 2026. Pricing is: Virtual warehouse
compute for DT refresh and key lookups + Hybrid Table storage (same rate as Hybrid Tables).
Confirm current cost estimates with your Snowflake account team.

**"XGBoost at 17ms — what's the latency at 10x volume?"**

SPCS auto-scales (`min_instances=1, max_instances=2`). The Online Feature Store scales
horizontally as a managed Hybrid Table service. Point lookup latency on Hybrid Tables does not
degrade at volume in the same way a warehouse query would. Load test at 600 txn/min (10x
Zilch volume) before go-live is a production checklist item.

---

## Key Numbers to Have Ready

| Metric | Value |
|---|---|
| Feature freshness (velocity) | ~70 seconds (refresh_freq=1min + target_lag=10s) |
| Feature freshness vs daily batch | 1,200x improvement (70s vs 24 hours) |
| OFS lookup latency | Measured in benchmark (Hybrid Table point lookup) |
| XGBoost inference | ~5ms p50 |
| End-to-end scoring (internal mesh) | ~17ms p50 |
| End-to-end scoring (with PrivateLink inbound) | ~20-25ms p50 |
| EHI service budget | < 50ms — scoring uses ~17-25ms, ~25-33ms remaining |
| Training cycle | ~5 minutes on Snowpark-Optimized MEDIUM |
| Training warehouse cost | ~0.5 credits/run, suspended at idle |
| Fraud recall at 0.05% fraud rate | ~80% (card-testing velocity as primary signal) |
