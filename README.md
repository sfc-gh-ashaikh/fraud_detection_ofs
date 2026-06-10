# Real-Time Fraud Detection on Snowflake
## Online Feature Store Solution

> Sub-2 second feature freshness. ~$400-700/month platform cost. Single platform, zero warehouse on the hot path.

This repository contains a single, clean production architecture for real-time fraud detection using the **Snowflake Online Feature Store** — replacing a multi-service setup (daily batch features, external model serving, manual monitoring) with a unified Snowflake-native solution.

---

## Table of Contents

1. [Problem Statement](#problem-statement)
2. [Architecture](#architecture)
3. [Key Results](#key-results)
4. [Prerequisites](#prerequisites)
5. [Setup Guide](#setup-guide)
6. [Notebook Walkthrough](#notebook-walkthrough)
7. [Design Decisions](#design-decisions)
8. [Caveats & Production Considerations](#caveats--production-considerations)
9. [Supporting Documentation](#supporting-documentation)
10. [Teardown](#teardown)

---

## Problem Statement

### The Business Problem

Fraud losses are directly tied to how fresh your features are.

At Zilch's volume (~66,000 transactions/day, 0.05% fraud rate), the most damaging pattern is **card testing**: fraudsters make 5-10 rapid purchases in under 30 seconds to validate stolen credentials. The model's ability to catch these attacks depends entirely on one thing: **can it see the velocity spike in time?**

| Feature Freshness | What the Model Sees | Outcome |
|---|---|---|
| **24 hours (daily batch)** | Yesterday's activity | Attack completes undetected. All fraudulent transactions approved. |
| **33-39 seconds (Dynamic Tables)** | Near-current activity | Catches slower attacks. Misses bursts completing in < 39s. |
| **< 2 seconds (Online Feature Store)** | Real-time activity | Catches attacks from transaction 2 onwards, regardless of burst speed. |

This isn't a model accuracy problem — it's a **data freshness problem**. The same model, with the same weights, produces dramatically different fraud hit rates depending solely on whether features reflect what happened 24 hours ago or 2 seconds ago.

### Current State vs This Solution

| | Current (Daily Batch) | This Solution |
|---|---|---|
| Feature freshness | 24 hours | **< 2 seconds** |
| Card-testing detection | Missed entirely | Caught from transaction 2 onwards |
| Monthly platform cost | High (multi-service) | **~$400-700/month** |
| Infrastructure | 5+ services to coordinate | **Single platform** |
| Model iteration | Hours | **3-5 minutes** |
| Monitoring | Manual | **Automated drift detection** |

---

## Architecture

```
Transaction arrives (payment gateway)
      │
      ├──► Snowflake FRAUD_TRANSACTIONS table   (persistence + training + audit)
      │
      └──► Online Feature Store REST Ingest API  (real-time velocity update, < 2s)
                    │
                    ▼
      Postgres-backed Online Service
      (continuous velocity aggregations per entity)
                    │
                    ▼
AWS API Gateway → PrivateLink → SPCS Scoring Container
                                      │
                                      ├── REST → Online Feature Store (~10-15ms)
                                      │          4 entity velocity lookups, concurrent
                                      ├── XGBoost inference (~105ms)
                                      │
                                      ▼
                               Decision (~130ms total)
```

### Platform Components

```
┌───────────────────────────────────────────────────────────────────────┐
│                        SNOWFLAKE (Single Platform)                      │
├───────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  INGESTION           FEATURES              ML             SERVING       │
│  ┌──────────┐       ┌─────────────┐    ┌──────────┐    ┌──────────┐  │
│  │Snowpipe  │──────►│Online       │───►│Model     │───►│SPCS      │  │
│  │Streaming │       │Feature Store│    │Registry  │    │Container │  │
│  │          │       │(Postgres)   │    │(XGBoost) │    │(REST API)│  │
│  └──────────┘       │Stream FVs   │    └──────────┘    └──────────┘  │
│  ┌──────────┐       │CONTINUOUS   │         │                │        │
│  │REST      │──────►│aggregation  │    ┌────▼─────┐    ┌────▼─────┐  │
│  │Ingest API│       └─────────────┘    │Model     │    │Inference │  │
│  └──────────┘                          │Monitor   │    │Log       │  │
│                                        │AUC-PR+PSI│    └──────────┘  │
│                                        └──────────┘                   │
└───────────────────────────────────────────────────────────────────────┘
```

### How Features Work

**Velocity features** (the primary card-testing signal) are maintained as running aggregates in the Online Feature Store:
- Every transaction is ingested via REST → continuous aggregation updates counts, sums, maxes per entity
- No warehouse refresh cycle — features are current within 2 seconds of each transaction

**Derived features** (velocity ratios, merchant concentration, amount deviation) are computed in the SPCS scoring container at inference time using the base velocity features fetched from the Online FS.

**Static/profile features** (customer account age, lifetime averages) are served from a DT-backed batch online feature view, refreshed daily.

### Feature Freshness by Type

| Feature type | Freshness | Where computed |
|---|---|---|
| Velocity aggregates (counts, sums, maxes) | < 2 seconds | Online FS continuous aggregation |
| Approx distinct counts (distinct merchants, DPANs) | < 2 seconds | Online FS (HyperLogLog, ~6.5% RSE) |
| Derived ratios (velocity ratios, concentration) | At scoring time | SPCS container inline computation |
| Customer profile (lifetime stats) | Daily | DT-backed batch online feature view |

---

## Key Results

| Metric | Value |
|---|---|
| Feature freshness (velocity) | < 2 seconds end-to-end |
| Feature lookup latency | ~10-15ms p50 (4 entities concurrent via REST) |
| End-to-end scoring latency | ~130ms p50 (lookup + inference + PrivateLink) |
| Monthly platform cost | ~$400-700 (Online Service + SPCS, no 24/7 DT warehouse) |
| Break-even fraud blocks | < 1 case/month |
| Training cycle | 3-5 minutes (Snowpark-Optimized MEDIUM) |
| Fraud detection rate | ~80% recall (card-testing velocity signals primary driver) |

---

## Prerequisites

| Requirement | Details |
|---|---|
| Snowflake Edition | Enterprise or higher |
| Role | ACCOUNTADMIN (initial setup) |
| `snowflake-ml-python` | >= 1.41 (Online Feature Store Preview) |
| Snowflake CLI | `snow` CLI (see below) |
| PAT | Programmatic Access Token for REST API auth |
| AWS region | Same region as Snowflake account (PrivateLink) |

### Installing the Snowflake CLI

```bash
brew install snowflake-cli        # macOS
pip install snowflake-cli          # any platform
snow --version
snow connection add                # configure account, user, role, warehouse
```

### Setting Up Your PAT

```bash
# Generate a PAT in Snowsight: Admin → Security → Access Tokens
export SNOWFLAKE_PAT="<your_pat_token>"
```

Full guide: [Snowflake CLI docs](https://docs.snowflake.com/en/developer-guide/snowflake-cli/installation/installation)

---

## Setup Guide

### Step 1: Infrastructure Setup

```bash
snow sql -f scripts/setup.sql
```

Creates databases, warehouses, roles, schemas, compute pool. The DT warehouse is not created — the Online Feature Store replaces it for serving.

### Step 2: Deploy Notebooks to Snowflake

```bash
snow stage copy notebooks/ @FRAUD_DEMO_DEV.PUBLIC.NOTEBOOKS --overwrite

snow notebook create nb01_data_generation  --from-stage @FRAUD_DEMO_DEV.PUBLIC.NOTEBOOKS/nb01_data_generation.ipynb  --database FRAUD_DEMO_DEV --schema PUBLIC
snow notebook create nb02_feature_store    --from-stage @FRAUD_DEMO_DEV.PUBLIC.NOTEBOOKS/nb02_feature_store.ipynb    --database FRAUD_DEMO_DEV --schema PUBLIC
snow notebook create nb03_training         --from-stage @FRAUD_DEMO_DEV.PUBLIC.NOTEBOOKS/nb03_training.ipynb         --database FRAUD_DEMO_DEV --schema PUBLIC
snow notebook create nb04_serving          --from-stage @FRAUD_DEMO_DEV.PUBLIC.NOTEBOOKS/nb04_serving.ipynb          --database FRAUD_DEMO_DEV --schema PUBLIC
snow notebook create nb05_monitoring       --from-stage @FRAUD_DEMO_DEV.PUBLIC.NOTEBOOKS/nb05_monitoring.ipynb       --database FRAUD_DEMO_DEV --schema PUBLIC
```

**Running locally?** Replace `get_active_session()` with `Session.builder.configs({...}).create()` and convert `%%sql` cells to `session.sql("...").collect()`.

### Step 3: Execute Notebooks (in order)

| # | Notebook | Duration | What It Does |
|---|----------|----------|--------------|
| 1 | `nb01_data_generation.ipynb` | ~3 min | 12M synthetic transactions replicating production fraud patterns |
| 2 | `nb02_feature_store.ipynb` | ~10 min | Online Feature Store setup, stream feature views, freshness + latency benchmarks |
| 3 | `nb03_training.ipynb` | ~5 min | XGBoost training from transactions table, Model Registry |
| 4 | `nb04_serving.ipynb` | ~10 min | SPCS deployment, scoring service reads from Online FS, latency benchmarks |
| 5 | `nb05_monitoring.ipynb` | ~5 min | Drift detection, automated retraining, ROI analysis |

### Step 4: Teardown (when done)

```bash
snow sql -f scripts/teardown.sql
```

---

## Notebook Walkthrough

### Notebook 1: Data Generation

Generates 12M training transactions (6 months) + 500k inference transactions (1 week) replicating Zilch's entity model, volumes, and fraud rate.

- 5 entities: Customer (200k), Merchant (5k), Wallet DPAN (50k), IP (10k), Card Token
- 0.05% fraud rate — ~6,000 fraud cases in training set
- Clustered by `transaction_ts` for efficient historical queries at training time

---

### Notebook 2: Online Feature Store

The core of this architecture. Sets up the Postgres-backed Online Service, registers stream feature views with continuous aggregation, and benchmarks freshness and lookup latency.

**What it does:**
1. Creates the Online Service (Postgres-backed, ~3-5 min to provision)
2. Registers stream source for transaction events (`FRAUD_TXN_EVENTS`)
3. Creates stream feature views with `CONTINUOUS` aggregation for 4 entity velocity groups
4. Creates a DT-backed batch feature view for static customer profile features (refreshed daily)
5. Runs freshness benchmark: REST ingest → poll until feature updates (target: < 2s)
6. Benchmarks REST Query API latency: single-entity and 4-entity concurrent lookups
7. Cost analysis: Online Service vs DT warehouse comparison

**Feature coverage per entity:**

| Entity | Stream aggregations | Derived (computed at scoring) |
|---|---|---|
| Customer | counts, sums, maxes across 1h/6h/24h/48h/1wk | velocity ratios, spend bursts, merchant concentration |
| Merchant | count, sum, approx distinct customers, max | — |
| Wallet DPAN | count, sum, approx distinct customers | — |
| IP Address | count, sum, approx distinct customers | — |

---

### Notebook 3: Model Training

Trains an XGBoost fraud classifier on 12M transactions. Training data is generated directly from the transactions table — no Dynamic Tables required.

- Snowpark-Optimized MEDIUM warehouse (256GB dedicated RAM, 6 credits/hr)
- 147 features at scoring time
- `scale_pos_weight=2000` for extreme class imbalance (0.05% fraud rate)
- AUC-PR evaluation (not ROC-AUC — appropriate for extreme imbalance)
- Model registered in Snowflake Model Registry: DEV → STAGING → PROD

---

### Notebook 4: Model Serving (SPCS + Online Feature Store)

Deploys the model as a REST endpoint and benchmarks end-to-end latency from feature lookup through to scoring decision.

- SPCS deployment via Model Registry (`create_service()`)
- Scoring service reads velocity features from Online FS REST API (~10-15ms, 4 entities concurrent)
- Derived features computed inline in the scoring container
- PrivateLink for production deployment (no public internet)
- Measured end-to-end: ~130ms p50

**Scoring flow:**
```
Incoming transaction
  → 4 concurrent Online FS REST lookups  (~10-15ms)
  → Compute derived features inline        (~1ms)
  → XGBoost.predict(147 features)          (~105ms)
  → Return fraud probability               (decision)
```

---

### Notebook 5: Monitoring, Drift Detection & ROI

Closes the loop — automated model monitoring and the full business case.

- Inference logging for audit and model performance tracking
- Model Monitor: AUC-PR baseline + PSI drift detection
- Auto-retraining trigger when AUC-PR drops > 5%
- ROI analysis: platform cost (~$555/month) vs fraud exposure ($623,700/month)
- Break-even: < 1 extra fraud case blocked per month

---

## Design Decisions

| # | Decision | Choice | Rationale |
|---|----------|--------|-----------|
| 1 | No 24/7 DT warehouse | Online FS stream FVs replace DT serving | $13,190/month warehouse cost eliminated |
| 2 | CONTINUOUS aggregation | Feature.count/sum/max with FeatureAggregationMethod.CONTINUOUS | Updates on every ingest event, < 2s freshness |
| 3 | Derived features in scoring container | Not in Online FS | Computed inline at inference time — no additional latency, no extra API call |
| 4 | DT-backed batch FV for profile features | Daily refresh | Static features don't need stream freshness; simpler as SQL aggregate |
| 5 | Dual write path | Transactions table + REST Ingest API | Persistence + real-time features, both idempotent and independently reliable |
| 6 | Snowpark-Optimized training WH | MEDIUM (256GB, 6 credits/hr, suspended) | Runs ~5 min/month for retraining. Cheaper and more memory than Standard XLARGE |
| 7 | CLUSTER BY (transaction_ts) | On transactions table | Efficient historical window queries at training time |
| 8 | scale_pos_weight=2000 | Inverse of fraud rate | Handles 0.05% imbalance without oversampling |
| 9 | AUC-PR metric | Not ROC-AUC | Appropriate for extreme class imbalance |
| 10 | PrivateLink | No public ingress | Data never leaves Snowflake's network |
| 11 | CPU_X64_XS for SPCS | Smallest instance | Right-sized for XGBoost inference at 60 txn/min |

---

## Caveats & Production Considerations

### 1. Stream Aggregation Does Not Cover All Features

`CONTINUOUS` aggregation supports `count`, `sum`, `max`, `min`, `approx_count_distinct`. Derived ratio features (~25-30 of 147) — velocity ratios, merchant concentration, decline rates, amount deviation — are computed in the SPCS scoring container using base velocity features from the Online FS. No additional lookup required.

### 2. Dual Write Path

Every transaction must be written to both the Snowflake transactions table and the REST Ingest API. The Ingest API is idempotent (deduplication by entity key + timestamp). Implement retry logic with exponential backoff on the ingest call.

### 3. approx_count_distinct Skew

Distinct-value features use HyperLogLog (~6.5% RSE at default precision). Training uses exact counts from the transactions table. This creates a small but consistent training-serving skew — negligible for fraud velocity signals. Set `precision=14` to reduce RSE to ~0.8%.

### 4. Preview Status

The Online Feature Store requires `snowflake-ml-python >= 1.41` and is in Preview. Pricing is not yet finalised — confirm with your Snowflake account team. APIs may change before GA. Validate in staging before production commitment.

### 5. PAT Rotation

REST API credentials must be rotated before expiry. Store in AWS Secrets Manager or equivalent. The SPCS scoring container needs secure runtime access.

### 6. Training-Serving Consistency

The Online FS stores serving features; it is not a training data source. Model retraining generates features from the transactions table using window aggregations. Ensure training and serving feature definitions stay in sync when the model is updated.

---

## Supporting Documentation

| Document | Description |
|---|---|
| [`docs/architecture.md`](docs/architecture.md) | Detailed architecture, data flow, production deployment pattern, and deployment checklist |
| [`docs/feature_catalogue.md`](docs/feature_catalogue.md) | Full specification of all 170+ features with computation method per entity |
| [`scripts/setup.sql`](scripts/setup.sql) | Infrastructure: databases, warehouses, roles, compute pool |
| [`scripts/teardown.sql`](scripts/teardown.sql) | Clean removal of all objects |

---

## Teardown

```bash
snow sql -f scripts/teardown.sql
```

Drops all databases, warehouses, compute pool, and the Online Service.

---

## Project Structure

```
fraud_detection_ofs/
├── README.md                          # This file
├── docs/
│   ├── architecture.md                # Architecture + production patterns
│   └── feature_catalogue.md           # Full feature specification
├── scripts/
│   ├── setup.sql                      # Infrastructure setup (run first)
│   └── teardown.sql                   # Clean removal (run last)
└── notebooks/
    ├── nb01_data_generation.ipynb     # 12M synthetic transactions
    ├── nb02_feature_store.ipynb       # Online Feature Store setup + benchmarks
    ├── nb03_training.ipynb            # XGBoost + Model Registry
    ├── nb04_serving.ipynb             # SPCS + Online FS integration + latency
    └── nb05_monitoring.ipynb          # Drift detection + ROI analysis
```
