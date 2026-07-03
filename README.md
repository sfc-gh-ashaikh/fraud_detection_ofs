# Fraud Detection — Online Feature Store Demo

Real-time card fraud detection on Snowflake. Demonstrates feature freshness < 2 seconds and end-to-end scoring latency < 20ms p50 using Snowflake Online Feature Store (CONTINUOUS aggregation) and a custom SPCS scoring service.

## Architecture

```
Transaction (Thredd format)
        |
        v
SPCS Fraud Scoring Service  (fraud_scorer container, FRAUD_OFS_CPU_POOL)
        |
        |-- Thread B: OFS REST ingest  -->  OFS Postgres  (async, <2s feature freshness)
        |                                   *.svc.spcs.internal:8080
        |
        '-- Thread C: OFS REST query x5 --> OFS Postgres  (sync, ~5-10ms)
                                            *.svc.spcs.internal:8081
                      --> XGBoost predict  (~5ms)
                      --> {score, decision, timing}
```

OFS internal URLs are used from within SPCS (same cluster network, no TLS hop).
Snowflake Notebooks use the public OFS URL for setup and freshness verification.

## Repository structure

```
notebooks/
  nb01_data_generation.ipynb   — 12M synthetic transactions (run once, ~10 min)
  nb02_feature_store.ipynb     — OFS setup: online service + 5 feature views
  nb03_training.ipynb          — XGBoost training + model export to stage
  nb04_serving.ipynb           — Build Docker image + deploy SPCS service
  nb05_monitoring.ipynb        — Inference logging + model monitor
  nb06_latency_proof.ipynb     — Customer demo: freshness + latency

services/
  fraud_scorer/
    app.py        — FastAPI scoring service (3-thread pattern)
    Dockerfile    — Container image (python:3.11-slim + XGBoost + FastAPI)
    spec.yaml     — SPCS service spec template (OFS URLs injected at deploy)

scripts/
  setup.sql       — Infrastructure (databases, warehouses, roles, image repo)
  teardown.sql    — Drop all objects
```

## Prerequisites

- Snowflake account with ACCOUNTADMIN access
- Docker Desktop running locally (for nb04 image build)
- `snow` CLI installed: `pip install snowflake-cli-labs`

## Step-by-step runbook

### Step 0: Infrastructure setup

Run `scripts/setup.sql` as ACCOUNTADMIN in Snowsight or the CLI:

```bash
snow sql -f scripts/setup.sql --role ACCOUNTADMIN
```

This creates databases, warehouses, compute pool, roles, stages, and the image repository.

### Step 1: Generate training data

Open `notebooks/nb01_data_generation.ipynb` in Snowsight and run all cells.

- Creates 12M synthetic fraud transactions (days 1–180)
- 500K inference transactions (most recent 30 days)
- Runtime: ~10 minutes on FRAUD_OFS_LOAD_WH (LARGE)

### Step 2: Set up the Online Feature Store

Open `notebooks/nb02_feature_store.ipynb` and run all cells in order.

- Creates the Postgres-backed online service (3-5 minutes first run)
- Registers 4 CONTINUOUS stream feature views (customer, merchant, DPAN, IP velocity)
- Registers 1 batch feature view (customer profile, daily refresh)
- Creates a FeatureGroup bundling all 5 views for single-call scoring
- Runs freshness and latency benchmarks

**Note:** The online service uses `OnlineServiceAccess.PUBLIC` in this notebook
because Snowflake Notebooks does not run inside the SPCS cluster. The scoring
service (nb04) uses internal URLs with much lower latency.

### Step 3: Train the XGBoost model

Open `notebooks/nb03_training.ipynb` and run all cells.

- Generates a training dataset with full feature parity with the OFS schema
  (4 entities × 5 windows + profile + derived ratio features)
- Trains an XGBoost classifier with class-imbalance handling
- Registers the model in the Snowflake Model Registry (DEV → STAGING → PROD)
- **Exports `fraud_model.json` + `feature_cols.json` to `@FRAUD_DEMO_PROD.ML.MODEL_STAGE`**
  These files are loaded by the SPCS scoring container at startup.

### Step 4: Build and deploy the scoring service

Open `notebooks/nb04_serving.ipynb` and run all cells.

Cell 4.2 prints Docker commands to run in your local terminal:

```bash
snow spcs image-registry login
docker build -t <repo_url>/fraud_scorer:latest services/fraud_scorer/
docker push <repo_url>/fraud_scorer:latest
```

Then run cell 4.3 to deploy the SPCS service with OFS internal URLs injected into the spec.

- Service endpoint: `https://<service>.snowflakecomputing.app`
- Health check: `GET /health`
- Score endpoint: `POST /score` (Thredd or internal field names)
- Benchmark endpoint: `POST /benchmark?n=100` (self-contained, measures internal OFS latency)

### Step 5: Monitoring (optional)

Open `notebooks/nb05_monitoring.ipynb` and run all cells.

- Creates inference log table for production scoring
- Simulates chargeback labels
- Sets up Snowflake Model Monitor for AUC-PR drift tracking

### Step 6: Customer demo

Open `notebooks/nb06_latency_proof.ipynb` and run all cells.

**Section 6.2 — Freshness demo:**
- Ingests one transaction via OFS REST API
- Polls until velocity feature increments (target: < 2 seconds)
- Shows the comparison: Snowflake CONTINUOUS vs customer batch pipeline

**Section 6.3 — Latency demo:**
- Sends 100 Thredd-format transactions to the SPCS `/score` endpoint
- Reports p50/p95/p99 timing from inside the container (true internal OFS latency)
- Comparison table: Snowflake vs customer (Kinesis + DynamoDB + SageMaker)

## Key latency numbers (expected)

| Metric | Expected p50 |
|---|---|
| OFS internal feature lookup (5 FVs parallel) | 5-10ms |
| XGBoost inference | ~5ms |
| Total end-to-end inside SPCS | ~15ms |
| Feature freshness (CONTINUOUS) | < 2s |
| Customer EHI budget | < 50ms |

## Authentication

### Snowflake Notebooks (nb01–nb06)

For most notebook cells, the active session is used automatically — no extra setup.

**Exception: OFS ingest and query calls require a PAT.**
The OFS ingress proxy rejects session tokens (including `session.connection.rest.token`).
Both direct REST calls and the SDK's `stream_ingest`/`read_feature_view` are affected.

**One-time PAT setup (required for nb02 sections 2.6 and 2.7, and nb06 section 6.2):**

1. In Snowsight, click your profile icon → **Programmatic Access Tokens**
2. Click **Add Token**
3. Name: `FRAUD_DEMO_PAT` | Role: `FRAUD_MLOPS` | Expiry: 30 days
4. Copy the token value (shown **once only** — save it securely)
5. In nb02, find the **PAT setup cell** (section 2.0) and paste your token:
   ```python
   OFS_PAT = 'your_token_here'
   ```
6. Run the cell — it sets `os.environ['SNOWFLAKE_PAT']` for the session

nb06 has the same PAT setup step in cell 1.

### SPCS scoring container (nb04 / production)

`SNOWFLAKE_TOKEN` is auto-injected by the SPCS platform — no PAT needed inside the container.

## Teardown

```bash
snow sql -f scripts/teardown.sql --role ACCOUNTADMIN
```
