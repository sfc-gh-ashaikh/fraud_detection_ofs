# Plan: Correct OFS API + Feature Groups

## What the Documentation Actually Shows

The correct API (`snowflake-ml-python >= 1.41`, LIMITEDACCESS Preview) uses:

| Correct | What was implemented (wrong) |
|---|---|
| `StreamSource`, `StreamConfig`, `Feature.count()` | SQL DataFrame feature views |
| `FeatureAggregationMethod.CONTINUOUS` | DT refresh + `target_lag` |
| `OnlineConfig(store_type=OnlineStoreType.POSTGRES)` | Hybrid Tables (default) |
| `fs.create_online_service()` | Not needed (Hybrid Table auto-created) |
| `from snowflake.ml.feature_store import online_service as ofs_utils` | `from ...feature_view import StoreType` |
| `ofs_utils.endpoint_url(status, 'query')` → REST API | `fs.read_feature_view(StoreType.ONLINE)` |
| REST Ingest: `POST /api/v1/ingest` (Thread B) | No ingest path |
| Freshness: **< 2 seconds** | ~70 seconds |

### Feature Groups — the single biggest latency improvement

`FeatureGroup` bundles all feature views and returns everything in one call:
```python
from snowflake.ml.feature_store import FeatureGroup

fraud_fg = FeatureGroup(
    name="FRAUD_SCORING_FG",
    features=[cust_fv, merch_fv, dpan_fv, ip_fv, profile_fv],
    auto_prefix=False,
)
registered_fg = fs.register_feature_group(fraud_fg, "V1")

# At scoring time — ONE round-trip for all 5 feature views
result = fs.read_feature_group(
    registered_fg,
    keys=[[customer_id, merchant_id, wallet_dpan, ip_address]],
)
```

Docs: *"a single `read_feature_group` call returns features from all sources in one round-trip"*
Constraint: all source FVs must have `store_type=OnlineStoreType.POSTGRES`.

### `OnlineServiceAccess.INTERNAL` — fastest path inside SPCS

```python
from snowflake.ml.feature_store.online_service import OnlineServiceAccess

fs = FeatureStore(
    session=session,
    database='FRAUD_DEMO_DEV',
    name='FEATURE_STORE',
    default_warehouse='FRAUD_OFS_SCORE_WH',
    creation_mode=CreationMode.FAIL_IF_NOT_EXIST,
    online_service_access=OnlineServiceAccess.INTERNAL,  # SPCS internal mesh URL
)
```

This forces the SPCS-internal endpoint URL, bypassing PrivateLink entirely for feature lookups made from within SPCS. Docs confirm this is the lowest-latency option when calling from inside the same SPCS cluster.

### REST Query API for production SPCS container

The REST API (`requests.post` directly) is faster than the Python SDK for the production SPCS container — no SDK session overhead, direct HTTP. The Query API supports `"object_type": "feature_view"` per the docs. Feature Groups via REST may use `"object_type": "feature_group"` — this should be tested.

---

## Scoring Path After Changes

```
Thredd webhook → EHI Service
  │
  ├── Thread A: Snowpipe Streaming → FRAUD_TRANSACTIONS        [async, ~0ms block]
  │
  ├── Thread B: OFS REST Ingest → CONTINUOUS velocity          [async, ~0ms block]
  │   POST /api/v1/ingest  {FRAUD_TXN_EVENTS: [{CUSTOMER_ID, MERCHANT_ID,
  │                          WALLET_DPAN, IP_ADDRESS, AMOUNT_USD, IS_GBR, EVENT_TS}]}
  │
  └── Thread C: Feature Group query + SPCS inference           [sync, blocks]
       │
       ├── fs.read_feature_group(fraud_fg, keys=[[cust, merch, dpan, ip]])
       │   ONE round-trip → all 5 feature views (customer+merchant+dpan+ip velocity + profile)
       │   ~10-12ms p50 via SPCS internal mesh
       │
       ├── compute_derived_features() inline                   ~0.1ms
       │
       └── XGBoost.predict(features)                           ~5ms p50
       │
       Total Thread C: ~15-17ms p50 (internal mesh)
       EHI budget: ~33-35ms remaining
       Freshness: < 2 seconds (CONTINUOUS aggregation)
```

---

## Implementation Steps

### Step 1 — Restore nb02 to the correct Preview API

Replace cell 2 imports (currently wrong GA API) with the correct Preview API imports:

```python
import time, os, numpy as np, requests, random, concurrent.futures
from datetime import datetime
from snowflake.snowpark.context import get_active_session
from snowflake.ml.feature_store import (
    FeatureStore, FeatureView, Entity, CreationMode,
    OnlineConfig, OnlineStoreType, StreamSource, StreamConfig, Feature,
    FeatureGroup,
)
from snowflake.ml.feature_store.spec.enums import FeatureAggregationMethod
from snowflake.ml.feature_store import online_service as ofs_utils
from snowflake.ml.feature_store.online_service import OnlineServiceAccess
from snowflake.snowpark.types import (
    StructType, StructField, StringType, DoubleType,
    TimestampType, TimestampTimeZone,
)
```

Session token stays as `session.connection.rest.token` (already correct from the PAT fix).

### Step 2 — Restore Online Service creation in nb02 cell 4

Replace the current "privilege grant + ONLINE_CFG" cell with:

```python
try:
    fs.create_online_service('FRAUD_MLOPS', 'FRAUD_MLOPS')
except Exception as e:
    if 'already exists' in str(e).lower():
        print('Online service already exists — continuing')
    else:
        raise

status = fs.get_online_service_status()
start = time.time()
while status.status != 'RUNNING':
    print(f'  [{time.time()-start:.0f}s] {status.status} — waiting 30s...')
    time.sleep(30)
    status = fs.get_online_service_status()

HEADERS   = {'Authorization': f'Snowflake Token="{token}"', 'Content-Type': 'application/json'}
QUERY_URL  = ofs_utils.endpoint_url(status, 'query')
INGEST_URL = ofs_utils.endpoint_url(status, 'ingest')
print(f'RUNNING — Query:  {QUERY_URL}')
print(f'           Ingest: {INGEST_URL}')
```

### Step 3 — Restore stream source and streaming feature views in nb02

Restore cells 7-11 to their original pattern using `StreamSource`, `StreamConfig`, `Feature.count()`, `Feature.sum()`, `Feature.approx_count_distinct()`, `FeatureAggregationMethod.CONTINUOUS`.

Key change: add `store_type=OnlineStoreType.POSTGRES` to every `OnlineConfig` (required for Feature Group):

```python
online_cfg = OnlineConfig(
    enable=True,
    store_type=OnlineStoreType.POSTGRES,  # required for FeatureGroup
)
```

### Step 4 — Add Feature Group registration in nb02 (new cell after all FVs registered)

```python
from snowflake.ml.feature_store import FeatureGroup

# FeatureGroup bundles all 5 feature views for single-call scoring.
# Requirement: all source FVs must have store_type=OnlineStoreType.POSTGRES.
# read_feature_group() returns all features in ONE round-trip vs 5 concurrent calls.
fraud_fg = FeatureGroup(
    name="FRAUD_SCORING_FG",
    features=[customer_fv, merchant_fv, dpan_fv, ip_fv, profile_fv],
    auto_prefix=False,
    desc="All fraud scoring features: 4 velocity views + customer profile",
)
registered_fg = fs.register_feature_group(fraud_fg, "V1")
print(f'Registered FeatureGroup: {registered_fg.name} V1')
print('Scoring: read_feature_group() = ONE round-trip for all features')
```

### Step 5 — Update nb04 cell 5 (Feature Store setup)

Replace the current wrong API with the correct Preview API + `OnlineServiceAccess.INTERNAL`:

```python
from snowflake.ml.feature_store import FeatureStore, CreationMode, FeatureGroup
from snowflake.ml.feature_store import online_service as ofs_utils
from snowflake.ml.feature_store.online_service import OnlineServiceAccess

# OnlineServiceAccess.INTERNAL: forces SPCS-internal URL when calling from within SPCS.
# This is the lowest-latency path — bypasses PrivateLink entirely for feature lookups.
fs = FeatureStore(
    session=session,
    database='FRAUD_DEMO_DEV',
    name='FEATURE_STORE',
    default_warehouse='FRAUD_OFS_SCORE_WH',
    creation_mode=CreationMode.FAIL_IF_NOT_EXIST,
    online_service_access=OnlineServiceAccess.INTERNAL,
)

status     = fs.get_online_service_status()
QUERY_URL  = ofs_utils.endpoint_url(status, 'query')
INGEST_URL = ofs_utils.endpoint_url(status, 'ingest')
HEADERS    = {'Authorization': f'Snowflake Token="{PAT}"', 'Content-Type': 'application/json'}

fraud_fg = fs.get_feature_group('FRAUD_SCORING_FG', 'V1')
print(f'Feature Group: {fraud_fg.name} V1')
print(f'Query URL:     {QUERY_URL}')
```

Keep the profile cache (`_profile_cache`) — it is still valid: profile features are daily, caching them saves one network call even with Feature Groups.

### Step 6 — Update nb04 cell 7 (benchmark)

Replace the current wrong Hybrid Table benchmark with the correct path:
1. Feature Group query (single `fs.read_feature_group()` call) for all velocity features
2. Profile from in-memory cache (0ms, daily features already cached)
3. Compute derived features inline
4. XGBoost via SPCS

Show a side-by-side benchmark:
- **Path A**: 5 concurrent REST calls (the original approach) — measures baseline
- **Path B**: `fs.read_feature_group()` — measures the Feature Group improvement

### Step 7 — Restore Thread B in nb06

Replace the current "Thread B no longer exists" comment with the REST ingest call:

```python
def thread_b_ofs_ingest(ofs_event):
    """Thread B: OFS REST Ingest — updates CONTINUOUS velocity aggregations.
    Freshness: < 2 seconds end-to-end (CONTINUOUS mode).
    """
    r = requests.post(
        f'{INGEST_URL}/api/v1/ingest',
        headers=HEADERS,
        json={
            'records': {
                'FRAUD_TXN_EVENTS': [ofs_event]
            }
        },
        timeout=5,
    )
    r.raise_for_status()
    return True
```

Update the burst simulation to show velocity incrementing in < 2 seconds (not ~70 seconds), and restore the freshness claim to "< 2 seconds".

### Step 8 — Update docs/architecture.md and customer_engineering_talking_points.md

- Restore < 2 second freshness claim
- Document Feature Group as the single-call scoring optimization
- Note `OnlineServiceAccess.INTERNAL` as the optimal SPCS-to-OFS network path
- Keep `FRAUD_OFS_SCORE_WH` (still valid — dedicated warehouse for scoring)

---

## Critical Files
- `notebooks/nb02_feature_store.ipynb` — full revert + FeatureGroup registration
- `notebooks/nb04_serving.ipynb` — correct API setup cell + Feature Group benchmark
- `notebooks/nb06_latency_proof.ipynb` — restore Thread B + correct freshness
- `docs/customer_engineering_talking_points.md` — restore < 2s freshness claim

## Verification
1. nb02 imports load without error (`no import error on online_service`)
2. Online service reaches RUNNING state
3. All 5 feature views registered with `store_type=OnlineStoreType.POSTGRES`
4. `FRAUD_SCORING_FG` registered and visible via `fs.list_feature_groups()`
5. nb02 freshness benchmark shows < 2 second ingest-to-visible latency
6. nb04 cell 7 benchmark shows Feature Group p50 ≤ concurrent REST calls p50
7. nb06 burst simulation: velocity increments visible in < 2s between transactions
