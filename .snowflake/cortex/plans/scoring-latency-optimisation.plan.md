# Plan: Scoring Latency Optimisation

## Context

**EHI service budget:** < 50ms total. Scoring must return within this window.

**Current scoring path (nb04 benchmark):**
- 4× `fs.read_feature_view(StoreType.ONLINE)` via `ThreadPoolExecutor` — each pays Python SDK overhead + HTTP round-trip + SQL compile + Hybrid Table lookup
- 1× additional `fs.read_feature_view()` for customer profile (5th concurrent read)
- `compute_derived_features()` inline
- `requests.post(SPCS_URL)` for XGBoost inference

**Critical blocker:** nb04 still imports `from snowflake.ml.feature_store import online_service as ofs_utils` and calls `fs.get_online_service_status()` — the broken Preview API. The previous rewrite edit did not apply to this file.

---

## Implementation Steps

### Step 1 — Fix nb04 broken imports (blocker)

Cell 5 of nb04 still has the old `online_service as ofs_utils` import and the REST endpoint retrieval pattern. Replace with the GA API: `from snowflake.ml.feature_store.feature_view import StoreType` and load feature views via `fs.get_feature_view()`.

```python
# Replace
from snowflake.ml.feature_store import online_service as ofs_utils
status = fs.get_online_service_status()
QUERY_URL = ofs_utils.endpoint_url(status, 'query')

# With
from snowflake.ml.feature_store.feature_view import StoreType
cust_fv  = fs.get_feature_view('FRAUD_CUSTOMER_VELOCITY', 'V1')
merch_fv = fs.get_feature_view('FRAUD_MERCHANT_VELOCITY', 'V1')
dpan_fv  = fs.get_feature_view('FRAUD_DPAN_VELOCITY',     'V1')
ip_fv    = fs.get_feature_view('FRAUD_IP_VELOCITY',       'V1')
profile_fv = fs.get_feature_view('FRAUD_CUSTOMER_PROFILE', 'V1')
```

### Step 2 — Add a dedicated scoring warehouse

Add `FRAUD_OFS_SCORE_WH` (Standard XS, AUTO_SUSPEND=60) to `scripts/setup.sql`. The SPCS scoring container and all `read_feature_view()` calls during scoring should use this warehouse, not `FRAUD_OFS_TRAIN_WH`. This eliminates the risk of training jobs queuing against scoring requests.

```sql
CREATE WAREHOUSE IF NOT EXISTS FRAUD_OFS_SCORE_WH
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = FALSE  -- keep warm, always-on for scoring
    COMMENT = 'Dedicated to online feature store reads on the scoring path';
```

`INITIALLY_SUSPENDED = FALSE` and no AUTO_SUSPEND race with a scoring request. This warehouse runs 24/7 but at XS (0.5 credits/hr, ~$1.10/day) it's the right trade-off for a production payment scoring path.

### Step 3 — Single SQL join across all 4 Online Feature Tables

Replace 4 concurrent SDK calls with one parameterised SQL query that joins all 4 Online Feature Tables. The Online Feature Tables are Hybrid Tables — key lookups are O(1), so joining 4 of them in one query has no fanout cost.

**Discover table names at startup:**
```python
online_tables = session.sql(
    "SHOW ONLINE FEATURE TABLES IN SCHEMA FRAUD_DEMO_DEV.FEATURE_STORE"
).collect()
# Inspect the NAME column to find the correct table name format
```

**Single-join feature read:**
```python
def fetch_all_features_fast(customer_id, merchant_id, wallet_dpan, ip_address, session, warehouse):
    """Single SQL round-trip for all 4 entity velocity features."""
    result = session.sql(f"""
        SELECT c.*, m.*, d.*, i.*
        FROM (SELECT 1 AS _dummy) AS _t
        LEFT JOIN FRAUD_DEMO_DEV.FEATURE_STORE.{CUST_OFT}  c ON c.CUSTOMER_ID = '{customer_id}'
        LEFT JOIN FRAUD_DEMO_DEV.FEATURE_STORE.{MERCH_OFT} m ON m.MERCHANT_ID = '{merchant_id}'
        LEFT JOIN FRAUD_DEMO_DEV.FEATURE_STORE.{DPAN_OFT}  d ON d.WALLET_DPAN  = '{wallet_dpan}'
        LEFT JOIN FRAUD_DEMO_DEV.FEATURE_STORE.{IP_OFT}    i ON i.IP_ADDRESS   = '{ip_address}'
    """).collect()
    return dict(zip(result[0]._fields, result[0])) if result else {}
```

This reduces 4 SDK calls + 4 HTTP round-trips + 4 SQL compiles to 1 of each. **Expected latency reduction: ~30-50% on the feature lookup step.**

Note: If the underlying table names are not directly queryable, the fallback is `fs.read_feature_view()` with all 4 feature view names passed in a single call using `retrieve_feature_values()` if available, or keeping the concurrent pattern but switching to the dedicated scoring warehouse.

### Step 4 — Cache customer profile features

Profile features (customer age, lifetime stats) refresh daily. Caching them in the scoring container eliminates the 5th concurrent read from the hot path. Cache is populated at container startup and refreshed every hour.

```python
import threading, time

_profile_cache: dict = {}
_cache_lock = threading.Lock()

def _refresh_profile_cache(session, fs):
    fv = fs.get_feature_view('FRAUD_CUSTOMER_PROFILE', 'V1')
    rows = fs.read_feature_view(fv, store_type=StoreType.ONLINE).collect()
    new_cache = {r['CUSTOMER_ID']: dict(zip(r._fields, r)) for r in rows}
    with _cache_lock:
        _profile_cache.update(new_cache)

def get_profile_features(customer_id: str) -> dict:
    with _cache_lock:
        return _profile_cache.get(customer_id, {})

# Refresh on startup + every 3600 seconds via background thread
threading.Thread(target=lambda: [
    _refresh_profile_cache(session, fs) or time.sleep(3600) for _ in iter(int, 1)
], daemon=True).start()
```

### Step 5 — Update nb04 benchmark to reflect the optimised path

Rewrite the benchmark cell (cell 7) to:
1. Use the single-join query for velocity feature reads
2. Use the profile cache (or skip profile for the benchmark)
3. Use `FRAUD_OFS_SCORE_WH` as the warehouse for all feature reads
4. Show per-component breakdown: feature lookup time vs derived computation time vs inference time
5. Report p50/p95/p99 with the latency budget context (< 50ms EHI)

### Step 6 — Update setup.sql and architecture.md

- `scripts/setup.sql`: Add `FRAUD_OFS_SCORE_WH` creation
- `docs/architecture.md`: Update warehouse strategy section to document the dedicated scoring warehouse
- Note: the latency numbers in architecture.md reference the old Postgres-backed REST endpoints. Update to reflect Hybrid Table-based lookup numbers once benchmarked.

---

## Verification

1. Run cell 5 of nb04 — no ImportError
2. Run the benchmark cell — all lookups complete using `FRAUD_OFS_SCORE_WH`
3. Verify single-join query returns features from all 4 entities
4. Confirm p50 end-to-end latency is within < 50ms EHI budget
5. Check that `FRAUD_OFS_SCORE_WH` shows query activity (not `FRAUD_OFS_TRAIN_WH`) during scoring

---

## Critical Files
- `notebooks/nb04_serving.ipynb` — broken imports (step 1) + benchmark rewrite (steps 3-5)
- `scripts/setup.sql` — add dedicated scoring warehouse (step 2)
- `docs/architecture.md` — update warehouse strategy and latency claims
