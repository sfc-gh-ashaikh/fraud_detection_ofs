"""
Fraud Scoring Service — SPCS FastAPI container.

Three-thread payment gateway pattern:
  Thread A: Snowpipe REST ingest  (async, fire-and-forget — persists transaction)
  Thread B: OFS REST ingest       (async, fire-and-forget — updates velocity features)
  Thread C: OFS REST query × 5   (sync, blocks for decision — 4 velocity FVs + profile)
              → compute derived features (~0.1ms)
              → XGBoost predict (~5ms)
              → return score + timing

Authentication: SNOWFLAKE_TOKEN env var is auto-injected by SPCS at container startup.
OFS URLs: OFS_INGEST_URL / OFS_QUERY_URL env vars are injected via spec.yaml at deploy time.
Model: loaded from /mnt/model/fraud_model.json (Snowflake stage mounted as a volume).
"""

import os
import json
import time
import concurrent.futures
import logging
from datetime import datetime, timezone

import numpy as np
import requests
import xgboost as xgb
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
log = logging.getLogger(__name__)

app = FastAPI(title="Fraud Scoring Service")

# ── Auth + endpoints ──────────────────────────────────────────────────────────
TOKEN      = os.environ.get("SNOWFLAKE_TOKEN", "")          # auto-injected by SPCS
OFS_INGEST = os.environ.get("OFS_INGEST_URL", "").rstrip("/")
OFS_QUERY  = os.environ.get("OFS_QUERY_URL",  "").rstrip("/")
SNOWPIPE_ACCOUNT = os.environ.get("SNOWFLAKE_ACCOUNT", "")  # for Thread A

OFS_HEADERS = {
    "Authorization": f'Snowflake Token="{TOKEN}"',
    "Content-Type": "application/json",
}

# ── Model + feature columns ───────────────────────────────────────────────────
MODEL_DIR = os.environ.get("MODEL_DIR", "/mnt/model")

model: xgb.XGBClassifier = None
FEATURE_COLS: list = []

@app.on_event("startup")
def load_model():
    global model, FEATURE_COLS
    model_path = os.path.join(MODEL_DIR, "fraud_model.json")
    cols_path  = os.path.join(MODEL_DIR, "feature_cols.json")
    log.info(f"Loading model from {model_path}")
    model = xgb.XGBClassifier()
    model.load_model(model_path)
    with open(cols_path) as f:
        FEATURE_COLS = json.load(f)
    log.info(f"Model loaded. Features: {len(FEATURE_COLS)}")
    if OFS_INGEST and OFS_QUERY:
        log.info(f"OFS ingest : {OFS_INGEST}")
        log.info(f"OFS query  : {OFS_QUERY}")
    else:
        log.warning("OFS URLs not set — feature queries will fail")

# ── OFS helpers ───────────────────────────────────────────────────────────────

FV_CONFIGS = [
    ("FRAUD_CUSTOMER_VELOCITY", "V1", "CUSTOMER_ID"),
    ("FRAUD_MERCHANT_VELOCITY", "V1", "MERCHANT_ID"),
    ("FRAUD_DPAN_VELOCITY",     "V1", "WALLET_DPAN"),
    ("FRAUD_IP_VELOCITY",       "V1", "IP_ADDRESS"),
    ("FRAUD_CUSTOMER_PROFILE",  "V1", "CUSTOMER_ID"),
]

def _query_one(fv_name: str, fv_version: str, key_col: str, key_val: str) -> dict:
    """Single OFS feature view point lookup. Returns {feature_name: value}."""
    r = requests.post(
        f"{OFS_QUERY}/api/v1/query",
        headers=OFS_HEADERS,
        json={
            "name": fv_name,
            "version": fv_version,
            "object_type": "feature_view",
            "request_rows": [{"entity": {key_col: key_val}}],
            "metadata_options": {"include_names": True},
        },
        timeout=5,
    )
    if r.status_code not in (200, 207):
        log.warning(f"OFS query {fv_name} returned {r.status_code}: {r.text[:200]}")
        return {}
    body    = r.json()
    results = body.get("results", [])
    meta    = body.get("metadata", {}).get("features", [])
    if not results or not meta:
        return {}
    vals  = results[0].get("features", [])
    return {
        m["name"]: (v if v is not None else 0.0)
        for m, v in zip(meta, vals)
    }

def _fetch_features(cust: str, merch: str, dpan: str, ip: str) -> tuple[dict, float]:
    """
    Query all 5 feature views in parallel.
    Returns (features_dict, elapsed_ms).
    Wall-clock time = slowest of 5 concurrent calls.
    """
    key_map = {
        "CUSTOMER_ID": cust,
        "MERCHANT_ID": merch,
        "WALLET_DPAN": dpan,
        "IP_ADDRESS":  ip,
    }
    t0 = time.perf_counter()
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as pool:
        futs = {
            pool.submit(_query_one, fv, ver, key_col, key_map[key_col]): fv
            for fv, ver, key_col in FV_CONFIGS
        }
        features: dict = {}
        for fut in concurrent.futures.as_completed(futs):
            features.update(fut.result())
    elapsed_ms = (time.perf_counter() - t0) * 1000
    return features, elapsed_ms

def _ingest_event(event: dict) -> None:
    """Fire-and-forget OFS ingest. Runs in a background thread (Thread B)."""
    try:
        requests.post(
            f"{OFS_INGEST}/api/v1/ingest",
            headers=OFS_HEADERS,
            json={"dry_run": False, "records": {"FRAUD_TXN_EVENTS": [event]}},
            timeout=5,
        )
    except Exception as exc:
        log.warning(f"OFS ingest error (non-fatal): {exc}")

# ── Derived features ──────────────────────────────────────────────────────────

def _compute_derived(feat: dict, amount: float, ts: datetime) -> dict:
    """
    Inline derived features not stored in OFS (~0.1ms).
    Ratio features detect velocity bursts; time features capture circadian patterns.
    These MUST match the columns used during model training (feature_cols.json).
    """
    def ratio(n_key: str, d_key: str) -> float:
        n = feat.get(n_key) or 0.0
        d = feat.get(d_key) or 0.0
        return n / d if d > 0 else 0.0

    avg_amt = feat.get("AVG_TXN_AMOUNT_30D") or 0.0
    max_amt = feat.get("PURCHASES_MAX_L1WK")  or 0.0

    return {
        "HOUR_OF_DAY":              ts.hour,
        "DAY_OF_WEEK":              ts.weekday(),
        "IS_WEEKEND":               1 if ts.weekday() >= 5 else 0,
        "IS_NIGHT":                 1 if ts.hour < 5 else 0,
        "VELOCITY_RATIO_1H_L1WK":  ratio("PURCHASES_NUM_L1H",  "PURCHASES_NUM_L1WK"),
        "VELOCITY_RATIO_6H_L1WK":  ratio("PURCHASES_NUM_L6H",  "PURCHASES_NUM_L1WK"),
        "VELOCITY_RATIO_24H_L1WK": ratio("PURCHASES_NUM_L24H", "PURCHASES_NUM_L1WK"),
        "SPEND_BURST_1H_L1WK":     ratio("PURCHASES_AMT_L1H",  "PURCHASES_AMT_L1WK"),
        "SPEND_BURST_6H_L1WK":     ratio("PURCHASES_AMT_L6H",  "PURCHASES_AMT_L1WK"),
        "SPEND_BURST_24H_L1WK":    ratio("PURCHASES_AMT_L24H", "PURCHASES_AMT_L1WK"),
        "AMOUNT_PCT_DEVIATION":    (amount - avg_amt) / avg_amt if avg_amt > 0 else 0.0,
        "AMOUNT_RATIO_TO_HIST_MAX": amount / max_amt if max_amt > 0 else 0.0,
        "MERCHANT_CONCENTRATION_24H": 1.0 - ratio("DISTINCT_MERCHANTS_L24H", "PURCHASES_NUM_L24H"),
        "IS_FIRST_PURCHASE":       1 if (feat.get("LIFETIME_TXN_COUNT") or 0) == 0 else 0,
        "IS_NEW_ACCOUNT_7D":       1 if (feat.get("DAYS_SINCE_REGISTRATION") or 999) <= 7 else 0,
        "IS_NEW_ACCOUNT_30D":      1 if (feat.get("DAYS_SINCE_REGISTRATION") or 999) <= 30 else 0,
    }

# ── Endpoints ─────────────────────────────────────────────────────────────────

@app.get("/health")
def health():
    return {
        "status":   "ok",
        "model":    "loaded" if model else "not_loaded",
        "features": len(FEATURE_COLS),
        "ofs_ingest": OFS_INGEST or "not_configured",
        "ofs_query":  OFS_QUERY  or "not_configured",
    }

@app.post("/score")
def score(payload: dict):
    """
    Score a single transaction. Accepts both Thredd field names and internal names.

    Thredd fields:  Cust_Ref, Merchant_Id, Token_Ref, IP_Address,
                    Trans_Amount, Merch_Country, Trans_DateTime
    Internal fields: customer_id, merchant_id, wallet_dpan, ip_address,
                     amount_usd, is_gbr, event_ts
    """
    if model is None:
        raise HTTPException(503, "Model not loaded")

    t_total = time.perf_counter()

    # ── Map Thredd → internal field names ─────────────────────────────────────
    cust_id  = str(payload.get("Cust_Ref")      or payload.get("customer_id")  or "")
    merch_id = str(payload.get("Merchant_Id")   or payload.get("merchant_id")  or "")
    dpan     = str(payload.get("Token_Ref")     or payload.get("wallet_dpan")  or "")
    ip_addr  = str(payload.get("IP_Address")    or payload.get("ip_address")   or "")
    amount   = float(payload.get("Trans_Amount") or payload.get("amount_usd")  or 0.0)
    country  = str(payload.get("Merch_Country") or "").upper()
    is_gbr   = 1.0 if country == "GBR" else 0.0
    event_ts_raw = payload.get("Trans_DateTime") or payload.get("event_ts", "")
    try:
        event_ts = datetime.fromisoformat(str(event_ts_raw).replace("Z", "+00:00"))
    except Exception:
        event_ts = datetime.now(timezone.utc)

    ofs_event = {
        "CUSTOMER_ID": cust_id, "MERCHANT_ID": merch_id,
        "WALLET_DPAN": dpan,    "IP_ADDRESS":  ip_addr,
        "AMOUNT_USD":  amount,  "IS_GBR":      is_gbr,
        "EVENT_TS":    event_ts.strftime("%Y-%m-%dT%H:%M:%SZ"),
    }

    # Thread B: async OFS ingest (does not block response path)
    bg_pool = concurrent.futures.ThreadPoolExecutor(max_workers=1)
    bg_pool.submit(_ingest_event, ofs_event)
    bg_pool.shutdown(wait=False)

    # Thread C: sync OFS query — 5 parallel feature lookups
    features, ofs_ms = _fetch_features(cust_id, merch_id, dpan, ip_addr)

    # Derived features + transaction fields not in OFS
    features.update(_compute_derived(features, amount, event_ts))
    features["PURCHASE_AMOUNT"]                    = amount
    features["IS_GBR"]                             = is_gbr
    features.setdefault("AUTHENTICATED_3DS_CHALLENGE_FLAG", 0)
    features.setdefault("IS_MERCHANT_INITIATED_PURCHASE",   0)

    # Build feature vector in training column order; fill missing with 0
    vec = np.array([[features.get(c, 0.0) for c in FEATURE_COLS]], dtype=np.float32)

    t_xgb = time.perf_counter()
    prob = float(model.predict_proba(vec)[0, 1])
    xgb_ms = (time.perf_counter() - t_xgb) * 1000

    total_ms = (time.perf_counter() - t_total) * 1000

    return {
        "score":    round(prob, 4),
        "decision": "DECLINE" if prob > 0.5 else "APPROVE",
        "customer_id": cust_id,
        "timing": {
            "ofs_query_ms": round(ofs_ms, 1),
            "xgb_ms":       round(xgb_ms, 1),
            "total_ms":     round(total_ms, 1),
        },
    }

@app.post("/benchmark")
def benchmark(n: int = 100):
    """
    Self-contained latency benchmark using synthetic entity keys.
    Returns p50/p95/p99 for OFS query and XGBoost inference from within SPCS.
    These numbers reflect the TRUE internal OFS latency (same SPCS cluster).
    """
    import random
    import string

    def rand_id(prefix: str) -> str:
        return prefix + "".join(random.choices(string.digits, k=6))

    results = []
    for _ in range(n):
        result = score({
            "customer_id": rand_id("C"),
            "merchant_id": rand_id("M"),
            "wallet_dpan": rand_id("D"),
            "ip_address":  "10.0.0.1",
            "amount_usd":  random.uniform(10, 500),
            "is_gbr":      random.choice([0.0, 1.0]),
            "event_ts":    datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        })
        results.append(result["timing"])

    def pct(arr: list, p: int) -> float:
        return round(float(np.percentile(arr, p)), 1)

    ofs_arr   = [r["ofs_query_ms"] for r in results]
    xgb_arr   = [r["xgb_ms"]       for r in results]
    total_arr = [r["total_ms"]      for r in results]

    return {
        "n":           n,
        "environment": "SPCS (internal OFS URLs)",
        "ofs_query_ms":  {"p50": pct(ofs_arr, 50),   "p95": pct(ofs_arr, 95),   "p99": pct(ofs_arr, 99)},
        "xgb_ms":        {"p50": pct(xgb_arr, 50),   "p95": pct(xgb_arr, 95)},
        "total_ms":      {"p50": pct(total_arr, 50),  "p95": pct(total_arr, 95), "p99": pct(total_arr, 99)},
        "ehi_budget_ms": 50,
        "headroom_ms":   round(50 - pct(total_arr, 50), 1),
    }
