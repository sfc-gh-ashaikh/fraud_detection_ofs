# Fraud scoring service with Feature Group single-call, persistent connections, and dedicated ingest
# Co-authored with CoCo
"""
Fraud Scoring Service — SPCS FastAPI container (production-ready).

Optimizations (all production-realistic patterns):
  1. Feature Group: single OFS round-trip for all 5 feature views
  2. Persistent connection pool: requests.Session with keep-alive (standard practice)
  3. Dedicated ingest thread: daemon thread with queue decouples I/O from scoring
  4. Synchronous scoring: no async/thread scheduling jitter on the hot path
  5. Pre-warmed connections: TCP handshake done at startup, not on first request

Architecture:
  Ingest thread: daemon thread drains queue, posts to OFS ingest (non-blocking to scorer)
  Score path:    synchronous — OFS query → derived features → XGBoost → return
                 No thread hops, no async scheduling on the critical path.
"""

import os
import json
import time
import queue
import logging
import threading
from datetime import datetime, timezone

import numpy as np
import requests
import xgboost as xgb
from requests.adapters import HTTPAdapter
from fastapi import FastAPI, HTTPException
from contextlib import asynccontextmanager

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ── Auth + endpoints ──────────────────────────────────────────────────────────
TOKEN      = os.environ.get("SNOWFLAKE_TOKEN", "")
OFS_INGEST = os.environ.get("OFS_INGEST_URL", "").rstrip("/")
OFS_QUERY  = os.environ.get("OFS_QUERY_URL",  "").rstrip("/")

OFS_HEADERS = {
    "Authorization": f'Snowflake Token="{TOKEN}"',
    "Content-Type": "application/json",
}

# ── Config ────────────────────────────────────────────────────────────────────
MODEL_DIR             = os.environ.get("MODEL_DIR", "/mnt/model")
FEATURE_GROUP_NAME    = "FRAUD_SCORING_FG"
FEATURE_GROUP_VERSION = "V1"
OFS_TIMEOUT           = 10.0  # Allow OFS to complete — no artificial cutoff

FV_CONFIGS = [
    ("FRAUD_CUSTOMER_VELOCITY", "V1", "CUSTOMER_ID"),
    ("FRAUD_MERCHANT_VELOCITY", "V1", "MERCHANT_ID"),
    ("FRAUD_DPAN_VELOCITY",     "V1", "WALLET_DPAN"),
    ("FRAUD_IP_VELOCITY",       "V1", "IP_ADDRESS"),
    ("FRAUD_CUSTOMER_PROFILE",  "V1", "CUSTOMER_ID"),
]

# ── Globals (set at startup) ──────────────────────────────────────────────────
model: xgb.XGBClassifier = None
FEATURE_COLS: list = []
ofs_session: requests.Session = None
ingest_queue: queue.Queue = None
_ingest_thread: threading.Thread = None


def _ingest_worker():
    """Daemon thread: drains ingest queue and posts to OFS. Never blocks the scorer."""
    ingest_session = requests.Session()
    ingest_session.headers.update(OFS_HEADERS)
    adapter = HTTPAdapter(pool_connections=2, pool_maxsize=4, max_retries=0)
    ingest_session.mount("http://", adapter)
    ingest_session.mount("https://", adapter)

    while True:
        try:
            event = ingest_queue.get(timeout=5)
            if event is None:
                break  # Poison pill for shutdown
            ingest_session.post(
                f"{OFS_INGEST}/api/v1/ingest",
                json={"dry_run": False, "records": {"FRAUD_TXN_EVENTS": [event]}},
                timeout=5,
            )
        except queue.Empty:
            continue
        except Exception as exc:
            log.debug(f"Ingest error (non-fatal): {exc}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup: load model, create sessions, start ingest thread."""
    global model, FEATURE_COLS, ofs_session, ingest_queue, _ingest_thread

    # Load XGBoost model
    model_path = os.path.join(MODEL_DIR, "fraud_model.json")
    cols_path  = os.path.join(MODEL_DIR, "feature_cols.json")
    log.info(f"Loading model from {model_path}")
    model = xgb.XGBClassifier()
    model.load_model(model_path)
    with open(cols_path) as f:
        FEATURE_COLS = json.load(f)
    log.info(f"Model loaded. Features: {len(FEATURE_COLS)}")

    # Persistent OFS query session with connection pooling
    ofs_session = requests.Session()
    ofs_session.headers.update(OFS_HEADERS)
    adapter = HTTPAdapter(pool_connections=10, pool_maxsize=20, max_retries=0)
    ofs_session.mount("http://", adapter)
    ofs_session.mount("https://", adapter)

    # Pre-warm connections: establish TCP so first real request doesn't pay handshake
    if OFS_QUERY:
        try:
            ofs_session.get(f"{OFS_QUERY}/health", timeout=2)
            log.info("OFS connection pre-warmed")
        except Exception:
            log.info("OFS pre-warm failed (will connect on first query)")

    # Dedicated ingest thread (decouples ingest I/O from scoring path)
    ingest_queue = queue.Queue(maxsize=1000)
    _ingest_thread = threading.Thread(target=_ingest_worker, daemon=True, name="ofs-ingest")
    _ingest_thread.start()
    log.info("Ingest daemon thread started")

    log.info(f"OFS query  : {OFS_QUERY}")
    log.info(f"OFS ingest : {OFS_INGEST}")
    log.info("Ready to score")

    yield

    # Shutdown
    ingest_queue.put(None)  # Poison pill
    _ingest_thread.join(timeout=3)
    ofs_session.close()


app = FastAPI(title="Fraud Scoring Service", lifespan=lifespan)


# ── OFS query (synchronous, no thread hop) ────────────────────────────────────

def _query_features(cust: str, merch: str, dpan: str, ip: str) -> tuple[dict, float]:
    """
    Single OFS round-trip via Feature Group. Synchronous — no asyncio overhead.
    Falls back to parallel individual queries if Feature Group errors.
    """
    t0 = time.perf_counter()
    try:
        r = ofs_session.post(
            f"{OFS_QUERY}/api/v1/query",
            json={
                "name": FEATURE_GROUP_NAME,
                "version": FEATURE_GROUP_VERSION,
                "object_type": "feature_group",
                "request_rows": [{"entity": {
                    "CUSTOMER_ID": cust,
                    "MERCHANT_ID": merch,
                    "WALLET_DPAN": dpan,
                    "IP_ADDRESS":  ip,
                }}],
                "metadata_options": {"include_names": True},
            },
            timeout=OFS_TIMEOUT,
        )
    except requests.exceptions.Timeout:
        log.warning("Feature Group query timed out, falling back to individual")
        return _query_individual_fallback(cust, merch, dpan, ip)

    elapsed_ms = (time.perf_counter() - t0) * 1000

    if r.status_code not in (200, 207):
        log.warning(f"Feature Group {r.status_code}, falling back")
        return _query_individual_fallback(cust, merch, dpan, ip)

    body    = r.json()
    results = body.get("results", [])
    meta    = body.get("metadata", {}).get("features", [])
    if not results or not meta:
        return {}, elapsed_ms

    vals = results[0].get("features", [])
    features = {m["name"]: (v if v is not None else 0.0) for m, v in zip(meta, vals)}
    return features, elapsed_ms


def _query_individual_fallback(cust: str, merch: str, dpan: str, ip: str) -> tuple[dict, float]:
    """Fallback: 5 concurrent queries via ThreadPoolExecutor (still uses connection pool)."""
    from concurrent.futures import ThreadPoolExecutor, as_completed

    key_map = {"CUSTOMER_ID": cust, "MERCHANT_ID": merch, "WALLET_DPAN": dpan, "IP_ADDRESS": ip}

    def _one(fv_name, fv_ver, key_col):
        try:
            r = ofs_session.post(
                f"{OFS_QUERY}/api/v1/query",
                json={
                    "name": fv_name, "version": fv_ver, "object_type": "feature_view",
                    "request_rows": [{"entity": {key_col: key_map[key_col]}}],
                    "metadata_options": {"include_names": True},
                },
                timeout=OFS_TIMEOUT,
            )
            if r.status_code not in (200, 207):
                return {}
            body = r.json()
            results = body.get("results", [])
            meta = body.get("metadata", {}).get("features", [])
            if not results or not meta:
                return {}
            return {m["name"]: (v if v is not None else 0.0) for m, v in zip(meta, results[0].get("features", []))}
        except Exception:
            return {}

    t0 = time.perf_counter()
    features = {}
    with ThreadPoolExecutor(max_workers=5) as pool:
        futs = [pool.submit(_one, fv, ver, kc) for fv, ver, kc in FV_CONFIGS]
        for f in as_completed(futs):
            features.update(f.result())
    return features, (time.perf_counter() - t0) * 1000


# ── Derived features ──────────────────────────────────────────────────────────

def _compute_derived(feat: dict, amount: float, ts: datetime) -> dict:
    get = feat.get

    num_l1wk = get("PURCHASES_NUM_L1WK") or 0.0
    amt_l1wk = get("PURCHASES_AMT_L1WK") or 0.0
    avg_amt  = get("AVG_TXN_AMOUNT_30D")  or 0.0
    max_amt  = get("PURCHASES_MAX_L1WK")  or 0.0
    num_l24h = get("PURCHASES_NUM_L24H")  or 0.0

    inv_num_l1wk = 1.0 / num_l1wk if num_l1wk > 0 else 0.0
    inv_amt_l1wk = 1.0 / amt_l1wk if amt_l1wk > 0 else 0.0

    return {
        "HOUR_OF_DAY":              ts.hour,
        "DAY_OF_WEEK":              ts.weekday(),
        "IS_WEEKEND":               1 if ts.weekday() >= 5 else 0,
        "IS_NIGHT":                 1 if ts.hour < 5 else 0,
        "VELOCITY_RATIO_1H_L1WK":  (get("PURCHASES_NUM_L1H") or 0.0) * inv_num_l1wk,
        "VELOCITY_RATIO_6H_L1WK":  (get("PURCHASES_NUM_L6H") or 0.0) * inv_num_l1wk,
        "VELOCITY_RATIO_24H_L1WK": num_l24h * inv_num_l1wk,
        "SPEND_BURST_1H_L1WK":     (get("PURCHASES_AMT_L1H") or 0.0) * inv_amt_l1wk,
        "SPEND_BURST_6H_L1WK":     (get("PURCHASES_AMT_L6H") or 0.0) * inv_amt_l1wk,
        "SPEND_BURST_24H_L1WK":    (get("PURCHASES_AMT_L24H") or 0.0) * inv_amt_l1wk,
        "AMOUNT_PCT_DEVIATION":    (amount - avg_amt) / avg_amt if avg_amt > 0 else 0.0,
        "AMOUNT_RATIO_TO_HIST_MAX": amount / max_amt if max_amt > 0 else 0.0,
        "MERCHANT_CONCENTRATION_24H": 1.0 - ((get("DISTINCT_MERCHANTS_L24H") or 0.0) / num_l24h if num_l24h > 0 else 0.0),
        "IS_FIRST_PURCHASE":       1 if (get("LIFETIME_TXN_COUNT") or 0) == 0 else 0,
        "IS_NEW_ACCOUNT_7D":       1 if (get("DAYS_SINCE_REGISTRATION") or 999) <= 7 else 0,
        "IS_NEW_ACCOUNT_30D":      1 if (get("DAYS_SINCE_REGISTRATION") or 999) <= 30 else 0,
    }


# ── Endpoints ─────────────────────────────────────────────────────────────────

@app.get("/health")
def health():
    return {
        "status":        "ok",
        "model":         "loaded" if model else "not_loaded",
        "features":      len(FEATURE_COLS),
        "ofs_ingest":    OFS_INGEST or "not_configured",
        "ofs_query":     OFS_QUERY  or "not_configured",
        "feature_group": FEATURE_GROUP_NAME,
        "ingest_queue":  ingest_queue.qsize() if ingest_queue else 0,
    }


@app.post("/score")
def score(payload: dict):
    """
    Score a single transaction. Fully synchronous — no async/thread scheduling overhead.
    """
    if model is None:
        raise HTTPException(503, "Model not loaded")

    t_total = time.perf_counter()

    # ── Parse payload ─────────────────────────────────────────────────────────
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

    # ── Background ingest (non-blocking queue put) ────────────────────────────
    try:
        ingest_queue.put_nowait({
            "CUSTOMER_ID": cust_id, "MERCHANT_ID": merch_id,
            "WALLET_DPAN": dpan,    "IP_ADDRESS":  ip_addr,
            "AMOUNT_USD":  amount,  "IS_GBR":      is_gbr,
            "EVENT_TS":    event_ts.strftime("%Y-%m-%dT%H:%M:%SZ"),
        })
    except queue.Full:
        pass  # Drop ingest if queue full — scoring latency is priority

    # ── Feature lookup (single synchronous call, no thread hop) ───────────────
    features, ofs_ms = _query_features(cust_id, merch_id, dpan, ip_addr)

    # ── Derived features + static fields ──────────────────────────────────────
    features.update(_compute_derived(features, amount, event_ts))
    features["PURCHASE_AMOUNT"] = amount
    features["IS_GBR"] = is_gbr
    features.setdefault("AUTHENTICATED_3DS_CHALLENGE_FLAG", 0)
    features.setdefault("IS_MERCHANT_INITIATED_PURCHASE", 0)

    # ── XGBoost predict ───────────────────────────────────────────────────────
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
