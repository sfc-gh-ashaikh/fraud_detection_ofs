-- =============================================================================
-- FRAUD DETECTION (ONLINE FEATURE STORE): Infrastructure Setup
-- =============================================================================
-- Single-solution setup for the Online Feature Store architecture.
--
-- KEY DIFFERENCE FROM DT APPROACH:
--   - No DT warehouse (FRAUD_DEMO_WH) — the Online Feature Store replaces
--     Dynamic Tables for real-time feature serving entirely.
--   - Training warehouse (FRAUD_OFS_TRAIN_WH) is INITIALLY_SUSPENDED and
--     only runs for ~5 minutes per monthly retraining cycle.
--   - Total ongoing compute: Online Service (Postgres, ~$200-500/month)
--     + SPCS scoring pool (~$198/month). No 24/7 warehouse.
-- =============================================================================

USE ROLE ACCOUNTADMIN;

-- =============================================================================
-- SECTION 1: DATABASES
-- =============================================================================
CREATE DATABASE IF NOT EXISTS FRAUD_DEMO_DEV;
CREATE DATABASE IF NOT EXISTS FRAUD_DEMO_STAGING;
CREATE DATABASE IF NOT EXISTS FRAUD_DEMO_PROD;

-- =============================================================================
-- SECTION 2: SCHEMAS
-- =============================================================================
CREATE SCHEMA IF NOT EXISTS FRAUD_DEMO_DEV.TRANSACTIONS;
CREATE SCHEMA IF NOT EXISTS FRAUD_DEMO_DEV.FEATURES;
CREATE SCHEMA IF NOT EXISTS FRAUD_DEMO_DEV.ML;
CREATE SCHEMA IF NOT EXISTS FRAUD_DEMO_DEV.SERVING;
CREATE SCHEMA IF NOT EXISTS FRAUD_DEMO_DEV.MONITORING;

CREATE SCHEMA IF NOT EXISTS FRAUD_DEMO_STAGING.TRANSACTIONS;
CREATE SCHEMA IF NOT EXISTS FRAUD_DEMO_STAGING.FEATURES;
CREATE SCHEMA IF NOT EXISTS FRAUD_DEMO_STAGING.ML;
CREATE SCHEMA IF NOT EXISTS FRAUD_DEMO_STAGING.SERVING;
CREATE SCHEMA IF NOT EXISTS FRAUD_DEMO_STAGING.MONITORING;

CREATE SCHEMA IF NOT EXISTS FRAUD_DEMO_PROD.TRANSACTIONS;
CREATE SCHEMA IF NOT EXISTS FRAUD_DEMO_PROD.FEATURES;
CREATE SCHEMA IF NOT EXISTS FRAUD_DEMO_PROD.ML;
CREATE SCHEMA IF NOT EXISTS FRAUD_DEMO_PROD.SERVING;
CREATE SCHEMA IF NOT EXISTS FRAUD_DEMO_PROD.MONITORING;

-- =============================================================================
-- SECTION 3: WAREHOUSES
-- =============================================================================

-- FRAUD_OFS_LOAD_WH: One-time data generation (12M rows, ~3 min)
-- INITIALLY_SUSPENDED: only starts when NB01 runs. Suspends 60s after.
CREATE WAREHOUSE IF NOT EXISTS FRAUD_OFS_LOAD_WH
    WAREHOUSE_SIZE = 'LARGE'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'One-time bulk data generation. Suspend after NB01 completes.';

-- FRAUD_OFS_TRAIN_WH: Snowpark-Optimized MEDIUM for ML training
-- Runs ~5 minutes per monthly retraining cycle. Cost: ~0.5 credits/run.
-- WHY SNOWPARK-OPTIMIZED: 256GB dedicated RAM for 12M x 147 features in memory.
-- Cheaper AND more memory than Standard XLARGE (16 credits/hr, ~80GB usable).
-- INITIALLY_SUSPENDED: starts on demand, suspends 60s after training completes.
CREATE WAREHOUSE IF NOT EXISTS FRAUD_OFS_TRAIN_WH
    WAREHOUSE_SIZE = 'MEDIUM'
    WAREHOUSE_TYPE = 'SNOWPARK-OPTIMIZED'
    MAX_CONCURRENCY_LEVEL = 1
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'ML training only (~5 min/month). SP-Opt MEDIUM = 256GB dedicated RAM.';

-- FRAUD_OFS_SCORE_WH: Dedicated to Online Feature Store reads on the scoring path.
-- Standard XS, always-on (INITIALLY_SUSPENDED=FALSE, no AUTO_SUSPEND competition).
-- Isolation from training jobs is critical: if FRAUD_OFS_TRAIN_WH is running a
-- retraining job when a scoring request arrives, the feature read query would queue.
-- XS at 0.5 credits/hr (~$1.10/day) is the right trade-off for a payment scoring path.
CREATE WAREHOUSE IF NOT EXISTS FRAUD_OFS_SCORE_WH
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = FALSE
    COMMENT = 'Online Feature Store reads on scoring path. Dedicated, always-on.';

-- NOTE: No FRAUD_OFS_WH (general/DT warehouse).
-- The Online Feature Store handles real-time feature serving without a warehouse.
-- General queries use FRAUD_OFS_TRAIN_WH on demand (auto-resumes, auto-suspends).

-- =============================================================================
-- SECTION 4: COMPUTE POOL (SPCS Model Serving)
-- =============================================================================
-- CPU_X64_XS: right-sized for XGBoost inference at 60 txn/min.
-- 2 vCPU + 8GB RAM — sufficient for a loaded XGBoost model + request handling.
-- MIN=1 ensures always-warm (no cold-start on first scoring request).
-- MAX=2 provides burst capacity + HA.
CREATE COMPUTE POOL IF NOT EXISTS FRAUD_OFS_CPU_POOL
    MIN_NODES = 1
    MAX_NODES = 2
    INSTANCE_FAMILY = CPU_X64_XS
    COMMENT = 'Fraud model serving. XS right-sized for XGBoost inference at 60 txn/min.';

-- =============================================================================
-- SECTION 5: ROLES & GRANTS
-- =============================================================================
CREATE ROLE IF NOT EXISTS FRAUD_DS_DEV;
CREATE ROLE IF NOT EXISTS FRAUD_MLOPS;

GRANT ROLE FRAUD_DS_DEV TO ROLE SYSADMIN;
GRANT ROLE FRAUD_MLOPS TO ROLE SYSADMIN;

GRANT ROLE FRAUD_DS_DEV TO USER ASHAIKH;
GRANT ROLE FRAUD_MLOPS TO USER ASHAIKH;

GRANT ALL ON DATABASE FRAUD_DEMO_DEV TO ROLE FRAUD_DS_DEV;
GRANT ALL ON ALL SCHEMAS IN DATABASE FRAUD_DEMO_DEV TO ROLE FRAUD_DS_DEV;
GRANT USAGE ON DATABASE FRAUD_DEMO_STAGING TO ROLE FRAUD_DS_DEV;
GRANT USAGE ON ALL SCHEMAS IN DATABASE FRAUD_DEMO_STAGING TO ROLE FRAUD_DS_DEV;
GRANT USAGE ON DATABASE FRAUD_DEMO_PROD TO ROLE FRAUD_DS_DEV;
GRANT USAGE ON ALL SCHEMAS IN DATABASE FRAUD_DEMO_PROD TO ROLE FRAUD_DS_DEV;

GRANT ALL ON DATABASE FRAUD_DEMO_DEV TO ROLE FRAUD_MLOPS;
GRANT ALL ON ALL SCHEMAS IN DATABASE FRAUD_DEMO_DEV TO ROLE FRAUD_MLOPS;
GRANT ALL ON DATABASE FRAUD_DEMO_STAGING TO ROLE FRAUD_MLOPS;
GRANT ALL ON ALL SCHEMAS IN DATABASE FRAUD_DEMO_STAGING TO ROLE FRAUD_MLOPS;
GRANT ALL ON DATABASE FRAUD_DEMO_PROD TO ROLE FRAUD_MLOPS;
GRANT ALL ON ALL SCHEMAS IN DATABASE FRAUD_DEMO_PROD TO ROLE FRAUD_MLOPS;

GRANT USAGE ON WAREHOUSE FRAUD_OFS_LOAD_WH  TO ROLE FRAUD_DS_DEV;
GRANT USAGE ON WAREHOUSE FRAUD_OFS_TRAIN_WH TO ROLE FRAUD_DS_DEV;
GRANT USAGE ON WAREHOUSE FRAUD_OFS_SCORE_WH TO ROLE FRAUD_DS_DEV;
GRANT USAGE ON WAREHOUSE FRAUD_OFS_LOAD_WH  TO ROLE FRAUD_MLOPS;
GRANT USAGE ON WAREHOUSE FRAUD_OFS_TRAIN_WH TO ROLE FRAUD_MLOPS;
GRANT USAGE ON WAREHOUSE FRAUD_OFS_SCORE_WH TO ROLE FRAUD_MLOPS;

GRANT USAGE   ON COMPUTE POOL FRAUD_OFS_CPU_POOL TO ROLE FRAUD_MLOPS;
GRANT MONITOR ON COMPUTE POOL FRAUD_OFS_CPU_POOL TO ROLE FRAUD_DS_DEV;

GRANT ALL ON FUTURE TABLES  IN DATABASE FRAUD_DEMO_DEV TO ROLE FRAUD_DS_DEV;
GRANT ALL ON FUTURE VIEWS   IN DATABASE FRAUD_DEMO_DEV TO ROLE FRAUD_DS_DEV;
GRANT SELECT ON FUTURE TABLES IN DATABASE FRAUD_DEMO_STAGING TO ROLE FRAUD_DS_DEV;
GRANT SELECT ON FUTURE TABLES IN DATABASE FRAUD_DEMO_PROD    TO ROLE FRAUD_DS_DEV;

-- Online Feature Store requires CREATE SCHEMA on the feature store database
GRANT CREATE SCHEMA ON DATABASE FRAUD_DEMO_DEV  TO ROLE FRAUD_MLOPS;
GRANT CREATE SCHEMA ON DATABASE FRAUD_DEMO_PROD TO ROLE FRAUD_MLOPS;

-- =============================================================================
-- SECTION 6: STAGES
-- =============================================================================
CREATE STAGE IF NOT EXISTS FRAUD_DEMO_DEV.ML.MODEL_STAGE
    COMMENT = 'Model artifacts and experiment metadata';
CREATE STAGE IF NOT EXISTS FRAUD_DEMO_STAGING.ML.MODEL_STAGE
    COMMENT = 'Validated model artifacts awaiting production';
CREATE STAGE IF NOT EXISTS FRAUD_DEMO_PROD.ML.MODEL_STAGE
    COMMENT = 'Production model artifacts (fraud_model.json + feature_cols.json)';

-- =============================================================================
-- SECTION 7: IMAGE REPOSITORY (for SPCS scoring service)
-- =============================================================================
-- Stores the Docker image for the custom fraud scoring service.
-- nb04_serving.ipynb builds and pushes the image here before deploying.
CREATE IMAGE REPOSITORY IF NOT EXISTS FRAUD_DEMO_PROD.ML.FRAUD_SCORER_REPO
    COMMENT = 'Container images for the custom SPCS fraud scoring service';

GRANT READ  ON IMAGE REPOSITORY FRAUD_DEMO_PROD.ML.FRAUD_SCORER_REPO TO ROLE FRAUD_MLOPS;
GRANT WRITE ON IMAGE REPOSITORY FRAUD_DEMO_PROD.ML.FRAUD_SCORER_REPO TO ROLE FRAUD_MLOPS;

-- Required for the scoring service to expose a public HTTPS endpoint.
GRANT BIND SERVICE ENDPOINT ON ACCOUNT TO ROLE FRAUD_MLOPS;

-- =============================================================================
-- SETUP COMPLETE
-- =============================================================================
-- Run notebooks in this order:
--   1. nb01_data_generation.ipynb   — generates 12M synthetic transactions (~10 min)
--   2. nb02_feature_store.ipynb     — creates OFS online service + 5 feature views
--   3. nb03_training.ipynb          — trains XGBoost, exports model to stage
--   4. nb04_serving.ipynb           — builds Docker image, deploys SPCS scoring service
--   5. nb05_monitoring.ipynb        — sets up inference logging + model monitor
--   6. nb06_latency_proof.ipynb     — customer-facing freshness + latency demo
--
-- Prerequisites:
--   - Run this script as ACCOUNTADMIN before any notebook
--   - Docker Desktop must be running on your local machine for nb04 image build
