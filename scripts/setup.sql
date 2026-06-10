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
GRANT USAGE ON WAREHOUSE FRAUD_OFS_LOAD_WH  TO ROLE FRAUD_MLOPS;
GRANT USAGE ON WAREHOUSE FRAUD_OFS_TRAIN_WH TO ROLE FRAUD_MLOPS;

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
    COMMENT = 'Production model artifacts';

-- =============================================================================
-- SETUP COMPLETE
-- =============================================================================
-- Next steps:
--   1. Set SNOWFLAKE_PAT environment variable (for Online FS REST API auth)
--      export SNOWFLAKE_PAT="<your_pat_token>"
--   2. Run nb01_data_generation.ipynb  (generates 12M transactions)
--   3. Run nb02_feature_store.ipynb    (sets up Online Feature Store)
--   4. Run nb03_training.ipynb         (trains XGBoost model)
--   5. Run nb04_serving.ipynb          (deploys SPCS endpoint)
--   6. Run nb05_monitoring.ipynb       (sets up monitoring + ROI analysis)
