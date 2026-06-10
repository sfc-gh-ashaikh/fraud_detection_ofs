-- =============================================================================
-- FRAUD DETECTION (ONLINE FEATURE STORE): Teardown
-- =============================================================================
-- Removes all objects created by setup.sql and the notebooks.
-- Run when the demo is complete.
-- =============================================================================

USE ROLE ACCOUNTADMIN;

-- Drop the Online Feature Store online service (created in NB02)
-- This drops the Postgres instance and all registered feature views
-- Must be dropped before dropping the database schemas
ALTER SESSION SET STATEMENT_TIMEOUT_IN_SECONDS = 120;

-- Drop Online Services (if they exist)
BEGIN
    CALL SYSTEM$DROP_ONLINE_SERVICE('FRAUD_DEMO_DEV.FEATURE_STORE');
EXCEPTION
    WHEN OTHER THEN NULL;
END;

BEGIN
    CALL SYSTEM$DROP_ONLINE_SERVICE('FRAUD_DEMO_PROD.FEATURE_STORE');
EXCEPTION
    WHEN OTHER THEN NULL;
END;

-- Drop compute pool (stops and removes SPCS nodes)
DROP COMPUTE POOL IF EXISTS FRAUD_OFS_CPU_POOL;

-- Drop warehouses
DROP WAREHOUSE IF EXISTS FRAUD_OFS_LOAD_WH;
DROP WAREHOUSE IF EXISTS FRAUD_OFS_TRAIN_WH;

-- Drop databases (cascades to all schemas, tables, stages, models)
DROP DATABASE IF EXISTS FRAUD_DEMO_DEV;
DROP DATABASE IF EXISTS FRAUD_DEMO_STAGING;
DROP DATABASE IF EXISTS FRAUD_DEMO_PROD;

-- Drop roles
DROP ROLE IF EXISTS FRAUD_DS_DEV;
DROP ROLE IF EXISTS FRAUD_MLOPS;

-- =============================================================================
-- TEARDOWN COMPLETE
-- All fraud detection demo objects have been removed.
-- =============================================================================
