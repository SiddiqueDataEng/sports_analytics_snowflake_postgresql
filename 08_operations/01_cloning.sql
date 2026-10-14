-- =============================================================================
-- SportZone Retail Analytics Platform
-- 08_operations / 01_cloning.sql
-- Purpose : Zero-copy cloning for DEV sandbox, hotfixes, and testing
-- Run as  : SPORTZONE_DATA_ENG
-- =============================================================================

USE ROLE SPORTZONE_DATA_ENG;
USE WAREHOUSE DEV_LOAD_WH;

-- ===========================================================================
-- USE CASE 1 – Clone PROD → DEV for pipeline development
-- Engineers work on an exact copy of prod data without touching prod
-- ===========================================================================

CREATE DATABASE IF NOT EXISTS SPORTZONE_DEV_SANDBOX
    CLONE SPORTZONE_DEV
    COMMENT = 'Zero-copy sandbox cloned from DEV for feature development';

-- Verify cloned objects
USE DATABASE SPORTZONE_DEV_SANDBOX;
SHOW SCHEMAS;
SHOW TABLES IN SCHEMA RAW;
SHOW TABLES IN SCHEMA WAREHOUSE;

-- Quick row count comparison
SELECT 'SANDBOX FACT_SALES' AS src, COUNT(*) AS cnt
FROM SPORTZONE_DEV_SANDBOX.WAREHOUSE.FACT_SALES
UNION ALL
SELECT 'DEV FACT_SALES', COUNT(*)
FROM SPORTZONE_DEV.WAREHOUSE.FACT_SALES;

-- ===========================================================================
-- USE CASE 2 – Clone a single schema for targeted testing
-- ===========================================================================

USE DATABASE SPORTZONE_DEV;

CREATE SCHEMA IF NOT EXISTS WAREHOUSE_TEST
    CLONE WAREHOUSE
    COMMENT = 'Schema clone – test new dimension load logic without risk';

-- Test a new ELT change against the clone
-- Once validated, apply to the real WAREHOUSE schema

-- ===========================================================================
-- USE CASE 3 – Clone a table for point-in-time recovery (time travel)
-- Useful if a bad pipeline run corrupts a fact table
-- ===========================================================================

-- Restore FACT_SALES to state 1 hour ago
CREATE OR REPLACE TABLE WAREHOUSE.FACT_SALES_BACKUP
    CLONE WAREHOUSE.FACT_SALES
    BEFORE (OFFSET => -3600)          -- 3600 seconds = 1 hour ago
    COMMENT = 'Pre-recovery backup cloned via time travel';

-- Inspect backup
SELECT COUNT(*) FROM WAREHOUSE.FACT_SALES_BACKUP;

-- Swap backup into place if needed (rename approach)
/*
ALTER TABLE WAREHOUSE.FACT_SALES         RENAME TO WAREHOUSE.FACT_SALES_BAD;
ALTER TABLE WAREHOUSE.FACT_SALES_BACKUP  RENAME TO WAREHOUSE.FACT_SALES;
DROP TABLE  WAREHOUSE.FACT_SALES_BAD;
*/

-- ===========================================================================
-- USE CASE 4 – Clone for QA/UAT environment
-- ===========================================================================

CREATE DATABASE IF NOT EXISTS SPORTZONE_QA
    CLONE SPORTZONE_DEV
    COMMENT = 'QA environment – cloned from DEV for release validation';

USE DATABASE SPORTZONE_QA;

-- Create a named internal stage + load a test file to the cloned table
CREATE OR REPLACE FILE FORMAT RAW.csv_fmt
    TYPE = CSV FIELD_DELIMITER = ',' SKIP_HEADER = 1
    NULL_IF = ('NULL','null','') EMPTY_FIELD_AS_NULL = TRUE;

CREATE STAGE IF NOT EXISTS RAW.stg_qa_customers
    FILE_FORMAT = RAW.csv_fmt
    COMMENT = 'QA stage for customer test files';

-- PUT and COPY (run PUT from SnowSQL CLI):
-- PUT file:///path/to/test_customers.csv @RAW.stg_qa_customers;
-- COPY INTO RAW.RAW_CUSTOMERS FROM @RAW.stg_qa_customers;

-- ===========================================================================
-- STORAGE COST – measure clone overhead
-- Clones share micro-partitions with the source; only changed data is billed
-- ===========================================================================

SELECT
    TABLE_CATALOG,
    TABLE_SCHEMA,
    TABLE_NAME,
    ROUND(ACTIVE_BYTES            / POWER(1024,3), 3) AS ACTIVE_GB,
    ROUND(TIME_TRAVEL_BYTES       / POWER(1024,3), 3) AS TIME_TRAVEL_GB,
    ROUND(FAILSAFE_BYTES          / POWER(1024,3), 3) AS FAILSAFE_GB,
    ROUND(CLONE_GROUP_BYTES       / POWER(1024,3), 3) AS CLONE_SHARED_GB
FROM SPORTZONE_DEV.INFORMATION_SCHEMA.TABLE_STORAGE_METRICS
ORDER BY ACTIVE_BYTES DESC;

-- Database-level storage
SELECT
    DATABASE_NAME,
    ROUND(AVERAGE_DATABASE_BYTES / POWER(1024,3), 3) AS AVG_SIZE_GB,
    ROUND(AVERAGE_FAILSAFE_BYTES / POWER(1024,3), 3) AS FAILSAFE_GB
FROM SNOWFLAKE.ACCOUNT_USAGE.DATABASE_STORAGE_USAGE_HISTORY
WHERE USAGE_DATE = CURRENT_DATE() - 1
ORDER BY AVERAGE_DATABASE_BYTES DESC;
