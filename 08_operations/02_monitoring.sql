-- =============================================================================
-- SportZone Retail Analytics Platform
-- 08_operations / 02_monitoring.sql
-- Purpose : Query performance, cost, pipeline health monitoring
-- Run as  : ACCOUNTADMIN / SPORTZONE_SYSADMIN
-- =============================================================================

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE DEV_QUERY_WH;

-- ===========================================================================
-- 1. PIPELINE HEALTH – Task run history (last 24 hours)
-- ===========================================================================

SELECT
    NAME                                        AS TASK_NAME,
    STATE,                                       -- SUCCEEDED | FAILED | SKIPPED
    QUERY_START_TIME,
    COMPLETED_TIME,
    DATEDIFF(SECOND, QUERY_START_TIME,
             COMPLETED_TIME)                    AS DURATION_SEC,
    ERROR_CODE,
    ERROR_MESSAGE
FROM TABLE(SNOWFLAKE.INFORMATION_SCHEMA.TASK_HISTORY(
    SCHEDULED_TIME_RANGE_START => DATEADD(HOUR, -24, CURRENT_TIMESTAMP()),
    RESULT_LIMIT               => 100
))
ORDER BY QUERY_START_TIME DESC;

-- Failed tasks in last 7 days
SELECT
    NAME,
    COUNT(*)                                    AS FAIL_COUNT,
    MAX(QUERY_START_TIME)                       AS LAST_FAILURE,
    MAX(ERROR_MESSAGE)                          AS LAST_ERROR
FROM SNOWFLAKE.ACCOUNT_USAGE.TASK_HISTORY
WHERE STATE = 'FAILED'
AND   SCHEDULED_TIME >= DATEADD(DAY, -7, CURRENT_TIMESTAMP())
GROUP BY NAME
ORDER BY FAIL_COUNT DESC;

-- ===========================================================================
-- 2. QUERY PERFORMANCE – Top 20 slowest queries (last 7 days)
-- ===========================================================================

SELECT
    QUERY_ID,
    QUERY_TEXT,
    USER_NAME,
    ROLE_NAME,
    WAREHOUSE_NAME,
    DATABASE_NAME,
    SCHEMA_NAME,
    EXECUTION_STATUS,
    ROUND(TOTAL_ELAPSED_TIME / 1000, 2)         AS ELAPSED_SEC,
    ROUND(BYTES_SCANNED / POWER(1024,3), 3)     AS GB_SCANNED,
    ROUND(BYTES_SPILLED_TO_REMOTE_STORAGE
         / POWER(1024,3), 3)                    AS GB_SPILLED,
    PARTITIONS_SCANNED,
    PARTITIONS_TOTAL,
    ROUND(PARTITIONS_SCANNED * 100.0
         / NULLIF(PARTITIONS_TOTAL,0), 1)       AS PARTITION_PRUNING_PCT,
    START_TIME
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE START_TIME    >= DATEADD(DAY, -7, CURRENT_TIMESTAMP())
AND   EXECUTION_STATUS = 'SUCCESS'
AND   TOTAL_ELAPSED_TIME > 0
ORDER BY TOTAL_ELAPSED_TIME DESC
LIMIT 20;

-- ===========================================================================
-- 3. WAREHOUSE CREDIT USAGE – per warehouse per day
-- ===========================================================================

SELECT
    WAREHOUSE_NAME,
    DATE_TRUNC('DAY', START_TIME)               AS USAGE_DATE,
    ROUND(SUM(CREDITS_USED), 4)                 AS TOTAL_CREDITS,
    ROUND(SUM(CREDITS_USED_COMPUTE), 4)         AS COMPUTE_CREDITS,
    ROUND(SUM(CREDITS_USED_CLOUD_SERVICES), 4)  AS CLOUD_SERVICES_CREDITS
FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
WHERE START_TIME >= DATEADD(DAY, -30, CURRENT_TIMESTAMP())
GROUP BY 1, 2
ORDER BY USAGE_DATE DESC, TOTAL_CREDITS DESC;

-- Credit spend by role (helps chargeback)
SELECT
    ROLE_NAME,
    WAREHOUSE_NAME,
    ROUND(SUM(CREDITS_USED_COMPUTE), 4)         AS COMPUTE_CREDITS_30D
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY qh
JOIN SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY wh
    ON qh.WAREHOUSE_ID = wh.WAREHOUSE_ID
    AND DATE_TRUNC('HOUR', qh.START_TIME) = DATE_TRUNC('HOUR', wh.START_TIME)
WHERE qh.START_TIME >= DATEADD(DAY, -30, CURRENT_TIMESTAMP())
GROUP BY 1, 2
ORDER BY COMPUTE_CREDITS_30D DESC;

-- ===========================================================================
-- 4. DATA FRESHNESS – confirm pipeline ran and loaded data today
-- ===========================================================================

SELECT
    'RAW_ORDERS'     AS TABLE_NAME,
    MAX(_LOAD_TS)    AS LATEST_LOAD,
    COUNT(*)         AS TOTAL_ROWS,
    COUNTIF(DATE(_LOAD_TS) = CURRENT_DATE()) AS ROWS_TODAY
FROM SPORTZONE_DEV.RAW.RAW_ORDERS
UNION ALL
SELECT
    'FACT_SALES',
    MAX(_LOAD_TS),
    COUNT(*),
    COUNTIF(DATE(_LOAD_TS) = CURRENT_DATE())
FROM SPORTZONE_DEV.WAREHOUSE.FACT_SALES
UNION ALL
SELECT
    'FACT_INVENTORY',
    MAX(_LOAD_TS),
    COUNT(*),
    COUNTIF(DATE(_LOAD_TS) = CURRENT_DATE())
FROM SPORTZONE_DEV.WAREHOUSE.FACT_INVENTORY;

-- ===========================================================================
-- 5. STREAM LAG – are streams consuming data fast enough?
-- ===========================================================================

SHOW STREAMS IN DATABASE SPORTZONE_DEV;

-- Streams with data waiting to be consumed
SELECT *
FROM TABLE(SNOWFLAKE.INFORMATION_SCHEMA.STREAM_READER_OFFSETS(
    'SPORTZONE_DEV', 'RAW'
))
WHERE HAS_DATA = TRUE;

-- ===========================================================================
-- 6. DATA QUALITY CHECKS
-- ===========================================================================

-- Orphan order items (no matching order header)
SELECT COUNT(*) AS ORPHAN_ITEMS
FROM SPORTZONE_DEV.STAGING.STG_ORDER_ITEMS oi
LEFT JOIN SPORTZONE_DEV.STAGING.STG_ORDERS ord ON oi.ORDER_ID = ord.ORDER_ID
WHERE ord.ORDER_ID IS NULL;

-- Orders with unknown customer in fact table
SELECT COUNT(*) AS UNKNOWN_CUSTOMER_SALES
FROM SPORTZONE_DEV.WAREHOUSE.FACT_SALES
WHERE CUST_SK = -1;

-- Products with NULL cost (would break margin calc)
SELECT PRD_ID, PRD_NAME, UNIT_PRICE, UNIT_COST
FROM SPORTZONE_DEV.WAREHOUSE.DIM_PRODUCT
WHERE UNIT_COST IS NULL AND IS_CURRENT = TRUE;

-- Duplicate orders in fact (should be 0)
SELECT ORDER_ITEM_ID, COUNT(*) AS CNT
FROM SPORTZONE_DEV.WAREHOUSE.FACT_SALES
GROUP BY ORDER_ITEM_ID
HAVING CNT > 1;

-- Revenue sanity: daily net revenue should be positive
SELECT
    ORDER_DATE_KEY,
    SUM(NET_REVENUE)  AS DAILY_NET_REV,
    SUM(GROSS_PROFIT) AS DAILY_GROSS_PROFIT
FROM SPORTZONE_DEV.WAREHOUSE.FACT_SALES
WHERE ORDER_STATUS NOT IN ('CANCELLED','RETURNED')
GROUP BY 1
HAVING DAILY_NET_REV < 0 OR DAILY_GROSS_PROFIT < 0
ORDER BY 1;

-- ===========================================================================
-- 7. RESOURCE MONITOR – guardrail against runaway credits
-- ===========================================================================

USE ROLE ACCOUNTADMIN;

CREATE OR REPLACE RESOURCE MONITOR sportzone_monthly_cap
    WITH CREDIT_QUOTA = 200             -- 200 credits / month ceiling
    FREQUENCY        = MONTHLY
    START_TIMESTAMP  = IMMEDIATELY
    TRIGGERS
        ON 50  PERCENT DO NOTIFY
        ON 80  PERCENT DO NOTIFY
        ON 95  PERCENT DO SUSPEND       -- suspend non-critical WH
        ON 100 PERCENT DO SUSPEND_IMMEDIATE;

-- Attach to all dev warehouses
ALTER WAREHOUSE DEV_LOAD_WH  SET RESOURCE_MONITOR = sportzone_monthly_cap;
ALTER WAREHOUSE DEV_QUERY_WH SET RESOURCE_MONITOR = sportzone_monthly_cap;

-- Check current consumption
SELECT *
FROM SNOWFLAKE.ACCOUNT_USAGE.RESOURCE_MONITORS
WHERE NAME = 'SPORTZONE_MONTHLY_CAP';
