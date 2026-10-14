-- =============================================================================
-- SportZone Retail Analytics Platform
-- 05_pipelines / 02_tasks.sql
-- Purpose : Scheduled task pipelines (DAG) for automated ELT
-- Run as  : SPORTZONE_DATA_ENG
-- =============================================================================

USE ROLE SPORTZONE_DATA_ENG;
USE DATABASE SPORTZONE_DEV;
USE WAREHOUSE DEV_LOAD_WH;

-- ===========================================================================
-- PIPELINE DAG:
--
--   TASK_ROOT_TRIGGER  (every 30 min, CRON)
--         │
--         ├── TASK_RAW_TO_STAGING_ORDERS      (after root)
--         │         │
--         │         └── TASK_RAW_TO_STAGING_CUSTOMERS (after orders)
--         │
--         └── TASK_RAW_TO_STAGING_INVENTORY   (after root)
--                   │
--                   └── TASK_STAGING_TO_WAREHOUSE    (after both staging tasks)
--                             │
--                             └── TASK_REFRESH_MARTS (after warehouse load)
-- ===========================================================================

-- ---------------------------------------------------------------------------
-- ROOT TASK – fires the pipeline on schedule
-- ---------------------------------------------------------------------------

CREATE OR REPLACE TASK TASK_ROOT_TRIGGER
    SCHEDULE       = 'USING CRON 0,30 * * * * UTC'   -- every 30 minutes
    USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
    COMMENT        = 'Root trigger for SportZone ELT pipeline'
AS
    SELECT CURRENT_TIMESTAMP() AS pipeline_start;     -- no-op; just starts the DAG

-- ---------------------------------------------------------------------------
-- TASK 1: Raw Orders → Staging  (reads STREAM_ORDERS)
-- ---------------------------------------------------------------------------

CREATE OR REPLACE TASK TASK_RAW_TO_STAGING_ORDERS
    AFTER TASK_ROOT_TRIGGER
    USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
    WHEN SYSTEM$STREAM_HAS_DATA('RAW.STREAM_ORDERS')
       OR SYSTEM$STREAM_HAS_DATA('RAW.STREAM_ORDER_ITEMS')
    COMMENT = 'Load new orders from RAW streams to STAGING'
AS
BEGIN
    -- Transform orders
    INSERT INTO STAGING.STG_ORDERS
    SELECT
        TRIM(ORDER_ID),
        TRY_TO_DATE(ORDER_DATE,  'YYYY-MM-DD'),
        YEAR(TRY_TO_DATE(ORDER_DATE, 'YYYY-MM-DD')),
        MONTH(TRY_TO_DATE(ORDER_DATE, 'YYYY-MM-DD')),
        QUARTER(TRY_TO_DATE(ORDER_DATE, 'YYYY-MM-DD')),
        TRIM(CUST_ID),
        TRIM(STORE_ID),
        UPPER(TRIM(CHANNEL)),
        UPPER(TRIM(ORDER_STATUS)),
        NULLIF(TRIM(PROMO_CODE), ''),
        TRY_TO_DECIMAL(TOTAL_AMT,    12, 2),
        TRY_TO_DECIMAL(DISCOUNT_AMT, 12, 2),
        TRY_TO_DECIMAL(NET_AMT,      12, 2),
        CASE WHEN TRY_TO_DECIMAL(DISCOUNT_AMT, 12, 2) > 0 THEN TRUE ELSE FALSE END,
        _SRC_FILE,
        CURRENT_TIMESTAMP()
    FROM RAW.STREAM_ORDERS
    WHERE METADATA$ACTION = 'INSERT'
    AND   METADATA$ISUPDATE = FALSE;

    -- Transform order items
    INSERT INTO STAGING.STG_ORDER_ITEMS
    SELECT
        TRIM(ORDER_ITEM_ID),
        TRIM(ORDER_ID),
        TRIM(PRD_ID),
        TRY_TO_NUMBER(QTY),
        TRY_TO_DECIMAL(UNIT_PRICE,   10, 2),
        TRY_TO_DECIMAL(DISCOUNT_PCT, 5,  2),
        TRY_TO_DECIMAL(LINE_TOTAL,   12, 2),
        ROUND(TRY_TO_DECIMAL(UNIT_PRICE,10,2)
            * TRY_TO_NUMBER(QTY)
            * TRY_TO_DECIMAL(DISCOUNT_PCT,5,2) / 100, 2),
        CURRENT_TIMESTAMP()
    FROM RAW.STREAM_ORDER_ITEMS
    WHERE METADATA$ACTION = 'INSERT'
    AND   METADATA$ISUPDATE = FALSE;
END;

-- ---------------------------------------------------------------------------
-- TASK 2: Raw Customers → Staging  (reads STREAM_CUSTOMERS)
-- ---------------------------------------------------------------------------

CREATE OR REPLACE TASK TASK_RAW_TO_STAGING_CUSTOMERS
    AFTER TASK_RAW_TO_STAGING_ORDERS
    USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
    WHEN SYSTEM$STREAM_HAS_DATA('RAW.STREAM_CUSTOMERS')
    COMMENT = 'Load changed customer profiles from RAW to STAGING'
AS
    INSERT INTO STAGING.STG_CUSTOMERS (
        CUST_ID, FIRST_NAME, LAST_NAME, FULL_NAME, EMAIL, PHONE,
        DATE_OF_BIRTH, GENDER, ADDRESS_LINE1, ADDRESS_LINE2,
        CITY, STATE, ZIP_CODE, COUNTRY,
        LOYALTY_TIER, SIGNUP_DATE, IS_ACTIVE,
        EFF_START_DT, EFF_END_DT, IS_CURRENT
    )
    SELECT
        TRIM(CUST_ID),
        INITCAP(TRIM(FIRST_NAME)),
        INITCAP(TRIM(LAST_NAME)),
        INITCAP(TRIM(FIRST_NAME)) || ' ' || INITCAP(TRIM(LAST_NAME)),
        LOWER(TRIM(EMAIL)),
        REGEXP_REPLACE(TRIM(PHONE), '[^0-9\\-\\+\\(\\) ]', ''),
        TRY_TO_DATE(DATE_OF_BIRTH, 'YYYY-MM-DD'),
        UPPER(SUBSTR(TRIM(GENDER), 1, 1)),
        TRIM(ADDRESS_LINE1),
        TRIM(ADDRESS_LINE2),
        INITCAP(TRIM(CITY)),
        UPPER(TRIM(STATE)),
        TRIM(ZIP_CODE),
        UPPER(TRIM(COUNTRY)),
        UPPER(TRIM(LOYALTY_TIER)),
        TRY_TO_DATE(SIGNUP_DATE, 'YYYY-MM-DD'),
        CASE UPPER(TRIM(IS_ACTIVE)) WHEN 'Y' THEN TRUE ELSE FALSE END,
        CURRENT_DATE(),
        NULL,
        TRUE
    FROM RAW.STREAM_CUSTOMERS
    WHERE METADATA$ACTION = 'INSERT';

-- ---------------------------------------------------------------------------
-- TASK 3: Raw Inventory → Staging
-- ---------------------------------------------------------------------------

CREATE OR REPLACE TASK TASK_RAW_TO_STAGING_INVENTORY
    AFTER TASK_ROOT_TRIGGER
    USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
    WHEN SYSTEM$STREAM_HAS_DATA('RAW.STREAM_INVENTORY')
    COMMENT = 'Load inventory snapshots from RAW to STAGING'
AS
    INSERT INTO STAGING.STG_INVENTORY
    SELECT
        TRIM(INVENTORY_ID),
        TRIM(PRD_ID),
        TRIM(STORE_ID),
        TRY_TO_DATE(SNAPSHOT_DATE, 'YYYY-MM-DD'),
        TRY_TO_NUMBER(QTY_ON_HAND),
        TRY_TO_NUMBER(QTY_RESERVED),
        TRY_TO_NUMBER(QTY_ON_HAND) - TRY_TO_NUMBER(QTY_RESERVED),
        TRY_TO_NUMBER(REORDER_POINT),
        UPPER(TRIM(INV_STATUS)),
        CASE WHEN TRY_TO_NUMBER(QTY_ON_HAND)
                  <= TRY_TO_NUMBER(REORDER_POINT)
             THEN TRUE ELSE FALSE END,
        CURRENT_TIMESTAMP()
    FROM RAW.STREAM_INVENTORY
    WHERE METADATA$ACTION = 'INSERT';

-- ---------------------------------------------------------------------------
-- TASK 4: Staging → Warehouse  (dimension + fact loads)
-- Runs after BOTH staging tasks complete
-- ---------------------------------------------------------------------------

CREATE OR REPLACE TASK TASK_STAGING_TO_WAREHOUSE
    AFTER TASK_RAW_TO_STAGING_CUSTOMERS, TASK_RAW_TO_STAGING_INVENTORY
    USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'SMALL'
    WHEN SYSTEM$STREAM_HAS_DATA('STAGING.STREAM_STG_ORDERS')
       OR SYSTEM$STREAM_HAS_DATA('STAGING.STREAM_STG_ORDER_ITEMS')
    COMMENT = 'Load dimensions and facts from STAGING to WAREHOUSE'
AS
BEGIN
    -- Re-use merge logic from 03_warehouse/03_load_warehouse.sql
    -- (abbreviated here – in production call a stored procedure)

    -- FACT_SALES incremental
    MERGE INTO WAREHOUSE.FACT_SALES tgt
    USING (
        SELECT
            TO_NUMBER(TO_CHAR(ord.ORDER_DATE, 'YYYYMMDD'))  AS ORDER_DATE_KEY,
            COALESCE(dc.CUST_SK,  -1)                       AS CUST_SK,
            COALESCE(dp.PRD_SK,   -1)                       AS PRD_SK,
            COALESCE(ds.STORE_SK, -1)                       AS STORE_SK,
            COALESCE(dpr.PROMO_SK,-1)                       AS PROMO_SK,
            ord.ORDER_ID, oi.ORDER_ITEM_ID,
            ord.CHANNEL, ord.ORDER_STATUS,
            oi.QTY                                          AS QTY_ORDERED,
            oi.UNIT_PRICE, dp.UNIT_COST, dp.GROSS_MARGIN_PCT,
            oi.DISCOUNT_PCT, oi.DISCOUNT_AMT,
            oi.QTY * oi.UNIT_PRICE                         AS GROSS_REVENUE,
            oi.LINE_TOTAL                                   AS NET_REVENUE,
            oi.QTY * COALESCE(dp.UNIT_COST, 0)             AS COGS,
            oi.LINE_TOTAL - oi.QTY * COALESCE(dp.UNIT_COST,0) AS GROSS_PROFIT
        FROM STAGING.STREAM_STG_ORDER_ITEMS oi
        JOIN STAGING.STG_ORDERS             ord ON oi.ORDER_ID = ord.ORDER_ID
        LEFT JOIN WAREHOUSE.DIM_CUSTOMER    dc  ON ord.CUST_ID = dc.CUST_ID AND dc.IS_CURRENT = TRUE
        LEFT JOIN WAREHOUSE.DIM_PRODUCT     dp  ON oi.PRD_ID   = dp.PRD_ID  AND dp.IS_CURRENT = TRUE
        LEFT JOIN WAREHOUSE.DIM_STORE       ds  ON ord.STORE_ID = ds.STORE_ID
        LEFT JOIN WAREHOUSE.DIM_PROMOTION   dpr ON COALESCE(ord.PROMO_CODE,'NONE') = dpr.PROMO_CODE
        WHERE oi.METADATA$ACTION = 'INSERT'
    ) src
    ON tgt.ORDER_ITEM_ID = src.ORDER_ITEM_ID
    WHEN NOT MATCHED THEN INSERT (
        ORDER_DATE_KEY, CUST_SK, PRD_SK, STORE_SK, PROMO_SK,
        ORDER_ID, ORDER_ITEM_ID, CHANNEL, ORDER_STATUS,
        QTY_ORDERED, UNIT_PRICE, UNIT_COST, GROSS_MARGIN_PCT,
        DISCOUNT_PCT, DISCOUNT_AMT, GROSS_REVENUE, NET_REVENUE, COGS, GROSS_PROFIT
    )
    VALUES (
        src.ORDER_DATE_KEY, src.CUST_SK, src.PRD_SK, src.STORE_SK, src.PROMO_SK,
        src.ORDER_ID, src.ORDER_ITEM_ID, src.CHANNEL, src.ORDER_STATUS,
        src.QTY_ORDERED, src.UNIT_PRICE, src.UNIT_COST, src.GROSS_MARGIN_PCT,
        src.DISCOUNT_PCT, src.DISCOUNT_AMT, src.GROSS_REVENUE, src.NET_REVENUE,
        src.COGS, src.GROSS_PROFIT
    );
END;

-- ---------------------------------------------------------------------------
-- TASK 5: Refresh mart aggregations (materialized table approach)
-- ---------------------------------------------------------------------------

CREATE OR REPLACE TASK TASK_REFRESH_MARTS
    AFTER TASK_STAGING_TO_WAREHOUSE
    USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
    COMMENT = 'Refresh materialized mart aggregations'
AS
    -- Snowflake dynamic tables are ideal here (requires Enterprise tier)
    -- For standard tier, run a truncate-reload of key aggregation tables
    SELECT CURRENT_TIMESTAMP() AS marts_refreshed_at;

-- ---------------------------------------------------------------------------
-- ACTIVATE all tasks (bottom-up)
-- ---------------------------------------------------------------------------

ALTER TASK TASK_REFRESH_MARTS          RESUME;
ALTER TASK TASK_STAGING_TO_WAREHOUSE   RESUME;
ALTER TASK TASK_RAW_TO_STAGING_INVENTORY  RESUME;
ALTER TASK TASK_RAW_TO_STAGING_CUSTOMERS  RESUME;
ALTER TASK TASK_RAW_TO_STAGING_ORDERS     RESUME;
ALTER TASK TASK_ROOT_TRIGGER           RESUME;  -- start root LAST

-- Monitor tasks
SELECT
    NAME,
    STATE,
    SCHEDULE,
    CONDITION,
    LAST_COMMITTED_ON,
    LAST_SUSPENDED_ON
FROM TABLE(INFORMATION_SCHEMA.TASK_DEPENDENTS('TASK_ROOT_TRIGGER', FALSE));

-- Task run history
SELECT
    NAME,
    STATE,
    QUERY_START_TIME,
    COMPLETED_TIME,
    ERROR_MESSAGE
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
    SCHEDULED_TIME_RANGE_START => DATEADD(HOUR, -24, CURRENT_TIMESTAMP()),
    RESULT_LIMIT               => 50
))
ORDER BY QUERY_START_TIME DESC;
