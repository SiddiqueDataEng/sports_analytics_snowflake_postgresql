-- =============================================================================
-- SportZone Retail Analytics Platform
-- 03_warehouse / 03_load_warehouse.sql
-- Purpose : MERGE logic to load staging → dimensions → facts (incremental)
-- Run as  : SPORTZONE_DATA_ENG
-- =============================================================================

USE ROLE SPORTZONE_DATA_ENG;
USE DATABASE SPORTZONE_DEV;
USE WAREHOUSE DEV_LOAD_WH;

-- ===========================================================================
-- STEP 1: LOAD DIM_CUSTOMER  (SCD Type 2)
-- ===========================================================================

-- Expire rows where loyalty tier, address, or active flag changed
UPDATE WAREHOUSE.DIM_CUSTOMER tgt
SET    tgt.EFF_END_DT  = CURRENT_DATE() - 1,
       tgt.IS_CURRENT  = FALSE
WHERE  tgt.IS_CURRENT  = TRUE
AND EXISTS (
    SELECT 1
    FROM STAGING.STG_CUSTOMERS src
    WHERE src.CUST_ID = tgt.CUST_ID
    AND (
        src.LOYALTY_TIER <> tgt.LOYALTY_TIER OR
        src.CITY         <> tgt.CITY         OR
        src.STATE        <> tgt.STATE        OR
        src.IS_ACTIVE    <> tgt.IS_ACTIVE
    )
);

-- Insert new / changed rows
INSERT INTO WAREHOUSE.DIM_CUSTOMER (
    CUST_ID, FIRST_NAME, LAST_NAME, FULL_NAME,
    EMAIL, PHONE, DATE_OF_BIRTH, AGE_BAND, GENDER,
    CITY, STATE, COUNTRY, LOYALTY_TIER,
    SIGNUP_DATE, IS_ACTIVE,
    EFF_START_DT, EFF_END_DT, IS_CURRENT
)
SELECT
    src.CUST_ID,
    src.FIRST_NAME,
    src.LAST_NAME,
    src.FULL_NAME,
    src.EMAIL,
    src.PHONE,
    src.DATE_OF_BIRTH,
    CASE
        WHEN DATEDIFF(YEAR, src.DATE_OF_BIRTH, CURRENT_DATE()) < 25 THEN '18-24'
        WHEN DATEDIFF(YEAR, src.DATE_OF_BIRTH, CURRENT_DATE()) < 35 THEN '25-34'
        WHEN DATEDIFF(YEAR, src.DATE_OF_BIRTH, CURRENT_DATE()) < 45 THEN '35-44'
        WHEN DATEDIFF(YEAR, src.DATE_OF_BIRTH, CURRENT_DATE()) < 55 THEN '45-54'
        ELSE '55+'
    END                                     AS AGE_BAND,
    src.GENDER,
    src.CITY,
    src.STATE,
    src.COUNTRY,
    src.LOYALTY_TIER,
    src.SIGNUP_DATE,
    src.IS_ACTIVE,
    CURRENT_DATE()                          AS EFF_START_DT,
    NULL                                    AS EFF_END_DT,
    TRUE                                    AS IS_CURRENT
FROM STAGING.STG_CUSTOMERS src
WHERE NOT EXISTS (
    SELECT 1
    FROM WAREHOUSE.DIM_CUSTOMER tgt
    WHERE tgt.CUST_ID    = src.CUST_ID
    AND   tgt.IS_CURRENT = TRUE
);

-- ===========================================================================
-- STEP 2: LOAD DIM_PRODUCT  (SCD Type 2 on price changes)
-- ===========================================================================

UPDATE WAREHOUSE.DIM_PRODUCT tgt
SET    tgt.EFF_END_DT = CURRENT_DATE() - 1,
       tgt.IS_CURRENT = FALSE
WHERE  tgt.IS_CURRENT = TRUE
AND EXISTS (
    SELECT 1 FROM STAGING.STG_PRODUCTS src
    WHERE src.PRD_ID = tgt.PRD_ID
    AND (src.UNIT_PRICE <> tgt.UNIT_PRICE OR src.IS_ACTIVE <> tgt.IS_ACTIVE)
);

INSERT INTO WAREHOUSE.DIM_PRODUCT (
    PRD_ID, PRD_NAME, PRD_CATEGORY, PRD_SUBCATEGORY, BRAND,
    SKU, UNIT_PRICE, UNIT_COST, GROSS_MARGIN_PCT,
    SUPPLIER_ID, PRICE_TIER, IS_ACTIVE,
    LAUNCH_DATE, DISCONTINUE_DT,
    EFF_START_DT, EFF_END_DT, IS_CURRENT
)
SELECT
    src.PRD_ID,
    src.PRD_NAME,
    src.PRD_CATEGORY,
    src.PRD_SUBCATEGORY,
    src.BRAND,
    src.SKU,
    src.UNIT_PRICE,
    src.UNIT_COST,
    src.GROSS_MARGIN_PCT,
    src.SUPPLIER_ID,
    CASE
        WHEN src.UNIT_PRICE < 50  THEN 'BUDGET'
        WHEN src.UNIT_PRICE < 120 THEN 'MID'
        ELSE 'PREMIUM'
    END                             AS PRICE_TIER,
    src.IS_ACTIVE,
    src.LAUNCH_DATE,
    src.DISCONTINUE_DT,
    CURRENT_DATE(),
    NULL,
    TRUE
FROM STAGING.STG_PRODUCTS src
WHERE NOT EXISTS (
    SELECT 1 FROM WAREHOUSE.DIM_PRODUCT tgt
    WHERE tgt.PRD_ID = src.PRD_ID AND tgt.IS_CURRENT = TRUE
);

-- ===========================================================================
-- STEP 3: LOAD DIM_STORE  (SCD Type 1 – upsert)
-- ===========================================================================

MERGE INTO WAREHOUSE.DIM_STORE tgt
USING STAGING.STG_STORES src
    ON tgt.STORE_ID = src.STORE_ID
WHEN MATCHED THEN UPDATE SET
    tgt.STORE_NAME   = src.STORE_NAME,
    tgt.STORE_TYPE   = src.STORE_TYPE,
    tgt.REGION       = src.REGION,
    tgt.CITY         = src.CITY,
    tgt.STATE        = src.STATE,
    tgt.COUNTRY      = src.COUNTRY,
    tgt.MANAGER_NAME = src.MANAGER_NAME,
    tgt._LOAD_TS     = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (
    STORE_ID, STORE_NAME, STORE_TYPE, REGION,
    CITY, STATE, COUNTRY, OPEN_DATE, MANAGER_NAME
)
VALUES (
    src.STORE_ID, src.STORE_NAME, src.STORE_TYPE, src.REGION,
    src.CITY, src.STATE, src.COUNTRY, src.OPEN_DATE, src.MANAGER_NAME
);

-- ===========================================================================
-- STEP 4: LOAD DIM_PROMOTION  (SCD Type 1)
-- ===========================================================================

MERGE INTO WAREHOUSE.DIM_PROMOTION tgt
USING STAGING.STG_PROMOTIONS src
    ON tgt.PROMO_ID = src.PROMO_ID
WHEN MATCHED THEN UPDATE SET
    tgt.PROMO_CODE    = src.PROMO_CODE,
    tgt.PROMO_NAME    = src.PROMO_NAME,
    tgt.PROMO_TYPE    = src.PROMO_TYPE,
    tgt.DISCOUNT_VALUE = src.DISCOUNT_VALUE,
    tgt.START_DATE    = src.START_DATE,
    tgt.END_DATE      = src.END_DATE,
    tgt.IS_ACTIVE     = src.IS_ACTIVE,
    tgt._LOAD_TS      = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (
    PROMO_ID, PROMO_CODE, PROMO_NAME, PROMO_TYPE,
    DISCOUNT_VALUE, START_DATE, END_DATE, MIN_ORDER_AMT, IS_ACTIVE
)
VALUES (
    src.PROMO_ID, src.PROMO_CODE, src.PROMO_NAME, src.PROMO_TYPE,
    src.DISCOUNT_VALUE, src.START_DATE, src.END_DATE, src.MIN_ORDER_AMT, src.IS_ACTIVE
);

-- ===========================================================================
-- STEP 5: LOAD FACT_SALES
-- ===========================================================================

-- Use MERGE to prevent duplicates on re-runs
MERGE INTO WAREHOUSE.FACT_SALES tgt
USING (
    SELECT
        TO_NUMBER(TO_CHAR(ord.ORDER_DATE, 'YYYYMMDD'))      AS ORDER_DATE_KEY,
        COALESCE(dc.CUST_SK,  -1)                           AS CUST_SK,
        COALESCE(dp.PRD_SK,   -1)                           AS PRD_SK,
        COALESCE(ds.STORE_SK, -1)                           AS STORE_SK,
        COALESCE(dpr.PROMO_SK, -1)                          AS PROMO_SK,
        ord.ORDER_ID,
        oi.ORDER_ITEM_ID,
        ord.CHANNEL,
        ord.ORDER_STATUS,
        oi.QTY                                              AS QTY_ORDERED,
        oi.UNIT_PRICE,
        dp_cost.UNIT_COST,
        dp_cost.GROSS_MARGIN_PCT,
        oi.DISCOUNT_PCT,
        oi.DISCOUNT_AMT,
        oi.QTY * oi.UNIT_PRICE                             AS GROSS_REVENUE,
        oi.LINE_TOTAL                                       AS NET_REVENUE,
        oi.QTY * COALESCE(dp_cost.UNIT_COST, 0)           AS COGS,
        oi.LINE_TOTAL - (oi.QTY * COALESCE(dp_cost.UNIT_COST, 0)) AS GROSS_PROFIT
    FROM STAGING.STG_ORDER_ITEMS  oi
    JOIN STAGING.STG_ORDERS       ord  ON oi.ORDER_ID = ord.ORDER_ID
    LEFT JOIN WAREHOUSE.DIM_CUSTOMER dc
           ON ord.CUST_ID = dc.CUST_ID AND dc.IS_CURRENT = TRUE
    LEFT JOIN WAREHOUSE.DIM_PRODUCT  dp
           ON oi.PRD_ID  = dp.PRD_ID  AND dp.IS_CURRENT  = TRUE
    LEFT JOIN WAREHOUSE.DIM_PRODUCT  dp_cost
           ON oi.PRD_ID  = dp_cost.PRD_ID AND dp_cost.IS_CURRENT = TRUE
    LEFT JOIN WAREHOUSE.DIM_STORE    ds
           ON ord.STORE_ID = ds.STORE_ID
    LEFT JOIN WAREHOUSE.DIM_PROMOTION dpr
           ON COALESCE(ord.PROMO_CODE,'NONE') = dpr.PROMO_CODE
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

-- ===========================================================================
-- STEP 6: LOAD FACT_INVENTORY
-- ===========================================================================

MERGE INTO WAREHOUSE.FACT_INVENTORY tgt
USING (
    SELECT
        TO_NUMBER(TO_CHAR(inv.SNAPSHOT_DATE, 'YYYYMMDD'))   AS SNAPSHOT_DATE_KEY,
        COALESCE(dp.PRD_SK,   -1)                           AS PRD_SK,
        COALESCE(ds.STORE_SK, -1)                           AS STORE_SK,
        inv.INVENTORY_ID,
        inv.QTY_ON_HAND,
        inv.QTY_RESERVED,
        inv.QTY_AVAILABLE,
        inv.REORDER_POINT,
        inv.INV_STATUS,
        inv.NEEDS_REORDER,
        inv.QTY_ON_HAND * COALESCE(dp.UNIT_COST, 0)        AS STOCK_VALUE
    FROM STAGING.STG_INVENTORY    inv
    LEFT JOIN WAREHOUSE.DIM_PRODUCT dp
           ON inv.PRD_ID   = dp.PRD_ID   AND dp.IS_CURRENT  = TRUE
    LEFT JOIN WAREHOUSE.DIM_STORE   ds
           ON inv.STORE_ID = ds.STORE_ID
) src
ON  tgt.INVENTORY_ID      = src.INVENTORY_ID
AND tgt.SNAPSHOT_DATE_KEY = src.SNAPSHOT_DATE_KEY
WHEN NOT MATCHED THEN INSERT (
    SNAPSHOT_DATE_KEY, PRD_SK, STORE_SK, INVENTORY_ID,
    QTY_ON_HAND, QTY_RESERVED, QTY_AVAILABLE, REORDER_POINT,
    INV_STATUS, NEEDS_REORDER, STOCK_VALUE
)
VALUES (
    src.SNAPSHOT_DATE_KEY, src.PRD_SK, src.STORE_SK, src.INVENTORY_ID,
    src.QTY_ON_HAND, src.QTY_RESERVED, src.QTY_AVAILABLE, src.REORDER_POINT,
    src.INV_STATUS, src.NEEDS_REORDER, src.STOCK_VALUE
);

-- ---------------------------------------------------------------------------
-- Validation
-- ---------------------------------------------------------------------------

SELECT 'DIM_CUSTOMER'   AS tbl, COUNT(*) FROM WAREHOUSE.DIM_CUSTOMER   WHERE IS_CURRENT = TRUE UNION ALL
SELECT 'DIM_PRODUCT'    AS tbl, COUNT(*) FROM WAREHOUSE.DIM_PRODUCT    WHERE IS_CURRENT = TRUE UNION ALL
SELECT 'DIM_STORE'      AS tbl, COUNT(*) FROM WAREHOUSE.DIM_STORE      UNION ALL
SELECT 'DIM_PROMOTION'  AS tbl, COUNT(*) FROM WAREHOUSE.DIM_PROMOTION  UNION ALL
SELECT 'DIM_DATE'       AS tbl, COUNT(*) FROM WAREHOUSE.DIM_DATE       UNION ALL
SELECT 'FACT_SALES'     AS tbl, COUNT(*) FROM WAREHOUSE.FACT_SALES     UNION ALL
SELECT 'FACT_INVENTORY' AS tbl, COUNT(*) FROM WAREHOUSE.FACT_INVENTORY;
