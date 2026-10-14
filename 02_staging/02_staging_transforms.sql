-- =============================================================================
-- SportZone Retail Analytics Platform
-- 02_staging / 02_staging_transforms.sql
-- Purpose : ELT transforms – raw → staging (cleanse, cast, enrich)
-- Run as  : SPORTZONE_DATA_ENG
-- =============================================================================

USE ROLE SPORTZONE_DATA_ENG;
USE DATABASE SPORTZONE_DEV;
USE WAREHOUSE DEV_LOAD_WH;

-- ---------------------------------------------------------------------------
-- CUSTOMERS – cast types, derive full_name, deduplicate
-- ---------------------------------------------------------------------------

INSERT INTO STAGING.STG_CUSTOMERS
SELECT
    TRIM(CUST_ID)                                       AS CUST_ID,
    INITCAP(TRIM(FIRST_NAME))                           AS FIRST_NAME,
    INITCAP(TRIM(LAST_NAME))                            AS LAST_NAME,
    INITCAP(TRIM(FIRST_NAME)) || ' ' ||
        INITCAP(TRIM(LAST_NAME))                        AS FULL_NAME,
    LOWER(TRIM(EMAIL))                                  AS EMAIL,
    REGEXP_REPLACE(TRIM(PHONE), '[^0-9\\-\\+\\(\\) ]', '') AS PHONE,
    TRY_TO_DATE(DATE_OF_BIRTH, 'YYYY-MM-DD')            AS DATE_OF_BIRTH,
    UPPER(SUBSTR(TRIM(GENDER), 1, 1))                   AS GENDER,
    TRIM(ADDRESS_LINE1)                                 AS ADDRESS_LINE1,
    TRIM(ADDRESS_LINE2)                                 AS ADDRESS_LINE2,
    INITCAP(TRIM(CITY))                                 AS CITY,
    UPPER(TRIM(STATE))                                  AS STATE,
    TRIM(ZIP_CODE)                                      AS ZIP_CODE,
    UPPER(TRIM(COUNTRY))                                AS COUNTRY,
    UPPER(TRIM(LOYALTY_TIER))                           AS LOYALTY_TIER,
    TRY_TO_DATE(SIGNUP_DATE, 'YYYY-MM-DD')              AS SIGNUP_DATE,
    CASE UPPER(TRIM(IS_ACTIVE)) WHEN 'Y' THEN TRUE
                                WHEN '1' THEN TRUE
                                ELSE FALSE END          AS IS_ACTIVE,
    CURRENT_DATE()                                      AS EFF_START_DT,
    NULL                                                AS EFF_END_DT,
    TRUE                                                AS IS_CURRENT,
    _SRC_FILE,
    _LOAD_TS
FROM (
    -- Deduplicate: keep latest load per CUST_ID
    SELECT *, ROW_NUMBER() OVER (
        PARTITION BY TRIM(CUST_ID)
        ORDER BY _LOAD_TS DESC
    ) AS rn
    FROM RAW.RAW_CUSTOMERS
    WHERE TRIM(CUST_ID) IS NOT NULL
)
WHERE rn = 1;

-- ---------------------------------------------------------------------------
-- PRODUCTS – cast, compute margin
-- ---------------------------------------------------------------------------

INSERT INTO STAGING.STG_PRODUCTS
SELECT
    TRIM(PRD_ID),
    TRIM(PRD_NAME),
    UPPER(TRIM(PRD_CATEGORY)),
    UPPER(TRIM(PRD_SUBCATEGORY)),
    TRIM(BRAND),
    TRY_TO_DECIMAL(UNIT_PRICE, 10, 2),
    TRY_TO_DECIMAL(UNIT_COST, 10, 2),
    CASE
        WHEN TRY_TO_DECIMAL(UNIT_PRICE, 10, 2) > 0
        THEN ROUND((TRY_TO_DECIMAL(UNIT_PRICE,10,2)
                  - TRY_TO_DECIMAL(UNIT_COST, 10,2))
                 / TRY_TO_DECIMAL(UNIT_PRICE,10,2) * 100, 2)
        ELSE NULL
    END                                                 AS GROSS_MARGIN_PCT,
    UPPER(TRIM(SKU)),
    TRIM(SUPPLIER_ID),
    CASE UPPER(TRIM(IS_ACTIVE)) WHEN 'Y' THEN TRUE ELSE FALSE END,
    TRY_TO_DATE(LAUNCH_DATE,    'YYYY-MM-DD'),
    TRY_TO_DATE(DISCONTINUE_DT, 'YYYY-MM-DD'),
    CURRENT_TIMESTAMP()
FROM (
    SELECT *, ROW_NUMBER() OVER (
        PARTITION BY TRIM(PRD_ID) ORDER BY _LOAD_TS DESC
    ) AS rn
    FROM RAW.RAW_PRODUCTS
    WHERE TRIM(PRD_ID) IS NOT NULL
)
WHERE rn = 1;

-- ---------------------------------------------------------------------------
-- STORES
-- ---------------------------------------------------------------------------

INSERT INTO STAGING.STG_STORES
SELECT
    TRIM(STORE_ID),
    TRIM(STORE_NAME),
    UPPER(TRIM(STORE_TYPE)),
    UPPER(TRIM(REGION)),
    TRIM(ADDRESS),
    INITCAP(TRIM(CITY)),
    UPPER(TRIM(STATE)),
    TRIM(ZIP_CODE),
    UPPER(TRIM(COUNTRY)),
    TRY_TO_DATE(OPEN_DATE, 'YYYY-MM-DD'),
    TRIM(MANAGER_NAME),
    CURRENT_TIMESTAMP()
FROM (
    SELECT *, ROW_NUMBER() OVER (
        PARTITION BY TRIM(STORE_ID) ORDER BY _LOAD_TS DESC
    ) AS rn
    FROM RAW.RAW_STORES
    WHERE TRIM(STORE_ID) IS NOT NULL
)
WHERE rn = 1;

-- ---------------------------------------------------------------------------
-- ORDERS – cast, derive date parts, flag discounts
-- ---------------------------------------------------------------------------

INSERT INTO STAGING.STG_ORDERS
SELECT
    TRIM(ORDER_ID),
    TRY_TO_DATE(ORDER_DATE, 'YYYY-MM-DD')               AS ORDER_DATE,
    YEAR(TRY_TO_DATE(ORDER_DATE, 'YYYY-MM-DD'))          AS ORDER_YEAR,
    MONTH(TRY_TO_DATE(ORDER_DATE, 'YYYY-MM-DD'))         AS ORDER_MONTH,
    QUARTER(TRY_TO_DATE(ORDER_DATE, 'YYYY-MM-DD'))       AS ORDER_QUARTER,
    TRIM(CUST_ID),
    TRIM(STORE_ID),
    UPPER(TRIM(CHANNEL)),
    UPPER(TRIM(ORDER_STATUS)),
    NULLIF(TRIM(PROMO_CODE), ''),
    TRY_TO_DECIMAL(TOTAL_AMT,    12, 2),
    TRY_TO_DECIMAL(DISCOUNT_AMT, 12, 2),
    TRY_TO_DECIMAL(NET_AMT,      12, 2),
    CASE WHEN TRY_TO_DECIMAL(DISCOUNT_AMT, 12, 2) > 0
         THEN TRUE ELSE FALSE END                        AS IS_DISCOUNTED,
    _SRC_FILE,
    _LOAD_TS
FROM (
    SELECT *, ROW_NUMBER() OVER (
        PARTITION BY TRIM(ORDER_ID) ORDER BY _LOAD_TS DESC
    ) AS rn
    FROM RAW.RAW_ORDERS
    WHERE TRIM(ORDER_ID) IS NOT NULL
)
WHERE rn = 1;

-- ---------------------------------------------------------------------------
-- ORDER ITEMS – cast, compute discount amount
-- ---------------------------------------------------------------------------

INSERT INTO STAGING.STG_ORDER_ITEMS
SELECT
    TRIM(ORDER_ITEM_ID),
    TRIM(ORDER_ID),
    TRIM(PRD_ID),
    TRY_TO_NUMBER(QTY)                                  AS QTY,
    TRY_TO_DECIMAL(UNIT_PRICE,   10, 2),
    TRY_TO_DECIMAL(DISCOUNT_PCT, 5,  2),
    TRY_TO_DECIMAL(LINE_TOTAL,   12, 2),
    ROUND(
        TRY_TO_DECIMAL(UNIT_PRICE, 10, 2)
        * TRY_TO_NUMBER(QTY)
        * TRY_TO_DECIMAL(DISCOUNT_PCT, 5, 2) / 100
    , 2)                                                AS DISCOUNT_AMT,
    CURRENT_TIMESTAMP()
FROM (
    SELECT *, ROW_NUMBER() OVER (
        PARTITION BY TRIM(ORDER_ITEM_ID) ORDER BY _LOAD_TS DESC
    ) AS rn
    FROM RAW.RAW_ORDER_ITEMS
    WHERE TRIM(ORDER_ITEM_ID) IS NOT NULL
)
WHERE rn = 1;

-- ---------------------------------------------------------------------------
-- INVENTORY – compute available qty, flag reorder
-- ---------------------------------------------------------------------------

INSERT INTO STAGING.STG_INVENTORY
SELECT
    TRIM(INVENTORY_ID),
    TRIM(PRD_ID),
    TRIM(STORE_ID),
    TRY_TO_DATE(SNAPSHOT_DATE, 'YYYY-MM-DD'),
    TRY_TO_NUMBER(QTY_ON_HAND),
    TRY_TO_NUMBER(QTY_RESERVED),
    TRY_TO_NUMBER(QTY_ON_HAND)
        - TRY_TO_NUMBER(QTY_RESERVED)                  AS QTY_AVAILABLE,
    TRY_TO_NUMBER(REORDER_POINT),
    UPPER(TRIM(INV_STATUS)),
    CASE WHEN TRY_TO_NUMBER(QTY_ON_HAND)
              <= TRY_TO_NUMBER(REORDER_POINT)
         THEN TRUE ELSE FALSE END                       AS NEEDS_REORDER,
    CURRENT_TIMESTAMP()
FROM (
    SELECT *, ROW_NUMBER() OVER (
        PARTITION BY TRIM(INVENTORY_ID) ORDER BY _LOAD_TS DESC
    ) AS rn
    FROM RAW.RAW_INVENTORY
    WHERE TRIM(INVENTORY_ID) IS NOT NULL
)
WHERE rn = 1;

-- ---------------------------------------------------------------------------
-- PROMOTIONS
-- ---------------------------------------------------------------------------

INSERT INTO STAGING.STG_PROMOTIONS
SELECT
    TRIM(PROMO_ID),
    UPPER(TRIM(PROMO_CODE)),
    TRIM(PROMO_NAME),
    UPPER(TRIM(PROMO_TYPE)),
    TRY_TO_DECIMAL(DISCOUNT_VALUE, 10, 2),
    TRY_TO_DATE(START_DATE, 'YYYY-MM-DD'),
    TRY_TO_DATE(END_DATE,   'YYYY-MM-DD'),
    TRY_TO_DECIMAL(MIN_ORDER_AMT, 10, 2),
    CASE UPPER(TRIM(IS_ACTIVE)) WHEN 'Y' THEN TRUE ELSE FALSE END,
    CURRENT_TIMESTAMP()
FROM (
    SELECT *, ROW_NUMBER() OVER (
        PARTITION BY TRIM(PROMO_ID) ORDER BY _LOAD_TS DESC
    ) AS rn
    FROM RAW.RAW_PROMOTIONS
    WHERE TRIM(PROMO_ID) IS NOT NULL
)
WHERE rn = 1;

-- ---------------------------------------------------------------------------
-- Quick validation
-- ---------------------------------------------------------------------------

SELECT 'STG_CUSTOMERS'   AS tbl, COUNT(*) AS cnt FROM STAGING.STG_CUSTOMERS   UNION ALL
SELECT 'STG_PRODUCTS'    AS tbl, COUNT(*) AS cnt FROM STAGING.STG_PRODUCTS    UNION ALL
SELECT 'STG_STORES'      AS tbl, COUNT(*) AS cnt FROM STAGING.STG_STORES      UNION ALL
SELECT 'STG_ORDERS'      AS tbl, COUNT(*) AS cnt FROM STAGING.STG_ORDERS      UNION ALL
SELECT 'STG_ORDER_ITEMS' AS tbl, COUNT(*) AS cnt FROM STAGING.STG_ORDER_ITEMS UNION ALL
SELECT 'STG_INVENTORY'   AS tbl, COUNT(*) AS cnt FROM STAGING.STG_INVENTORY   UNION ALL
SELECT 'STG_PROMOTIONS'  AS tbl, COUNT(*) AS cnt FROM STAGING.STG_PROMOTIONS;
