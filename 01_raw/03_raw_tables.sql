-- =============================================================================
-- SportZone Retail Analytics Platform
-- 01_raw / 03_raw_tables.sql
-- Purpose : Raw landing tables – exact shape of source files, no transforms
-- Run as  : SPORTZONE_DATA_ENG
-- =============================================================================

USE ROLE SPORTZONE_DATA_ENG;
USE DATABASE SPORTZONE_DEV;
USE SCHEMA RAW;
USE WAREHOUSE DEV_LOAD_WH;

-- ---------------------------------------------------------------------------
-- RAW_CUSTOMERS
-- Source: CRM system CSV export
-- ---------------------------------------------------------------------------

CREATE OR REPLACE TABLE RAW_CUSTOMERS (
    CUST_ID         STRING,
    FIRST_NAME      STRING,
    LAST_NAME       STRING,
    EMAIL           STRING,
    PHONE           STRING,
    DATE_OF_BIRTH   STRING,          -- kept as string; cast in STAGING
    GENDER          STRING,
    ADDRESS_LINE1   STRING,
    ADDRESS_LINE2   STRING,
    CITY            STRING,
    STATE           STRING,
    ZIP_CODE        STRING,
    COUNTRY         STRING,
    LOYALTY_TIER    STRING,          -- BRONZE | SILVER | GOLD | PLATINUM
    SIGNUP_DATE     STRING,
    IS_ACTIVE       STRING,
    -- Audit columns
    _SRC_FILE       STRING,
    _LOAD_TS        TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ---------------------------------------------------------------------------
-- RAW_PRODUCTS
-- Source: Product catalog CSV
-- ---------------------------------------------------------------------------

CREATE OR REPLACE TABLE RAW_PRODUCTS (
    PRD_ID          STRING,
    PRD_NAME        STRING,
    PRD_CATEGORY    STRING,          -- e.g. FOOTWEAR, APPAREL, EQUIPMENT
    PRD_SUBCATEGORY STRING,
    BRAND           STRING,
    UNIT_PRICE      STRING,
    UNIT_COST       STRING,
    SKU             STRING,
    SUPPLIER_ID     STRING,
    IS_ACTIVE       STRING,
    LAUNCH_DATE     STRING,
    DISCONTINUE_DT  STRING,
    -- Audit
    _SRC_FILE       STRING,
    _LOAD_TS        TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ---------------------------------------------------------------------------
-- RAW_STORES
-- Source: Store master CSV
-- ---------------------------------------------------------------------------

CREATE OR REPLACE TABLE RAW_STORES (
    STORE_ID        STRING,
    STORE_NAME      STRING,
    STORE_TYPE      STRING,          -- ONLINE | PHYSICAL
    REGION          STRING,
    ADDRESS         STRING,
    CITY            STRING,
    STATE           STRING,
    ZIP_CODE        STRING,
    COUNTRY         STRING,
    OPEN_DATE       STRING,
    MANAGER_NAME    STRING,
    -- Audit
    _SRC_FILE       STRING,
    _LOAD_TS        TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ---------------------------------------------------------------------------
-- RAW_ORDERS
-- Source: ERP pipe-delimited export
-- ---------------------------------------------------------------------------

CREATE OR REPLACE TABLE RAW_ORDERS (
    ORDER_ID        STRING,
    ORDER_DATE      STRING,
    CUST_ID         STRING,
    STORE_ID        STRING,
    CHANNEL         STRING,          -- ONLINE | IN-STORE
    ORDER_STATUS    STRING,          -- PENDING | COMPLETED | CANCELLED | RETURNED
    PROMO_CODE      STRING,
    TOTAL_AMT       STRING,
    DISCOUNT_AMT    STRING,
    NET_AMT         STRING,
    -- Audit
    _SRC_FILE       STRING,
    _LOAD_TS        TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ---------------------------------------------------------------------------
-- RAW_ORDER_ITEMS
-- Source: ERP pipe-delimited export (line items)
-- ---------------------------------------------------------------------------

CREATE OR REPLACE TABLE RAW_ORDER_ITEMS (
    ORDER_ITEM_ID   STRING,
    ORDER_ID        STRING,
    PRD_ID          STRING,
    QTY             STRING,
    UNIT_PRICE      STRING,
    DISCOUNT_PCT    STRING,
    LINE_TOTAL      STRING,
    -- Audit
    _SRC_FILE       STRING,
    _LOAD_TS        TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ---------------------------------------------------------------------------
-- RAW_INVENTORY
-- Source: Warehouse inventory snapshots CSV (daily)
-- ---------------------------------------------------------------------------

CREATE OR REPLACE TABLE RAW_INVENTORY (
    INVENTORY_ID    STRING,
    PRD_ID          STRING,
    STORE_ID        STRING,
    SNAPSHOT_DATE   STRING,
    QTY_ON_HAND     STRING,
    QTY_RESERVED    STRING,
    REORDER_POINT   STRING,
    INV_STATUS      STRING,          -- STOCKED | LIMITED | OUT_OF_STOCK
    -- Audit
    _SRC_FILE       STRING,
    _LOAD_TS        TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ---------------------------------------------------------------------------
-- RAW_PROMOTIONS
-- Source: Marketing promotions CSV
-- ---------------------------------------------------------------------------

CREATE OR REPLACE TABLE RAW_PROMOTIONS (
    PROMO_ID        STRING,
    PROMO_CODE      STRING,
    PROMO_NAME      STRING,
    PROMO_TYPE      STRING,          -- PERCENTAGE | FIXED | BOGO
    DISCOUNT_VALUE  STRING,
    START_DATE      STRING,
    END_DATE        STRING,
    MIN_ORDER_AMT   STRING,
    IS_ACTIVE       STRING,
    -- Audit
    _SRC_FILE       STRING,
    _LOAD_TS        TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ---------------------------------------------------------------------------
-- COPY INTO commands — load from internal stages
-- ---------------------------------------------------------------------------

-- Load customers
COPY INTO RAW_CUSTOMERS (
    CUST_ID, FIRST_NAME, LAST_NAME, EMAIL, PHONE, DATE_OF_BIRTH,
    GENDER, ADDRESS_LINE1, ADDRESS_LINE2, CITY, STATE, ZIP_CODE,
    COUNTRY, LOYALTY_TIER, SIGNUP_DATE, IS_ACTIVE, _SRC_FILE
)
FROM (
    SELECT
        $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16,
        METADATA$FILENAME
    FROM @stg_customers
)
FILE_FORMAT = csv_comma_fmt
ON_ERROR    = 'SKIP_FILE'
PURGE       = FALSE;

-- Load products
COPY INTO RAW_PRODUCTS (
    PRD_ID, PRD_NAME, PRD_CATEGORY, PRD_SUBCATEGORY, BRAND,
    UNIT_PRICE, UNIT_COST, SKU, SUPPLIER_ID, IS_ACTIVE,
    LAUNCH_DATE, DISCONTINUE_DT, _SRC_FILE
)
FROM (
    SELECT $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12, METADATA$FILENAME
    FROM @stg_products
)
FILE_FORMAT = csv_comma_fmt
ON_ERROR    = 'SKIP_FILE';

-- Load stores
COPY INTO RAW_STORES (
    STORE_ID, STORE_NAME, STORE_TYPE, REGION, ADDRESS,
    CITY, STATE, ZIP_CODE, COUNTRY, OPEN_DATE, MANAGER_NAME, _SRC_FILE
)
FROM (
    SELECT $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11, METADATA$FILENAME
    FROM @stg_stores
)
FILE_FORMAT = csv_comma_fmt
ON_ERROR    = 'SKIP_FILE';

-- Load orders (pipe-delimited)
COPY INTO RAW_ORDERS (
    ORDER_ID, ORDER_DATE, CUST_ID, STORE_ID, CHANNEL,
    ORDER_STATUS, PROMO_CODE, TOTAL_AMT, DISCOUNT_AMT, NET_AMT, _SRC_FILE
)
FROM (
    SELECT $1,$2,$3,$4,$5,$6,$7,$8,$9,$10, METADATA$FILENAME
    FROM @stg_orders
)
FILE_FORMAT = csv_pipe_fmt
ON_ERROR    = 'SKIP_FILE';

-- Load order items
COPY INTO RAW_ORDER_ITEMS (
    ORDER_ITEM_ID, ORDER_ID, PRD_ID, QTY,
    UNIT_PRICE, DISCOUNT_PCT, LINE_TOTAL, _SRC_FILE
)
FROM (
    SELECT $1,$2,$3,$4,$5,$6,$7, METADATA$FILENAME
    FROM @stg_orders
    PATTERN = '.*order_items.*'
)
FILE_FORMAT = csv_pipe_fmt
ON_ERROR    = 'SKIP_FILE';

-- Load inventory
COPY INTO RAW_INVENTORY (
    INVENTORY_ID, PRD_ID, STORE_ID, SNAPSHOT_DATE,
    QTY_ON_HAND, QTY_RESERVED, REORDER_POINT, INV_STATUS, _SRC_FILE
)
FROM (
    SELECT $1,$2,$3,$4,$5,$6,$7,$8, METADATA$FILENAME
    FROM @stg_inventory
)
FILE_FORMAT = csv_comma_fmt
ON_ERROR    = 'SKIP_FILE';

-- Load promotions
COPY INTO RAW_PROMOTIONS (
    PROMO_ID, PROMO_CODE, PROMO_NAME, PROMO_TYPE, DISCOUNT_VALUE,
    START_DATE, END_DATE, MIN_ORDER_AMT, IS_ACTIVE, _SRC_FILE
)
FROM (
    SELECT $1,$2,$3,$4,$5,$6,$7,$8,$9, METADATA$FILENAME
    FROM @stg_promotions
)
FILE_FORMAT = csv_comma_fmt
ON_ERROR    = 'SKIP_FILE';

-- Quick row-count checks
SELECT 'RAW_CUSTOMERS'   AS tbl, COUNT(*) AS cnt FROM RAW_CUSTOMERS   UNION ALL
SELECT 'RAW_PRODUCTS'    AS tbl, COUNT(*) AS cnt FROM RAW_PRODUCTS    UNION ALL
SELECT 'RAW_STORES'      AS tbl, COUNT(*) AS cnt FROM RAW_STORES      UNION ALL
SELECT 'RAW_ORDERS'      AS tbl, COUNT(*) AS cnt FROM RAW_ORDERS      UNION ALL
SELECT 'RAW_ORDER_ITEMS' AS tbl, COUNT(*) AS cnt FROM RAW_ORDER_ITEMS UNION ALL
SELECT 'RAW_INVENTORY'   AS tbl, COUNT(*) AS cnt FROM RAW_INVENTORY   UNION ALL
SELECT 'RAW_PROMOTIONS'  AS tbl, COUNT(*) AS cnt FROM RAW_PROMOTIONS;
