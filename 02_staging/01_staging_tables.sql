-- =============================================================================
-- SportZone Retail Analytics Platform
-- 02_staging / 01_staging_tables.sql
-- Purpose : Cleansed, typed, and deduplicated staging tables
-- Run as  : SPORTZONE_DATA_ENG
-- =============================================================================

USE ROLE SPORTZONE_DATA_ENG;
USE DATABASE SPORTZONE_DEV;
USE SCHEMA STAGING;
USE WAREHOUSE DEV_LOAD_WH;

-- ---------------------------------------------------------------------------
-- STG_CUSTOMERS
-- ---------------------------------------------------------------------------

CREATE OR REPLACE TABLE STG_CUSTOMERS (
    CUST_ID         VARCHAR(20)     NOT NULL,
    FIRST_NAME      VARCHAR(100),
    LAST_NAME       VARCHAR(100),
    FULL_NAME       VARCHAR(200),
    EMAIL           VARCHAR(255),
    PHONE           VARCHAR(30),
    DATE_OF_BIRTH   DATE,
    GENDER          VARCHAR(1),     -- M | F | U
    ADDRESS_LINE1   VARCHAR(200),
    ADDRESS_LINE2   VARCHAR(200),
    CITY            VARCHAR(100),
    STATE           VARCHAR(50),
    ZIP_CODE        VARCHAR(20),
    COUNTRY         VARCHAR(2),     -- ISO 2-letter
    LOYALTY_TIER    VARCHAR(20),
    SIGNUP_DATE     DATE,
    IS_ACTIVE       BOOLEAN,
    -- SCD tracking
    EFF_START_DT    DATE            DEFAULT CURRENT_DATE(),
    EFF_END_DT      DATE,
    IS_CURRENT      BOOLEAN         DEFAULT TRUE,
    -- Audit
    _SRC_FILE       STRING,
    _LOAD_TS        TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP()
);

-- ---------------------------------------------------------------------------
-- STG_PRODUCTS
-- ---------------------------------------------------------------------------

CREATE OR REPLACE TABLE STG_PRODUCTS (
    PRD_ID          VARCHAR(20)     NOT NULL,
    PRD_NAME        VARCHAR(255),
    PRD_CATEGORY    VARCHAR(100),
    PRD_SUBCATEGORY VARCHAR(100),
    BRAND           VARCHAR(100),
    UNIT_PRICE      NUMBER(10,2),
    UNIT_COST       NUMBER(10,2),
    GROSS_MARGIN_PCT NUMBER(5,2),
    SKU             VARCHAR(50),
    SUPPLIER_ID     VARCHAR(20),
    IS_ACTIVE       BOOLEAN,
    LAUNCH_DATE     DATE,
    DISCONTINUE_DT  DATE,
    -- Audit
    _LOAD_TS        TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP()
);

-- ---------------------------------------------------------------------------
-- STG_STORES
-- ---------------------------------------------------------------------------

CREATE OR REPLACE TABLE STG_STORES (
    STORE_ID        VARCHAR(20)     NOT NULL,
    STORE_NAME      VARCHAR(200),
    STORE_TYPE      VARCHAR(20),
    REGION          VARCHAR(50),
    ADDRESS         VARCHAR(255),
    CITY            VARCHAR(100),
    STATE           VARCHAR(50),
    ZIP_CODE        VARCHAR(20),
    COUNTRY         VARCHAR(2),
    OPEN_DATE       DATE,
    MANAGER_NAME    VARCHAR(200),
    -- Audit
    _LOAD_TS        TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP()
);

-- ---------------------------------------------------------------------------
-- STG_ORDERS
-- ---------------------------------------------------------------------------

CREATE OR REPLACE TABLE STG_ORDERS (
    ORDER_ID        VARCHAR(20)     NOT NULL,
    ORDER_DATE      DATE,
    ORDER_YEAR      INTEGER,
    ORDER_MONTH     INTEGER,
    ORDER_QUARTER   INTEGER,
    CUST_ID         VARCHAR(20),
    STORE_ID        VARCHAR(20),
    CHANNEL         VARCHAR(20),
    ORDER_STATUS    VARCHAR(20),
    PROMO_CODE      VARCHAR(50),
    TOTAL_AMT       NUMBER(12,2),
    DISCOUNT_AMT    NUMBER(12,2),
    NET_AMT         NUMBER(12,2),
    IS_DISCOUNTED   BOOLEAN,
    -- Audit
    _SRC_FILE       STRING,
    _LOAD_TS        TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP()
);

-- ---------------------------------------------------------------------------
-- STG_ORDER_ITEMS
-- ---------------------------------------------------------------------------

CREATE OR REPLACE TABLE STG_ORDER_ITEMS (
    ORDER_ITEM_ID   VARCHAR(30)     NOT NULL,
    ORDER_ID        VARCHAR(20),
    PRD_ID          VARCHAR(20),
    QTY             INTEGER,
    UNIT_PRICE      NUMBER(10,2),
    DISCOUNT_PCT    NUMBER(5,2),
    LINE_TOTAL      NUMBER(12,2),
    DISCOUNT_AMT    NUMBER(12,2),
    -- Audit
    _LOAD_TS        TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP()
);

-- ---------------------------------------------------------------------------
-- STG_INVENTORY
-- ---------------------------------------------------------------------------

CREATE OR REPLACE TABLE STG_INVENTORY (
    INVENTORY_ID    VARCHAR(30)     NOT NULL,
    PRD_ID          VARCHAR(20),
    STORE_ID        VARCHAR(20),
    SNAPSHOT_DATE   DATE,
    QTY_ON_HAND     INTEGER,
    QTY_RESERVED    INTEGER,
    QTY_AVAILABLE   INTEGER,        -- ON_HAND - RESERVED
    REORDER_POINT   INTEGER,
    INV_STATUS      VARCHAR(20),
    NEEDS_REORDER   BOOLEAN,
    -- Audit
    _LOAD_TS        TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP()
);

-- ---------------------------------------------------------------------------
-- STG_PROMOTIONS
-- ---------------------------------------------------------------------------

CREATE OR REPLACE TABLE STG_PROMOTIONS (
    PROMO_ID        VARCHAR(20)     NOT NULL,
    PROMO_CODE      VARCHAR(50),
    PROMO_NAME      VARCHAR(200),
    PROMO_TYPE      VARCHAR(30),
    DISCOUNT_VALUE  NUMBER(10,2),
    START_DATE      DATE,
    END_DATE        DATE,
    MIN_ORDER_AMT   NUMBER(10,2),
    IS_ACTIVE       BOOLEAN,
    -- Audit
    _LOAD_TS        TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP()
);
