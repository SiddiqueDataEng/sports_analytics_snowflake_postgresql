-- =============================================================================
-- SportZone Retail Analytics Platform
-- 03_warehouse / 02_facts.sql
-- Purpose : Fact tables – sales transactions and inventory snapshots
-- Run as  : SPORTZONE_DATA_ENG
-- =============================================================================

USE ROLE SPORTZONE_DATA_ENG;
USE DATABASE SPORTZONE_DEV;
USE SCHEMA WAREHOUSE;
USE WAREHOUSE DEV_LOAD_WH;

-- ---------------------------------------------------------------------------
-- FACT_SALES  (grain: one row per order line item)
-- ---------------------------------------------------------------------------

CREATE OR REPLACE TABLE FACT_SALES (
    -- Surrogate key
    SALES_SK        INTEGER         NOT NULL AUTOINCREMENT PRIMARY KEY,

    -- Dimension foreign keys
    ORDER_DATE_KEY  INTEGER,        -- → DIM_DATE
    CUST_SK         INTEGER,        -- → DIM_CUSTOMER
    PRD_SK          INTEGER,        -- → DIM_PRODUCT
    STORE_SK        INTEGER,        -- → DIM_STORE
    PROMO_SK        INTEGER,        -- → DIM_PROMOTION

    -- Degenerate dimensions (keep for drill-through)
    ORDER_ID        VARCHAR(20),
    ORDER_ITEM_ID   VARCHAR(30),
    CHANNEL         VARCHAR(20),
    ORDER_STATUS    VARCHAR(20),

    -- Measures
    QTY_ORDERED     INTEGER,
    UNIT_PRICE      NUMBER(10,2),
    UNIT_COST       NUMBER(10,2),
    GROSS_MARGIN_PCT NUMBER(5,2),
    DISCOUNT_PCT    NUMBER(5,2),
    DISCOUNT_AMT    NUMBER(12,2),
    GROSS_REVENUE   NUMBER(12,2),   -- QTY * UNIT_PRICE
    NET_REVENUE     NUMBER(12,2),   -- LINE_TOTAL after discount
    COGS            NUMBER(12,2),   -- QTY * UNIT_COST
    GROSS_PROFIT    NUMBER(12,2),   -- NET_REVENUE - COGS

    -- Audit
    _LOAD_TS        TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP()
)
CLUSTER BY (ORDER_DATE_KEY, PRD_SK);  -- common filter + join keys

-- ---------------------------------------------------------------------------
-- FACT_INVENTORY  (grain: one row per product × store × snapshot date)
-- ---------------------------------------------------------------------------

CREATE OR REPLACE TABLE FACT_INVENTORY (
    INVENTORY_SK    INTEGER         NOT NULL AUTOINCREMENT PRIMARY KEY,

    -- Dimension FKs
    SNAPSHOT_DATE_KEY INTEGER,      -- → DIM_DATE
    PRD_SK          INTEGER,        -- → DIM_PRODUCT
    STORE_SK        INTEGER,        -- → DIM_STORE

    -- Degenerate
    INVENTORY_ID    VARCHAR(30),

    -- Measures
    QTY_ON_HAND     INTEGER,
    QTY_RESERVED    INTEGER,
    QTY_AVAILABLE   INTEGER,
    REORDER_POINT   INTEGER,
    INV_STATUS      VARCHAR(20),
    NEEDS_REORDER   BOOLEAN,
    STOCK_VALUE     NUMBER(14,2),   -- QTY_ON_HAND * UNIT_COST

    -- Audit
    _LOAD_TS        TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP()
)
CLUSTER BY (SNAPSHOT_DATE_KEY, PRD_SK);
