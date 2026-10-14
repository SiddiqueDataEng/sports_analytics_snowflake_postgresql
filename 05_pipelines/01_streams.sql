-- =============================================================================
-- SportZone Retail Analytics Platform
-- 05_pipelines / 01_streams.sql
-- Purpose : Streams for change data capture on raw landing tables
-- Run as  : SPORTZONE_DATA_ENG
-- =============================================================================

USE ROLE SPORTZONE_DATA_ENG;
USE DATABASE SPORTZONE_DEV;
USE WAREHOUSE DEV_LOAD_WH;

-- ---------------------------------------------------------------------------
-- Stream on RAW_ORDERS – captures new/updated orders for incremental ELT
-- ---------------------------------------------------------------------------

CREATE OR REPLACE STREAM RAW.STREAM_ORDERS
    ON TABLE RAW.RAW_ORDERS
    COMMENT = 'CDC stream – new and updated rows in RAW_ORDERS';

-- ---------------------------------------------------------------------------
-- Stream on RAW_ORDER_ITEMS
-- ---------------------------------------------------------------------------

CREATE OR REPLACE STREAM RAW.STREAM_ORDER_ITEMS
    ON TABLE RAW.RAW_ORDER_ITEMS
    COMMENT = 'CDC stream – new order line items';

-- ---------------------------------------------------------------------------
-- Stream on RAW_CUSTOMERS – detect loyalty tier or address changes
-- ---------------------------------------------------------------------------

CREATE OR REPLACE STREAM RAW.STREAM_CUSTOMERS
    ON TABLE RAW.RAW_CUSTOMERS
    COMMENT = 'CDC stream – customer profile changes';

-- ---------------------------------------------------------------------------
-- Stream on RAW_INVENTORY – daily snapshot changes
-- ---------------------------------------------------------------------------

CREATE OR REPLACE STREAM RAW.STREAM_INVENTORY
    ON TABLE RAW.RAW_INVENTORY
    COMMENT = 'CDC stream – inventory level changes';

-- ---------------------------------------------------------------------------
-- How to query streams (sample)
-- ---------------------------------------------------------------------------

-- New orders since last pipeline run
SELECT
    ORDER_ID,
    ORDER_DATE,
    CUST_ID,
    ORDER_STATUS,
    NET_AMT,
    METADATA$ACTION,        -- INSERT | DELETE
    METADATA$ISUPDATE,      -- TRUE if this is the new side of an UPDATE
    METADATA$ROW_ID
FROM RAW.STREAM_ORDERS
WHERE METADATA$ACTION = 'INSERT';

-- All changes to customer profiles
SELECT *
FROM RAW.STREAM_CUSTOMERS
ORDER BY METADATA$ACTION, CUST_ID;

-- ---------------------------------------------------------------------------
-- STREAM_STAGING_ORDERS – second-level stream on STAGING for warehouse load
-- ---------------------------------------------------------------------------

CREATE OR REPLACE STREAM STAGING.STREAM_STG_ORDERS
    ON TABLE STAGING.STG_ORDERS
    COMMENT = 'CDC stream – cleansed orders ready for warehouse load';

CREATE OR REPLACE STREAM STAGING.STREAM_STG_ORDER_ITEMS
    ON TABLE STAGING.STG_ORDER_ITEMS
    COMMENT = 'CDC stream – cleansed order items ready for warehouse load';

-- List all streams
SHOW STREAMS IN DATABASE SPORTZONE_DEV;
