-- =============================================================================
-- SportZone Retail Analytics Platform
-- 07_sharing / 01_data_shares.sql
-- Purpose : Secure views + outbound data shares for downstream consumers
--           (Marketing agency, Inventory partner, Finance team)
-- Run as  : ACCOUNTADMIN  (share DDL requires ACCOUNTADMIN)
-- =============================================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE SPORTZONE_DEV;
USE SCHEMA REPORTING;

-- ===========================================================================
-- STEP 1 – Create secure views (consumers only see what they need)
-- ===========================================================================

-- ---------------------------------------------------------------------------
-- SALES_SECURE_VIEW  →  Marketing agency consumer
-- No unit cost / margin exposed; no PII beyond first name + city
-- ---------------------------------------------------------------------------

CREATE OR REPLACE SECURE VIEW REPORTING.SALES_SHARE_VIEW AS
SELECT
    d.FULL_DATE                         AS ORDER_DATE,
    d.YEAR,
    d.MONTH_NAME,
    d.QUARTER_NAME,
    f.CHANNEL,
    f.ORDER_STATUS,
    c.FIRST_NAME,                        -- first name only (no email/phone)
    c.CITY,
    c.STATE,
    c.COUNTRY,
    c.LOYALTY_TIER,
    c.AGE_BAND,
    c.GENDER,
    p.PRD_CATEGORY,
    p.PRD_SUBCATEGORY,
    p.BRAND,
    p.PRICE_TIER,
    s.STORE_TYPE,
    s.REGION,
    pr.PROMO_CODE,
    pr.PROMO_TYPE,
    f.QTY_ORDERED,
    f.GROSS_REVENUE,
    f.DISCOUNT_AMT,
    f.NET_REVENUE
    -- NOTE: UNIT_COST and GROSS_PROFIT intentionally excluded
FROM SPORTZONE_DEV.WAREHOUSE.FACT_SALES       f
JOIN SPORTZONE_DEV.WAREHOUSE.DIM_DATE         d  ON f.ORDER_DATE_KEY = d.DATE_KEY
JOIN SPORTZONE_DEV.WAREHOUSE.DIM_CUSTOMER     c  ON f.CUST_SK        = c.CUST_SK
                                                 AND c.IS_CURRENT     = TRUE
JOIN SPORTZONE_DEV.WAREHOUSE.DIM_PRODUCT      p  ON f.PRD_SK         = p.PRD_SK
                                                 AND p.IS_CURRENT     = TRUE
JOIN SPORTZONE_DEV.WAREHOUSE.DIM_STORE        s  ON f.STORE_SK       = s.STORE_SK
LEFT JOIN SPORTZONE_DEV.WAREHOUSE.DIM_PROMOTION pr ON f.PROMO_SK     = pr.PROMO_SK
WHERE f.ORDER_STATUS NOT IN ('CANCELLED', 'RETURNED');

-- ---------------------------------------------------------------------------
-- INVENTORY_SHARE_VIEW  →  Supplier / inventory partner
-- Exposes stock levels; no revenue or customer data
-- ---------------------------------------------------------------------------

CREATE OR REPLACE SECURE VIEW REPORTING.INVENTORY_SHARE_VIEW AS
SELECT
    d.FULL_DATE                         AS SNAPSHOT_DATE,
    p.PRD_ID,
    p.PRD_NAME,
    p.PRD_CATEGORY,
    p.SKU,
    p.BRAND,
    s.STORE_ID,
    s.STORE_NAME,
    s.REGION,
    s.COUNTRY,
    fi.QTY_ON_HAND,
    fi.QTY_AVAILABLE,
    fi.REORDER_POINT,
    fi.INV_STATUS,
    fi.NEEDS_REORDER,
    fi.STOCK_VALUE
    -- NOTE: no customer or order data
FROM SPORTZONE_DEV.WAREHOUSE.FACT_INVENTORY   fi
JOIN SPORTZONE_DEV.WAREHOUSE.DIM_DATE         d  ON fi.SNAPSHOT_DATE_KEY = d.DATE_KEY
JOIN SPORTZONE_DEV.WAREHOUSE.DIM_PRODUCT      p  ON fi.PRD_SK   = p.PRD_SK
                                                 AND p.IS_CURRENT = TRUE
JOIN SPORTZONE_DEV.WAREHOUSE.DIM_STORE        s  ON fi.STORE_SK  = s.STORE_SK;

-- ---------------------------------------------------------------------------
-- FINANCE_SHARE_VIEW  →  Finance / FP&A team (separate Snowflake account)
-- Full P&L metrics; no individual customer PII
-- ---------------------------------------------------------------------------

CREATE OR REPLACE SECURE VIEW REPORTING.FINANCE_SHARE_VIEW AS
SELECT
    d.FULL_DATE,
    d.YEAR,
    d.MONTH_NUM,
    d.MONTH_NAME,
    d.QUARTER_NAME,
    f.CHANNEL,
    p.PRD_CATEGORY,
    p.PRD_SUBCATEGORY,
    p.BRAND,
    p.PRICE_TIER,
    s.REGION,
    s.COUNTRY,
    s.STORE_TYPE,
    pr.PROMO_TYPE,
    -- Financial measures
    COUNT(DISTINCT f.ORDER_ID)              AS ORDER_COUNT,
    SUM(f.QTY_ORDERED)                      AS UNITS_SOLD,
    SUM(f.GROSS_REVENUE)                    AS GROSS_REVENUE,
    SUM(f.DISCOUNT_AMT)                     AS DISCOUNT_GIVEN,
    SUM(f.NET_REVENUE)                      AS NET_REVENUE,
    SUM(f.COGS)                             AS COGS,
    SUM(f.GROSS_PROFIT)                     AS GROSS_PROFIT,
    ROUND(SUM(f.GROSS_PROFIT)
         / NULLIF(SUM(f.NET_REVENUE),0)*100, 2) AS GROSS_MARGIN_PCT
FROM SPORTZONE_DEV.WAREHOUSE.FACT_SALES       f
JOIN SPORTZONE_DEV.WAREHOUSE.DIM_DATE         d  ON f.ORDER_DATE_KEY = d.DATE_KEY
JOIN SPORTZONE_DEV.WAREHOUSE.DIM_PRODUCT      p  ON f.PRD_SK         = p.PRD_SK
                                                 AND p.IS_CURRENT     = TRUE
JOIN SPORTZONE_DEV.WAREHOUSE.DIM_STORE        s  ON f.STORE_SK       = s.STORE_SK
LEFT JOIN SPORTZONE_DEV.WAREHOUSE.DIM_PROMOTION pr ON f.PROMO_SK     = pr.PROMO_SK
WHERE f.ORDER_STATUS NOT IN ('CANCELLED', 'RETURNED')
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14;

-- ===========================================================================
-- STEP 2 – Database role for sharing
-- ===========================================================================

CREATE DATABASE ROLE IF NOT EXISTS SPORTZONE_DEV.SHARE_CONSUMER
    COMMENT = 'Read-only role granted to outbound shares';

GRANT USAGE  ON SCHEMA  SPORTZONE_DEV.REPORTING                   TO DATABASE ROLE SPORTZONE_DEV.SHARE_CONSUMER;
GRANT SELECT ON VIEW    SPORTZONE_DEV.REPORTING.SALES_SHARE_VIEW  TO DATABASE ROLE SPORTZONE_DEV.SHARE_CONSUMER;
GRANT SELECT ON VIEW    SPORTZONE_DEV.REPORTING.INVENTORY_SHARE_VIEW TO DATABASE ROLE SPORTZONE_DEV.SHARE_CONSUMER;
GRANT SELECT ON VIEW    SPORTZONE_DEV.REPORTING.FINANCE_SHARE_VIEW   TO DATABASE ROLE SPORTZONE_DEV.SHARE_CONSUMER;

-- ===========================================================================
-- STEP 3 – Create shares
-- ===========================================================================

CREATE SHARE IF NOT EXISTS SPORTZONE_SALES_SHARE
    COMMENT = 'Sales data share – Marketing agency consumer';

CREATE SHARE IF NOT EXISTS SPORTZONE_INVENTORY_SHARE
    COMMENT = 'Inventory data share – Supplier partner';

CREATE SHARE IF NOT EXISTS SPORTZONE_FINANCE_SHARE
    COMMENT = 'Finance data share – Internal FP&A Snowflake account';

-- Grant database + database role to each share
GRANT USAGE ON DATABASE SPORTZONE_DEV TO SHARE SPORTZONE_SALES_SHARE;
GRANT USAGE ON DATABASE SPORTZONE_DEV TO SHARE SPORTZONE_INVENTORY_SHARE;
GRANT USAGE ON DATABASE SPORTZONE_DEV TO SHARE SPORTZONE_FINANCE_SHARE;

GRANT DATABASE ROLE SPORTZONE_DEV.SHARE_CONSUMER TO SHARE SPORTZONE_SALES_SHARE;
GRANT DATABASE ROLE SPORTZONE_DEV.SHARE_CONSUMER TO SHARE SPORTZONE_INVENTORY_SHARE;
GRANT DATABASE ROLE SPORTZONE_DEV.SHARE_CONSUMER TO SHARE SPORTZONE_FINANCE_SHARE;

-- ===========================================================================
-- STEP 4 – Reader account (for partners without a Snowflake account)
-- Replace ADMIN_NAME / ADMIN_PASSWORD before running in a real account
-- ===========================================================================

/*
CREATE MANAGED ACCOUNT SPORTZONE_SUPPLIER_READER
    ADMIN_NAME     = sz_reader_admin,
    ADMIN_PASSWORD = 'SportZone_Reader#2024',
    TYPE           = READER
    COMMENT        = 'Reader account for inventory supplier partner';
-- Output contains the reader account URL – note it down

-- Add reader account to inventory share:
ALTER SHARE SPORTZONE_INVENTORY_SHARE ADD ACCOUNTS = <reader_account_locator>;

-- ---- Inside the reader account ----
USE ROLE ACCOUNTADMIN;

CREATE OR REPLACE WAREHOUSE reader_wh
    WAREHOUSE_SIZE  = 'X-SMALL'
    AUTO_SUSPEND    = 60
    AUTO_RESUME     = TRUE
    INITIALLY_SUSPENDED = TRUE;

CREATE OR REPLACE USER reader_user
    PASSWORD            = 'Reader#2024!'
    DEFAULT_ROLE        = PUBLIC
    MUST_CHANGE_PASSWORD = FALSE;

GRANT USAGE ON WAREHOUSE reader_wh TO ROLE PUBLIC;

-- Accept the share and create a local database from it
SHOW SHARES;
-- Use Snowsight: Data → Private Sharing → Get Data → create DB from share

-- Query shared data
SELECT * FROM <shared_db>.REPORTING.INVENTORY_SHARE_VIEW LIMIT 20;
*/

-- ===========================================================================
-- STEP 5 – Resource monitor for reader account usage control
-- ===========================================================================

/*
CREATE RESOURCE MONITOR sportzone_reader_monitor
    WITH CREDIT_QUOTA = 10          -- 10 credits / month cap
    FREQUENCY        = MONTHLY
    START_TIMESTAMP  = IMMEDIATELY
    TRIGGERS
        ON 75 PERCENT DO NOTIFY
        ON 90 PERCENT DO NOTIFY
        ON 100 PERCENT DO SUSPEND;

ALTER WAREHOUSE reader_wh
    SET RESOURCE_MONITOR = sportzone_reader_monitor;
*/

-- ===========================================================================
-- STEP 6 – Validate shares
-- ===========================================================================

SHOW SHARES;

SHOW GRANTS TO SHARE SPORTZONE_SALES_SHARE;
SHOW GRANTS TO SHARE SPORTZONE_INVENTORY_SHARE;
SHOW GRANTS TO SHARE SPORTZONE_FINANCE_SHARE;
