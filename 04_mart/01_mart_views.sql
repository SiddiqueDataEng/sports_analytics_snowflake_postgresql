-- =============================================================================
-- SportZone Retail Analytics Platform
-- 04_mart / 01_mart_views.sql
-- Purpose : Business-specific views for analysts and BI tools
-- Run as  : SPORTZONE_DATA_ENG
-- =============================================================================

USE ROLE SPORTZONE_DATA_ENG;
USE DATABASE SPORTZONE_DEV;
USE SCHEMA MART;
USE WAREHOUSE DEV_QUERY_WH;

-- ---------------------------------------------------------------------------
-- MART_DAILY_SALES_SUMMARY
-- Grain: one row per day × channel × category
-- ---------------------------------------------------------------------------

CREATE OR REPLACE VIEW MART_DAILY_SALES_SUMMARY AS
SELECT
    d.FULL_DATE,
    d.YEAR,
    d.MONTH_NAME,
    d.QUARTER_NAME,
    d.DAY_NAME,
    d.IS_WEEKEND,
    f.CHANNEL,
    p.PRD_CATEGORY,
    p.PRD_SUBCATEGORY,
    p.BRAND,
    s.REGION,
    s.COUNTRY,
    COUNT(DISTINCT f.ORDER_ID)              AS ORDER_COUNT,
    SUM(f.QTY_ORDERED)                      AS UNITS_SOLD,
    SUM(f.GROSS_REVENUE)                    AS GROSS_REVENUE,
    SUM(f.DISCOUNT_AMT)                     AS TOTAL_DISCOUNT,
    SUM(f.NET_REVENUE)                      AS NET_REVENUE,
    SUM(f.COGS)                             AS COGS,
    SUM(f.GROSS_PROFIT)                     AS GROSS_PROFIT,
    ROUND(SUM(f.GROSS_PROFIT)
         / NULLIF(SUM(f.NET_REVENUE), 0) * 100, 2) AS GROSS_MARGIN_PCT,
    ROUND(SUM(f.NET_REVENUE)
         / NULLIF(COUNT(DISTINCT f.ORDER_ID), 0), 2) AS AVG_ORDER_VALUE
FROM WAREHOUSE.FACT_SALES       f
JOIN WAREHOUSE.DIM_DATE         d  ON f.ORDER_DATE_KEY  = d.DATE_KEY
JOIN WAREHOUSE.DIM_PRODUCT      p  ON f.PRD_SK          = p.PRD_SK
JOIN WAREHOUSE.DIM_STORE        s  ON f.STORE_SK        = s.STORE_SK
WHERE f.ORDER_STATUS NOT IN ('CANCELLED', 'RETURNED')
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12;

-- ---------------------------------------------------------------------------
-- MART_CUSTOMER_360
-- One row per current customer with lifetime value stats
-- ---------------------------------------------------------------------------

CREATE OR REPLACE VIEW MART_CUSTOMER_360 AS
SELECT
    c.CUST_ID,
    c.FULL_NAME,
    c.EMAIL,
    c.AGE_BAND,
    c.GENDER,
    c.CITY,
    c.STATE,
    c.COUNTRY,
    c.LOYALTY_TIER,
    c.SIGNUP_DATE,
    c.IS_ACTIVE,
    -- Lifetime metrics
    COUNT(DISTINCT f.ORDER_ID)              AS TOTAL_ORDERS,
    SUM(f.QTY_ORDERED)                      AS TOTAL_UNITS,
    SUM(f.NET_REVENUE)                      AS LIFETIME_VALUE,
    ROUND(SUM(f.NET_REVENUE)
         / NULLIF(COUNT(DISTINCT f.ORDER_ID), 0), 2) AS AVG_ORDER_VALUE,
    MIN(d.FULL_DATE)                        AS FIRST_ORDER_DATE,
    MAX(d.FULL_DATE)                        AS LAST_ORDER_DATE,
    DATEDIFF(DAY, MAX(d.FULL_DATE), CURRENT_DATE()) AS DAYS_SINCE_LAST_ORDER,
    -- RFM buckets (simple)
    CASE
        WHEN DATEDIFF(DAY, MAX(d.FULL_DATE), CURRENT_DATE()) <= 30  THEN 'ACTIVE'
        WHEN DATEDIFF(DAY, MAX(d.FULL_DATE), CURRENT_DATE()) <= 90  THEN 'AT_RISK'
        WHEN DATEDIFF(DAY, MAX(d.FULL_DATE), CURRENT_DATE()) <= 180 THEN 'LAPSED'
        ELSE 'CHURNED'
    END                                     AS RECENCY_SEGMENT,
    -- Preferred channel
    MODE(f.CHANNEL)                         AS PREFERRED_CHANNEL,
    -- Top category
    MODE(p.PRD_CATEGORY)                    AS TOP_CATEGORY
FROM WAREHOUSE.DIM_CUSTOMER    c
LEFT JOIN WAREHOUSE.FACT_SALES  f  ON c.CUST_SK = f.CUST_SK
                                   AND f.ORDER_STATUS NOT IN ('CANCELLED','RETURNED')
LEFT JOIN WAREHOUSE.DIM_DATE    d  ON f.ORDER_DATE_KEY = d.DATE_KEY
LEFT JOIN WAREHOUSE.DIM_PRODUCT p  ON f.PRD_SK = p.PRD_SK
WHERE c.IS_CURRENT = TRUE AND c.CUST_SK <> -1
GROUP BY 1,2,3,4,5,6,7,8,9,10,11;

-- ---------------------------------------------------------------------------
-- MART_PRODUCT_PERFORMANCE
-- Grain: product × month
-- ---------------------------------------------------------------------------

CREATE OR REPLACE VIEW MART_PRODUCT_PERFORMANCE AS
SELECT
    p.PRD_ID,
    p.PRD_NAME,
    p.PRD_CATEGORY,
    p.PRD_SUBCATEGORY,
    p.BRAND,
    p.PRICE_TIER,
    d.YEAR,
    d.MONTH_NUM,
    d.MONTH_NAME,
    d.QUARTER_NAME,
    SUM(f.QTY_ORDERED)                      AS UNITS_SOLD,
    SUM(f.NET_REVENUE)                      AS NET_REVENUE,
    SUM(f.GROSS_PROFIT)                     AS GROSS_PROFIT,
    ROUND(SUM(f.GROSS_PROFIT)
         / NULLIF(SUM(f.NET_REVENUE), 0) * 100, 2) AS GROSS_MARGIN_PCT,
    COUNT(DISTINCT f.CUST_SK)               AS UNIQUE_BUYERS,
    -- Return/cancel rate
    SUM(CASE WHEN f.ORDER_STATUS = 'RETURNED' THEN 1 ELSE 0 END) AS RETURN_COUNT,
    ROUND(
        SUM(CASE WHEN f.ORDER_STATUS = 'RETURNED' THEN 1 ELSE 0 END)
        / NULLIF(COUNT(*), 0) * 100, 2
    )                                       AS RETURN_RATE_PCT
FROM WAREHOUSE.FACT_SALES       f
JOIN WAREHOUSE.DIM_PRODUCT      p  ON f.PRD_SK = p.PRD_SK AND p.IS_CURRENT = TRUE
JOIN WAREHOUSE.DIM_DATE         d  ON f.ORDER_DATE_KEY = d.DATE_KEY
GROUP BY 1,2,3,4,5,6,7,8,9,10;

-- ---------------------------------------------------------------------------
-- MART_INVENTORY_STATUS
-- Current inventory health per product × store
-- ---------------------------------------------------------------------------

CREATE OR REPLACE VIEW MART_INVENTORY_STATUS AS
SELECT
    p.PRD_ID,
    p.PRD_NAME,
    p.PRD_CATEGORY,
    p.BRAND,
    s.STORE_ID,
    s.STORE_NAME,
    s.REGION,
    s.STORE_TYPE,
    d.FULL_DATE                             AS SNAPSHOT_DATE,
    fi.QTY_ON_HAND,
    fi.QTY_RESERVED,
    fi.QTY_AVAILABLE,
    fi.REORDER_POINT,
    fi.INV_STATUS,
    fi.NEEDS_REORDER,
    fi.STOCK_VALUE,
    -- Cover days (using last 30-day avg daily sales)
    COALESCE(sales_30d.AVG_DAILY_SALES, 0) AS AVG_DAILY_SALES_30D,
    CASE
        WHEN COALESCE(sales_30d.AVG_DAILY_SALES, 0) = 0 THEN NULL
        ELSE ROUND(fi.QTY_AVAILABLE / sales_30d.AVG_DAILY_SALES, 1)
    END                                     AS COVER_DAYS
FROM WAREHOUSE.FACT_INVENTORY   fi
JOIN WAREHOUSE.DIM_DATE         d  ON fi.SNAPSHOT_DATE_KEY = d.DATE_KEY
JOIN WAREHOUSE.DIM_PRODUCT      p  ON fi.PRD_SK  = p.PRD_SK  AND p.IS_CURRENT = TRUE
JOIN WAREHOUSE.DIM_STORE        s  ON fi.STORE_SK = s.STORE_SK
-- Avg daily sales last 30 days per product × store
LEFT JOIN (
    SELECT
        PRD_SK, STORE_SK,
        ROUND(SUM(QTY_ORDERED)::FLOAT / 30, 2) AS AVG_DAILY_SALES
    FROM WAREHOUSE.FACT_SALES f2
    JOIN WAREHOUSE.DIM_DATE d2 ON f2.ORDER_DATE_KEY = d2.DATE_KEY
    WHERE d2.FULL_DATE >= CURRENT_DATE() - 30
    AND   f2.ORDER_STATUS NOT IN ('CANCELLED','RETURNED')
    GROUP BY PRD_SK, STORE_SK
) sales_30d
    ON fi.PRD_SK = sales_30d.PRD_SK AND fi.STORE_SK = sales_30d.STORE_SK
-- Only latest snapshot per product × store
WHERE fi.SNAPSHOT_DATE_KEY = (
    SELECT MAX(SNAPSHOT_DATE_KEY)
    FROM WAREHOUSE.FACT_INVENTORY fi2
    WHERE fi2.PRD_SK   = fi.PRD_SK
    AND   fi2.STORE_SK = fi.STORE_SK
);

-- ---------------------------------------------------------------------------
-- MART_PROMOTION_EFFECTIVENESS
-- How well are promotions performing?
-- ---------------------------------------------------------------------------

CREATE OR REPLACE VIEW MART_PROMOTION_EFFECTIVENESS AS
SELECT
    pr.PROMO_CODE,
    pr.PROMO_NAME,
    pr.PROMO_TYPE,
    pr.DISCOUNT_VALUE,
    d.YEAR,
    d.MONTH_NAME,
    COUNT(DISTINCT f.ORDER_ID)              AS ORDERS_WITH_PROMO,
    COUNT(DISTINCT f.CUST_SK)               AS UNIQUE_CUSTOMERS,
    SUM(f.GROSS_REVENUE)                    AS GROSS_REVENUE_BEFORE_DISC,
    SUM(f.DISCOUNT_AMT)                     AS TOTAL_DISCOUNT_GIVEN,
    SUM(f.NET_REVENUE)                      AS NET_REVENUE_AFTER_DISC,
    ROUND(SUM(f.DISCOUNT_AMT)
         / NULLIF(SUM(f.GROSS_REVENUE), 0) * 100, 2) AS DISCOUNT_RATE_PCT,
    SUM(f.GROSS_PROFIT)                     AS GROSS_PROFIT,
    ROUND(SUM(f.NET_REVENUE)
         / NULLIF(COUNT(DISTINCT f.ORDER_ID), 0), 2) AS AVG_PROMO_ORDER_VALUE
FROM WAREHOUSE.FACT_SALES       f
JOIN WAREHOUSE.DIM_PROMOTION    pr ON f.PROMO_SK = pr.PROMO_SK AND pr.PROMO_SK <> -1
JOIN WAREHOUSE.DIM_DATE         d  ON f.ORDER_DATE_KEY = d.DATE_KEY
WHERE f.ORDER_STATUS NOT IN ('CANCELLED','RETURNED')
GROUP BY 1,2,3,4,5,6;

-- Grant access
GRANT SELECT ON ALL VIEWS IN SCHEMA SPORTZONE_DEV.MART TO ROLE SPORTZONE_ANALYST;
