"""
Analytics Dashboard — full analytics + AI + SQL editor + ML
Reads from Snowflake SPORTZONE_DEV (WAREHOUSE.* and MART.*)
"""
import sys, warnings, re, html, time
from pathlib import Path

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st

warnings.filterwarnings("ignore")
ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(Path(__file__).parent.parent))
from conn import (sf_query, neon_query, ask_openai, get_openai_key,
                  default_snowflake_params, get_sf_conn)

BG = "#0d1117"; BG2 = "#161b22"; FONT = "#f0f6fc"; GRID = "#30363d"
PL = dict(paper_bgcolor=BG, plot_bgcolor=BG2, font=dict(color=FONT),
          margin=dict(l=8, r=8, t=40, b=8))
CLR = ["#58a6ff", "#3fb950", "#d29922", "#f85149", "#bc8cff", "#39d0d8", "#f78166", "#ffa657"]

# ── Educational tooltips for every chart ─────────────────────────────────────
CHART_HELP = {
    "kpi_revenue":    "💰 **Total Revenue** = sum of all NET_REVENUE from FACT_SALES (after discounts). Higher is better. Compare to previous periods to spot growth.",
    "kpi_profit":     "💵 **Gross Profit** = Revenue − COGS (Cost of Goods Sold). Shows how much money remains after product costs.",
    "kpi_margin":     "📊 **Margin %** = Gross Profit / Revenue × 100. Industry benchmark for sporting goods is 35-45%. Below 30% = pricing issue.",
    "kpi_orders":     "📦 **Orders** = count of unique ORDER_IDs. Useful for understanding transaction volume independent of order size.",
    "kpi_aov":        "🛒 **AOV (Avg Order Value)** = Total Revenue / Total Orders. Higher AOV = customers buying more per visit.",
    "kpi_return":     "↩️ **Return Rate** = % of orders with status RETURNED. Above 10% signals product quality or expectation issues.",
    "daily_revenue":  "📈 **Daily Revenue Trend** — Each bar/point = total revenue on that calendar day across all channels. Rolling average smooths noise to reveal the true trend direction.",
    "channel_mix":    "🔀 **Channel Mix** — How revenue splits across IN-STORE, ONLINE, MOBILE. Helps decide where to invest marketing budget.",
    "product_perf":   "🛍️ **Product Performance** — Revenue and margin by category/brand. High revenue + low margin = pricing opportunity. High margin + low revenue = growth opportunity.",
    "customer_rfm":   "👥 **RFM Analysis** — Recency (when did they last buy?), Frequency (how often?), Monetary (how much?). The foundation of customer segmentation.",
    "ltv_dist":       "💎 **LTV Distribution** — Lifetime Value = total spend per customer. The top 20% of customers typically drive 80% of revenue (Pareto principle).",
    "inventory":      "📦 **Inventory Status** — STOCKED = healthy, LIMITED = approaching reorder point, OUT_OF_STOCK = lost sales opportunity. Reorder Point = minimum qty before restocking.",
    "forecast":       "🔮 **Revenue Forecast** — Uses Linear Regression (y = mx + b) on daily revenue. The model finds the best-fit line through historical data and extrapolates forward. R² measures fit quality (1.0 = perfect, 0 = random).",
    "segmentation":   "🎯 **K-Means Clustering** — Groups customers by similarity in RFM features. PCA reduces dimensions to 2D for visualization. Each cluster = a distinct customer behavior pattern.",
    "churn":          "🚨 **Churn Risk** — Rule-based scoring: high recency + low orders + Bronze tier = high churn risk. Proactively targeting high-risk customers with offers reduces revenue loss.",
    "market_basket":  "🛒 **Market Basket Analysis** — Finds products frequently bought together (co-occurrence). Foundation for 'customers also bought' recommendations.",
    "anomaly":        "⚠️ **Anomaly Detection** — Isolation Forest algorithm identifies days where revenue deviates significantly from normal patterns. Anomalies may indicate promotions, events, or data quality issues.",
    "waterfall":      "💧 **Waterfall Chart** — Shows how each promotion contributes to total profit. Green bars = profit, red bars = discount cost.",
    "promo_roi":      "🎯 **Promotion ROI** — Revenue generated per $1 of discount given. ROI > 5x = effective promotion. Below 2x = discount eating into margin.",
}

PIPELINE_EXPLANATION = """
### 🏗️ How This Data Platform Works

```
Neon PostgreSQL (OLTP)          Snowflake (DWH)              This Dashboard
┌─────────────────────┐        ┌──────────────────────┐     ┌─────────────┐
│ oltp.customer        │──ETL──▶│ RAW.RAW_CUSTOMERS     │     │             │
│ oltp.order_header    │──ETL──▶│ RAW.RAW_ORDERS        │     │  Analytics  │
│ oltp.order_item      │──ETL──▶│ STAGING.STG_ORDERS    │──▶  │  ML Models  │
│ oltp.product         │──ETL──▶│ WAREHOUSE.DIM_PRODUCT │     │  AI Insights│
│ oltp.inventory       │──ETL──▶│ WAREHOUSE.FACT_SALES  │     │             │
└─────────────────────┘        └──────────────────────┘     └─────────────┘
     Source of Truth               Star Schema (DWH)           Presentation
```

**ETL Pipeline** (Extract → Transform → Load):
- **Extract**: Pulls only NEW rows since last run using watermark timestamps
- **Transform**: Cleans data, fixes types, derives business metrics (margins, age bands)
- **Load**: TRUNCATE + INSERT for staging, MERGE for warehouse (prevents duplicates)

**Star Schema**: FACT_SALES is the center. DIM tables (Customer, Product, Store, Date) are the spokes. This design makes analytics queries fast and simple.
"""


def fmt(v, p="", s="", d=0):
    if v is None:
        return "—"
    try:
        v = float(v)
    except (TypeError, ValueError):
        return "—"
    if v != v:
        return "—"  # NaN check
    if abs(v) >= 1e6:
        return f"{p}{v/1e6:.1f}M{s}"
    if abs(v) >= 1e3:
        return f"{p}{v/1e3:.1f}K{s}"
    return f"{p}{v:,.{d}f}{s}"


def _explain(key: str):
    """Show a self-explaining tooltip for any chart or metric."""
    if key in CHART_HELP:
        with st.expander("ℹ️ What does this mean?", expanded=False):
            st.markdown(CHART_HELP[key])


# ── Data loaders ──────────────────────────────────────────────────────────────
def _cast_numerics(df: pd.DataFrame) -> pd.DataFrame:
    """Force-cast Snowflake Decimal / object columns to proper numeric dtypes."""
    if df.empty:
        return df
    import decimal
    for col in df.columns:
        sample = df[col].dropna()
        if len(sample) == 0:
            continue
        first = sample.iloc[0]
        # Always convert Decimal
        if isinstance(first, decimal.Decimal):
            df[col] = pd.to_numeric(df[col], errors="coerce")
        elif df[col].dtype == object:
            converted = pd.to_numeric(df[col], errors="coerce")
            if converted.notna().sum() >= df[col].notna().sum() * 0.5:
                df[col] = converted
    return df


@st.cache_data(ttl=120, show_spinner=False)
def load_kpis(_dummy=None):
    df = sf_query("""
        SELECT
            COUNT(DISTINCT ORDER_ID)                                    AS TOTAL_ORDERS,
            SUM(NET_REVENUE)                                            AS TOTAL_REVENUE,
            SUM(GROSS_PROFIT)                                           AS TOTAL_PROFIT,
            ROUND(SUM(NET_REVENUE)/NULLIF(COUNT(DISTINCT ORDER_ID),0),2) AS AVG_ORDER_VALUE,
            SUM(QTY_ORDERED)                                            AS UNITS_SOLD,
            COUNT(DISTINCT CUST_SK)                                     AS UNIQUE_CUSTOMERS,
            ROUND(SUM(GROSS_PROFIT)/NULLIF(SUM(NET_REVENUE),0)*100,1)   AS MARGIN_PCT,
            ROUND(COUNT(DISTINCT CASE WHEN ORDER_STATUS='RETURNED' THEN ORDER_ID END)*100.0
                  /NULLIF(COUNT(DISTINCT ORDER_ID),0),1)                AS RETURN_RATE
        FROM WAREHOUSE.FACT_SALES
        WHERE ORDER_STATUS NOT IN ('CANCELLED')
    """)
    return _cast_numerics(df)


@st.cache_data(ttl=120, show_spinner=False)
def load_daily_sales(days=180, _dummy=None):
    # The window is anchored on the latest order date in the data (not CURRENT_DATE),
    # so the charts are never empty when the generated data is a bit old.
    df = sf_query(f"""
        WITH BOUNDS AS (
            SELECT MAX(D2.FULL_DATE) AS MAX_DT
            FROM WAREHOUSE.FACT_SALES F2
            JOIN WAREHOUSE.DIM_DATE   D2 ON F2.ORDER_DATE_KEY = D2.DATE_KEY
        )
        SELECT D.FULL_DATE, D.MONTH_NAME, D.YEAR, D.QUARTER_NAME, F.CHANNEL,
               SUM(F.NET_REVENUE)   AS REVENUE,
               SUM(F.GROSS_PROFIT)  AS PROFIT,
               COUNT(DISTINCT F.ORDER_ID) AS ORDERS,
               SUM(F.QTY_ORDERED)   AS UNITS
        FROM WAREHOUSE.FACT_SALES F
        JOIN WAREHOUSE.DIM_DATE   D ON F.ORDER_DATE_KEY = D.DATE_KEY
        CROSS JOIN BOUNDS B
        WHERE F.ORDER_STATUS NOT IN ('CANCELLED','RETURNED')
          AND D.FULL_DATE >= DATEADD(DAY,-{int(days)}, B.MAX_DT)
        GROUP BY 1,2,3,4,5 ORDER BY 1
    """)
    df = _cast_numerics(df)
    # Guarantee FULL_DATE is datetime and numerics are float — critical for ML
    if not df.empty:
        df["FULL_DATE"] = pd.to_datetime(df["FULL_DATE"], errors="coerce")
        for col in ["REVENUE", "PROFIT", "ORDERS", "UNITS"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)
    return df


@st.cache_data(ttl=120, show_spinner=False)
def load_category(_dummy=None):
    df = sf_query("""
        SELECT P.PRD_CATEGORY, P.PRD_SUBCATEGORY, P.BRAND, P.PRICE_TIER,
               SUM(F.NET_REVENUE)  AS REVENUE,
               SUM(F.GROSS_PROFIT) AS PROFIT,
               SUM(F.QTY_ORDERED)  AS UNITS,
               COUNT(DISTINCT F.ORDER_ID) AS ORDERS,
               ROUND(SUM(F.GROSS_PROFIT)/NULLIF(SUM(F.NET_REVENUE),0)*100,1) AS MARGIN
        FROM WAREHOUSE.FACT_SALES   F
        JOIN WAREHOUSE.DIM_PRODUCT  P ON F.PRD_SK = P.PRD_SK
        WHERE F.ORDER_STATUS NOT IN ('CANCELLED','RETURNED')
        GROUP BY 1,2,3,4 ORDER BY REVENUE DESC
    """)
    return _cast_numerics(df)


@st.cache_data(ttl=120, show_spinner=False)
def load_customers(_dummy=None):
    # Sales are summed per business key (CUST_ID) over every SCD2 version of the
    # customer, then joined to the CURRENT customer row for the descriptive columns.
    df = sf_query("""
        WITH SALES AS (
            SELECT C.CUST_ID,
                   COUNT(DISTINCT F.ORDER_ID)  AS ORDERS,
                   SUM(F.NET_REVENUE)          AS LTV,
                   ROUND(SUM(F.NET_REVENUE)/NULLIF(COUNT(DISTINCT F.ORDER_ID),0),2) AS AOV,
                   MAX(D.FULL_DATE)            AS LAST_ORDER
            FROM WAREHOUSE.FACT_SALES   F
            JOIN WAREHOUSE.DIM_CUSTOMER C ON F.CUST_SK = C.CUST_SK
            JOIN WAREHOUSE.DIM_DATE     D ON F.ORDER_DATE_KEY = D.DATE_KEY
            WHERE F.ORDER_STATUS NOT IN ('CANCELLED','RETURNED')
              AND C.CUST_SK <> -1
            GROUP BY C.CUST_ID
        )
        SELECT C.CUST_ID, C.FULL_NAME, C.LOYALTY_TIER, C.COUNTRY,
               C.AGE_BAND, C.GENDER, C.IS_ACTIVE,
               S.ORDERS, S.LTV, S.AOV, S.LAST_ORDER,
               DATEDIFF(DAY, S.LAST_ORDER, CURRENT_DATE()) AS RECENCY_DAYS
        FROM WAREHOUSE.DIM_CUSTOMER C
        JOIN SALES S ON C.CUST_ID = S.CUST_ID
        WHERE C.IS_CURRENT AND C.CUST_SK <> -1
        ORDER BY S.LTV DESC NULLS LAST
    """)
    return _cast_numerics(df)


@st.cache_data(ttl=120, show_spinner=False)
def load_inventory(_dummy=None):
    # Latest snapshot per product + store only (otherwise stock is counted once per snapshot)
    df = sf_query("""
        SELECT P.PRD_NAME, P.PRD_CATEGORY, P.BRAND,
               S.STORE_NAME, S.REGION, S.STORE_TYPE,
               FI.QTY_ON_HAND, FI.QTY_AVAILABLE, FI.REORDER_POINT,
               FI.INV_STATUS, FI.NEEDS_REORDER, FI.STOCK_VALUE,
               D.FULL_DATE AS SNAPSHOT_DATE
        FROM WAREHOUSE.FACT_INVENTORY FI
        JOIN WAREHOUSE.DIM_PRODUCT P ON FI.PRD_SK = P.PRD_SK
        JOIN WAREHOUSE.DIM_STORE   S ON FI.STORE_SK = S.STORE_SK AND S.STORE_SK <> -1
        JOIN WAREHOUSE.DIM_DATE    D ON FI.SNAPSHOT_DATE_KEY = D.DATE_KEY
        QUALIFY ROW_NUMBER() OVER (PARTITION BY FI.PRD_SK, FI.STORE_SK
                                   ORDER BY FI.SNAPSHOT_DATE_KEY DESC) = 1
        ORDER BY FI.NEEDS_REORDER DESC, FI.QTY_ON_HAND
    """)
    return _cast_numerics(df)


@st.cache_data(ttl=120, show_spinner=False)
def load_promo(_dummy=None):
    df = sf_query("""
        SELECT PR.PROMO_CODE, PR.PROMO_NAME, PR.PROMO_TYPE,
               COUNT(DISTINCT F.ORDER_ID) AS ORDERS,
               SUM(F.DISCOUNT_AMT)       AS DISCOUNT_GIVEN,
               SUM(F.NET_REVENUE)        AS REVENUE,
               SUM(F.GROSS_PROFIT)       AS PROFIT,
               ROUND(SUM(F.DISCOUNT_AMT)/NULLIF(SUM(F.GROSS_REVENUE),0)*100,1) AS DISC_RATE
        FROM WAREHOUSE.FACT_SALES     F
        JOIN WAREHOUSE.DIM_PROMOTION PR ON F.PROMO_SK=PR.PROMO_SK AND PR.PROMO_SK<>-1
        WHERE F.ORDER_STATUS NOT IN ('CANCELLED','RETURNED')
        GROUP BY 1,2,3 ORDER BY REVENUE DESC
    """)
    return _cast_numerics(df)


# ── SNOWFLAKE SCHEMA for NL→SQL ───────────────────────────────────────────────
SF_SCHEMA = """
Snowflake database: SPORTZONE_DEV
Tables and views:
  WAREHOUSE.FACT_SALES(SALES_SK,ORDER_DATE_KEY,CUST_SK,PRD_SK,STORE_SK,PROMO_SK,ORDER_ID,ORDER_ITEM_ID,CHANNEL,ORDER_STATUS,QTY_ORDERED,UNIT_PRICE,UNIT_COST,GROSS_MARGIN_PCT,DISCOUNT_PCT,DISCOUNT_AMT,GROSS_REVENUE,NET_REVENUE,COGS,GROSS_PROFIT)
  WAREHOUSE.FACT_INVENTORY(INVENTORY_SK,SNAPSHOT_DATE_KEY,PRD_SK,STORE_SK,INVENTORY_ID,QTY_ON_HAND,QTY_RESERVED,QTY_AVAILABLE,REORDER_POINT,INV_STATUS,NEEDS_REORDER,STOCK_VALUE)
  WAREHOUSE.DIM_DATE(DATE_KEY,FULL_DATE,DAY_OF_WEEK,DAY_NAME,MONTH_NUM,MONTH_NAME,QUARTER,QUARTER_NAME,YEAR,IS_WEEKEND,IS_HOLIDAY,FISCAL_YEAR,FISCAL_QUARTER)
  WAREHOUSE.DIM_CUSTOMER(CUST_SK,CUST_ID,FULL_NAME,EMAIL,AGE_BAND,GENDER,CITY,STATE,COUNTRY,LOYALTY_TIER,SIGNUP_DATE,IS_ACTIVE,EFF_START_DT,EFF_END_DT,IS_CURRENT)
  WAREHOUSE.DIM_PRODUCT(PRD_SK,PRD_ID,PRD_NAME,PRD_CATEGORY,PRD_SUBCATEGORY,BRAND,SKU,UNIT_PRICE,UNIT_COST,GROSS_MARGIN_PCT,PRICE_TIER,IS_ACTIVE,EFF_START_DT,IS_CURRENT)
  WAREHOUSE.DIM_STORE(STORE_SK,STORE_ID,STORE_NAME,STORE_TYPE,REGION,CITY,STATE,COUNTRY,OPEN_DATE,MANAGER_NAME)
  WAREHOUSE.DIM_PROMOTION(PROMO_SK,PROMO_ID,PROMO_CODE,PROMO_NAME,PROMO_TYPE,DISCOUNT_VALUE,START_DATE,END_DATE,IS_ACTIVE)
  MART.DAILY_SALES_SUMMARY(FULL_DATE,YEAR,MONTH_NAME,QUARTER_NAME,DAY_NAME,IS_WEEKEND,CHANNEL,PRD_CATEGORY,PRD_SUBCATEGORY,BRAND,REGION,COUNTRY,ORDER_COUNT,UNITS_SOLD,GROSS_REVENUE,TOTAL_DISCOUNT,NET_REVENUE,COGS,GROSS_PROFIT,GROSS_MARGIN_PCT,AVG_ORDER_VALUE)
  MART.CUSTOMER_360(CUST_ID,FULL_NAME,EMAIL,AGE_BAND,GENDER,CITY,STATE,COUNTRY,LOYALTY_TIER,SIGNUP_DATE,IS_ACTIVE,TOTAL_ORDERS,LIFETIME_VALUE,AVG_ORDER_VALUE,FIRST_ORDER_DATE,LAST_ORDER_DATE,DAYS_SINCE_LAST_ORDER,RECENCY_SEGMENT)
  MART.PRODUCT_PERFORMANCE(PRD_ID,PRD_NAME,PRD_CATEGORY,PRD_SUBCATEGORY,BRAND,PRICE_TIER,YEAR,MONTH_NUM,MONTH_NAME,QUARTER_NAME,UNITS_SOLD,NET_REVENUE,GROSS_PROFIT,GROSS_MARGIN_PCT,UNIQUE_BUYERS)
  MART.INVENTORY_STATUS(PRD_ID,PRD_NAME,PRD_CATEGORY,BRAND,STORE_ID,STORE_NAME,REGION,STORE_TYPE,SNAPSHOT_DATE,QTY_ON_HAND,QTY_AVAILABLE,REORDER_POINT,INV_STATUS,NEEDS_REORDER,STOCK_VALUE)
Rules: use Snowflake SQL dialect. Always qualify table names with schema. Use DATEADD,DATEDIFF,TO_CHAR,ROUND,NULLIF,QUALIFY,WINDOW functions where appropriate.
Window frames must be written as ROWS BETWEEN <n> PRECEDING AND CURRENT ROW.
Exclude cancelled/returned orders with ORDER_STATUS NOT IN ('CANCELLED','RETURNED').
"""

# ── Prewritten questions (all verified against the schema above) ──────────────
# Notes:
#  * Only WAREHOUSE tables are used (the same ones the dashboard tabs read), so
#    these run even if the MART views were never built.
#  * Fact rows are joined to dimensions on the surrogate key only (no IS_CURRENT),
#    so sales that point at an older SCD2 version are not silently dropped.
#  * DIM_DATE has no WEEK column, so weeks use DATE_TRUNC('WEEK', FULL_DATE).
#  * Window frames use the explicit  ROWS BETWEEN n PRECEDING AND CURRENT ROW  form.
PREWRITTEN_QS = {
    "Simple": {
        "Total revenue and profit": """
SELECT SUM(NET_REVENUE)   AS TOTAL_REVENUE,
       SUM(GROSS_PROFIT)  AS TOTAL_PROFIT,
       ROUND(SUM(GROSS_PROFIT)/NULLIF(SUM(NET_REVENUE),0)*100,2) AS MARGIN_PCT
FROM WAREHOUSE.FACT_SALES
WHERE ORDER_STATUS NOT IN ('CANCELLED','RETURNED')""",

        "Revenue by channel": """
SELECT CHANNEL,
       COUNT(DISTINCT ORDER_ID) AS ORDERS,
       ROUND(SUM(NET_REVENUE),2) AS REVENUE
FROM WAREHOUSE.FACT_SALES
WHERE ORDER_STATUS NOT IN ('CANCELLED','RETURNED')
GROUP BY 1
ORDER BY REVENUE DESC""",

        "Top 10 customers by LTV": """
SELECT C.FULL_NAME, C.LOYALTY_TIER,
       COUNT(DISTINCT F.ORDER_ID)  AS ORDERS,
       ROUND(SUM(F.NET_REVENUE),2) AS LTV
FROM WAREHOUSE.FACT_SALES F
JOIN WAREHOUSE.DIM_CUSTOMER C ON F.CUST_SK = C.CUST_SK
WHERE F.ORDER_STATUS NOT IN ('CANCELLED','RETURNED')
  AND C.CUST_SK <> -1
GROUP BY C.CUST_ID, C.FULL_NAME, C.LOYALTY_TIER
ORDER BY LTV DESC
LIMIT 10""",

        "Products out of stock (latest snapshot)": """
WITH LATEST AS (
    SELECT *
    FROM WAREHOUSE.FACT_INVENTORY
    QUALIFY ROW_NUMBER() OVER (PARTITION BY PRD_SK, STORE_SK
                               ORDER BY SNAPSHOT_DATE_KEY DESC) = 1
)
SELECT P.PRD_NAME, P.BRAND, S.STORE_NAME, S.REGION,
       L.QTY_ON_HAND, L.REORDER_POINT
FROM LATEST L
JOIN WAREHOUSE.DIM_PRODUCT P ON L.PRD_SK = P.PRD_SK
JOIN WAREHOUSE.DIM_STORE   S ON L.STORE_SK = S.STORE_SK
WHERE L.INV_STATUS = 'OUT_OF_STOCK'
ORDER BY L.QTY_ON_HAND, P.PRD_NAME""",

        "Monthly revenue by year": """
SELECT D.YEAR, D.MONTH_NUM, D.MONTH_NAME,
       ROUND(SUM(F.NET_REVENUE),2) AS REVENUE,
       COUNT(DISTINCT F.ORDER_ID)  AS ORDERS
FROM WAREHOUSE.FACT_SALES F
JOIN WAREHOUSE.DIM_DATE   D ON F.ORDER_DATE_KEY = D.DATE_KEY
WHERE F.ORDER_STATUS NOT IN ('CANCELLED','RETURNED')
GROUP BY 1,2,3
ORDER BY 1,2""",

        "Preview a MART view": """
SELECT *
FROM MART.DAILY_SALES_SUMMARY
LIMIT 20""",
    },
    "Intermediate": {
        "Revenue by category and channel": """
SELECT P.PRD_CATEGORY, F.CHANNEL,
       ROUND(SUM(F.NET_REVENUE),2) AS REVENUE,
       ROUND(SUM(F.GROSS_PROFIT)/NULLIF(SUM(F.NET_REVENUE),0)*100,1) AS MARGIN
FROM WAREHOUSE.FACT_SALES F
JOIN WAREHOUSE.DIM_PRODUCT P ON F.PRD_SK = P.PRD_SK
WHERE F.ORDER_STATUS NOT IN ('CANCELLED','RETURNED')
GROUP BY 1,2
ORDER BY 1, REVENUE DESC""",

        "Customer retention by signup cohort": """
WITH COHORT AS (
    SELECT DATE_TRUNC('MONTH', C.SIGNUP_DATE)::DATE AS COHORT_MONTH,
           COUNT(DISTINCT C.CUST_ID) AS COHORT_SIZE,
           COUNT(DISTINCT CASE
                 WHEN D.FULL_DATE BETWEEN C.SIGNUP_DATE AND DATEADD(DAY,30,C.SIGNUP_DATE)
                 THEN C.CUST_ID END)  AS BOUGHT_WITHIN_30D
    FROM WAREHOUSE.DIM_CUSTOMER C
    LEFT JOIN WAREHOUSE.FACT_SALES F
           ON C.CUST_SK = F.CUST_SK
          AND F.ORDER_STATUS NOT IN ('CANCELLED','RETURNED')
    LEFT JOIN WAREHOUSE.DIM_DATE D ON F.ORDER_DATE_KEY = D.DATE_KEY
    WHERE C.CUST_SK <> -1
    GROUP BY 1
)
SELECT COHORT_MONTH, COHORT_SIZE, BOUGHT_WITHIN_30D,
       ROUND(BOUGHT_WITHIN_30D * 100.0 / NULLIF(COHORT_SIZE,0), 1) AS RETAINED_30D_PCT
FROM COHORT
ORDER BY COHORT_MONTH""",

        "Promotion ROI": """
SELECT PR.PROMO_CODE, PR.PROMO_TYPE,
       COUNT(DISTINCT F.ORDER_ID) AS ORDERS,
       ROUND(SUM(F.DISCOUNT_AMT),2) AS COST,
       ROUND(SUM(F.NET_REVENUE),2)  AS REVENUE,
       ROUND((SUM(F.NET_REVENUE)-SUM(F.DISCOUNT_AMT))/NULLIF(SUM(F.DISCOUNT_AMT),0)*100,1) AS ROI_PCT
FROM WAREHOUSE.FACT_SALES F
JOIN WAREHOUSE.DIM_PROMOTION PR ON F.PROMO_SK = PR.PROMO_SK AND PR.PROMO_SK <> -1
WHERE F.ORDER_STATUS NOT IN ('CANCELLED','RETURNED')
GROUP BY 1,2
ORDER BY ROI_PCT DESC NULLS LAST""",

        "Week-over-week revenue growth": """
WITH WEEKLY AS (
    SELECT DATE_TRUNC('WEEK', D.FULL_DATE)::DATE AS WEEK_START,
           SUM(F.NET_REVENUE) AS REVENUE
    FROM WAREHOUSE.FACT_SALES F
    JOIN WAREHOUSE.DIM_DATE   D ON F.ORDER_DATE_KEY = D.DATE_KEY
    WHERE F.ORDER_STATUS NOT IN ('CANCELLED','RETURNED')
    GROUP BY 1
)
SELECT WEEK_START,
       ROUND(REVENUE,2) AS REVENUE,
       ROUND(LAG(REVENUE) OVER (ORDER BY WEEK_START),2) AS PREV_WEEK,
       ROUND((REVENUE / NULLIF(LAG(REVENUE) OVER (ORDER BY WEEK_START),0) - 1) * 100, 1) AS WOW_PCT
FROM WEEKLY
ORDER BY WEEK_START""",

        "Store inventory coverage (days)": """
WITH LATEST AS (
    SELECT *
    FROM WAREHOUSE.FACT_INVENTORY
    QUALIFY ROW_NUMBER() OVER (PARTITION BY PRD_SK, STORE_SK
                               ORDER BY SNAPSHOT_DATE_KEY DESC) = 1
), DAILY AS (
    SELECT PRD_SK, STORE_SK,
           SUM(QTY_ORDERED) / NULLIF(COUNT(DISTINCT ORDER_DATE_KEY),0) AS DAILY_UNITS
    FROM WAREHOUSE.FACT_SALES
    WHERE ORDER_STATUS NOT IN ('CANCELLED','RETURNED')
    GROUP BY 1,2
)
SELECT S.STORE_NAME, P.PRD_CATEGORY,
       SUM(L.QTY_AVAILABLE) AS AVAILABLE,
       ROUND(SUM(L.QTY_AVAILABLE) / NULLIF(SUM(DY.DAILY_UNITS),0), 1) AS COVER_DAYS
FROM LATEST L
JOIN WAREHOUSE.DIM_PRODUCT P ON L.PRD_SK = P.PRD_SK
JOIN WAREHOUSE.DIM_STORE   S ON L.STORE_SK = S.STORE_SK
LEFT JOIN DAILY DY ON L.PRD_SK = DY.PRD_SK AND L.STORE_SK = DY.STORE_SK
GROUP BY 1,2
ORDER BY COVER_DAYS ASC NULLS LAST""",
    },
    "Advanced": {
        "RFM Segmentation": """
WITH RFM AS (
    SELECT C.CUST_ID, C.FULL_NAME,
           DATEDIFF(DAY, MAX(D.FULL_DATE), CURRENT_DATE()) AS RECENCY,
           COUNT(DISTINCT F.ORDER_ID)                      AS FREQUENCY,
           SUM(F.NET_REVENUE)                              AS MONETARY
    FROM WAREHOUSE.DIM_CUSTOMER C
    JOIN WAREHOUSE.FACT_SALES   F ON C.CUST_SK = F.CUST_SK
                                 AND F.ORDER_STATUS NOT IN ('CANCELLED','RETURNED')
    JOIN WAREHOUSE.DIM_DATE     D ON F.ORDER_DATE_KEY = D.DATE_KEY
    WHERE C.CUST_SK <> -1
    GROUP BY 1,2
), SCORED AS (
    SELECT *,
           NTILE(5) OVER (ORDER BY RECENCY DESC) AS R_SCORE,
           NTILE(5) OVER (ORDER BY FREQUENCY)    AS F_SCORE,
           NTILE(5) OVER (ORDER BY MONETARY)     AS M_SCORE
    FROM RFM
)
SELECT CUST_ID, FULL_NAME, RECENCY, FREQUENCY, ROUND(MONETARY,2) AS MONETARY,
       R_SCORE, F_SCORE, M_SCORE,
       CASE WHEN R_SCORE>=4 AND F_SCORE>=4 THEN 'Champions'
            WHEN R_SCORE>=3 AND F_SCORE>=3 THEN 'Loyal'
            WHEN R_SCORE>=4 AND F_SCORE<=2 THEN 'New Customers'
            WHEN R_SCORE<=2 AND F_SCORE>=3 THEN 'At Risk'
            WHEN R_SCORE<=2 AND F_SCORE<=2 THEN 'Lost'
            ELSE 'Potential' END AS SEGMENT
FROM SCORED
ORDER BY MONETARY DESC
LIMIT 50""",

        "SCD2 Customer tier history": """
SELECT CUST_ID, FULL_NAME, LOYALTY_TIER, EFF_START_DT, EFF_END_DT,
       DATEDIFF(DAY, EFF_START_DT, COALESCE(EFF_END_DT, CURRENT_DATE())) AS DAYS_IN_TIER
FROM WAREHOUSE.DIM_CUSTOMER
WHERE CUST_SK <> -1
ORDER BY CUST_ID, EFF_START_DT
LIMIT 500""",

        "Revenue contribution by price tier (Pareto)": """
SELECT P.PRICE_TIER,
       ROUND(SUM(F.NET_REVENUE),2) AS REVENUE,
       ROUND(SUM(SUM(F.NET_REVENUE)) OVER (
             ORDER BY SUM(F.NET_REVENUE) DESC
             ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),2) AS CUMULATIVE,
       ROUND(SUM(SUM(F.NET_REVENUE)) OVER (
             ORDER BY SUM(F.NET_REVENUE) DESC
             ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
             / SUM(SUM(F.NET_REVENUE)) OVER () * 100, 1) AS CUMULATIVE_PCT
FROM WAREHOUSE.FACT_SALES F
JOIN WAREHOUSE.DIM_PRODUCT P ON F.PRD_SK = P.PRD_SK
WHERE F.ORDER_STATUS NOT IN ('CANCELLED','RETURNED')
GROUP BY 1
ORDER BY REVENUE DESC""",

        "Moving average revenue (7/30 day)": """
WITH DAILY AS (
    SELECT D.FULL_DATE, SUM(F.NET_REVENUE) AS DAILY_REVENUE
    FROM WAREHOUSE.FACT_SALES F
    JOIN WAREHOUSE.DIM_DATE   D ON F.ORDER_DATE_KEY = D.DATE_KEY
    WHERE F.ORDER_STATUS NOT IN ('CANCELLED','RETURNED')
    GROUP BY 1
)
SELECT FULL_DATE,
       ROUND(DAILY_REVENUE,2) AS DAILY_REVENUE,
       ROUND(AVG(DAILY_REVENUE) OVER (ORDER BY FULL_DATE
             ROWS BETWEEN 6 PRECEDING AND CURRENT ROW),2)  AS MA7,
       ROUND(AVG(DAILY_REVENUE) OVER (ORDER BY FULL_DATE
             ROWS BETWEEN 29 PRECEDING AND CURRENT ROW),2) AS MA30
FROM DAILY
ORDER BY FULL_DATE""",
    },
    "Extreme": {
        "Customer 12-month value forecast (trend)": """
WITH MONTHLY AS (
    SELECT C.CUST_ID,
           DATE_TRUNC('MONTH', D.FULL_DATE)::DATE AS MONTH_START,
           SUM(F.NET_REVENUE) AS MONTHLY_REV
    FROM WAREHOUSE.FACT_SALES   F
    JOIN WAREHOUSE.DIM_CUSTOMER C ON F.CUST_SK = C.CUST_SK
    JOIN WAREHOUSE.DIM_DATE     D ON F.ORDER_DATE_KEY = D.DATE_KEY
    WHERE F.ORDER_STATUS NOT IN ('CANCELLED','RETURNED')
      AND C.CUST_SK <> -1
    GROUP BY 1,2
), REGR AS (
    SELECT CUST_ID,
           REGR_SLOPE(MONTHLY_REV, DATEDIFF(MONTH, '2024-01-01'::DATE, MONTH_START)) AS SLOPE,
           COUNT(*)         AS MONTHS_ACTIVE,
           SUM(MONTHLY_REV) AS ACTUAL_LTV,
           AVG(MONTHLY_REV) AS AVG_MONTHLY
    FROM MONTHLY
    GROUP BY 1
)
SELECT C.FULL_NAME, C.LOYALTY_TIER, R.MONTHS_ACTIVE,
       ROUND(R.ACTUAL_LTV,2) AS ACTUAL_LTV,
       -- next 12 months = average month x 12, plus the monthly trend applied to months 1..12 (sum = 78)
       ROUND(GREATEST(0, R.AVG_MONTHLY * 12 + COALESCE(R.SLOPE,0) * 78), 2) AS PREDICTED_NEXT_12M,
       ROUND(COALESCE(R.SLOPE,0), 2) AS MONTHLY_TREND
FROM REGR R
JOIN WAREHOUSE.DIM_CUSTOMER C ON R.CUST_ID = C.CUST_ID AND C.IS_CURRENT
ORDER BY PREDICTED_NEXT_12M DESC NULLS LAST
LIMIT 20""",

        "Market basket analysis (product pairs)": """
WITH PAIRS AS (
    SELECT A.PRD_SK AS PROD_A, B.PRD_SK AS PROD_B,
           COUNT(DISTINCT A.ORDER_ID) AS CO_OCCURRENCES
    FROM WAREHOUSE.FACT_SALES A
    JOIN WAREHOUSE.FACT_SALES B ON A.ORDER_ID = B.ORDER_ID AND A.PRD_SK < B.PRD_SK
    WHERE A.ORDER_STATUS NOT IN ('CANCELLED','RETURNED')
    GROUP BY 1,2
), TOTALS AS (
    SELECT COUNT(DISTINCT ORDER_ID) AS TOTAL
    FROM WAREHOUSE.FACT_SALES
    WHERE ORDER_STATUS NOT IN ('CANCELLED','RETURNED')
)
SELECT PA.PRD_NAME AS PRODUCT_A, PB.PRD_NAME AS PRODUCT_B,
       P.CO_OCCURRENCES,
       ROUND(P.CO_OCCURRENCES * 100.0 / T.TOTAL, 2) AS SUPPORT_PCT
FROM PAIRS P
JOIN WAREHOUSE.DIM_PRODUCT PA ON P.PROD_A = PA.PRD_SK
JOIN WAREHOUSE.DIM_PRODUCT PB ON P.PROD_B = PB.PRD_SK
CROSS JOIN TOTALS T
ORDER BY P.CO_OCCURRENCES DESC
LIMIT 20""",
    },
}

DEFAULT_SQL = "SELECT *\nFROM WAREHOUSE.FACT_SALES\nLIMIT 10"


# ── SQL helpers (module level so every tab can use them) ──────────────────────
SQL_KEYWORDS = {
    "SELECT", "FROM", "WHERE", "JOIN", "LEFT", "RIGHT", "INNER", "OUTER", "FULL", "CROSS",
    "ON", "AS", "GROUP", "BY", "ORDER", "HAVING", "LIMIT", "WITH", "UNION", "ALL",
    "INTERSECT", "EXCEPT", "INSERT", "INTO", "UPDATE", "SET", "DELETE", "CREATE", "ALTER",
    "DROP", "TABLE", "VIEW", "SCHEMA", "DATABASE", "IF", "NOT", "EXISTS", "REPLACE", "OR",
    "AND", "IN", "BETWEEN", "LIKE", "ILIKE", "IS", "NULL", "CASE", "WHEN", "THEN", "ELSE",
    "END", "DISTINCT", "QUALIFY", "OVER", "PARTITION", "ROWS", "RANGE", "PRECEDING",
    "FOLLOWING", "UNBOUNDED", "CURRENT", "ROW", "NULLS", "FIRST", "LAST", "ASC", "DESC",
    "TRUE", "FALSE", "MERGE", "USING", "MATCHED", "COPY", "STAGE", "TEMPORARY", "EXPLAIN",
    "SHOW", "DESCRIBE", "USE", "TOP", "LATERAL", "FLATTEN",
}

_TOKEN_RE = re.compile(
    r"""
      (?P<comment>--[^\n]*|/\*.*?\*/)
    | (?P<string>'(?:[^']|'')*')
    | (?P<number>\b\d+(?:\.\d+)?\b)
    | (?P<word>[A-Za-z_][A-Za-z0-9_$]*(?:\.[A-Za-z_][A-Za-z0-9_$]*)*)
    """,
    re.VERBOSE | re.DOTALL,
)


def highlight_sql(sql_text: str) -> str:
    """Syntax-highlight SQL in ONE pass (no nested replacements, so the HTML stays valid)."""
    out, pos = [], 0
    for m in _TOKEN_RE.finditer(sql_text):
        out.append(html.escape(sql_text[pos:m.start()]))
        tok, kind = m.group(), m.lastgroup
        esc = html.escape(tok)
        if kind == "comment":
            out.append(f'<span style="color:#8b949e;font-style:italic">{esc}</span>')
        elif kind == "string":
            out.append(f'<span style="color:#a5d6ff">{esc}</span>')
        elif kind == "number":
            out.append(f'<span style="color:#f0883e">{esc}</span>')
        else:
            up = tok.upper()
            nxt = sql_text[m.end():].lstrip()[:1]
            if up in SQL_KEYWORDS:
                out.append(f'<span style="color:#ff7b72;font-weight:700">{esc}</span>')
            elif "." in tok:
                out.append(f'<span style="color:#79c0ff">{esc}</span>')
            elif nxt == "(":
                out.append(f'<span style="color:#d2a8ff">{esc}</span>')
            else:
                out.append(esc)
        pos = m.end()
    out.append(html.escape(sql_text[pos:]))
    return ('<pre style="background:#161b22;border:1px solid #30363d;border-radius:8px;'
            'padding:14px;font-family:Consolas,monospace;font-size:13px;line-height:1.6;'
            'overflow-x:auto;color:#e6edf3;white-space:pre-wrap">' + "".join(out) + "</pre>")


def format_sql(raw: str) -> str:
    """Pretty-print SQL with sqlparse (keywords upper-cased, identifiers untouched)."""
    try:
        import sqlparse
        return sqlparse.format(raw, reindent=True, keyword_case="upper", indent_width=4)
    except Exception:
        return raw


def strip_fences(text: str) -> str:
    """Remove ```sql ... ``` fences without touching the SQL itself."""
    t = (text or "").strip()
    t = re.sub(r"^```[A-Za-z]*\s*", "", t)
    t = re.sub(r"\s*```$", "", t)
    return t.strip()


def queue_sql(text: str):
    """Put text into the SQL editor.
    A widget's value can only be changed BEFORE the widget is drawn, so we park the
    text in 'sql_pending' and rerun; the SQL tab applies it at the very top."""
    st.session_state["sql_pending"] = text
    st.rerun()


def run_sql(sql: str) -> pd.DataFrame:
    """Run one statement on the shared Snowflake connection (never cached, so DDL/DML
    and repeated runs always execute)."""
    import snowflake.connector
    conn = get_sf_conn()
    cur = conn.cursor(snowflake.connector.DictCursor)
    try:
        cur.execute("USE DATABASE SPORTZONE_DEV")
        cur.execute("USE WAREHOUSE COMPUTE_WH")
        cur.execute(sql)
        rows = cur.fetchall()
    finally:
        cur.close()
    return _cast_numerics(pd.DataFrame(rows)) if rows else pd.DataFrame()


def render_sql_result():
    """Show the last result. Reads from session_state so chart selectors / downloads
    don't make the result disappear."""
    res = st.session_state.get("sql_result")
    if not res:
        return
    df = res["df"]
    st.success(f"✅ {len(df):,} rows · {len(df.columns)} columns · {res['secs']:.2f}s")
    if df.empty:
        st.info("The statement ran successfully but returned no rows.")
        return
    t1, t2, t3 = st.tabs(["📋 Results", "📊 Auto Chart", "📈 Stats"])
    with t1:
        st.dataframe(df, use_container_width=True, hide_index=True)
        st.download_button("⬇️ Download CSV", df.to_csv(index=False).encode(),
                           "query_result.csv", "text/csv", key="dl_csv")
    with t2:
        num_cols = df.select_dtypes(include="number").columns.tolist()
        str_cols = df.select_dtypes(exclude="number").columns.tolist()
        if num_cols and str_cols:
            chart_type = st.selectbox("Chart type", ["Bar", "Line", "Scatter", "Pie", "Treemap"],
                                      key="chart_type_sel")
            x_col = st.selectbox("X axis", str_cols, key="x_sel")
            y_col = st.selectbox("Y axis", num_cols, key="y_sel")
            plot_df = df.head(50)
            if chart_type == "Bar":
                fig = px.bar(plot_df, x=x_col, y=y_col, color_discrete_sequence=CLR)
            elif chart_type == "Line":
                fig = px.line(plot_df, x=x_col, y=y_col, color_discrete_sequence=CLR)
            elif chart_type == "Scatter":
                fig = px.scatter(plot_df, x=x_col, y=y_col, color_discrete_sequence=CLR)
            elif chart_type == "Pie":
                fig = px.pie(plot_df, names=x_col, values=y_col, color_discrete_sequence=CLR)
            else:
                fig = px.treemap(plot_df, path=[x_col], values=y_col)
            fig.update_layout(**PL, height=400, title=f"{y_col} by {x_col}")
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Need at least one text/date column and one numeric column to chart.")
    with t3:
        try:
            st.dataframe(df.describe(), use_container_width=True)
        except Exception:
            st.info("No statistics available for this result.")


_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_$]*$")


# ── render ────────────────────────────────────────────────────────────────────
def render():
    st.markdown("## 📊 SportZone Analytics — Complete Dashboard")

    # ── Problem Statement Banner ──────────────────────────────────────────────
    with st.expander("📚 **What is this platform? Click to learn** — SportZone Data Engineering Platform", expanded=False):
        st.markdown(PIPELINE_EXPLANATION)
        col1, col2, col3 = st.columns(3)
        col1.info("**🎯 Business Goal**\nUnderstand sales performance, customer behavior, and inventory health to make data-driven retail decisions.")
        col2.info("**🏗️ Tech Stack**\nNeon PostgreSQL (OLTP source) → Python ETL → Snowflake (DWH) → Streamlit (Dashboard) → scikit-learn (ML)")
        col3.info("**📊 Data Volume**\n500+ customers · 10K+ orders · 20 products · 11 stores · 6 channels tracked daily")

    if st.button("🔄 Refresh data", key="btn_refresh_data"):
        st.cache_data.clear()
        st.rerun()
    st.divider()

    # One connection attempt up front: gives a clear message instead of six failed logins.
    try:
        get_sf_conn()
    except Exception as e:
        st.error(f"❌ Could not connect to Snowflake: {e}")
        st.info("If your Snowflake user requires MFA: enter the 6-digit code in the sidebar, "
                "click **Apply**, then open this page again within ~30 seconds. "
                "(A user of TYPE = LEGACY_SERVICE or key-pair auth needs no code.)")
        return

    dummy = st.session_state.get("_cache_bust", 0)
    tabs = st.tabs([
        "📈 Overview", "🛍️ Products", "👥 Customers",
        "📦 Inventory", "🎯 Promotions",
        "🔍 SQL Explorer", "🤖 AI Analytics", "🧪 ML / Data Science"
    ])

    # Pre-load data — one failing query no longer blocks the whole page
    load_errors = {}

    def _safe_load(fn, *args):
        try:
            return fn(*args)
        except Exception as e:
            load_errors[fn.__name__] = str(e)
            return pd.DataFrame()

    with st.spinner("Loading Snowflake data …"):
        kpi_df = _safe_load(load_kpis, dummy)
        sales = _safe_load(load_daily_sales, 180, dummy)
        catdf = _safe_load(load_category, dummy)
        custdf = _safe_load(load_customers, dummy)
        invdf = _safe_load(load_inventory, dummy)
        promodf = _safe_load(load_promo, dummy)

    if load_errors:
        with st.expander(f"⚠️ {len(load_errors)} data query(ies) failed — click for details", expanded=False):
            for name, msg in load_errors.items():
                st.markdown(f"**{name}**")
                st.code(msg)

    has_ai = bool(get_openai_key())

    def ai_story(prompt):
        if has_ai:
            with st.expander("💡 AI Insight", expanded=False):
                with st.spinner("Generating insight …"):
                    st.markdown(ask_openai(prompt, "You are a data analyst. Be concise, 2-3 sentences."))

    # ── TAB 1: Overview ───────────────────────────────
    with tabs[0]:
        st.markdown("### Key Performance Indicators — Snowflake DWH")
        r = {}
        if not kpi_df.empty:
            r = {k: (float(v) if v is not None and v == v else 0.0)
                 for k, v in kpi_df.iloc[0].items()}
            c = st.columns(8)
            c[0].metric("💰 Revenue",     fmt(r.get("TOTAL_REVENUE", 0), "$"))
            c[1].metric("💵 Profit",      fmt(r.get("TOTAL_PROFIT", 0), "$"))
            c[2].metric("📦 Orders",      fmt(r.get("TOTAL_ORDERS", 0)))
            c[3].metric("👤 Customers",   fmt(r.get("UNIQUE_CUSTOMERS", 0)))
            c[4].metric("👟 Units",       fmt(r.get("UNITS_SOLD", 0)))
            c[5].metric("🛒 AOV",         fmt(r.get("AVG_ORDER_VALUE", 0), "$", d=0))
            c[6].metric("📊 Margin",      f"{r.get('MARGIN_PCT', 0):.1f}%")
            c[7].metric("↩️ Return Rate", f"{r.get('RETURN_RATE', 0):.1f}%")

            with st.expander("📚 Understanding these KPIs — click to learn", expanded=False):
                k1, k2, k3 = st.columns(3)
                k1.info(CHART_HELP["kpi_revenue"] + "\n\n" + CHART_HELP["kpi_profit"])
                k2.info(CHART_HELP["kpi_margin"] + "\n\n" + CHART_HELP["kpi_aov"])
                k3.info(CHART_HELP["kpi_orders"] + "\n\n" + CHART_HELP["kpi_return"])
        else:
            st.info("No KPI data yet — run the ETL pipeline first (Management → ETL Pipeline).")

        if not sales.empty:
            st.markdown("---")
            sales2 = sales.copy()
            sales2["FULL_DATE"] = pd.to_datetime(sales2["FULL_DATE"])
            daily = sales2.groupby("FULL_DATE").agg(
                REVENUE=("REVENUE", "sum"), ORDERS=("ORDERS", "sum")
            ).reset_index()
            daily["MA7"] = daily["REVENUE"].rolling(7, min_periods=1).mean()
            daily["MA30"] = daily["REVENUE"].rolling(30, min_periods=1).mean()

            fig = make_subplots(specs=[[{"secondary_y": True}]])
            fig.add_scatter(x=daily["FULL_DATE"], y=daily["REVENUE"], name="Revenue",
                            line=dict(color=CLR[0], width=1.5), opacity=.6, secondary_y=False)
            fig.add_scatter(x=daily["FULL_DATE"], y=daily["MA7"], name="7d MA",
                            line=dict(color=CLR[1], width=2), secondary_y=False)
            fig.add_scatter(x=daily["FULL_DATE"], y=daily["MA30"], name="30d MA",
                            line=dict(color=CLR[2], width=2, dash="dash"), secondary_y=False)
            fig.add_bar(x=daily["FULL_DATE"], y=daily["ORDERS"], name="Orders",
                        marker_color=CLR[4], opacity=.3, secondary_y=True)
            fig.update_layout(**PL, title="Revenue Trend (Snowflake)", height=360,
                              legend=dict(orientation="h", yanchor="bottom", y=1.02))
            fig.update_yaxes(title_text="Revenue ($)", showgrid=True, gridcolor=GRID, secondary_y=False)
            fig.update_yaxes(title_text="Orders", showgrid=False, secondary_y=True)
            st.plotly_chart(fig, use_container_width=True)

            if not catdf.empty:
                cc1, cc2 = st.columns(2)
                with cc1:
                    ch = sales2.groupby("CHANNEL")["REVENUE"].sum().reset_index()
                    fig = px.bar(ch, x="CHANNEL", y="REVENUE", color="CHANNEL",
                                 title="Revenue by Channel", color_discrete_sequence=CLR)
                    fig.update_layout(**PL, height=300, showlegend=False)
                    st.plotly_chart(fig, use_container_width=True)
                with cc2:
                    cat = catdf.groupby("PRD_CATEGORY")["REVENUE"].sum().reset_index()
                    fig = px.pie(cat, names="PRD_CATEGORY", values="REVENUE",
                                 title="Revenue by Category", color_discrete_sequence=CLR)
                    fig.update_layout(**PL, height=300)
                    st.plotly_chart(fig, use_container_width=True)

            monthly = sales2.groupby(["YEAR", "MONTH_NAME"])["REVENUE"].sum().reset_index()
            fig = px.bar(monthly, x="MONTH_NAME", y="REVENUE", color="YEAR",
                         barmode="group", title="Monthly Revenue by Year",
                         color_discrete_sequence=CLR)
            fig.update_layout(**PL, height=300)
            st.plotly_chart(fig, use_container_width=True)

        ai_story(f"Summarise these KPIs for SportZone retail: revenue={fmt(r.get('TOTAL_REVENUE', 0), '$')}, "
                 f"margin={r.get('MARGIN_PCT', 0):.1f}%")

    # ── TAB 2: Products ───────────────────────────────
    with tabs[1]:
        st.markdown("### Product Performance")
        if not catdf.empty:
            fig = px.treemap(catdf, path=["PRD_CATEGORY", "PRD_SUBCATEGORY", "BRAND"],
                             values="REVENUE", color="MARGIN",
                             color_continuous_scale=["#f85149", "#ffa657", "#3fb950"],
                             title="Revenue Treemap — Category > Subcategory > Brand")
            fig.update_layout(**PL, height=420)
            st.plotly_chart(fig, use_container_width=True)

            p1, p2 = st.columns(2)
            with p1:
                top = catdf.nlargest(10, "REVENUE")[["BRAND", "PRD_SUBCATEGORY", "REVENUE", "MARGIN", "UNITS"]].copy()
                top["REVENUE"] = top["REVENUE"].map(lambda x: f"${x:,.0f}")
                top["MARGIN"] = top["MARGIN"].map(lambda x: f"{x:.1f}%")
                st.markdown("#### 🏆 Top 10 by Revenue")
                st.dataframe(top, use_container_width=True, hide_index=True)
            with p2:
                brand = catdf.groupby("BRAND").agg(REVENUE=("REVENUE", "sum"), UNITS=("UNITS", "sum"),
                                                   MARGIN=("MARGIN", "mean")).reset_index()
                fig = px.scatter(brand, x="REVENUE", y="MARGIN", size="UNITS",
                                 color="BRAND", text="BRAND", title="Brand — Revenue vs Margin")
                fig.update_layout(**PL, height=340)
                st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No product data available.")

        ai_story(f"Top category is {catdf.iloc[0]['PRD_CATEGORY'] if not catdf.empty else 'N/A'} — give 2 product strategy insights.")

    # ── TAB 3: Customers ──────────────────────────────
    with tabs[2]:
        st.markdown("### Customer Analytics & Segmentation")
        if not custdf.empty:
            custdf["RECENCY_DAYS"] = pd.to_numeric(custdf["RECENCY_DAYS"], errors="coerce").fillna(999)
            custdf["LTV"] = pd.to_numeric(custdf["LTV"], errors="coerce").fillna(0)
            custdf["ORDERS"] = pd.to_numeric(custdf["ORDERS"], errors="coerce").fillna(0)
            custdf["AOV"] = pd.to_numeric(custdf["AOV"], errors="coerce").fillna(0)

            k1, k2, k3, k4 = st.columns(4)
            k1.metric("Total Customers", f"{len(custdf):,}")
            k2.metric("Avg LTV", fmt(custdf["LTV"].mean(), "$", d=0))
            k3.metric("Avg Orders", f"{custdf['ORDERS'].mean():.1f}")
            k4.metric("Avg AOV", fmt(custdf["AOV"].mean(), "$", d=0))

            rc1, rc2 = st.columns(2)
            with rc1:
                tier = custdf.groupby("LOYALTY_TIER").agg(
                    CUSTOMERS=("CUST_ID", "count"), AVG_LTV=("LTV", "mean")).reset_index()
                fig = px.bar(tier, x="LOYALTY_TIER", y="CUSTOMERS", color="AVG_LTV",
                             text="CUSTOMERS", title="Customers by Loyalty Tier",
                             color_continuous_scale=["#1f6feb", "#3fb950"])
                fig.update_layout(**PL, height=320)
                st.plotly_chart(fig, use_container_width=True)
            with rc2:
                fig = px.scatter(custdf.head(300), x="RECENCY_DAYS", y="LTV",
                                 size="ORDERS", color="LOYALTY_TIER", hover_data=["FULL_NAME", "AOV"],
                                 title="RFM Scatter — Recency vs LTV",
                                 color_discrete_sequence=CLR)
                fig.update_layout(**PL, height=320)
                st.plotly_chart(fig, use_container_width=True)

            a1, a2 = st.columns(2)
            with a1:
                age = custdf.groupby("AGE_BAND")["LTV"].mean().reset_index()
                fig = px.bar(age, x="AGE_BAND", y="LTV", color="AGE_BAND",
                             title="Avg LTV by Age Band", color_discrete_sequence=CLR)
                fig.update_layout(**PL, height=280)
                st.plotly_chart(fig, use_container_width=True)
            with a2:
                gen = custdf.groupby("GENDER")["LTV"].sum().reset_index()
                fig = px.pie(gen, names="GENDER", values="LTV",
                             title="LTV by Gender", color_discrete_sequence=CLR)
                fig.update_layout(**PL, height=280)
                st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No customer data available.")

        ai_story("Give 2 customer retention strategies based on typical RFM analysis for a retail company.")

    # ── TAB 4: Inventory ──────────────────────────────
    with tabs[3]:
        st.markdown("### Inventory Management")
        if not invdf.empty:
            invdf = invdf.copy()
            invdf["NEEDS_REORDER"] = invdf["NEEDS_REORDER"].fillna(False).astype(bool)
            i1, i2, i3, i4 = st.columns(4)
            i1.metric("SKU-Locations", f"{len(invdf):,}")
            i2.metric("Out of Stock", f"{(invdf['INV_STATUS'] == 'OUT_OF_STOCK').sum():,}", delta_color="inverse")
            i3.metric("Needs Reorder", f"{int(invdf['NEEDS_REORDER'].sum()):,}")
            i4.metric("Stock Value", fmt(pd.to_numeric(invdf["STOCK_VALUE"], errors="coerce").sum(), "$"))

            s1, s2 = st.columns(2)
            with s1:
                sc = invdf["INV_STATUS"].value_counts().reset_index()
                sc.columns = ["Status", "Count"]
                fig = px.pie(sc, names="Status", values="Count", title="Inventory Status Mix",
                             color_discrete_map={"STOCKED": "#3fb950", "LIMITED": "#d29922", "OUT_OF_STOCK": "#f85149"})
                fig.update_layout(**PL, height=300)
                st.plotly_chart(fig, use_container_width=True)
            with s2:
                rv = invdf.groupby("REGION")["STOCK_VALUE"].apply(
                    lambda x: pd.to_numeric(x, errors="coerce").sum()
                ).reset_index()
                fig = px.bar(rv, x="REGION", y="STOCK_VALUE", color="REGION",
                             title="Stock Value by Region", color_discrete_sequence=CLR)
                fig.update_layout(**PL, height=300)
                st.plotly_chart(fig, use_container_width=True)

            alerts = invdf[invdf["NEEDS_REORDER"]].sort_values("QTY_ON_HAND")
            if not alerts.empty:
                st.markdown("#### ⚠️ Reorder Alerts")
                st.dataframe(alerts[["PRD_NAME", "BRAND", "STORE_NAME", "REGION",
                                     "QTY_ON_HAND", "REORDER_POINT", "INV_STATUS"]].head(20),
                             use_container_width=True, hide_index=True)
        else:
            st.info("No inventory data available.")

    # ── TAB 5: Promotions ─────────────────────────────
    with tabs[4]:
        st.markdown("### 🎯 Promotion Effectiveness & Campaign Analytics")

        if promodf.empty:
            st.info("No promotion data available.")
        else:
            # ── KPIs ──────────────────────────────────────────
            pk1, pk2, pk3, pk4, pk5 = st.columns(5)
            total_promo_orders = int(promodf["ORDERS"].sum())
            total_discount = float(promodf["DISCOUNT_GIVEN"].sum())
            total_promo_revenue = float(promodf["REVENUE"].sum())
            total_promo_profit = float(promodf["PROFIT"].sum())
            avg_disc_rate = float(promodf["DISC_RATE"].mean())
            pk1.metric("🎟️ Promo Orders",   f"{total_promo_orders:,}")
            pk2.metric("💸 Discount Given", fmt(total_discount, "$"))
            pk3.metric("💰 Promo Revenue",  fmt(total_promo_revenue, "$"))
            pk4.metric("📊 Promo Profit",   fmt(total_promo_profit, "$"))
            pk5.metric("🏷️ Avg Disc Rate",  f"{avg_disc_rate:.1f}%")

            st.divider()
            pr1, pr2 = st.columns(2)

            with pr1:
                fig = px.scatter(promodf, x="DISCOUNT_GIVEN", y="REVENUE",
                                 size="ORDERS", color="PROMO_TYPE", text="PROMO_CODE",
                                 title="Promotion ROI — Discount Cost vs Revenue",
                                 color_discrete_sequence=CLR,
                                 labels={"DISCOUNT_GIVEN": "Discount ($)", "REVENUE": "Revenue ($)"})
                fig.update_traces(textposition="top center")
                fig.update_layout(**PL, height=380)
                st.plotly_chart(fig, use_container_width=True)

            with pr2:
                fig = go.Figure(go.Waterfall(
                    name="Promo P&L",
                    orientation="v",
                    measure=["relative"] * len(promodf) + ["total"],
                    x=list(promodf["PROMO_CODE"]) + ["NET"],
                    y=list(promodf["PROFIT"].astype(float)) + [0],
                    connector={"line": {"color": "#30363d"}},
                    increasing={"marker": {"color": "#3fb950"}},
                    decreasing={"marker": {"color": "#f85149"}},
                    totals={"marker": {"color": "#58a6ff"}},
                ))
                fig.update_layout(**PL, title="Profit Waterfall by Promotion", height=380)
                st.plotly_chart(fig, use_container_width=True)

            pr3, pr4 = st.columns(2)
            with pr3:
                fig = px.bar(promodf.sort_values("ORDERS", ascending=True),
                             x="ORDERS", y="PROMO_CODE", orientation="h",
                             color="PROMO_TYPE", title="Orders per Promotion",
                             color_discrete_sequence=CLR)
                fig.update_layout(**PL, height=320)
                st.plotly_chart(fig, use_container_width=True)

            with pr4:
                peff = promodf.copy()
                peff["EFFICIENCY"] = peff["REVENUE"] / peff["DISCOUNT_GIVEN"].replace(0, np.nan)
                peff = peff.sort_values("EFFICIENCY", ascending=False)
                fig = px.bar(peff, x="PROMO_CODE", y="EFFICIENCY",
                             color="EFFICIENCY", title="Revenue per $1 Discount (Efficiency)",
                             color_continuous_scale=["#f85149", "#3fb950"],
                             text=peff["EFFICIENCY"].map(lambda x: f"${x:.1f}"))
                fig.update_layout(**PL, height=320)
                fig.update_traces(textposition="outside")
                st.plotly_chart(fig, use_container_width=True)

            peff2 = promodf.copy()
            peff2["PROFIT_MARGIN"] = peff2["PROFIT"] / peff2["REVENUE"].replace(0, np.nan) * 100
            fig = px.scatter(peff2, x="DISC_RATE", y="PROFIT_MARGIN",
                             size="ORDERS", color="PROMO_TYPE", text="PROMO_CODE",
                             title="Discount Rate vs Profit Margin — Sweet Spot Analysis",
                             labels={"DISC_RATE": "Discount Rate (%)", "PROFIT_MARGIN": "Profit Margin (%)"},
                             color_discrete_sequence=CLR)
            fig.add_vline(x=avg_disc_rate, line_dash="dash", line_color="#d29922",
                          annotation_text=f"Avg disc {avg_disc_rate:.1f}%")
            fig.update_layout(**PL, height=360)
            st.plotly_chart(fig, use_container_width=True)

            st.markdown("#### 📋 Promotion Detail")
            disp = promodf.copy()
            disp["DISCOUNT_GIVEN"] = disp["DISCOUNT_GIVEN"].map(lambda x: f"${x:,.0f}")
            disp["REVENUE"] = disp["REVENUE"].map(lambda x: f"${x:,.0f}")
            disp["PROFIT"] = disp["PROFIT"].map(lambda x: f"${x:,.0f}")
            disp["DISC_RATE"] = disp["DISC_RATE"].map(lambda x: f"{x:.1f}%")
            st.dataframe(disp, use_container_width=True, hide_index=True)

        ai_story("For a sporting goods retailer, what makes a promotion effective? Give 2 insights based on discount rate vs profit margin tradeoffs.")

    # ── TAB 6: SQL Explorer ───────────────────────────
    with tabs[5]:
        st.markdown("### 🔍 SQL Explorer — Snowflake Ready")

        # 1) Apply text queued by Load / Format / AI buttons BEFORE the editor is created.
        if "sql_pending" in st.session_state:
            st.session_state["sql_editor_area"] = st.session_state.pop("sql_pending")
        if "sql_editor_area" not in st.session_state:
            st.session_state["sql_editor_area"] = DEFAULT_SQL

        left_col, right_col = st.columns([3, 1])

        with right_col:
            st.markdown("**📚 Query Library**")
            difficulty = st.selectbox("Level", list(PREWRITTEN_QS.keys()), key="q_diff")
            # key depends on the level, so the query list always matches the chosen level
            question = st.selectbox("Query", list(PREWRITTEN_QS[difficulty].keys()),
                                    key=f"q_sel_{difficulty}")
            if st.button("📥 Load", key="btn_load_q", use_container_width=True):
                queue_sql(PREWRITTEN_QS[difficulty][question].strip())
            st.divider()
            st.markdown("**⚡ Quick Actions**")
            if st.button("✨ Format SQL", key="btn_fmt", use_container_width=True):
                queue_sql(format_sql(st.session_state.get("sql_editor_area", "")))
            if has_ai and st.button("🤖 AI Optimise", key="btn_ai_opt", use_container_width=True):
                raw = st.session_state.get("sql_editor_area", "")
                if raw.strip():
                    with st.spinner("AI optimising …"):
                        fixed = ask_openai(
                            f"Optimise this Snowflake SQL for performance and readability. Return only the SQL:\n{raw}",
                            "You are a Snowflake SQL expert.")
                    if fixed.startswith(("⚠️", "❌")):
                        st.error(fixed)
                    else:
                        queue_sql(strip_fences(fixed))
            if st.button("🗑️ Clear", key="btn_clear_sql", use_container_width=True):
                queue_sql("")

        with left_col:
            sql = st.text_area(
                "✏️ Edit SQL (Snowflake dialect):",
                height=260, key="sql_editor_area",
                help="Full Snowflake SQL — one statement at a time. DDL, DML, CTEs, window functions, QUALIFY, etc."
            )
            if sql.strip():
                with st.expander("🎨 Syntax Highlighted Preview", expanded=False):
                    st.markdown(highlight_sql(format_sql(sql)), unsafe_allow_html=True)

        # ── Run buttons ────────────────────────────────────────────────────────
        rc1, rc2, rc3, rc4 = st.columns([1, 1, 1, 2])
        run_btn = rc1.button("▶️ Run", type="primary", key="btn_run_sql")
        explain_btn = rc2.button("📖 Explain", key="btn_explain") if has_ai else False
        plan_btn = rc3.button("📋 Exec Plan", key="btn_plan")

        clean_sql = sql.strip().rstrip(";").strip()

        if run_btn:
            if not clean_sql:
                st.warning("The editor is empty — pick a query and click **Load**, or type your own SQL.")
            elif ";" in clean_sql:
                st.error("Run one statement at a time (remove the extra `;`).")
            else:
                with st.spinner("Running on Snowflake …"):
                    t0 = time.perf_counter()
                    try:
                        result_df = run_sql(clean_sql)
                        st.session_state["sql_result"] = {
                            "df": result_df, "secs": time.perf_counter() - t0}
                    except Exception as e:
                        st.session_state.pop("sql_result", None)
                        st.error(f"SQL Error: {e}")
                        if has_ai:
                            with st.spinner("AI diagnosing error …"):
                                fix = ask_openai(
                                    f"Snowflake SQL error: {e}\nSQL: {clean_sql}\nFix it — return only corrected SQL.",
                                    "You are a Snowflake SQL expert.")
                            if not fix.startswith(("⚠️", "❌")):
                                st.markdown("**AI Suggested Fix:**")
                                st.code(strip_fences(fix), language="sql")

        render_sql_result()

        if explain_btn and clean_sql:
            with st.spinner("AI explaining SQL …"):
                explanation = ask_openai(
                    f"Explain this Snowflake SQL in plain English, step by step:\n{clean_sql}",
                    "You are a data analyst explaining SQL to a business user.")
            st.info(explanation)

        if plan_btn and clean_sql:
            with st.spinner("Fetching execution plan …"):
                try:
                    st.dataframe(run_sql(f"EXPLAIN {clean_sql}"), use_container_width=True, hide_index=True)
                except Exception as e:
                    st.warning(f"Plan not available: {e}")

        # ── Bulk Load from CSV ─────────────────────────────────────────────────
        st.divider()
        st.markdown("#### 📂 Bulk Load CSV → Snowflake (COPY INTO)")
        with st.expander("Upload a CSV file to load into a Snowflake table", expanded=False):
            ul1, ul2 = st.columns([2, 1])
            with ul2:
                target_db = st.text_input("Database", value="SPORTZONE_DEV", key="ul_db")
                target_sch = st.text_input("Schema", value="RAW", key="ul_sch")
                target_tbl = st.text_input("Table", value="", key="ul_tbl",
                                           placeholder="e.g. RAW_CUSTOMERS")
                delimiter = st.selectbox("Delimiter", [",", "|", "\t", "~"], key="ul_delim")
                skip_header = st.checkbox("Skip header row", value=True, key="ul_header")
                on_error = st.selectbox("On error",
                                        ["CONTINUE", "ABORT_STATEMENT", "SKIP_FILE"], key="ul_onerr")
            with ul1:
                uploaded = st.file_uploader("Choose CSV file", type=["csv", "txt"], key="ul_file")
                if uploaded and target_tbl:
                    st.info(f"File: **{uploaded.name}** ({uploaded.size/1024:.1f} KB)")
                    if st.button("🚀 Upload & Load", type="primary", key="btn_ul_load"):
                        if not all(_IDENT_RE.match(x) for x in (target_db, target_sch, target_tbl)):
                            st.error("Database, schema and table may only contain letters, digits and underscores.")
                        else:
                            import tempfile, os
                            with st.spinner("Uploading to Snowflake stage and running COPY INTO …"):
                                tmp_path = None
                                try:
                                    conn2 = get_sf_conn()          # shared session — no new MFA login
                                    cur2 = conn2.cursor()
                                    cur2.execute(f"USE DATABASE {target_db}")
                                    cur2.execute("USE WAREHOUSE COMPUTE_WH")

                                    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".csv", mode="wb")
                                    tmp.write(uploaded.getvalue())
                                    tmp.close()
                                    tmp_path = tmp.name

                                    delim_map = {",": "','", "|": "'|'", "\t": "'\\t'", "~": "'~'"}
                                    delim_str = delim_map.get(delimiter, "','")

                                    cur2.execute(f"""
                                        CREATE OR REPLACE TEMPORARY FILE FORMAT tmp_fmt
                                        TYPE=CSV FIELD_DELIMITER={delim_str}
                                        SKIP_HEADER={1 if skip_header else 0}
                                        NULL_IF=('NULL','null','')
                                        EMPTY_FIELD_AS_NULL=TRUE
                                    """)
                                    stage = f"@{target_db}.{target_sch}.%{target_tbl}"
                                    cur2.execute(f"PUT file://{tmp_path.replace(chr(92), '/')} {stage} "
                                                 f"OVERWRITE=TRUE AUTO_COMPRESS=TRUE")
                                    cur2.fetchall()

                                    cur2.execute(f"""
                                        COPY INTO {target_db}.{target_sch}.{target_tbl}
                                        FROM {stage}
                                        PATTERN='.*{Path(tmp_path).stem}.*'
                                        FILE_FORMAT=(FORMAT_NAME='tmp_fmt')
                                        ON_ERROR='{on_error}'
                                    """)
                                    copy_res = cur2.fetchall()
                                    cur2.close()

                                    loaded = sum(int(r[3]) for r in copy_res
                                                 if len(r) > 3 and str(r[3]).isdigit())
                                    st.success(f"✅ Loaded **{loaded:,} rows** into "
                                               f"`{target_db}.{target_sch}.{target_tbl}`")
                                    st.dataframe(pd.DataFrame(copy_res), use_container_width=True, hide_index=True)
                                except Exception as e:
                                    st.error(f"Load failed: {e}")
                                finally:
                                    if tmp_path and os.path.exists(tmp_path):
                                        os.unlink(tmp_path)

        # ── Query library reference (no nested expanders — Streamlit forbids them) ──
        st.divider()
        with st.expander("📖 Full Query Library Reference", expanded=False):
            level_tabs = st.tabs(list(PREWRITTEN_QS.keys()))
            for lvl_tab, (level, qs) in zip(level_tabs, PREWRITTEN_QS.items()):
                with lvl_tab:
                    for i, (qname, qsql) in enumerate(qs.items()):
                        st.markdown(f"**{qname}**")
                        st.code(qsql.strip(), language="sql")
                        if st.button(f"📥 Load '{qname}' into editor", key=f"ref_load_{level}_{i}"):
                            queue_sql(qsql.strip())
                        st.divider()

    # ── TAB 7: AI Analytics ───────────────────────────
    with tabs[6]:
        st.markdown("### 🤖 AI-Powered Analytics")

        if not has_ai:
            st.warning("⚠️ Configure your OpenAI API key in **Management → AI Settings** to enable AI features.")
            st.info("AI features: Natural Language → SQL, automated insights, chart storytelling, anomaly detection, business recommendations.")
        else:
            st.markdown("#### 💬 Natural Language → Snowflake SQL")
            nl_input = st.text_area(
                "Ask a business question in plain English:",
                placeholder="e.g. Show me total revenue by country for GOLD tier customers in 2024",
                height=80, key="nl_q"
            )
            c_nl1, c_nl2 = st.columns([1, 3])
            if c_nl1.button("🔮 Generate SQL", type="primary", key="btn_nl_sql") and nl_input:
                with st.spinner("Generating SQL …"):
                    system = (f"You are a Snowflake SQL expert. Schema:\n{SF_SCHEMA}\n"
                              f"Return ONLY the SQL query, no explanations, no markdown.")
                    sql_out = ask_openai(nl_input, system)
                    st.session_state["ai_sql"] = strip_fences(sql_out)

            if st.session_state.get("ai_sql"):
                formatted_ai = format_sql(st.session_state["ai_sql"])
                st.markdown(highlight_sql(formatted_ai), unsafe_allow_html=True)
                col_run, col_load = st.columns(2)
                if col_run.button("▶️ Run Generated SQL", key="btn_run_ai_sql"):
                    with st.spinner("Running …"):
                        try:
                            df = run_sql(st.session_state["ai_sql"].strip().rstrip(";"))
                            st.success(f"✅ {len(df):,} rows")
                            st.dataframe(df, use_container_width=True, hide_index=True)
                            num_cols = df.select_dtypes(include="number").columns.tolist()
                            str_cols = df.select_dtypes(exclude="number").columns.tolist()
                            if num_cols and str_cols:
                                fig = px.bar(df.head(20), x=str_cols[0], y=num_cols[0],
                                             color_discrete_sequence=CLR,
                                             title=f"Result: {nl_input[:60]}")
                                fig.update_layout(**PL, height=320)
                                st.plotly_chart(fig, use_container_width=True)
                        except Exception as e:
                            st.error(f"Error: {e}")
                if col_load.button("📥 Load into SQL Editor", key="btn_ai_to_editor"):
                    queue_sql(st.session_state["ai_sql"])

            st.divider()
            st.markdown("#### 🧠 AI Business Insights")
            if st.button("Generate Automated Insights", key="btn_insights"):
                if not kpi_df.empty:
                    kr = kpi_df.iloc[0]
                    kpi_str = (f"Revenue=${float(kr.get('TOTAL_REVENUE', 0) or 0):,.0f}, "
                               f"Profit=${float(kr.get('TOTAL_PROFIT', 0) or 0):,.0f}, "
                               f"Margin={float(kr.get('MARGIN_PCT', 0) or 0):.1f}%, "
                               f"Orders={kr.get('TOTAL_ORDERS', 0)}")
                else:
                    kpi_str = "N/A"
                top_cat = catdf.iloc[0]["PRD_CATEGORY"] if not catdf.empty else "N/A"
                oos = int((invdf["INV_STATUS"] == "OUT_OF_STOCK").sum()) if not invdf.empty else 0
                with st.spinner("Analysing …"):
                    insights = ask_openai(
                        f"SportZone retail: KPIs={kpi_str}, Top cat={top_cat}, OOS SKUs={oos}. "
                        f"5 specific actionable insights with priority.",
                        "You are a retail analytics consultant.")
                st.markdown(insights)

            st.divider()
            st.markdown("#### 🎯 Suggested Business Questions")
            if st.button("Get AI Question Suggestions", key="btn_suggest_q"):
                with st.spinner("Thinking …"):
                    sugg = ask_openai(
                        "For SportZone sporting goods retailer with Snowflake DWH, "
                        "suggest 8 business questions from operational to strategic. Numbered list.",
                        "You are a retail data analyst.")
                st.markdown(sugg)

    # ── TAB 8: ML / Data Science ──────────────────────
    with tabs[7]:
        st.markdown("### 🧪 ML & Data Science")
        ml_section = st.radio(
            "Model",
            ["📈 Forecast", "🎯 Segmentation", "🚨 Churn",
             "🛒 Market Basket", "📊 Anomaly Detection"],
            horizontal=True,
            key="ml_nav",
        )
        st.divider()

        # ── Forecast ──────────────────────────────────────────────────
        if ml_section == "📈 Forecast":
            st.markdown("#### Revenue Forecast — Linear Regression + Trend")
            if sales.empty or len(sales) < 14:
                st.info("Need at least 14 days of sales data.")
            else:
                with st.expander("📚 Learn: What is Revenue Forecasting?", expanded=False):
                    st.markdown("""
**Linear Regression Forecasting** fits a straight line through historical daily revenue data.

- **X-axis (input)**: Days since start (0, 1, 2, 3...)
- **Y-axis (output)**: Daily revenue ($)
- **Formula**: Revenue = slope × day + intercept
- **Slope > 0** = business growing, **slope < 0** = declining
- **R² score**: How well the line fits (0.0 = random, 1.0 = perfect fit)
- **Confidence Band (95% CI)**: The shaded area — actual revenue should fall here 95% of the time

**Limitation**: Linear regression assumes the trend continues in a straight line. Real revenue has seasonality and cycles that require more advanced models (ARIMA, Prophet).
                    """)
                try:
                    from sklearn.linear_model import LinearRegression
                    fc1, fc2 = st.columns([3, 1])
                    with fc2:
                        horizon = st.slider("Forecast horizon (days)", 7, 90, 30, key="fc_horizon")
                        show_ci = st.checkbox("Show confidence band", True, key="fc_ci")
                    sales3 = sales.copy()
                    sales3["FULL_DATE"] = pd.to_datetime(sales3["FULL_DATE"], errors="coerce")
                    sales3["REVENUE"] = pd.to_numeric(sales3["REVENUE"], errors="coerce").fillna(0.0)
                    daily3 = (sales3.groupby("FULL_DATE")["REVENUE"]
                              .sum().reset_index().sort_values("FULL_DATE")
                              .dropna(subset=["FULL_DATE"]).reset_index(drop=True))
                    if len(daily3) < 14:
                        st.info("Not enough daily data points yet.")
                    else:
                        t0 = pd.Timestamp(daily3["FULL_DATE"].iloc[0])
                        daily3["t"] = [(pd.Timestamp(d) - t0).days for d in daily3["FULL_DATE"]]
                        X = np.array(daily3["t"].tolist(), dtype=float).reshape(-1, 1)
                        y = np.array(daily3["REVENUE"].tolist(), dtype=float)
                        mdl = LinearRegression().fit(X, y)
                        resid_std = float(np.std(y - mdl.predict(X)))
                        horizon_int = int(horizon)
                        last_t = float(daily3["t"].iloc[-1])
                        ft = np.arange(last_t + 1, last_t + horizon_int + 1, dtype=float).reshape(-1, 1)
                        tmax = pd.Timestamp(daily3["FULL_DATE"].iloc[-1])
                        fdates = pd.date_range(tmax + pd.Timedelta(days=1), periods=horizon_int)
                        fcast = np.array(mdl.predict(ft), dtype=float)
                        slope = float(mdl.coef_[0])
                        with fc1:
                            fig = go.Figure()
                            fig.add_scatter(
                                x=daily3["FULL_DATE"], y=y.tolist(), name="Actual",
                                line=dict(color=CLR[0], width=1.5), opacity=.8,
                                hovertemplate="<b>%{x}</b><br>Revenue: $%{y:,.0f}<extra>Actual Daily Revenue</extra>")
                            fig.add_scatter(
                                x=daily3["FULL_DATE"],
                                y=pd.Series(y).rolling(7, min_periods=1).mean().tolist(),
                                name="7d MA", line=dict(color=CLR[1], width=2),
                                hovertemplate="<b>%{x}</b><br>7-Day Avg: $%{y:,.0f}<extra>Rolling Average</extra>")
                            fig.add_scatter(
                                x=fdates, y=fcast.tolist(), name="Forecast",
                                line=dict(color=CLR[2], width=2.5, dash="dash"),
                                hovertemplate="<b>%{x}</b><br>Forecast: $%{y:,.0f}<extra>Predicted Revenue</extra>")
                            if show_ci:
                                upper = (fcast + 1.96 * resid_std).tolist()
                                lower = (fcast - 1.96 * resid_std).tolist()
                                fig.add_scatter(
                                    x=list(fdates) + list(fdates[::-1]),
                                    y=upper + lower[::-1],
                                    fill="toself", fillcolor="rgba(210,153,34,0.12)",
                                    line=dict(color="rgba(0,0,0,0)"), name="95% CI",
                                    hoverinfo="skip")
                            try:
                                fig.add_shape(type="line",
                                              x0=tmax, x1=tmax, y0=0, y1=1, yref="paper",
                                              line=dict(color="#8b949e", width=1.5, dash="dot"))
                                fig.add_annotation(x=tmax, y=1, yref="paper",
                                                   text="◀ History | Forecast ▶",
                                                   showarrow=False, font=dict(color="#8b949e", size=11),
                                                   xanchor="left", yanchor="bottom")
                            except Exception:
                                pass
                            fig.update_layout(
                                **PL, title=f"📈 Revenue Forecast — Next {horizon_int} Days",
                                height=400,
                                legend=dict(orientation="h", yanchor="bottom", y=1.02))
                            st.plotly_chart(fig, use_container_width=True)
                            _explain("forecast")
                        fc_total = float(fcast.sum())
                        r2 = float(mdl.score(X, y))
                        direction = "📈 upward" if slope > 0 else "📉 downward"
                        m1, m2, m3, m4 = st.columns(4)
                        m1.metric("Daily Trend", f"${slope:+.0f}/day",
                                  help="Revenue change per day. Positive = growing business.")
                        m2.metric(f"{horizon_int}d Forecast", fmt(fc_total, "$"),
                                  help="Total predicted revenue over the forecast window.")
                        m3.metric("Model R²", f"{r2:.3f}",
                                  help="Fit quality: 1.0=perfect, 0.0=random. Above 0.5 is good.")
                        m4.metric("Volatility (σ)", fmt(resid_std, "$"),
                                  help="Standard deviation of daily revenue — how noisy the data is.")
                        st.info(
                            f"Revenue is trending **{direction}** at **${slope:+.0f}/day**. "
                            f"Model explains **{r2*100:.0f}%** of revenue variation (R²={r2:.3f}).")
                        if has_ai:
                            ai_story(
                                f"Revenue trend ${slope:+.0f}/day, R²={r2:.2f}, "
                                f"{horizon_int}d forecast=${fc_total:,.0f}. 2-sentence business implication?")
                except Exception as e:
                    st.error(f"Forecast error: {e}")
                    import traceback
                    st.code(traceback.format_exc())

        # ── Segmentation ──────────────────────────────────────────────
        elif ml_section == "🎯 Segmentation":
            st.markdown("#### Customer Segmentation — K-Means + PCA")
            if custdf.empty or len(custdf) < 10:
                st.info("Need at least 10 customers.")
            else:
                with st.expander("📚 Learn: What is K-Means Clustering?", expanded=False):
                    st.markdown("""
**K-Means** groups customers into K clusters based on similarity.

**Features used**: Recency (days since last order), LTV (lifetime spend), Orders (count), AOV (avg order value)

**Algorithm**:
1. Place K random centroids in feature space
2. Assign each customer to nearest centroid
3. Move centroid to mean of its cluster
4. Repeat until stable

**PCA (Principal Component Analysis)**: Reduces 4 features to 2D so we can visualize clusters. The axes (PCA1, PCA2) are combinations of the original features.

**Business Use**: Target each segment differently — Champions get loyalty rewards, At Risk get win-back offers.
                    """)
                _explain("segmentation")
                try:
                    from sklearn.preprocessing import StandardScaler
                    from sklearn.cluster import KMeans
                    from sklearn.decomposition import PCA as _PCA
                    seg_col1, seg_col2 = st.columns([3, 1])
                    with seg_col2:
                        n_clusters = st.slider("Segments", 2, 6, 4, key="km_k")
                    seg_df = custdf[["RECENCY_DAYS", "LTV", "ORDERS", "AOV"]].copy()
                    for _c in ["RECENCY_DAYS", "LTV", "ORDERS", "AOV"]:
                        seg_df[_c] = pd.to_numeric(seg_df[_c], errors="coerce").fillna(0)
                    seg_df["RECENCY_DAYS"] = seg_df["RECENCY_DAYS"].clip(upper=365)
                    seg_df["LTV"] = seg_df["LTV"].clip(upper=float(seg_df["LTV"].quantile(.99)) or 1)
                    Xs = StandardScaler().fit_transform(seg_df)
                    labels = KMeans(n_clusters=n_clusters, random_state=42, n_init=10).fit_predict(Xs)
                    cd2 = custdf.copy()
                    cd2["CLUSTER"] = labels
                    name_map = {s: n for s, n in zip(
                        cd2.groupby("CLUSTER")["LTV"].mean().sort_values(ascending=False).index,
                        ["Champions", "Loyal", "Promising", "At Risk", "Needs Attention", "Lost"][:n_clusters]
                    )}
                    cd2["SEGMENT"] = cd2["CLUSTER"].map(name_map)
                    pca_ = _PCA(n_components=2)
                    coords = pca_.fit_transform(Xs)
                    cd2["PCA1"] = coords[:, 0]
                    cd2["PCA2"] = coords[:, 1]
                    with seg_col1:
                        fig = px.scatter(cd2.head(400), x="PCA1", y="PCA2", color="SEGMENT",
                                         hover_data=["FULL_NAME", "LTV", "ORDERS"],
                                         title=f"Segments — PCA 2D (var={pca_.explained_variance_ratio_.sum()*100:.0f}%)",
                                         color_discrete_sequence=CLR)
                        fig.update_layout(**PL, height=400)
                        st.plotly_chart(fig, use_container_width=True)
                    summary = cd2.groupby("SEGMENT").agg(
                        Customers=("CUST_ID", "count"), Avg_LTV=("LTV", "mean"),
                        Avg_Orders=("ORDERS", "mean"), Avg_Recency=("RECENCY_DAYS", "mean"),
                    ).reset_index()
                    sb1, sb2 = st.columns([2, 1])
                    with sb1:
                        fig2 = px.bar(summary, x="SEGMENT", y="Customers", color="Avg_LTV",
                                      text="Customers", title="Segment Size & Avg LTV",
                                      color_continuous_scale=["#f85149", "#3fb950"])
                        fig2.update_layout(**PL, height=320)
                        st.plotly_chart(fig2, use_container_width=True)
                    with sb2:
                        disp = summary.copy()
                        disp["Avg_LTV"] = disp["Avg_LTV"].map(lambda x: f"${x:,.0f}")
                        disp["Avg_Orders"] = disp["Avg_Orders"].map(lambda x: f"{x:.1f}")
                        disp["Avg_Recency"] = disp["Avg_Recency"].map(lambda x: f"{x:.0f}d")
                        st.dataframe(disp, use_container_width=True, hide_index=True)
                    if has_ai:
                        ai_story(f"K-Means segments: {summary[['SEGMENT','Customers','Avg_LTV']].to_dict('records')}. "
                                 f"1 marketing action per segment.")
                except Exception as e:
                    st.error(f"Segmentation error: {e}")

        # ── Churn ─────────────────────────────────────────────────────
        elif ml_section == "🚨 Churn":
            st.markdown("#### Churn Risk Scoring")
            if custdf.empty:
                st.info("No customer data.")
            else:
                with st.expander("📚 Learn: What is Churn Prediction?", expanded=False):
                    st.markdown("""
**Customer Churn** = a customer stops buying. In retail, "churned" typically means no purchase in 90-180 days.

**Rule-Based Scoring** assigns risk points:
- Recency > 180 days → +3 points (hasn't bought in 6 months)
- Orders < 2 → +2 points (only bought once)
- Bronze loyalty tier → +1 point (lowest engagement)
- LTV below 25th percentile → +1 point (low value customer)

**Score ≥ 5 = High Risk** | **Score 3-4 = Medium** | **Score < 3 = Low**

**Business Impact**: Retaining a customer costs 5-7x less than acquiring a new one. Revenue at Risk = LTV of high-risk customers.
                    """)
                _explain("churn")
                try:
                    ch1, ch2 = st.columns([2, 1])
                    with ch2:
                        recency_hi = st.slider("High-risk recency (days)", 60, 365, 180, key="ch_hi")
                        recency_md = st.slider("Med-risk recency (days)", 30, 180, 90, key="ch_md")
                        min_orders = st.slider("Low order threshold", 1, 5, 2, key="ch_ord")
                    df_ch = custdf.copy()
                    for _c in ["RECENCY_DAYS", "ORDERS", "LTV"]:
                        df_ch[_c] = pd.to_numeric(df_ch[_c], errors="coerce").fillna(0)
                    _q25 = float(df_ch["LTV"].quantile(.25))

                    def _risk(row):
                        s = 0
                        rr = float(row["RECENCY_DAYS"])
                        if rr > recency_hi:
                            s += 3
                        elif rr > recency_md:
                            s += 2
                        elif rr > 30:
                            s += 1
                        if float(row["ORDERS"]) < min_orders:
                            s += 2
                        if str(row.get("LOYALTY_TIER", "")).upper() == "BRONZE":
                            s += 1
                        if float(row["LTV"]) < _q25:
                            s += 1
                        return s

                    df_ch["SCORE"] = df_ch.apply(_risk, axis=1)
                    df_ch["CHURN_RISK"] = df_ch["SCORE"].map(
                        lambda s: "🔴 High" if s >= 5 else ("🟡 Medium" if s >= 3 else "🟢 Low"))
                    with ch1:
                        cnt = df_ch["CHURN_RISK"].value_counts().reset_index()
                        cnt.columns = ["Risk", "Customers"]
                        fig = px.pie(cnt, names="Risk", values="Customers",
                                     title="Churn Risk Distribution",
                                     color_discrete_map={"🔴 High": "#f85149",
                                                         "🟡 Medium": "#d29922",
                                                         "🟢 Low": "#3fb950"})
                        fig.update_layout(**PL, height=320)
                        st.plotly_chart(fig, use_container_width=True)
                    fig2 = px.histogram(df_ch, x="SCORE", color="CHURN_RISK", nbins=10,
                                        title="Score Distribution",
                                        color_discrete_map={"🔴 High": "#f85149",
                                                            "🟡 Medium": "#d29922",
                                                            "🟢 Low": "#3fb950"})
                    fig2.update_layout(**PL, height=260)
                    st.plotly_chart(fig2, use_container_width=True)
                    rev_risk = float(df_ch[df_ch["CHURN_RISK"] == "🔴 High"]["LTV"].sum())
                    tot_ltv = float(df_ch["LTV"].sum()) or 1
                    r1, r2, r3 = st.columns(3)
                    r1.metric("High Risk", f"{(df_ch['CHURN_RISK'] == '🔴 High').sum():,}")
                    r2.metric("LTV at Risk", fmt(rev_risk, "$"))
                    r3.metric("% of Total LTV", f"{rev_risk/tot_ltv*100:.1f}%")
                    high_df = df_ch[df_ch["CHURN_RISK"] == "🔴 High"][
                        ["FULL_NAME", "LOYALTY_TIER", "RECENCY_DAYS", "LTV", "ORDERS", "SCORE"]
                    ].sort_values("LTV", ascending=False).head(15)
                    st.dataframe(high_df, use_container_width=True, hide_index=True)
                    if has_ai:
                        ai_story(f"{len(high_df)} high-risk customers, ${rev_risk:,.0f} LTV at risk. "
                                 f"3 retention tactics for sporting goods retail?")
                except Exception as e:
                    st.error(f"Churn error: {e}")

        # ── Market Basket ─────────────────────────────────────────────
        elif ml_section == "🛒 Market Basket":
            st.markdown("#### Market Basket Analysis — Product Co-occurrence")
            if st.button("Run Market Basket Analysis", type="primary", key="btn_mba"):
                with st.spinner("Querying Snowflake …"):
                    try:
                        mba_df = run_sql("""
                            WITH PAIRS AS (
                                SELECT A.PRD_SK AS PROD_A, B.PRD_SK AS PROD_B,
                                       COUNT(DISTINCT A.ORDER_ID) AS CO_OCC
                                FROM WAREHOUSE.FACT_SALES A
                                JOIN WAREHOUSE.FACT_SALES B ON A.ORDER_ID=B.ORDER_ID AND A.PRD_SK<B.PRD_SK
                                WHERE A.ORDER_STATUS NOT IN ('CANCELLED','RETURNED')
                                GROUP BY 1,2
                            ),
                            TOTALS AS (SELECT COUNT(DISTINCT ORDER_ID) AS TOTAL FROM WAREHOUSE.FACT_SALES
                                       WHERE ORDER_STATUS NOT IN ('CANCELLED','RETURNED'))
                            SELECT PA.PRD_NAME AS PRODUCT_A, PB.PRD_NAME AS PRODUCT_B,
                                   P.CO_OCC AS CO_OCCURRENCES,
                                   ROUND(P.CO_OCC*100.0/T.TOTAL,2) AS SUPPORT_PCT
                            FROM PAIRS P
                            JOIN WAREHOUSE.DIM_PRODUCT PA ON P.PROD_A=PA.PRD_SK
                            JOIN WAREHOUSE.DIM_PRODUCT PB ON P.PROD_B=PB.PRD_SK
                            CROSS JOIN TOTALS T
                            ORDER BY CO_OCCURRENCES DESC LIMIT 30
                        """)
                        if not mba_df.empty:
                            fig = px.scatter(mba_df.head(20), x="PRODUCT_A", y="PRODUCT_B",
                                             size="CO_OCCURRENCES", color="SUPPORT_PCT",
                                             title="Product Co-occurrence",
                                             color_continuous_scale=["#161b22", "#3fb950"])
                            fig.update_layout(**PL, height=400)
                            st.plotly_chart(fig, use_container_width=True)
                            st.dataframe(mba_df, use_container_width=True, hide_index=True)
                        else:
                            st.info("No product pairs found.")
                    except Exception as e:
                        st.error(f"Market basket error: {e}")

        # ── Anomaly Detection ─────────────────────────────────────────
        elif ml_section == "📊 Anomaly Detection":
            st.markdown("#### Anomaly Detection — Daily Revenue Outliers")
            if sales.empty or len(sales) < 14:
                st.info("Need at least 14 days of data.")
            else:
                with st.expander("📚 Learn: What is Anomaly Detection?", expanded=False):
                    st.markdown("""
**Isolation Forest** detects outliers by randomly partitioning data. Points that are easy to isolate (few splits needed) are anomalies.

**contamination=0.05** means we expect ~5% of days to be anomalous.

**Why anomalies happen in retail**:
- Flash sales or promotions → revenue spike
- System outage or payment failure → revenue dip
- Major sporting event → unexpected demand
- Data quality issues → incorrect values

**Action**: Investigate each anomaly — is it a real business event or a data problem?
                    """)
                _explain("anomaly")
                try:
                    from sklearn.ensemble import IsolationForest
                    ad = sales.copy()
                    ad["FULL_DATE"] = pd.to_datetime(ad["FULL_DATE"], errors="coerce")
                    ad["REVENUE"] = pd.to_numeric(ad["REVENUE"], errors="coerce").fillna(0.0)
                    daily_rev = (ad.groupby("FULL_DATE")["REVENUE"].sum()
                                 .reset_index().sort_values("FULL_DATE")
                                 .dropna(subset=["FULL_DATE"]).reset_index(drop=True))
                    daily_rev["REVENUE"] = daily_rev["REVENUE"].astype(float)
                    iso = IsolationForest(contamination=0.05, random_state=42)
                    daily_rev["ANOMALY"] = iso.fit_predict(daily_rev[["REVENUE"]])
                    daily_rev["IS_ANOMALY"] = daily_rev["ANOMALY"] == -1
                    normal = daily_rev[~daily_rev["IS_ANOMALY"]]
                    outlier = daily_rev[daily_rev["IS_ANOMALY"]]
                    fig = go.Figure()
                    fig.add_scatter(x=normal["FULL_DATE"], y=normal["REVENUE"],
                                    mode="lines+markers", name="Normal",
                                    line=dict(color=CLR[0]), marker=dict(size=4))
                    fig.add_scatter(x=outlier["FULL_DATE"], y=outlier["REVENUE"],
                                    mode="markers", name="Anomaly",
                                    marker=dict(color="#f85149", size=12, symbol="x"))
                    fig.update_layout(**PL, title="Revenue Anomaly Detection (Isolation Forest)", height=380)
                    st.plotly_chart(fig, use_container_width=True)
                    st.metric("Anomalies detected", f"{int(daily_rev['IS_ANOMALY'].sum())} days")
                    if not outlier.empty:
                        disp = outlier[["FULL_DATE", "REVENUE"]].copy()
                        disp["REVENUE"] = disp["REVENUE"].map(lambda x: f"${x:,.0f}")
                        st.dataframe(disp, use_container_width=True, hide_index=True)
                    if has_ai and not outlier.empty:
                        worst = outlier.sort_values("REVENUE").iloc[0]
                        ai_story(f"Revenue anomaly {worst['FULL_DATE']:%Y-%m-%d} "
                                 f"(${float(worst['REVENUE']):,.0f}). Causes and investigation steps?")
                except Exception as e:
                    st.error(f"Anomaly detection error: {e}")
