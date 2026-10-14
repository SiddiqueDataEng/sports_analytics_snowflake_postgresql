"""
Analytics Dashboard — full analytics + AI + SQL editor + ML
Reads from Snowflake SPORTZONE_DEV (WAREHOUSE.* and MART.*)
"""
import sys, warnings
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
from conn import sf_query, neon_query, ask_openai, get_openai_key, default_snowflake_params

BG="#0d1117"; BG2="#161b22"; FONT="#f0f6fc"; GRID="#30363d"
PL = dict(paper_bgcolor=BG, plot_bgcolor=BG2, font=dict(color=FONT),
          margin=dict(l=8,r=8,t=40,b=8))
CLR=["#58a6ff","#3fb950","#d29922","#f85149","#bc8cff","#39d0d8","#f78166","#ffa657"]

def fmt(v, p="", s="", d=0):
    if v is None: return "—"
    try:
        v = float(v)
    except (TypeError, ValueError):
        return "—"
    if v != v: return "—"  # NaN check
    if abs(v) >= 1e6: return f"{p}{v/1e6:.1f}M{s}"
    if abs(v) >= 1e3: return f"{p}{v/1e3:.1f}K{s}"
    return f"{p}{v:,.{d}f}{s}"

# ── Data loaders ──────────────────────────────────────────────────────────────
def _cast_numerics(df: pd.DataFrame) -> pd.DataFrame:
    """
    Force-cast Snowflake Decimal / object columns to proper numeric dtypes.
    Uses 'coerce' so non-numeric strings become NaN instead of crashing.
    """
    if df.empty:
        return df
    import decimal
    for col in df.columns:
        # If any value in column is a Decimal, force the whole column to float
        sample = df[col].dropna()
        if len(sample) > 0 and isinstance(sample.iloc[0], decimal.Decimal):
            df[col] = pd.to_numeric(df[col], errors="coerce")
        elif df[col].dtype == object:
            converted = pd.to_numeric(df[col], errors="coerce")
            # Only replace if >50% of non-null values converted successfully
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
            ROUND(AVG(NET_REVENUE),2)                                   AS AVG_ORDER_VALUE,
            SUM(QTY_ORDERED)                                            AS UNITS_SOLD,
            COUNT(DISTINCT CUST_SK)                                     AS UNIQUE_CUSTOMERS,
            ROUND(SUM(GROSS_PROFIT)/NULLIF(SUM(NET_REVENUE),0)*100,1)   AS MARGIN_PCT,
            ROUND(SUM(CASE WHEN ORDER_STATUS='RETURNED' THEN 1 ELSE 0 END)*100.0/NULLIF(COUNT(*),0),1) AS RETURN_RATE
        FROM WAREHOUSE.FACT_SALES
        WHERE ORDER_STATUS NOT IN ('CANCELLED')
    """)
    return _cast_numerics(df)

@st.cache_data(ttl=120, show_spinner=False)
def load_daily_sales(days=180, _dummy=None):
    df = sf_query(f"""
        SELECT D.FULL_DATE, D.MONTH_NAME, D.YEAR, D.QUARTER_NAME, F.CHANNEL,
               SUM(F.NET_REVENUE)   AS REVENUE,
               SUM(F.GROSS_PROFIT)  AS PROFIT,
               COUNT(DISTINCT F.ORDER_ID) AS ORDERS,
               SUM(F.QTY_ORDERED)   AS UNITS
        FROM WAREHOUSE.FACT_SALES F
        JOIN WAREHOUSE.DIM_DATE   D ON F.ORDER_DATE_KEY = D.DATE_KEY
        WHERE F.ORDER_STATUS NOT IN ('CANCELLED','RETURNED')
          AND D.FULL_DATE >= DATEADD(DAY,-{days},CURRENT_DATE())
        GROUP BY 1,2,3,4,5 ORDER BY 1
    """)
    return _cast_numerics(df)

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
        JOIN WAREHOUSE.DIM_PRODUCT  P ON F.PRD_SK = P.PRD_SK AND P.IS_CURRENT
        WHERE F.ORDER_STATUS NOT IN ('CANCELLED','RETURNED')
        GROUP BY 1,2,3,4 ORDER BY REVENUE DESC
    """)
    return _cast_numerics(df)

@st.cache_data(ttl=120, show_spinner=False)
def load_customers(_dummy=None):
    df = sf_query("""
        SELECT C.CUST_ID, C.FULL_NAME, C.LOYALTY_TIER, C.COUNTRY,
               C.AGE_BAND, C.GENDER, C.IS_ACTIVE,
               COUNT(DISTINCT F.ORDER_ID)       AS ORDERS,
               SUM(F.NET_REVENUE)               AS LTV,
               ROUND(AVG(F.NET_REVENUE),2)       AS AOV,
               MAX(D.FULL_DATE)                 AS LAST_ORDER,
               DATEDIFF(DAY,MAX(D.FULL_DATE),CURRENT_DATE()) AS RECENCY_DAYS
        FROM WAREHOUSE.DIM_CUSTOMER C
        LEFT JOIN WAREHOUSE.FACT_SALES F ON C.CUST_SK=F.CUST_SK
            AND F.ORDER_STATUS NOT IN ('CANCELLED','RETURNED')
        LEFT JOIN WAREHOUSE.DIM_DATE D ON F.ORDER_DATE_KEY=D.DATE_KEY
        WHERE C.IS_CURRENT AND C.CUST_SK<>-1
        GROUP BY 1,2,3,4,5,6,7 HAVING COUNT(DISTINCT F.ORDER_ID)>0
        ORDER BY LTV DESC NULLS LAST
    """)
    return _cast_numerics(df)

@st.cache_data(ttl=120, show_spinner=False)
def load_inventory(_dummy=None):
    df = sf_query("""
        SELECT P.PRD_NAME, P.PRD_CATEGORY, P.BRAND,
               S.STORE_NAME, S.REGION, S.STORE_TYPE,
               FI.QTY_ON_HAND, FI.QTY_AVAILABLE, FI.REORDER_POINT,
               FI.INV_STATUS, FI.NEEDS_REORDER, FI.STOCK_VALUE,
               D.FULL_DATE AS SNAPSHOT_DATE
        FROM WAREHOUSE.FACT_INVENTORY FI
        JOIN WAREHOUSE.DIM_PRODUCT P ON FI.PRD_SK=P.PRD_SK AND P.IS_CURRENT
        JOIN WAREHOUSE.DIM_STORE   S ON FI.STORE_SK=S.STORE_SK AND S.STORE_SK<>-1
        JOIN WAREHOUSE.DIM_DATE    D ON FI.SNAPSHOT_DATE_KEY=D.DATE_KEY
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
"""

# ── Prewritten questions ──────────────────────────────────────────────────────
PREWRITTEN_QS = {
    "Simple": {
        "Total revenue and profit": "SELECT SUM(NET_REVENUE) AS TOTAL_REVENUE, SUM(GROSS_PROFIT) AS TOTAL_PROFIT, ROUND(SUM(GROSS_PROFIT)/NULLIF(SUM(NET_REVENUE),0)*100,2) AS MARGIN_PCT FROM WAREHOUSE.FACT_SALES WHERE ORDER_STATUS NOT IN ('CANCELLED','RETURNED')",
        "Revenue by channel": "SELECT CHANNEL, COUNT(DISTINCT ORDER_ID) AS ORDERS, SUM(NET_REVENUE) AS REVENUE FROM WAREHOUSE.FACT_SALES WHERE ORDER_STATUS NOT IN ('CANCELLED','RETURNED') GROUP BY 1 ORDER BY REVENUE DESC",
        "Top 10 customers by LTV": "SELECT FULL_NAME, LOYALTY_TIER, TOTAL_ORDERS, LIFETIME_VALUE, RECENCY_SEGMENT FROM MART.CUSTOMER_360 ORDER BY LIFETIME_VALUE DESC NULLS LAST LIMIT 10",
        "Products out of stock": "SELECT PRD_NAME, BRAND, STORE_NAME, REGION, QTY_ON_HAND, REORDER_POINT FROM MART.INVENTORY_STATUS WHERE INV_STATUS='OUT_OF_STOCK' ORDER BY QTY_ON_HAND",
        "Monthly revenue 2024": "SELECT MONTH_NAME, YEAR, SUM(NET_REVENUE) AS REVENUE, SUM(ORDER_COUNT) AS ORDERS FROM MART.DAILY_SALES_SUMMARY WHERE YEAR=2024 GROUP BY 1,2 ORDER BY MIN(FULL_DATE)",
    },
    "Intermediate": {
        "Revenue by category and channel": "SELECT PRD_CATEGORY, CHANNEL, SUM(NET_REVENUE) AS REVENUE, ROUND(SUM(GROSS_PROFIT)/NULLIF(SUM(NET_REVENUE),0)*100,1) AS MARGIN FROM MART.DAILY_SALES_SUMMARY GROUP BY 1,2 ORDER BY 1,REVENUE DESC",
        "Customer retention by cohort (month)": """
SELECT DATE_TRUNC('MONTH', C.SIGNUP_DATE) AS COHORT_MONTH,
       COUNT(DISTINCT C.CUST_SK) AS COHORT_SIZE,
       COUNT(DISTINCT CASE WHEN DATEDIFF(DAY,C.SIGNUP_DATE,D.FULL_DATE)<=30 THEN F.CUST_SK END) AS RETAINED_30D
FROM WAREHOUSE.DIM_CUSTOMER C
LEFT JOIN WAREHOUSE.FACT_SALES F ON C.CUST_SK=F.CUST_SK AND F.ORDER_STATUS='COMPLETED'
LEFT JOIN WAREHOUSE.DIM_DATE   D ON F.ORDER_DATE_KEY=D.DATE_KEY
WHERE C.IS_CURRENT AND C.CUST_SK<>-1
GROUP BY 1 ORDER BY 1""",
        "Promotion ROI": """
SELECT PR.PROMO_CODE, PR.PROMO_TYPE,
       COUNT(DISTINCT F.ORDER_ID) AS ORDERS,
       SUM(F.DISCOUNT_AMT) AS COST,
       SUM(F.NET_REVENUE)  AS REVENUE,
       ROUND((SUM(F.NET_REVENUE)-SUM(F.DISCOUNT_AMT))/NULLIF(SUM(F.DISCOUNT_AMT),0)*100,1) AS ROI_PCT
FROM WAREHOUSE.FACT_SALES F
JOIN WAREHOUSE.DIM_PROMOTION PR ON F.PROMO_SK=PR.PROMO_SK AND PR.PROMO_SK<>-1
WHERE F.ORDER_STATUS NOT IN ('CANCELLED','RETURNED')
GROUP BY 1,2 ORDER BY ROI_PCT DESC""",
        "Week-over-week revenue growth": """
SELECT YEAR, WEEK_OF_YEAR,
       SUM(NET_REVENUE) AS REVENUE,
       LAG(SUM(NET_REVENUE)) OVER (ORDER BY YEAR,WEEK_OF_YEAR) AS PREV_WEEK,
       ROUND((SUM(NET_REVENUE)-LAG(SUM(NET_REVENUE)) OVER (ORDER BY YEAR,WEEK_OF_YEAR))
             /NULLIF(LAG(SUM(NET_REVENUE)) OVER (ORDER BY YEAR,WEEK_OF_YEAR),0)*100,1) AS WOW_PCT
FROM WAREHOUSE.FACT_SALES F
JOIN WAREHOUSE.DIM_DATE D ON F.ORDER_DATE_KEY=D.DATE_KEY
WHERE F.ORDER_STATUS NOT IN ('CANCELLED','RETURNED')
GROUP BY 1,2 ORDER BY 1,2""",
        "Store inventory coverage (days)": """
SELECT S.STORE_NAME, P.PRD_CATEGORY,
       SUM(FI.QTY_AVAILABLE) AS AVAILABLE,
       ROUND(SUM(FI.QTY_AVAILABLE)/NULLIF(AVG(DAILY.DAILY_UNITS),0),1) AS COVER_DAYS
FROM WAREHOUSE.FACT_INVENTORY FI
JOIN WAREHOUSE.DIM_PRODUCT P ON FI.PRD_SK=P.PRD_SK AND P.IS_CURRENT
JOIN WAREHOUSE.DIM_STORE   S ON FI.STORE_SK=S.STORE_SK
LEFT JOIN (
    SELECT PRD_SK, STORE_SK, AVG(QTY_ORDERED) AS DAILY_UNITS
    FROM WAREHOUSE.FACT_SALES
    WHERE ORDER_STATUS NOT IN ('CANCELLED','RETURNED')
    GROUP BY 1,2
) DAILY ON FI.PRD_SK=DAILY.PRD_SK AND FI.STORE_SK=DAILY.STORE_SK
GROUP BY 1,2 ORDER BY COVER_DAYS ASC NULLS LAST""",
    },
    "Advanced": {
        "RFM Segmentation": """
WITH RFM AS (
    SELECT C.CUST_ID, C.FULL_NAME,
           DATEDIFF(DAY,MAX(D.FULL_DATE),CURRENT_DATE()) AS RECENCY,
           COUNT(DISTINCT F.ORDER_ID)                     AS FREQUENCY,
           SUM(F.NET_REVENUE)                             AS MONETARY
    FROM WAREHOUSE.DIM_CUSTOMER C
    JOIN WAREHOUSE.FACT_SALES   F ON C.CUST_SK=F.CUST_SK AND F.ORDER_STATUS='COMPLETED'
    JOIN WAREHOUSE.DIM_DATE     D ON F.ORDER_DATE_KEY=D.DATE_KEY
    WHERE C.IS_CURRENT GROUP BY 1,2
), SCORED AS (
    SELECT *, NTILE(5) OVER (ORDER BY RECENCY DESC)  AS R_SCORE,
              NTILE(5) OVER (ORDER BY FREQUENCY)      AS F_SCORE,
              NTILE(5) OVER (ORDER BY MONETARY)       AS M_SCORE
    FROM RFM
)
SELECT *,
       CASE WHEN R_SCORE>=4 AND F_SCORE>=4 THEN 'Champions'
            WHEN R_SCORE>=3 AND F_SCORE>=3 THEN 'Loyal'
            WHEN R_SCORE>=4 AND F_SCORE<=2 THEN 'New Customers'
            WHEN R_SCORE<=2 AND F_SCORE>=3 THEN 'At Risk'
            WHEN R_SCORE<=2 AND F_SCORE<=2 THEN 'Lost'
            ELSE 'Potential' END AS SEGMENT
FROM SCORED ORDER BY MONETARY DESC LIMIT 50""",
        "SCD2 Customer tier history": """
SELECT CUST_ID, FULL_NAME, LOYALTY_TIER, EFF_START_DT, EFF_END_DT,
       DATEDIFF(DAY, EFF_START_DT, COALESCE(EFF_END_DT, CURRENT_DATE())) AS DAYS_IN_TIER
FROM WAREHOUSE.DIM_CUSTOMER
ORDER BY CUST_ID, EFF_START_DT""",
        "Revenue contribution by price tier (Pareto)": """
SELECT PRICE_TIER,
       SUM(F.NET_REVENUE) AS REVENUE,
       SUM(SUM(F.NET_REVENUE)) OVER (ORDER BY SUM(F.NET_REVENUE) DESC
           ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS CUMULATIVE,
       ROUND(SUM(SUM(F.NET_REVENUE)) OVER (ORDER BY SUM(F.NET_REVENUE) DESC
           ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
           / SUM(SUM(F.NET_REVENUE)) OVER () * 100, 1) AS CUMULATIVE_PCT
FROM WAREHOUSE.FACT_SALES F
JOIN WAREHOUSE.DIM_PRODUCT P ON F.PRD_SK=P.PRD_SK AND P.IS_CURRENT
WHERE F.ORDER_STATUS NOT IN ('CANCELLED','RETURNED')
GROUP BY 1 ORDER BY REVENUE DESC""",
        "Moving average revenue (7/30 day)": """
SELECT FULL_DATE,
       SUM(NET_REVENUE) AS DAILY_REVENUE,
       AVG(SUM(NET_REVENUE)) OVER (ORDER BY FULL_DATE ROWS 6  PRECEDING) AS MA7,
       AVG(SUM(NET_REVENUE)) OVER (ORDER BY FULL_DATE ROWS 29 PRECEDING) AS MA30
FROM WAREHOUSE.FACT_SALES F
JOIN WAREHOUSE.DIM_DATE   D ON F.ORDER_DATE_KEY=D.DATE_KEY
WHERE F.ORDER_STATUS NOT IN ('CANCELLED','RETURNED')
GROUP BY 1 ORDER BY 1""",
    },
    "Extreme": {
        "Customer Lifetime Value prediction (linear)": """
WITH MONTHLY AS (
    SELECT CUST_SK,
           DATE_TRUNC('MONTH',D.FULL_DATE)::DATE AS MONTH,
           SUM(NET_REVENUE) AS MONTHLY_REV
    FROM WAREHOUSE.FACT_SALES F
    JOIN WAREHOUSE.DIM_DATE D ON F.ORDER_DATE_KEY=D.DATE_KEY
    WHERE ORDER_STATUS NOT IN ('CANCELLED','RETURNED')
    GROUP BY 1,2
), REGR AS (
    SELECT CUST_SK,
           REGR_SLOPE(MONTHLY_REV, DATEDIFF(MONTH,'2024-01-01',MONTH))   AS SLOPE,
           REGR_INTERCEPT(MONTHLY_REV, DATEDIFF(MONTH,'2024-01-01',MONTH)) AS INTERCEPT,
           COUNT(*) AS MONTHS_ACTIVE,
           SUM(MONTHLY_REV) AS ACTUAL_LTV
    FROM MONTHLY GROUP BY 1
)
SELECT C.FULL_NAME, C.LOYALTY_TIER, R.MONTHS_ACTIVE, R.ACTUAL_LTV,
       ROUND(R.SLOPE * 12 + R.ACTUAL_LTV, 2) AS PREDICTED_12M_LTV,
       ROUND(R.SLOPE, 2) AS MONTHLY_TREND
FROM REGR R
JOIN WAREHOUSE.DIM_CUSTOMER C ON R.CUST_SK=C.CUST_SK AND C.IS_CURRENT
ORDER BY PREDICTED_12M_LTV DESC NULLS LAST LIMIT 20""",
        "Market basket analysis (product pairs)": """
WITH PAIRS AS (
    SELECT A.PRD_SK AS PROD_A, B.PRD_SK AS PROD_B,
           COUNT(DISTINCT A.ORDER_ID) AS CO_OCCURRENCES
    FROM WAREHOUSE.FACT_SALES A
    JOIN WAREHOUSE.FACT_SALES B ON A.ORDER_ID=B.ORDER_ID AND A.PRD_SK<B.PRD_SK
    WHERE A.ORDER_STATUS NOT IN ('CANCELLED','RETURNED')
    GROUP BY 1,2
)
SELECT PA.PRD_NAME AS PRODUCT_A, PB.PRD_NAME AS PRODUCT_B,
       P.CO_OCCURRENCES,
       ROUND(P.CO_OCCURRENCES * 100.0 / CA.TOTAL, 1) AS SUPPORT_PCT
FROM PAIRS P
JOIN WAREHOUSE.DIM_PRODUCT PA ON P.PROD_A=PA.PRD_SK AND PA.IS_CURRENT
JOIN WAREHOUSE.DIM_PRODUCT PB ON P.PROD_B=PB.PRD_SK AND PB.IS_CURRENT
CROSS JOIN (SELECT COUNT(DISTINCT ORDER_ID) AS TOTAL FROM WAREHOUSE.FACT_SALES WHERE ORDER_STATUS NOT IN ('CANCELLED','RETURNED')) CA
ORDER BY CO_OCCURRENCES DESC LIMIT 20""",
    }
}

# ── render ────────────────────────────────────────────────────────────────────
def render():
    st.markdown("## 📊 SportZone Analytics — Complete Dashboard")

    dummy = st.session_state.get("_cache_bust", 0)
    tabs = st.tabs([
        "📈 Overview", "🛍️ Products", "👥 Customers",
        "📦 Inventory", "🎯 Promotions",
        "🔍 SQL Explorer", "🤖 AI Analytics", "🧪 ML / Data Science"
    ])

    # Pre-load data
    with st.spinner("Loading Snowflake data …"):
        try:
            kpi_df  = load_kpis(dummy)
            sales   = load_daily_sales(180, dummy)
            catdf   = load_category(dummy)
            custdf  = load_customers(dummy)
            invdf   = load_inventory(dummy)
            promodf = load_promo(dummy)
        except Exception as e:
            st.error(f"Data load error: {e}")
            return

    has_ai = bool(get_openai_key())

    def ai_story(prompt):
        if has_ai:
            with st.expander("💡 AI Insight", expanded=False):
                with st.spinner("Generating insight …"):
                    st.markdown(ask_openai(prompt, "You are a data analyst. Be concise, 2-3 sentences."))

    # ── TAB 1: Overview ───────────────────────────────
    with tabs[0]:
        st.markdown("### Key Performance Indicators — Snowflake DWH")
        if not kpi_df.empty:
            r = {k: (float(v) if v is not None else 0) for k, v in kpi_df.iloc[0].items()}
            c = st.columns(8)
            c[0].metric("💰 Revenue",     fmt(r.get("TOTAL_REVENUE",0),  "$"))
            c[1].metric("💵 Profit",      fmt(r.get("TOTAL_PROFIT",0),   "$"))
            c[2].metric("📦 Orders",      fmt(r.get("TOTAL_ORDERS",0)))
            c[3].metric("👤 Customers",   fmt(r.get("UNIQUE_CUSTOMERS",0)))
            c[4].metric("👟 Units",       fmt(r.get("UNITS_SOLD",0)))
            c[5].metric("🛒 AOV",         fmt(r.get("AVG_ORDER_VALUE",0),"$",d=0))
            c[6].metric("📊 Margin",      f"{r.get('MARGIN_PCT',0):.1f}%")
            c[7].metric("↩️ Return Rate", f"{r.get('RETURN_RATE',0):.1f}%")

        if not sales.empty:
            st.markdown("---")
            sales2 = sales.copy()
            sales2["FULL_DATE"] = pd.to_datetime(sales2["FULL_DATE"])
            daily = sales2.groupby("FULL_DATE").agg(
                REVENUE=("REVENUE","sum"), ORDERS=("ORDERS","sum")
            ).reset_index()
            daily["MA7"]  = daily["REVENUE"].rolling(7,  min_periods=1).mean()
            daily["MA30"] = daily["REVENUE"].rolling(30, min_periods=1).mean()

            fig = make_subplots(specs=[[{"secondary_y":True}]])
            fig.add_scatter(x=daily["FULL_DATE"], y=daily["REVENUE"],   name="Revenue",
                            line=dict(color=CLR[0],width=1.5), opacity=.6, secondary_y=False)
            fig.add_scatter(x=daily["FULL_DATE"], y=daily["MA7"],       name="7d MA",
                            line=dict(color=CLR[1],width=2),             secondary_y=False)
            fig.add_scatter(x=daily["FULL_DATE"], y=daily["MA30"],      name="30d MA",
                            line=dict(color=CLR[2],width=2,dash="dash"), secondary_y=False)
            fig.add_bar(    x=daily["FULL_DATE"], y=daily["ORDERS"],    name="Orders",
                            marker_color=CLR[4], opacity=.3,             secondary_y=True)
            fig.update_layout(**PL, title="Revenue Trend (Snowflake)", height=360,
                              legend=dict(orientation="h",yanchor="bottom",y=1.02))
            fig.update_yaxes(title_text="Revenue ($)", showgrid=True, gridcolor=GRID, secondary_y=False)
            fig.update_yaxes(title_text="Orders",      showgrid=False,                secondary_y=True)
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

            monthly = sales2.groupby(["YEAR","MONTH_NAME"])["REVENUE"].sum().reset_index()
            fig = px.bar(monthly, x="MONTH_NAME", y="REVENUE", color="YEAR",
                         barmode="group", title="Monthly Revenue by Year",
                         color_discrete_sequence=CLR)
            fig.update_layout(**PL, height=300)
            st.plotly_chart(fig, use_container_width=True)

        ai_story(f"Summarise these KPIs for SportZone retail: revenue={fmt(kpi_df.iloc[0].get('TOTAL_REVENUE',0),'$') if not kpi_df.empty else 'N/A'}, margin={kpi_df.iloc[0].get('MARGIN_PCT',0) if not kpi_df.empty else 0:.1f}%")

    # ── TAB 2: Products ───────────────────────────────
    with tabs[1]:
        st.markdown("### Product Performance")
        if not catdf.empty:
            fig = px.treemap(catdf, path=["PRD_CATEGORY","PRD_SUBCATEGORY","BRAND"],
                             values="REVENUE", color="MARGIN",
                             color_continuous_scale=["#f85149","#ffa657","#3fb950"],
                             title="Revenue Treemap — Category > Subcategory > Brand")
            fig.update_layout(**PL, height=420)
            st.plotly_chart(fig, use_container_width=True)

            p1,p2 = st.columns(2)
            with p1:
                top = catdf.nlargest(10,"REVENUE")[["BRAND","PRD_SUBCATEGORY","REVENUE","MARGIN","UNITS"]].copy()
                top["REVENUE"] = top["REVENUE"].map(lambda x: f"${x:,.0f}")
                top["MARGIN"]  = top["MARGIN"].map(lambda x: f"{x:.1f}%")
                st.markdown("#### 🏆 Top 10 by Revenue")
                st.dataframe(top, use_container_width=True, hide_index=True)
            with p2:
                brand = catdf.groupby("BRAND").agg(REVENUE=("REVENUE","sum"),UNITS=("UNITS","sum"),MARGIN=("MARGIN","mean")).reset_index()
                fig = px.scatter(brand, x="REVENUE", y="MARGIN", size="UNITS",
                                 color="BRAND", text="BRAND", title="Brand — Revenue vs Margin")
                fig.update_layout(**PL, height=340)
                st.plotly_chart(fig, use_container_width=True)

        ai_story(f"Top category is {catdf.iloc[0]['PRD_CATEGORY'] if not catdf.empty else 'N/A'} — give 2 product strategy insights.")

    # ── TAB 3: Customers ──────────────────────────────
    with tabs[2]:
        st.markdown("### Customer Analytics & Segmentation")
        if not custdf.empty:
            custdf["RECENCY_DAYS"] = pd.to_numeric(custdf["RECENCY_DAYS"], errors="coerce").fillna(999)
            custdf["LTV"]          = pd.to_numeric(custdf["LTV"],          errors="coerce").fillna(0)
            custdf["ORDERS"]       = pd.to_numeric(custdf["ORDERS"],       errors="coerce").fillna(0)
            custdf["AOV"]          = pd.to_numeric(custdf["AOV"],          errors="coerce").fillna(0)

            k1,k2,k3,k4 = st.columns(4)
            k1.metric("Total Customers", f"{len(custdf):,}")
            k2.metric("Avg LTV",         fmt(custdf["LTV"].mean(),"$",d=0))
            k3.metric("Avg Orders",      f"{custdf['ORDERS'].mean():.1f}")
            k4.metric("Avg AOV",         fmt(custdf["AOV"].mean(),"$",d=0))

            rc1,rc2 = st.columns(2)
            with rc1:
                tier = custdf.groupby("LOYALTY_TIER").agg(
                    CUSTOMERS=("CUST_ID","count"),AVG_LTV=("LTV","mean")).reset_index()
                fig = px.bar(tier, x="LOYALTY_TIER", y="CUSTOMERS", color="AVG_LTV",
                             text="CUSTOMERS", title="Customers by Loyalty Tier",
                             color_continuous_scale=["#1f6feb","#3fb950"])
                fig.update_layout(**PL, height=320)
                st.plotly_chart(fig, use_container_width=True)
            with rc2:
                fig = px.scatter(custdf.head(300), x="RECENCY_DAYS", y="LTV",
                                 size="ORDERS", color="LOYALTY_TIER", hover_data=["FULL_NAME","AOV"],
                                 title="RFM Scatter — Recency vs LTV",
                                 color_discrete_sequence=CLR)
                fig.update_layout(**PL, height=320)
                st.plotly_chart(fig, use_container_width=True)

            a1,a2 = st.columns(2)
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

        ai_story("Give 2 customer retention strategies based on typical RFM analysis for a retail company.")

    # ── TAB 4: Inventory ──────────────────────────────
    with tabs[3]:
        st.markdown("### Inventory Management")
        if not invdf.empty:
            i1,i2,i3,i4 = st.columns(4)
            i1.metric("SKU-Locations",  f"{len(invdf):,}")
            i2.metric("Out of Stock",   f"{(invdf['INV_STATUS']=='OUT_OF_STOCK').sum():,}", delta_color="inverse")
            i3.metric("Needs Reorder",  f"{invdf['NEEDS_REORDER'].sum():,}")
            i4.metric("Stock Value",    fmt(pd.to_numeric(invdf["STOCK_VALUE"],errors="coerce").sum(),"$"))

            s1,s2 = st.columns(2)
            with s1:
                sc = invdf["INV_STATUS"].value_counts().reset_index()
                sc.columns=["Status","Count"]
                fig = px.pie(sc, names="Status", values="Count", title="Inventory Status Mix",
                             color_discrete_map={"STOCKED":"#3fb950","LIMITED":"#d29922","OUT_OF_STOCK":"#f85149"})
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
                st.dataframe(alerts[["PRD_NAME","BRAND","STORE_NAME","REGION",
                                     "QTY_ON_HAND","REORDER_POINT","INV_STATUS"]].head(20),
                             use_container_width=True, hide_index=True)

    # ── TAB 5: Promotions ─────────────────────────────
    with tabs[4]:
        st.markdown("### Promotion Effectiveness")
        if not promodf.empty:
            fig = px.scatter(promodf, x="DISCOUNT_GIVEN", y="REVENUE",
                             size="ORDERS", color="PROMO_TYPE", text="PROMO_CODE",
                             title="Promotion ROI — Discount Cost vs Revenue Generated",
                             color_discrete_sequence=CLR)
            fig.update_layout(**PL, height=380)
            st.plotly_chart(fig, use_container_width=True)
            st.dataframe(promodf, use_container_width=True, hide_index=True)

    # ── TAB 6: SQL Explorer ───────────────────────────
    with tabs[5]:
        st.markdown("### 🔍 SQL Explorer — Snowflake Ready")

        st_q1, st_q2 = st.columns([2,1])
        with st_q2:
            difficulty = st.selectbox("Question Library", list(PREWRITTEN_QS.keys()), key="q_diff")
            question   = st.selectbox("Pick a question", list(PREWRITTEN_QS[difficulty].keys()), key="q_sel")
            if st.button("Load Question →", key="btn_load_q"):
                st.session_state["sql_editor"] = PREWRITTEN_QS[difficulty][question].strip()

        with st_q1:
            sql = st.text_area(
                "SQL Editor (Snowflake dialect)",
                value=st.session_state.get("sql_editor", "SELECT * FROM MART.DAILY_SALES_SUMMARY LIMIT 10"),
                height=180, key="sql_editor_area"
            )
            st.session_state["sql_editor"] = sql

        run_col, ai_col = st.columns([1,3])
        run_btn = run_col.button("▶️ Run Query", type="primary", key="btn_run_sql")
        ai_fix  = ai_col.button("🤖 AI Fix / Optimise SQL", key="btn_ai_fix") if has_ai else False

        if run_btn and sql:
            with st.spinner("Running on Snowflake …"):
                try:
                    df = sf_query(sql)
                    st.success(f"✅ {len(df):,} rows returned")
                    st.dataframe(df, use_container_width=True, hide_index=True)
                    # Auto-chart if numeric columns exist
                    num_cols = df.select_dtypes(include="number").columns.tolist()
                    str_cols = df.select_dtypes(exclude="number").columns.tolist()
                    if num_cols and str_cols:
                        fig = px.bar(df.head(25), x=str_cols[0], y=num_cols[0],
                                     color_discrete_sequence=CLR, title=f"{num_cols[0]} by {str_cols[0]}")
                        fig.update_layout(**PL, height=320)
                        st.plotly_chart(fig, use_container_width=True)
                    st.session_state["last_result"] = df
                except Exception as e:
                    st.error(f"SQL Error: {e}")

        if ai_fix and sql:
            with st.spinner("AI reviewing SQL …"):
                fixed = ask_openai(
                    f"Review and optimise this Snowflake SQL. Return only the corrected SQL, no explanation:\n{sql}",
                    "You are a Snowflake SQL expert. Return only valid Snowflake SQL."
                )
                st.code(fixed, language="sql")
                if st.button("Use this SQL", key="btn_use_ai_sql"):
                    st.session_state["sql_editor"] = fixed
                    st.rerun()

        st.divider()
        st.markdown("#### 💡 Problem → Solution Reference")
        with st.expander("View all prewritten queries", expanded=False):
            for level, qs in PREWRITTEN_QS.items():
                st.markdown(f"**{level}**")
                for qname, qsql in qs.items():
                    with st.expander(qname, expanded=False):
                        st.code(qsql.strip(), language="sql")

    # ── TAB 7: AI Analytics ───────────────────────────
    with tabs[6]:
        st.markdown("### 🤖 AI-Powered Analytics")

        if not has_ai:
            st.warning("⚠️ Configure your OpenAI API key in **Management → AI Settings** to enable AI features.")
            st.info("AI features include: Natural Language to SQL, automated insights, chart storytelling, anomaly detection, and business recommendations.")
            return

        # NL → SQL
        st.markdown("#### 💬 Natural Language → Snowflake SQL")
        nl_input = st.text_area(
            "Ask a business question in plain English:",
            placeholder="e.g. Show me total revenue by country for GOLD tier customers in 2024",
            height=80, key="nl_q"
        )
        c_nl1, c_nl2 = st.columns([1,3])
        if c_nl1.button("🔮 Generate SQL", type="primary", key="btn_nl_sql") and nl_input:
            with st.spinner("Generating SQL …"):
                system = f"""You are a Snowflake SQL expert. Schema:\n{SF_SCHEMA}
Generate ONLY the SQL query, no explanations, no markdown code blocks. The SQL must run on Snowflake."""
                sql_out = ask_openai(nl_input, system)
                sql_out = sql_out.strip().strip("```sql").strip("```").strip()
                st.session_state["ai_sql"] = sql_out

        if "ai_sql" in st.session_state:
            st.code(st.session_state["ai_sql"], language="sql")
            col_run, col_exp = st.columns(2)
            if col_run.button("▶️ Run Generated SQL", key="btn_run_ai_sql"):
                with st.spinner("Running …"):
                    try:
                        df = sf_query(st.session_state["ai_sql"])
                        st.success(f"✅ {len(df):,} rows")
                        st.dataframe(df, use_container_width=True, hide_index=True)
                        num_cols = df.select_dtypes(include="number").columns.tolist()
                        str_cols = df.select_dtypes(exclude="number").columns.tolist()
                        if num_cols and str_cols:
                            fig = px.bar(df.head(20), x=str_cols[0], y=num_cols[0],
                                         color_discrete_sequence=CLR, title=f"Result: {nl_input[:60]}")
                            fig.update_layout(**PL, height=320)
                            st.plotly_chart(fig, use_container_width=True)
                            st.markdown("**AI Chart Story:**")
                            story = ask_openai(
                                f"In 2-3 sentences, explain what this chart shows: {nl_input}. The top result is {df.iloc[0].to_dict() if not df.empty else 'no data'}",
                                "You are a business analyst. Be concise and insightful."
                            )
                            st.info(story)
                    except Exception as e:
                        st.error(f"Error: {e}")
                        val_msg = ask_openai(
                            f"This Snowflake SQL returned an error: {e}\nSQL: {st.session_state['ai_sql']}\nFix it and return only the corrected SQL.",
                            "You are a Snowflake SQL expert."
                        )
                        st.code(val_msg, language="sql")

        st.divider()
        st.markdown("#### 🧠 AI Business Insights")
        if st.button("Generate Automated Insights", key="btn_insights"):
            kpi_summary = ""
            if not kpi_df.empty:
                r = kpi_df.iloc[0]
                kpi_summary = (f"Revenue=${float(r.get('TOTAL_REVENUE',0)):,.0f}, "
                               f"Profit=${float(r.get('TOTAL_PROFIT',0)):,.0f}, "
                               f"Margin={r.get('MARGIN_PCT',0):.1f}%, "
                               f"Orders={r.get('TOTAL_ORDERS',0)}, "
                               f"ReturnRate={r.get('RETURN_RATE',0):.1f}%")
            top_cat = catdf.iloc[0]["PRD_CATEGORY"] if not catdf.empty else "N/A"
            top_cust_tier = custdf.groupby("LOYALTY_TIER")["LTV"].mean().idxmax() if not custdf.empty else "N/A"
            oos = (invdf["INV_STATUS"]=="OUT_OF_STOCK").sum() if not invdf.empty else 0

            prompt = f"""SportZone retail business data:
KPIs: {kpi_summary}
Top category: {top_cat}
Highest LTV tier: {top_cust_tier}
Out of stock SKUs: {oos}

Provide 5 specific, actionable business insights with priority levels (HIGH/MEDIUM/LOW)."""
            with st.spinner("Analysing …"):
                insights = ask_openai(prompt, "You are a retail analytics consultant. Be specific and data-driven.")
            st.markdown(insights)

        st.divider()
        st.markdown("#### 🎯 Suggested Business Questions")
        if st.button("Get AI Question Suggestions", key="btn_suggest_q"):
            with st.spinner():
                suggestions = ask_openai(
                    "For a sporting goods retailer with sales, inventory, customer, and promotion data in Snowflake, suggest 8 business questions ranging from operational to strategic. Format as a numbered list.",
                    "You are a retail data analyst. Be practical and specific."
                )
            st.markdown(suggestions)

    # ── TAB 8: ML / Data Science ──────────────────────
    with tabs[7]:
        st.markdown("### 🧪 ML & Data Science")

        ml_tab1, ml_tab2, ml_tab3 = st.tabs(["📈 Forecast","🎯 Segmentation","🚨 Churn"])

        with ml_tab1:
            st.markdown("#### Revenue Forecast (Linear Regression)")
            if not sales.empty and len(sales) >= 14:
                horizon = st.slider("Forecast days", 7, 90, 30, key="fc_horizon")
                try:
                    from sklearn.linear_model import LinearRegression
                    sales3 = sales.copy()
                    sales3["FULL_DATE"] = pd.to_datetime(sales3["FULL_DATE"])
                    daily3 = sales3.groupby("FULL_DATE")["REVENUE"].sum().reset_index().sort_values("FULL_DATE")
                    daily3["t"] = (daily3["FULL_DATE"] - daily3["FULL_DATE"].min()).dt.days
                    X = daily3[["t"]].values; y = daily3["REVENUE"].values
                    model = LinearRegression().fit(X, y)
                    last_t = int(daily3["t"].max())
                    ft = np.arange(last_t+1, last_t+horizon+1).reshape(-1,1)
                    fdates = pd.date_range(daily3["FULL_DATE"].max() + pd.Timedelta(days=1), periods=horizon)
                    fcast  = model.predict(ft)
                    fig = go.Figure()
                    fig.add_scatter(x=daily3["FULL_DATE"], y=y,   name="Actual",
                                    line=dict(color=CLR[0],width=2))
                    fig.add_scatter(x=fdates,              y=fcast, name=f"{horizon}d Forecast",
                                    line=dict(color=CLR[2],width=2,dash="dash"),
                                    fill="tonexty", fillcolor="rgba(210,153,34,0.1)")
                    fig.update_layout(**PL, title=f"Revenue Forecast +{horizon} days", height=360)
                    st.plotly_chart(fig, use_container_width=True)
                    slope = float(model.coef_[0])
                    direction = "📈 upward" if slope > 0 else "📉 downward"
                    st.info(f"Trend: {direction} at **${slope:+.0f}/day**")
                    if has_ai:
                        st.markdown(ask_openai(
                            f"Revenue trend is {direction} at ${slope:+.0f}/day over the last 6 months for SportZone. What does this mean for the business in 2-3 sentences?",
                            "You are a retail financial analyst."
                        ))
                except Exception as e:
                    st.warning(f"Forecast error: {e}")

        with ml_tab2:
            st.markdown("#### K-Means Customer Segmentation")
            if not custdf.empty and len(custdf) >= 10:
                try:
                    from sklearn.preprocessing import StandardScaler
                    from sklearn.cluster import KMeans
                    seg = custdf[["RECENCY_DAYS","LTV","ORDERS","AOV"]].fillna(0).copy()
                    seg["RECENCY_DAYS"] = seg["RECENCY_DAYS"].clip(upper=365)
                    seg["LTV"]          = seg["LTV"].clip(upper=seg["LTV"].quantile(.99))
                    X2 = StandardScaler().fit_transform(seg)
                    km = KMeans(n_clusters=4, random_state=42, n_init=10)
                    custdf2 = custdf.copy()
                    custdf2["SEGMENT"] = km.fit_predict(X2)
                    seg_ltv = custdf2.groupby("SEGMENT")["LTV"].mean().sort_values(ascending=False)
                    seg_names = {s:n for s,n in zip(seg_ltv.index,["Champions","Loyal","At Risk","Lost"])}
                    custdf2["SEG_NAME"] = custdf2["SEGMENT"].map(seg_names)
                    summary = custdf2.groupby("SEG_NAME").agg(
                        Customers=("CUST_ID","count"),
                        Avg_LTV=("LTV","mean"),
                        Avg_Orders=("ORDERS","mean"),
                        Avg_Recency=("RECENCY_DAYS","mean")
                    ).reset_index()
                    sc1,sc2 = st.columns([2,1])
                    with sc1:
                        fig = px.bar(summary, x="SEG_NAME", y="Customers",
                                     color="Avg_LTV", text="Customers",
                                     title="Customer Segments (K-Means)",
                                     color_continuous_scale=["#f85149","#3fb950"])
                        fig.update_layout(**PL, height=340)
                        st.plotly_chart(fig, use_container_width=True)
                    with sc2:
                        summary["Avg_LTV"]     = summary["Avg_LTV"].map(lambda x: f"${x:,.0f}")
                        summary["Avg_Orders"]  = summary["Avg_Orders"].map(lambda x: f"{x:.1f}")
                        summary["Avg_Recency"] = summary["Avg_Recency"].map(lambda x: f"{x:.0f}d")
                        st.dataframe(summary, use_container_width=True, hide_index=True)
                except Exception as e:
                    st.warning(f"Segmentation error: {e}")

        with ml_tab3:
            st.markdown("#### Churn Risk Scoring")
            if not custdf.empty:
                df_churn = custdf.copy()
                def risk(row):
                    s = 0
                    if row["RECENCY_DAYS"] > 180: s += 3
                    elif row["RECENCY_DAYS"] > 90: s += 2
                    elif row["RECENCY_DAYS"] > 30: s += 1
                    if row["ORDERS"] < 2: s += 2
                    if row["LOYALTY_TIER"] == "BRONZE": s += 1
                    if s >= 5: return "🔴 High"
                    if s >= 3: return "🟡 Medium"
                    return "🟢 Low"
                df_churn["CHURN_RISK"] = df_churn.apply(risk, axis=1)
                cnt = df_churn["CHURN_RISK"].value_counts().reset_index()
                cnt.columns=["Risk","Customers"]
                ch1,ch2 = st.columns(2)
                with ch1:
                    fig = px.pie(cnt, names="Risk", values="Customers",
                                 title="Churn Risk Distribution",
                                 color_discrete_map={"🔴 High":"#f85149","🟡 Medium":"#d29922","🟢 Low":"#3fb950"})
                    fig.update_layout(**PL, height=320)
                    st.plotly_chart(fig, use_container_width=True)
                with ch2:
                    high = df_churn[df_churn["CHURN_RISK"]=="🔴 High"][
                        ["FULL_NAME","LOYALTY_TIER","RECENCY_DAYS","LTV","ORDERS"]
                    ].head(10)
                    st.markdown("**High-Risk Customers:**")
                    st.dataframe(high, use_container_width=True, hide_index=True)
                if has_ai:
                    high_cnt = (df_churn["CHURN_RISK"]=="🔴 High").sum()
                    ai_story(f"{high_cnt} customers are at high churn risk out of {len(df_churn)}. Suggest 3 retention actions for a sporting goods retailer.")
