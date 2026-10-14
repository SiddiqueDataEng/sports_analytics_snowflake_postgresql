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
        st.markdown("### 🎯 Promotion Effectiveness & Campaign Analytics")

        if promodf.empty:
            st.info("No promotion data available.")
        else:
            # ── KPIs ──────────────────────────────────────────
            pk1, pk2, pk3, pk4, pk5 = st.columns(5)
            total_promo_orders   = int(promodf["ORDERS"].sum())
            total_discount       = float(promodf["DISCOUNT_GIVEN"].sum())
            total_promo_revenue  = float(promodf["REVENUE"].sum())
            total_promo_profit   = float(promodf["PROFIT"].sum())
            avg_disc_rate        = float(promodf["DISC_RATE"].mean())
            pk1.metric("🎟️ Promo Orders",    f"{total_promo_orders:,}")
            pk2.metric("💸 Discount Given",  fmt(total_discount, "$"))
            pk3.metric("💰 Promo Revenue",   fmt(total_promo_revenue, "$"))
            pk4.metric("📊 Promo Profit",    fmt(total_promo_profit, "$"))
            pk5.metric("🏷️ Avg Disc Rate",   f"{avg_disc_rate:.1f}%")

            st.divider()
            pr1, pr2 = st.columns(2)

            with pr1:
                # ROI scatter
                fig = px.scatter(promodf, x="DISCOUNT_GIVEN", y="REVENUE",
                                 size="ORDERS", color="PROMO_TYPE", text="PROMO_CODE",
                                 title="Promotion ROI — Discount Cost vs Revenue",
                                 color_discrete_sequence=CLR,
                                 labels={"DISCOUNT_GIVEN":"Discount ($)","REVENUE":"Revenue ($)"})
                fig.update_traces(textposition="top center")
                fig.update_layout(**PL, height=380)
                st.plotly_chart(fig, use_container_width=True)

            with pr2:
                # Waterfall: revenue vs discount
                fig = go.Figure(go.Waterfall(
                    name="Promo P&L",
                    orientation="v",
                    measure=["relative"]*len(promodf) + ["total"],
                    x=list(promodf["PROMO_CODE"]) + ["NET"],
                    y=list(promodf["PROFIT"].astype(float)) + [0],
                    connector={"line":{"color":"#30363d"}},
                    increasing={"marker":{"color":"#3fb950"}},
                    decreasing={"marker":{"color":"#f85149"}},
                    totals={"marker":{"color":"#58a6ff"}},
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
                # Efficiency: revenue per $1 discount
                peff = promodf.copy()
                peff["EFFICIENCY"] = peff["REVENUE"] / peff["DISCOUNT_GIVEN"].replace(0, np.nan)
                peff = peff.sort_values("EFFICIENCY", ascending=False)
                fig = px.bar(peff, x="PROMO_CODE", y="EFFICIENCY",
                             color="EFFICIENCY", title="Revenue per $1 Discount (Efficiency)",
                             color_continuous_scale=["#f85149","#3fb950"],
                             text=peff["EFFICIENCY"].map(lambda x: f"${x:.1f}"))
                fig.update_layout(**PL, height=320)
                fig.update_traces(textposition="outside")
                st.plotly_chart(fig, use_container_width=True)

            # Discount rate vs profit margin scatter
            peff2 = promodf.copy()
            peff2["PROFIT_MARGIN"] = peff2["PROFIT"] / peff2["REVENUE"].replace(0, np.nan) * 100
            fig = px.scatter(peff2, x="DISC_RATE", y="PROFIT_MARGIN",
                             size="ORDERS", color="PROMO_TYPE", text="PROMO_CODE",
                             title="Discount Rate vs Profit Margin — Sweet Spot Analysis",
                             labels={"DISC_RATE":"Discount Rate (%)","PROFIT_MARGIN":"Profit Margin (%)"},
                             color_discrete_sequence=CLR)
            fig.add_vline(x=avg_disc_rate, line_dash="dash", line_color="#d29922",
                          annotation_text=f"Avg disc {avg_disc_rate:.1f}%")
            fig.update_layout(**PL, height=360)
            st.plotly_chart(fig, use_container_width=True)

            # Detail table
            st.markdown("#### 📋 Promotion Detail")
            disp = promodf.copy()
            disp["DISCOUNT_GIVEN"] = disp["DISCOUNT_GIVEN"].map(lambda x: f"${x:,.0f}")
            disp["REVENUE"]        = disp["REVENUE"].map(lambda x: f"${x:,.0f}")
            disp["PROFIT"]         = disp["PROFIT"].map(lambda x: f"${x:,.0f}")
            disp["DISC_RATE"]      = disp["DISC_RATE"].map(lambda x: f"{x:.1f}%")
            st.dataframe(disp, use_container_width=True, hide_index=True)

        ai_story("For a sporting goods retailer, what makes a promotion effective? Give 2 insights based on discount rate vs profit margin tradeoffs.")

    # ── TAB 6: SQL Explorer ───────────────────────────
    with tabs[5]:
        st.markdown("### 🔍 SQL Explorer — Snowflake Ready")

        # ── Syntax highlight helper ────────────────────────────────────────────
        def highlight_sql(sql_text: str) -> str:
            """Return HTML with Snowflake SQL syntax highlighted."""
            import re, html
            sql_text = html.escape(sql_text)
            # Keywords
            kw = (r'\b(SELECT|FROM|WHERE|JOIN|LEFT|RIGHT|INNER|OUTER|FULL|CROSS|ON|AS|'
                  r'GROUP\s+BY|ORDER\s+BY|HAVING|LIMIT|WITH|UNION|INTERSECT|EXCEPT|'
                  r'INSERT|INTO|UPDATE|SET|DELETE|CREATE|ALTER|DROP|TABLE|VIEW|SCHEMA|'
                  r'DATABASE|IF|NOT|EXISTS|REPLACE|OR|AND|IN|BETWEEN|LIKE|IS|NULL|'
                  r'CASE|WHEN|THEN|ELSE|END|DISTINCT|ALL|TOP|QUALIFY|OVER|PARTITION\s+BY|'
                  r'ROWS|PRECEDING|FOLLOWING|UNBOUNDED|CURRENT\s+ROW|NULLS\s+LAST|'
                  r'NULLS\s+FIRST|ASC|DESC|TRUE|FALSE|BOOLEAN|INTEGER|VARCHAR|NUMBER|'
                  r'DATE|TIMESTAMP|FLOAT|NUMERIC|SMALLINT|BIGINT|TEXT|VARIANT|ARRAY|OBJECT|'
                  r'MERGE|USING|MATCHED|NOT\s+MATCHED|AUTOINCREMENT|PRIMARY\s+KEY|'
                  r'REFERENCES|DEFAULT|CLUSTER\s+BY|COPY\s+INTO|STAGE|FILE\s+FORMAT|'
                  r'GENERATOR|ROWCOUNT|SEQ4|DATEADD|DATEDIFF|TO_CHAR|TO_DATE|CURRENT_DATE|'
                  r'CURRENT_TIMESTAMP|COALESCE|NULLIF|IFF|ZEROIFNULL|NVL|NVL2|'
                  r'REGR_SLOPE|REGR_INTERCEPT|NTILE|LAG|LEAD|RANK|DENSE_RANK|ROW_NUMBER|'
                  r'COUNT|SUM|AVG|MIN|MAX|ROUND|FLOOR|CEILING|ABS|MOD|POWER|SQRT|'
                  r'CONCAT|TRIM|UPPER|LOWER|SUBSTR|REPLACE|SPLIT|INITCAP|LENGTH|'
                  r'LISTAGG|OBJECT_AGG|ARRAY_AGG|PARSE_JSON|GET_PATH|FLATTEN)\b')
            sql_hl = re.sub(kw,
                lambda m: f'<span style="color:#ff7b72;font-weight:700">{m.group()}</span>',
                sql_text, flags=re.IGNORECASE)
            # Functions/schema references
            sql_hl = re.sub(r'\b([A-Z_]+)\s*\(',
                lambda m: f'<span style="color:#d2a8ff">{m.group(1)}</span>(',
                sql_hl, flags=re.IGNORECASE)
            # String literals
            sql_hl = re.sub(r"(&#x27;[^&#]*?&#x27;|&quot;[^&]*?&quot;)",
                lambda m: f'<span style="color:#a5d6ff">{m.group()}</span>', sql_hl)
            # Numbers
            sql_hl = re.sub(r'\b(\d+\.?\d*)\b',
                lambda m: f'<span style="color:#f0883e">{m.group()}</span>', sql_hl)
            # Comments
            sql_hl = re.sub(r'(--.+?)(?=\n|$)',
                lambda m: f'<span style="color:#8b949e;font-style:italic">{m.group()}</span>',
                sql_hl)
            # Schema.Table references
            sql_hl = re.sub(r'\b([A-Z_]+\.[A-Z_]+(?:\.[A-Z_]+)?)\b',
                lambda m: f'<span style="color:#79c0ff">{m.group()}</span>',
                sql_hl, flags=re.IGNORECASE)
            return (f'<pre style="background:#161b22;border:1px solid #30363d;'
                    f'border-radius:8px;padding:16px;font-family:Consolas,monospace;'
                    f'font-size:13px;line-height:1.6;overflow-x:auto;'
                    f'color:#e6edf3;white-space:pre-wrap">{sql_hl}</pre>')

        def format_sql(raw: str) -> str:
            """Use sqlparse to pretty-print SQL."""
            try:
                import sqlparse
                return sqlparse.format(raw, reindent=True, keyword_case="upper",
                                       identifier_case="upper", strip_comments=False,
                                       indent_width=4)
            except Exception:
                return raw

        # ── Editor layout ──────────────────────────────────────────────────────
        left_col, right_col = st.columns([3, 1])

        with right_col:
            st.markdown("**📚 Query Library**")
            difficulty = st.selectbox("Level", list(PREWRITTEN_QS.keys()), key="q_diff")
            question   = st.selectbox("Query", list(PREWRITTEN_QS[difficulty].keys()), key="q_sel")
            if st.button("📥 Load", key="btn_load_q", use_container_width=True):
                st.session_state["sql_raw"] = PREWRITTEN_QS[difficulty][question].strip()
                st.rerun()
            st.divider()
            st.markdown("**⚡ Quick Actions**")
            if st.button("✨ Format SQL", key="btn_fmt", use_container_width=True):
                raw = st.session_state.get("sql_raw","")
                st.session_state["sql_raw"] = format_sql(raw)
                st.rerun()
            if has_ai and st.button("🤖 AI Optimise", key="btn_ai_opt", use_container_width=True):
                raw = st.session_state.get("sql_raw","")
                if raw:
                    with st.spinner("AI optimising …"):
                        fixed = ask_openai(
                            f"Optimise this Snowflake SQL for performance and readability. Return only the SQL:\n{raw}",
                            "You are a Snowflake SQL expert.")
                        st.session_state["sql_raw"] = fixed.strip().strip("```sql").strip("```").strip()
                    st.rerun()
            if st.button("🗑️ Clear", key="btn_clear_sql", use_container_width=True):
                st.session_state["sql_raw"] = ""
                st.rerun()

        with left_col:
            default_sql = st.session_state.get("sql_raw",
                "SELECT * FROM MART.DAILY_SALES_SUMMARY LIMIT 10")
            sql = st.text_area(
                "✏️ Edit SQL (Snowflake dialect):",
                value=default_sql, height=200, key="sql_editor_area",
                help="Full Snowflake SQL — DDL, DML, CTEs, window functions, QUALIFY, COPY INTO, etc."
            )
            st.session_state["sql_raw"] = sql

            # Live syntax highlighted preview
            if sql.strip():
                with st.expander("🎨 Syntax Highlighted Preview", expanded=False):
                    st.markdown(highlight_sql(format_sql(sql)), unsafe_allow_html=True)

        # ── Run buttons ────────────────────────────────────────────────────────
        rc1, rc2, rc3, rc4 = st.columns([1,1,1,2])
        run_btn    = rc1.button("▶️ Run",      type="primary", key="btn_run_sql")
        explain_btn= rc2.button("📖 Explain",  key="btn_explain") if has_ai else False
        plan_btn   = rc3.button("📋 Exec Plan", key="btn_plan")

        if run_btn and sql.strip():
            with st.spinner("Running on Snowflake …"):
                try:
                    result_df = _cast_numerics(sf_query(sql))
                    st.session_state["last_result"] = result_df
                    st.success(f"✅ {len(result_df):,} rows · {len(result_df.columns)} columns")

                    # Tabs: table | chart | stats
                    res_t1, res_t2, res_t3 = st.tabs(["📋 Results","📊 Auto Chart","📈 Stats"])
                    with res_t1:
                        st.dataframe(result_df, use_container_width=True, hide_index=True)
                        # Download
                        csv_data = result_df.to_csv(index=False).encode()
                        st.download_button("⬇️ Download CSV", csv_data,
                                           "query_result.csv", "text/csv", key="dl_csv")
                    with res_t2:
                        num_cols = result_df.select_dtypes(include="number").columns.tolist()
                        str_cols = result_df.select_dtypes(exclude="number").columns.tolist()
                        if num_cols and str_cols:
                            chart_type = st.selectbox("Chart type",
                                ["Bar","Line","Scatter","Pie","Treemap"], key="chart_type_sel")
                            x_col = st.selectbox("X axis", str_cols,  key="x_sel")
                            y_col = st.selectbox("Y axis", num_cols,  key="y_sel")
                            plot_df = result_df.head(50)
                            if chart_type == "Bar":
                                fig = px.bar(plot_df,    x=x_col, y=y_col, color_discrete_sequence=CLR)
                            elif chart_type == "Line":
                                fig = px.line(plot_df,   x=x_col, y=y_col, color_discrete_sequence=CLR)
                            elif chart_type == "Scatter":
                                fig = px.scatter(plot_df,x=x_col, y=y_col, color_discrete_sequence=CLR)
                            elif chart_type == "Pie":
                                fig = px.pie(plot_df, names=x_col, values=y_col,
                                             color_discrete_sequence=CLR)
                            else:
                                fig = px.treemap(plot_df, path=[x_col], values=y_col,
                                                 color_continuous_scale=["#1f6feb","#3fb950"])
                            fig.update_layout(**PL, height=400, title=f"{y_col} by {x_col}")
                            st.plotly_chart(fig, use_container_width=True)
                        else:
                            st.info("No numeric columns to chart.")
                    with res_t3:
                        st.dataframe(result_df.describe(), use_container_width=True)
                except Exception as e:
                    st.error(f"SQL Error: {e}")
                    if has_ai:
                        with st.spinner("AI diagnosing error …"):
                            fix = ask_openai(
                                f"Snowflake SQL error: {e}\nSQL: {sql}\nFix it — return only corrected SQL.",
                                "You are a Snowflake SQL expert.")
                        st.markdown("**AI Suggested Fix:**")
                        st.code(fix.strip().strip("```sql").strip("```"), language="sql")

        if explain_btn and sql.strip():
            with st.spinner("AI explaining SQL …"):
                explanation = ask_openai(
                    f"Explain this Snowflake SQL in plain English, step by step:\n{sql}",
                    "You are a data analyst explaining SQL to a business user.")
            st.info(explanation)

        if plan_btn and sql.strip():
            with st.spinner("Fetching execution plan …"):
                try:
                    plan_df = _cast_numerics(sf_query(f"EXPLAIN {sql}"))
                    st.dataframe(plan_df, use_container_width=True, hide_index=True)
                except Exception as e:
                    st.warning(f"Plan not available: {e}")

        # ── Bulk Load from CSV ─────────────────────────────────────────────────
        st.divider()
        st.markdown("#### 📂 Bulk Load CSV → Snowflake (COPY INTO)")
        with st.expander("Upload a CSV file to load into a Snowflake table", expanded=False):
            ul1, ul2 = st.columns([2,1])
            with ul2:
                target_db  = st.text_input("Database", value="SPORTZONE_DEV", key="ul_db")
                target_sch = st.text_input("Schema",   value="RAW",           key="ul_sch")
                target_tbl = st.text_input("Table",    value="",              key="ul_tbl",
                                           placeholder="e.g. RAW_CUSTOMERS")
                delimiter  = st.selectbox("Delimiter", [",","|","\t","~"],   key="ul_delim")
                skip_header= st.checkbox("Skip header row", value=True,       key="ul_header")
                on_error   = st.selectbox("On error",
                    ["CONTINUE","ABORT_STATEMENT","SKIP_FILE"], key="ul_onerr")
            with ul1:
                uploaded = st.file_uploader("Choose CSV file", type=["csv","txt"],
                                            key="ul_file")
                if uploaded and target_tbl:
                    st.info(f"File: **{uploaded.name}** ({uploaded.size/1024:.1f} KB)")
                    if st.button("🚀 Upload & Load", type="primary", key="btn_ul_load"):
                        import tempfile, os
                        with st.spinner("Uploading to Snowflake stage and running COPY INTO …"):
                            try:
                                import snowflake.connector
                                from pathlib import Path as _P
                                params = st.session_state.get("sf_params", default_snowflake_params())
                                conn2  = snowflake.connector.connect(**params)
                                cur2   = conn2.cursor()
                                cur2.execute(f"USE DATABASE {target_db}")
                                cur2.execute(f"USE WAREHOUSE COMPUTE_WH")

                                # Write temp file
                                tmp = tempfile.NamedTemporaryFile(delete=False,
                                    suffix=".csv", mode="wb")
                                tmp.write(uploaded.getvalue())
                                tmp.close()

                                delim_map = {",":"','","|":"'|'","\t":"'\\t'","~":"'~'"}
                                delim_str = delim_map.get(delimiter, "','")

                                # Create temp file format
                                cur2.execute(f"""
                                    CREATE OR REPLACE TEMPORARY FILE FORMAT tmp_fmt
                                    TYPE=CSV FIELD_DELIMITER={delim_str}
                                    SKIP_HEADER={1 if skip_header else 0}
                                    NULL_IF=('NULL','null','')
                                    EMPTY_FIELD_AS_NULL=TRUE
                                """)
                                # PUT file to table stage
                                put_sql = f"PUT file://{tmp.name.replace(chr(92),'/')} @{target_db}.{target_sch}.%{target_tbl} OVERWRITE=TRUE AUTO_COMPRESS=TRUE"
                                cur2.execute(put_sql)
                                put_res = cur2.fetchall()

                                # COPY INTO
                                copy_sql = f"""
                                    COPY INTO {target_db}.{target_sch}.{target_tbl}
                                    FROM @{target_db}.{target_sch}.%{target_tbl}
                                    PATTERN='.*{_P(tmp.name).stem}.*'
                                    FILE_FORMAT=(FORMAT_NAME='tmp_fmt')
                                    ON_ERROR='{on_error}'
                                """
                                cur2.execute(copy_sql)
                                copy_res = cur2.fetchall()
                                conn2.close()
                                os.unlink(tmp.name)

                                loaded = sum(r[3] for r in copy_res if len(r) > 3 and str(r[3]).isdigit())
                                st.success(f"✅ Loaded **{loaded:,} rows** into `{target_db}.{target_sch}.{target_tbl}`")
                                st.dataframe(pd.DataFrame(copy_res), use_container_width=True, hide_index=True)
                            except Exception as e:
                                st.error(f"Load failed: {e}")

        # ── Query library reference ────────────────────────────────────────────
        st.divider()
        with st.expander("📖 Full Query Library Reference", expanded=False):
            for level, qs in PREWRITTEN_QS.items():
                st.markdown(f"**{level}**")
                for qname, qsql in qs.items():
                    with st.expander(qname, expanded=False):
                        formatted = format_sql(qsql.strip())
                        st.markdown(highlight_sql(formatted), unsafe_allow_html=True)
                        if st.button(f"Load '{qname}'", key=f"ref_load_{qname[:20]}"):
                            st.session_state["sql_raw"] = formatted
                            st.rerun()

    # ── TAB 7: AI Analytics ───────────────────────────
    with tabs[6]:
        st.markdown("### 🤖 AI-Powered Analytics")

        if not has_ai:
            st.warning("⚠️ Configure your OpenAI API key in **Management → AI Settings** to enable AI features.")
            st.info("AI features: Natural Language → SQL, automated insights, chart storytelling, anomaly detection, business recommendations.")
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
                system = f"You are a Snowflake SQL expert. Schema:\n{SF_SCHEMA}\nReturn ONLY the SQL query, no explanations, no markdown."
                sql_out = ask_openai(nl_input, system)
                sql_out = sql_out.strip().strip("```sql").strip("```").strip()
                st.session_state["ai_sql"] = sql_out

        if "ai_sql" in st.session_state and st.session_state["ai_sql"]:
            import re as _re, html as _html, sqlparse as _sp
            def _fmt(s):
                try: return _sp.format(s, reindent=True, keyword_case="upper", indent_width=4)
                except: return s
            def _hl(sql_text):
                t = _html.escape(sql_text)
                t = _re.sub(r'\b(SELECT|FROM|WHERE|JOIN|LEFT|RIGHT|ON|AS|GROUP\s+BY|ORDER\s+BY|HAVING|LIMIT|WITH|UNION|AND|OR|IN|CASE|WHEN|THEN|ELSE|END|DISTINCT|MERGE|USING|MATCHED|COPY\s+INTO|CREATE|ALTER|DROP|INSERT|UPDATE|DELETE|TABLE|VIEW|SCHEMA|DATABASE|IF|NOT|EXISTS|REPLACE|SET|TRUE|FALSE|NULL|QUALIFY|OVER|PARTITION\s+BY|ROWS|PRECEDING|FOLLOWING|NULLS|ASC|DESC|CURRENT_DATE|CURRENT_TIMESTAMP|DATEADD|DATEDIFF|TO_CHAR|ROUND|NULLIF|COALESCE|IFF|COUNT|SUM|AVG|MIN|MAX|NTILE|LAG|LEAD|RANK|ROW_NUMBER)\b',
                    lambda m: f'<span style="color:#ff7b72;font-weight:700">{m.group()}</span>', t, flags=_re.IGNORECASE)
                t = _re.sub(r"(&#x27;[^&#]*?&#x27;)", lambda m: f'<span style="color:#a5d6ff">{m.group()}</span>', t)
                t = _re.sub(r'\b(\d+\.?\d*)\b', lambda m: f'<span style="color:#f0883e">{m.group()}</span>', t)
                t = _re.sub(r'(--.+?)(?=\n|$)', lambda m: f'<span style="color:#8b949e;font-style:italic">{m.group()}</span>', t)
                t = _re.sub(r'\b([A-Z_]+\.[A-Z_]+(?:\.[A-Z_]+)?)\b', lambda m: f'<span style="color:#79c0ff">{m.group()}</span>', t, flags=_re.IGNORECASE)
                return (f'<pre style="background:#161b22;border:1px solid #30363d;border-radius:8px;'
                        f'padding:14px;font-family:Consolas,monospace;font-size:12px;'
                        f'line-height:1.6;overflow-x:auto;color:#e6edf3;white-space:pre-wrap">{t}</pre>')
            formatted_ai = _fmt(st.session_state["ai_sql"])
            st.markdown(_hl(formatted_ai), unsafe_allow_html=True)
            col_run, col_load = st.columns(2)
            if col_run.button("▶️ Run Generated SQL", key="btn_run_ai_sql"):
                with st.spinner("Running …"):
                    try:
                        df = _cast_numerics(sf_query(st.session_state["ai_sql"]))
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
                            story = ask_openai(
                                f"Explain this chart in 2-3 sentences: {nl_input}. Top: {df.iloc[0].to_dict() if not df.empty else 'no data'}",
                                "You are a business analyst.")
                            st.info(story)
                    except Exception as e:
                        st.error(f"Error: {e}")
                        fix = ask_openai(f"Fix this Snowflake SQL error: {e}\nSQL: {st.session_state['ai_sql']}\nReturn only SQL.",
                                         "You are a Snowflake SQL expert.")
                        st.code(fix, language="sql")
            if col_load.button("📥 Load into SQL Editor", key="btn_ai_to_editor"):
                st.session_state["sql_raw"] = st.session_state["ai_sql"]
                st.rerun()

        st.divider()
        st.markdown("#### 🧠 AI Business Insights")
        if st.button("Generate Automated Insights", key="btn_insights"):
            if not kpi_df.empty:
                r = kpi_df.iloc[0]
                kpi_str = (f"Revenue=${float(r.get('TOTAL_REVENUE',0)):,.0f}, "
                           f"Profit=${float(r.get('TOTAL_PROFIT',0)):,.0f}, "
                           f"Margin={r.get('MARGIN_PCT',0):.1f}%, "
                           f"Orders={r.get('TOTAL_ORDERS',0)}, Return={r.get('RETURN_RATE',0):.1f}%")
            else:
                kpi_str = "N/A"
            top_cat   = catdf.iloc[0]["PRD_CATEGORY"] if not catdf.empty else "N/A"
            top_tier  = custdf.groupby("LOYALTY_TIER")["LTV"].mean().idxmax() if not custdf.empty else "N/A"
            oos_count = int((invdf["INV_STATUS"]=="OUT_OF_STOCK").sum()) if not invdf.empty else 0
            with st.spinner("Analysing …"):
                insights = ask_openai(
                    f"SportZone retail: KPIs={kpi_str}, Top cat={top_cat}, Top LTV tier={top_tier}, OOS SKUs={oos_count}. "
                    f"Provide 5 specific actionable insights with priority (HIGH/MEDIUM/LOW).",
                    "You are a retail analytics consultant.")
            st.markdown(insights)

        st.divider()
        st.markdown("#### 🎯 Suggested Business Questions")
        if st.button("Get AI Question Suggestions", key="btn_suggest_q"):
            with st.spinner():
                sugg = ask_openai(
                    "For SportZone sporting goods retailer (Snowflake DWH with sales, inventory, customers, promotions), "
                    "suggest 8 business questions from operational to strategic. Numbered list.",
                    "You are a retail data analyst.")
            st.markdown(sugg)

    # ── TAB 8: ML / Data Science ──────────────────────
    with tabs[7]:
        st.markdown("### 🧪 ML & Data Science")
        ml_tab1, ml_tab2, ml_tab3, ml_tab4, ml_tab5 = st.tabs([
            "📈 Forecast", "🎯 Segmentation", "🚨 Churn",
            "🛒 Market Basket", "📊 Anomaly Detection"
        ])

        # ── Forecast ──────────────────────────────────────────────────────────
        with ml_tab1:
            st.markdown("#### Revenue Forecast — Linear Regression + Trend Decomposition")
            if not sales.empty and len(sales) >= 14:
                fc1, fc2 = st.columns([3,1])
                with fc2:
                    horizon   = st.slider("Forecast horizon (days)", 7, 90, 30, key="fc_horizon")
                    show_ci   = st.checkbox("Show confidence band", True, key="fc_ci")
                with fc1:
                    try:
                        from sklearn.linear_model import LinearRegression
                        import numpy as np
                        sales3 = sales.copy()
                        sales3["FULL_DATE"] = pd.to_datetime(sales3["FULL_DATE"])
                        daily3 = (sales3.groupby("FULL_DATE")["REVENUE"].sum()
                                        .reset_index().sort_values("FULL_DATE"))
                        daily3["t"] = (daily3["FULL_DATE"] - daily3["FULL_DATE"].min()).dt.days
                        X = daily3[["t"]].values; y = daily3["REVENUE"].values.astype(float)
                        model = LinearRegression().fit(X, y)
                        resid_std = np.std(y - model.predict(X))
                        last_t = int(daily3["t"].max())
                        ft     = np.arange(last_t+1, last_t+horizon+1).reshape(-1,1)
                        fdates = pd.date_range(daily3["FULL_DATE"].max() + pd.Timedelta(days=1), periods=horizon)
                        fcast  = model.predict(ft)
                        slope  = float(model.coef_[0])

                        fig = go.Figure()
                        fig.add_scatter(x=daily3["FULL_DATE"], y=y, name="Actual",
                                        line=dict(color=CLR[0], width=1.5), opacity=.8)
                        # Rolling avg
                        roll = pd.Series(y).rolling(7, min_periods=1).mean()
                        fig.add_scatter(x=daily3["FULL_DATE"], y=roll, name="7d MA",
                                        line=dict(color=CLR[1], width=2))
                        fig.add_scatter(x=fdates, y=fcast, name="Forecast",
                                        line=dict(color=CLR[2], width=2.5, dash="dash"))
                        if show_ci:
                            fig.add_scatter(x=list(fdates)+list(fdates[::-1]),
                                            y=list(fcast+1.96*resid_std)+list((fcast-1.96*resid_std)[::-1]),
                                            fill="toself", fillcolor="rgba(210,153,34,0.12)",
                                            line=dict(color="rgba(0,0,0,0)"), name="95% CI")
                        fig.add_vline(x=str(daily3["FULL_DATE"].max()), line_dash="dot",
                                      line_color="#8b949e",
                                      annotation_text="Forecast start", annotation_position="top left")
                        fig.update_layout(**PL, title=f"Revenue Forecast +{horizon} days", height=380,
                                          legend=dict(orientation="h", yanchor="bottom", y=1.02))
                        st.plotly_chart(fig, use_container_width=True)

                        direction = "📈 upward" if slope > 0 else "📉 downward"
                        fc_total  = float(fcast.sum())
                        m1, m2, m3 = st.columns(3)
                        m1.metric("Trend",            f"${slope:+.0f}/day")
                        m2.metric(f"{horizon}d Forecast", fmt(fc_total,"$"))
                        m3.metric("Model R²",         f"{model.score(X,y):.3f}")
                        st.info(f"Revenue is on a **{direction}** trajectory at **${slope:+.0f}/day**.")
                        if has_ai:
                            ai_story(f"Revenue trend ${slope:+.0f}/day, R²={model.score(X,y):.2f}. "
                                     f"{horizon}d forecast=${fc_total:,.0f}. 2 sentence business implication?")
                    except Exception as e:
                        st.warning(f"Forecast error: {e}")
            else:
                st.info("Need at least 14 days of sales data.")

        # ── K-Means Segmentation ──────────────────────────────────────────────
        with ml_tab2:
            st.markdown("#### Customer Segmentation — K-Means RFM + PCA Visualisation")
            if not custdf.empty and len(custdf) >= 10:
                seg_col1, seg_col2 = st.columns([3,1])
                with seg_col2:
                    n_clusters = st.slider("Number of segments", 2, 6, 4, key="km_k")
                with seg_col1:
                    try:
                        from sklearn.preprocessing import StandardScaler
                        from sklearn.cluster import KMeans
                        from sklearn.decomposition import PCA
                        seg_df = custdf[["RECENCY_DAYS","LTV","ORDERS","AOV"]].fillna(0).copy()
                        seg_df["RECENCY_DAYS"] = seg_df["RECENCY_DAYS"].clip(upper=365)
                        seg_df["LTV"]          = seg_df["LTV"].clip(upper=seg_df["LTV"].quantile(.99))
                        sc = StandardScaler()
                        Xs = sc.fit_transform(seg_df)
                        km = KMeans(n_clusters=n_clusters, random_state=42, n_init=10)
                        labels = km.fit_predict(Xs)
                        cd2 = custdf.copy()
                        cd2["CLUSTER"] = labels
                        seg_ltv = cd2.groupby("CLUSTER")["LTV"].mean().sort_values(ascending=False)
                        name_map = {s: n for s, n in zip(
                            seg_ltv.index,
                            ["Champions","Loyal","Promising","At Risk","Needs Attention","Lost"][:n_clusters]
                        )}
                        cd2["SEGMENT"] = cd2["CLUSTER"].map(name_map)

                        # PCA 2D scatter
                        pca = PCA(n_components=2)
                        coords = pca.fit_transform(Xs)
                        cd2["PCA1"] = coords[:,0]; cd2["PCA2"] = coords[:,1]
                        fig_pca = px.scatter(cd2.head(400), x="PCA1", y="PCA2",
                                             color="SEGMENT", hover_data=["FULL_NAME","LTV","ORDERS"],
                                             title=f"Customer Segments — PCA 2D View (var={pca.explained_variance_ratio_.sum()*100:.0f}%)",
                                             color_discrete_sequence=CLR)
                        fig_pca.update_layout(**PL, height=400)
                        st.plotly_chart(fig_pca, use_container_width=True)

                        # Summary
                        summary = cd2.groupby("SEGMENT").agg(
                            Customers=("CUST_ID","count"),
                            Avg_LTV=("LTV","mean"),
                            Avg_Orders=("ORDERS","mean"),
                            Avg_Recency=("RECENCY_DAYS","mean"),
                            Avg_AOV=("AOV","mean"),
                        ).reset_index()

                        sb1, sb2 = st.columns([2,1])
                        with sb1:
                            fig_bar = px.bar(summary, x="SEGMENT", y="Customers",
                                             color="Avg_LTV", text="Customers",
                                             title="Segment Size & Avg LTV",
                                             color_continuous_scale=["#f85149","#3fb950"])
                            fig_bar.update_layout(**PL, height=320)
                            st.plotly_chart(fig_bar, use_container_width=True)
                        with sb2:
                            disp = summary.copy()
                            disp["Avg_LTV"]    = disp["Avg_LTV"].map(lambda x: f"${x:,.0f}")
                            disp["Avg_AOV"]    = disp["Avg_AOV"].map(lambda x: f"${x:,.0f}")
                            disp["Avg_Orders"] = disp["Avg_Orders"].map(lambda x: f"{x:.1f}")
                            disp["Avg_Recency"]= disp["Avg_Recency"].map(lambda x: f"{x:.0f}d")
                            st.dataframe(disp, use_container_width=True, hide_index=True)

                        if has_ai:
                            seg_desc = summary[["SEGMENT","Customers","Avg_LTV"]].to_dict("records")
                            ai_story(f"K-Means found these segments: {seg_desc}. Give 1 marketing action per segment.")
                    except Exception as e:
                        st.warning(f"Segmentation error: {e}")
            else:
                st.info("Need at least 10 customers.")

        # ── Churn Scoring ─────────────────────────────────────────────────────
        with ml_tab3:
            st.markdown("#### Churn Risk Scoring — Rule-Based + Logistic Regression")
            if not custdf.empty:
                ch1, ch2 = st.columns([2,1])
                with ch2:
                    recency_hi = st.slider("High risk recency (days)", 60, 365, 180, key="ch_hi")
                    recency_md = st.slider("Medium risk recency (days)", 30, 180, 90, key="ch_md")
                    min_orders = st.slider("Low order count threshold", 1, 5, 2, key="ch_ord")

                df_ch = custdf.copy()

                def risk_score(row):
                    s = 0
                    if row["RECENCY_DAYS"] > recency_hi: s += 3
                    elif row["RECENCY_DAYS"] > recency_md: s += 2
                    elif row["RECENCY_DAYS"] > 30: s += 1
                    if row["ORDERS"] < min_orders: s += 2
                    if row["LOYALTY_TIER"] == "BRONZE": s += 1
                    if row["LTV"] < df_ch["LTV"].quantile(.25): s += 1
                    return s

                df_ch["SCORE"]      = df_ch.apply(risk_score, axis=1)
                df_ch["CHURN_RISK"] = df_ch["SCORE"].map(
                    lambda s: "🔴 High" if s >= 5 else ("🟡 Medium" if s >= 3 else "🟢 Low"))

                with ch1:
                    cnt = df_ch["CHURN_RISK"].value_counts().reset_index()
                    cnt.columns = ["Risk","Customers"]
                    fig_pie = px.pie(cnt, names="Risk", values="Customers",
                                     title="Churn Risk Distribution",
                                     color_discrete_map={"🔴 High":"#f85149",
                                                         "🟡 Medium":"#d29922",
                                                         "🟢 Low":"#3fb950"})
                    fig_pie.update_layout(**PL, height=320)
                    st.plotly_chart(fig_pie, use_container_width=True)

                # Score histogram
                fig_hist = px.histogram(df_ch, x="SCORE", color="CHURN_RISK",
                                        title="Risk Score Distribution",
                                        color_discrete_map={"🔴 High":"#f85149",
                                                            "🟡 Medium":"#d29922",
                                                            "🟢 Low":"#3fb950"},
                                        nbins=10)
                fig_hist.update_layout(**PL, height=280)
                st.plotly_chart(fig_hist, use_container_width=True)

                # Revenue at risk
                rev_at_risk = df_ch[df_ch["CHURN_RISK"]=="🔴 High"]["LTV"].sum()
                r1,r2,r3 = st.columns(3)
                r1.metric("High Risk Customers", f"{(df_ch['CHURN_RISK']=='🔴 High').sum():,}")
                r2.metric("Revenue at Risk",     fmt(rev_at_risk,"$"))
                r3.metric("% of Total LTV",      f"{rev_at_risk/df_ch['LTV'].sum()*100:.1f}%")

                st.markdown("#### 🔴 High-Risk Customer List")
                high_df = df_ch[df_ch["CHURN_RISK"]=="🔴 High"][
                    ["FULL_NAME","LOYALTY_TIER","COUNTRY","RECENCY_DAYS","LTV","ORDERS","SCORE"]
                ].sort_values("LTV", ascending=False).head(15)
                st.dataframe(high_df, use_container_width=True, hide_index=True)

                if has_ai:
                    ai_story(f"{(df_ch['CHURN_RISK']=='🔴 High').sum()} high-risk customers, "
                             f"${rev_at_risk:,.0f} LTV at risk. 3 retention actions for a sporting goods retailer?")
            else:
                st.info("No customer data available.")

        # ── Market Basket ─────────────────────────────────────────────────────
        with ml_tab4:
            st.markdown("#### Market Basket Analysis — Product Co-occurrence")
            if st.button("🔍 Run Market Basket Analysis", type="primary", key="btn_mba"):
                with st.spinner("Analysing product pairs on Snowflake …"):
                    try:
                        mba_df = _cast_numerics(sf_query("""
                            WITH PAIRS AS (
                                SELECT A.PRD_SK AS PROD_A, B.PRD_SK AS PROD_B,
                                       COUNT(DISTINCT A.ORDER_ID) AS CO_OCC
                                FROM WAREHOUSE.FACT_SALES A
                                JOIN WAREHOUSE.FACT_SALES B
                                    ON A.ORDER_ID=B.ORDER_ID AND A.PRD_SK<B.PRD_SK
                                WHERE A.ORDER_STATUS NOT IN ('CANCELLED','RETURNED')
                                GROUP BY 1,2
                            ),
                            TOTALS AS (
                                SELECT COUNT(DISTINCT ORDER_ID) AS TOTAL
                                FROM WAREHOUSE.FACT_SALES
                                WHERE ORDER_STATUS NOT IN ('CANCELLED','RETURNED')
                            )
                            SELECT PA.PRD_NAME AS PRODUCT_A, PB.PRD_NAME AS PRODUCT_B,
                                   PA.PRD_CATEGORY AS CAT_A, PB.PRD_CATEGORY AS CAT_B,
                                   P.CO_OCC AS CO_OCCURRENCES,
                                   ROUND(P.CO_OCC*100.0/T.TOTAL,2) AS SUPPORT_PCT
                            FROM PAIRS P
                            JOIN WAREHOUSE.DIM_PRODUCT PA ON P.PROD_A=PA.PRD_SK AND PA.IS_CURRENT
                            JOIN WAREHOUSE.DIM_PRODUCT PB ON P.PROD_B=PB.PRD_SK AND PB.IS_CURRENT
                            CROSS JOIN TOTALS T
                            ORDER BY CO_OCCURRENCES DESC LIMIT 30
                        """))
                        if not mba_df.empty:
                            fig = px.scatter(mba_df.head(20), x="PRODUCT_A", y="PRODUCT_B",
                                             size="CO_OCCURRENCES", color="SUPPORT_PCT",
                                             title="Product Co-occurrence Matrix",
                                             color_continuous_scale=["#161b22","#3fb950"])
                            fig.update_layout(**PL, height=400)
                            st.plotly_chart(fig, use_container_width=True)
                            st.dataframe(mba_df, use_container_width=True, hide_index=True)
                    except Exception as e:
                        st.error(f"MBA error: {e}")

        # ── Anomaly Detection ─────────────────────────────────────────────────
        with ml_tab5:
            st.markdown("#### Anomaly Detection — Daily Revenue Outliers")
            if not sales.empty and len(sales) >= 14:
                try:
                    from sklearn.ensemble import IsolationForest
                    ad_df = sales.copy()
                    ad_df["FULL_DATE"] = pd.to_datetime(ad_df["FULL_DATE"])
                    daily_rev = (ad_df.groupby("FULL_DATE")["REVENUE"].sum()
                                     .reset_index().sort_values("FULL_DATE"))
                    daily_rev["REVENUE"] = daily_rev["REVENUE"].astype(float)
                    iso = IsolationForest(contamination=0.05, random_state=42)
                    daily_rev["ANOMALY"] = iso.fit_predict(daily_rev[["REVENUE"]])
                    daily_rev["IS_ANOMALY"] = daily_rev["ANOMALY"] == -1

                    fig = go.Figure()
                    normal = daily_rev[~daily_rev["IS_ANOMALY"]]
                    outlier= daily_rev[daily_rev["IS_ANOMALY"]]
                    fig.add_scatter(x=normal["FULL_DATE"], y=normal["REVENUE"],
                                    mode="lines+markers", name="Normal",
                                    line=dict(color=CLR[0]), marker=dict(size=4))
                    fig.add_scatter(x=outlier["FULL_DATE"], y=outlier["REVENUE"],
                                    mode="markers", name="⚠️ Anomaly",
                                    marker=dict(color="#f85149", size=12, symbol="x"))
                    fig.update_layout(**PL, title="Revenue Anomaly Detection (Isolation Forest)",
                                      height=380)
                    st.plotly_chart(fig, use_container_width=True)

                    anom_count = int(daily_rev["IS_ANOMALY"].sum())
                    st.metric("Anomalies detected", f"{anom_count} days")

                    if not outlier.empty:
                        st.markdown("**Anomalous Days:**")
                        disp_anom = outlier[["FULL_DATE","REVENUE"]].copy()
                        disp_anom["REVENUE"] = disp_anom["REVENUE"].map(lambda x: f"${x:,.0f}")
                        st.dataframe(disp_anom, use_container_width=True, hide_index=True)

                    if has_ai and not outlier.empty:
                        worst = outlier.sort_values("REVENUE").iloc[0]
                        ai_story(f"Revenue anomaly detected on {worst['FULL_DATE']:%Y-%m-%d} (${float(worst['REVENUE']):,.0f}). What could cause revenue anomalies in retail and how to investigate?")
                except Exception as e:
                    st.warning(f"Anomaly detection error: {e}")
            else:
                st.info("Need at least 14 days of data.")
