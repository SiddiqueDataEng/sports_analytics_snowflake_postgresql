"""
load.py
=======
Load transformed DataFrames into Snowflake (RAW + STAGING layers),
then trigger the MERGE into WAREHOUSE dimensions and facts.

Strategy
--------
  RAW tables    → INSERT  (append-only audit log)
  STAGING tables → TRUNCATE + INSERT (always fresh staging)
  WAREHOUSE     → MERGE via existing 03_warehouse/03_load_warehouse.sql logic
                  re-implemented as Python MERGE statements for incremental runs

Uses snowflake-connector-python write_pandas() for bulk loads via PUT+COPY.
"""

import logging
import re
import sys
from datetime import date, datetime
from pathlib import Path

import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

PROJECT_DIR = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_DIR / "deploy"))
from config import load_config

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Connection
# ---------------------------------------------------------------------------

def get_sf_conn():
    params = load_config()
    logger.info("  Connecting to Snowflake (browser SSO) …")
    conn = snowflake.connector.connect(**params)
    return conn


# ---------------------------------------------------------------------------
# Warehouse MERGE statements
# ---------------------------------------------------------------------------

WAREHOUSE_MERGES = {

"DIM_CUSTOMER": """
MERGE INTO SPORTZONE_DEV.WAREHOUSE.DIM_CUSTOMER tgt
USING (
    SELECT
        CUST_ID,
        FIRST_NAME,
        LAST_NAME,
        FULL_NAME,
        EMAIL,
        PHONE,
        DATE_OF_BIRTH::DATE                AS DATE_OF_BIRTH,
        AGE_BAND,
        GENDER,
        CITY,
        STATE,
        COUNTRY,
        LOYALTY_TIER,
        SIGNUP_DATE::DATE                  AS SIGNUP_DATE,
        IS_ACTIVE::BOOLEAN                 AS IS_ACTIVE,
        CURRENT_DATE()                     AS EFF_START_DT,
        CAST(NULL AS DATE)                 AS EFF_END_DT,
        TRUE                               AS IS_CURRENT
    FROM SPORTZONE_DEV.STAGING.STG_CUSTOMERS
    QUALIFY ROW_NUMBER() OVER (PARTITION BY CUST_ID ORDER BY _LOAD_TS DESC) = 1
) src
ON tgt.CUST_ID = src.CUST_ID AND tgt.IS_CURRENT = TRUE
WHEN MATCHED AND (
    tgt.LOYALTY_TIER <> src.LOYALTY_TIER OR
    tgt.CITY         <> src.CITY         OR
    tgt.IS_ACTIVE    <> src.IS_ACTIVE
)
THEN UPDATE SET
    tgt.EFF_END_DT = CURRENT_DATE() - 1,
    tgt.IS_CURRENT = FALSE
WHEN NOT MATCHED THEN
INSERT (CUST_ID, FIRST_NAME, LAST_NAME, FULL_NAME, EMAIL, PHONE,
        DATE_OF_BIRTH, AGE_BAND, GENDER,
        CITY, STATE, COUNTRY, LOYALTY_TIER,
        SIGNUP_DATE, IS_ACTIVE, EFF_START_DT, EFF_END_DT, IS_CURRENT)
VALUES (src.CUST_ID, src.FIRST_NAME, src.LAST_NAME, src.FULL_NAME,
        src.EMAIL, src.PHONE, src.DATE_OF_BIRTH, src.AGE_BAND, src.GENDER,
        src.CITY, src.STATE, src.COUNTRY, src.LOYALTY_TIER,
        src.SIGNUP_DATE, src.IS_ACTIVE, src.EFF_START_DT, src.EFF_END_DT, src.IS_CURRENT)
""",

"DIM_PRODUCT": """
MERGE INTO SPORTZONE_DEV.WAREHOUSE.DIM_PRODUCT tgt
USING (
    SELECT
        PRD_ID,
        PRD_NAME,
        PRD_CATEGORY,
        PRD_SUBCATEGORY,
        BRAND,
        SKU,
        UNIT_PRICE::FLOAT                   AS UNIT_PRICE,
        UNIT_COST::FLOAT                    AS UNIT_COST,
        GROSS_MARGIN_PCT::FLOAT             AS GROSS_MARGIN_PCT,
        PRICE_TIER,
        IS_ACTIVE::BOOLEAN                  AS IS_ACTIVE,
        LAUNCH_DATE::DATE                   AS LAUNCH_DATE,
        DISCONTINUE_DT::DATE                AS DISCONTINUE_DT,
        CURRENT_DATE()                      AS EFF_START_DT,
        CAST(NULL AS DATE)                  AS EFF_END_DT,
        TRUE                                AS IS_CURRENT
    FROM SPORTZONE_DEV.STAGING.STG_PRODUCTS
    QUALIFY ROW_NUMBER() OVER (PARTITION BY PRD_ID ORDER BY _LOAD_TS DESC) = 1
) src
ON tgt.PRD_ID = src.PRD_ID AND tgt.IS_CURRENT = TRUE
WHEN MATCHED AND tgt.UNIT_PRICE <> src.UNIT_PRICE
THEN UPDATE SET
    tgt.EFF_END_DT = CURRENT_DATE() - 1,
    tgt.IS_CURRENT = FALSE
WHEN NOT MATCHED THEN
INSERT (PRD_ID, PRD_NAME, PRD_CATEGORY, PRD_SUBCATEGORY, BRAND, SKU,
        UNIT_PRICE, UNIT_COST, GROSS_MARGIN_PCT, PRICE_TIER,
        IS_ACTIVE, LAUNCH_DATE, DISCONTINUE_DT,
        EFF_START_DT, EFF_END_DT, IS_CURRENT)
VALUES (src.PRD_ID, src.PRD_NAME, src.PRD_CATEGORY, src.PRD_SUBCATEGORY,
        src.BRAND, src.SKU, src.UNIT_PRICE, src.UNIT_COST, src.GROSS_MARGIN_PCT,
        src.PRICE_TIER, src.IS_ACTIVE, src.LAUNCH_DATE, src.DISCONTINUE_DT,
        src.EFF_START_DT, src.EFF_END_DT, src.IS_CURRENT)
""",

"DIM_STORE": """
MERGE INTO SPORTZONE_DEV.WAREHOUSE.DIM_STORE tgt
USING (
    SELECT
        STORE_ID,
        STORE_NAME,
        STORE_TYPE,
        REGION,
        CITY,
        STATE,
        COUNTRY,
        TRY_CAST(OPEN_DATE AS DATE) AS OPEN_DATE,
        MANAGER_NAME
    FROM SPORTZONE_DEV.STAGING.STG_STORES
    QUALIFY ROW_NUMBER() OVER (PARTITION BY STORE_ID ORDER BY _LOAD_TS DESC) = 1
) src
ON tgt.STORE_ID = src.STORE_ID
WHEN MATCHED THEN UPDATE SET
    tgt.MANAGER_NAME = src.MANAGER_NAME,
    tgt._LOAD_TS     = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN
INSERT (STORE_ID, STORE_NAME, STORE_TYPE, REGION, CITY, STATE, COUNTRY, OPEN_DATE, MANAGER_NAME)
VALUES (src.STORE_ID, src.STORE_NAME, src.STORE_TYPE, src.REGION,
        src.CITY, src.STATE, src.COUNTRY, src.OPEN_DATE, src.MANAGER_NAME)
""",

"DIM_PROMOTION": """
MERGE INTO SPORTZONE_DEV.WAREHOUSE.DIM_PROMOTION tgt
USING (
    SELECT
        PROMO_ID,
        PROMO_CODE,
        PROMO_NAME,
        PROMO_TYPE,
        DISCOUNT_VALUE::FLOAT              AS DISCOUNT_VALUE,
        START_DATE::DATE                   AS START_DATE,
        END_DATE::DATE                     AS END_DATE,
        MIN_ORDER_AMT::FLOAT               AS MIN_ORDER_AMT,
        IS_ACTIVE::BOOLEAN                 AS IS_ACTIVE
    FROM SPORTZONE_DEV.STAGING.STG_PROMOTIONS
    QUALIFY ROW_NUMBER() OVER (PARTITION BY PROMO_ID ORDER BY _LOAD_TS DESC) = 1
) src
ON tgt.PROMO_ID = src.PROMO_ID
WHEN MATCHED THEN UPDATE SET
    tgt.IS_ACTIVE = src.IS_ACTIVE,
    tgt._LOAD_TS  = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN
INSERT (PROMO_ID, PROMO_CODE, PROMO_NAME, PROMO_TYPE,
        DISCOUNT_VALUE, START_DATE, END_DATE, MIN_ORDER_AMT, IS_ACTIVE)
VALUES (src.PROMO_ID, src.PROMO_CODE, src.PROMO_NAME, src.PROMO_TYPE,
        src.DISCOUNT_VALUE, src.START_DATE, src.END_DATE, src.MIN_ORDER_AMT, src.IS_ACTIVE)
""",

"FACT_SALES": """
MERGE INTO SPORTZONE_DEV.WAREHOUSE.FACT_SALES tgt
USING (
    SELECT
        TO_NUMBER(TO_CHAR(ord.ORDER_DATE, 'YYYYMMDD'))         AS ORDER_DATE_KEY,
        COALESCE(dc.CUST_SK,  -1)                              AS CUST_SK,
        COALESCE(dp.PRD_SK,   -1)                              AS PRD_SK,
        COALESCE(ds.STORE_SK, -1)                              AS STORE_SK,
        COALESCE(dpr.PROMO_SK,-1)                              AS PROMO_SK,
        ord.ORDER_ID,
        oi.ORDER_ITEM_ID,
        ord.CHANNEL,
        ord.ORDER_STATUS,
        oi.QTY::INTEGER                                        AS QTY_ORDERED,
        oi.UNIT_PRICE::FLOAT                                   AS UNIT_PRICE,
        dp.UNIT_COST::FLOAT                                    AS UNIT_COST,
        dp.GROSS_MARGIN_PCT::FLOAT                             AS GROSS_MARGIN_PCT,
        oi.DISCOUNT_PCT::FLOAT                                 AS DISCOUNT_PCT,
        oi.DISCOUNT_AMT::FLOAT                                 AS DISCOUNT_AMT,
        (oi.QTY::FLOAT * oi.UNIT_PRICE::FLOAT)                AS GROSS_REVENUE,
        oi.LINE_TOTAL::FLOAT                                   AS NET_REVENUE,
        (oi.QTY::FLOAT * COALESCE(dp.UNIT_COST::FLOAT, 0))    AS COGS,
        (oi.LINE_TOTAL::FLOAT - oi.QTY::FLOAT * COALESCE(dp.UNIT_COST::FLOAT, 0)) AS GROSS_PROFIT
    FROM SPORTZONE_DEV.STAGING.STG_ORDER_ITEMS oi
    JOIN SPORTZONE_DEV.STAGING.STG_ORDERS      ord ON oi.ORDER_ID = ord.ORDER_ID
    LEFT JOIN SPORTZONE_DEV.WAREHOUSE.DIM_CUSTOMER  dc  ON ord.CUST_ID   = dc.CUST_ID  AND dc.IS_CURRENT
    LEFT JOIN SPORTZONE_DEV.WAREHOUSE.DIM_PRODUCT   dp  ON oi.PRD_ID     = dp.PRD_ID   AND dp.IS_CURRENT
    LEFT JOIN SPORTZONE_DEV.WAREHOUSE.DIM_STORE     ds  ON ord.STORE_ID  = ds.STORE_ID
    LEFT JOIN SPORTZONE_DEV.WAREHOUSE.DIM_PROMOTION dpr ON COALESCE(ord.PROMO_CODE,'NONE') = dpr.PROMO_CODE
) src
ON tgt.ORDER_ITEM_ID = src.ORDER_ITEM_ID
WHEN NOT MATCHED THEN
INSERT (ORDER_DATE_KEY, CUST_SK, PRD_SK, STORE_SK, PROMO_SK,
        ORDER_ID, ORDER_ITEM_ID, CHANNEL, ORDER_STATUS,
        QTY_ORDERED, UNIT_PRICE, UNIT_COST, GROSS_MARGIN_PCT,
        DISCOUNT_PCT, DISCOUNT_AMT, GROSS_REVENUE, NET_REVENUE, COGS, GROSS_PROFIT)
VALUES (src.ORDER_DATE_KEY, src.CUST_SK, src.PRD_SK, src.STORE_SK, src.PROMO_SK,
        src.ORDER_ID, src.ORDER_ITEM_ID, src.CHANNEL, src.ORDER_STATUS,
        src.QTY_ORDERED, src.UNIT_PRICE, src.UNIT_COST, src.GROSS_MARGIN_PCT,
        src.DISCOUNT_PCT, src.DISCOUNT_AMT, src.GROSS_REVENUE, src.NET_REVENUE,
        src.COGS, src.GROSS_PROFIT)
""",

"FACT_INVENTORY": """
MERGE INTO SPORTZONE_DEV.WAREHOUSE.FACT_INVENTORY tgt
USING (
    SELECT
        TO_NUMBER(TO_CHAR(inv.SNAPSHOT_DATE::DATE, 'YYYYMMDD'))  AS SNAPSHOT_DATE_KEY,
        COALESCE(dp.PRD_SK,   -1)                                AS PRD_SK,
        COALESCE(ds.STORE_SK, -1)                                AS STORE_SK,
        inv.INVENTORY_ID,
        inv.QTY_ON_HAND::INTEGER                                 AS QTY_ON_HAND,
        inv.QTY_RESERVED::INTEGER                                AS QTY_RESERVED,
        inv.QTY_AVAILABLE::INTEGER                               AS QTY_AVAILABLE,
        inv.REORDER_POINT::INTEGER                               AS REORDER_POINT,
        inv.INV_STATUS,
        inv.NEEDS_REORDER::BOOLEAN                               AS NEEDS_REORDER,
        (inv.QTY_ON_HAND::FLOAT * COALESCE(dp.UNIT_COST::FLOAT, 0)) AS STOCK_VALUE
    FROM SPORTZONE_DEV.STAGING.STG_INVENTORY   inv
    LEFT JOIN SPORTZONE_DEV.WAREHOUSE.DIM_PRODUCT  dp ON inv.PRD_ID   = dp.PRD_ID   AND dp.IS_CURRENT
    LEFT JOIN SPORTZONE_DEV.WAREHOUSE.DIM_STORE    ds ON inv.STORE_ID = ds.STORE_ID
) src
ON  tgt.INVENTORY_ID      = src.INVENTORY_ID
AND tgt.SNAPSHOT_DATE_KEY = src.SNAPSHOT_DATE_KEY
WHEN NOT MATCHED THEN
INSERT (SNAPSHOT_DATE_KEY, PRD_SK, STORE_SK, INVENTORY_ID,
        QTY_ON_HAND, QTY_RESERVED, QTY_AVAILABLE, REORDER_POINT,
        INV_STATUS, NEEDS_REORDER, STOCK_VALUE)
VALUES (src.SNAPSHOT_DATE_KEY, src.PRD_SK, src.STORE_SK, src.INVENTORY_ID,
        src.QTY_ON_HAND, src.QTY_RESERVED, src.QTY_AVAILABLE, src.REORDER_POINT,
        src.INV_STATUS, src.NEEDS_REORDER, src.STOCK_VALUE)
""",
}

# Tables where staging is truncated before insert (always-fresh)
TRUNCATE_STAGING = [
    "STAGING.STG_CUSTOMERS",
    "STAGING.STG_PRODUCTS",
    "STAGING.STG_STORES",
    "STAGING.STG_PROMOTIONS",
    "STAGING.STG_ORDERS",
    "STAGING.STG_ORDER_ITEMS",
    "STAGING.STG_INVENTORY",
]


# ---------------------------------------------------------------------------
# Load helpers
# ---------------------------------------------------------------------------

def truncate_staging(conn, tables_loaded: list[str] = None):
    """
    Truncate only the staging tables that are about to be reloaded.
    If tables_loaded is None, truncate all. Otherwise only truncate
    the ones whose data is being refreshed this run to avoid leaving
    dependent tables (e.g. STG_ORDERS) empty when only order_items loaded.
    """
    cur = conn.cursor()
    for tbl in TRUNCATE_STAGING:
        # Derive the short name, e.g. "STAGING.STG_ORDERS" → "STG_ORDERS"
        short = tbl.split(".")[-1]
        # Only truncate if we're actually loading this table this run
        if tables_loaded is not None:
            if not any(short in t.upper() for t in tables_loaded):
                logger.debug("  SKIP truncate %s (not in this run)", tbl)
                continue
        try:
            cur.execute(f"TRUNCATE TABLE SPORTZONE_DEV.{tbl}")
            logger.debug("  TRUNCATE %s", tbl)
        except Exception as e:
            logger.warning("  TRUNCATE %s failed: %s", tbl, e)
    cur.close()


def df_to_snowflake(conn, df: pd.DataFrame, database: str, schema: str, table: str) -> int:
    """Bulk-load a DataFrame into Snowflake using write_pandas (PUT+COPY)."""
    if df.empty:
        return 0

    # Snowflake column names must be uppercase
    df.columns = [c.upper() for c in df.columns]

    # Convert Python date/datetime objects to strings so the connector handles them reliably.
    # Keep None as None (not empty string) so Snowflake inserts NULL for date columns.
    for col in df.columns:
        if df[col].dtype == object or str(df[col].dtype).startswith("datetime"):
            # Convert date/datetime objects to ISO strings; leave None/NaT as None
            def _to_str(v):
                if v is None or (isinstance(v, float) and pd.isna(v)):
                    return None
                try:
                    import datetime as _dt
                    if isinstance(v, (_dt.date, _dt.datetime)):
                        return v.isoformat()
                except Exception:
                    pass
                s = str(v)
                return None if s in ("NaT", "None", "nan", "") else s

            if any(isinstance(v, __import__("datetime").date) for v in df[col].dropna()):
                df[col] = df[col].apply(_to_str)

    # Replace pandas NA/NaT with None so the connector writes NULL
    df = df.where(pd.notnull(df), None)

    success, n_chunks, n_rows, output = write_pandas(
        conn, df,
        table_name=table,
        database=database,
        schema=schema,
        overwrite=False,
        quote_identifiers=False,
    )
    return n_rows


def run_warehouse_merges(conn) -> dict[str, int]:
    """Execute all DIM + FACT MERGE statements. Returns {table: rows_affected}."""
    cur = conn.cursor()
    logger.info("  Running WAREHOUSE merges …")
    results = {}
    for name, sql in WAREHOUSE_MERGES.items():
        try:
            cur.execute(sql)
            affected = cur.rowcount
            results[f"WAREHOUSE.{name}"] = affected
            logger.info("    MERGE %-20s  [%d rows affected]", name, affected)
        except Exception as e:
            logger.error("    MERGE %s FAILED: %s", name, e)
            results[f"WAREHOUSE.{name}"] = -1
    cur.close()
    return results


# ---------------------------------------------------------------------------
# Main load
# ---------------------------------------------------------------------------

def run_load(transformed: dict[str, pd.DataFrame]) -> dict[str, int]:
    """
    Load transformed DataFrames into Snowflake.
    Returns {table: rows_loaded}.
    """
    conn    = get_sf_conn()
    results = {}

    logger.info("=== LOAD phase ===")

    # 1 — Truncate only staging tables that are being reloaded this run
    truncate_staging(conn, tables_loaded=list(transformed.keys()))

    # 2 — Load RAW + STAGING tables
    for full_table, df in transformed.items():
        schema_tbl = full_table.split(".")          # ["RAW","RAW_CUSTOMERS"]
        if len(schema_tbl) != 2:
            logger.warning("  Unexpected table format: %s", full_table)
            continue

        schema, table = schema_tbl
        logger.info("  Loading %-40s  %d rows …", full_table, len(df))

        try:
            rows = df_to_snowflake(conn, df.copy(),
                                   database="SPORTZONE_DEV",
                                   schema=schema,
                                   table=table)
            results[full_table] = rows
            logger.info("    → %d rows loaded", rows)
        except Exception as e:
            logger.error("    FAILED for %s: %s", full_table, e)
            results[full_table] = -1

    # 3 — Warehouse MERGE (dims + facts)
    merge_results = run_warehouse_merges(conn)
    results.update(merge_results)

    conn.close()
    logger.info("Load complete.\n")
    return results
