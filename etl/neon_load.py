"""
neon_load.py
============
Loads transformed DataFrames into Neon Postgres DWH schemas
(raw, staging, warehouse) using psycopg2 execute_values — fast, no ORM.

Called by neon_pipeline.py as an alternative to the Snowflake load.
Can also be run standalone to reload from the latest Parquet staging files.

Usage
-----
    python etl/neon_load.py --full-load        # reload everything
    python etl/neon_load.py                    # incremental (new parquet files only)
    python etl/neon_load.py --status           # show DWH row counts
"""

import logging
import sys
from datetime import date, datetime, timezone
from pathlib import Path

import pandas as pd
import psycopg2
import psycopg2.extras

PROJECT_DIR = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_DIR))
from db import get_neon_conn

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Column maps: DataFrame column → Postgres table column
# ---------------------------------------------------------------------------

RAW_COLS = {
    "RAW.RAW_CUSTOMERS": [
        "CUST_ID","FIRST_NAME","LAST_NAME","EMAIL","PHONE","DATE_OF_BIRTH",
        "GENDER","ADDRESS_LINE1","ADDRESS_LINE2","CITY","STATE","ZIP_CODE",
        "COUNTRY","LOYALTY_TIER","SIGNUP_DATE","IS_ACTIVE","_SRC_FILE","_LOAD_TS"
    ],
    "RAW.RAW_PRODUCTS": [
        "PRD_ID","PRD_NAME","PRD_CATEGORY","PRD_SUBCATEGORY","BRAND",
        "UNIT_PRICE","UNIT_COST","SKU","SUPPLIER_ID","IS_ACTIVE",
        "LAUNCH_DATE","DISCONTINUE_DT","_SRC_FILE","_LOAD_TS"
    ],
    "RAW.RAW_STORES": [
        "STORE_ID","STORE_NAME","STORE_TYPE","REGION","ADDRESS",
        "CITY","STATE","ZIP_CODE","COUNTRY","OPEN_DATE","MANAGER_NAME","_SRC_FILE","_LOAD_TS"
    ],
    "RAW.RAW_PROMOTIONS": [
        "PROMO_ID","PROMO_CODE","PROMO_NAME","PROMO_TYPE","DISCOUNT_VALUE",
        "START_DATE","END_DATE","MIN_ORDER_AMT","IS_ACTIVE","_SRC_FILE","_LOAD_TS"
    ],
    "RAW.RAW_ORDERS": [
        "ORDER_ID","ORDER_DATE","CUST_ID","STORE_ID","CHANNEL",
        "ORDER_STATUS","PROMO_CODE","TOTAL_AMT","DISCOUNT_AMT","NET_AMT","_SRC_FILE","_LOAD_TS"
    ],
    "RAW.RAW_ORDER_ITEMS": [
        "ORDER_ITEM_ID","ORDER_ID","PRD_ID","QTY","UNIT_PRICE",
        "DISCOUNT_PCT","LINE_TOTAL","_SRC_FILE","_LOAD_TS"
    ],
    "RAW.RAW_INVENTORY": [
        "INVENTORY_ID","PRD_ID","STORE_ID","SNAPSHOT_DATE",
        "QTY_ON_HAND","QTY_RESERVED","REORDER_POINT","INV_STATUS","_SRC_FILE","_LOAD_TS"
    ],
}

STG_COLS = {
    "STAGING.STG_CUSTOMERS": [
        "CUST_ID","FIRST_NAME","LAST_NAME","FULL_NAME","EMAIL","PHONE",
        "DATE_OF_BIRTH","AGE_BAND","GENDER","ADDRESS_LINE1","ADDRESS_LINE2",
        "CITY","STATE","ZIP_CODE","COUNTRY","LOYALTY_TIER","SIGNUP_DATE",
        "IS_ACTIVE","EFF_START_DT","EFF_END_DT","IS_CURRENT","_LOAD_TS"
    ],
    "STAGING.STG_PRODUCTS": [
        "PRD_ID","PRD_NAME","PRD_CATEGORY","PRD_SUBCATEGORY","BRAND","SKU",
        "UNIT_PRICE","UNIT_COST","GROSS_MARGIN_PCT","PRICE_TIER","SUPPLIER_ID",
        "IS_ACTIVE","LAUNCH_DATE","DISCONTINUE_DT","EFF_START_DT","EFF_END_DT","IS_CURRENT","_LOAD_TS"
    ],
    "STAGING.STG_STORES": [
        "STORE_ID","STORE_NAME","STORE_TYPE","REGION","ADDRESS",
        "CITY","STATE","ZIP_CODE","COUNTRY","OPEN_DATE","MANAGER_NAME","_SRC_FILE","_LOAD_TS"
    ],
    "STAGING.STG_PROMOTIONS": [
        "PROMO_ID","PROMO_CODE","PROMO_NAME","PROMO_TYPE","DISCOUNT_VALUE",
        "START_DATE","END_DATE","MIN_ORDER_AMT","IS_ACTIVE","_LOAD_TS"
    ],
    "STAGING.STG_ORDERS": [
        "ORDER_ID","ORDER_DATE","ORDER_YEAR","ORDER_MONTH","ORDER_QUARTER",
        "CUST_ID","STORE_ID","CHANNEL","ORDER_STATUS","PROMO_CODE",
        "TOTAL_AMT","DISCOUNT_AMT","NET_AMT","IS_DISCOUNTED","_LOAD_TS"
    ],
    "STAGING.STG_ORDER_ITEMS": [
        "ORDER_ITEM_ID","ORDER_ID","PRD_ID","QTY","UNIT_PRICE",
        "DISCOUNT_PCT","LINE_TOTAL","DISCOUNT_AMT","_LOAD_TS"
    ],
    "STAGING.STG_INVENTORY": [
        "INVENTORY_ID","PRD_ID","STORE_ID","SNAPSHOT_DATE",
        "QTY_ON_HAND","QTY_RESERVED","QTY_AVAILABLE","REORDER_POINT",
        "INV_STATUS","NEEDS_REORDER","_LOAD_TS"
    ],
}

# Map Snowflake-style target table names → Neon Postgres table names
TABLE_MAP = {
    "RAW.RAW_CUSTOMERS":      "raw.customers",
    "RAW.RAW_PRODUCTS":       "raw.products",
    "RAW.RAW_STORES":         "raw.stores",
    "RAW.RAW_PROMOTIONS":     "raw.promotions",
    "RAW.RAW_ORDERS":         "raw.orders",
    "RAW.RAW_ORDER_ITEMS":    "raw.order_items",
    "RAW.RAW_INVENTORY":      "raw.inventory",
    "STAGING.STG_CUSTOMERS":  "staging.customers",
    "STAGING.STG_PRODUCTS":   "staging.products",
    "STAGING.STG_STORES":     "staging.stores",
    "STAGING.STG_PROMOTIONS": "staging.promotions",
    "STAGING.STG_ORDERS":     "staging.orders",
    "STAGING.STG_ORDER_ITEMS":"staging.order_items",
    "STAGING.STG_INVENTORY":  "staging.inventory",
}

# Staging tables to TRUNCATE before reload (always fresh)
TRUNCATE_STG = [
    "staging.customers","staging.products","staging.stores","staging.promotions",
    "staging.orders","staging.order_items","staging.inventory",
]


# ---------------------------------------------------------------------------
# Postgres column name map (Snowflake uppercase → Postgres lowercase)
# ---------------------------------------------------------------------------

def pg_col(c: str) -> str:
    return c.lower()


def df_to_postgres(cur, df: pd.DataFrame, pg_table: str, conflict: str = "DO NOTHING"):
    """Bulk insert a DataFrame into a Postgres table using execute_values."""
    if df.empty:
        return 0

    # Align column names to lowercase
    df = df.copy()
    df.columns = [c.lower() for c in df.columns]

    # Get actual columns that exist in the table
    schema, tbl = pg_table.split(".")
    cur.execute("""
        SELECT column_name FROM information_schema.columns
        WHERE table_schema=%s AND table_name=%s
        ORDER BY ordinal_position
    """, (schema, tbl))
    db_cols = [r[0] for r in cur.fetchall()]

    # Only keep df columns that exist in the table
    insert_cols = [c for c in df.columns if c in db_cols]
    if not insert_cols:
        logger.warning("  No matching columns for %s", pg_table)
        return 0

    df = df[insert_cols].copy()

    # Normalise every column to Python-native types
    for col in df.columns:
        dtype_str = str(df[col].dtype)
        if "int" in dtype_str:
            df[col] = [None if (v is None or v is pd.NA or (isinstance(v, float) and v != v))
                       else int(v) for v in df[col]]
        elif "float" in dtype_str:
            df[col] = [None if (v is None or v is pd.NA or (isinstance(v, float) and v != v))
                       else float(v) for v in df[col]]
        elif "datetime" in dtype_str or "Timestamp" in dtype_str:
            df[col] = [None if pd.isna(v) else v for v in df[col]]
        else:
            df[col] = [
                None if (
                    v is None or
                    v is pd.NA or
                    (isinstance(v, float) and v != v) or
                    str(v) in ("NaT", "nan", "None", "")
                ) else v
                for v in df[col]
            ]

    col_str = ", ".join(insert_cols)
    sql = f"INSERT INTO {pg_table} ({col_str}) VALUES %s ON CONFLICT {conflict}"

    rows = [tuple(row) for row in df.itertuples(index=False, name=None)]
    psycopg2.extras.execute_values(cur, sql, rows, page_size=500)
    return len(rows)


# ---------------------------------------------------------------------------
# Warehouse merge logic (pure SQL on Neon)
# ---------------------------------------------------------------------------

WAREHOUSE_MERGES = [
    # dim_customer SCD2 — expire changed rows
    """
    UPDATE warehouse.dim_customer tgt
    SET eff_end_dt = CURRENT_DATE - 1, is_current = FALSE
    FROM staging.customers src
    WHERE tgt.cust_id = src.cust_id
      AND tgt.is_current = TRUE
      AND (tgt.loyalty_tier <> src.loyalty_tier
           OR tgt.city <> src.city
           OR tgt.is_active <> src.is_active)
    """,
    # dim_customer — insert new/changed
    """
    INSERT INTO warehouse.dim_customer
        (cust_id, first_name, last_name, full_name, email, phone, date_of_birth,
         age_band, gender, city, state, country, loyalty_tier,
         signup_date, is_active, eff_start_dt, is_current)
    SELECT
        cust_id, first_name, last_name, full_name, email, phone, date_of_birth,
        age_band, gender, city, state, country, loyalty_tier,
        signup_date, is_active, CURRENT_DATE, TRUE
    FROM staging.customers src
    WHERE NOT EXISTS (
        SELECT 1 FROM warehouse.dim_customer tgt
        WHERE tgt.cust_id = src.cust_id AND tgt.is_current = TRUE
    )
    """,
    # dim_product SCD2
    """
    UPDATE warehouse.dim_product tgt
    SET eff_end_dt = CURRENT_DATE - 1, is_current = FALSE
    FROM staging.products src
    WHERE tgt.prd_id = src.prd_id
      AND tgt.is_current = TRUE
      AND tgt.unit_price <> src.unit_price
    """,
    """
    INSERT INTO warehouse.dim_product
        (prd_id, prd_name, prd_category, prd_subcategory, brand, sku,
         unit_price, unit_cost, gross_margin_pct, price_tier,
         is_active, launch_date, discontinue_dt, eff_start_dt, is_current)
    SELECT
        prd_id, prd_name, prd_category, prd_subcategory, brand, sku,
        unit_price, unit_cost, gross_margin_pct,
        CASE WHEN unit_price < 50 THEN 'BUDGET'
             WHEN unit_price < 120 THEN 'MID'
             ELSE 'PREMIUM' END,
        is_active, launch_date, discontinue_dt, CURRENT_DATE, TRUE
    FROM staging.products src
    WHERE NOT EXISTS (
        SELECT 1 FROM warehouse.dim_product tgt
        WHERE tgt.prd_id = src.prd_id AND tgt.is_current = TRUE
    )
    """,
    # dim_store upsert
    """
    INSERT INTO warehouse.dim_store
        (store_id, store_name, store_type, region, city, state, country, open_date, manager_name)
    SELECT DISTINCT ON (store_id)
        store_id::TEXT, store_name, store_type, region, city, state, country,
        CASE WHEN open_date IS NOT NULL AND open_date::TEXT ~ '^\d{4}-\d{2}-\d{2}$'
             THEN open_date::DATE ELSE NULL END,
        manager_name
    FROM raw.stores
    WHERE store_id IS NOT NULL
    ON CONFLICT (store_id) DO UPDATE SET
        manager_name = EXCLUDED.manager_name,
        _load_ts = NOW()
    """,
    # dim_promotion upsert — from raw to avoid duplicate constraint issues
    """
    INSERT INTO warehouse.dim_promotion
        (promo_id, promo_code, promo_name, promo_type,
         discount_value, start_date, end_date, min_order_amt, is_active)
    SELECT DISTINCT ON (promo_code)
        promo_id::TEXT, UPPER(promo_code), promo_name, UPPER(promo_type),
        NULLIF(discount_value,'')::NUMERIC,
        start_date::DATE, end_date::DATE,
        NULLIF(min_order_amt,'')::NUMERIC,
        UPPER(TRIM(is_active)) IN ('Y','1','TRUE','t')
    FROM raw.promotions
    WHERE promo_code IS NOT NULL
    ON CONFLICT (promo_code) DO UPDATE SET
        is_active = EXCLUDED.is_active, _load_ts = NOW()
    """,
    # fact_sales
    """
    INSERT INTO warehouse.fact_sales
        (order_date_key, cust_sk, prd_sk, store_sk, promo_sk,
         order_id, order_item_id, channel, order_status,
         qty_ordered, unit_price, unit_cost, gross_margin_pct,
         discount_pct, discount_amt, gross_revenue, net_revenue, cogs, gross_profit)
    SELECT
        TO_CHAR(o.order_date,'YYYYMMDD')::INTEGER,
        COALESCE(dc.cust_sk, -1),
        COALESCE(dp.prd_sk,  -1),
        COALESCE(ds.store_sk,-1),
        COALESCE(dpr.promo_sk,-1),
        o.order_id::TEXT,
        oi.order_item_id::TEXT,
        o.channel, o.order_status,
        oi.qty::INTEGER,
        oi.unit_price::NUMERIC,
        dp.unit_cost::NUMERIC,
        dp.gross_margin_pct::NUMERIC,
        oi.discount_pct::NUMERIC,
        oi.discount_amt::NUMERIC,
        (oi.qty * oi.unit_price)::NUMERIC,
        oi.line_total::NUMERIC,
        (oi.qty * COALESCE(dp.unit_cost,0))::NUMERIC,
        (oi.line_total - oi.qty * COALESCE(dp.unit_cost,0))::NUMERIC
    FROM staging.order_items         oi
    JOIN staging.orders              o   ON oi.order_id::TEXT    = o.order_id::TEXT
    LEFT JOIN warehouse.dim_customer dc  ON o.cust_id::TEXT      = dc.cust_id AND dc.is_current
    LEFT JOIN warehouse.dim_product  dp  ON oi.prd_id::TEXT      = dp.prd_id  AND dp.is_current
    LEFT JOIN warehouse.dim_store    ds  ON o.store_id::TEXT     = ds.store_id
    LEFT JOIN warehouse.dim_promotion dpr ON COALESCE(o.promo_code,'NONE') = dpr.promo_code
    ON CONFLICT (order_item_id) DO NOTHING
    """,
    # fact_inventory
    """
    INSERT INTO warehouse.fact_inventory
        (snapshot_date_key, prd_sk, store_sk, inventory_id,
         qty_on_hand, qty_reserved, qty_available, reorder_point,
         inv_status, needs_reorder, stock_value)
    SELECT
        TO_CHAR(si.snapshot_date,'YYYYMMDD')::INTEGER,
        COALESCE(dp.prd_sk,  -1),
        COALESCE(ds.store_sk,-1),
        si.inventory_id::TEXT,
        si.qty_on_hand, si.qty_reserved, si.qty_available, si.reorder_point,
        si.inv_status, si.needs_reorder,
        si.qty_on_hand * COALESCE(dp.unit_cost, 0)
    FROM staging.inventory          si
    LEFT JOIN warehouse.dim_product dp ON si.prd_id::TEXT   = dp.prd_id   AND dp.is_current
    LEFT JOIN warehouse.dim_store   ds ON si.store_id::TEXT = ds.store_id
    ON CONFLICT DO NOTHING
    """,
]


# ---------------------------------------------------------------------------
# Status check
# ---------------------------------------------------------------------------

def show_status(cur):
    tables = [
        "raw.customers","raw.products","raw.stores","raw.promotions",
        "raw.orders","raw.order_items","raw.inventory",
        "staging.customers","staging.products","staging.orders",
        "staging.order_items","staging.inventory",
        "warehouse.dim_date","warehouse.dim_customer","warehouse.dim_product",
        "warehouse.dim_store","warehouse.dim_promotion",
        "warehouse.fact_sales","warehouse.fact_inventory",
    ]
    print(f"\n{'Table':<40} {'Rows':>8}")
    print("-" * 50)
    for t in tables:
        try:
            cur.execute(f"SELECT COUNT(*) FROM {t}")
            print(f"  {t:<38} {cur.fetchone()[0]:>8,}")
        except Exception as e:
            print(f"  {t:<38}  {str(e)[:30]}")
    print()


# ---------------------------------------------------------------------------
# Main load
# ---------------------------------------------------------------------------

def run_neon_load(transformed: dict, conn=None) -> dict:
    """
    Load transformed DataFrames into Neon raw + staging schemas,
    then run warehouse MERGE SQL.
    Returns {table: rows_loaded}.
    """
    close_conn = conn is None
    if conn is None:
        conn = get_neon_conn(direct=True)

    cur = conn.cursor()
    results = {}

    logger.info("=== NEON LOAD phase ===")

    # TRUNCATE with explicit existence check
    logger.info("  Truncating staging tables …")
    cur.execute("""
        SELECT table_name FROM information_schema.tables
        WHERE table_schema = 'staging'
    """)
    existing = {r[0] for r in cur.fetchall()}
    for tbl in TRUNCATE_STG:
        tbl_name = tbl.split(".")[-1]
        if tbl_name in existing:
            try:
                cur.execute(f"TRUNCATE TABLE {tbl} RESTART IDENTITY CASCADE")
            except Exception as e:
                conn.rollback()
                logger.warning("  TRUNCATE %s: %s", tbl, e)
        else:
            logger.debug("  %s not found — skip truncate", tbl)
    conn.commit()

    # 2 — Load RAW + STAGING
    for sf_table, df in transformed.items():
        pg_table = TABLE_MAP.get(sf_table)
        if not pg_table:
            continue

        logger.info("  Loading %-40s  %d rows …", pg_table, len(df))
        try:
            rows = df_to_postgres(cur, df, pg_table)
            conn.commit()
            results[pg_table] = rows
            logger.info("    → %d rows", rows)
        except Exception as e:
            conn.rollback()
            logger.error("  FAILED %s: %s", pg_table, e)
            results[pg_table] = -1

    # 3 — Warehouse merges
    logger.info("  Running warehouse merges …")
    for sql in WAREHOUSE_MERGES:
        label = sql.strip()[:60].replace("\n", " ")
        try:
            cur.execute(sql)
            logger.info("    [%d rows]  %s …", cur.rowcount, label[:55])
        except Exception as e:
            conn.rollback()
            logger.error("  MERGE FAILED: %s\n  SQL: %s", e, label)
        else:
            conn.commit()

    if close_conn:
        cur.close()
        conn.close()

    logger.info("Neon load complete.\n")
    return results


# ---------------------------------------------------------------------------
# Standalone entry point
# ---------------------------------------------------------------------------

def parse_args():
    import argparse
    p = argparse.ArgumentParser(description="Load transformed data into Neon DWH")
    p.add_argument("--full-load", action="store_true")
    p.add_argument("--status",    action="store_true")
    return p.parse_args()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-7s  %(message)s",
        datefmt="%H:%M:%S",
    )
    args = parse_args()
    conn = get_neon_conn(direct=True)
    cur  = conn.cursor()

    if args.status:
        show_status(cur)
        conn.close()
        sys.exit(0)

    # Load from latest Parquet staging files
    STAGING_DIR = PROJECT_DIR / "etl" / "staging"
    sys.path.insert(0, str(PROJECT_DIR / "etl"))
    from extract   import run_extract
    from transform import run_transform

    extracted   = run_extract(full_load=args.full_load)
    transformed = run_transform(extracted)
    run_neon_load(transformed, conn=conn)

    show_status(cur)
    conn.close()
