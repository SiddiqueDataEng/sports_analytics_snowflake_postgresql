"""
extract.py
==========
Incremental extraction from Neon Postgres OLTP → local Parquet staging files.
Uses the etl_watermark table to track last-extracted timestamp per table.

Every extracted file is written as a gzip-compressed Parquet file to:
    etl/staging/<table_name>/<YYYYMMDD_HHMMSS>.parquet.gz

These files are then picked up by load.py to push into Snowflake.
"""

import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import psycopg2.extras

# Ensure project root is on sys.path before importing db
PROJECT_DIR = Path(__file__).parent.parent
STAGING_DIR = PROJECT_DIR / "etl" / "staging"
sys.path.insert(0, str(PROJECT_DIR))
sys.path.insert(0, str(PROJECT_DIR / "deploy"))

from db import get_neon_conn as _get_neon_conn

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Source queries — extract only changed rows since watermark
# ---------------------------------------------------------------------------

EXTRACT_QUERIES = {
    "customers": """
        SELECT
            c.customer_id,
            c.external_id,
            c.first_name,
            c.last_name,
            c.first_name || ' ' || c.last_name   AS full_name,
            c.email,
            c.phone,
            c.date_of_birth,
            c.gender,
            c.address_line1,
            c.address_line2,
            c.city,
            c.state,
            c.zip_code,
            c.country,
            c.loyalty_tier,
            c.loyalty_points,
            c.signup_date,
            c.is_active,
            c.created_at,
            c.updated_at
        FROM oltp.customer c
        WHERE c.updated_at > %(watermark)s
        ORDER BY c.updated_at
    """,

    "products": """
        SELECT
            p.product_id,
            p.sku,
            p.name                                AS product_name,
            cat.name                              AS category,
            parent_cat.name                       AS parent_category,
            b.name                                AS brand,
            s.name                                AS supplier_name,
            p.unit_cost,
            p.unit_price,
            ROUND((p.unit_price - p.unit_cost) / NULLIF(p.unit_price,0) * 100, 2)
                                                  AS gross_margin_pct,
            p.weight_kg,
            p.is_active,
            p.launch_date,
            p.discontinue_dt,
            p.created_at,
            p.updated_at
        FROM oltp.product     p
        JOIN oltp.category    cat         ON p.category_id    = cat.category_id
        LEFT JOIN oltp.category parent_cat ON cat.parent_id   = parent_cat.category_id
        JOIN oltp.brand       b            ON p.brand_id      = b.brand_id
        JOIN oltp.supplier    s            ON p.supplier_id   = s.supplier_id
        WHERE p.updated_at > %(watermark)s
        ORDER BY p.updated_at
    """,

    "stores": """
        SELECT
            store_id, code, name AS store_name, store_type, region,
            address, city, state, zip_code, country,
            open_date, manager_name, is_active, created_at
        FROM oltp.store
        WHERE created_at > %(watermark)s
        ORDER BY created_at
    """,

    "promotions": """
        SELECT
            promotion_id, code AS promo_code, name AS promo_name,
            promo_type, discount_value, min_order_amt, max_uses,
            uses_count, start_date, end_date, is_active, created_at
        FROM oltp.promotion
        WHERE created_at > %(watermark)s
        ORDER BY created_at
    """,

    "orders": """
        SELECT
            o.order_id,
            o.order_ref,
            o.customer_id,
            o.store_id,
            o.promotion_id,
            o.channel,
            o.order_status,
            o.order_date,
            o.shipped_date,
            o.delivered_date,
            o.subtotal_amt,
            o.discount_amt,
            o.tax_amt,
            o.shipping_amt,
            o.total_amt,
            o.currency,
            o.created_at,
            o.updated_at
        FROM oltp.order_header o
        WHERE o.updated_at > %(watermark)s
        ORDER BY o.updated_at
    """,

    "order_items": """
        SELECT
            oi.order_item_id,
            oi.order_id,
            oi.product_id,
            p.sku                                 AS product_sku,
            oi.qty,
            oi.unit_price,
            oi.unit_cost,
            oi.discount_pct,
            oi.line_subtotal,
            oi.line_discount,
            oi.line_total,
            oi.created_at
        FROM oltp.order_item oi
        JOIN oltp.product    p  ON oi.product_id = p.product_id
        WHERE oi.created_at > %(watermark)s
        ORDER BY oi.created_at
    """,

    "payments": """
        SELECT
            payment_id, order_id, payment_method, payment_status,
            amount, currency, transaction_ref, processed_at
        FROM oltp.payment
        WHERE processed_at > %(watermark)s
        ORDER BY processed_at
    """,

    "inventory": """
        SELECT
            i.inventory_id,
            i.product_id,
            p.sku                                 AS product_sku,
            i.store_id,
            s.code                                AS store_code,
            i.qty_on_hand,
            i.qty_reserved,
            i.qty_on_hand - i.qty_reserved        AS qty_available,
            i.reorder_point,
            i.qty_on_hand <= i.reorder_point      AS needs_reorder,
            CASE
                WHEN i.qty_on_hand = 0            THEN 'OUT_OF_STOCK'
                WHEN i.qty_on_hand <= i.reorder_point THEN 'LIMITED'
                ELSE 'STOCKED'
            END                                   AS inv_status,
            i.last_updated                        AS updated_at
        FROM oltp.inventory   i
        JOIN oltp.product     p ON i.product_id = p.product_id
        JOIN oltp.store       s ON i.store_id   = s.store_id
        WHERE i.last_updated > %(watermark)s
        ORDER BY i.last_updated
    """,

    "customer_events": """
        SELECT
            event_id, customer_id, session_id, event_type,
            product_id, event_data::TEXT AS event_data_json, occurred_at
        FROM oltp.customer_event
        WHERE occurred_at > %(watermark)s
        ORDER BY occurred_at
    """,
}

# Map extract key → watermark table key
WATERMARK_KEYS = {
    "customers":      "customer",
    "products":       "order_item",    # products don't have watermark row; use created_at
    "stores":         "order_item",
    "promotions":     "order_item",
    "orders":         "order_header",
    "order_items":    "order_item",
    "payments":       "payment",
    "inventory":      "inventory",
    "customer_events":"customer_event",
}


def get_neon_conn_impl():
    """ETL uses direct/unpooled connection for bulk queries."""
    return _get_neon_conn(direct=True)


def get_watermark(cur, key: str) -> datetime:
    cur.execute(
        "SELECT last_extracted FROM oltp.etl_watermark WHERE table_name = %s",
        (key,)
    )
    row = cur.fetchone()
    if row:
        return row[0]
    return datetime(1970, 1, 1, tzinfo=timezone.utc)


def update_watermark(cur, key: str, ts: datetime, rows: int):
    cur.execute("""
        INSERT INTO oltp.etl_watermark (table_name, last_extracted, last_run_at, rows_extracted)
        VALUES (%s, %s, NOW(), %s)
        ON CONFLICT (table_name) DO UPDATE
        SET last_extracted = EXCLUDED.last_extracted,
            last_run_at    = NOW(),
            rows_extracted = EXCLUDED.rows_extracted
    """, (key, ts, rows))


def extract_table(cur, name: str, watermark: datetime) -> pd.DataFrame:
    """Run extract query and return a DataFrame — uses cursor.fetchall() not pd.read_sql."""
    sql = EXTRACT_QUERIES[name]
    cur.execute(sql, {"watermark": watermark})
    rows = cur.fetchall()
    if not rows:
        return pd.DataFrame()
    cols = [d[0] for d in cur.description]
    return pd.DataFrame(rows, columns=cols)


def run_extract(tables: list[str] = None, full_load: bool = False) -> dict[str, Path]:
    """
    Extract changed data from OLTP into Parquet files.
    Returns dict of {table_name: parquet_path}.
    """
    conn = get_neon_conn_impl()
    cur  = conn.cursor()

    run_ts  = datetime.now(tz=timezone.utc)
    ts_str  = run_ts.strftime("%Y%m%d_%H%M%S")
    targets = tables or list(EXTRACT_QUERIES.keys())
    outputs = {}

    logger.info("=== EXTRACT phase  [%s] ===", ts_str)

    for name in targets:
        wm_key    = WATERMARK_KEYS.get(name, name)
        watermark = datetime(1970,1,1, tzinfo=timezone.utc) if full_load else get_watermark(cur, wm_key)

        logger.info("  %-20s  watermark=%s", name, watermark.strftime("%Y-%m-%d %H:%M:%S"))

        df = extract_table(cur, name, watermark)

        if df.empty:
            logger.info("  %-20s  no new rows.", name)
            continue

        # Write Parquet
        out_dir  = STAGING_DIR / name
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / f"{ts_str}.parquet"
        df.to_parquet(out_path, index=False, compression="gzip")

        logger.info("  %-20s  %d rows → %s", name, len(df), out_path.name)

        # Update watermark to max ts in this batch
        ts_col = None
        for col in ("updated_at","processed_at","occurred_at","created_at"):
            if col in df.columns:
                ts_col = col
                break
        if ts_col:
            new_wm = pd.to_datetime(df[ts_col]).max()
            update_watermark(cur, wm_key, new_wm.to_pydatetime(), len(df))

        outputs[name] = out_path

    conn.commit()
    cur.close()
    conn.close()

    logger.info("Extract complete: %d tables with new data.\n", len(outputs))
    return outputs
