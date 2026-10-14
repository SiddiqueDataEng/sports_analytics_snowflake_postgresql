"""Add unique constraints to warehouse dims and fix dim_store/promotion loading."""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))
from db import get_neon_conn

conn = get_neon_conn(direct=True)
cur  = conn.cursor()

# Add constraints needed for ON CONFLICT upserts
constraints = [
    "ALTER TABLE warehouse.dim_store ADD CONSTRAINT uq_store_id UNIQUE (store_id)",
    "ALTER TABLE warehouse.dim_promotion ADD CONSTRAINT uq_promo_code UNIQUE (promo_code)",
]
for sql in constraints:
    try:
        cur.execute(sql)
        print("OK:", sql[:60])
    except Exception as e:
        conn.rollback()
        print("SKIP:", str(e)[:60])

# Load dim_store from raw.stores
cur.execute("""
    INSERT INTO warehouse.dim_store
        (store_id, store_name, store_type, region, city, state, country, open_date, manager_name)
    SELECT DISTINCT ON (store_id)
        store_id, store_name, UPPER(store_type), UPPER(region),
        INITCAP(city), UPPER(state), UPPER(country),
        CASE WHEN open_date ~ '^\d{4}-\d{2}-\d{2}$' THEN open_date::DATE ELSE NULL END,
        manager_name
    FROM raw.stores
    WHERE store_id IS NOT NULL
    ON CONFLICT (store_id) DO UPDATE SET manager_name = EXCLUDED.manager_name
""")
print(f"dim_store: {cur.rowcount} rows upserted")

# Load dim_promotion from raw.promotions
cur.execute("""
    INSERT INTO warehouse.dim_promotion
        (promo_id, promo_code, promo_name, promo_type,
         discount_value, start_date, end_date, min_order_amt, is_active)
    SELECT DISTINCT ON (promo_code)
        promo_id, UPPER(promo_code), promo_name, UPPER(promo_type),
        NULLIF(discount_value,'')::NUMERIC,
        start_date::DATE, end_date::DATE,
        NULLIF(min_order_amt,'')::NUMERIC,
        UPPER(TRIM(is_active)) IN ('Y','1','TRUE','t')
    FROM raw.promotions
    WHERE promo_code IS NOT NULL
    ON CONFLICT (promo_code) DO UPDATE SET is_active = EXCLUDED.is_active
""")
print(f"dim_promotion: {cur.rowcount} rows upserted")

# Re-run fact_sales to pick up correct store/promo FKs
cur.execute("""
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
    JOIN staging.orders              o   ON oi.order_id::TEXT = o.order_id::TEXT
    LEFT JOIN warehouse.dim_customer dc  ON o.cust_id::TEXT   = dc.cust_id AND dc.is_current
    LEFT JOIN warehouse.dim_product  dp  ON oi.prd_id::TEXT   = dp.prd_id  AND dp.is_current
    LEFT JOIN warehouse.dim_store    ds  ON o.store_id::TEXT  = ds.store_id
    LEFT JOIN warehouse.dim_promotion dpr ON COALESCE(o.promo_code,'NONE') = dpr.promo_code
    ON CONFLICT (order_item_id) DO NOTHING
""")
print(f"fact_sales: {cur.rowcount} new rows inserted")

conn.commit()

# Final counts
print("\nFinal DWH counts:")
for t in ["warehouse.dim_customer","warehouse.dim_product","warehouse.dim_store",
          "warehouse.dim_promotion","warehouse.fact_sales","warehouse.fact_inventory"]:
    cur.execute(f"SELECT COUNT(*) FROM {t}")
    print(f"  {t:<40} {cur.fetchone()[0]:>8,}")

conn.close()
