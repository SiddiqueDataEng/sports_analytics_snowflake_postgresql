"""Reset Neon DWH tables for a clean reload."""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))
from db import get_neon_conn

conn = get_neon_conn(direct=True)
cur  = conn.cursor()

truncate = [
    "warehouse.fact_sales", "warehouse.fact_inventory",
    "warehouse.dim_customer", "warehouse.dim_product",
    "warehouse.dim_store", "warehouse.dim_promotion",
    "raw.customers", "raw.products", "raw.stores", "raw.promotions",
    "raw.orders", "raw.order_items", "raw.inventory",
    "staging.customers", "staging.products", "staging.promotions",
    "staging.orders", "staging.order_items", "staging.inventory",
]

for t in truncate:
    try:
        cur.execute(f"TRUNCATE TABLE {t} RESTART IDENTITY CASCADE")
        print(f"  TRUNCATED {t}")
    except Exception as e:
        conn.rollback()
        print(f"  SKIP      {t}  ({str(e)[:50]})")

# Unknown members
cur.execute("""
    INSERT INTO warehouse.dim_customer (cust_sk,cust_id,full_name,eff_start_dt,is_current)
    VALUES (-1,'UNKNOWN','Unknown Customer','1900-01-01',TRUE)
    ON CONFLICT DO NOTHING
""")
cur.execute("""
    INSERT INTO warehouse.dim_product (prd_sk,prd_id,prd_name,eff_start_dt,is_current)
    VALUES (-1,'UNKNOWN','Unknown Product','1900-01-01',TRUE)
    ON CONFLICT DO NOTHING
""")
cur.execute("""
    INSERT INTO warehouse.dim_store (store_sk,store_id,store_name)
    VALUES (-1,'UNKNOWN','Unknown Store')
    ON CONFLICT DO NOTHING
""")
cur.execute("""
    INSERT INTO warehouse.dim_promotion (promo_sk,promo_id,promo_code,promo_name)
    VALUES (-1,'NONE','NONE','No Promotion')
    ON CONFLICT DO NOTHING
""")

conn.commit()
conn.close()
print("Reset complete.")
