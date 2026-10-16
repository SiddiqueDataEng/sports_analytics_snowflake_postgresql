"""Final system check — Neon OLTP + Snowflake DWH + dashboard files."""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))
sys.path.insert(0, str(Path(__file__).parent / "deploy"))

print("=" * 58)
print("SportZone Platform — Final System Check")
print("=" * 58)

# ── Neon OLTP ─────────────────────────────────────────────
print("\n1. Neon Postgres OLTP (sportzone db)")
from db import get_neon_conn
conn = get_neon_conn()
cur  = conn.cursor()
oltp = ["oltp.customer","oltp.order_header","oltp.order_item",
        "oltp.payment","oltp.inventory","oltp.customer_event"]
for t in oltp:
    cur.execute(f"SELECT COUNT(*) FROM {t}")
    print(f"   {t:<30} {cur.fetchone()[0]:>6,}")
conn.close()

# ── Snowflake DWH ─────────────────────────────────────────
print("\n2. Snowflake DWH (SPORTZONE_DEV)")
from config import load_config
import snowflake.connector
p = load_config()
sf = snowflake.connector.connect(**p)
cur2 = sf.cursor()
cur2.execute("USE DATABASE SPORTZONE_DEV")
cur2.execute("USE WAREHOUSE COMPUTE_WH")
dwh = ["WAREHOUSE.DIM_DATE","WAREHOUSE.DIM_CUSTOMER","WAREHOUSE.DIM_PRODUCT",
       "WAREHOUSE.DIM_STORE","WAREHOUSE.DIM_PROMOTION",
       "WAREHOUSE.FACT_SALES","WAREHOUSE.FACT_INVENTORY"]
for t in dwh:
    cur2.execute(f"SELECT COUNT(*) FROM {t}")
    print(f"   {t:<35} {cur2.fetchone()[0]:>6,}")

# ── Mart views ────────────────────────────────────────────
print("\n3. Mart Views")
cur2.execute("SELECT SUM(NET_REVENUE), SUM(UNITS_SOLD), COUNT(*) FROM MART.DAILY_SALES_SUMMARY")
rev, units, rows = cur2.fetchone()
print(f"   MART.DAILY_SALES_SUMMARY  rev=${float(rev):,.0f}  units={units}  rows={rows}")
cur2.execute("SELECT COUNT(*) FROM MART.CUSTOMER_360")
print(f"   MART.CUSTOMER_360         customers={cur2.fetchone()[0]}")
cur2.execute("SELECT COUNT(*) FROM MART.INVENTORY_STATUS")
print(f"   MART.INVENTORY_STATUS     sku_locs={cur2.fetchone()[0]}")
sf.close()

# ── Dashboard files ───────────────────────────────────────
print("\n4. Dashboard Files")
files = ["dashboard/app.py","dashboard/conn.py",
         "dashboard/pages/management.py","dashboard/pages/analytics.py"]
for f in files:
    size = (Path(__file__).parent / f).stat().st_size
    print(f"   {f:<45} {size:>7,} bytes")

print("\n" + "=" * 58)
print("All systems OK. Dashboard at http://localhost:8501")
print("=" * 58)
