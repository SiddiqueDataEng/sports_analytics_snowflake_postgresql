"""Verify Snowflake DWH data and mart views."""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent / "deploy"))
from config import load_config
import snowflake.connector

params = load_config()
conn = snowflake.connector.connect(**params)
cur  = conn.cursor()
cur.execute("USE DATABASE SPORTZONE_DEV")
cur.execute("USE WAREHOUSE COMPUTE_WH")

print("=== Snowflake DWH — Row Counts ===")
tables = [
    "WAREHOUSE.DIM_DATE", "WAREHOUSE.DIM_CUSTOMER", "WAREHOUSE.DIM_PRODUCT",
    "WAREHOUSE.DIM_STORE", "WAREHOUSE.DIM_PROMOTION",
    "WAREHOUSE.FACT_SALES", "WAREHOUSE.FACT_INVENTORY",
]
for t in tables:
    cur.execute(f"SELECT COUNT(*) FROM {t}")
    print(f"  {t:<40} {cur.fetchone()[0]:>8,}")

print("\n=== Mart Views ===")

print("\nSales by Category:")
cur.execute("""
    SELECT PRD_CATEGORY, SUM(NET_REVENUE) AS REV, SUM(UNITS_SOLD) AS UNITS
    FROM MART.DAILY_SALES_SUMMARY
    GROUP BY PRD_CATEGORY ORDER BY REV DESC
""")
for r in cur.fetchall():
    print(f"  {str(r[0]):<15}  ${float(r[1]):>10,.0f}  {r[2]:>5} units")

print("\nTop 5 Customers by LTV:")
cur.execute("""
    SELECT FULL_NAME, LOYALTY_TIER, TOTAL_ORDERS, LIFETIME_VALUE, RECENCY_SEGMENT
    FROM MART.CUSTOMER_360
    ORDER BY LIFETIME_VALUE DESC NULLS LAST
    LIMIT 5
""")
for r in cur.fetchall():
    print(f"  {str(r[0]):<25} {str(r[1]):<10} {r[2]:>3} orders  ${float(r[3] or 0):>8,.0f}  {r[4]}")

print("\nInventory needing reorder:")
cur.execute("""
    SELECT PRD_NAME, BRAND, STORE_NAME, QTY_ON_HAND, REORDER_POINT, INV_STATUS
    FROM MART.INVENTORY_STATUS
    WHERE NEEDS_REORDER = TRUE
    ORDER BY QTY_ON_HAND LIMIT 5
""")
for r in cur.fetchall():
    print(f"  {str(r[0]):<30} {str(r[1]):<12} QOH={r[3]} ROP={r[4]} {r[5]}")

conn.close()
print("\nSnowflake DWH verified OK!")
