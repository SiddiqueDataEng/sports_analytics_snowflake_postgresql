"""Verification queries against Neon Postgres — uses shared db.py."""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from db import get_neon_conn

conn = get_neon_conn()
cur  = conn.cursor()

print("\n=== Sales by Category + Channel ===")
cur.execute("""
    SELECT p.prd_category, f.channel,
           SUM(f.qty_ordered)                        AS units,
           ROUND(SUM(f.net_revenue)::numeric,2)      AS revenue,
           ROUND(SUM(f.gross_profit)::numeric,2)     AS profit
    FROM warehouse.fact_sales  f
    JOIN warehouse.dim_product p ON f.prd_sk = p.prd_sk
    WHERE f.order_status NOT IN ('CANCELLED','RETURNED')
    GROUP BY 1,2 ORDER BY revenue DESC
""")
print(f"  {'Category':<15} {'Channel':<12} {'Units':>6} {'Revenue':>10} {'Profit':>10}")
print("  " + "-"*57)
for r in cur.fetchall():
    print(f"  {str(r[0]):<15} {str(r[1]):<12} {r[2]:>6} {r[3]:>10} {r[4]:>10}")

print("\n=== Customer 360 — Top 5 by Lifetime Value ===")
cur.execute("""
    SELECT cust_id, full_name, loyalty_tier,
           total_orders, lifetime_value, recency_segment
    FROM mart.customer_360
    ORDER BY lifetime_value DESC NULLS LAST
    LIMIT 5
""")
print(f"  {'ID':<6} {'Name':<22} {'Tier':<10} {'Orders':>6} {'LTV':>8} {'Segment':<10}")
print("  " + "-"*68)
for r in cur.fetchall():
    ltv = f"{r[4]:.2f}" if r[4] is not None else "0.00"
    print(f"  {str(r[0]):<6} {str(r[1]):<22} {str(r[2]):<10} {r[3] or 0:>6} {ltv:>8} {str(r[5] or 'N/A'):<10}")

print("\n=== Inventory — Items Needing Reorder ===")
cur.execute("""
    SELECT prd_name, brand, store_name, region,
           qty_on_hand, reorder_point, inv_status
    FROM mart.inventory_status
    WHERE needs_reorder = TRUE
    ORDER BY qty_on_hand LIMIT 10
""")
print(f"  {'Product':<25} {'Brand':<12} {'Store':<12} {'QOH':>5} {'ROP':>5}  Status")
print("  " + "-"*75)
for r in cur.fetchall():
    print(f"  {str(r[0]):<25} {str(r[1]):<12} {str(r[2]):<12} {r[4]:>5} {r[5]:>5}  {r[6]}")

print("\n=== dim_date sanity check ===")
cur.execute("SELECT MIN(full_date), MAX(full_date), COUNT(*) FROM warehouse.dim_date")
r = cur.fetchone()
print(f"  Date range: {r[0]} → {r[1]}  ({r[2]:,} days)\n")

conn.close()
