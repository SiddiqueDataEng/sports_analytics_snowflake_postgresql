"""Verify all data the dashboard needs is available."""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))
from db import get_neon_conn

conn = get_neon_conn()
cur  = conn.cursor()

print("=== Mart Views ===")
cur.execute("""
    SELECT table_name FROM information_schema.views
    WHERE table_schema = 'mart' ORDER BY 1
""")
views = [r[0] for r in cur.fetchall()]
print("  ", views or "NONE — will create")

print("\n=== DWH Tables ===")
for t in ["warehouse.dim_customer","warehouse.dim_product","warehouse.dim_store",
          "warehouse.dim_promotion","warehouse.dim_date",
          "warehouse.fact_sales","warehouse.fact_inventory"]:
    cur.execute(f"SELECT COUNT(*) FROM {t}")
    print(f"  {t:<40} {cur.fetchone()[0]:>6,}")

print("\n=== Sales by Category ===")
cur.execute("""
    SELECT p.prd_category, SUM(f.net_revenue)::numeric AS rev,
           COUNT(DISTINCT f.order_id) AS orders
    FROM warehouse.fact_sales f
    JOIN warehouse.dim_product p ON f.prd_sk = p.prd_sk AND p.is_current
    WHERE f.order_status NOT IN ('CANCELLED','RETURNED')
    GROUP BY p.prd_category ORDER BY rev DESC
""")
for r in cur.fetchall():
    print(f"  {str(r[0]):<15}  ${float(r[1]):>10,.0f}  {r[2]:>4} orders")

print("\n=== Top 3 Customers ===")
cur.execute("""
    SELECT c.full_name, c.loyalty_tier,
           COUNT(DISTINCT f.order_id) AS orders,
           SUM(f.net_revenue)::numeric AS ltv
    FROM warehouse.dim_customer c
    JOIN warehouse.fact_sales f ON c.cust_sk = f.cust_sk
        AND f.order_status NOT IN ('CANCELLED','RETURNED')
    WHERE c.is_current AND c.cust_sk <> -1
    GROUP BY c.full_name, c.loyalty_tier
    ORDER BY ltv DESC NULLS LAST
    LIMIT 3
""")
for r in cur.fetchall():
    print(f"  {str(r[0]):<25} {str(r[1]):<10} {r[2]:>3} orders  ${float(r[3]):>8,.0f}")

# Create mart views if missing
if not views:
    print("\n=== Creating Mart Views ===")
    cur.execute("""
        CREATE SCHEMA IF NOT EXISTS mart;
        CREATE OR REPLACE VIEW mart.daily_sales_summary AS
        SELECT d.full_date, d.year, d.month_name, d.quarter_name, d.day_name, d.is_weekend,
               f.channel, p.prd_category, p.prd_subcategory, p.brand, s.region, s.country,
               COUNT(DISTINCT f.order_id) AS order_count,
               SUM(f.qty_ordered) AS units_sold,
               SUM(f.gross_revenue) AS gross_revenue,
               SUM(f.discount_amt) AS total_discount,
               SUM(f.net_revenue) AS net_revenue,
               SUM(f.cogs) AS cogs,
               SUM(f.gross_profit) AS gross_profit,
               ROUND(SUM(f.gross_profit)/NULLIF(SUM(f.net_revenue),0)*100,2) AS gross_margin_pct,
               ROUND(SUM(f.net_revenue)/NULLIF(COUNT(DISTINCT f.order_id),0),2) AS avg_order_value
        FROM warehouse.fact_sales f
        JOIN warehouse.dim_date d ON f.order_date_key = d.date_key
        JOIN warehouse.dim_product p ON f.prd_sk = p.prd_sk
        JOIN warehouse.dim_store s ON f.store_sk = s.store_sk
        WHERE f.order_status NOT IN ('CANCELLED','RETURNED')
        GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12
    """)
    conn.commit()
    print("  mart.daily_sales_summary created")

conn.close()
print("\nDashboard data: OK")
print("Open browser: http://localhost:8501")
