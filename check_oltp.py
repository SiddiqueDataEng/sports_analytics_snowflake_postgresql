"""Quick OLTP status check — uses shared db.py connection helper."""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

from db import get_neon_conn

conn = get_neon_conn()
cur  = conn.cursor()

tables = [
    "oltp.category","oltp.brand","oltp.supplier","oltp.product","oltp.store",
    "oltp.promotion","oltp.customer","oltp.inventory",
    "oltp.order_header","oltp.order_item","oltp.payment","oltp.customer_event",
    "oltp.etl_watermark",
]

print(f"\n{'Table':<35} {'Rows':>8}")
print("-" * 45)
for t in tables:
    try:
        cur.execute(f"SELECT COUNT(*) FROM {t}")
        print(f"  {t:<33} {cur.fetchone()[0]:>8,}")
    except Exception as e:
        print(f"  {t:<33}  ERROR: {str(e)[:40]}")

print("\nETL Watermarks:")
try:
    cur.execute("SELECT table_name, last_extracted, rows_extracted FROM oltp.etl_watermark ORDER BY 1")
    for r in cur.fetchall():
        print(f"  {r[0]:<25} {str(r[1])[:19]}  rows={r[2]}")
except Exception as e:
    print(f"  {e}")

conn.close()
print()
