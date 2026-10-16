"""Verify Neon connection database and list schemas."""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))
from db import get_neon_conn

conn = get_neon_conn()
cur  = conn.cursor()

cur.execute("SELECT current_database(), current_user")
db, user = cur.fetchone()
print(f"Database : {db}")
print(f"User     : {user}")

cur.execute("""
    SELECT schema_name FROM information_schema.schemata
    WHERE schema_name NOT LIKE 'pg_%'
    AND schema_name != 'information_schema'
    ORDER BY 1
""")
schemas = [r[0] for r in cur.fetchall()]
print(f"Schemas  : {schemas}")

for schema in schemas:
    cur.execute("""
        SELECT table_name FROM information_schema.tables
        WHERE table_schema = %s ORDER BY 1
    """, (schema,))
    tables = [r[0] for r in cur.fetchall()]
    if tables:
        print(f"  {schema}: {tables}")
    else:
        print(f"  {schema}: (empty)")

conn.close()
