"""
test_snowflake.py
=================
Tests Snowflake connection (keypair auth — no MFA needed) and creates
the SPORTZONE_DEV database with all schemas.

Prerequisites:
  Run in Snowflake SQL Editor first:
    ALTER USER SIDDIQUE SET RSA_PUBLIC_KEY='MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEArKOGDUxoj9gqEAOD41+YIkjou+UdxrujgWfgozA2Jb7N8PB8b884o0FAxXq3bLAvN70DC1BQXXioCll7G67w41UUMvlU8gqXeGs4nqHFoKJ7ghgIr/wRrmI1VdV+gK+5TRzp24OjpjWf8L0e12+1y56v4zeJ5qfagu3nFDNK8Kqm8X7z2TLlbSTfye9Ek53ujHr9zQUCfouHpwDnwEUpRsNiwx4D1+hRG9PT2YKdKxMdENnt5HrrfBKexQeeDhrmv9tWmvlKqE8F+eNm6Tb0h0NoMERr+KmFBQj94ePCh3s7MwdTdXucAoMrshzFpa0DZ+MaVlo5XW8yHZ5otwTX6QIDAQAB';
"""
import sys
from pathlib import Path

ROOT = Path(__file__).parent
sys.path.insert(0, str(ROOT / "deploy"))

from config import load_config

params = load_config()
auth_type = "keypair" if "private_key" in params else params.get("authenticator", "password")
safe = {k: "***" if k in ("password", "private_key") else v for k, v in params.items()}
print(f"Auth method : {auth_type}")
print(f"Params      : {safe}")

import snowflake.connector

print("\nConnecting to Snowflake ...")
try:
    conn = snowflake.connector.connect(**params)
    cur  = conn.cursor()

    cur.execute("SELECT CURRENT_ACCOUNT(), CURRENT_USER(), CURRENT_ROLE(), CURRENT_WAREHOUSE()")
    acct, user, role, wh = cur.fetchone()
    print(f"\nConnected!")
    print(f"  Account  : {acct}")
    print(f"  User     : {user}")
    print(f"  Role     : {role}")
    print(f"  Warehouse: {wh}")

    # Create SPORTZONE_DEV database and all DWH schemas
    print("\nCreating SPORTZONE_DEV database + schemas ...")
    for sql in [
        "CREATE DATABASE IF NOT EXISTS SPORTZONE_DEV COMMENT = 'SportZone Retail DWH'",
        "USE DATABASE SPORTZONE_DEV",
        "CREATE SCHEMA IF NOT EXISTS RAW",
        "CREATE SCHEMA IF NOT EXISTS STAGING",
        "CREATE SCHEMA IF NOT EXISTS WAREHOUSE",
        "CREATE SCHEMA IF NOT EXISTS MART",
        "CREATE SCHEMA IF NOT EXISTS REPORTING",
    ]:
        cur.execute(sql)
        print(f"  OK  {sql[:70]}")

    # Verify
    cur.execute("SHOW SCHEMAS IN DATABASE SPORTZONE_DEV")
    schemas = sorted(r[1] for r in cur.fetchall())
    print(f"\nSchemas: {schemas}")

    # Check if COMPUTE_WH exists, create if not
    cur.execute("SHOW WAREHOUSES LIKE 'COMPUTE_WH'")
    wh_rows = cur.fetchall()
    if not wh_rows:
        print("\nCreating COMPUTE_WH warehouse ...")
        cur.execute("""
            CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH
            WAREHOUSE_SIZE = 'X-SMALL'
            AUTO_SUSPEND = 60
            AUTO_RESUME = TRUE
        """)
        print("  OK  COMPUTE_WH created")
    else:
        print(f"\nWarehouse COMPUTE_WH: {wh_rows[0][1]} (status: {wh_rows[0][2]})")

    conn.close()
    print("\nSnowflake DWH setup complete! Ready for ETL.")

except snowflake.connector.errors.DatabaseError as e:
    msg = str(e)
    if "RSA_PUBLIC_KEY" in msg or "JWT" in msg or "key" in msg.lower():
        print(f"\nKeypair auth failed: {e}")
        print("\nDid you run the ALTER USER command in Snowflake?")
        print("Run in Snowflake SQL Editor:")
        print("  ALTER USER SIDDIQUE SET RSA_PUBLIC_KEY='MIIBIjANBgkqhkiG9w0BAQE...'")
    elif "MFA" in msg or "TOTP" in msg:
        print(f"\nMFA required: {e}")
        print("Keypair not registered yet — run ALTER USER first.")
    else:
        print(f"\nDatabase error: {e}")
    sys.exit(1)
except Exception as e:
    print(f"\nError: {type(e).__name__}: {e}")
    sys.exit(1)
