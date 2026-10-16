"""
db.py  —  shared Neon Postgres connection helper
=================================================
Single source of truth for the connection string.
Import this from any module — no circular deps, no module-level connections.

Priority for connection string (reads .env in project root):
  1. NEON_CONNECTION_STRING
  2. DATABASE_URL
  3. DATABASE_URL_UNPOOLED
"""

import re
from pathlib import Path

_ENV_PATH = Path(__file__).parent / ".env"


def _read_env() -> str:
    return _ENV_PATH.read_text(encoding="utf-8") if _ENV_PATH.exists() else ""


def get_neon_url(direct: bool = False) -> str:
    """
    Parse .env and return the Neon connection URL (no network call).
    direct=True → prefer DATABASE_URL_UNPOOLED (better for ETL/long queries).
    direct=False → prefer NEON_CONNECTION_STRING / DATABASE_URL (pooler).
    """
    text = _read_env()

    if direct:
        # For ETL: prefer direct/unpooled connection
        for key in ("DATABASE_URL_UNPOOLED", "NEON_CONNECTION_STRING", "DATABASE_URL"):
            m = re.search(rf'^{key}\s*=\s*"?([^"\n]+)"?', text, re.MULTILINE)
            if m:
                return m.group(1).strip()
    else:
        # For short ops: prefer pooler
        for key in ("NEON_CONNECTION_STRING", "DATABASE_URL", "DATABASE_URL_UNPOOLED"):
            m = re.search(rf'^{key}\s*=\s*"?([^"\n]+)"?', text, re.MULTILINE)
            if m:
                return m.group(1).strip()

    raise ValueError(
        "No Neon URL found in .env. "
        "Set NEON_CONNECTION_STRING, DATABASE_URL, or DATABASE_URL_UNPOOLED."
    )


def get_neon_conn(direct: bool = False, **kwargs):
    """
    Open and return a psycopg connection.
    direct=True uses the unpooled URL — recommended for ETL/bulk operations.
    direct=False uses the pooler URL — for short queries.
    """
    import psycopg
    url  = get_neon_url(direct=direct)
    opts = {"connect_timeout": 30}
    opts.update(kwargs)
    conn = psycopg.connect(url, **opts)
    conn.autocommit = False
    return conn


if __name__ == "__main__":
    url = get_neon_url()
    safe = re.sub(r'(:)[^@]+(@)', r'\1***\2', url)
    print(f"URL  : {safe}")
    conn = get_neon_conn()
    cur  = conn.cursor()
    cur.execute("SELECT version()")
    print(f"DB   : {cur.fetchone()[0][:70]}")
    conn.close()
    print("OK")
