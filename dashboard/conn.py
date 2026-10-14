"""
conn.py — unified connection manager for the SportZone dashboard.
Handles Neon Postgres (OLTP) and Snowflake (DWH).
"""
import re, sys
from pathlib import Path
import pandas as pd
import streamlit as st

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "deploy"))

# ── .env helpers ──────────────────────────────────────────────────────────────
def _read_env() -> str:
    p = ROOT / ".env"
    return p.read_text(encoding="utf-8") if p.exists() else ""

def default_neon_url() -> str:
    text = _read_env()
    for key in ("NEON_CONNECTION_STRING", "DATABASE_URL", "DATABASE_URL_UNPOOLED"):
        m = re.search(rf'^{key}\s*=\s*"?([^"\n]+)"?', text, re.MULTILINE)
        if m:
            return m.group(1).strip()
    return ""

def default_snowflake_params() -> dict:
    try:
        from config import load_config
        return load_config()
    except Exception:
        return {}

def mask_url(url: str) -> str:
    return re.sub(r'(:)[^@]+(@)', r'\1***\2', url) if url else ""

def mask_sf(params: dict) -> dict:
    return {k: "***" if k in ("password","private_key") else v for k, v in params.items()}

# ── Neon ──────────────────────────────────────────────────────────────────────
def get_neon_url() -> str:
    return st.session_state.get("neon_url", default_neon_url())

def neon_conn():
    import psycopg2
    url = get_neon_url()
    c = psycopg2.connect(url, connect_timeout=20)
    c.autocommit = True
    return c

def neon_query(sql: str, params=None) -> pd.DataFrame:
    conn = neon_conn()
    try:
        cur = conn.cursor()
        cur.execute(sql, params)
        cols = [d[0] for d in cur.description]
        return pd.DataFrame(cur.fetchall(), columns=cols)
    finally:
        conn.close()

def neon_execute(sql: str, params=None):
    import psycopg2
    url = get_neon_url()
    conn = psycopg2.connect(url, connect_timeout=30)
    conn.autocommit = False
    try:
        cur = conn.cursor()
        cur.execute(sql, params)
        conn.commit()
    finally:
        conn.close()

def test_neon(url: str) -> tuple[bool, str]:
    try:
        import psycopg2
        conn = psycopg2.connect(url, connect_timeout=10)
        cur  = conn.cursor()
        cur.execute("SELECT current_database(), current_user, version()")
        db, user, ver = cur.fetchone()
        conn.close()
        return True, f"✅ db={db}, user={user}, pg={ver.split(',')[0]}"
    except Exception as e:
        return False, f"❌ {e}"

# ── Snowflake ─────────────────────────────────────────────────────────────────
def sf_conn():
    import snowflake.connector
    params = st.session_state.get("sf_params", default_snowflake_params())
    conn = snowflake.connector.connect(**params)
    conn.cursor().execute("USE DATABASE SPORTZONE_DEV")
    conn.cursor().execute("USE WAREHOUSE COMPUTE_WH")
    return conn

@st.cache_data(ttl=60, show_spinner=False)
def sf_query(sql: str) -> pd.DataFrame:
    import snowflake.connector
    params = st.session_state.get("sf_params", default_snowflake_params())
    conn   = snowflake.connector.connect(**params)
    cur    = conn.cursor(snowflake.connector.DictCursor)
    cur.execute("USE DATABASE SPORTZONE_DEV")
    cur.execute("USE WAREHOUSE COMPUTE_WH")
    cur.execute(sql)
    rows = cur.fetchall()
    conn.close()
    return pd.DataFrame(rows) if rows else pd.DataFrame()

def sf_execute(sql: str):
    import snowflake.connector
    params = st.session_state.get("sf_params", default_snowflake_params())
    conn   = snowflake.connector.connect(**params)
    cur    = conn.cursor()
    cur.execute("USE DATABASE SPORTZONE_DEV")
    cur.execute("USE WAREHOUSE COMPUTE_WH")
    cur.execute(sql)
    conn.close()

def test_snowflake(params: dict) -> tuple[bool, str]:
    try:
        import snowflake.connector
        conn = snowflake.connector.connect(**{k:v for k,v in params.items() if v})
        cur  = conn.cursor()
        cur.execute("SELECT CURRENT_DATABASE(), CURRENT_WAREHOUSE(), CURRENT_ROLE()")
        db, wh, role = cur.fetchone()
        conn.close()
        return True, f"✅ db={db}, wh={wh}, role={role}"
    except Exception as e:
        return False, f"❌ {e}"

# ── OpenAI ────────────────────────────────────────────────────────────────────
def get_openai_key() -> str:
    return st.session_state.get("openai_key", "")

def ask_openai(prompt: str, system: str = "") -> str:
    key = get_openai_key()
    if not key:
        return "⚠️ No OpenAI API key configured. Set it in Management → AI Settings."
    try:
        from openai import OpenAI
        client = OpenAI(api_key=key)
        msgs = []
        if system:
            msgs.append({"role": "system", "content": system})
        msgs.append({"role": "user", "content": prompt})
        resp = client.chat.completions.create(
            model="gpt-4o-mini", messages=msgs, max_tokens=2000, temperature=0.1
        )
        return resp.choices[0].message.content.strip()
    except ImportError:
        return "⚠️ openai package not installed. Run: pip install openai"
    except Exception as e:
        return f"❌ OpenAI error: {e}"
