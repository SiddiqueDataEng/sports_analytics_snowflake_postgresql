"""
conn.py — unified connection manager for the SportZone dashboard.
Works locally (.env / keypair) AND on Streamlit Cloud (st.secrets).
Uses psycopg v3 (psycopg package) for Python 3.14 compatibility.
"""
import re, sys
from pathlib import Path
import pandas as pd
import streamlit as st

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "deploy"))

# ── .env reader (local only) ──────────────────────────────────────────────────
def _read_env() -> str:
    p = ROOT / ".env"
    return p.read_text(encoding="utf-8") if p.exists() else ""

# ── Neon URL ──────────────────────────────────────────────────────────────────
def default_neon_url() -> str:
    # 1. Streamlit Cloud secrets
    try:
        v = st.secrets.get("neon", {}).get("connection_string", "")
        if v and "postgresql" in v:
            return v
    except Exception:
        pass
    # 2. Local .env
    text = _read_env()
    for key in ("NEON_CONNECTION_STRING", "DATABASE_URL", "DATABASE_URL_UNPOOLED"):
        m = re.search(rf'^{key}\s*=\s*"?([^"\n]+)"?', text, re.MULTILINE)
        if m:
            return m.group(1).strip()
    return ""

# ── Snowflake params ──────────────────────────────────────────────────────────
def default_snowflake_params() -> dict:
    # 1. Streamlit Cloud secrets
    try:
        sf = dict(st.secrets.get("snowflake", {}))
        if sf.get("account"):
            params = {
                "account":   sf["account"],
                "user":      sf["user"],
                "role":      sf.get("role", "ACCOUNTADMIN"),
                "warehouse": sf.get("warehouse", "COMPUTE_WH"),
                "database":  sf.get("database", "SPORTZONE_DEV"),
                "schema":    sf.get("schema", "WAREHOUSE"),
                "authenticator": "snowflake",
            }
            if sf.get("password"):
                params["password"] = sf["password"]
            elif sf.get("private_key"):
                from cryptography.hazmat.primitives.serialization import load_pem_private_key
                from cryptography.hazmat.backends import default_backend
                pem = sf["private_key"].encode()
                params["private_key"] = load_pem_private_key(pem, None, default_backend())
            return params
    except Exception:
        pass
    # 2. Local deploy/config.py
    try:
        from config import load_config
        return load_config()
    except Exception:
        return {}

# ── OpenAI key ────────────────────────────────────────────────────────────────
def default_openai_key() -> str:
    try:
        return st.secrets.get("openai", {}).get("api_key", "")
    except Exception:
        return ""

def mask_url(url: str) -> str:
    return re.sub(r'(:)[^@]+(@)', r'\1***\2', url) if url else ""

def mask_sf(params: dict) -> dict:
    return {k: "***" if k in ("password", "private_key", "passcode") else v
            for k, v in params.items()}

# ── Neon connection helpers ───────────────────────────────────────────────────
def get_neon_url() -> str:
    return st.session_state.get("neon_url") or default_neon_url()

def neon_conn():
    """psycopg v3 connection (Python 3.14 compatible)."""
    import psycopg
    url = get_neon_url()
    c = psycopg.connect(url, connect_timeout=20, autocommit=True)
    return c

def neon_query(sql: str, params=None) -> pd.DataFrame:
    with neon_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            cols = [d[0] for d in cur.description]
            return pd.DataFrame(cur.fetchall(), columns=cols)

def neon_execute(sql: str, params=None):
    import psycopg
    url = get_neon_url()
    with psycopg.connect(url, connect_timeout=30) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
        conn.commit()

def test_neon(url: str) -> tuple[bool, str]:
    try:
        import psycopg
        with psycopg.connect(url, connect_timeout=10) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT current_database(), current_user, version()")
                db, user, ver = cur.fetchone()
        return True, f"✅ db={db}, user={user}, pg={ver.split(',')[0]}"
    except Exception as e:
        return False, f"❌ {e}"

# ── Snowflake helpers (MFA-aware, single shared connection) ───────────────────
def _sf_params() -> dict:
    return st.session_state.get("sf_params") or default_snowflake_params()

def _sf_connect(params: dict | None = None):
    """
    Open ONE Snowflake connection and keep it in session_state.
    Adds the TOTP code applied in the sidebar (sf_params['passcode']).
    A TOTP code works only once, so it is removed after a successful login
    and every later call reuses the open connection.
    """
    import snowflake.connector

    p = {k: v for k, v in (params or _sf_params()).items() if v}

    session_params = st.session_state.get("sf_params") or {}
    code = session_params.get("passcode") or p.get("passcode")
    if code:
        p["passcode"] = str(code).strip()

    conn = snowflake.connector.connect(**p)

    # code is now used up: drop it so a reconnect doesn't reuse a stale code
    if isinstance(st.session_state.get("sf_params"), dict):
        st.session_state["sf_params"].pop("passcode", None)

    st.session_state["sf_conn"] = conn
    return conn

def get_sf_conn():
    """Return the open connection, or log in again if there isn't one."""
    conn = st.session_state.get("sf_conn")
    if conn is not None and not conn.is_closed():
        return conn
    return _sf_connect()

def _use_context(cur):
    cur.execute("USE DATABASE SPORTZONE_DEV")
    cur.execute("USE WAREHOUSE COMPUTE_WH")

def sf_conn():
    """Shared connection. Do not call .close() on it."""
    conn = get_sf_conn()
    _use_context(conn.cursor())
    return conn

@st.cache_data(ttl=60, show_spinner=False)
def sf_query(sql: str) -> pd.DataFrame:
    import snowflake.connector
    conn = get_sf_conn()
    cur  = conn.cursor(snowflake.connector.DictCursor)
    try:
        _use_context(cur)
        cur.execute(sql)
        rows = cur.fetchall()
    finally:
        cur.close()
    return pd.DataFrame(rows) if rows else pd.DataFrame()

def sf_execute(sql: str):
    conn = get_sf_conn()
    cur  = conn.cursor()
    try:
        _use_context(cur)
        cur.execute(sql)
    finally:
        cur.close()

def test_snowflake(params: dict) -> tuple[bool, str]:
    try:
        conn = _sf_connect(params)      # keeps the connection for later use
        cur  = conn.cursor()
        cur.execute("SELECT CURRENT_DATABASE(), CURRENT_WAREHOUSE(), CURRENT_ROLE()")
        db, wh, role = cur.fetchone()
        cur.close()
        return True, f"✅ db={db}, wh={wh}, role={role}"
    except Exception as e:
        return False, f"❌ {e}"

# ── OpenAI ────────────────────────────────────────────────────────────────────
def get_openai_key() -> str:
    return st.session_state.get("openai_key") or default_openai_key()

def ask_openai(prompt: str, system: str = "") -> str:
    key = get_openai_key()
    if not key:
        return "⚠️ No OpenAI API key. Add it in Management → AI Settings or Streamlit Cloud Secrets."
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
        return "⚠️ openai package not installed."
    except Exception as e:
        return f"❌ OpenAI error: {e}"
