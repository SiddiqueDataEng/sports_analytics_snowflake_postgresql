"""
config.py — Snowflake connection params from .env
Supports:
  - password auth     (authenticator = snowflake + password)
  - keypair auth      (private_key_path = /path/to/rsa_key.p8)   ← preferred (no MFA)
  - externalbrowser   (SSO, opens browser)
"""
import re
from pathlib import Path

ENV_PATH = Path(__file__).parent.parent / ".env"
CONNECTION_SECTION = "connections.my_example_connection"


def load_config() -> dict:
    if not ENV_PATH.exists():
        raise FileNotFoundError(f".env not found at {ENV_PATH}")

    text = ENV_PATH.read_text(encoding="utf-8")

    # Find INI section block (handles dots in section name)
    m = re.search(
        r'^\[' + re.escape(CONNECTION_SECTION) + r'\](.*?)(?=^\[|\Z)',
        text, re.MULTILINE | re.DOTALL
    )
    if not m:
        raise KeyError(f"Section [{CONNECTION_SECTION}] not found in .env")

    cfg = {}
    for line in m.group(1).splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        kv = re.match(r'^(\w+)\s*=\s*"?([^"\n]*)"?', line)
        if kv:
            cfg[kv.group(1).lower()] = kv.group(2).strip().strip('"').strip("'")

    account = cfg.get("account", "").replace(".snowflakecomputing.com", "")
    auth    = cfg.get("authenticator", "snowflake")
    pwd     = cfg.get("password", "").strip()
    pk_path = cfg.get("private_key_path", "").strip()

    params = {
        "account":       account,
        "user":          cfg.get("user", ""),
        "role":          cfg.get("role", "ACCOUNTADMIN"),
        "warehouse":     cfg.get("warehouse") or None,
        "database":      cfg.get("database") or None,
        "schema":        cfg.get("schema") or None,
    }

    # ── Keypair auth (preferred — no MFA/TOTP needed) ──────────────────────
    if pk_path and Path(pk_path).exists():
        from cryptography.hazmat.primitives.serialization import load_pem_private_key
        from cryptography.hazmat.backends import default_backend
        pem = Path(pk_path).read_bytes()
        private_key = load_pem_private_key(pem, password=None, backend=default_backend())
        params["private_key"] = private_key
        params["authenticator"] = "snowflake"
        # Do NOT pass password when using keypair

    # ── Password auth (fallback) ───────────────────────────────────────────
    elif auth == "externalbrowser":
        params["authenticator"] = "externalbrowser"

    elif pwd:
        params["authenticator"] = "snowflake"
        params["password"]      = pwd

    # Drop None / empty values
    params = {k: v for k, v in params.items() if v not in (None, "", "<none selected>")}
    return params


if __name__ == "__main__":
    import json
    p = load_config()
    # Don't print the actual key object
    safe = {k: ("***" if k in ("password", "private_key") else v)
            for k, v in p.items()}
    print(json.dumps(safe, indent=2))
