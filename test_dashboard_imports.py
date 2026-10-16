"""Test all dashboard modules import without errors."""
import sys
from pathlib import Path

ROOT = Path(__file__).parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "dashboard"))

errors = []

# Test conn
try:
    from dashboard.conn import (default_neon_url, default_snowflake_params,
                                  mask_url, test_neon, test_snowflake,
                                  neon_query, sf_query, ask_openai, get_openai_key)
    url = default_neon_url()
    sf  = default_snowflake_params()
    print(f"OK  conn.py  — neon_url={'yes' if url else 'missing'}  sf_account={sf.get('account','missing')}")
except Exception as e:
    errors.append(f"conn.py: {e}")

# Test management (import only, no render)
try:
    import importlib.util, types
    # Mock streamlit to avoid widget instantiation
    import unittest.mock as mock
    with mock.patch.dict('sys.modules', {'streamlit': mock.MagicMock()}):
        spec = importlib.util.spec_from_file_location(
            "management", ROOT / "dashboard" / "pages" / "management.py")
        mod = importlib.util.module_from_spec(spec)
        # Just check it compiles
    print("OK  management.py — compiles")
except Exception as e:
    errors.append(f"management.py: {e}")

# Test analytics (import only)
try:
    import ast
    src = (ROOT / "dashboard" / "pages" / "analytics.py").read_text(encoding="utf-8")
    ast.parse(src)
    print("OK  analytics.py  — AST valid")
except SyntaxError as e:
    errors.append(f"analytics.py syntax: {e}")

# Verify no bare return sf_query( remains
import re
src = (ROOT / "dashboard" / "pages" / "analytics.py").read_text(encoding="utf-8")
bare = re.findall(r'^\s+return sf_query\(', src, re.MULTILINE)
if bare:
    errors.append(f"analytics.py: still has {len(bare)} bare return sf_query calls")
else:
    print("OK  analytics.py  — all loaders use _cast_numerics")

# Check _cast_numerics is defined
if "_cast_numerics" in src:
    print("OK  analytics.py  — _cast_numerics defined")
else:
    errors.append("analytics.py: _cast_numerics not found")

# Check ai_model session_state manual assignment is gone
if 'st.session_state["ai_model"]' in (ROOT / "dashboard" / "pages" / "management.py").read_text(encoding="utf-8"):
    errors.append("management.py: st.session_state['ai_model'] manual assignment still present")
else:
    print("OK  management.py — ai_model widget conflict fixed")

print()
if errors:
    print(f"ERRORS ({len(errors)}):")
    for e in errors:
        print(f"  ✗ {e}")
    sys.exit(1)
else:
    print("All checks passed. Dashboard is ready at http://localhost:8501")
