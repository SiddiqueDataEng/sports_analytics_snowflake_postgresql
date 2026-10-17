"""SportZone Data Engineering & Analytics Platform — Streamlit App
Run: python -m streamlit run dashboard/app.py
"""
import sys
from pathlib import Path
import streamlit as st

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(Path(__file__).parent))

st.set_page_config(
    page_title="SportZone Platform",
    page_icon="🏆",
    layout="wide",
    initial_sidebar_state="expanded",
)

from conn import default_neon_url, default_snowflake_params

if "neon_url"     not in st.session_state: st.session_state["neon_url"]     = default_neon_url()
if "sf_params"    not in st.session_state: st.session_state["sf_params"]    = default_snowflake_params()
if "openai_key"   not in st.session_state: st.session_state["openai_key"]   = ""
if "pipeline_log" not in st.session_state: st.session_state["pipeline_log"] = []

st.markdown("""
<style>
[data-testid="stSidebar"]{background:#0d1117}
.metric-card{background:#161b22;border-radius:10px;padding:16px 20px;
  border-left:4px solid #58a6ff;margin-bottom:8px}
.metric-card h3{color:#8b949e;font-size:.78rem;margin:0 0 4px;
  text-transform:uppercase;letter-spacing:.08em}
.metric-card h2{color:#f0f6fc;font-size:1.6rem;margin:0;font-weight:700}
.metric-card p{color:#3fb950;font-size:.76rem;margin:4px 0 0}
.tag{display:inline-block;padding:2px 8px;border-radius:12px;
  font-size:.72rem;font-weight:600;margin:2px}
.tag-green{background:#1a4731;color:#3fb950}
.tag-blue{background:#1c3553;color:#58a6ff}
.tag-red{background:#4d1919;color:#f85149}
.tag-yellow{background:#3d2e00;color:#d29922}
div[data-testid="stMetric"]{background:#161b22;border-radius:8px;padding:12px 16px}
</style>""", unsafe_allow_html=True)

with st.sidebar:
    st.markdown("## 🏆 SportZone")
    st.markdown("**Data Engineering Platform**")
    st.divider()
    page = st.radio("Navigation", [
        "🏗️ Architecture",
        "🔧 Management & ETL",
        "⚙️ dbt Transforms",
        "📊 Analytics Dashboard",
    ], label_visibility="collapsed")
    st.divider()

    # ── Snowflake MFA (TOTP) code ─────────────────────────────
    _c = st.session_state.get("sf_conn")
    connected = _c is not None and not _c.is_closed()
    has_code  = bool(st.session_state.get("sf_totp"))

    with st.expander("❄️ Snowflake MFA code", expanded=not connected):
        if connected:
            st.success("Connected to Snowflake.")
        elif has_code:
            st.success("Code applied. Now click Test Snowflake.")
        totp = st.text_input("Authenticator code (6 digits)", max_chars=6, key="totp_input")
        c1, c2 = st.columns(2)
        if c1.button("Apply", use_container_width=True):
            if totp.strip().isdigit() and len(totp.strip()) == 6:
                st.session_state["sf_totp"] = totp.strip()
                old = st.session_state.pop("sf_conn", None)   # force a fresh login
                try:
                    if old is not None:
                        old.close()
                except Exception:
                    pass
                st.rerun()
            else:
                st.error("Enter the 6-digit code.")
        if c2.button("Clear", use_container_width=True):
            st.session_state.pop("sf_totp", None)
            st.rerun()
    st.divider()

    neon_ok = "🟢" if st.session_state.get("neon_url") else "🔴"
    sf_ok   = "🟢" if connected else "🔴"
    ai_ok   = "🟢" if st.session_state.get("openai_key") else "🔴"
    st.markdown(f"{neon_ok} **Neon OLTP**  \n{sf_ok} **Snowflake DWH**  \n{ai_ok} **AI (OpenAI)**")
    st.divider()
    st.caption("v3.0 · Neon × Snowflake × dbt × OpenAI")

if page == "🏗️ Architecture":
    from pages import architecture
    architecture.render()
elif page == "🔧 Management & ETL":
    from pages import management
    management.render()
elif page == "⚙️ dbt Transforms":
    from pages import dbt_page
    dbt_page.render()
else:
    from pages import analytics
    analytics.render()
