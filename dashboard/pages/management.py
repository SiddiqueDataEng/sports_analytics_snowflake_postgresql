"""
Management & ETL Page — full data engineering control panel.
Tabs: Connections | Generate Data | ETL Pipeline | Monitoring | Lineage | AI Settings
"""
import json, subprocess, sys, time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
import streamlit as st

ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(Path(__file__).parent.parent))
from conn import (default_neon_url, default_snowflake_params, mask_url,
                  mask_sf, test_neon, test_snowflake, neon_query, sf_query,
                  sf_live_conn)

BG = "#0d1117"; BG2 = "#161b22"; FONT = "#f0f6fc"; GRID = "#30363d"
PL = dict(paper_bgcolor=BG, plot_bgcolor=BG2, font=dict(color=FONT),
          margin=dict(l=8,r=8,t=36,b=8))

OLTP_TABLES = ["oltp.customer","oltp.order_header","oltp.order_item",
               "oltp.payment","oltp.inventory","oltp.customer_event",
               "oltp.product","oltp.store","oltp.promotion"]
SF_TABLES   = ["WAREHOUSE.DIM_CUSTOMER","WAREHOUSE.DIM_PRODUCT","WAREHOUSE.DIM_STORE",
               "WAREHOUSE.DIM_PROMOTION","WAREHOUSE.DIM_DATE",
               "WAREHOUSE.FACT_SALES","WAREHOUSE.FACT_INVENTORY"]

# ── helpers ───────────────────────────────────────────────────────────────────
def neon_counts(url=None):
    import psycopg
    url = url or st.session_state.get("neon_url", default_neon_url())
    try:
        conn = psycopg.connect(url, connect_timeout=15, autocommit=True)
        cur  = conn.cursor()
        out  = {}
        for t in OLTP_TABLES:
            try:
                cur.execute(f"SELECT COUNT(*) FROM {t}")
                out[t] = cur.fetchone()[0]
            except Exception:
                out[t] = 0
        conn.close()
        return out
    except Exception:
        return {t: 0 for t in OLTP_TABLES}

def sf_counts():
    """Row counts from the shared Snowflake connection.
    Never opens a new login (a TOTP code works only once) — it only uses the
    connection created by 'Test Snowflake'."""
    conn = sf_live_conn()
    if conn is None:
        return {t: 0 for t in SF_TABLES}
    out = {}
    cur = conn.cursor()
    try:
        cur.execute("USE DATABASE SPORTZONE_DEV")
        cur.execute("USE WAREHOUSE COMPUTE_WH")
        for t in SF_TABLES:
            try:
                cur.execute(f"SELECT COUNT(*) FROM {t}")
                out[t] = cur.fetchone()[0]
            except Exception:
                out[t] = 0
    except Exception:
        out = {t: 0 for t in SF_TABLES}
    finally:
        cur.close()
    return out

def bar_before_after(before, after, title):
    labels = [k.split(".")[-1] for k in before]
    b = list(before.values())
    new = [max(0, after.get(k,0)-before.get(k,0)) for k in before]
    fig = go.Figure()
    fig.add_bar(name="Existing", x=labels, y=b, marker_color="#1f6feb", opacity=.8)
    fig.add_bar(name="New", x=labels, y=new, marker_color="#3fb950", base=b)
    fig.update_layout(**PL, title=title, barmode="stack", height=300,
                      legend=dict(orientation="h",yanchor="bottom",y=1.02))
    fig.update_xaxes(tickangle=-25, showgrid=False)
    fig.update_yaxes(showgrid=True, gridcolor=GRID)
    st.plotly_chart(fig, use_container_width=True)

def load_pipeline_log():
    log_file = ROOT / "etl" / "pipeline_runs.log"
    if not log_file.exists():
        return pd.DataFrame()
    rows = []
    for line in log_file.read_text().strip().splitlines()[-30:]:
        try:
            e = json.loads(line)
            rows.append({
                "Timestamp":   e.get("ts","")[:19],
                "Status":      e.get("status",""),
                "Duration(s)": round(e.get("duration",0),1),
                "Tables":      ", ".join(e.get("tables",[]))[:60],
                "Rows":        sum(v for v in e.get("rows",{}).values() if isinstance(v,int) and v>0),
            })
        except Exception:
            pass
    return pd.DataFrame(rows)

# ── render ────────────────────────────────────────────────────────────────────
def render():
    st.markdown("## 🔧 Management & ETL Control Center")
    st.markdown("Full data engineering workspace: connections, data generation, ETL pipeline, monitoring, lineage, and AI.")

    tab_conn, tab_gen, tab_etl, tab_mon, tab_lin, tab_ai = st.tabs([
        "🔗 Connections",
        "⚡ Generate Data",
        "▶️ ETL Pipeline",
        "📈 Monitoring",
        "🔀 Data Lineage",
        "🤖 AI Settings",
    ])

    # ══════════════════════════════════════════════════
    # TAB 1 — Connections
    # ══════════════════════════════════════════════════
    with tab_conn:
        st.markdown("### 🐘 Source — Neon Postgres (OLTP)")
        c1, c2 = st.columns([3,1])
        with c1:
            use_def = st.checkbox("Use preconfigured connection", value=True, key="neon_use_def")
            if use_def:
                url = default_neon_url()
                st.session_state["neon_url"] = url
                st.success(f"**Preconfigured:** `{mask_url(url)}`")
            else:
                new_url = st.text_input("Custom Neon connection string",
                    placeholder="postgresql://user:pass@host/db?sslmode=require",
                    type="password", key="neon_custom")
                if new_url:
                    st.session_state["neon_url"] = new_url

            b1, b2 = st.columns(2)
            if b1.button("🔌 Test Neon Connection", key="btn_test_neon"):
                with st.spinner("Connecting …"):
                    ok, msg = test_neon(st.session_state.get("neon_url", default_neon_url()))
                (st.success if ok else st.error)(msg)

            if b2.button("📊 Show OLTP Schema", key="btn_neon_schema"):
                with st.spinner():
                    try:
                        df = neon_query("""
                            SELECT table_schema, table_name,
                                   pg_size_pretty(pg_total_relation_size(
                                       quote_ident(table_schema)||'.'||quote_ident(table_name)
                                   )) AS size
                            FROM information_schema.tables
                            WHERE table_schema IN ('oltp','raw','staging','warehouse','mart')
                            ORDER BY table_schema, table_name
                        """)
                        st.dataframe(df, use_container_width=True, hide_index=True)
                    except Exception as e:
                        st.error(str(e))

        with c2:
            url = st.session_state.get("neon_url","")
            if url:
                host = url.split("@")[-1].split("/")[0][:28] if "@" in url else "—"
                db   = url.split("/")[-1].split("?")[0] if "/" in url else "—"
                st.metric("Host", host)
                st.metric("Database", db)

        st.divider()
        st.markdown("### ❄️ Destination — Snowflake (DWH)")
        s1, s2 = st.columns([3,1])
        with s1:
            sf_use_def = st.checkbox("Use preconfigured connection", value=True, key="sf_use_def")
            if sf_use_def:
                sf = default_snowflake_params()
                st.session_state["sf_params"] = sf
                safe = mask_sf(sf)
                st.success(f"**Preconfigured:** account=`{safe.get('account','—')}` user=`{safe.get('user','—')}` db=`{safe.get('database','—')}`")
            else:
                sc1, sc2 = st.columns(2)
                with sc1:
                    sf_acc  = st.text_input("Account", placeholder="ur59759.me-central2.gcp", key="sf_acc_inp")
                    sf_user = st.text_input("User", key="sf_user_inp")
                    sf_pwd  = st.text_input("Password", type="password", key="sf_pwd_inp")
                    sf_pk   = st.text_input("Private key path (optional)", key="sf_pk_inp")
                with sc2:
                    sf_role = st.text_input("Role",      value="ACCOUNTADMIN",  key="sf_role_inp")
                    sf_wh   = st.text_input("Warehouse", value="COMPUTE_WH",    key="sf_wh_inp")
                    sf_db   = st.text_input("Database",  value="SPORTZONE_DEV", key="sf_db_inp")
                if sf_acc:
                    new_sf = {"account":sf_acc,"user":sf_user,"role":sf_role,
                              "warehouse":sf_wh,"database":sf_db,"schema":"RAW"}
                    if sf_pwd: new_sf["password"] = sf_pwd
                    if sf_pk and Path(sf_pk).exists():
                        from cryptography.hazmat.primitives.serialization import load_pem_private_key
                        from cryptography.hazmat.backends import default_backend
                        pem = Path(sf_pk).read_bytes()
                        new_sf["private_key"] = load_pem_private_key(pem, None, default_backend())
                        new_sf["authenticator"] = "snowflake"
                    st.session_state["sf_params"] = new_sf

            if sf_live_conn() is not None:
                st.caption("🟢 Snowflake session is open — queries reuse it, no new MFA code needed.")
            elif st.session_state.get("sf_totp"):
                st.caption("🟡 MFA code applied — click **Test Snowflake** now (codes expire in ~30 s).")
            else:
                st.caption("🔴 Not connected — enter the MFA code in the sidebar, click Apply, then Test Snowflake.")

            t1, t2 = st.columns(2)
            if t1.button("🔌 Test Snowflake", key="btn_test_sf"):
                with st.spinner("Connecting …"):
                    ok, msg = test_snowflake(st.session_state.get("sf_params", default_snowflake_params()))
                (st.success if ok else st.error)(msg)

            if t2.button("📋 Show DWH Objects", key="btn_sf_objs"):
                with st.spinner():
                    try:
                        df = sf_query("""
                            SELECT TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE,
                                   ROW_COUNT, BYTES/1024/1024 AS SIZE_MB
                            FROM INFORMATION_SCHEMA.TABLES
                            WHERE TABLE_SCHEMA IN ('RAW','STAGING','WAREHOUSE','MART')
                            ORDER BY TABLE_SCHEMA, TABLE_NAME
                        """)
                        st.dataframe(df, use_container_width=True, hide_index=True)
                    except Exception as e:
                        st.error(str(e))

        with s2:
            sf = st.session_state.get("sf_params", default_snowflake_params())
            st.metric("Account",   str(sf.get("account","—"))[:22])
            st.metric("Database",  sf.get("database","—"))
            st.metric("Warehouse", sf.get("warehouse","—"))
            st.metric("Auth",      "keypair" if "private_key" in sf else sf.get("authenticator","snowflake"))

    # ══════════════════════════════════════════════════
    # TAB 2 — Generate Data
    # ══════════════════════════════════════════════════
    with tab_gen:
        st.markdown("### ⚡ OLTP Data Generator — SportZone Retail")
        st.info("Generates realistic transactional data into Neon Postgres `oltp.*` schemas using Faker.", icon="ℹ️")

        g1, g2, g3, g4 = st.columns(4)
        n_cust   = g1.number_input("Customers",  0, 5000,  50, 50,  key="g_cust")
        n_orders = g2.number_input("Orders",     0,10000, 200, 100, key="g_ord")
        n_events = g3.number_input("Events",     0,20000, 500, 200, key="g_evt")
        start_dt = g4.date_input("Order start date", value=None, key="g_start")

        with st.expander("📊 Current OLTP counts", expanded=True):
            with st.spinner("Loading …"):
                cur_counts = neon_counts()
            cols = st.columns(4)
            for i,(t,v) in enumerate(cur_counts.items()):
                cols[i%4].metric(t.split(".")[-1].replace("_"," ").title(), f"{v:,}")

        if st.button("🚀 Generate Now", type="primary", key="btn_gen"):
            before = dict(cur_counts)
            progress = st.progress(0, "Starting …")
            log_out  = st.empty()
            cmd = [sys.executable, str(ROOT/"oltp"/"generator.py"),
                   "--customers", str(n_cust), "--orders", str(n_orders),
                   "--events", str(n_events),
                   "--start-date", str(start_dt) if start_dt else "2024-01-01"]
            try:
                proc = subprocess.Popen(cmd, cwd=str(ROOT),
                    stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
                lines, pct = [], 0
                for ln in proc.stdout:
                    ln = ln.strip()
                    if ln and len(ln) < 200:
                        lines.append(ln)
                        log_out.code("\n".join(lines[-15:]))
                        pct = min(pct+4, 95)
                        progress.progress(pct, "Generating …")
                proc.wait()
                progress.progress(100, "Done!")
                after = neon_counts()
                added = sum(max(0,after.get(t,0)-before.get(t,0)) for t in OLTP_TABLES)
                st.success(f"✅ Generated **{added:,} new rows** across all OLTP tables.")
                bar_before_after(before, after, "OLTP — Before vs After")
            except Exception as e:
                st.error(f"Generation
