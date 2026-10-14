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
                  mask_sf, test_neon, test_snowflake, neon_query, sf_query)

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
    import psycopg2
    url = url or st.session_state.get("neon_url", default_neon_url())
    try:
        conn = psycopg2.connect(url, connect_timeout=15)
        conn.autocommit = True
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
    import snowflake.connector
    params = st.session_state.get("sf_params", default_snowflake_params())
    try:
        conn = snowflake.connector.connect(**params)
        cur  = conn.cursor()
        cur.execute("USE DATABASE SPORTZONE_DEV")
        cur.execute("USE WAREHOUSE COMPUTE_WH")
        out  = {}
        for t in SF_TABLES:
            cur.execute(f"SELECT COUNT(*) FROM {t}")
            out[t] = cur.fetchone()[0]
        conn.close()
        return out
    except Exception:
        return {t: 0 for t in SF_TABLES}

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
                st.error(f"Generation failed: {e}")
            finally:
                progress.empty(); log_out.empty()

    # ══════════════════════════════════════════════════
    # TAB 3 — ETL Pipeline
    # ══════════════════════════════════════════════════
    with tab_etl:
        st.markdown("### ▶️ ETL Pipeline — Neon OLTP → Snowflake DWH")

        col_opts, col_stats = st.columns([1, 2])

        with col_opts:
            st.markdown("#### Pipeline Options")
            load_mode = st.radio("Load Mode", [
                "🔄 Incremental (watermark)",
                "⬇️ Full Reload",
                "🪟 Window/Snapshot",
            ], key="etl_mode")

            st.divider()
            dry_run = st.checkbox("🧪 Dry-run (no write)", value=False, key="etl_dry")

            if "🪟" in load_mode:
                wstart = st.date_input("Window Start", key="w_start")
                wend   = st.date_input("Window End",   key="w_end")

            st.divider()
            st.markdown("##### Idempotency")
            st.markdown("""
- **Incremental**: uses `etl_watermark` table to only process new rows since last run
- **Full Reload**: truncates staging, reloads all data from scratch
- **Window**: extracts only rows within the specified date range
- All fact loads use `ON CONFLICT DO NOTHING` — safe to re-run
""")

        with col_stats:
            st.markdown("#### Current DWH State (Snowflake)")
            with st.spinner("Reading Snowflake …"):
                sf_c = sf_counts()
            c1,c2,c3 = st.columns(3)
            items = list(sf_c.items())
            for i,(t,v) in enumerate(items):
                [c1,c2,c3][i%3].metric(t.split(".")[-1], f"{v:,}")

        st.divider()

        if st.button("▶️ Run ETL Pipeline", type="primary", key="btn_run_etl"):
            sf_before = dict(sf_c)
            progress  = st.progress(0, "Initialising …")
            log_area  = st.empty()
            logs      = []

            cmd = [sys.executable, str(ROOT/"etl"/"pipeline.py")]
            if "Full" in load_mode:    cmd.append("--full-load")
            if dry_run:                cmd.append("--dry-run")

            try:
                proc = subprocess.Popen(cmd, cwd=str(ROOT),
                    stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
                pct = 0
                for ln in proc.stdout:
                    ln = ln.strip()
                    if ln and not ln.startswith("C:\\") and len(ln) < 300:
                        logs.append(ln)
                        log_area.code("\n".join(logs[-30:]))
                        pct = min(pct+2, 95)
                        progress.progress(pct, "Running …")
                proc.wait()
                progress.progress(100, "Complete!")
                sf_after = sf_counts()
                added = sum(max(0,sf_after.get(t,0)-sf_before.get(t,0)) for t in SF_TABLES)
                if dry_run:
                    st.info("Dry-run complete — no data written.")
                elif added > 0:
                    st.success(f"✅ ETL complete — **{added:,} new rows** in Snowflake DWH.")
                    bar_before_after(sf_before, sf_after, "Snowflake DWH — Before vs After ETL")
                else:
                    st.success("✅ ETL complete — DWH already up to date.")
            except Exception as e:
                st.error(f"ETL failed: {e}")
            finally:
                progress.empty()

        st.divider()
        st.markdown("#### ⏰ Watermark State")
        try:
            wm_df = neon_query("""
                SELECT table_name, last_extracted, last_run_at, rows_extracted
                FROM oltp.etl_watermark ORDER BY table_name
            """)
            if not wm_df.empty:
                st.dataframe(wm_df, use_container_width=True, hide_index=True)
        except Exception as e:
            st.warning(f"Could not load watermarks: {e}")

    # ══════════════════════════════════════════════════
    # TAB 4 — Monitoring
    # ══════════════════════════════════════════════════
    with tab_mon:
        st.markdown("### 📈 Pipeline Health & Monitoring")

        log_df = load_pipeline_log()

        if log_df.empty:
            st.info("No pipeline runs recorded yet. Run the ETL pipeline first.")
        else:
            # Summary KPIs
            k1,k2,k3,k4 = st.columns(4)
            k1.metric("Total Runs",    len(log_df))
            k2.metric("Success Rate",  f"{(log_df['Status']=='OK').mean()*100:.0f}%")
            k3.metric("Avg Duration",  f"{log_df['Duration(s)'].mean():.1f}s")
            k4.metric("Total Rows ETL'd", f"{log_df['Rows'].sum():,}")

            # Timeline chart
            fig = go.Figure()
            colors = {"OK":"#3fb950","FAILED":"#f85149","DRY-RUN":"#d29922","NOOP":"#8b949e"}
            for status, grp in log_df.groupby("Status"):
                fig.add_scatter(x=grp["Timestamp"], y=grp["Duration(s)"],
                                mode="markers+lines", name=status,
                                marker=dict(color=colors.get(status,"#58a6ff"), size=10),
                                line=dict(width=1.5))
            fig.update_layout(**PL, title="Pipeline Run History — Duration over Time",
                              height=300, xaxis_title="", yaxis_title="Duration (s)")
            st.plotly_chart(fig, use_container_width=True)

            # Log table
            st.dataframe(log_df.iloc[::-1], use_container_width=True, hide_index=True)

        st.divider()
        st.markdown("### ❄️ Snowflake Query History (last 10)")
        if st.button("🔍 Fetch SF Query History", key="btn_sf_hist"):
            with st.spinner():
                try:
                    df = sf_query("""
                        SELECT QUERY_TEXT, EXECUTION_STATUS,
                               ROUND(TOTAL_ELAPSED_TIME/1000,2) AS ELAPSED_SEC,
                               BYTES_SCANNED/1024/1024 AS MB_SCANNED,
                               START_TIME, USER_NAME
                        FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
                        WHERE START_TIME >= DATEADD(HOUR,-24,CURRENT_TIMESTAMP())
                          AND QUERY_TEXT NOT LIKE '%ACCOUNT_USAGE%'
                        ORDER BY START_TIME DESC
                        LIMIT 10
                    """)
                    if df.empty:
                        st.info("No recent queries found (may take up to 45 min to appear in ACCOUNT_USAGE).")
                    else:
                        st.dataframe(df, use_container_width=True, hide_index=True)
                except Exception as e:
                    st.error(str(e))

        st.markdown("### 🐘 Neon Live Stats")
        if st.button("📊 Fetch Neon Stats", key="btn_neon_stats"):
            with st.spinner():
                try:
                    df = neon_query("""
                        SELECT schemaname, tablename,
                               n_live_tup AS live_rows,
                               n_dead_tup AS dead_rows,
                               last_autovacuum, last_autoanalyze
                        FROM pg_stat_user_tables
                        WHERE schemaname IN ('oltp','raw','staging','warehouse')
                        ORDER BY live_rows DESC
                    """)
                    st.dataframe(df, use_container_width=True, hide_index=True)
                except Exception as e:
                    st.error(str(e))

    # ══════════════════════════════════════════════════
    # TAB 5 — Data Lineage
    # ══════════════════════════════════════════════════
    with tab_lin:
        st.markdown("### 🔀 Data Lineage — Visual Flow Diagram")

        # ── Plotly Sankey / node diagram ──────────────────────────────────────
        # Nodes: each layer + key tables
        node_labels = [
            # OLTP (0-8)
            "Neon OLTP\nsportzone",
            "oltp.customer", "oltp.order_header", "oltp.order_item",
            "oltp.product",  "oltp.inventory",    "oltp.promotion",
            # ETL (7-8)
            "extract.py\n(watermark)", "transform.py\n(cleanse+cast)",
            # RAW (9-15)
            "RAW.RAW_CUSTOMERS", "RAW.RAW_ORDERS", "RAW.RAW_ORDER_ITEMS",
            "RAW.RAW_PRODUCTS",  "RAW.RAW_INVENTORY", "RAW.RAW_PROMOTIONS",
            # STAGING (15-20)
            "STG_CUSTOMERS", "STG_ORDERS", "STG_ORDER_ITEMS",
            "STG_PRODUCTS",  "STG_INVENTORY",
            # WAREHOUSE (20-26)
            "DIM_CUSTOMER", "DIM_PRODUCT", "DIM_STORE",
            "DIM_PROMOTION", "FACT_SALES", "FACT_INVENTORY",
            # MART (26-29)
            "MART.DAILY_SALES", "MART.CUSTOMER_360", "MART.INVENTORY_STATUS",
        ]

        # Colours per layer
        node_colors = (
            ["#388bfd"] +                    # Neon DB
            ["#1f6feb"] * 6 +               # OLTP tables
            ["#d29922"] * 2 +               # ETL steps
            ["#da3633"] * 6 +               # RAW
            ["#f0883e"] * 5 +               # STAGING
            ["#3fb950"] * 6 +               # WAREHOUSE
            ["#bc8cff"] * 3                 # MART
        )

        # Edges: (source_idx, target_idx, value, label)
        edges = [
            # OLTP → extract
            (1,7,3,"customer"), (2,7,5,"orders"), (3,7,5,"items"),
            (4,7,2,"products"), (5,7,2,"inventory"), (6,7,1,"promotions"),
            # extract → transform
            (7,8,18,"parquet files"),
            # transform → RAW
            (8,9,3,"customers"), (8,10,5,"orders"), (8,11,5,"items"),
            (8,12,2,"products"), (8,13,2,"inventory"), (8,14,1,"promotions"),
            # RAW → STAGING (cleanse)
            (9,15,3,"typed"), (10,16,5,"deduped"), (11,17,5,"enriched"),
            (12,18,2,"margin%"), (13,19,2,"available"),
            # STAGING → WAREHOUSE dims
            (15,20,3,"SCD2"), (18,21,2,"SCD2"),
            (16,22,1,"upsert"), (16,23,1,"upsert"),
            # STAGING → WAREHOUSE facts
            (16,24,5,"MERGE"), (17,24,5,"MERGE"),
            (19,25,2,"MERGE"),
            # WAREHOUSE → MART
            (24,26,5,"views"), (20,27,3,"views"), (25,28,2,"views"),
        ]

        src   = [e[0] for e in edges]
        tgt   = [e[1] for e in edges]
        vals  = [e[2] for e in edges]
        elabels = [e[3] for e in edges]

        edge_colors = []
        for s in src:
            if s <= 6:   edge_colors.append("rgba(31,111,235,0.4)")   # OLTP blue
            elif s <= 8: edge_colors.append("rgba(210,153,34,0.5)")   # ETL amber
            elif s <= 14:edge_colors.append("rgba(218,54,51,0.4)")    # RAW red
            elif s <= 19:edge_colors.append("rgba(240,136,62,0.4)")   # STG orange
            else:        edge_colors.append("rgba(63,185,80,0.4)")    # WH green

        fig_sankey = go.Figure(go.Sankey(
            arrangement="snap",
            node=dict(
                pad=20, thickness=22, line=dict(color="#30363d", width=0.5),
                label=node_labels,
                color=node_colors,
                hovertemplate="%{label}<extra></extra>",
            ),
            link=dict(
                source=src, target=tgt, value=vals,
                label=elabels,
                color=edge_colors,
                hovertemplate="%{label}: %{value} tables<extra></extra>",
            ),
        ))
        fig_sankey.update_layout(
            title_text="SportZone Data Lineage — Medallion Architecture Flow",
            font=dict(size=11, color="#f0f6fc"),
            paper_bgcolor="#0d1117",
            height=560,
            margin=dict(l=10, r=10, t=48, b=10),
        )
        st.plotly_chart(fig_sankey, use_container_width=True)

        # ── Layer legend ──────────────────────────────────────────────────────
        st.markdown("""
<div style="display:flex;gap:16px;flex-wrap:wrap;margin-top:4px">
  <span style="background:#1f6feb;padding:4px 12px;border-radius:12px;font-size:.8rem">🟦 Neon OLTP Source</span>
  <span style="background:#d29922;padding:4px 12px;border-radius:12px;font-size:.8rem">🟨 ETL Pipeline</span>
  <span style="background:#da3633;padding:4px 12px;border-radius:12px;font-size:.8rem">🟥 RAW Layer (Bronze)</span>
  <span style="background:#f0883e;padding:4px 12px;border-radius:12px;font-size:.8rem">🟧 Staging Layer (Silver)</span>
  <span style="background:#3fb950;padding:4px 12px;border-radius:12px;font-size:.8rem">🟩 Warehouse Layer (Gold)</span>
  <span style="background:#bc8cff;padding:4px 12px;border-radius:12px;font-size:.8rem">🟪 Mart Layer (Platinum)</span>
</div>
""", unsafe_allow_html=True)

        st.divider()
        st.markdown("#### 🔬 Column-Level Lineage — WAREHOUSE.FACT_SALES")
        lineage_rows = [
            ("ORDER_DATE_KEY",  "oltp.order_header.order_date",  "TO_NUMBER(TO_CHAR(date,'YYYYMMDD'))", "DIM_DATE join key"),
            ("CUST_SK",         "oltp.customer.customer_id",      "DIM_CUSTOMER lookup",               "SCD2 surrogate key"),
            ("PRD_SK",          "oltp.product.product_id",        "DIM_PRODUCT lookup",                "SCD2 surrogate key"),
            ("STORE_SK",        "oltp.store.store_id",            "DIM_STORE lookup",                  "Type-1 surrogate key"),
            ("PROMO_SK",        "oltp.promotion.code",            "DIM_PROMOTION lookup",              "NULL → -1 (no promo)"),
            ("QTY_ORDERED",     "oltp.order_item.qty",            "CAST INTEGER",                      "Direct copy"),
            ("UNIT_PRICE",      "oltp.order_item.unit_price",     "CAST NUMERIC",                      "At time of sale"),
            ("UNIT_COST",       "oltp.product.unit_cost",         "DIM_PRODUCT.UNIT_COST",             "From current dim row"),
            ("GROSS_REVENUE",   "Derived",                        "qty × unit_price",                  "Computed in transform"),
            ("NET_REVENUE",     "oltp.order_item.line_total",     "CAST NUMERIC",                      "After discount"),
            ("COGS",            "Derived",                        "qty × unit_cost",                   "Computed in transform"),
            ("GROSS_PROFIT",    "Derived",                        "net_revenue - cogs",                "Computed in transform"),
            ("DISCOUNT_AMT",    "oltp.order_item.line_discount",  "CAST NUMERIC",                      "From OLTP"),
            ("GROSS_MARGIN_PCT","Derived",                        "(net_revenue-cogs)/net_revenue×100", "Computed in transform"),
        ]
        col_df = pd.DataFrame(lineage_rows,
                              columns=["Target Column","Source","Transformation","Notes"])
        st.dataframe(col_df, use_container_width=True, hide_index=True)

        st.divider()
        st.markdown("#### ⏱️ ETL Watermarks — Current State")
        try:
            wm = neon_query("""
                SELECT table_name AS "Table",
                       last_extracted AS "Last Extracted",
                       rows_extracted AS "Rows",
                       last_run_at AS "Last Run"
                FROM oltp.etl_watermark ORDER BY table_name
            """)
            if not wm.empty:
                st.dataframe(wm, use_container_width=True, hide_index=True)
        except Exception as e:
            st.warning(str(e))

    # ══════════════════════════════════════════════════
    # TAB 6 — AI Settings
    # ══════════════════════════════════════════════════
    with tab_ai:
        st.markdown("### 🤖 AI Configuration")
        st.info("Configure OpenAI API to enable: NL→SQL, AI insights, automated storytelling, and intelligent data exploration.", icon="🔑")

        current_key = st.session_state.get("openai_key","")
        new_key = st.text_input(
            "OpenAI API Key",
            value=current_key,
            type="password",
            placeholder="sk-...",
            help="Your OpenAI API key — stored in session only, never written to disk",
            key="ai_key_inp"
        )
        if new_key != current_key:
            st.session_state["openai_key"] = new_key

        model_choice = st.selectbox("Model", ["gpt-4o-mini","gpt-4o","gpt-4-turbo"], key="ai_model")

        if st.button("🔌 Test OpenAI Connection", key="btn_test_ai"):
            key = st.session_state.get("openai_key","")
            if not key:
                st.error("No API key set.")
            else:
                with st.spinner("Testing …"):
                    try:
                        from openai import OpenAI
                        client = OpenAI(api_key=key)
                        r = client.chat.completions.create(
                            model=model_choice,
                            messages=[{"role":"user","content":"Say: AI connected to SportZone!"}],
                            max_tokens=20
                        )
                        st.success(f"✅ {r.choices[0].message.content.strip()}")
                    except ImportError:
                        st.error("Run: pip install openai")
                    except Exception as e:
                        st.error(str(e))

        st.divider()
        st.markdown("#### AI Capabilities (once key is set)")
        st.markdown("""
| Feature | Description |
|---------|------------|
| **NL → SQL** | Convert plain English to Snowflake SQL |
| **Auto-insights** | AI-generated business insights from KPIs |
| **Chart Storytelling** | Narrative explanations of every chart |
| **Anomaly Narration** | AI explains unusual patterns |
| **SQL Validation** | AI reviews generated SQL for correctness |
| **Question Suggestions** | AI recommends follow-up questions |
""")
