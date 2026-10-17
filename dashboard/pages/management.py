"""
Management & ETL Page — full data engineering control panel.
Tabs: Connections | Generate Data | ETL Pipeline | Monitoring | Lineage | AI Settings
"""
import json, subprocess, sys, time
from datetime import datetime, date
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
          margin=dict(l=8, r=8, t=36, b=8))

OLTP_TABLES = ["oltp.customer", "oltp.order_header", "oltp.order_item",
               "oltp.payment", "oltp.inventory", "oltp.customer_event",
               "oltp.product", "oltp.store", "oltp.promotion"]
SF_TABLES   = ["WAREHOUSE.DIM_CUSTOMER", "WAREHOUSE.DIM_PRODUCT", "WAREHOUSE.DIM_STORE",
               "WAREHOUSE.DIM_PROMOTION", "WAREHOUSE.DIM_DATE",
               "WAREHOUSE.FACT_SALES", "WAREHOUSE.FACT_INVENTORY"]

# ── presets ───────────────────────────────────────────────────────────────────
PRESETS = {
    "🔬 Small / Dev":   dict(customers=50,   orders=200,  events=300),
    "📊 Medium / Test": dict(customers=200,  orders=1000, events=2000),
    "🏭 Large / Prod":  dict(customers=1000, orders=5000, events=8000),
    "⚡ Quick smoke":   dict(customers=10,   orders=50,   events=100),
    "🎯 Custom":        dict(customers=None, orders=None, events=None),
}

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


def bar_chart(before, after, title, height=280):
    labels = [k.split(".")[-1] for k in before]
    b      = list(before.values())
    new    = [max(0, after.get(k, 0) - before.get(k, 0)) for k in before]
    fig = go.Figure()
    fig.add_bar(name="Existing", x=labels, y=b,   marker_color="#1f6feb", opacity=.8)
    fig.add_bar(name="New",      x=labels, y=new, marker_color="#3fb950", base=b)
    fig.update_layout(**PL, title=title, barmode="stack", height=height,
                      legend=dict(orientation="h", yanchor="bottom", y=1.02))
    fig.update_xaxes(tickangle=-25, showgrid=False)
    fig.update_yaxes(showgrid=True, gridcolor=GRID)
    st.plotly_chart(fig, use_container_width=True)


def load_pipeline_log():
    log_file = ROOT / "etl" / "pipeline_runs.log"
    if not log_file.exists():
        return pd.DataFrame()
    rows = []
    for line in log_file.read_text().strip().splitlines()[-50:]:
        try:
            e = json.loads(line)
            rows.append({
                "Timestamp":   e.get("ts", "")[:19],
                "Status":      e.get("status", ""),
                "Duration(s)": round(e.get("duration", 0), 1),
                "Tables":      ", ".join(e.get("tables", []))[:60],
                "Rows":        sum(v for v in e.get("rows", {}).values()
                                   if isinstance(v, int) and v > 0),
            })
        except Exception:
            pass
    return pd.DataFrame(rows)


def _status_badge(status: str) -> str:
    c = {"OK": "#3fb950", "FAILED": "#f85149",
         "DRY-RUN": "#d29922", "NOOP": "#8b949e"}.get(status, "#58a6ff")
    return f'<span style="background:{c};padding:2px 8px;border-radius:10px;' \
           f'font-size:.75rem;color:#fff;font-weight:600">{status}</span>'


# ── render ────────────────────────────────────────────────────────────────────
def render():
    st.markdown("## 🔧 Management & ETL Control Center")
    st.markdown(
        "Full data engineering workspace: connections, data generation, "
        "ETL pipeline, monitoring, lineage, and AI."
    )

    tab_conn, tab_gen, tab_etl, tab_mon, tab_lin, tab_ai = st.tabs([
        "🔗 Connections",
        "⚡ Generate Data",
        "▶️ ETL Pipeline",
        "📈 Monitoring",
        "🔀 Data Lineage",
        "🤖 AI Settings",
    ])

    # ══════════════════════════════════════════════════════════════
    # TAB 1 — Connections
    # ══════════════════════════════════════════════════════════════
    with tab_conn:
        st.markdown("### 🐘 Source — Neon Postgres (OLTP)")
        c1, c2 = st.columns([3, 1])
        with c1:
            use_def = st.checkbox("Use preconfigured connection", value=True, key="neon_use_def")
            if use_def:
                url = default_neon_url()
                st.session_state["neon_url"] = url
                st.success(f"**Preconfigured:** `{mask_url(url)}`")
            else:
                new_url = st.text_input(
                    "Custom Neon connection string",
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
                with st.spinner("Loading …"):
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
            url = st.session_state.get("neon_url", "")
            if url:
                host = url.split("@")[-1].split("/")[0][:28] if "@" in url else "—"
                db   = url.split("/")[-1].split("?")[0] if "/" in url else "—"
                st.metric("Host", host)
                st.metric("Database", db)

        st.divider()
        st.markdown("### ❄️ Destination — Snowflake (DWH)")
        s1, s2 = st.columns([3, 1])
        with s1:
            sf_use_def = st.checkbox("Use preconfigured connection", value=True, key="sf_use_def")
            if sf_use_def:
                sf = default_snowflake_params()
                st.session_state["sf_params"] = sf
                safe = mask_sf(sf)
                st.success(
                    f"**Preconfigured:** account=`{safe.get('account', '—')}` "
                    f"user=`{safe.get('user', '—')}` db=`{safe.get('database', '—')}`")
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
                    new_sf = {"account": sf_acc, "user": sf_user, "role": sf_role,
                              "warehouse": sf_wh, "database": sf_db, "schema": "RAW"}
                    if sf_pwd:
                        new_sf["password"] = sf_pwd
                    if sf_pk and Path(sf_pk).exists():
                        from cryptography.hazmat.primitives.serialization import load_pem_private_key
                        from cryptography.hazmat.backends import default_backend
                        pem = Path(sf_pk).read_bytes()
                        new_sf["private_key"] = load_pem_private_key(pem, None, default_backend())
                    st.session_state["sf_params"] = new_sf

            if sf_live_conn() is not None:
                st.caption("🟢 Snowflake session is open — queries reuse it.")
            elif st.session_state.get("sf_totp"):
                st.caption("🟡 MFA code applied — click **Test Snowflake** now.")
            else:
                st.caption("🔴 Not connected — enter MFA code in sidebar, then Test Snowflake.")

            t1, t2 = st.columns(2)
            if t1.button("🔌 Test Snowflake", key="btn_test_sf"):
                with st.spinner("Connecting …"):
                    ok, msg = test_snowflake(
                        st.session_state.get("sf_params", default_snowflake_params()))
                (st.success if ok else st.error)(msg)
            if t2.button("📋 Show DWH Objects", key="btn_sf_objs"):
                with st.spinner("Loading …"):
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
            st.metric("Account",   str(sf.get("account",   "—"))[:22])
            st.metric("Database",  sf.get("database",  "—"))
            st.metric("Warehouse", sf.get("warehouse", "—"))
            st.metric("Auth", "keypair" if "private_key" in sf
                      else sf.get("authenticator", "snowflake"))

    # ══════════════════════════════════════════════════════════════
    # TAB 2 — Generate Data  (enhanced)
    # ══════════════════════════════════════════════════════════════
    with tab_gen:
        st.markdown("### ⚡ OLTP Data Generator — SportZone Retail")

        # ── top-level config columns ──────────────────────────────
        cfg_col, counts_col = st.columns([1, 2])

        with cfg_col:
            st.markdown("#### ⚙️ Generator Config")

            # preset picker
            preset_name = st.selectbox(
                "Quick Preset",
                list(PRESETS.keys()),
                key="gen_preset",
                help="Pick a preset or choose Custom to set values manually",
            )
            p = PRESETS[preset_name]
            is_custom = preset_name == "🎯 Custom"

            st.divider()

            n_cust   = st.number_input("Customers",  0, 10000,
                                       p["customers"]  if not is_custom else st.session_state.get("g_cust_v", 50),
                                       step=50,  key="g_cust")
            n_orders = st.number_input("Orders",     0, 50000,
                                       p["orders"]    if not is_custom else st.session_state.get("g_ord_v", 200),
                                       step=100, key="g_ord")
            n_events = st.number_input("Events",     0, 100000,
                                       p["events"]    if not is_custom else st.session_state.get("g_evt_v", 500),
                                       step=200, key="g_evt")

            st.divider()
            st.markdown("#### 📅 Date Range")
            start_dt = st.date_input("Order start date", value=date(2024, 1, 1), key="g_start")
            end_dt   = st.date_input("Order end date",   value=date(2025, 12, 31), key="g_end")

            st.divider()
            st.markdown("#### 🎛️ Advanced Options")
            with st.expander("Show advanced options", expanded=False):
                gen_dest = st.radio(
                    "Data destination",
                    ["PostgreSQL (Neon)", "CSV files", "Both"],
                    key="gen_dest",
                    help="Where to write the generated data",
                )
                gen_locale = st.selectbox(
                    "Customer locale",
                    ["en_US", "en_GB", "ar_AE", "en_CA", "de_DE", "fr_FR"],
                    key="gen_locale",
                    help="Controls names, addresses, phone formats",
                )
                clear_first = st.checkbox(
                    "Clear existing data before generating",
                    value=False, key="gen_clear",
                    help="TRUNCATE all OLTP tables before inserting new rows",
                )
                seed_val = st.number_input(
                    "Random seed (0 = random)",
                    0, 99999, 0, key="gen_seed",
                    help="Fixed seed produces repeatable data",
                )
                with_anomalies = st.checkbox(
                    "Inject anomalies (outlier orders, null dates)",
                    value=False, key="gen_anomalies",
                    help="Adds ~2% anomalous rows to test data quality checks",
                )

        with counts_col:
            st.markdown("#### 📊 Current OLTP State")
            with st.spinner("Loading counts …"):
                cur_counts = neon_counts()

            # metric grid
            metric_cols = st.columns(3)
            icons = ["👤","🛒","📦","💳","📋","🎯","🛍️","🏪","🎁","🔔"]
            for i, (t, v) in enumerate(cur_counts.items()):
                label = t.split(".")[-1].replace("_", " ").title()
                metric_cols[i % 3].metric(
                    f"{icons[i]} {label}", f"{v:,}",
                    delta=st.session_state.get(f"gen_delta_{t}")
                )

            st.divider()
            # estimated output preview
            st.markdown("#### 📐 Generation Estimate")
            est_rows = n_cust + n_orders + (n_orders * 3) + n_events
            est_cols = st.columns(4)
            est_cols[0].metric("Customers",    f"{n_cust:,}")
            est_cols[1].metric("Orders",       f"{n_orders:,}")
            est_cols[2].metric("Order Items",  f"~{n_orders*3:,}")
            est_cols[3].metric("Total Rows",   f"~{est_rows:,}")

            st.progress(
                min(est_rows / 50000, 1.0),
                f"~{est_rows:,} rows  ({est_rows/1000:.1f}K)"
            )
            if est_rows > 30000:
                st.warning("Large generation may take 2–5 minutes.", icon="⏱️")

        st.divider()

        # ── action buttons ────────────────────────────────────────
        act1, act2, act3 = st.columns([2, 1, 1])
        run_btn      = act1.button("🚀 Generate Now", type="primary", key="btn_gen",
                                   use_container_width=True)
        preview_btn  = act2.button("👁️ Preview Schema", key="btn_preview",
                                   use_container_width=True)
        reset_btn    = act3.button("🗑️ Reset OLTP Data", key="btn_reset",
                                   use_container_width=True,
                                   help="TRUNCATE all oltp.* tables — irreversible!")

        if preview_btn:
            with st.spinner("Loading …"):
                try:
                    df = neon_query("""
                        SELECT c.table_name, c.column_name, c.data_type,
                               c.character_maximum_length AS max_len,
                               c.is_nullable
                        FROM information_schema.columns c
                        WHERE c.table_schema = 'oltp'
                        ORDER BY c.table_name, c.ordinal_position
                    """)
                    tables = df["table_name"].unique().tolist()
                    sel    = st.selectbox("Table", tables, key="preview_tbl")
                    st.dataframe(
                        df[df["table_name"] == sel].drop(columns="table_name"),
                        use_container_width=True, hide_index=True
                    )
                except Exception as e:
                    st.error(str(e))

        if reset_btn:
            st.warning("This will TRUNCATE all OLTP tables. Are you sure?")
            if st.button("⚠️ Yes, reset all OLTP data", key="btn_reset_confirm"):
                with st.spinner("Truncating …"):
                    try:
                        from conn import neon_execute
                        for t in reversed(OLTP_TABLES):
                            neon_execute(f"TRUNCATE {t} CASCADE")
                        st.success("✅ All OLTP tables cleared.")
                    except Exception as e:
                        st.error(str(e))

        if run_btn:
            before = dict(cur_counts)
            progress = st.progress(0, "Starting generator …")
            log_out  = st.empty()

            cmd = [sys.executable, str(ROOT / "oltp" / "generator.py"),
                   "--customers",  str(n_cust),
                   "--orders",     str(n_orders),
                   "--events",     str(n_events),
                   "--start-date", str(start_dt),
                   "--end-date",   str(end_dt)]

            if seed_val > 0:
                cmd += ["--seed", str(seed_val)]
            if gen_locale != "en_US":
                cmd += ["--locale", gen_locale]
            if clear_first:
                cmd.append("--clear")
            if with_anomalies:
                cmd.append("--anomalies")
            if "CSV" in gen_dest:
                cmd.append("--csv")
            if gen_dest == "CSV files":
                cmd.append("--no-db")

            try:
                proc = subprocess.Popen(cmd, cwd=str(ROOT),
                                        stdout=subprocess.PIPE,
                                        stderr=subprocess.STDOUT, text=True)
                lines, pct = [], 0
                for ln in proc.stdout:
                    ln = ln.strip()
                    if ln and len(ln) < 300:
                        lines.append(ln)
                        log_out.code("\n".join(lines[-20:]))
                        pct = min(pct + 3, 95)
                        progress.progress(pct, "Generating …")
                proc.wait()
                progress.progress(100, "Complete!")

                after = neon_counts()
                added = sum(max(0, after.get(t, 0) - before.get(t, 0)) for t in OLTP_TABLES)

                # store deltas for metric display
                for t in OLTP_TABLES:
                    delta = after.get(t, 0) - before.get(t, 0)
                    st.session_state[f"gen_delta_{t}"] = f"+{delta:,}" if delta > 0 else None

                if added > 0:
                    st.success(f"✅ Generated **{added:,} new rows** across all OLTP tables.")
                    bar_chart(before, after, "OLTP — Before vs After Generation")
                else:
                    st.info("Generator ran but no new rows were added (check generator output above).")

            except Exception as e:
                st.error(f"Generator failed: {e}")
            finally:
                progress.empty()
                log_out.empty()

    # ══════════════════════════════════════════════════════════════
    # TAB 3 — ETL Pipeline  (enhanced)
    # ══════════════════════════════════════════════════════════════
    with tab_etl:
        st.markdown("### ▶️ ETL Pipeline — Neon OLTP → Snowflake DWH")

        # ── Status banner ─────────────────────────────────────────
        try:
            wm_status = neon_query("""
                SELECT table_name, last_extracted, rows_extracted,
                       NOW() - last_extracted AS age
                FROM oltp.etl_watermark
                ORDER BY last_extracted DESC
                LIMIT 1
            """)
            if not wm_status.empty:
                row      = wm_status.iloc[0]
                last_run = str(row["last_extracted"])[:19]
                age_hrs  = row["age"].total_seconds() / 3600 if hasattr(row["age"], "total_seconds") else 0
                if age_hrs < 1:
                    st.success(f"✅ Watermarks are current — last extract **{last_run} UTC** "
                               f"({age_hrs*60:.0f} min ago). No new data in Neon.")
                    st.info("**Tip:** Generate fresh data first (⚡ Generate Data tab), "
                            "or use **--full-load** to re-process everything.", icon="💡")
                elif age_hrs < 24:
                    st.warning(f"⚠️ Last extract was **{last_run} UTC** ({age_hrs:.1f} hrs ago). "
                               "New data may be available.")
                else:
                    st.error(f"❌ Last extract was **{last_run} UTC** — over 24 hrs ago. "
                             "Pipeline may be stale.")
        except Exception:
            pass  # Neon not connected — banner skipped

        st.divider()

        col_opts, col_stats = st.columns([1, 2])

        with col_opts:
            st.markdown("#### ⚙️ Pipeline Options")

            load_mode = st.radio(
                "Load Mode",
                ["🔄 Incremental (watermark)",
                 "⬇️ Full Reload (ignore watermarks)",
                 "🪟 Window / Date Range"],
                key="etl_mode",
                help=(
                    "Incremental = only new rows since last run.\n"
                    "Full Reload = re-extract ALL rows (slow, safe).\n"
                    "Window = specific date range (backfill)."
                ),
            )

            if "🪟" in load_mode:
                w1, w2 = st.columns(2)
                wstart = w1.date_input("From", key="w_start")
                wend   = w2.date_input("To",   key="w_end")

            st.divider()
            st.markdown("#### 🎛️ Stage Flags")
            dry_run  = st.checkbox("🧪 Dry-run (no Snowflake writes)", value=False, key="etl_dry",
                                   help="Extract + transform only — useful to preview what would load")
            verbose  = st.checkbox("📝 Verbose logging",               value=True,  key="etl_verbose")

            st.divider()
            st.markdown("#### 🗂️ Table Scope")
            all_tables = st.checkbox("All tables", value=True, key="etl_all_tbl")
            if not all_tables:
                sel_tables = st.multiselect(
                    "Select tables",
                    ["customers", "products", "stores", "promotions",
                     "orders", "order_items", "payments",
                     "inventory", "customer_events"],
                    default=["orders", "order_items"],
                    key="etl_sel_tbl",
                )

            st.divider()
            # Mode explanation
            mode_desc = {
                "🔄 Incremental (watermark)":
                    "Only rows with `updated_at` > last watermark. "
                    "Fastest. Safe to re-run.",
                "⬇️ Full Reload (ignore watermarks)":
                    "Re-extracts ALL rows regardless of watermark. "
                    "Use when DWH data is stale or watermarks are wrong.",
                "🪟 Window / Date Range":
                    "Extracts rows in the specified date window. "
                    "Ideal for backfills or fixing a specific period.",
            }
            st.caption(mode_desc[load_mode])

        with col_stats:
            st.markdown("#### 📊 DWH Row Counts — Snowflake")
            if sf_live_conn() is None:
                st.info("Not connected to Snowflake — go to **Connections** tab.", icon="❄️")
                sf_c = {t: 0 for t in SF_TABLES}
            else:
                sf_c = sf_counts()

            m1, m2, m3 = st.columns(3)
            icons_sf   = ["👤", "🛍️", "🏪", "🎁", "📅", "💰", "📦"]
            for i, (t, v) in enumerate(sf_c.items()):
                [m1, m2, m3][i % 3].metric(
                    icons_sf[i] + " " + t.split(".")[-1], f"{v:,}"
                )

            st.divider()
            st.markdown("#### 🔁 Watermark State")
            try:
                wm_df = neon_query("""
                    SELECT table_name        AS "Table",
                           last_extracted   AS "Last Extracted",
                           rows_extracted   AS "Last Rows",
                           last_run_at      AS "Last Run At"
                    FROM oltp.etl_watermark
                    ORDER BY last_extracted DESC
                """)
                if not wm_df.empty:
                    st.dataframe(wm_df, use_container_width=True, hide_index=True)
            except Exception as e:
                st.caption(f"Watermarks unavailable: {e}")

        st.divider()

        # ── Action buttons ────────────────────────────────────────
        r1, r2, r3, r4 = st.columns([3, 1, 1, 1])
        run_etl  = r1.button("▶️ Run ETL Pipeline", type="primary",
                              key="btn_run_etl", use_container_width=True)
        reset_wm = r2.button("🔄 Reset\nWatermarks", key="btn_reset_wm",
                              use_container_width=True,
                              help="Set all watermarks to 1970 — next run extracts everything")
        force_full = r3.button("⬇️ Force\nFull Load", key="btn_force_full",
                               use_container_width=True,
                               help="Same as Full Reload mode — re-extract + reload all tables now")
        show_hist = r4.button("📋 Run\nHistory",    key="btn_show_hist",
                               use_container_width=True)

        if reset_wm:
            with st.spinner("Resetting watermarks …"):
                try:
                    from conn import neon_execute
                    neon_execute("""
                        UPDATE oltp.etl_watermark
                        SET last_extracted = '1970-01-01 00:00:00+00',
                            rows_extracted = 0
                    """)
                    st.success("✅ All watermarks reset to epoch. "
                               "Next incremental run will extract all rows.")
                except Exception as e:
                    st.error(str(e))

        if force_full:
            st.session_state["etl_force_full"] = True
            st.rerun()

        if show_hist:
            log_df_quick = load_pipeline_log()
            if log_df_quick.empty:
                st.info("No pipeline run history yet.")
            else:
                st.dataframe(log_df_quick.iloc[::-1].head(20),
                             use_container_width=True, hide_index=True)

        # ── Run ETL ───────────────────────────────────────────────
        force_full_flag = st.session_state.pop("etl_force_full", False)
        actually_full   = ("Full" in load_mode) or force_full_flag

        if run_etl or force_full_flag:
            sf_before = dict(sf_c)
            progress  = st.progress(0, "Initialising pipeline …")
            log_area  = st.empty()
            logs      = []

            cmd = [sys.executable, str(ROOT / "etl" / "pipeline.py")]

            if actually_full:
                cmd.append("--full-load")
            if "🪟" in load_mode:
                cmd += ["--start-date", str(wstart), "--end-date", str(wend)]
            if dry_run:
                cmd.append("--dry-run")
            if verbose:
                cmd.append("--verbose")
            if not all_tables and sel_tables:
                cmd += ["--tables"] + sel_tables

            try:
                proc = subprocess.Popen(cmd, cwd=str(ROOT),
                                        stdout=subprocess.PIPE,
                                        stderr=subprocess.STDOUT, text=True)
                pct  = 0
                last_status = "running"
                for ln in proc.stdout:
                    ln = ln.strip()
                    # filter noise
                    if not ln or ln.startswith("C:\\") or "pyarrow" in ln or "UserWarning" in ln:
                        continue
                    if len(ln) < 400:
                        logs.append(ln)
                        log_area.code("\n".join(logs[-30:]))
                        pct = min(pct + 2, 95)
                        progress.progress(pct, "Running …")
                        # detect status from output
                        if "Pipeline complete" in ln:
                            last_status = "complete"
                        elif "Nothing new" in ln:
                            last_status = "noop"
                        elif "FAILED" in ln:
                            last_status = "failed"

                proc.wait()
                progress.progress(100, "Done!")

                if last_status == "noop":
                    st.warning(
                        "⚠️ **No new data found** — watermarks are current.\n\n"
                        "**Options:**\n"
                        "1. Go to **⚡ Generate Data** tab and generate new records first\n"
                        "2. Click **🔄 Reset Watermarks** above, then run again\n"
                        "3. Use **⬇️ Force Full Load** to reprocess all existing data",
                        icon="💡"
                    )
                elif last_status == "failed":
                    st.error("❌ Pipeline failed — check the log output above.")
                else:
                    sf_after = sf_counts()
                    added = sum(
                        max(0, sf_after.get(t, 0) - sf_before.get(t, 0))
                        for t in SF_TABLES
                    )
                    if dry_run:
                        st.info("🧪 Dry-run complete — no data was written to Snowflake.")
                    elif added > 0:
                        st.success(
                            f"✅ ETL complete — **{added:,} new rows** loaded into Snowflake DWH."
                        )
                        bar_chart(sf_before, sf_after, "Snowflake DWH — Before vs After ETL")
                        st.cache_data.clear()
                    else:
                        st.info("Pipeline ran successfully. DWH row counts unchanged "
                                "(rows may have been updated, not inserted).")
                        st.cache_data.clear()

            except Exception as e:
                st.error(f"Failed to launch pipeline: {e}")
            finally:
                progress.empty()

        st.divider()
        st.markdown("#### 📋 Pipeline DAG")
        st.code("""
Neon Postgres OLTP  (oltp.* tables)
        │
        │  extract.py — incremental via etl_watermark
        │  Parquet files → etl/staging/<table>/<timestamp>.parquet
        ▼
transform.py — cleanse, cast, derive columns
        │  pandas DataFrames
        ▼
load.py — write_pandas() into Snowflake RAW.*
        │  COPY INTO + MERGE
        ▼
Snowflake: RAW → STAGING → WAREHOUSE (dims + facts) → MART
        │
        ▼
Dashboard (queries MART.* views)
        """, language="text")

    # ══════════════════════════════════════════════════════════════
    # TAB 4 — Monitoring
    # ══════════════════════════════════════════════════════════════
    with tab_mon:
        st.markdown("### 📈 Pipeline Health & Monitoring")

        log_df = load_pipeline_log()

        if log_df.empty:
            st.info("No pipeline runs recorded yet. Run the ETL pipeline first.", icon="📋")
        else:
            # KPIs
            k1, k2, k3, k4, k5 = st.columns(5)
            k1.metric("Total Runs",      len(log_df))
            k2.metric("Success Rate",    f"{(log_df['Status']=='OK').mean()*100:.0f}%")
            k3.metric("Avg Duration",    f"{log_df['Duration(s)'].mean():.1f}s")
            k4.metric("Fastest Run",     f"{log_df['Duration(s)'].min():.1f}s")
            k5.metric("Total Rows ETL'd", f"{log_df['Rows'].sum():,}")

            # Timeline
            fig = go.Figure()
            colors = {"OK": "#3fb950", "FAILED": "#f85149",
                      "DRY-RUN": "#d29922", "NOOP": "#8b949e"}
            for status, grp in log_df.groupby("Status"):
                fig.add_scatter(
                    x=grp["Timestamp"], y=grp["Duration(s)"],
                    mode="markers+lines", name=status,
                    marker=dict(color=colors.get(status, "#58a6ff"), size=10),
                    line=dict(width=1.5),
                )
            fig.update_layout(**PL, title="Pipeline Run History",
                              height=280, xaxis_title="", yaxis_title="Duration (s)")
            st.plotly_chart(fig, use_container_width=True)

            # Rows histogram
            fig2 = px.bar(log_df.iloc[::-1].head(20), x="Timestamp", y="Rows",
                          color="Status",
                          color_discrete_map=colors,
                          title="Rows ETL'd per Run (last 20)")
            fig2.update_layout(**PL, height=240)
            st.plotly_chart(fig2, use_container_width=True)

            # Log table with status badges
            st.dataframe(log_df.iloc[::-1], use_container_width=True, hide_index=True)

        st.divider()
        mc1, mc2 = st.columns(2)

        with mc1:
            st.markdown("### ❄️ Snowflake Query History")
            sf_hist_limit = st.slider("Rows", 5, 50, 10, key="sf_hist_lim")
            if st.button("🔍 Fetch", key="btn_sf_hist"):
                with st.spinner("Fetching …"):
                    try:
                        df = sf_query(f"""
                            SELECT QUERY_TEXT, EXECUTION_STATUS,
                                   ROUND(TOTAL_ELAPSED_TIME/1000,2) AS ELAPSED_SEC,
                                   BYTES_SCANNED/1024/1024 AS MB_SCANNED,
                                   START_TIME, USER_NAME
                            FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
                            WHERE START_TIME >= DATEADD(HOUR,-24,CURRENT_TIMESTAMP())
                              AND QUERY_TEXT NOT LIKE '%ACCOUNT_USAGE%'
                            ORDER BY START_TIME DESC
                            LIMIT {sf_hist_limit}
                        """)
                        if df.empty:
                            st.info("No recent queries (ACCOUNT_USAGE latency ~45 min).")
                        else:
                            st.dataframe(df, use_container_width=True, hide_index=True)
                    except Exception as e:
                        st.error(str(e))

        with mc2:
            st.markdown("### 🐘 Neon Table Stats")
            neon_schema = st.selectbox(
                "Schema", ["oltp", "raw", "staging", "warehouse"], key="neon_schema_sel"
            )
            if st.button("📊 Fetch Stats", key="btn_neon_stats"):
                with st.spinner("Fetching …"):
                    try:
                        df = neon_query(f"""
                            SELECT tablename,
                                   n_live_tup  AS live_rows,
                                   n_dead_tup  AS dead_rows,
                                   last_autovacuum::date AS vacuumed,
                                   last_autoanalyze::date AS analyzed
                            FROM pg_stat_user_tables
                            WHERE schemaname = '{neon_schema}'
                            ORDER BY live_rows DESC
                        """)
                        st.dataframe(df, use_container_width=True, hide_index=True)
                    except Exception as e:
                        st.error(str(e))

    # ══════════════════════════════════════════════════════════════
    # TAB 5 — Data Lineage
    # ══════════════════════════════════════════════════════════════
    with tab_lin:
        st.markdown("### 🔀 Data Lineage — Visual Flow Diagram")

        node_labels = [
            "Neon OLTP\nsportzone",
            "oltp.customer", "oltp.order_header", "oltp.order_item",
            "oltp.product", "oltp.inventory", "oltp.promotion",
            "extract.py\n(watermark)", "transform.py\n(cleanse+cast)",
            "RAW.RAW_CUSTOMERS", "RAW.RAW_ORDERS", "RAW.RAW_ORDER_ITEMS",
            "RAW.RAW_PRODUCTS", "RAW.RAW_INVENTORY", "RAW.RAW_PROMOTIONS",
            "STG_CUSTOMERS", "STG_ORDERS", "STG_ORDER_ITEMS",
            "STG_PRODUCTS", "STG_INVENTORY",
            "DIM_CUSTOMER", "DIM_PRODUCT", "DIM_STORE",
            "DIM_PROMOTION", "FACT_SALES", "FACT_INVENTORY",
            "MART.DAILY_SALES", "MART.CUSTOMER_360", "MART.INVENTORY_STATUS",
        ]
        node_colors = (
            ["#388bfd"] + ["#1f6feb"] * 6 + ["#d29922"] * 2 +
            ["#da3633"] * 6 + ["#f0883e"] * 5 + ["#3fb950"] * 6 + ["#bc8cff"] * 3
        )
        edges = [
            (1,7,3,"customer"),(2,7,5,"orders"),(3,7,5,"items"),
            (4,7,2,"products"),(5,7,2,"inventory"),(6,7,1,"promotions"),
            (7,8,18,"parquet files"),
            (8,9,3,"customers"),(8,10,5,"orders"),(8,11,5,"items"),
            (8,12,2,"products"),(8,13,2,"inventory"),(8,14,1,"promotions"),
            (9,15,3,"typed"),(10,16,5,"deduped"),(11,17,5,"enriched"),
            (12,18,2,"margin%"),(13,19,2,"available"),
            (15,20,3,"SCD2"),(18,21,2,"SCD2"),
            (16,22,1,"upsert"),(16,23,1,"upsert"),
            (16,24,5,"MERGE"),(17,24,5,"MERGE"),(19,25,2,"MERGE"),
            (24,26,5,"views"),(20,27,3,"views"),(25,28,2,"views"),
        ]
        src=[e[0] for e in edges]; tgt=[e[1] for e in edges]
        vals=[e[2] for e in edges]; elabels=[e[3] for e in edges]
        edge_colors=[]
        for s in src:
            if s<=6:    edge_colors.append("rgba(31,111,235,0.4)")
            elif s<=8:  edge_colors.append("rgba(210,153,34,0.5)")
            elif s<=14: edge_colors.append("rgba(218,54,51,0.4)")
            elif s<=19: edge_colors.append("rgba(240,136,62,0.4)")
            else:       edge_colors.append("rgba(63,185,80,0.4)")

        fig_sankey = go.Figure(go.Sankey(
            arrangement="snap",
            node=dict(pad=20, thickness=22, line=dict(color="#30363d", width=0.5),
                      label=node_labels, color=node_colors,
                      hovertemplate="%{label}<extra></extra>"),
            link=dict(source=src, target=tgt, value=vals, label=elabels,
                      color=edge_colors,
                      hovertemplate="%{label}: %{value} tables<extra></extra>"),
        ))
        fig_sankey.update_layout(
            title_text="SportZone Data Lineage — Medallion Architecture Flow",
            font=dict(size=11, color="#f0f6fc"), paper_bgcolor="#0d1117",
            height=560, margin=dict(l=10, r=10, t=48, b=10),
        )
        st.plotly_chart(fig_sankey, use_container_width=True)

        st.markdown("""
<div style="display:flex;gap:12px;flex-wrap:wrap;margin-top:4px">
  <span style="background:#1f6feb;padding:4px 12px;border-radius:12px;font-size:.8rem;color:#fff">🟦 Neon OLTP Source</span>
  <span style="background:#d29922;padding:4px 12px;border-radius:12px;font-size:.8rem;color:#fff">🟨 ETL Pipeline</span>
  <span style="background:#da3633;padding:4px 12px;border-radius:12px;font-size:.8rem;color:#fff">🟥 RAW (Bronze)</span>
  <span style="background:#f0883e;padding:4px 12px;border-radius:12px;font-size:.8rem;color:#fff">🟧 Staging (Silver)</span>
  <span style="background:#3fb950;padding:4px 12px;border-radius:12px;font-size:.8rem;color:#fff">🟩 Warehouse (Gold)</span>
  <span style="background:#bc8cff;padding:4px 12px;border-radius:12px;font-size:.8rem;color:#fff">🟪 Mart (Platinum)</span>
</div>
""", unsafe_allow_html=True)

        st.divider()

        lin_tab1, lin_tab2 = st.tabs(["🔬 Column Lineage — FACT_SALES",
                                       "⏱️ ETL Watermarks"])
        with lin_tab1:
            lineage_rows = [
                ("ORDER_DATE_KEY",  "oltp.order_header.order_date",  "TO_NUMBER(TO_CHAR(date,'YYYYMMDD'))", "DIM_DATE join key"),
                ("CUST_SK",         "oltp.customer.customer_id",      "DIM_CUSTOMER lookup",               "SCD2 surrogate key"),
                ("PRD_SK",          "oltp.product.product_id",        "DIM_PRODUCT lookup",                "SCD2 surrogate key"),
                ("STORE_SK",        "oltp.store.store_id",            "DIM_STORE lookup",                  "SCD1 surrogate key"),
                ("PROMO_SK",        "oltp.promotion.code",            "DIM_PROMOTION lookup",              "NULL → -1"),
                ("QTY_ORDERED",     "oltp.order_item.qty",            "CAST INTEGER",                      "Direct copy"),
                ("UNIT_PRICE",      "oltp.order_item.unit_price",     "CAST NUMERIC",                      "At time of sale"),
                ("UNIT_COST",       "oltp.product.unit_cost",         "DIM_PRODUCT.UNIT_COST",             "From current dim row"),
                ("GROSS_REVENUE",   "Derived",                        "qty × unit_price",                  "Computed"),
                ("NET_REVENUE",     "oltp.order_item.line_total",     "CAST NUMERIC",                      "After discount"),
                ("COGS",            "Derived",                        "qty × unit_cost",                   "Computed"),
                ("GROSS_PROFIT",    "Derived",                        "net_revenue − cogs",                "Computed"),
                ("DISCOUNT_AMT",    "Derived in stg_order_items",     "unit_price × qty × pct / 100",      "Computed"),
                ("GROSS_MARGIN_PCT","Derived",                        "(net_revenue−cogs)/net_revenue×100","Computed"),
            ]
            st.dataframe(
                pd.DataFrame(lineage_rows, columns=["Target Column","Source","Transformation","Notes"]),
                use_container_width=True, hide_index=True,
            )
        with lin_tab2:
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
                else:
                    st.info("No watermarks found.")
            except Exception as e:
                st.warning(str(e))

    # ══════════════════════════════════════════════════════════════
    # TAB 6 — AI Settings
    # ══════════════════════════════════════════════════════════════
    with tab_ai:
        st.markdown("### 🤖 AI Configuration")
        st.info("Configure OpenAI API to enable: NL→SQL, AI insights, "
                "automated storytelling, and intelligent data exploration.", icon="🔑")

        current_key = st.session_state.get("openai_key", "")
        new_key = st.text_input(
            "OpenAI API Key", value=current_key, type="password",
            placeholder="sk-...",
            help="Stored in session only, never written to disk",
            key="ai_key_inp",
        )
        if new_key != current_key:
            st.session_state["openai_key"] = new_key

        a1, a2 = st.columns(2)
        model_choice = a1.selectbox(
            "Model", ["gpt-4o-mini", "gpt-4o", "gpt-4-turbo"], key="ai_model"
        )
        ai_temp = a2.slider("Temperature", 0.0, 1.0, 0.1, 0.05, key="ai_temp",
                            help="Lower = more deterministic SQL; higher = more creative")

        if st.button("🔌 Test OpenAI Connection", key="btn_test_ai"):
            key = st.session_state.get("openai_key", "")
            if not key:
                st.error("No API key set.")
            else:
                with st.spinner("Testing …"):
                    try:
                        from openai import OpenAI
                        client = OpenAI(api_key=key)
                        r = client.chat.completions.create(
                            model=model_choice,
                            messages=[{"role": "user",
                                       "content": "Say: AI connected to SportZone!"}],
                            max_tokens=20,
                        )
                        st.success(f"✅ {r.choices[0].message.content.strip()}")
                    except ImportError:
                        st.error("Run: pip install openai")
                    except Exception as e:
                        st.error(str(e))

        st.divider()
        st.markdown("#### AI Capabilities")
        st.dataframe(pd.DataFrame([
            ("NL → SQL",              "Convert plain English to Snowflake SQL",              "Analytics → SQL Explorer"),
            ("Auto-insights",         "AI-generated KPI commentary",                         "Analytics → Overview"),
            ("Chart Storytelling",    "Narrative explanation of every chart",                 "Analytics → all tabs"),
            ("Anomaly Narration",     "AI explains unusual patterns in data",                 "Analytics → AI tab"),
            ("SQL Validation",        "Reviews generated SQL for correctness",               "Analytics → SQL Explorer"),
            ("Question Suggestions",  "Recommends follow-up analytical questions",           "Analytics → AI tab"),
        ], columns=["Feature", "Description", "Where Used"]),
        use_container_width=True, hide_index=True)
