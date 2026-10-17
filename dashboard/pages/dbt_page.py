"""
dbt Page — run dbt commands, browse models, view logs, show DAG lineage.
"""
import json, subprocess, sys, time
from datetime import datetime
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

ROOT    = Path(__file__).parent.parent.parent
DBT_DIR = ROOT / "dbt"
DBT_EXE = ROOT.parent / ".venv" / "Scripts" / "dbt.exe"   # Windows venv
if not DBT_EXE.exists():
    DBT_EXE = Path(sys.executable).parent / "dbt"          # fallback

sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(Path(__file__).parent.parent))
from conn import sf_query

BG  = "#0d1117"; BG2 = "#161b22"; FONT = "#f0f6fc"; GRID = "#30363d"
PL  = dict(paper_bgcolor=BG, plot_bgcolor=BG2, font=dict(color=FONT),
           margin=dict(l=8, r=8, t=40, b=8))

# ── colour per layer ──────────────────────────────────────────────────────────
LAYER_COLOR = {
    "source":    "#388bfd",
    "staging":   "#f0883e",
    "snapshot":  "#d29922",
    "warehouse": "#3fb950",
    "mart":      "#bc8cff",
}

# ── DAG node definitions (label, layer, x, y) ────────────────────────────────
NODES = [
    # raw sources
    ("RAW_CUSTOMERS",    "source",    0.00, 1.00),
    ("RAW_PRODUCTS",     "source",    0.00, 0.80),
    ("RAW_STORES",       "source",    0.00, 0.60),
    ("RAW_ORDERS",       "source",    0.00, 0.40),
    ("RAW_ORDER_ITEMS",  "source",    0.00, 0.20),
    ("RAW_INVENTORY",    "source",    0.00, 0.05),
    ("RAW_PROMOTIONS",   "source",    0.00, -0.15),
    # staging
    ("stg_customers",    "staging",   0.22, 1.00),
    ("stg_products",     "staging",   0.22, 0.80),
    ("stg_stores",       "staging",   0.22, 0.60),
    ("stg_orders",       "staging",   0.22, 0.40),
    ("stg_order_items",  "staging",   0.22, 0.20),
    ("stg_inventory",    "staging",   0.22, 0.05),
    ("stg_promotions",   "staging",   0.22, -0.15),
    # snapshots
    ("snap_customers",   "snapshot",  0.44, 0.90),
    ("snap_products",    "snapshot",  0.44, 0.70),
    # warehouse dims
    ("dim_customer",     "warehouse", 0.66, 1.00),
    ("dim_product",      "warehouse", 0.66, 0.80),
    ("dim_store",        "warehouse", 0.66, 0.60),
    ("dim_promotion",    "warehouse", 0.66, 0.40),
    ("dim_date",         "warehouse", 0.66, 0.20),
    # warehouse facts
    ("fact_sales",       "warehouse", 0.66, -0.05),
    ("fact_inventory",   "warehouse", 0.66, -0.25),
    # mart
    ("mart_daily_sales_summary",       "mart", 0.88, 1.00),
    ("mart_customer_360",              "mart", 0.88, 0.75),
    ("mart_product_performance",       "mart", 0.88, 0.50),
    ("mart_inventory_status",          "mart", 0.88, 0.25),
    ("mart_promotion_effectiveness",   "mart", 0.88, 0.00),
]

# idx lookup
IDX = {n[0]: i for i, n in enumerate(NODES)}

EDGES = [
    # source → staging
    ("RAW_CUSTOMERS",   "stg_customers"),
    ("RAW_PRODUCTS",    "stg_products"),
    ("RAW_STORES",      "stg_stores"),
    ("RAW_ORDERS",      "stg_orders"),
    ("RAW_ORDER_ITEMS", "stg_order_items"),
    ("RAW_INVENTORY",   "stg_inventory"),
    ("RAW_PROMOTIONS",  "stg_promotions"),
    # staging → snapshots
    ("stg_customers",   "snap_customers"),
    ("stg_products",    "snap_products"),
    # snapshots → dims
    ("snap_customers",  "dim_customer"),
    ("snap_products",   "dim_product"),
    # staging → dims (SCD1)
    ("stg_stores",      "dim_store"),
    ("stg_promotions",  "dim_promotion"),
    # staging → facts
    ("stg_orders",      "fact_sales"),
    ("stg_order_items", "fact_sales"),
    ("stg_inventory",   "fact_inventory"),
    # dims → facts (foreign keys)
    ("dim_customer",    "fact_sales"),
    ("dim_product",     "fact_sales"),
    ("dim_store",       "fact_sales"),
    ("dim_promotion",   "fact_sales"),
    ("dim_date",        "fact_sales"),
    ("dim_product",     "fact_inventory"),
    ("dim_store",       "fact_inventory"),
    ("dim_date",        "fact_inventory"),
    # facts + dims → mart
    ("fact_sales",      "mart_daily_sales_summary"),
    ("fact_sales",      "mart_customer_360"),
    ("fact_sales",      "mart_product_performance"),
    ("fact_sales",      "mart_promotion_effectiveness"),
    ("dim_customer",    "mart_customer_360"),
    ("fact_inventory",  "mart_inventory_status"),
    ("dim_product",     "mart_inventory_status"),
    ("dim_store",       "mart_inventory_status"),
]


def _dbt_cmd(args: list[str]) -> tuple[str, int]:
    """Run a dbt command, return (stdout+stderr, returncode)."""
    cmd = [str(DBT_EXE)] + args + [
        "--profiles-dir", str(DBT_DIR),
        "--project-dir",  str(DBT_DIR),
        "--no-partial-parse",
    ]
    try:
        proc = subprocess.run(
            cmd, cwd=str(ROOT),
            capture_output=True, text=True, timeout=600
        )
        out = proc.stdout + proc.stderr
        # strip pyarrow warning noise
        clean = "\n".join(
            l for l in out.splitlines()
            if "pyarrow" not in l and "UserWarning" not in l
            and "warn_incompatible_dep" not in l
        )
        return clean, proc.returncode
    except FileNotFoundError:
        return "❌  dbt executable not found. Check venv is activated.", 1
    except subprocess.TimeoutExpired:
        return "❌  dbt command timed out (>10 min).", 1
    except Exception as e:
        return f"❌  {e}", 1


def _parse_run_results() -> pd.DataFrame:
    """Parse dbt run_results.json into a dataframe."""
    rr_path = DBT_DIR / "target" / "run_results.json"
    if not rr_path.exists():
        return pd.DataFrame()
    try:
        data = json.loads(rr_path.read_text())
        rows = []
        for r in data.get("results", []):
            rows.append({
                "Node":      r.get("unique_id", "").split(".")[-1],
                "Status":    r.get("status", ""),
                "Time (s)":  round(r.get("execution_time", 0), 2),
                "Rows":      r.get("adapter_response", {}).get("rows_affected", "—"),
                "Type":      r.get("unique_id", "").split(".")[0],
            })
        return pd.DataFrame(rows)
    except Exception:
        return pd.DataFrame()


def _hex_rgba(hex_color: str, alpha: float = 0.35) -> str:
    """Convert '#rrggbb' to 'rgba(r,g,b,alpha)'."""
    h = hex_color.lstrip("#")
    return f"rgba({int(h[0:2],16)},{int(h[2:4],16)},{int(h[4:6],16)},{alpha})"


def _build_dag_fig(highlight: set[str] | None = None) -> go.Figure:
    """Build a Plotly scatter-based DAG diagram."""
    highlight = highlight or set()

    xs     = [n[2] for n in NODES]
    ys     = [n[3] for n in NODES]
    labels = [n[0] for n in NODES]
    layers = [n[1] for n in NODES]
    colors = [LAYER_COLOR[l] for l in layers]
    sizes  = [16 if lbl in highlight else 11 for lbl in labels]

    fig = go.Figure()

    # ── edges (drawn first, behind nodes) ──────────────────────────────────
    for src_name, tgt_name in EDGES:
        xi, yi = NODES[IDX[src_name]][2], NODES[IDX[src_name]][3]
        xj, yj = NODES[IDX[tgt_name]][2], NODES[IDX[tgt_name]][3]
        edge_col = _hex_rgba(LAYER_COLOR[NODES[IDX[src_name]][1]], 0.35)
        fig.add_trace(go.Scatter(
            x=[xi, xj, None], y=[yi, yj, None],
            mode="lines",
            line=dict(color=edge_col, width=1.2),
            hoverinfo="skip",
            showlegend=False,
        ))

    # ── nodes grouped by layer for legend ──────────────────────────────────
    for layer, lcolor in LAYER_COLOR.items():
        mask = [i for i, l in enumerate(layers) if l == layer]
        fig.add_trace(go.Scatter(
            x=[xs[i] for i in mask],
            y=[ys[i] for i in mask],
            mode="markers+text",
            name=layer.capitalize(),
            marker=dict(
                color=lcolor,
                size=[sizes[i] for i in mask],
                line=dict(color="#30363d", width=1),
                symbol="circle",
            ),
            text=[labels[i] for i in mask],
            textposition="middle right",
            textfont=dict(size=9, color=FONT),
            hovertemplate="%{text}<extra></extra>",
        ))

    fig.update_layout(
        **PL,
        height=560,
        title="dbt Model DAG — SportZone",
        xaxis=dict(showgrid=False, zeroline=False, showticklabels=False,
                   range=[-0.05, 1.15]),
        yaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
        legend=dict(orientation="h", yanchor="bottom", y=-0.12,
                    font=dict(size=10), bgcolor="rgba(0,0,0,0)"),
        hovermode="closest",
    )

    # layer column headers
    for layer, lx, lcolor in [
        ("Sources",   0.00, LAYER_COLOR["source"]),
        ("Staging",   0.22, LAYER_COLOR["staging"]),
        ("Snapshots", 0.44, LAYER_COLOR["snapshot"]),
        ("Warehouse", 0.66, LAYER_COLOR["warehouse"]),
        ("Mart",      0.88, LAYER_COLOR["mart"]),
    ]:
        fig.add_annotation(x=lx, y=1.18, xref="x", yref="y",
                           text=f"<b>{layer}</b>", showarrow=False,
                           font=dict(size=11, color=lcolor), align="center")
    return fig


def _model_inventory() -> pd.DataFrame:
    """Return a static dataframe of all dbt models."""
    models = [
        # staging
        ("stg_customers",    "staging",   "view",        "RAW_CUSTOMERS",   "Deduplicate, type-cast, derive full_name"),
        ("stg_products",     "staging",   "view",        "RAW_PRODUCTS",    "Deduplicate, compute gross_margin_pct"),
        ("stg_stores",       "staging",   "view",        "RAW_STORES",      "Deduplicate, upper/initcap"),
        ("stg_orders",       "staging",   "view",        "RAW_ORDERS",      "Deduplicate, derive date parts, flag discounts"),
        ("stg_order_items",  "staging",   "view",        "RAW_ORDER_ITEMS", "Deduplicate, compute discount_amt"),
        ("stg_inventory",    "staging",   "view",        "RAW_INVENTORY",   "Deduplicate, compute qty_available, needs_reorder"),
        ("stg_promotions",   "staging",   "view",        "RAW_PROMOTIONS",  "Deduplicate, type-cast"),
        # snapshots
        ("snap_customers",   "snapshot",  "table",       "stg_customers",   "SCD2 check: loyalty_tier, city, state, is_active"),
        ("snap_products",    "snapshot",  "table",       "stg_products",    "SCD2 check: unit_price, is_active"),
        # warehouse dims
        ("dim_date",         "warehouse", "table",       "date_spine",      "Static calendar 2023-2026, fiscal year"),
        ("dim_customer",     "warehouse", "table",       "snap_customers",  "SCD2 + unknown member (-1)"),
        ("dim_product",      "warehouse", "table",       "snap_products",   "SCD2 + price_tier macro + unknown member"),
        ("dim_store",        "warehouse", "table",       "stg_stores",      "SCD1 upsert, abs(hash) surrogate key"),
        ("dim_promotion",    "warehouse", "table",       "stg_promotions",  "SCD1 upsert, unknown member NONE"),
        # warehouse facts
        ("fact_sales",       "warehouse", "incremental", "stg_orders + stg_order_items + dims", "MERGE on order_item_id"),
        ("fact_inventory",   "warehouse", "incremental", "stg_inventory + dims",                "MERGE on inventory_id × snapshot_date_key"),
        # mart
        ("mart_daily_sales_summary",     "mart", "view", "fact_sales + dims",     "Daily × channel × category"),
        ("mart_customer_360",            "mart", "view", "dim_customer + fact_sales", "LTV + RFM + preferred channel"),
        ("mart_product_performance",     "mart", "view", "fact_sales + dims",     "Monthly product revenue + return rate"),
        ("mart_inventory_status",        "mart", "view", "fact_inventory + dims", "Latest snapshot + cover days"),
        ("mart_promotion_effectiveness", "mart", "view", "fact_sales + dim_promotion", "Promo ROI + discount rate"),
    ]
    return pd.DataFrame(models, columns=[
        "Model", "Layer", "Materialisation", "Depends On", "Business Logic",
    ])


# ── main render ───────────────────────────────────────────────────────────────
def render():
    st.markdown("## ⚙️ dbt Transformation Layer")
    st.markdown(
        "Run dbt commands, inspect the model DAG, browse model metadata, "
        "and view test results — all from inside the dashboard."
    )

    tab_run, tab_dag, tab_models, tab_tests, tab_docs = st.tabs([
        "▶️ Run dbt",
        "🗺️ Model DAG",
        "📋 Model Inventory",
        "✅ Test Results",
        "📚 Documentation",
    ])

    # ══════════════════════════════════════════════════
    # TAB 1 — Run dbt
    # ══════════════════════════════════════════════════
    with tab_run:
        st.markdown("### ▶️ Execute dbt Commands")
        st.caption(f"Project dir: `{DBT_DIR}`  |  dbt: `{DBT_EXE}`")

        col_a, col_b = st.columns([1, 2])

        with col_a:
            st.markdown("#### Command")
            cmd_choice = st.radio(
                "Select command",
                [
                    "dbt snapshot",
                    "dbt run",
                    "dbt test",
                    "dbt run + test",
                    "dbt snapshot + run + test",
                    "dbt build (all)",
                    "dbt source freshness",
                    "dbt docs generate",
                ],
                key="dbt_cmd_choice",
            )

            st.markdown("#### Scope (optional)")
            model_select = st.text_input(
                "Model / tag selector",
                placeholder="e.g.  mart  or  tag:facts  or  dim_customer+",
                key="dbt_model_sel",
                help="Leave blank to run all. Supports dbt node selectors.",
            )

            full_refresh = st.checkbox(
                "⚠️ --full-refresh  (rebuild incrementals from scratch)",
                value=False, key="dbt_fr",
            )

            st.divider()
            run_btn = st.button("🚀 Run", type="primary", key="dbt_run_btn",
                                use_container_width=True)

        with col_b:
            st.markdown("#### Output")
            log_area = st.empty()
            status_area = st.empty()

        if run_btn:
            # Build arg list
            def _make_args(base_cmd: list[str]) -> list[str]:
                args = base_cmd[:]
                if model_select.strip():
                    args += ["--select", model_select.strip()]
                if full_refresh:
                    args.append("--full-refresh")
                return args

            t0 = time.time()
            combined_out = ""

            cmd_map = {
                "dbt snapshot":                [["snapshot"]],
                "dbt run":                     [["run"]],
                "dbt test":                    [["test"]],
                "dbt run + test":              [["run"], ["test"]],
                "dbt snapshot + run + test":   [["snapshot"], ["run"], ["test"]],
                "dbt build (all)":             [["build"]],
                "dbt source freshness":        [["source", "freshness"]],
                "dbt docs generate":           [["docs", "generate"]],
            }
            steps = cmd_map.get(cmd_choice, [["run"]])

            all_ok = True
            for step in steps:
                args = _make_args(step)
                log_area.code(
                    combined_out + f"\n\n$ dbt {' '.join(args)}\n(running…)",
                    language="bash"
                )
                out, rc = _dbt_cmd(args)
                combined_out += f"\n$ dbt {' '.join(args)}\n{out}\n"
                log_area.code(combined_out, language="bash")
                if rc not in (0, 1):   # 1 = warnings only (pyarrow)
                    # check for real errors in output
                    if "ERROR" in out and "Completed with" in out:
                        all_ok = False
                        break

            elapsed = time.time() - t0
            if all_ok:
                status_area.success(f"✅ Completed in {elapsed:.1f}s")
                st.cache_data.clear()   # refresh any cached sf_query data
            else:
                status_area.error(f"❌ Failed after {elapsed:.1f}s — check output above")

        # ── Quick stats from last run ──────────────────────────────────────
        st.divider()
        st.markdown("#### 📊 Last Run Results")
        df_rr = _parse_run_results()
        if df_rr.empty:
            st.info("No run_results.json found yet — run dbt first.")
        else:
            ok_n    = (df_rr["Status"] == "success").sum()
            err_n   = df_rr["Status"].isin(["error","fail"]).sum()
            skip_n  = (df_rr["Status"] == "skip").sum()
            k1, k2, k3, k4 = st.columns(4)
            k1.metric("Total Nodes",  len(df_rr))
            k2.metric("✅ Success",   ok_n,   delta=None)
            k3.metric("❌ Errors",    err_n,  delta=None)
            k4.metric("⏭️ Skipped",   skip_n, delta=None)

            # colour-code status
            def _style_status(val):
                if val == "success": return "color:#3fb950"
                if val in ("error","fail"): return "color:#f85149"
                if val == "skip":    return "color:#8b949e"
                return ""

            st.dataframe(
                df_rr.style.map(_style_status, subset=["Status"]),
                use_container_width=True,
                hide_index=True,
            )

    # ══════════════════════════════════════════════════
    # TAB 2 — Model DAG
    # ══════════════════════════════════════════════════
    with tab_dag:
        st.markdown("### 🗺️ dbt Model DAG")
        st.caption(
            "Directed acyclic graph showing every model and its upstream dependencies. "
            "Arrows flow left → right through layers."
        )

        # Layer filter
        layer_filter = st.multiselect(
            "Highlight layers",
            options=list(LAYER_COLOR.keys()),
            default=list(LAYER_COLOR.keys()),
            key="dag_layer_filter",
        )
        highlight_nodes = {n[0] for n in NODES if n[1] in layer_filter}
        fig_dag = _build_dag_fig(highlight_nodes)
        st.plotly_chart(fig_dag, use_container_width=True)

        # Layer colour legend
        cols = st.columns(5)
        for i, (layer, color) in enumerate(LAYER_COLOR.items()):
            cols[i].markdown(
                f'<span style="background:{color};padding:3px 10px;'
                f'border-radius:10px;font-size:.8rem;color:#fff">'
                f'{layer.capitalize()}</span>',
                unsafe_allow_html=True,
            )

        st.divider()
        st.markdown("#### SCD Strategy")
        scd_data = {
            "Dimension":  ["dim_customer", "dim_product", "dim_store", "dim_promotion"],
            "Type":       ["SCD2",         "SCD2",        "SCD1",      "SCD1"],
            "Tracked Cols": [
                "loyalty_tier, city, state, is_active",
                "unit_price, is_active",
                "All (overwrite)",
                "All (overwrite)",
            ],
            "Mechanism": [
                "dbt snapshot (check strategy)",
                "dbt snapshot (check strategy)",
                "CREATE OR REPLACE TABLE",
                "CREATE OR REPLACE TABLE",
            ],
        }
        st.dataframe(pd.DataFrame(scd_data), use_container_width=True, hide_index=True)

    # ══════════════════════════════════════════════════
    # TAB 3 — Model Inventory
    # ══════════════════════════════════════════════════
    with tab_models:
        st.markdown("### 📋 Model Inventory")

        inv_df   = _model_inventory()
        dbt_path = DBT_DIR / "models"
        snap_path = DBT_DIR / "snapshots"

        # ── model file map: name → Path ───────────────────────────
        def _find_model_files() -> dict[str, Path]:
            """Walk dbt/models and dbt/snapshots to build name→path map."""
            files = {}
            for folder in [dbt_path, snap_path]:
                if folder.exists():
                    for p in folder.rglob("*.sql"):
                        files[p.stem] = p
            return files

        model_files = _find_model_files()

        # ── filters ───────────────────────────────────────────────
        fc1, fc2 = st.columns([2, 1])
        with fc1:
            layer_sel = st.multiselect(
                "Filter by layer",
                options=["staging", "snapshot", "warehouse", "mart"],
                default=["staging", "snapshot", "warehouse", "mart"],
                key="model_inv_filter",
            )
        with fc2:
            search_q = st.text_input(
                "Search models",
                placeholder="e.g. customer or fact",
                key="model_search",
            )

        filtered = inv_df[inv_df["Layer"].isin(layer_sel)]
        if search_q:
            filtered = filtered[
                filtered["Model"].str.contains(search_q, case=False) |
                filtered["Business Logic"].str.contains(search_q, case=False)
            ]

        # ── colour rows by layer ──────────────────────────────────
        def _row_color(row):
            c = {
                "staging":   "background-color:#2a1a0a",
                "snapshot":  "background-color:#2a2000",
                "warehouse": "background-color:#0a1f0a",
                "mart":      "background-color:#1a0a2a",
            }.get(row["Layer"], "")
            return [c] * len(row)

        # ── layout: table left | SQL viewer right ─────────────────
        left_col, right_col = st.columns([1, 1])

        with left_col:
            st.markdown(f"**{len(filtered)} models** — click a row name to view SQL")
            st.dataframe(
                filtered.style.apply(_row_color, axis=1),
                use_container_width=True,
                hide_index=True,
                height=420,
            )

        with right_col:
            st.markdown("#### 📄 Model SQL Viewer")

            # model selector — all models from inventory
            all_model_names = inv_df["Model"].tolist()
            # default to first visible model
            default_model = filtered["Model"].iloc[0] if not filtered.empty else all_model_names[0]

            selected_model = st.selectbox(
                "Select model",
                options=all_model_names,
                index=all_model_names.index(default_model) if default_model in all_model_names else 0,
                key="sql_viewer_model",
                help="Choose any model to view its SQL source",
            )

            # find the file
            sql_path = model_files.get(selected_model)

            if sql_path and sql_path.exists():
                sql_content = sql_path.read_text(encoding="utf-8")
                row = inv_df[inv_df["Model"] == selected_model].iloc[0]

                # metadata badges
                layer_colors = {
                    "staging":   "#f0883e", "snapshot":  "#e3b341",
                    "warehouse": "#3fb950", "mart":      "#bc8cff",
                }
                lcolor = layer_colors.get(row["Layer"], "#58a6ff")
                mat_colors = {
                    "view":        "#388bfd", "table":      "#3fb950",
                    "incremental": "#d29922", "snapshot table": "#e3b341",
                }
                mcolor = mat_colors.get(row["Materialisation"], "#8b949e")

                st.markdown(
                    f'<div style="display:flex;gap:8px;flex-wrap:wrap;margin-bottom:8px">'
                    f'<span style="background:{lcolor};padding:3px 10px;border-radius:10px;'
                    f'font-size:.78rem;color:#fff;font-weight:600">{row["Layer"]}</span>'
                    f'<span style="background:{mcolor};padding:3px 10px;border-radius:10px;'
                    f'font-size:.78rem;color:#fff;font-weight:600">{row["Materialisation"]}</span>'
                    f'<span style="background:#161b22;padding:3px 10px;border-radius:10px;'
                    f'font-size:.78rem;color:#8b949e;border:1px solid #30363d">'
                    f'{sql_path.relative_to(DBT_DIR)}</span>'
                    f'</div>',
                    unsafe_allow_html=True,
                )
                st.caption(f"**Depends on:** {row['Depends On']}  |  {row['Business Logic']}")

                # line count
                lines = sql_content.splitlines()
                st.caption(f"{len(lines)} lines · {len(sql_content):,} chars")

                # SQL code display
                st.code(sql_content, language="sql")

                # copy hint
                st.caption("💡 Use the copy button (top-right of code block) to copy SQL")

            elif selected_model:
                st.warning(
                    f"SQL file not found for `{selected_model}`. "
                    f"Expected at `dbt/models/.../{selected_model}.sql`"
                )
                st.info("Run `dbt compile` to generate compiled SQL in `dbt/target/compiled/`")

        # ── compiled SQL viewer ───────────────────────────────────
        st.divider()
        with st.expander("🔧 View Compiled SQL (after `dbt compile`)", expanded=False):
            compiled_base = DBT_DIR / "target" / "compiled" / "sportzone"
            compiled_files: dict[str, Path] = {}
            if compiled_base.exists():
                for p in compiled_base.rglob("*.sql"):
                    compiled_files[p.stem] = p

            if compiled_files:
                comp_model = st.selectbox(
                    "Compiled model",
                    sorted(compiled_files.keys()),
                    key="compiled_model_sel",
                )
                comp_path = compiled_files.get(comp_model)
                if comp_path and comp_path.exists():
                    comp_sql = comp_path.read_text(encoding="utf-8")
                    st.caption(
                        f"Compiled: `{comp_path.relative_to(DBT_DIR)}`  "
                        f"({len(comp_sql.splitlines())} lines)"
                    )
                    st.code(comp_sql, language="sql")
            else:
                st.info(
                    "No compiled SQL found. Run `dbt compile` or `dbt run` first.\n\n"
                    "```bash\ncd retail_snowflake_project/dbt\ndbt compile\n```"
                )

        # ── row counts ────────────────────────────────────────────
        st.divider()
        st.markdown("#### 🔴 Live Row Counts (from Snowflake)")
        rc1, rc2 = st.columns([1, 3])
        rc_schema = rc1.multiselect(
            "Schemas",
            ["STAGING", "WAREHOUSE", "MART"],
            default=["STAGING", "WAREHOUSE", "MART"],
            key="rc_schema_filter",
        )
        if rc2.button("🔄 Fetch Row Counts", key="dbt_row_counts_btn"):
            all_queries = {
                "STAGING": {
                    "stg_customers":   "SELECT COUNT(*) FROM SPORTZONE_DEV.STAGING.stg_customers",
                    "stg_orders":      "SELECT COUNT(*) FROM SPORTZONE_DEV.STAGING.stg_orders",
                    "stg_order_items": "SELECT COUNT(*) FROM SPORTZONE_DEV.STAGING.stg_order_items",
                    "stg_products":    "SELECT COUNT(*) FROM SPORTZONE_DEV.STAGING.stg_products",
                    "stg_inventory":   "SELECT COUNT(*) FROM SPORTZONE_DEV.STAGING.stg_inventory",
                    "stg_promotions":  "SELECT COUNT(*) FROM SPORTZONE_DEV.STAGING.stg_promotions",
                    "stg_stores":      "SELECT COUNT(*) FROM SPORTZONE_DEV.STAGING.stg_stores",
                },
                "WAREHOUSE": {
                    "dim_customer":    "SELECT COUNT(*) FROM SPORTZONE_DEV.WAREHOUSE.dim_customer",
                    "dim_product":     "SELECT COUNT(*) FROM SPORTZONE_DEV.WAREHOUSE.dim_product",
                    "dim_store":       "SELECT COUNT(*) FROM SPORTZONE_DEV.WAREHOUSE.dim_store",
                    "dim_promotion":   "SELECT COUNT(*) FROM SPORTZONE_DEV.WAREHOUSE.dim_promotion",
                    "dim_date":        "SELECT COUNT(*) FROM SPORTZONE_DEV.WAREHOUSE.dim_date",
                    "fact_sales":      "SELECT COUNT(*) FROM SPORTZONE_DEV.WAREHOUSE.fact_sales",
                    "fact_inventory":  "SELECT COUNT(*) FROM SPORTZONE_DEV.WAREHOUSE.fact_inventory",
                },
                "MART": {
                    "mart_daily_sales_summary":     "SELECT COUNT(*) FROM SPORTZONE_DEV.MART.mart_daily_sales_summary",
                    "mart_customer_360":            "SELECT COUNT(*) FROM SPORTZONE_DEV.MART.mart_customer_360",
                    "mart_product_performance":     "SELECT COUNT(*) FROM SPORTZONE_DEV.MART.mart_product_performance",
                    "mart_inventory_status":        "SELECT COUNT(*) FROM SPORTZONE_DEV.MART.mart_inventory_status",
                    "mart_promotion_effectiveness": "SELECT COUNT(*) FROM SPORTZONE_DEV.MART.mart_promotion_effectiveness",
                },
            }
            counts = []
            flat_queries = {
                f"{schema}.{model}": sql
                for schema, models in all_queries.items()
                if schema in rc_schema
                for model, sql in models.items()
            }
            prog = st.progress(0)
            for i, (key, sql) in enumerate(flat_queries.items()):
                try:
                    df_cnt = sf_query(sql)
                    cnt = int(df_cnt.iloc[0, 0]) if not df_cnt.empty else 0
                except Exception:
                    cnt = -1
                schema_name, model_name = key.split(".", 1)
                counts.append({
                    "Schema": schema_name,
                    "Model":  model_name,
                    "Row Count": cnt,
                    "Status": "✅" if cnt >= 0 else "❌",
                })
                prog.progress((i + 1) / len(flat_queries))
            prog.empty()
            cnt_df = pd.DataFrame(counts)

            # bar chart of row counts
            if not cnt_df.empty and cnt_df["Row Count"].max() > 0:
                fig = go.Figure()
                for schema_name in cnt_df["Schema"].unique():
                    sub = cnt_df[cnt_df["Schema"] == schema_name]
                    schema_colors = {
                        "STAGING":   "#f0883e",
                        "WAREHOUSE": "#3fb950",
                        "MART":      "#bc8cff",
                    }
                    fig.add_bar(
                        name=schema_name,
                        x=sub["Model"],
                        y=sub["Row Count"],
                        marker_color=schema_colors.get(schema_name, "#58a6ff"),
                    )
                BG_  = "#0d1117"; BG2_ = "#161b22"; FONT_ = "#f0f6fc"; GRID_ = "#30363d"
                fig.update_layout(
                    paper_bgcolor=BG_, plot_bgcolor=BG2_,
                    font=dict(color=FONT_),
                    margin=dict(l=8, r=8, t=36, b=8),
                    barmode="group", height=260,
                    title="Row Counts by Model",
                    legend=dict(orientation="h", yanchor="bottom", y=1.02),
                )
                fig.update_xaxes(tickangle=-35, showgrid=False)
                fig.update_yaxes(showgrid=True, gridcolor=GRID_)
                st.plotly_chart(fig, use_container_width=True)

            st.dataframe(
                cnt_df.style.format({"Row Count": "{:,}"}),
                use_container_width=True,
                hide_index=True,
            )

    # ══════════════════════════════════════════════════
    # TAB 4 — Test Results
    # ══════════════════════════════════════════════════
    with tab_tests:
        st.markdown("### ✅ Test Results")

        # parse test results from run_results.json if available
        rr_path = DBT_DIR / "target" / "run_results.json"
        if rr_path.exists():
            try:
                data = json.loads(rr_path.read_text())
                test_rows = []
                for r in data.get("results", []):
                    uid = r.get("unique_id", "")
                    if not uid.startswith("test."):
                        continue
                    test_rows.append({
                        "Test":    uid.replace("test.sportzone.", ""),
                        "Status":  r.get("status", ""),
                        "Failures": r.get("failures", 0),
                        "Time (s)": round(r.get("execution_time", 0), 2),
                        "Message":  r.get("message", "")[:80],
                    })
                if test_rows:
                    test_df = pd.DataFrame(test_rows)
                    pass_n = (test_df["Status"] == "pass").sum()
                    warn_n = (test_df["Status"] == "warn").sum()
                    fail_n = test_df["Status"].isin(["error", "fail"]).sum()

                    k1, k2, k3, k4 = st.columns(4)
                    k1.metric("Total Tests", len(test_df))
                    k2.metric("✅ Pass",  pass_n)
                    k3.metric("⚠️ Warn",  warn_n)
                    k4.metric("❌ Fail",  fail_n)

                    if fail_n == 0 and warn_n <= 2:
                        st.success("All tests passing! (2 data-quality warnings on NULL order_dates in source data)")
                    elif fail_n > 0:
                        st.error(f"{fail_n} test(s) failing — check dbt output above.")

                    def _style_test(val):
                        if val == "pass":  return "color:#3fb950"
                        if val == "warn":  return "color:#d29922"
                        if val in ("error","fail"): return "color:#f85149"
                        return ""

                    st.dataframe(
                        test_df.style.map(_style_test, subset=["Status"]),
                        use_container_width=True, hide_index=True,
                    )
                else:
                    st.info("No test results found in run_results.json. Run `dbt test` first.")
            except Exception as e:
                st.warning(f"Could not parse run_results.json: {e}")
        else:
            st.info("No run_results.json yet. Use the **▶️ Run dbt** tab to execute tests.")

        st.divider()
        st.markdown("#### 📝 Test Catalogue")
        test_cat = [
            ("unique",           "stg_customers.cust_id, stg_products.prd_id, stg_orders.order_id, …",   "Generic", "No duplicate business keys in staging"),
            ("not_null",         "All primary/FK columns across all layers",                              "Generic", "No NULLs on key columns"),
            ("accepted_values",  "loyalty_tier, gender, price_tier, recency_segment",                    "Generic", "Only valid enum values allowed"),
            ("relationships",    "fact_sales.cust_sk → dim_customer, prd_sk → dim_product, …",          "Generic", "Referential integrity between fact and dims"),
            ("singular",         "assert_fact_sales_no_negative_revenue",                                "Custom",  "No negative net_revenue on non-returned orders"),
            ("singular",         "assert_fact_sales_fk_integrity",                                       "Custom",  "Every cust_sk in fact_sales exists in dim_customer"),
            ("warn",             "not_null on order_date_key, stg_orders.order_date",                    "Custom",  "Data quality warning: ~5K orders have NULL dates in source"),
        ]
        st.dataframe(
            pd.DataFrame(test_cat, columns=["Type", "Applies To", "Category", "Purpose"]),
            use_container_width=True, hide_index=True,
        )

    # ══════════════════════════════════════════════════
    # TAB 5 — Documentation
    # ══════════════════════════════════════════════════
    with tab_docs:
        st.markdown("### 📚 dbt Project Documentation")

        doc_section = st.radio(
            "Section",
            ["📁 Project Structure", "⚙️ Commands", "🔧 Macros",
             "🌀 Jinja & Variables", "📦 Materialisations", "📸 Snapshots (SCD2)",
             "🧪 Testing", "🔗 Packages & Config"],
            horizontal=True,
            key="doc_section",
            label_visibility="collapsed",
        )

        st.divider()

        # ─── 1. Project Structure ────────────────────────────────
        if doc_section == "📁 Project Structure":
            st.markdown("#### 📁 Project Layout")
            st.code("""dbt/
├── dbt_project.yml        ← project name, profile, model paths, schema config
├── profiles.yml           ← Snowflake connection (keypair auth, COMPUTE_WH)
├── packages.yml           ← dbt_utils 1.4 · audit_helper 0.14
│
├── models/
│   ├── staging/           ← 7 VIEWS  — cleanse & deduplicate RAW tables
│   │   ├── sources.yml    ← declares RAW.* as dbt sources
│   │   ├── schema.yml     ← column tests (unique, not_null, accepted_values)
│   │   ├── stg_customers.sql
│   │   ├── stg_products.sql
│   │   ├── stg_stores.sql
│   │   ├── stg_orders.sql
│   │   ├── stg_order_items.sql
│   │   ├── stg_inventory.sql
│   │   └── stg_promotions.sql
│   │
│   ├── warehouse/         ← 7 TABLES — star schema (dims + incremental facts)
│   │   ├── schema.yml     ← FK relationship tests
│   │   ├── dim_date.sql
│   │   ├── dim_customer.sql   ← SCD2 via snap_customers
│   │   ├── dim_product.sql    ← SCD2 via snap_products
│   │   ├── dim_store.sql      ← SCD1 upsert
│   │   ├── dim_promotion.sql  ← SCD1 upsert
│   │   ├── fact_sales.sql     ← incremental MERGE on order_item_id
│   │   └── fact_inventory.sql ← incremental MERGE on inventory_id × date
│   │
│   └── mart/              ← 5 VIEWS — business aggregates for dashboard
│       ├── schema.yml
│       ├── mart_daily_sales_summary.sql
│       ├── mart_customer_360.sql
│       ├── mart_product_performance.sql
│       ├── mart_inventory_status.sql
│       └── mart_promotion_effectiveness.sql
│
├── snapshots/             ← SCD2 history tables
│   ├── snap_customers.sql ← tracks loyalty_tier, city, state, is_active
│   └── snap_products.sql  ← tracks unit_price, is_active
│
├── macros/                ← reusable Jinja functions
│   ├── generate_schema_name.sql  ← prevents RAW_STAGING prefix
│   ├── age_band.sql              ← DOB → age bracket CASE expression
│   └── price_tier.sql            ← unit_price → BUDGET/MID/PREMIUM
│
├── tests/                 ← custom singular tests
│   ├── assert_fact_sales_no_negative_revenue.sql
│   └── assert_fact_sales_fk_integrity.sql
│
└── analyses/              ← ad-hoc SQL (not materialized)
    └── revenue_by_channel.sql""", language="text")

            st.divider()
            st.markdown("#### Naming Conventions")
            st.dataframe(pd.DataFrame([
                ("stg_*",  "Staging views",         "STAGING",   "view",             "Cleanse + deduplicate RAW"),
                ("snap_*", "dbt Snapshots (SCD2)",  "SNAPSHOTS", "snapshot table",   "Track slowly changing attributes"),
                ("dim_*",  "Dimension tables",      "WAREHOUSE", "table",            "Star schema dimensions"),
                ("fact_*", "Fact tables",           "WAREHOUSE", "incremental",      "Grain-level measurements"),
                ("mart_*", "Mart views",            "MART",      "view",             "Pre-aggregated for dashboard"),
            ], columns=["Prefix", "Type", "Schema", "Materialisation", "Purpose"]),
            use_container_width=True, hide_index=True)

        # ─── 2. Commands ─────────────────────────────────────────
        elif doc_section == "⚙️ Commands":
            st.markdown("#### ⚙️ dbt CLI Commands")

            cmd_tabs = st.tabs(["🔧 Setup", "▶️ Run", "✅ Test", "📚 Docs", "🎯 Selectors"])

            with cmd_tabs[0]:
                st.code("""# Install dbt-snowflake adapter
pip install dbt-snowflake==1.8.*

# From the dbt directory
cd retail_snowflake_project/dbt

# Install packages (dbt_utils, audit_helper)
dbt deps

# Test Snowflake connection
dbt debug

# Clean build artifacts
dbt clean""", language="bash")

            with cmd_tabs[1]:
                st.code("""# Build all models in DAG order
dbt run

# Snapshots first (SCD2) then run
dbt snapshot && dbt run

# Rebuild incremental models from scratch
dbt run --full-refresh

# Build only mart models
dbt run --select tag:mart

# Build dim_customer and everything downstream
dbt run --select dim_customer+

# Build everything upstream of fact_sales
dbt run --select +fact_sales

# Build a specific model
dbt run --select stg_orders

# Build all warehouse tables
dbt run --select warehouse.*

# Build (run + test) in one command
dbt build""", language="bash")

            with cmd_tabs[2]:
                st.code("""# Run all tests
dbt test

# Test a specific model
dbt test --select dim_customer

# Test only generic tests
dbt test --select test_type:generic

# Test only singular (custom) tests
dbt test --select test_type:singular

# Run tests for a tag
dbt test --select tag:facts""", language="bash")

            with cmd_tabs[3]:
                st.code("""# Generate docs site
dbt docs generate

# Serve docs locally (http://localhost:8080)
dbt docs serve

# Compile SQL without running
dbt compile

# Show model SQL after compilation
dbt compile --select fact_sales
cat target/compiled/sportzone/models/warehouse/fact_sales.sql""", language="bash")

            with cmd_tabs[4]:
                st.markdown("##### Node Selectors")
                st.dataframe(pd.DataFrame([
                    ("model_name",           "dbt run --select dim_customer",         "Specific model by name"),
                    ("model_name+",          "dbt run --select dim_customer+",        "Model + all downstream"),
                    ("+model_name",          "dbt run --select +fact_sales",          "Model + all upstream"),
                    ("tag:tagname",          "dbt run --select tag:facts",            "All models with tag"),
                    ("folder.*",             "dbt run --select warehouse.*",          "All in folder"),
                    ("source:raw",           "dbt run --select source:raw",           "All using raw source"),
                    ("path:models/mart",     "dbt run --select path:models/mart",     "All in path"),
                    ("config.materialized:incremental",
                     "dbt run --select config.materialized:incremental",
                     "All incremental models"),
                ], columns=["Selector", "Example", "What It Does"]),
                use_container_width=True, hide_index=True)

        # ─── 3. Macros ───────────────────────────────────────────
        elif doc_section == "🔧 Macros":
            st.markdown("#### 🔧 Macros — Reusable Jinja Functions")
            st.info("Macros in dbt are Jinja functions you define once and call across multiple models. "
                    "They prevent duplication and enforce consistency.", icon="💡")

            macro_sel = st.selectbox(
                "Select macro to view",
                ["generate_schema_name", "age_band", "price_tier",
                 "Writing your own macro", "Using dbt_utils macros"],
                key="macro_sel",
            )

            if macro_sel == "generate_schema_name":
                st.markdown("**Purpose:** Override dbt's default schema naming to use custom schema as-is. "
                            "Without this, dbt prefixes `target.schema_` to every custom schema "
                            "(e.g. `RAW_STAGING` instead of `STAGING`).")
                st.code("""-- macros/generate_schema_name.sql
{% macro generate_schema_name(custom_schema_name, node) %}
  {%- if custom_schema_name is none -%}
    {{ target.schema | trim }}
  {%- else -%}
    {{ custom_schema_name | trim }}
  {%- endif -%}
{% endmacro %}

-- dbt_project.yml sets custom schemas per layer:
models:
  sportzone:
    staging:
      +schema: STAGING      -- becomes SPORTZONE_DEV.STAGING (not RAW_STAGING)
    warehouse:
      +schema: WAREHOUSE
    mart:
      +schema: MART""", language="sql")

            elif macro_sel == "age_band":
                st.markdown("**Purpose:** Convert a date-of-birth column into an age band string. "
                            "Called inside `dim_customer.sql` — centralises the CASE logic in one place.")
                st.code("""-- macros/age_band.sql
{% macro age_band(dob_column) %}
  case
    when datediff(year, {{ dob_column }}, current_date()) < 25 then '18-24'
    when datediff(year, {{ dob_column }}, current_date()) < 35 then '25-34'
    when datediff(year, {{ dob_column }}, current_date()) < 45 then '35-44'
    when datediff(year, {{ dob_column }}, current_date()) < 55 then '45-54'
    else '55+'
  end
{% endmacro %}

-- Usage in dim_customer.sql:
select
    cust_id,
    {{ age_band('date_of_birth') }} as age_band,
    ...
from snap""", language="sql")

            elif macro_sel == "price_tier":
                st.markdown("**Purpose:** Map a numeric price to a tier label. "
                            "Called in `dim_product.sql` — thresholds in one file, not scattered everywhere.")
                st.code("""-- macros/price_tier.sql
{% macro price_tier(price_column) %}
  case
    when {{ price_column }} < 50  then 'BUDGET'
    when {{ price_column }} < 120 then 'MID'
    else 'PREMIUM'
  end
{% endmacro %}

-- Usage in dim_product.sql:
select
    prd_id,
    unit_price,
    {{ price_tier('unit_price') }} as price_tier,
    ...
from snap""", language="sql")

            elif macro_sel == "Writing your own macro":
                st.code("""-- macros/safe_divide.sql
-- Usage: {{ safe_divide('numerator_col', 'denominator_col') }}
{% macro safe_divide(numerator, denominator, default=0) %}
  case
    when {{ denominator }} = 0 or {{ denominator }} is null
    then {{ default }}
    else {{ numerator }} / {{ denominator }}
  end
{% endmacro %}

-- macros/current_timestamp_utc.sql
{% macro current_timestamp_utc() %}
  convert_timezone('UTC', current_timestamp())
{% endmacro %}

-- macros/is_weekday.sql
{% macro is_weekday(date_column) %}
  case when dayofweek({{ date_column }}) in (1, 7)
    then false else true
  end
{% endmacro %}

-- macros/cents_to_dollars.sql
{% macro cents_to_dollars(col) %}
  round({{ col }} / 100.0, 2)
{% endmacro %}""", language="sql")

            elif macro_sel == "Using dbt_utils macros":
                st.code("""-- dbt_utils is installed via packages.yml
-- Surrogate key (MD5 hash of multiple columns)
{{ dbt_utils.generate_surrogate_key(['order_id', 'product_id']) }}

-- Date spine (generate a row for every date in a range)
{{ dbt_utils.date_spine(
    datepart = 'day',
    start_date = "cast('2023-01-01' as date)",
    end_date   = "cast('2026-12-31' as date)"
) }}

-- Union relations (UNION ALL multiple models)
{{ dbt_utils.union_relations(
    relations=[ref('orders_2023'), ref('orders_2024')]
) }}

-- Pivot (convert rows to columns)
{{ dbt_utils.pivot(
    'channel',
    dbt_utils.get_column_values(ref('stg_orders'), 'channel'),
    agg='sum',
    then_value='net_amt'
) }}

-- Star (SELECT all columns except some)
{{ dbt_utils.star(ref('stg_customers'), except=['_src_file', '_load_ts']) }}""",
                    language="sql")

        # ─── 4. Jinja & Variables ────────────────────────────────
        elif doc_section == "🌀 Jinja & Variables":
            st.markdown("#### 🌀 Jinja Templating in dbt")
            st.info("dbt SQL files are Jinja templates. Everything inside `{{ }}` or `{% %}` "
                    "is evaluated at compile time — before SQL runs in Snowflake.", icon="💡")

            jinja_sel = st.selectbox(
                "Topic",
                ["ref() & source()", "Variables (var)", "Environment (env_var)",
                 "Conditionals (if/else)", "Loops (for)", "set & do", "Filters",
                 "Config blocks", "this & model metadata"],
                key="jinja_topic",
            )

            if jinja_sel == "ref() & source()":
                st.code("""-- ref() — reference another dbt model
-- dbt resolves this to the correct schema.table at compile time
select * from {{ ref('stg_orders') }}
-- compiles to: SPORTZONE_DEV.STAGING.stg_orders

-- source() — reference a raw source table declared in sources.yml
select * from {{ source('raw', 'RAW_ORDERS') }}
-- compiles to: SPORTZONE_DEV.RAW.RAW_ORDERS

-- sources.yml declaration:
sources:
  - name: raw
    database: SPORTZONE_DEV
    schema: RAW
    tables:
      - name: RAW_ORDERS
      - name: RAW_CUSTOMERS

-- Why ref() instead of hardcoding?
-- 1. dbt builds a dependency graph → correct execution order
-- 2. Works across dev/prod environments automatically
-- 3. Column-level lineage in dbt docs""", language="sql")

            elif jinja_sel == "Variables (var)":
                st.code("""-- Define in dbt_project.yml
vars:
  start_date: '2023-01-01'
  env_name: 'dev'
  max_rows: 1000

-- Use in any model
where order_date >= '{{ var("start_date") }}'

-- Override at runtime
dbt run --vars '{"start_date": "2024-01-01", "max_rows": 5000}'

-- Default value if var not set
where order_date >= '{{ var("start_date", "2020-01-01") }}'

-- Conditional logic based on var
{% if var('env_name') == 'dev' %}
  limit 1000
{% endif %}

-- Boolean variable
{% if var('run_full_refresh', false) %}
  -- full refresh logic
{% else %}
  -- incremental logic
{% endif %}""", language="sql")

            elif jinja_sel == "Environment (env_var)":
                st.code("""-- Read OS environment variables (useful for secrets)
-- env_var(name, default=None) raises if not set and no default

-- In profiles.yml (connection credentials from env):
sportzone:
  outputs:
    dev:
      account: "{{ env_var('SNOWFLAKE_ACCOUNT') }}"
      user:    "{{ env_var('SNOWFLAKE_USER') }}"
      role:    "{{ env_var('SNOWFLAKE_ROLE', 'ACCOUNTADMIN') }}"

-- In models (feature flags, config):
{% if env_var('DBT_ENV', 'dev') == 'prod' %}
  {{ config(full_refresh=false) }}
{% endif %}

-- Set env vars before running:
export SNOWFLAKE_ACCOUNT="ur59759.me-central2.gcp"
export SNOWFLAKE_USER="SIDDIQUE"
dbt run""", language="yaml")

            elif jinja_sel == "Conditionals (if/else)":
                st.code("""-- Basic if/else
{% if is_incremental() %}
  where _load_ts > (select max(_load_ts) from {{ this }})
{% else %}
  -- full refresh: no filter
{% endif %}

-- if/elif/else
{% set env = var('env_name', 'dev') %}
{% if env == 'prod' %}
  {{ config(warehouse='PROD_LOAD_WH') }}
{% elif env == 'staging' %}
  {{ config(warehouse='STAGING_WH') }}
{% else %}
  {{ config(warehouse='DEV_WH') }}
{% endif %}

-- exists check
{% if target.name == 'prod' %}
  {{ config(full_refresh=false) }}    -- never full-refresh in prod
{% endif %}

-- Column conditional
select
  {% if var('include_pii', false) %}
    email,
    phone,
  {% endif %}
  cust_id,
  loyalty_tier
from {{ ref('stg_customers') }}""", language="sql")

            elif jinja_sel == "Loops (for)":
                st.code("""-- Loop over a list to generate SQL
{% set channels = ['ONLINE', 'IN-STORE', 'MOBILE'] %}

select
  order_date,
  {% for channel in channels %}
  sum(case when channel = '{{ channel }}' then net_revenue else 0 end)
    as revenue_{{ channel | lower | replace('-', '_') }}
  {{ "," if not loop.last }}
  {% endfor %}
from {{ ref('fact_sales') }}
group by 1

-- loop.index  → 1-based index
-- loop.index0 → 0-based index
-- loop.first  → true on first iteration
-- loop.last   → true on last iteration
-- loop.length → total number of iterations

-- Compiles to:
-- select order_date,
--   sum(case when channel = 'ONLINE' then net_revenue else 0 end) as revenue_online,
--   sum(case when channel = 'IN-STORE' then net_revenue else 0 end) as revenue_in_store,
--   sum(case when channel = 'MOBILE' then net_revenue else 0 end) as revenue_mobile
-- from MART.fact_sales group by 1""", language="sql")

            elif jinja_sel == "set & do":
                st.code("""-- set: assign a variable
{% set table_name = 'stg_orders' %}
select * from {{ ref(table_name) }}

-- set with list
{% set dim_tables = ['dim_customer', 'dim_product', 'dim_store'] %}
{% for t in dim_tables %}
  -- process {{ t }}
{% endfor %}

-- set with dict
{% set scd2_dims = {
  'dim_customer': 'cust_id',
  'dim_product':  'prd_id'
} %}

-- do: execute a statement without outputting anything
-- (requires dbt-labs/dbt_utils or jinja2.ext.do extension)
{% do log("Building model: " ~ model.name, info=true) %}

-- log() — print to dbt console output
{% do log("Row count will be: " ~ 1000, info=true) %}

-- execute: run a query and capture results
{% set results = run_query("select distinct channel from " ~ ref('stg_orders')) %}
{% set channels = results.columns[0].values() %}""", language="sql")

            elif jinja_sel == "Filters":
                st.code("""-- Jinja filters transform values using the | operator

-- upper / lower
{{ 'hello' | upper }}           -- HELLO
{{ var('env_name') | lower }}   -- dev

-- replace
{{ 'IN-STORE' | replace('-', '_') }}  -- IN_STORE

-- trim whitespace
{{ '  SIDDIQUE  ' | trim }}     -- SIDDIQUE

-- default (if None or undefined)
{{ var('start_date') | default('2023-01-01') }}

-- join (list → string)
{% set cols = ['a', 'b', 'c'] %}
{{ cols | join(', ') }}         -- a, b, c

-- length
{{ ['a', 'b', 'c'] | length }} -- 3

-- selectattr (filter list of objects)
{% set current_dims = dimensions | selectattr('is_current', 'eq', true) | list %}

-- map (extract attribute from list)
{{ columns | map(attribute='name') | list }}

-- Combined example
{{ 'MY_TABLE_NAME' | lower | replace('_', ' ') | title }}  -- My Table Name""",
                    language="sql")

            elif jinja_sel == "Config blocks":
                st.code("""-- Config block at top of model file
{{ config(
    materialized         = 'incremental',
    unique_key           = 'order_item_id',
    incremental_strategy = 'merge',
    cluster_by           = ['order_date_key', 'prd_sk'],
    tags                 = ['facts', 'warehouse'],
    schema               = 'WAREHOUSE',
    alias                = 'fact_sales',          -- override table name
    enabled              = true,
    full_refresh         = false,                 -- never full-refresh
    on_schema_change     = 'fail',
    post_hook            = "ALTER TABLE {{ this }} CLUSTER BY (order_date_key)",
    meta = {
        'owner': 'data_team',
        'description': 'Grain: one row per order line item'
    }
) }}

-- All config options for Snowflake:
-- materialized: view | table | incremental | ephemeral
-- unique_key: column or list of columns
-- incremental_strategy: append | merge | delete+insert
-- cluster_by: list of columns
-- transient: true (no Fail-safe, cheaper)
-- copy_grants: true
-- snowflake_warehouse: 'LARGE_WH'  -- override for this model""", language="sql")

            elif jinja_sel == "this & model metadata":
                st.code("""-- {{ this }} — fully-qualified name of current model
-- Use in incremental models and post-hooks

select * from {{ this }}
-- compiles to: SPORTZONE_DEV.WAREHOUSE.fact_sales

-- this.name       → fact_sales
-- this.schema     → WAREHOUSE
-- this.database   → SPORTZONE_DEV
-- this.identifier → fact_sales (or alias if set)

-- Common incremental pattern using {{ this }}:
{% if is_incremental() %}
  where order_date_key > (
    select max(order_date_key) from {{ this }}
  )
{% endif %}

-- model metadata in macros
-- model.name          → 'fact_sales'
-- model.config        → config dict
-- model.tags          → ['facts', 'warehouse']
-- model.path          → 'models/warehouse/fact_sales.sql'
-- model.unique_id     → 'model.sportzone.fact_sales'

-- Example: dynamic post-hook using model name
{{ config(
  post_hook = "GRANT SELECT ON {{ this }} TO ROLE SPORTZONE_ANALYST"
) }}""", language="sql")

        # ─── 5. Materialisations ─────────────────────────────────
        elif doc_section == "📦 Materialisations":
            st.markdown("#### 📦 Materialisation Strategies")

            mat_tabs = st.tabs(["view", "table", "incremental", "ephemeral"])

            with mat_tabs[0]:
                st.markdown("**view** — SQL is stored as a view. Rebuilt on every `dbt run`. No data storage cost.")
                st.code("""{{ config(materialized='view') }}

-- All staging models use view:
-- + Always shows current data from RAW tables
-- + No storage cost
-- - Slightly slower queries (re-executes SQL every time)
-- - If RAW table is dropped, view breaks

-- Example: stg_customers.sql
{{ config(materialized='view', schema='STAGING') }}

with source as (
    select * from {{ source('raw', 'RAW_CUSTOMERS') }}
),
deduped as (
    select *, row_number() over (
        partition by trim(cust_id) order by _load_ts desc
    ) as rn
    from source
    where trim(cust_id) is not null
)
select ... from deduped where rn = 1""", language="sql")

            with mat_tabs[1]:
                st.markdown("**table** — Creates a physical table. Fully rebuilt on every `dbt run`. Used for dimensions.")
                st.code("""{{ config(materialized='table', schema='WAREHOUSE') }}

-- Dimension tables use table materialisation:
-- + Fast query performance (data is stored)
-- + Predictable — you know exactly what's in it
-- - Full rebuild every run (can be slow for large tables)
-- - Use --full-refresh with incremental if needed

-- dim_date.sql — static calendar table
{{ config(
    materialized = 'table',
    tags         = ['dimensions', 'static']
) }}

with date_spine as (
    select dateadd(day, seq4(), '2023-01-01') as dt
    from table(generator(rowcount => 1461))
)
select
    to_number(to_char(dt, 'YYYYMMDD')) as date_key,
    dt as full_date,
    ...
from date_spine""", language="sql")

            with mat_tabs[2]:
                st.markdown("**incremental** — Only processes NEW rows since last run using a unique key. Used for facts.")
                st.code("""{{ config(
    materialized         = 'incremental',
    unique_key           = 'order_item_id',   -- dedup key for MERGE
    incremental_strategy = 'merge',           -- append | merge | delete+insert
    cluster_by           = ['order_date_key', 'prd_sk'],
    tags                 = ['facts']
) }}

-- The is_incremental() check:
-- First run (or --full-refresh): is_incremental() = false → no filter
-- Subsequent runs: is_incremental() = true → filter to new rows only

select
    order_date_key,
    cust_sk,
    prd_sk,
    net_revenue,
    current_timestamp() as _load_ts
from joined

{% if is_incremental() %}
  -- Only rows not already in the table
  where order_item_id not in (
    select order_item_id from {{ this }}
  )
{% endif %}

-- Rebuild from scratch:
dbt run --select fact_sales --full-refresh

-- Incremental strategies:
-- append       → INSERT only (no dedup, fastest)
-- merge        → MERGE on unique_key (safe, handles updates)
-- delete+insert → DELETE matching keys then INSERT (for partitioned tables)""",
                    language="sql")

            with mat_tabs[3]:
                st.markdown("**ephemeral** — Not materialized at all. Injected as a CTE into models that ref() it.")
                st.code("""{{ config(materialized='ephemeral') }}

-- Ephemeral models:
-- + No table/view created in database
-- + Becomes a CTE (WITH clause) inside any model that refs it
-- + Good for intermediate logic you want to keep DRY
-- - Cannot be queried directly in Snowflake
-- - Can make compiled SQL complex

-- Example: an ephemeral deduplication CTE
-- deduped_customers.sql (ephemeral)
{{ config(materialized='ephemeral') }}

select *, row_number() over (
    partition by cust_id order by _load_ts desc
) as rn
from {{ source('raw', 'RAW_CUSTOMERS') }}

-- stg_customers.sql (references ephemeral)
select * from {{ ref('deduped_customers') }}
where rn = 1

-- Compiles to:
-- with deduped_customers as (
--   select *, row_number() ...
-- )
-- select * from deduped_customers where rn = 1""", language="sql")

        # ─── 6. Snapshots ────────────────────────────────────────
        elif doc_section == "📸 Snapshots (SCD2)":
            st.markdown("#### 📸 dbt Snapshots — SCD Type 2")
            st.info("Snapshots track historical changes to records (Slowly Changing Dimensions). "
                    "dbt appends a new row whenever tracked columns change, preserving full history.",
                    icon="💡")
            st.code("""-- snapshots/snap_customers.sql
{% snapshot snap_customers %}

{{
    config(
        target_schema  = 'SNAPSHOTS',        -- write to SNAPSHOTS schema
        unique_key     = 'cust_id',          -- business key to match records
        strategy       = 'check',            -- check specific columns for changes
        check_cols     = ['loyalty_tier', 'city', 'state', 'is_active'],
        invalidate_hard_deletes = true       -- mark deleted source rows as expired
    )
}}

select * from {{ ref('stg_customers') }}

{% endsnapshot %}

-- Columns dbt adds automatically:
-- dbt_scd_id      → unique identifier for this snapshot row
-- dbt_updated_at  → when this row was inserted/updated
-- dbt_valid_from  → when this version became active
-- dbt_valid_to    → when superseded (NULL = current/active row)

-- Example snapshot result after a loyalty_tier change:
-- cust_id  loyalty_tier  dbt_valid_from  dbt_valid_to
-- C001     BRONZE        2023-01-15      2024-03-20    ← old row (expired)
-- C001     SILVER        2024-03-20      NULL          ← current row

-- Strategies:
-- check   → compare specific columns (efficient, explicit)
-- timestamp → compare updated_at column (simple, needs updated_at)

-- In dim_customer.sql — read from snapshot:
with snap as (
    select * from {{ ref('snap_customers') }}
)
select
    {{ dbt_utils.generate_surrogate_key(['cust_id', 'dbt_updated_at']) }} as cust_sk,
    cust_id,
    loyalty_tier,
    dbt_valid_from  as eff_start_dt,
    dbt_valid_to    as eff_end_dt,
    case when dbt_valid_to is null then true else false end as is_current
from snap

-- Run snapshots:
dbt snapshot

-- Run snapshot for specific model:
dbt snapshot --select snap_customers""", language="sql")

        # ─── 7. Testing ──────────────────────────────────────────
        elif doc_section == "🧪 Testing":
            st.markdown("#### 🧪 dbt Testing Framework")
            st.code("""-- schema.yml — generic tests
models:
  - name: stg_customers
    columns:
      - name: cust_id
        tests:
          - unique                                   # no duplicates
          - not_null                                 # no NULLs

      - name: loyalty_tier
        tests:
          - accepted_values:
              arguments:
                values: ['BRONZE', 'SILVER', 'GOLD', 'PLATINUM']

      - name: email
        tests:
          - not_null
          - unique

  - name: fact_sales
    columns:
      - name: order_item_id
        tests:
          - unique
          - not_null

      - name: cust_sk
        tests:
          - relationships:
              arguments:
                to: ref('dim_customer')
                field: cust_sk                       # FK integrity

-- Custom singular test: tests/assert_fact_sales_no_negative_revenue.sql
-- Returns rows that FAIL the test (0 rows = test passes)
select *
from {{ ref('fact_sales') }}
where net_revenue < 0
  and order_status not in ('RETURNED')

-- Severity configuration
- name: order_date
  tests:
    - not_null:
        config:
          severity: warn     # warn instead of error

-- Run tests:
dbt test                          # all tests
dbt test --select dim_customer    # tests for one model
dbt test --select tag:facts       # tests for tagged models

-- Test output shows:
-- 75 PASS, 2 WARN, 0 ERROR (77 total in SportZone)""", language="yaml")

        # ─── 8. Packages & Config ────────────────────────────────
        elif doc_section == "🔗 Packages & Config":
            st.markdown("#### 🔗 Packages & Project Config")

            pc_tabs = st.tabs(["packages.yml", "dbt_project.yml", "profiles.yml"])

            with pc_tabs[0]:
                st.code("""-- packages.yml — install with: dbt deps
packages:
  - package: dbt-labs/dbt_utils
    version: [">=1.0.0", "<2.0.0"]
    # Provides: generate_surrogate_key, date_spine, union_relations,
    #           pivot, star, get_column_values, and 50+ more utilities

  - package: dbt-labs/audit_helper
    version: [">=0.9.0", "<1.0.0"]
    # Provides: compare_relations, compare_column_values
    # Useful for validating refactors without breaking prod

# After adding packages:
dbt deps

# Other useful packages:
# - dbt-labs/dbt_project_evaluator  (best practices linter)
# - calogica/dbt_expectations        (Great Expectations for dbt)
# - dbt-labs/codegen                 (generate model boilerplate)""",
                    language="yaml")

            with pc_tabs[1]:
                st.code("""-- dbt_project.yml — project-wide configuration
name: 'sportzone'
version: '1.0.0'
config-version: 2

profile: 'sportzone'           # matches entry in profiles.yml

model-paths:    ["models"]
snapshot-paths: ["snapshots"]
macro-paths:    ["macros"]
test-paths:     ["tests"]
seed-paths:     ["seeds"]
analysis-paths: ["analyses"]

target-path: "target"
clean-targets: ["target", "dbt_packages"]

# Default materialisation and schema per folder
models:
  sportzone:
    staging:
      +schema:       STAGING
      +materialized: view
    warehouse:
      +schema:       WAREHOUSE
      +materialized: table
    mart:
      +schema:       MART
      +materialized: view

# Project-wide variables
vars:
  start_date:  '2023-01-01'
  env_name:    'dev'

# Snapshot config defaults
snapshots:
  sportzone:
    +target_schema: SNAPSHOTS""", language="yaml")

            with pc_tabs[2]:
                st.code("""-- profiles.yml — connection configuration
-- Location: retail_snowflake_project/dbt/profiles.yml

sportzone:
  target: dev
  outputs:
    dev:
      type:              snowflake
      account:           "ur59759.me-central2.gcp"
      user:              "SIDDIQUE"
      private_key_path:  "C:/path/to/rsa_key.p8"  # keypair = no MFA
      private_key_passphrase: ""
      role:              ACCOUNTADMIN
      database:          SPORTZONE_DEV
      warehouse:         COMPUTE_WH
      schema:            RAW
      threads:           4
      client_session_keep_alive: false
      query_tag:         dbt_sportzone

    prod:
      type:      snowflake
      account:   "{{ env_var('SNOWFLAKE_ACCOUNT') }}"
      user:      "{{ env_var('SNOWFLAKE_USER') }}"
      role:      SPORTZONE_DATA_ENG
      database:  SPORTZONE_PROD
      warehouse: PROD_LOAD_WH
      schema:    RAW
      threads:   8

# Switch between targets:
dbt run --target prod
dbt run              # uses 'target: dev'""", language="yaml")
