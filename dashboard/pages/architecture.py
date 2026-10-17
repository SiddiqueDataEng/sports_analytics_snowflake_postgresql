"""
Architecture Page — full platform diagram, tech stack, and data flow.
Shows: Data Generator → Neon OLTP → ETL Pipeline → Snowflake (RAW → dbt → MART) → Dashboard
"""
import pandas as pd
import plotly.graph_objects as go
import streamlit as st

BG   = "#0d1117"
BG2  = "#0d1117"
FONT = "#f0f6fc"
GRID = "#30363d"

# ── layer palette ─────────────────────────────────────────────────────────────
C = {
    "generator": "#58a6ff",   # blue
    "oltp":      "#1f6feb",   # darker blue
    "etl":       "#d29922",   # amber
    "raw":       "#da3633",   # red
    "staging":   "#f0883e",   # orange
    "snapshot":  "#e3b341",   # yellow
    "warehouse": "#3fb950",   # green
    "mart":      "#bc8cff",   # purple
    "dashboard": "#f78166",   # coral
}


# ─────────────────────────────────────────────────────────────────────────────
# DIAGRAM 1 — High-level end-to-end architecture (swimlane style)
# ─────────────────────────────────────────────────────────────────────────────
def _arch_diagram() -> go.Figure:
    fig = go.Figure()

    # ── swimlane backgrounds ──────────────────────────────────────────────────
    lanes = [
        ("Source Layer",          -0.10, 0.18, "#0a1628"),
        ("ETL Pipeline",           0.20, 0.38, "#1a1200"),
        ("Snowflake — RAW",        0.40, 0.58, "#1a0505"),
        ("Snowflake — dbt",        0.60, 0.88, "#051a05"),
        ("Presentation",           0.90, 1.05, "#14051a"),
    ]
    for label, x0, x1, bg in lanes:
        fig.add_shape(type="rect",
            x0=x0, x1=x1, y0=-0.25, y1=1.25,
            fillcolor=bg, line=dict(color=GRID, width=1),
            layer="below")
        fig.add_annotation(x=(x0+x1)/2, y=1.20, text=f"<b>{label}</b>",
            showarrow=False, font=dict(size=10, color="#8b949e"),
            xref="x", yref="y")

    # ── nodes: (label, sublabel, x, y, color, symbol) ────────────────────────
    nodes = [
        # Source
        ("Faker\nGenerator",   "Python + Faker",           0.04,  1.00, C["generator"], "square"),
        ("Neon Postgres\nOLTP","sportzone db\noltp.*",     0.04,  0.50, C["oltp"],      "circle"),
        ("etl_watermark",      "Incremental\ncheckpoint",  0.04,  0.05, C["oltp"],      "diamond"),

        # ETL
        ("extract.py",         "Watermark-based\nextract", 0.29,  0.80, C["etl"],       "circle"),
        ("transform.py",       "Cleanse, type-cast\ndeduplicate", 0.29, 0.50, C["etl"], "circle"),
        ("load.py",            "write_pandas()\n+ MERGE",  0.29,  0.20, C["etl"],       "circle"),
        ("pipeline.py",        "Orchestrator\n--incremental/--full", 0.29, 0.05, C["etl"], "square"),

        # RAW (Snowflake)
        ("RAW Layer\n(Bronze)", "7 tables\nAS-IS strings", 0.49,  0.70, C["raw"],       "square"),
        ("Streams",             "CDC on\nRAW tables",      0.49,  0.35, C["raw"],       "diamond"),
        ("Snowflake\nTasks",    "30-min CRON\nDAG",        0.49,  0.10, C["raw"],       "diamond"),

        # dbt layers (Snowflake)
        ("dbt\nstg_*",          "7 views\nSilver",          0.69,  1.00, C["staging"],  "circle"),
        ("dbt\nsnap_*",         "SCD2 snapshots\ncustomers, products", 0.69, 0.75, C["snapshot"], "circle"),
        ("dbt\ndim_*",          "5 dims\nGold",             0.69,  0.50, C["warehouse"],"circle"),
        ("dbt\nfact_*",         "2 facts\nincremental",     0.69,  0.28, C["warehouse"],"circle"),
        ("dbt\nmart_*",         "5 views\nPlatinum",        0.69,  0.05, C["mart"],     "circle"),

        # Presentation
        ("Streamlit\nDashboard","Analytics &\nManagement", 0.97,  0.70, C["dashboard"], "square"),
        ("BI Tools\n/ API",     "Snowflake\nconnect",      0.97,  0.30, C["dashboard"], "square"),
    ]

    idx = {n[0]: i for i, n in enumerate(nodes)}

    # ── edges: (src_label, tgt_label, edge_label, color_key) ─────────────────
    edges = [
        ("Faker\nGenerator",    "Neon Postgres\nOLTP",   "generates", "generator"),
        ("Neon Postgres\nOLTP", "extract.py",            "reads",     "etl"),
        ("etl_watermark",       "extract.py",            "checkpoint","etl"),
        ("extract.py",          "transform.py",          "DataFrames","etl"),
        ("transform.py",        "load.py",               "clean data","etl"),
        ("load.py",             "RAW Layer\n(Bronze)",   "COPY INTO", "raw"),
        ("RAW Layer\n(Bronze)", "Streams",               "CDC",       "raw"),
        ("Streams",             "Snowflake\nTasks",      "triggers",  "raw"),
        ("RAW Layer\n(Bronze)", "dbt\nstg_*",            "sources",   "staging"),
        ("dbt\nstg_*",          "dbt\nsnap_*",           "SCD2",      "snapshot"),
        ("dbt\nstg_*",          "dbt\ndim_*",            "SCD1 dims", "warehouse"),
        ("dbt\nsnap_*",         "dbt\ndim_*",            "SCD2 dims", "warehouse"),
        ("dbt\nstg_*",          "dbt\nfact_*",           "MERGE",     "warehouse"),
        ("dbt\ndim_*",          "dbt\nfact_*",           "SK joins",  "warehouse"),
        ("dbt\nfact_*",         "dbt\nmart_*",           "views",     "mart"),
        ("dbt\ndim_*",          "dbt\nmart_*",           "views",     "mart"),
        ("dbt\nmart_*",         "Streamlit\nDashboard",  "queries",   "dashboard"),
        ("dbt\nmart_*",         "BI Tools\n/ API",       "queries",   "dashboard"),
    ]

    # Draw edges
    def _hex_to_rgba(h, alpha=0.5):
        h = h.lstrip("#")
        r, g, b = int(h[0:2],16), int(h[2:4],16), int(h[4:6],16)
        return f"rgba({r},{g},{b},{alpha})"

    for src_lbl, tgt_lbl, elabel, color_key in edges:
        xi, yi = nodes[idx[src_lbl]][2], nodes[idx[src_lbl]][3]
        xj, yj = nodes[idx[tgt_lbl]][2], nodes[idx[tgt_lbl]][3]
        col = _hex_to_rgba(C[color_key], 0.55)
        # curved midpoint
        mx = (xi + xj) / 2
        fig.add_trace(go.Scatter(
            x=[xi, mx, xj], y=[yi, (yi+yj)/2, yj],
            mode="lines",
            line=dict(color=col, width=1.5),
            hoverinfo="skip",
            showlegend=False,
        ))
        # edge label at midpoint
        fig.add_annotation(
            x=mx, y=(yi+yj)/2,
            text=f'<span style="font-size:7px;color:#8b949e">{elabel}</span>',
            showarrow=False, font=dict(size=7, color="#8b949e"),
            xref="x", yref="y",
        )

    # Draw nodes
    for label, sublabel, x, y, color, symbol in nodes:
        fig.add_trace(go.Scatter(
            x=[x], y=[y],
            mode="markers+text",
            marker=dict(
                color=color, size=22,
                symbol=symbol,
                line=dict(color="#ffffff", width=1.5),
            ),
            text=[f"<b>{label}</b>"],
            textposition="top center",
            textfont=dict(size=8, color=FONT),
            hovertext=[f"<b>{label}</b><br>{sublabel}"],
            hovertemplate="%{hovertext}<extra></extra>",
            showlegend=False,
        ))
        # sublabel below
        fig.add_annotation(
            x=x, y=y - 0.085,
            text=f'<span style="font-size:7px;color:#8b949e">{sublabel}</span>',
            showarrow=False, font=dict(size=7, color="#8b949e"),
            xref="x", yref="y",
        )

    fig.update_layout(
        paper_bgcolor=BG2,
        plot_bgcolor=BG2,
        font=dict(color=FONT, size=9),
        height=620,
        margin=dict(l=8, r=8, t=50, b=8),
        title=dict(
            text="<b>SportZone — End-to-End Data Platform Architecture</b>",
            font=dict(size=14, color=FONT),
            x=0.5,
        ),
        xaxis=dict(showgrid=False, zeroline=False, showticklabels=False,
                   range=[-0.02, 1.10]),
        yaxis=dict(showgrid=False, zeroline=False, showticklabels=False,
                   range=[-0.35, 1.30]),
        hovermode="closest",
    )
    return fig


# ─────────────────────────────────────────────────────────────────────────────
# DIAGRAM 2 — dbt DAG (detailed model lineage)
# ─────────────────────────────────────────────────────────────────────────────
def _dbt_dag_diagram() -> go.Figure:
    """Detailed node-link DAG of every dbt model."""
    LAYER_COLOR = {
        "source":    C["raw"],
        "staging":   C["staging"],
        "snapshot":  C["snapshot"],
        "warehouse": C["warehouse"],
        "mart":      C["mart"],
    }

    nodes = [
        # RAW sources
        ("RAW_CUSTOMERS",   "source",    0.00, 1.00),
        ("RAW_PRODUCTS",    "source",    0.00, 0.83),
        ("RAW_STORES",      "source",    0.00, 0.67),
        ("RAW_ORDERS",      "source",    0.00, 0.50),
        ("RAW_ORDER_ITEMS", "source",    0.00, 0.33),
        ("RAW_INVENTORY",   "source",    0.00, 0.17),
        ("RAW_PROMOTIONS",  "source",    0.00, 0.00),
        # staging
        ("stg_customers",   "staging",   0.22, 1.00),
        ("stg_products",    "staging",   0.22, 0.83),
        ("stg_stores",      "staging",   0.22, 0.67),
        ("stg_orders",      "staging",   0.22, 0.50),
        ("stg_order_items", "staging",   0.22, 0.33),
        ("stg_inventory",   "staging",   0.22, 0.17),
        ("stg_promotions",  "staging",   0.22, 0.00),
        # snapshots
        ("snap_customers",  "snapshot",  0.44, 0.92),
        ("snap_products",   "snapshot",  0.44, 0.75),
        # warehouse dims
        ("dim_date",        "warehouse", 0.44, 0.42),
        ("dim_customer",    "warehouse", 0.66, 1.00),
        ("dim_product",     "warehouse", 0.66, 0.83),
        ("dim_store",       "warehouse", 0.66, 0.67),
        ("dim_promotion",   "warehouse", 0.66, 0.50),
        # warehouse facts
        ("fact_sales",      "warehouse", 0.66, 0.25),
        ("fact_inventory",  "warehouse", 0.66, 0.08),
        # mart
        ("mart_daily_sales_summary",     "mart", 0.88, 1.00),
        ("mart_customer_360",            "mart", 0.88, 0.80),
        ("mart_product_performance",     "mart", 0.88, 0.60),
        ("mart_inventory_status",        "mart", 0.88, 0.40),
        ("mart_promotion_effectiveness", "mart", 0.88, 0.20),
    ]
    idx = {n[0]: i for i, n in enumerate(nodes)}

    edges = [
        ("RAW_CUSTOMERS",   "stg_customers"),
        ("RAW_PRODUCTS",    "stg_products"),
        ("RAW_STORES",      "stg_stores"),
        ("RAW_ORDERS",      "stg_orders"),
        ("RAW_ORDER_ITEMS", "stg_order_items"),
        ("RAW_INVENTORY",   "stg_inventory"),
        ("RAW_PROMOTIONS",  "stg_promotions"),
        ("stg_customers",   "snap_customers"),
        ("stg_products",    "snap_products"),
        ("snap_customers",  "dim_customer"),
        ("snap_products",   "dim_product"),
        ("stg_stores",      "dim_store"),
        ("stg_promotions",  "dim_promotion"),
        ("stg_orders",      "fact_sales"),
        ("stg_order_items", "fact_sales"),
        ("stg_inventory",   "fact_inventory"),
        ("dim_customer",    "fact_sales"),
        ("dim_product",     "fact_sales"),
        ("dim_store",       "fact_sales"),
        ("dim_promotion",   "fact_sales"),
        ("dim_date",        "fact_sales"),
        ("dim_product",     "fact_inventory"),
        ("dim_store",       "fact_inventory"),
        ("dim_date",        "fact_inventory"),
        ("fact_sales",      "mart_daily_sales_summary"),
        ("fact_sales",      "mart_customer_360"),
        ("fact_sales",      "mart_product_performance"),
        ("fact_sales",      "mart_promotion_effectiveness"),
        ("dim_customer",    "mart_customer_360"),
        ("fact_inventory",  "mart_inventory_status"),
        ("dim_product",     "mart_inventory_status"),
        ("dim_store",       "mart_inventory_status"),
    ]

    def _hex_to_rgba(h, alpha=0.35):
        h = h.lstrip("#")
        r, g, b = int(h[0:2],16), int(h[2:4],16), int(h[4:6],16)
        return f"rgba({r},{g},{b},{alpha})"

    fig = go.Figure()

    # edges
    for src, tgt in edges:
        xi, yi = nodes[idx[src]][2], nodes[idx[src]][3]
        xj, yj = nodes[idx[tgt]][2], nodes[idx[tgt]][3]
        col = _hex_to_rgba(LAYER_COLOR[nodes[idx[src]][1]])
        fig.add_trace(go.Scatter(
            x=[xi, xj, None], y=[yi, yj, None],
            mode="lines",
            line=dict(color=col, width=1.2),
            hoverinfo="skip", showlegend=False,
        ))

    # nodes grouped by layer for legend
    xs = [n[2] for n in nodes]
    ys = [n[3] for n in nodes]
    labels = [n[0] for n in nodes]
    layers = [n[1] for n in nodes]

    for layer, lcolor in LAYER_COLOR.items():
        mask = [i for i, l in enumerate(layers) if l == layer]
        short_labels = [labels[i].replace("_", "_") for i in mask]
        fig.add_trace(go.Scatter(
            x=[xs[i] for i in mask],
            y=[ys[i] for i in mask],
            mode="markers+text",
            name=layer.capitalize(),
            marker=dict(color=lcolor, size=13,
                        line=dict(color="#ffffff", width=1)),
            text=short_labels,
            textposition="middle right",
            textfont=dict(size=8, color=FONT),
            hovertemplate="%{text}<extra></extra>",
        ))

    # column headers
    for label, lx, lcolor in [
        ("Sources (RAW)", 0.00, LAYER_COLOR["source"]),
        ("Staging",       0.22, LAYER_COLOR["staging"]),
        ("Snapshots",     0.44, LAYER_COLOR["snapshot"]),
        ("Warehouse",     0.66, LAYER_COLOR["warehouse"]),
        ("Mart",          0.88, LAYER_COLOR["mart"]),
    ]:
        fig.add_annotation(x=lx, y=1.10, text=f"<b>{label}</b>",
            showarrow=False, font=dict(size=11, color=lcolor),
            xref="x", yref="y")

    fig.update_layout(
        paper_bgcolor=BG2, plot_bgcolor=BG2,
        font=dict(color=FONT, size=9),
        height=540,
        margin=dict(l=8, r=8, t=50, b=8),
        title=dict(
            text="<b>dbt Model DAG — Full Lineage (Sources → Staging → Warehouse → Mart)</b>",
            font=dict(size=13, color=FONT), x=0.5,
        ),
        xaxis=dict(showgrid=False, zeroline=False, showticklabels=False,
                   range=[-0.05, 1.20]),
        yaxis=dict(showgrid=False, zeroline=False, showticklabels=False,
                   range=[-0.10, 1.18]),
        legend=dict(orientation="h", yanchor="bottom", y=-0.10,
                    font=dict(size=9), bgcolor="rgba(0,0,0,0)"),
        hovermode="closest",
    )
    return fig


# ─────────────────────────────────────────────────────────────────────────────
# DIAGRAM 3 — Medallion / data quality flow (horizontal bar)
# ─────────────────────────────────────────────────────────────────────────────
def _medallion_diagram() -> go.Figure:
    layers = [
        ("🐘 Neon\nOLTP",     C["oltp"],      "Transactional source\nNeon Postgres (serverless)\noltp.* schema"),
        ("🐍 ETL\nPipeline",  C["etl"],       "Python extract/transform/load\nIncremental watermarks\nFaker data generator"),
        ("🥉 RAW\n(Bronze)",  C["raw"],       "7 raw tables\nAll-VARCHAR strings\nAs-is from source"),
        ("🥈 STAGING\n(Silver)", C["staging"],"7 dbt views\nType-cast + deduplicated\nBusiness logic applied"),
        ("📸 Snapshots",      C["snapshot"],  "SCD2 history\nsnap_customers\nsnap_products"),
        ("🥇 WAREHOUSE\n(Gold)", C["warehouse"],"5 dims + 2 facts\nStar schema\nIncremental MERGE"),
        ("💎 MART\n(Platinum)", C["mart"],    "5 business views\nPre-aggregated\nDashboard-ready"),
        ("📊 Dashboard",      C["dashboard"], "Streamlit app\nAnalytics + AI + ML\nReal-time queries"),
    ]

    n = len(layers)
    fig = go.Figure()

    for i, (label, color, tooltip) in enumerate(layers):
        x = i / (n - 1)
        # box
        fig.add_shape(type="rect",
            x0=x - 0.055, x1=x + 0.055, y0=0.3, y1=0.7,
            fillcolor=color,
            line=dict(color="#ffffff", width=1.5),
        )
        # label inside box
        fig.add_annotation(x=x, y=0.50,
            text=f"<b>{label}</b>",
            showarrow=False,
            font=dict(size=9, color="#ffffff"),
            align="center",
        )
        # tooltip below
        fig.add_annotation(x=x, y=0.18,
            text=f'<span style="font-size:8px;color:#8b949e">{tooltip}</span>',
            showarrow=False,
            font=dict(size=8, color="#8b949e"),
            align="center",
        )
        # arrow to next
        if i < n - 1:
            xn = (i + 1) / (n - 1)
            fig.add_annotation(
                x=x + 0.055 + 0.005, y=0.50,
                ax=xn - 0.055 - 0.005, ay=0.50,
                xref="x", yref="y", axref="x", ayref="y",
                showarrow=True,
                arrowhead=2, arrowwidth=2,
                arrowcolor="#30363d",
            )

    fig.update_layout(
        paper_bgcolor=BG2, plot_bgcolor=BG2,
        font=dict(color=FONT),
        height=300,
        margin=dict(l=10, r=10, t=40, b=60),
        title=dict(
            text="<b>Medallion Architecture — Data Quality Progression</b>",
            font=dict(size=13, color=FONT), x=0.5,
        ),
        xaxis=dict(showgrid=False, zeroline=False, showticklabels=False,
                   range=[-0.07, 1.07]),
        yaxis=dict(showgrid=False, zeroline=False, showticklabels=False,
                   range=[0.0, 0.85]),
    )
    return fig


# ─────────────────────────────────────────────────────────────────────────────
# DIAGRAM 4 — Star Schema (fact + dims)
# ─────────────────────────────────────────────────────────────────────────────
def _star_schema_diagram() -> go.Figure:
    fig = go.Figure()

    dims = [
        ("DIM_DATE",       0.50, 0.95, "date_key\nfull_date\nyear / month / quarter\nfiscal_year"),
        ("DIM_CUSTOMER",   0.10, 0.70, "cust_sk (SK)\ncust_id (BK)\nloyalty_tier\nage_band · SCD2"),
        ("DIM_PRODUCT",    0.90, 0.70, "prd_sk (SK)\nprd_id (BK)\nprice_tier\ngross_margin · SCD2"),
        ("DIM_STORE",      0.15, 0.25, "store_sk (SK)\nstore_id (BK)\nregion · SCD1"),
        ("DIM_PROMOTION",  0.85, 0.25, "promo_sk (SK)\npromo_code (BK)\ndiscount_value · SCD1"),
    ]
    facts = [
        ("FACT_SALES",      0.50, 0.50, "order_item_id\norder_date_key → DIM_DATE\ncust_sk / prd_sk / store_sk / promo_sk\nqty_ordered · unit_price · net_revenue\ncogs · gross_profit · discount_amt"),
        ("FACT_INVENTORY",  0.50, 0.10, "inventory_id\nsnapshot_date_key → DIM_DATE\nprd_sk / store_sk\nqty_on_hand · stock_value\nneeds_reorder · cover_days"),
    ]

    def _hex_to_rgba(h, alpha=0.15):
        h = h.lstrip("#")
        r, g, b = int(h[0:2],16), int(h[2:4],16), int(h[4:6],16)
        return f"rgba({r},{g},{b},{alpha})"

    # Draw dim boxes
    for label, x, y, body in dims:
        w, h_ = 0.17, 0.25
        fig.add_shape(type="rect",
            x0=x-w, x1=x+w, y0=y-h_, y1=y+h_/3,
            fillcolor=_hex_to_rgba(C["warehouse"], 0.25),
            line=dict(color=C["warehouse"], width=1.5),
        )
        # header bar
        fig.add_shape(type="rect",
            x0=x-w, x1=x+w, y0=y+h_/3-0.06, y1=y+h_/3,
            fillcolor=C["warehouse"],
            line=dict(color=C["warehouse"], width=0),
        )
        fig.add_annotation(x=x, y=y+h_/3-0.03,
            text=f"<b>{label}</b>",
            showarrow=False, font=dict(size=9, color="#ffffff"),
        )
        fig.add_annotation(x=x, y=y-h_/2+0.04,
            text=f'<span style="font-size:7.5px;color:#8b949e;font-family:monospace">{body}</span>',
            showarrow=False, align="left",
        )

    # Draw fact boxes
    for label, x, y, body in facts:
        w, h_ = 0.20, 0.28
        fig.add_shape(type="rect",
            x0=x-w, x1=x+w, y0=y-h_, y1=y+h_/3,
            fillcolor=_hex_to_rgba(C["mart"], 0.20),
            line=dict(color=C["mart"], width=2),
        )
        fig.add_shape(type="rect",
            x0=x-w, x1=x+w, y0=y+h_/3-0.06, y1=y+h_/3,
            fillcolor=C["mart"],
            line=dict(color=C["mart"], width=0),
        )
        fig.add_annotation(x=x, y=y+h_/3-0.03,
            text=f"<b>{label}</b>",
            showarrow=False, font=dict(size=10, color="#ffffff"),
        )
        fig.add_annotation(x=x, y=y-h_/2+0.04,
            text=f'<span style="font-size:7.5px;color:#8b949e;font-family:monospace">{body}</span>',
            showarrow=False, align="left",
        )

    # FK lines: dim → fact
    fk_lines = [
        ((0.50, 0.95), (0.50, 0.50+0.28/3)),   # DATE → FACT_SALES
        ((0.10, 0.70), (0.50-0.20, 0.50)),      # CUSTOMER → FACT_SALES left
        ((0.90, 0.70), (0.50+0.20, 0.50)),      # PRODUCT  → FACT_SALES right
        ((0.15, 0.25), (0.50-0.20, 0.50)),      # STORE    → FACT_SALES left
        ((0.85, 0.25), (0.50+0.20, 0.50)),      # PROMO    → FACT_SALES right
        ((0.50, 0.95), (0.50, 0.10+0.28/3)),    # DATE → FACT_INV
        ((0.90, 0.70), (0.50+0.20, 0.10)),      # PRODUCT  → FACT_INV
        ((0.15, 0.25), (0.50-0.20, 0.10)),      # STORE    → FACT_INV
    ]
    for (x0,y0), (x1,y1) in fk_lines:
        fig.add_trace(go.Scatter(
            x=[x0, x1], y=[y0, y1],
            mode="lines",
            line=dict(color="rgba(63,185,80,0.3)", width=1.5, dash="dot"),
            hoverinfo="skip", showlegend=False,
        ))

    fig.update_layout(
        paper_bgcolor=BG2, plot_bgcolor=BG2,
        font=dict(color=FONT),
        height=600,
        margin=dict(l=10, r=10, t=50, b=10),
        title=dict(
            text="<b>Star Schema — WAREHOUSE Layer (Dims + Facts)</b>",
            font=dict(size=13, color=FONT), x=0.5,
        ),
        xaxis=dict(showgrid=False, zeroline=False, showticklabels=False,
                   range=[-0.05, 1.05]),
        yaxis=dict(showgrid=False, zeroline=False, showticklabels=False,
                   range=[-0.20, 1.10]),
    )
    return fig


# ─────────────────────────────────────────────────────────────────────────────
# TECH STACK TABLE
# ─────────────────────────────────────────────────────────────────────────────
def _tech_stack() -> pd.DataFrame:
    return pd.DataFrame([
        ("🐘 OLTP Source",      "Neon Postgres",          "serverless",     "Transactional data — orders, customers, inventory"),
        ("🏭 Data Generator",   "Python + Faker",         "3.14",           "Synthetic but realistic SportZone retail data"),
        ("🔁 ETL Pipeline",     "Python + pandas",        "2.2.3",          "Incremental extract → transform → load to Snowflake"),
        ("❄️ Data Warehouse",   "Snowflake",              "me-central2.gcp","Star schema DWH — dims, facts, streams, tasks"),
        ("🔧 Transformation",   "dbt-snowflake",          "1.12.3",         "19 models · 2 snapshots · 77 tests · 3 macros"),
        ("📊 Dashboard",        "Streamlit",              "1.45.1",         "Analytics + Management + dbt + Architecture views"),
        ("📈 Charts",           "Plotly",                 "5.24.1",         "Interactive charts, Sankey diagrams, DAG visuals"),
        ("🤖 AI / NL→SQL",      "OpenAI GPT-4o",          "1.55.3",         "Natural language queries, insight storytelling"),
        ("🧠 ML",               "scikit-learn",           "1.6.1",          "Forecasting, clustering, churn, anomaly detection"),
        ("🔐 Auth",             "RSA keypair",            "cryptography 44","MFA-free Snowflake service account auth"),
        ("🗄️ Packages",         "dbt_utils + audit_helper","1.4 / 0.14",    "Surrogate keys, schema overrides, audit helpers"),
    ], columns=["Component", "Technology", "Version", "Purpose"])


# ─────────────────────────────────────────────────────────────────────────────
# RENDER
# ─────────────────────────────────────────────────────────────────────────────
def render():
    st.markdown("## 🏗️ Platform Architecture")
    st.markdown(
        "Full visual overview of the SportZone data engineering platform — "
        "from data generation through Snowflake layers to the dashboard."
    )

    tab_overview, tab_dbt, tab_star, tab_stack = st.tabs([
        "🗺️ End-to-End Overview",
        "⚙️ dbt Model DAG",
        "⭐ Star Schema",
        "🛠️ Tech Stack",
    ])

    # ── Tab 1: End-to-End ─────────────────────────────────────────────────────
    with tab_overview:
        # Medallion bar
        st.plotly_chart(_medallion_diagram(), use_container_width=True)

        st.divider()

        # Full architecture diagram
        st.plotly_chart(_arch_diagram(), use_container_width=True)

        # Layer legend
        st.markdown("")
        cols = st.columns(8)
        legend_items = [
            ("Generator",  C["generator"]),
            ("Neon OLTP",  C["oltp"]),
            ("ETL",        C["etl"]),
            ("RAW Bronze", C["raw"]),
            ("Staging Silver", C["staging"]),
            ("Warehouse Gold", C["warehouse"]),
            ("Mart Platinum",  C["mart"]),
            ("Dashboard",  C["dashboard"]),
        ]
        for col, (label, color) in zip(cols, legend_items):
            col.markdown(
                f'<div style="background:{color};border-radius:6px;padding:4px 8px;'
                f'text-align:center;font-size:.75rem;color:#fff">{label}</div>',
                unsafe_allow_html=True,
            )

        st.divider()

        # Detailed flow description
        with st.expander("📖 Architecture narrative", expanded=False):
            st.markdown("""
**1 · Data Generation**
Faker-powered Python generator creates realistic SportZone retail transactions — customers, orders,
products, stores, promotions, inventory snapshots — directly into **Neon Postgres** (`oltp.*` schema).

**2 · ETL Pipeline (Neon → Snowflake)**
`pipeline.py` orchestrates three steps:
- `extract.py` reads incrementally using the `etl_watermark` table (only rows since last run)
- `transform.py` cleanses and type-casts using pandas
- `load.py` writes to Snowflake `RAW.*` using `write_pandas()` + `COPY INTO`

Snowflake **Streams** and **Tasks** (30-min CRON DAG) provide an alternative CDC-based path.

**3 · dbt Transformation (RAW → Mart)**
dbt runs 19 models across 4 layers in ~12 seconds:
- **Staging** (7 views): deduplicate, type-cast, derive computed columns
- **Snapshots** (2 tables): SCD2 history for customers and products via `dbt snapshot`
- **Warehouse** (7 tables): 5 dims (SCD1/2) + 2 incremental facts with surrogate keys
- **Mart** (5 views): pre-aggregated business views consumed by the dashboard

**4 · Streamlit Dashboard**
Queries mart views (cached 60s) for analytics, runs dbt commands inline,
and provides AI (GPT-4o) NL→SQL and ML (scikit-learn) capabilities.
            """)

    # ── Tab 2: dbt DAG ────────────────────────────────────────────────────────
    with tab_dbt:
        st.plotly_chart(_dbt_dag_diagram(), use_container_width=True)

        st.divider()
        col1, col2 = st.columns(2)

        with col1:
            st.markdown("#### dbt Run Stats")
            run_stats = pd.DataFrame([
                ("Models",        "19",  "7 staging + 2 snap + 5 dims + 2 facts + 5 mart"),
                ("Materialisation","4 types","view · table · incremental · snapshot"),
                ("Run time",      "~12s", "4 threads, COMPUTE_WH"),
                ("Tests",         "77",  "75 pass · 2 warn (NULL order_date in source)"),
                ("Packages",      "2",   "dbt_utils 1.4.1 · audit_helper 0.14.0"),
                ("Macros",        "3",   "generate_schema_name · age_band · price_tier"),
                ("Snapshots",     "2",   "snap_customers · snap_products (check strategy)"),
                ("Schemas",       "4",   "STAGING · SNAPSHOTS · WAREHOUSE · MART"),
            ], columns=["Metric", "Value", "Detail"])
            st.dataframe(run_stats, use_container_width=True, hide_index=True)

        with col2:
            st.markdown("#### SCD Strategy")
            scd = pd.DataFrame([
                ("dim_customer",  "SCD2", "snap_customers",  "loyalty_tier · city · state · is_active"),
                ("dim_product",   "SCD2", "snap_products",   "unit_price · is_active"),
                ("dim_store",     "SCD1", "stg_stores",      "All columns (overwrite on change)"),
                ("dim_promotion", "SCD1", "stg_promotions",  "All columns (overwrite on change)"),
                ("dim_date",      "Static","date_spine",     "2023-01-01 → 2026-12-31 (1,461 rows)"),
            ], columns=["Dimension", "Type", "Source", "Tracked Columns"])
            st.dataframe(scd, use_container_width=True, hide_index=True)

            st.markdown("#### Incremental Facts")
            inc = pd.DataFrame([
                ("fact_sales",     "order_item_id",                  "MERGE", "CLUSTER BY (order_date_key, prd_sk)"),
                ("fact_inventory", "inventory_id × snapshot_date_key","MERGE", "CLUSTER BY (snapshot_date_key, prd_sk)"),
            ], columns=["Model", "Unique Key", "Strategy", "Clustering"])
            st.dataframe(inc, use_container_width=True, hide_index=True)

        st.divider()
        st.markdown("#### Mart Views (consumed by Analytics Dashboard)")
        mart = pd.DataFrame([
            ("mart_daily_sales_summary",     "fact_sales + DIM_DATE + DIM_PRODUCT + DIM_STORE",       "day × channel × category",      "Overview tab — revenue KPIs, trends"),
            ("mart_customer_360",            "DIM_CUSTOMER + fact_sales + DIM_DATE + DIM_PRODUCT",    "current customer",              "Customers tab — LTV, RFM, preferred channel"),
            ("mart_product_performance",     "fact_sales + DIM_PRODUCT + DIM_DATE",                  "product × year × month",        "Products tab — revenue, margin, return rate"),
            ("mart_inventory_status",        "fact_inventory + DIM_PRODUCT + DIM_STORE + DIM_DATE",  "product × store (latest snap)", "Inventory tab — stock, cover days, reorder alerts"),
            ("mart_promotion_effectiveness", "fact_sales + DIM_PROMOTION + DIM_DATE",                "promo × year × month",          "Promotions tab — ROI, discount rate"),
        ], columns=["Model", "Joins", "Grain", "Used By"])
        st.dataframe(mart, use_container_width=True, hide_index=True)

    # ── Tab 3: Star Schema ────────────────────────────────────────────────────
    with tab_star:
        st.plotly_chart(_star_schema_diagram(), use_container_width=True)

        st.divider()
        st.markdown("#### FACT_SALES — Column Lineage")
        lineage = pd.DataFrame([
            ("ORDER_DATE_KEY",   "oltp.order_header.order_date",       "TO_NUMBER(TO_CHAR(date,'YYYYMMDD'))",   "DIM_DATE join key"),
            ("CUST_SK",          "oltp.customer.customer_id → DIM_CUSTOMER", "abs(hash) surrogate",            "SCD2 — current row"),
            ("PRD_SK",           "oltp.product.product_id → DIM_PRODUCT",    "abs(hash) surrogate",            "SCD2 — current row"),
            ("STORE_SK",         "oltp.store.store_id → DIM_STORE",          "abs(hash) surrogate",            "SCD1"),
            ("PROMO_SK",         "oltp.promotion.code → DIM_PROMOTION",      "abs(hash) or -1 (no promo)",     "SCD1, unknown=-1"),
            ("QTY_ORDERED",      "oltp.order_item.qty",                 "CAST INTEGER",                         "Direct"),
            ("UNIT_PRICE",       "oltp.order_item.unit_price",          "CAST NUMERIC",                         "At time of sale"),
            ("UNIT_COST",        "oltp.product.unit_cost (via dim)",    "DIM_PRODUCT.unit_cost",                "Current product cost"),
            ("GROSS_REVENUE",    "Derived",                             "qty × unit_price",                     "Pre-discount"),
            ("NET_REVENUE",      "oltp.order_item.line_total",          "CAST NUMERIC",                         "After discount"),
            ("COGS",             "Derived",                             "qty × unit_cost",                      "Cost of goods sold"),
            ("GROSS_PROFIT",     "Derived",                             "net_revenue − cogs",                   "Margin contribution"),
            ("DISCOUNT_AMT",     "Derived in stg_order_items",         "unit_price × qty × discount_pct / 100","Computed in staging"),
        ], columns=["Column", "Source", "Transformation", "Notes"])
        st.dataframe(lineage, use_container_width=True, hide_index=True)

    # ── Tab 4: Tech Stack ─────────────────────────────────────────────────────
    with tab_stack:
        st.dataframe(_tech_stack(), use_container_width=True, hide_index=True)

        st.divider()
        col_l, col_r = st.columns(2)

        with col_l:
            st.markdown("#### Snowflake Objects")
            sf_objects = pd.DataFrame([
                ("SCHEMAS",    "RAW · STAGING · SNAPSHOTS · WAREHOUSE · MART"),
                ("WAREHOUSE",  "COMPUTE_WH (all dbt + dashboard queries)"),
                ("ROLE",       "ACCOUNTADMIN (development)"),
                ("FILE FORMATS","CSV comma · CSV pipe · JSON · Parquet"),
                ("STAGES",     "7 internal stages (one per entity)"),
                ("STREAMS",    "STREAM_ORDERS · STREAM_CUSTOMERS · STREAM_INVENTORY"),
                ("TASKS",      "5-step CRON DAG (30-min schedule)"),
                ("CLUSTERING", "fact_sales(order_date_key, prd_sk) · fact_inventory(snapshot_date_key, prd_sk)"),
            ], columns=["Object Type", "Details"])
            st.dataframe(sf_objects, use_container_width=True, hide_index=True)

        with col_r:
            st.markdown("#### dbt Commands Reference")
            st.code("""# Full pipeline (from retail_snowflake_project/dbt)
dbt deps                    # install packages
dbt debug                   # verify connection
dbt snapshot                # SCD2 history tables
dbt run                     # build all 19 models
dbt test                    # run 77 tests

# Selective
dbt run --select tag:mart
dbt run --select +fact_sales
dbt run --select dim_customer+
dbt run --full-refresh      # rebuild incrementals

# Docs
dbt docs generate
dbt docs serve              # http://localhost:8080""",
                language="bash")

        st.divider()
        st.markdown("#### Neon Postgres (OLTP) Schema")
        oltp = pd.DataFrame([
            ("oltp.customer",       "Customer master — loyalty tier, demographics"),
            ("oltp.order_header",   "Order summary — status, channel, promo code"),
            ("oltp.order_item",     "Line items — product, qty, price, discount"),
            ("oltp.payment",        "Payment records — method, amount, status"),
            ("oltp.inventory",      "Daily stock snapshots per product × store"),
            ("oltp.product",        "Product catalogue — category, cost, price"),
            ("oltp.store",          "Store master — region, type, manager"),
            ("oltp.promotion",      "Promotions — type, discount, date range"),
            ("oltp.customer_event", "Behavioural events — views, cart, purchase"),
            ("oltp.etl_watermark",  "Incremental checkpoint per table"),
        ], columns=["Table", "Description"])
        st.dataframe(oltp, use_container_width=True, hide_index=True)
