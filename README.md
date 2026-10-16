# 🏆 SportZone Retail Analytics Platform

> **A complete, production-grade data engineering and analytics project** built on **Neon Postgres** (OLTP), **Snowflake** (DWH), and **Streamlit** (dashboard) — covering everything from raw OLTP data generation through ETL pipelines, star schema modelling, governance, and AI-powered analytics.

---

## Table of Contents

1. [Architecture Overview](#architecture-overview)
2. [Technology Stack](#technology-stack)
3. [Project Structure](#project-structure)
4. [Quick Start](#quick-start)
5. [OLTP — Neon Postgres](#oltp--neon-postgres)
6. [ETL Pipeline](#etl-pipeline)
7. [Snowflake DWH](#snowflake-dwh)
8. [Streamlit Dashboard](#streamlit-dashboard)
9. [Security & Governance](#security--governance)
10. [Configuration (.env)](#configuration-env)
11. [Running Scripts Reference](#running-scripts-reference)

---

## Architecture Overview

```
┌──────────────────────────────────────────────────────────────────────────┐
│  SOURCE — Neon Postgres (sportzone database)                             │
│  Schema: oltp.*                                                          │
│  customer · order_header · order_item · payment · inventory              │
│  product  · store        · promotion  · customer_event                   │
│  etl_watermark  (incremental checkpoint)                                 │
└───────────────────────────────┬──────────────────────────────────────────┘
                                │
                    ┌───────────▼───────────┐
                    │   ETL Pipeline        │
                    │  extract.py           │  ← incremental via watermarks
                    │  transform.py         │  ← cleanse, type-cast, derive
                    │  load.py              │  ← write_pandas + MERGE
                    │  pipeline.py          │  ← orchestrator
                    └───────────┬───────────┘
                                │
┌───────────────────────────────▼──────────────────────────────────────────┐
│  DESTINATION — Snowflake  SPORTZONE_DEV database                         │
│                                                                          │
│  ┌─────────┐   ┌──────────┐   ┌─────────────────────┐   ┌───────────┐  │
│  │  RAW    │──▶│ STAGING  │──▶│     WAREHOUSE        │──▶│   MART    │  │
│  │ Bronze  │   │  Silver  │   │  Gold / Star Schema  │   │ Platinum  │  │
│  │ As-is   │   │ Cleansed │   │  Dims + Facts SCD2   │   │   Views   │  │
│  └─────────┘   └──────────┘   └─────────────────────┘   └───────────┘  │
└──────────────────────────────────────────────────────────────────────────┘
                                │
                    ┌───────────▼───────────┐
                    │  Streamlit Dashboard  │  http://localhost:8501
                    │  Management & ETL     │  ← connections, generate, run ETL
                    │  Analytics Dashboard  │  ← KPIs, charts, SQL, AI, ML
                    └───────────────────────┘
```

---

## Technology Stack

| Component | Technology | Purpose |
|-----------|-----------|---------|
| **OLTP Source** | Neon Postgres (serverless) | Transactional data — orders, customers, inventory |
| **Data Warehouse** | Snowflake (me-central2.gcp) | Star schema DWH — dims, facts, mart views |
| **ETL** | Python + pandas + snowflake-connector | Extract → Transform → Load pipeline |
| **Data Generator** | Python + Faker | Synthetic but realistic retail OLTP data |
| **Dashboard** | Streamlit + Plotly | Full analytics platform with AI |
| **AI** | OpenAI GPT (optional) | NL→SQL, insights, chart storytelling |
| **ML** | scikit-learn | Forecasting, segmentation, churn, anomaly detection |
| **Auth** | RSA keypair (Snowflake) | MFA-free service account authentication |
| **SQL Formatting** | sqlparse | Auto-indent and keyword-case normalisation |

---

## Project Structure

```
retail_snowflake_project/
│
├── .env                          # All credentials (Neon + Snowflake)
├── db.py                         # Shared Neon connection helper
├── requirements.txt              # Python dependencies
├── run_all.py                    # Single entry-point: generate + ETL
│
├── 00_setup/                     # Snowflake account setup SQLs
│   ├── 01_account_setup.sql      # Roles, users, warehouses
│   └── 02_database_setup.sql     # Databases, schemas, grants
│
├── 01_raw/                       # RAW layer
│   ├── 01_file_formats.sql       # CSV, Parquet, JSON file formats
│   ├── 02_stages.sql             # Internal + S3 external stages
│   ├── 03_raw_tables.sql         # RAW landing tables + COPY INTO
│   └── 04_sample_data/           # Sample CSVs (customers, orders, etc.)
│
├── 02_staging/                   # STAGING layer
│   ├── 01_staging_tables.sql     # Typed, cleansed staging tables
│   └── 02_staging_transforms.sql # ELT: RAW → STAGING SQL
│
├── 03_warehouse/                 # WAREHOUSE layer (star schema)
│   ├── 01_dimensions.sql         # DIM_DATE, DIM_CUSTOMER (SCD2), etc.
│   ├── 02_facts.sql              # FACT_SALES, FACT_INVENTORY
│   └── 03_load_warehouse.sql     # MERGE logic: STAGING → WAREHOUSE
│
├── 04_mart/                      # MART layer
│   └── 01_mart_views.sql         # DAILY_SALES_SUMMARY, CUSTOMER_360, etc.
│
├── 05_pipelines/                 # Snowflake Streams & Tasks
│   ├── 01_streams.sql            # CDC streams on RAW tables
│   └── 02_tasks.sql              # Scheduled task DAG (30-min pipeline)
│
├── 06_governance/                # Data governance
│   ├── 01_masking_policies.sql   # Dynamic data masking (email, phone, DOB)
│   └── 02_tags.sql               # PII / Financial / Domain object tags
│
├── 07_sharing/                   # Outbound data sharing
│   └── 01_data_shares.sql        # Secure views + Snowflake shares
│
├── 08_operations/                # Platform operations
│   ├── 01_cloning.sql            # Zero-copy cloning for DEV/QA/recovery
│   └── 02_monitoring.sql         # Query history, credit usage, DQ checks
│
├── oltp/                         # OLTP data layer (Neon Postgres)
│   ├── schema.sql                # 12-table 3NF schema (orders, payments, events)
│   └── generator.py             # Faker-based bulk data generator
│
├── etl/                          # ETL pipeline
│   ├── extract.py                # Incremental extract from Neon → Parquet
│   ├── transform.py              # Cleanse, cast, derive → DataFrames
│   ├── load.py                   # write_pandas + MERGE into Snowflake
│   ├── pipeline.py               # Orchestrator (--full-load, --dry-run, --schedule)
│   ├── neon_load.py              # Load transformed data into Neon DWH schemas
│   ├── load_from_parquet.py      # Fast reload from cached Parquet files
│   ├── reset_dwh.py              # Clean slate DWH reset
│   └── staging/                  # Local Parquet staging files (auto-created)
│
├── deploy/                       # Deployment utilities
│   ├── config.py                 # Snowflake .env parser (supports keypair)
│   ├── deploy.py                 # Full SQL deploy script
│   ├── migrate.py                # Idempotent migration runner
│   ├── neon_migrate.py           # Neon DWH schema setup
│   ├── local_db.py               # SQLite local dev database
│   └── runner.py                 # SQL file executor
│
├── dashboard/                    # Streamlit analytics platform
│   ├── app.py                    # Main app + routing
│   ├── conn.py                   # Connection manager (Neon + Snowflake + OpenAI)
│   └── pages/
│       ├── management.py         # Management & ETL control center
│       └── analytics.py          # Full analytics dashboard
│
└── keys/                         # RSA keypair for Snowflake auth (gitignore)
    ├── rsa_key.p8                # Private key
    └── rsa_key.pub               # Public key (registered in Snowflake)
```

---

## Quick Start

### Prerequisites

- Python 3.10+ (tested on 3.14)
- Neon Postgres account → [neon.tech](https://neon.tech)
- Snowflake account (trial OK) → [snowflake.com](https://snowflake.com)

### 1. Install dependencies

```bash
pip install -r requirements.txt
# Optional AI features:
pip install openai
```

### 2. Configure credentials

Edit `.env` with your connection details (see [Configuration](#configuration-env)).

### 3. Register RSA key in Snowflake (one-time)

```bash
python setup_keypair.py
```
Copy the `ALTER USER` command it prints, run it in your Snowflake SQL worksheet.

### 4. Set up Snowflake DWH schema

```bash
python setup_snowflake_dwh.py
python populate_dim_date.py
```

### 5. Generate OLTP data in Neon

```bash
# Seed reference data + generate 500 customers, 2000 orders
python oltp/generator.py --customers 500 --orders 2000 --events 3000
```

### 6. Run ETL pipeline (Neon → Snowflake)

```bash
# Full load (first time)
python etl/pipeline.py --full-load

# Incremental (subsequent runs — only new data)
python etl/pipeline.py

# Dry-run (extract + transform, skip Snowflake write)
python etl/pipeline.py --dry-run
```

### 7. Launch dashboard

The sidebar uses the dashboard's Management & ETL and Analytics Dashboard controls; Streamlit's duplicate file-based page links are hidden.

```bash
python -m streamlit run dashboard/app.py
# Opens at http://localhost:8501
```

### One-command shortcut

```bash
# Generate + ETL in one go
python run_all.py --customers 100 --orders 500 --events 1000
```

---

## OLTP — Neon Postgres

**Database:** `sportzone`  **Schema:** `oltp`

### Schema (12 tables)

| Table | Description | Key Columns |
|-------|-------------|-------------|
| `customer` | Customer master | `customer_id`, `loyalty_tier`, `signup_date` |
| `order_header` | Order transactions | `order_id`, `order_ref`, `channel`, `order_status` |
| `order_item` | Line items | `order_item_id`, `order_id`, `product_id`, `qty` |
| `payment` | Payment records | `payment_id`, `payment_method`, `payment_status` |
| `product` | Product catalogue | `product_id`, `sku`, `unit_price`, `unit_cost` |
| `category` | Product categories | Hierarchical (parent_id FK) |
| `brand` | Brand master | — |
| `supplier` | Supplier master | — |
| `store` | Store master | `store_type` (PHYSICAL/ONLINE) |
| `promotion` | Promotions | `promo_type` (PERCENTAGE/FIXED/BOGO) |
| `inventory` | Stock levels | `qty_on_hand`, `reorder_point` |
| `customer_event` | Clickstream | `event_type`, `session_id`, `event_data` (JSONB) |
| `etl_watermark` | ETL checkpoints | `last_extracted` per table |

### Data Generator

```bash
python oltp/generator.py --customers 500 --orders 2000 --events 3000 --start-date 2024-01-01
python oltp/generator.py --orders 2000 --start-date 2023-01-01 --end-date 2023-12-31 --csv-output output/historical_orders.csv
python oltp/generator.py --destination both --start-date 2023-01-01 --end-date 2023-12-31
python oltp/generator.py --stats                      # show row counts
python oltp/generator.py --continuous --batch 50 --interval 30  # live streaming
python oltp/generator.py --reset --customers 200 --orders 1000  # reset + regenerate
```

Without `--destination`, the generator prompts for PostgreSQL, CSV files, or both. If either date flag is omitted, it prompts for that date; both dates are inclusive. CSV mode writes a CSV per generated OLTP table to `output/oltp_csv` by default (override with `--csv-output-dir`). CSV exports contain unique primary keys and a few intentional data-quality examples (invalid email, missing phone, zero item quantity, and invalid event type) for ETL/testing. In `both` mode these issues exist only in the CSV files; PostgreSQL receives the clean generated records. CSV files are snapshots and are overwritten on later runs. Continuous mode supports PostgreSQL only.

The legacy `--csv-output` option still writes only generated order headers to a separate CSV file alongside PostgreSQL inserts. It is not the all-table export; use CSV or both mode for the OLTP table files.

---

## ETL Pipeline

### Flow

```
Neon OLTP  →  extract.py  →  [Parquet staging]  →  transform.py  →  load.py  →  Snowflake DWH
              (watermarks)    etl/staging/                              (write_pandas + MERGE)
```

### Incremental Loading (Watermarks)

Every table tracks its last-extracted timestamp in `oltp.etl_watermark`.  
On each run, only rows newer than the watermark are extracted — making reruns safe and fast.

### Load Modes

| Mode | Command | Behaviour |
|------|---------|-----------|
| Incremental | `python etl/pipeline.py` | Only new rows since last watermark |
| Full reload | `python etl/pipeline.py --full-load` | Ignore watermarks, reload everything |
| Dry-run | `python etl/pipeline.py --dry-run` | Extract + transform only, skip Snowflake |
| Scheduled | `python etl/pipeline.py --schedule 30` | Repeat every 30 minutes |

### Idempotency

- RAW tables: `INSERT` append-only (audit log)
- STAGING tables: `TRUNCATE` before every load (always fresh)
- WAREHOUSE facts: `MERGE … ON CONFLICT DO NOTHING` (safe to re-run)
- WAREHOUSE dims: SCD Type 2 — expire changed rows, insert new version

---

## Snowflake DWH

**Database:** `SPORTZONE_DEV`

### Layer Summary

| Layer | Schema | Tables | Description |
|-------|--------|--------|-------------|
| Bronze | `RAW` | 7 tables | As-is from source (VARCHAR columns) |
| Silver | `STAGING` | 7 tables | Typed, cleansed, deduplicated |
| Gold | `WAREHOUSE` | 7 tables | Star schema — SCD2 dims + clustered facts |
| Platinum | `MART` | 4 views | Aggregated business views |

### Star Schema

```
                    DIM_DATE
                       │
DIM_PROMOTION ────── FACT_SALES ────── DIM_CUSTOMER
                       │
                    DIM_PRODUCT
                       │
                    DIM_STORE
```

### Dimension Tables (SCD Type 2)

| Dimension | Grain | SCD Type | Key Attributes |
|-----------|-------|----------|----------------|
| `DIM_DATE` | One row per calendar day (2023–2026) | Static | fiscal_year, fiscal_quarter, is_weekend |
| `DIM_CUSTOMER` | One row per customer version | **Type 2** | loyalty_tier, age_band, country |
| `DIM_PRODUCT` | One row per product version | **Type 2** | price_tier, gross_margin_pct |
| `DIM_STORE` | One row per store | Type 1 | store_type, region |
| `DIM_PROMOTION` | One row per promotion | Type 1 | promo_type, discount_value |

### Fact Tables

| Fact | Grain | Cluster Key | Key Measures |
|------|-------|-------------|--------------|
| `FACT_SALES` | One row per order line item | `ORDER_DATE_KEY, PRD_SK` | qty, gross_revenue, net_revenue, cogs, gross_profit |
| `FACT_INVENTORY` | One row per product × store × snapshot | `SNAPSHOT_DATE_KEY, PRD_SK` | qty_on_hand, qty_available, stock_value |

### Mart Views

| View | Purpose |
|------|---------|
| `MART.DAILY_SALES_SUMMARY` | Daily revenue / margin / units by channel, category, region |
| `MART.CUSTOMER_360` | Customer lifetime value, RFM, recency segment |
| `MART.PRODUCT_PERFORMANCE` | Monthly product revenue, margin, unique buyers |
| `MART.INVENTORY_STATUS` | Latest stock levels, reorder alerts, cover days |

### Snowflake-Specific Features Used

- **QUALIFY** — deduplicate with window functions
- **CLUSTER BY** — micro-partition pruning on fact tables
- **MERGE** — upsert for dims and facts
- **Streams** — CDC on RAW tables (`05_pipelines/01_streams.sql`)
- **Tasks** — 30-min scheduled pipeline DAG (`05_pipelines/02_tasks.sql`)
- **Dynamic Data Masking** — PII columns (`06_governance/01_masking_policies.sql`)
- **Object Tags** — PII/Financial classification (`06_governance/02_tags.sql`)
- **Secure Views + Shares** — outbound data sharing (`07_sharing/`)
- **Zero-Copy Clone** — DEV/QA cloning (`08_operations/01_cloning.sql`)
- **GENERATOR + SEQ4** — DIM_DATE population
- **REGR_SLOPE / REGR_INTERCEPT** — in-database linear regression

---

## Streamlit Dashboard

```bash
python -m streamlit run dashboard/app.py
```

**URL:** http://localhost:8501

### Page A — 🔧 Management & ETL (6 tabs)

| Tab | Features |
|-----|---------|
| **🔗 Connections** | Neon + Snowflake preconfigured connections, custom override, test button, schema explorer |
| **⚡ Generate Data** | Faker data generator — set customer/order/event counts, before/after charts |
| **▶️ ETL Pipeline** | Run incremental / full-load / dry-run; watermark state; before/after DWH counts |
| **📈 Monitoring** | Pipeline run history chart, Snowflake query history, Neon live pg_stats |
| **🔀 Data Lineage** | Interactive Plotly Sankey flow diagram (colour-coded by layer), column-level lineage, watermarks |
| **🤖 AI Settings** | OpenAI API key configuration, model selection, capability overview |

### Page B — 📊 Analytics Dashboard (8 tabs)

| Tab | Features |
|-----|---------|
| **📈 Overview** | 8 KPIs, revenue trend (7/30d MA), channel/category mix, monthly bar |
| **🛍️ Products** | Category treemap, top-10 table, brand revenue vs margin scatter |
| **👥 Customers** | RFM scatter, loyalty tier breakdown, age/gender LTV, country revenue |
| **📦 Inventory** | Status pie, region stock value, reorder alert table |
| **🎯 Promotions** | ROI scatter, profit waterfall, efficiency chart, discount vs margin sweet-spot |
| **🔍 SQL Explorer** | Syntax-highlighted editor, Format SQL, EXPLAIN plan, AI optimise, chart selector, CSV download, Bulk CSV → Snowflake COPY INTO, 20+ prewritten queries (Simple→Extreme) |
| **🤖 AI Analytics** | NL→SQL (GPT), highlighted SQL output, chart storytelling, automated insights, question suggestions |
| **🧪 ML & Data Science** | Revenue forecast (LR + CI), K-Means + PCA (2–6 clusters), churn risk scoring, market basket analysis, anomaly detection (Isolation Forest) |

---

## Security & Governance

### Snowflake Authentication — RSA Keypair

The platform uses **RSA keypair authentication** — no password prompts, no MFA pop-ups.

```bash
# Generate keypair
python setup_keypair.py

# Register in Snowflake (run in Snowsight)
ALTER USER SIDDIQUE SET RSA_PUBLIC_KEY='<key from setup_keypair.py output>';
```

### Dynamic Data Masking (PII Protection)

Applied to `STAGING.STG_CUSTOMERS` and `WAREHOUSE.DIM_CUSTOMER`:

| Column | ACCOUNTADMIN | DATA_ENG | ANALYST | Others |
|--------|-------------|----------|---------|--------|
| `EMAIL` | Full | Full | `jd***@***.com` | `***@***.***` |
| `PHONE` | Full | Full | `***-***-1234` | `**REDACTED**` |
| `DATE_OF_BIRTH` | Full date | Full date | Year only | Year only |

### Object Tags (Data Classification)

```sql
-- PII classification
ALTER TABLE DIM_CUSTOMER MODIFY COLUMN EMAIL SET TAG PII = 'email-address';

-- Financial classification  
ALTER TABLE DIM_PRODUCT MODIFY COLUMN UNIT_COST SET TAG FINANCIAL = 'cost-price';
```

### Outbound Data Shares

Three shares created for downstream consumers:

| Share | Consumer | Data Exposed |
|-------|----------|-------------|
| `SPORTZONE_SALES_SHARE` | Marketing agency | Sales without costs/margins |
| `SPORTZONE_INVENTORY_SHARE` | Supplier partner | Stock levels only |
| `SPORTZONE_FINANCE_SHARE` | FP&A team | Full P&L aggregated |

---

## Configuration (.env)

```ini
# ── Snowflake ─────────────────────────────────────────────
[connections.my_example_connection]
account          = "ur59759.me-central2.gcp"      # from app.snowflake.com URL
user             = "SIDDIQUE"
authenticator    = "snowflake"
password         = "your_password"                 # used only if no keypair
private_key_path = "keys/rsa_key.p8"              # keypair takes priority
role             = "ACCOUNTADMIN"
warehouse        = "COMPUTE_WH"
database         = "SPORTZONE_DEV"
schema           = "RAW"

# ── Neon Postgres ─────────────────────────────────────────
NEON_CONNECTION_STRING="postgresql://neondb_owner:***@ep-*.neon.tech/sportzone?sslmode=require"
DATABASE_URL="postgresql://neondb_owner:***@ep-*-pooler.*.neon.tech/sportzone?sslmode=require"
DATABASE_URL_UNPOOLED="postgresql://neondb_owner:***@ep-*.neon.tech/sportzone?sslmode=require"
NEON_BRANCH=production
```

**Priority for Snowflake auth:** `keypair > externalbrowser > password`  
**Priority for Neon URL:** `NEON_CONNECTION_STRING > DATABASE_URL > DATABASE_URL_UNPOOLED`

> ⚠️ **Never commit `.env` or `keys/` to version control.** Both are in `.gitignore`.

---

## Running Scripts Reference

### Data Generation

```bash
python oltp/generator.py --seed-only                          # reference data only
python oltp/generator.py --customers 500 --orders 2000        # full generation
python oltp/generator.py --orders 2000 --start-date 2023-01-01 --end-date 2023-12-31 --csv-output output/historical_orders.csv
python oltp/generator.py --continuous --batch 50 --interval 15 # streaming mode
python oltp/generator.py --reset --customers 200 --orders 1000 # reset & rebuild
python oltp/generator.py --stats                               # show counts
```

### ETL

```bash
python etl/pipeline.py                     # incremental
python etl/pipeline.py --full-load         # full reload
python etl/pipeline.py --dry-run           # no Snowflake write
python etl/pipeline.py --schedule 30       # every 30 min
python etl/pipeline.py --history           # show run log
python etl/load_from_parquet.py            # reload from cached Parquet
python etl/reset_dwh.py                   # truncate all DWH tables
```

### Snowflake Setup

```bash
python setup_keypair.py          # generate RSA keypair
python test_snowflake.py         # test connection + create DB
python setup_snowflake_dwh.py    # create all DWH tables + views
python populate_dim_date.py      # populate DIM_DATE 2023-2026
python verify_snowflake_dwh.py   # verify data + run mart queries
```

### Neon / Local

```bash
python db.py                     # test Neon connection
python check_oltp.py             # OLTP row counts + watermarks
python check_conn.py             # Neon database + schema info
python deploy/local_db.py        # build SQLite local dev database
python check_syntax.py           # validate all dashboard Python files
```

### Dashboard

```bash
python -m streamlit run dashboard/app.py                    # default port 8501
python -m streamlit run dashboard/app.py --server.port 8080 # custom port
```

---

## Data Flow — Numbers (Current State)

| Layer | Location | Tables | Rows |
|-------|----------|--------|------|
| OLTP | Neon `oltp.*` | 12 | ~12,500 |
| RAW | Snowflake `RAW.*` | 7 | ~7,500 |
| STAGING | Snowflake `STAGING.*` | 7 | ~7,500 |
| WAREHOUSE | Snowflake `WAREHOUSE.*` | 7 | DIM_DATE=1,461 · FACT_SALES=3,796 · FACT_INVENTORY=220 |
| MART | Snowflake `MART.*` | 4 views | $388K revenue · 500 customers |

---

## Book Reference

This project applies concepts from:

> **"Snowflake Data Warehouse Engineering — Architecture, Modeling, ELT Pipelines, and Operations"**  
> ISBN: 978-93-55519764

| Chapter | Concept Applied |
|---------|----------------|
| Ch 3 | Account setup, roles, warehouses, databases, schemas, tables |
| Ch 4 | Stages, file formats, COPY INTO (internal + S3 external) |
| Ch 5 | Streams & Tasks — CDC and scheduled pipeline DAG |
| Ch 7 | DEV/PROD environment separation |
| Ch 8 | Dynamic data masking, object tagging (PII governance) |
| Ch 12 | Data sharing — secure views + outbound shares |
| Ch 13 | Zero-copy cloning for DEV/QA/recovery |

---

*Built with ❤️ using Neon Postgres + Snowflake + Python + Streamlit*
