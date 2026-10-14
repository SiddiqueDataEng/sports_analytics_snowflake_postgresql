"""
neon_migrate.py
===============
Migrates the full SportZone star schema to Neon Postgres.

Steps
-----
1. Reads DATABASE_URL_UNPOOLED from .env  (direct connection, not pooler)
2. Creates schemas: raw, staging, warehouse
3. Creates all tables (DDL translated from Snowflake SQL to Postgres)
4. Loads sample CSV data from 01_raw/04_sample_data/
5. Runs transforms: raw → staging → warehouse (dimensions + facts)

Usage
-----
    python deploy/neon_migrate.py              # full migrate
    python deploy/neon_migrate.py --status     # show row counts
    python deploy/neon_migrate.py --reset      # DROP all schemas + redo (⚠ destructive)
"""

import argparse
import csv
import logging
import os
import re
import sys
from datetime import date
from pathlib import Path

import psycopg2
import psycopg2.extras

DEPLOY_DIR  = Path(__file__).parent
PROJECT_DIR = DEPLOY_DIR.parent
SAMPLE_DIR  = PROJECT_DIR / "01_raw" / "04_sample_data"
sys.path.insert(0, str(PROJECT_DIR))

from db import get_neon_conn, get_neon_url

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Connection — now uses shared db.py (reads NEON_CONNECTION_STRING first)
# ---------------------------------------------------------------------------

def load_neon_url() -> str:
    return get_neon_url()


# ---------------------------------------------------------------------------
# DDL — PostgreSQL-compatible schema
# ---------------------------------------------------------------------------

DDL = """
-- ── Schemas ─────────────────────────────────────────────────────────────
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS warehouse;

-- ── RAW tables ───────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS raw.customers (
    cust_id         TEXT,
    first_name      TEXT,
    last_name       TEXT,
    email           TEXT,
    phone           TEXT,
    date_of_birth   TEXT,
    gender          TEXT,
    address_line1   TEXT,
    address_line2   TEXT,
    city            TEXT,
    state           TEXT,
    zip_code        TEXT,
    country         TEXT,
    loyalty_tier    TEXT,
    signup_date     TEXT,
    is_active       TEXT,
    _src_file       TEXT,
    _load_ts        TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS raw.products (
    prd_id          TEXT,
    prd_name        TEXT,
    prd_category    TEXT,
    prd_subcategory TEXT,
    brand           TEXT,
    unit_price      TEXT,
    unit_cost       TEXT,
    sku             TEXT,
    supplier_id     TEXT,
    is_active       TEXT,
    launch_date     TEXT,
    discontinue_dt  TEXT,
    _src_file       TEXT,
    _load_ts        TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS raw.stores (
    store_id        TEXT,
    store_name      TEXT,
    store_type      TEXT,
    region          TEXT,
    address         TEXT,
    city            TEXT,
    state           TEXT,
    zip_code        TEXT,
    country         TEXT,
    open_date       TEXT,
    manager_name    TEXT,
    _src_file       TEXT,
    _load_ts        TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS raw.orders (
    order_id        TEXT,
    order_date      TEXT,
    cust_id         TEXT,
    store_id        TEXT,
    channel         TEXT,
    order_status    TEXT,
    promo_code      TEXT,
    total_amt       TEXT,
    discount_amt    TEXT,
    net_amt         TEXT,
    _src_file       TEXT,
    _load_ts        TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS raw.order_items (
    order_item_id   TEXT,
    order_id        TEXT,
    prd_id          TEXT,
    qty             TEXT,
    unit_price      TEXT,
    discount_pct    TEXT,
    line_total      TEXT,
    _src_file       TEXT,
    _load_ts        TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS raw.inventory (
    inventory_id    TEXT,
    prd_id          TEXT,
    store_id        TEXT,
    snapshot_date   TEXT,
    qty_on_hand     TEXT,
    qty_reserved    TEXT,
    reorder_point   TEXT,
    inv_status      TEXT,
    _src_file       TEXT,
    _load_ts        TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS raw.promotions (
    promo_id        TEXT,
    promo_code      TEXT,
    promo_name      TEXT,
    promo_type      TEXT,
    discount_value  TEXT,
    start_date      TEXT,
    end_date        TEXT,
    min_order_amt   TEXT,
    is_active       TEXT,
    _src_file       TEXT,
    _load_ts        TIMESTAMPTZ DEFAULT NOW()
);

-- ── STAGING tables ────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS staging.customers (
    cust_id         TEXT NOT NULL,
    first_name      TEXT,
    last_name       TEXT,
    full_name       TEXT,
    email           TEXT,
    phone           TEXT,
    date_of_birth   DATE,
    age_band        TEXT,
    gender          CHAR(1),
    address_line1   TEXT,
    address_line2   TEXT,
    city            TEXT,
    state           TEXT,
    zip_code        TEXT,
    country         CHAR(2),
    loyalty_tier    TEXT,
    signup_date     DATE,
    is_active       BOOLEAN,
    eff_start_dt    DATE DEFAULT CURRENT_DATE,
    eff_end_dt      DATE,
    is_current      BOOLEAN DEFAULT TRUE,
    _load_ts        TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS staging.products (
    prd_id          TEXT NOT NULL,
    prd_name        TEXT,
    prd_category    TEXT,
    prd_subcategory TEXT,
    brand           TEXT,
    unit_price      NUMERIC(10,2),
    unit_cost       NUMERIC(10,2),
    gross_margin_pct NUMERIC(5,2),
    sku             TEXT,
    supplier_id     TEXT,
    is_active       BOOLEAN,
    launch_date     DATE,
    discontinue_dt  DATE,
    _load_ts        TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS staging.orders (
    order_id        TEXT NOT NULL,
    order_date      DATE,
    order_year      SMALLINT,
    order_month     SMALLINT,
    order_quarter   SMALLINT,
    cust_id         TEXT,
    store_id        TEXT,
    channel         TEXT,
    order_status    TEXT,
    promo_code      TEXT,
    total_amt       NUMERIC(12,2),
    discount_amt    NUMERIC(12,2),
    net_amt         NUMERIC(12,2),
    is_discounted   BOOLEAN,
    _load_ts        TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS staging.order_items (
    order_item_id   TEXT NOT NULL,
    order_id        TEXT,
    prd_id          TEXT,
    qty             INTEGER,
    unit_price      NUMERIC(10,2),
    discount_pct    NUMERIC(5,2),
    line_total      NUMERIC(12,2),
    discount_amt    NUMERIC(12,2),
    _load_ts        TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS staging.inventory (
    inventory_id    TEXT NOT NULL,
    prd_id          TEXT,
    store_id        TEXT,
    snapshot_date   DATE,
    qty_on_hand     INTEGER,
    qty_reserved    INTEGER,
    qty_available   INTEGER,
    reorder_point   INTEGER,
    inv_status      TEXT,
    needs_reorder   BOOLEAN,
    _load_ts        TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS staging.promotions (
    promo_id        TEXT NOT NULL,
    promo_code      TEXT,
    promo_name      TEXT,
    promo_type      TEXT,
    discount_value  NUMERIC(10,2),
    start_date      DATE,
    end_date        DATE,
    min_order_amt   NUMERIC(10,2),
    is_active       BOOLEAN,
    _load_ts        TIMESTAMPTZ DEFAULT NOW()
);

-- ── WAREHOUSE — Dimensions ────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS warehouse.dim_date (
    date_key        INTEGER PRIMARY KEY,
    full_date       DATE NOT NULL,
    day_of_week     SMALLINT,
    day_name        VARCHAR(10),
    day_of_month    SMALLINT,
    day_of_year     SMALLINT,
    week_of_year    SMALLINT,
    month_num       SMALLINT,
    month_name      VARCHAR(10),
    quarter         SMALLINT,
    quarter_name    VARCHAR(6),
    year            SMALLINT,
    is_weekend      BOOLEAN,
    is_holiday      BOOLEAN DEFAULT FALSE,
    fiscal_year     SMALLINT,
    fiscal_quarter  SMALLINT
);

CREATE TABLE IF NOT EXISTS warehouse.dim_customer (
    cust_sk         SERIAL PRIMARY KEY,
    cust_id         TEXT NOT NULL,
    first_name      TEXT,
    last_name       TEXT,
    full_name       TEXT,
    email           TEXT,
    phone           TEXT,
    date_of_birth   DATE,
    age_band        TEXT,
    gender          CHAR(1),
    city            TEXT,
    state           TEXT,
    country         CHAR(2),
    loyalty_tier    TEXT,
    signup_date     DATE,
    is_active       BOOLEAN,
    eff_start_dt    DATE NOT NULL DEFAULT CURRENT_DATE,
    eff_end_dt      DATE,
    is_current      BOOLEAN DEFAULT TRUE,
    _load_ts        TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS warehouse.dim_product (
    prd_sk          SERIAL PRIMARY KEY,
    prd_id          TEXT NOT NULL,
    prd_name        TEXT,
    prd_category    TEXT,
    prd_subcategory TEXT,
    brand           TEXT,
    sku             TEXT,
    unit_price      NUMERIC(10,2),
    unit_cost       NUMERIC(10,2),
    gross_margin_pct NUMERIC(5,2),
    price_tier      TEXT,
    is_active       BOOLEAN,
    launch_date     DATE,
    discontinue_dt  DATE,
    eff_start_dt    DATE DEFAULT CURRENT_DATE,
    eff_end_dt      DATE,
    is_current      BOOLEAN DEFAULT TRUE,
    _load_ts        TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS warehouse.dim_store (
    store_sk        SERIAL PRIMARY KEY,
    store_id        TEXT NOT NULL,
    store_name      TEXT,
    store_type      TEXT,
    region          TEXT,
    city            TEXT,
    state           TEXT,
    country         CHAR(2),
    open_date       DATE,
    manager_name    TEXT,
    _load_ts        TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS warehouse.dim_promotion (
    promo_sk        SERIAL PRIMARY KEY,
    promo_id        TEXT,
    promo_code      TEXT,
    promo_name      TEXT,
    promo_type      TEXT,
    discount_value  NUMERIC(10,2),
    start_date      DATE,
    end_date        DATE,
    min_order_amt   NUMERIC(10,2),
    is_active       BOOLEAN,
    _load_ts        TIMESTAMPTZ DEFAULT NOW()
);

-- ── WAREHOUSE — Facts ─────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS warehouse.fact_sales (
    sales_sk        SERIAL PRIMARY KEY,
    order_date_key  INTEGER REFERENCES warehouse.dim_date(date_key),
    cust_sk         INTEGER REFERENCES warehouse.dim_customer(cust_sk),
    prd_sk          INTEGER REFERENCES warehouse.dim_product(prd_sk),
    store_sk        INTEGER REFERENCES warehouse.dim_store(store_sk),
    promo_sk        INTEGER REFERENCES warehouse.dim_promotion(promo_sk),
    order_id        TEXT,
    order_item_id   TEXT UNIQUE,
    channel         TEXT,
    order_status    TEXT,
    qty_ordered     INTEGER,
    unit_price      NUMERIC(10,2),
    unit_cost       NUMERIC(10,2),
    gross_margin_pct NUMERIC(5,2),
    discount_pct    NUMERIC(5,2),
    discount_amt    NUMERIC(12,2),
    gross_revenue   NUMERIC(12,2),
    net_revenue     NUMERIC(12,2),
    cogs            NUMERIC(12,2),
    gross_profit    NUMERIC(12,2),
    _load_ts        TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS warehouse.fact_inventory (
    inventory_sk      SERIAL PRIMARY KEY,
    snapshot_date_key INTEGER REFERENCES warehouse.dim_date(date_key),
    prd_sk            INTEGER REFERENCES warehouse.dim_product(prd_sk),
    store_sk          INTEGER REFERENCES warehouse.dim_store(store_sk),
    inventory_id      TEXT,
    qty_on_hand       INTEGER,
    qty_reserved      INTEGER,
    qty_available     INTEGER,
    reorder_point     INTEGER,
    inv_status        TEXT,
    needs_reorder     BOOLEAN,
    stock_value       NUMERIC(14,2),
    _load_ts          TIMESTAMPTZ DEFAULT NOW()
);

-- ── Indexes ───────────────────────────────────────────────────────────────
CREATE INDEX IF NOT EXISTS idx_fact_sales_date    ON warehouse.fact_sales(order_date_key);
CREATE INDEX IF NOT EXISTS idx_fact_sales_cust    ON warehouse.fact_sales(cust_sk);
CREATE INDEX IF NOT EXISTS idx_fact_sales_prd     ON warehouse.fact_sales(prd_sk);
CREATE INDEX IF NOT EXISTS idx_fact_sales_store   ON warehouse.fact_sales(store_sk);
CREATE INDEX IF NOT EXISTS idx_fact_inv_date      ON warehouse.fact_inventory(snapshot_date_key);
CREATE INDEX IF NOT EXISTS idx_fact_inv_prd       ON warehouse.fact_inventory(prd_sk);
CREATE INDEX IF NOT EXISTS idx_dim_cust_id        ON warehouse.dim_customer(cust_id) WHERE is_current;
CREATE INDEX IF NOT EXISTS idx_dim_prd_id         ON warehouse.dim_product(prd_id)   WHERE is_current;
CREATE INDEX IF NOT EXISTS idx_dim_store_id       ON warehouse.dim_store(store_id);
CREATE INDEX IF NOT EXISTS idx_dim_promo_code     ON warehouse.dim_promotion(promo_code);
"""

RESET_DDL = """
DROP SCHEMA IF EXISTS warehouse CASCADE;
DROP SCHEMA IF EXISTS staging   CASCADE;
DROP SCHEMA IF EXISTS raw       CASCADE;
"""

# ---------------------------------------------------------------------------
# CSV → raw table loading
# ---------------------------------------------------------------------------

CSV_MAP = [
    ("customers.csv",   "raw.customers",   ",",  ["cust_id","first_name","last_name","email","phone","date_of_birth","gender","address_line1","address_line2","city","state","zip_code","country","loyalty_tier","signup_date","is_active"]),
    ("products.csv",    "raw.products",    ",",  ["prd_id","prd_name","prd_category","prd_subcategory","brand","unit_price","unit_cost","sku","supplier_id","is_active","launch_date","discontinue_dt"]),
    ("stores.csv",      "raw.stores",      ",",  ["store_id","store_name","store_type","region","address","city","state","zip_code","country","open_date","manager_name"]),
    ("orders.csv",      "raw.orders",      "|",  ["order_id","order_date","cust_id","store_id","channel","order_status","promo_code","total_amt","discount_amt","net_amt"]),
    ("order_items.csv", "raw.order_items", "|",  ["order_item_id","order_id","prd_id","qty","unit_price","discount_pct","line_total"]),
    ("inventory.csv",   "raw.inventory",   ",",  ["inventory_id","prd_id","store_id","snapshot_date","qty_on_hand","qty_reserved","reorder_point","inv_status"]),
    ("promotions.csv",  "raw.promotions",  ",",  ["promo_id","promo_code","promo_name","promo_type","discount_value","start_date","end_date","min_order_amt","is_active"]),
]


def load_csvs(cur):
    for filename, table, delim, cols in CSV_MAP:
        path = SAMPLE_DIR / filename
        if not path.exists():
            logger.warning("  CSV not found: %s", path)
            continue

        with open(path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f, delimiter=delim)
            rows = list(reader)

        if not rows:
            continue

        col_names = ", ".join(cols) + ", _src_file"
        placeholders = ", ".join(["%s"] * (len(cols) + 1))
        sql = f"INSERT INTO {table} ({col_names}) VALUES ({placeholders}) ON CONFLICT DO NOTHING"

        data = []
        for r in rows:
            vals = [r.get(c.upper(), r.get(c, "")).strip() or None for c in cols]
            vals.append(filename)
            data.append(vals)

        psycopg2.extras.execute_batch(cur, sql, data, page_size=500)
        logger.info("  %-20s → %-25s  %d rows", filename, table, len(data))


# ---------------------------------------------------------------------------
# Transforms
# ---------------------------------------------------------------------------

STAGING_SQL = """
-- staging.customers
INSERT INTO staging.customers
    (cust_id, first_name, last_name, full_name, email, phone,
     date_of_birth, age_band, gender, address_line1, address_line2,
     city, state, zip_code, country, loyalty_tier, signup_date,
     is_active, eff_start_dt, is_current)
SELECT DISTINCT ON (TRIM(cust_id))
    TRIM(cust_id),
    INITCAP(TRIM(first_name)),
    INITCAP(TRIM(last_name)),
    INITCAP(TRIM(first_name)) || ' ' || INITCAP(TRIM(last_name)),
    LOWER(TRIM(email)),
    REGEXP_REPLACE(TRIM(COALESCE(phone,'')), '[^0-9\\-\\+\\(\\) ]', '', 'g'),
    CASE WHEN date_of_birth ~ '^\\d{4}-\\d{2}-\\d{2}$'
         THEN date_of_birth::DATE ELSE NULL END,
    CASE
        WHEN date_of_birth ~ '^\\d{4}-\\d{2}-\\d{2}$' THEN
            CASE
                WHEN DATE_PART('year',AGE(date_of_birth::DATE)) < 25 THEN '18-24'
                WHEN DATE_PART('year',AGE(date_of_birth::DATE)) < 35 THEN '25-34'
                WHEN DATE_PART('year',AGE(date_of_birth::DATE)) < 45 THEN '35-44'
                WHEN DATE_PART('year',AGE(date_of_birth::DATE)) < 55 THEN '45-54'
                ELSE '55+'
            END
        ELSE 'UNKNOWN'
    END,
    UPPER(SUBSTR(TRIM(COALESCE(gender,'U')),1,1)),
    TRIM(address_line1), TRIM(address_line2),
    INITCAP(TRIM(city)), UPPER(TRIM(state)), TRIM(zip_code),
    UPPER(TRIM(country)),
    UPPER(TRIM(loyalty_tier)),
    CASE WHEN signup_date ~ '^\\d{4}-\\d{2}-\\d{2}$'
         THEN signup_date::DATE ELSE NULL END,
    UPPER(TRIM(is_active)) IN ('Y','1','TRUE','YES'),
    CURRENT_DATE, TRUE
FROM raw.customers
WHERE TRIM(cust_id) IS NOT NULL
ON CONFLICT DO NOTHING;

-- staging.products
INSERT INTO staging.products
    (prd_id, prd_name, prd_category, prd_subcategory, brand,
     unit_price, unit_cost, gross_margin_pct, sku, supplier_id,
     is_active, launch_date, discontinue_dt)
SELECT DISTINCT ON (TRIM(prd_id))
    TRIM(prd_id),
    TRIM(prd_name), UPPER(TRIM(prd_category)), UPPER(TRIM(prd_subcategory)),
    TRIM(brand),
    NULLIF(unit_price,'')::NUMERIC(10,2),
    NULLIF(unit_cost, '')::NUMERIC(10,2),
    CASE WHEN NULLIF(unit_price,'')::NUMERIC > 0
         THEN ROUND((NULLIF(unit_price,'')::NUMERIC - NULLIF(unit_cost,'')::NUMERIC)
                   / NULLIF(unit_price,'')::NUMERIC * 100, 2)
         ELSE NULL END,
    UPPER(TRIM(sku)), TRIM(supplier_id),
    UPPER(TRIM(is_active)) IN ('Y','1','TRUE'),
    CASE WHEN launch_date ~ '^\\d{4}-\\d{2}-\\d{2}$' THEN launch_date::DATE ELSE NULL END,
    CASE WHEN discontinue_dt ~ '^\\d{4}-\\d{2}-\\d{2}$' AND discontinue_dt <> 'NULL' THEN discontinue_dt::DATE ELSE NULL END
FROM raw.products
WHERE TRIM(prd_id) IS NOT NULL
ON CONFLICT DO NOTHING;

-- staging.orders
INSERT INTO staging.orders
    (order_id, order_date, order_year, order_month, order_quarter,
     cust_id, store_id, channel, order_status, promo_code,
     total_amt, discount_amt, net_amt, is_discounted)
SELECT DISTINCT ON (TRIM(order_id))
    TRIM(order_id),
    order_date::DATE,
    EXTRACT(YEAR  FROM order_date::DATE)::SMALLINT,
    EXTRACT(MONTH FROM order_date::DATE)::SMALLINT,
    EXTRACT(QUARTER FROM order_date::DATE)::SMALLINT,
    TRIM(cust_id), TRIM(store_id),
    UPPER(TRIM(channel)), UPPER(TRIM(order_status)),
    NULLIF(TRIM(promo_code),''),
    NULLIF(total_amt,   '')::NUMERIC(12,2),
    NULLIF(discount_amt,'')::NUMERIC(12,2),
    NULLIF(net_amt,     '')::NUMERIC(12,2),
    COALESCE(NULLIF(discount_amt,'')::NUMERIC(12,2), 0) > 0
FROM raw.orders
WHERE TRIM(order_id) IS NOT NULL
AND   order_date ~ '^\\d{4}-\\d{2}-\\d{2}$'
ON CONFLICT DO NOTHING;

-- staging.order_items
INSERT INTO staging.order_items
    (order_item_id, order_id, prd_id, qty, unit_price,
     discount_pct, line_total, discount_amt)
SELECT DISTINCT ON (TRIM(order_item_id))
    TRIM(order_item_id), TRIM(order_id), TRIM(prd_id),
    NULLIF(qty,'')::INTEGER,
    NULLIF(unit_price,  '')::NUMERIC(10,2),
    NULLIF(discount_pct,'')::NUMERIC(5,2),
    NULLIF(line_total,  '')::NUMERIC(12,2),
    ROUND(
        COALESCE(NULLIF(unit_price,'')::NUMERIC,0)
        * COALESCE(NULLIF(qty,'')::INTEGER,0)
        * COALESCE(NULLIF(discount_pct,'')::NUMERIC,0) / 100
    , 2)
FROM raw.order_items
WHERE TRIM(order_item_id) IS NOT NULL
ON CONFLICT DO NOTHING;

-- staging.inventory
INSERT INTO staging.inventory
    (inventory_id, prd_id, store_id, snapshot_date,
     qty_on_hand, qty_reserved, qty_available, reorder_point,
     inv_status, needs_reorder)
SELECT DISTINCT ON (TRIM(inventory_id))
    TRIM(inventory_id), TRIM(prd_id), TRIM(store_id),
    snapshot_date::DATE,
    NULLIF(qty_on_hand,  '')::INTEGER,
    NULLIF(qty_reserved, '')::INTEGER,
    COALESCE(NULLIF(qty_on_hand,'')::INTEGER,0)
        - COALESCE(NULLIF(qty_reserved,'')::INTEGER,0),
    NULLIF(reorder_point,'')::INTEGER,
    UPPER(TRIM(inv_status)),
    COALESCE(NULLIF(qty_on_hand,'')::INTEGER,0)
        <= COALESCE(NULLIF(reorder_point,'')::INTEGER,0)
FROM raw.inventory
WHERE TRIM(inventory_id) IS NOT NULL
AND   snapshot_date ~ '^\\d{4}-\\d{2}-\\d{2}$'
ON CONFLICT DO NOTHING;

-- staging.promotions
INSERT INTO staging.promotions
    (promo_id, promo_code, promo_name, promo_type,
     discount_value, start_date, end_date, min_order_amt, is_active)
SELECT DISTINCT ON (TRIM(promo_id))
    TRIM(promo_id), UPPER(TRIM(promo_code)), TRIM(promo_name), UPPER(TRIM(promo_type)),
    NULLIF(discount_value,'')::NUMERIC(10,2),
    start_date::DATE, end_date::DATE,
    NULLIF(min_order_amt,'')::NUMERIC(10,2),
    UPPER(TRIM(is_active)) IN ('Y','1','TRUE')
FROM raw.promotions
WHERE TRIM(promo_id) IS NOT NULL
ON CONFLICT DO NOTHING;
"""

WAREHOUSE_SQL = """
-- dim_date: generate 2023-01-01 → 2026-12-31
INSERT INTO warehouse.dim_date
SELECT
    TO_CHAR(d,'YYYYMMDD')::INTEGER,
    d,
    EXTRACT(DOW  FROM d)::SMALLINT + 1,
    TO_CHAR(d,'Day'),
    EXTRACT(DAY  FROM d)::SMALLINT,
    EXTRACT(DOY  FROM d)::SMALLINT,
    EXTRACT(WEEK FROM d)::SMALLINT,
    EXTRACT(MONTH   FROM d)::SMALLINT,
    TO_CHAR(d,'Month'),
    EXTRACT(QUARTER FROM d)::SMALLINT,
    'Q' || EXTRACT(QUARTER FROM d)::INTEGER,
    EXTRACT(YEAR FROM d)::SMALLINT,
    EXTRACT(DOW FROM d) IN (0,6),
    FALSE,
    CASE WHEN EXTRACT(MONTH FROM d) >= 2
         THEN EXTRACT(YEAR FROM d)::SMALLINT
         ELSE (EXTRACT(YEAR FROM d) - 1)::SMALLINT END,
    CASE WHEN EXTRACT(MONTH FROM d) BETWEEN 2 AND 4  THEN 1
         WHEN EXTRACT(MONTH FROM d) BETWEEN 5 AND 7  THEN 2
         WHEN EXTRACT(MONTH FROM d) BETWEEN 8 AND 10 THEN 3
         ELSE 4 END::SMALLINT
FROM GENERATE_SERIES('2023-01-01'::DATE, '2026-12-31'::DATE, '1 day') d
ON CONFLICT DO NOTHING;

-- dim_customer: unknown member
INSERT INTO warehouse.dim_customer
    (cust_sk, cust_id, full_name, eff_start_dt, is_current)
VALUES (-1, 'UNKNOWN', 'Unknown Customer', '1900-01-01', TRUE)
ON CONFLICT DO NOTHING;

-- dim_customer: from staging
INSERT INTO warehouse.dim_customer
    (cust_id, first_name, last_name, full_name, email, phone, date_of_birth,
     age_band, gender, city, state, country, loyalty_tier,
     signup_date, is_active, eff_start_dt, is_current)
SELECT
    cust_id, first_name, last_name, full_name, email, phone, date_of_birth,
    age_band, gender, city, state, country, loyalty_tier,
    signup_date, is_active, CURRENT_DATE, TRUE
FROM staging.customers
WHERE is_current = TRUE
ON CONFLICT DO NOTHING;

-- dim_product: unknown member
INSERT INTO warehouse.dim_product
    (prd_sk, prd_id, prd_name, eff_start_dt, is_current)
VALUES (-1, 'UNKNOWN', 'Unknown Product', '1900-01-01', TRUE)
ON CONFLICT DO NOTHING;

-- dim_product: from staging
INSERT INTO warehouse.dim_product
    (prd_id, prd_name, prd_category, prd_subcategory, brand, sku,
     unit_price, unit_cost, gross_margin_pct,
     price_tier, is_active, launch_date, discontinue_dt, eff_start_dt, is_current)
SELECT
    prd_id, prd_name, prd_category, prd_subcategory, brand, sku,
    unit_price, unit_cost, gross_margin_pct,
    CASE WHEN unit_price < 50  THEN 'BUDGET'
         WHEN unit_price < 120 THEN 'MID'
         ELSE 'PREMIUM' END,
    is_active, launch_date, discontinue_dt, CURRENT_DATE, TRUE
FROM staging.products
ON CONFLICT DO NOTHING;

-- dim_store: unknown member
INSERT INTO warehouse.dim_store (store_sk, store_id, store_name)
VALUES (-1, 'UNKNOWN', 'Unknown Store')
ON CONFLICT DO NOTHING;

-- dim_store: from raw
INSERT INTO warehouse.dim_store
    (store_id, store_name, store_type, region, city, state, country, open_date, manager_name)
SELECT
    TRIM(store_id), TRIM(store_name), UPPER(TRIM(store_type)), UPPER(TRIM(region)),
    INITCAP(TRIM(city)), UPPER(TRIM(state)), UPPER(TRIM(country)),
    CASE WHEN open_date ~ '^\\d{4}-\\d{2}-\\d{2}$' THEN open_date::DATE ELSE NULL END,
    TRIM(manager_name)
FROM raw.stores
ON CONFLICT DO NOTHING;

-- dim_promotion: unknown member
INSERT INTO warehouse.dim_promotion (promo_sk, promo_id, promo_code, promo_name)
VALUES (-1, 'NONE', 'NONE', 'No Promotion')
ON CONFLICT DO NOTHING;

-- dim_promotion: from staging
INSERT INTO warehouse.dim_promotion
    (promo_id, promo_code, promo_name, promo_type, discount_value,
     start_date, end_date, min_order_amt, is_active)
SELECT promo_id, promo_code, promo_name, promo_type, discount_value,
       start_date, end_date, min_order_amt, is_active
FROM staging.promotions
ON CONFLICT DO NOTHING;

-- fact_sales
INSERT INTO warehouse.fact_sales
    (order_date_key, cust_sk, prd_sk, store_sk, promo_sk,
     order_id, order_item_id, channel, order_status,
     qty_ordered, unit_price, unit_cost, gross_margin_pct,
     discount_pct, discount_amt, gross_revenue, net_revenue, cogs, gross_profit)
SELECT
    TO_CHAR(o.order_date,'YYYYMMDD')::INTEGER,
    COALESCE(dc.cust_sk, -1),
    COALESCE(dp.prd_sk,  -1),
    COALESCE(ds.store_sk,-1),
    COALESCE(dpr.promo_sk,-1),
    o.order_id, oi.order_item_id,
    o.channel, o.order_status,
    oi.qty, oi.unit_price, dp.unit_cost, dp.gross_margin_pct,
    oi.discount_pct, oi.discount_amt,
    oi.qty * oi.unit_price,
    oi.line_total,
    oi.qty * COALESCE(dp.unit_cost, 0),
    oi.line_total - oi.qty * COALESCE(dp.unit_cost, 0)
FROM staging.order_items           oi
JOIN staging.orders                o   ON oi.order_id   = o.order_id
LEFT JOIN warehouse.dim_customer   dc  ON o.cust_id     = dc.cust_id  AND dc.is_current
LEFT JOIN warehouse.dim_product    dp  ON oi.prd_id     = dp.prd_id   AND dp.is_current
LEFT JOIN warehouse.dim_store      ds  ON o.store_id    = ds.store_id
LEFT JOIN warehouse.dim_promotion  dpr ON COALESCE(o.promo_code,'NONE') = dpr.promo_code
ON CONFLICT (order_item_id) DO NOTHING;

-- fact_inventory
INSERT INTO warehouse.fact_inventory
    (snapshot_date_key, prd_sk, store_sk, inventory_id,
     qty_on_hand, qty_reserved, qty_available, reorder_point,
     inv_status, needs_reorder, stock_value)
SELECT
    TO_CHAR(si.snapshot_date,'YYYYMMDD')::INTEGER,
    COALESCE(dp.prd_sk,  -1),
    COALESCE(ds.store_sk,-1),
    si.inventory_id,
    si.qty_on_hand, si.qty_reserved, si.qty_available, si.reorder_point,
    si.inv_status, si.needs_reorder,
    si.qty_on_hand * COALESCE(dp.unit_cost, 0)
FROM staging.inventory            si
LEFT JOIN warehouse.dim_product   dp ON si.prd_id   = dp.prd_id   AND dp.is_current
LEFT JOIN warehouse.dim_store     ds ON si.store_id = ds.store_id
ON CONFLICT DO NOTHING;
"""

# ---------------------------------------------------------------------------
# Mart views (Postgres)
# ---------------------------------------------------------------------------

MART_VIEWS_SQL = """
CREATE SCHEMA IF NOT EXISTS mart;

CREATE OR REPLACE VIEW mart.daily_sales_summary AS
SELECT
    d.full_date, d.year, d.month_name, d.quarter_name, d.day_name, d.is_weekend,
    f.channel, p.prd_category, p.prd_subcategory, p.brand,
    s.region, s.country,
    COUNT(DISTINCT f.order_id)                           AS order_count,
    SUM(f.qty_ordered)                                   AS units_sold,
    SUM(f.gross_revenue)                                 AS gross_revenue,
    SUM(f.discount_amt)                                  AS total_discount,
    SUM(f.net_revenue)                                   AS net_revenue,
    SUM(f.cogs)                                          AS cogs,
    SUM(f.gross_profit)                                  AS gross_profit,
    ROUND(SUM(f.gross_profit)/NULLIF(SUM(f.net_revenue),0)*100,2) AS gross_margin_pct,
    ROUND(SUM(f.net_revenue)/NULLIF(COUNT(DISTINCT f.order_id),0),2) AS avg_order_value
FROM warehouse.fact_sales        f
JOIN warehouse.dim_date          d  ON f.order_date_key = d.date_key
JOIN warehouse.dim_product       p  ON f.prd_sk         = p.prd_sk
JOIN warehouse.dim_store         s  ON f.store_sk       = s.store_sk
WHERE f.order_status NOT IN ('CANCELLED','RETURNED')
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12;

CREATE OR REPLACE VIEW mart.product_performance AS
SELECT
    p.prd_id, p.prd_name, p.prd_category, p.prd_subcategory,
    p.brand, p.price_tier,
    d.year, d.month_num, d.month_name, d.quarter_name,
    SUM(f.qty_ordered)                                   AS units_sold,
    SUM(f.net_revenue)                                   AS net_revenue,
    SUM(f.gross_profit)                                  AS gross_profit,
    ROUND(SUM(f.gross_profit)/NULLIF(SUM(f.net_revenue),0)*100,2) AS gross_margin_pct,
    COUNT(DISTINCT f.cust_sk)                            AS unique_buyers
FROM warehouse.fact_sales        f
JOIN warehouse.dim_product       p  ON f.prd_sk         = p.prd_sk AND p.is_current
JOIN warehouse.dim_date          d  ON f.order_date_key = d.date_key
GROUP BY 1,2,3,4,5,6,7,8,9,10;

CREATE OR REPLACE VIEW mart.customer_360 AS
SELECT
    c.cust_id, c.full_name, c.loyalty_tier, c.country,
    c.age_band, c.gender, c.is_active,
    COUNT(DISTINCT f.order_id)                           AS total_orders,
    SUM(f.net_revenue)                                   AS lifetime_value,
    ROUND(SUM(f.net_revenue)/NULLIF(COUNT(DISTINCT f.order_id),0),2) AS avg_order_value,
    MIN(d.full_date)                                     AS first_order_date,
    MAX(d.full_date)                                     AS last_order_date,
    (CURRENT_DATE - MAX(d.full_date))                    AS days_since_last_order,
    CASE
        WHEN (CURRENT_DATE - MAX(d.full_date)) <= 30  THEN 'ACTIVE'
        WHEN (CURRENT_DATE - MAX(d.full_date)) <= 90  THEN 'AT_RISK'
        WHEN (CURRENT_DATE - MAX(d.full_date)) <= 180 THEN 'LAPSED'
        ELSE 'CHURNED'
    END                                                  AS recency_segment
FROM warehouse.dim_customer      c
LEFT JOIN warehouse.fact_sales   f  ON c.cust_sk        = f.cust_sk
                                    AND f.order_status NOT IN ('CANCELLED','RETURNED')
LEFT JOIN warehouse.dim_date     d  ON f.order_date_key = d.date_key
WHERE c.is_current AND c.cust_sk <> -1
GROUP BY 1,2,3,4,5,6,7;

CREATE OR REPLACE VIEW mart.inventory_status AS
SELECT
    p.prd_id, p.prd_name, p.prd_category, p.brand,
    s.store_id, s.store_name, s.region,
    d.full_date                                          AS snapshot_date,
    fi.qty_on_hand, fi.qty_available, fi.reorder_point,
    fi.inv_status, fi.needs_reorder, fi.stock_value
FROM warehouse.fact_inventory    fi
JOIN warehouse.dim_date          d  ON fi.snapshot_date_key = d.date_key
JOIN warehouse.dim_product       p  ON fi.prd_sk   = p.prd_sk AND p.is_current
JOIN warehouse.dim_store         s  ON fi.store_sk = s.store_sk;
"""


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run_sql_block(cur, sql_block: str, label: str):
    """Split on ; and execute each statement."""
    statements = [s.strip() for s in sql_block.split(";") if s.strip()]
    ok = err = 0
    for stmt in statements:
        try:
            cur.execute(stmt)
            ok += 1
        except Exception as e:
            logger.error("  [%s] FAIL: %s\n  stmt: %.120s", label, e, stmt)
            err += 1
    return ok, err


def show_counts(cur):
    tables = [
        ("raw","customers"), ("raw","products"), ("raw","stores"),
        ("raw","orders"), ("raw","order_items"), ("raw","inventory"), ("raw","promotions"),
        ("staging","customers"), ("staging","products"), ("staging","orders"),
        ("staging","order_items"), ("staging","inventory"), ("staging","promotions"),
        ("warehouse","dim_date"), ("warehouse","dim_customer"), ("warehouse","dim_product"),
        ("warehouse","dim_store"), ("warehouse","dim_promotion"),
        ("warehouse","fact_sales"), ("warehouse","fact_inventory"),
    ]
    print(f"\n  {'Table':<45} {'Rows':>8}")
    print("  " + "-" * 55)
    for schema, table in tables:
        try:
            cur.execute(f"SELECT COUNT(*) FROM {schema}.{table}")
            cnt = cur.fetchone()[0]
            print(f"  {schema+'.'+table:<45} {cnt:>8,}")
        except Exception:
            print(f"  {schema+'.'+table:<45} {'N/A':>8}")
    print()


def parse_args():
    p = argparse.ArgumentParser(description="Migrate SportZone to Neon Postgres")
    p.add_argument("--reset",  action="store_true", help="DROP all schemas and re-create")
    p.add_argument("--status", action="store_true", help="Show row counts only")
    return p.parse_args()


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-7s  %(message)s",
        datefmt="%H:%M:%S",
    )
    args = parse_args()

    url = load_neon_url()
    logger.info("Connecting to Neon Postgres …")
    conn = get_neon_conn()
    conn.autocommit = False
    cur = conn.cursor()
    logger.info("Connected.\n")

    if args.status:
        show_counts(cur)
        cur.close(); conn.close()
        return

    if args.reset:
        logger.warning("RESET: dropping all schemas …")
        cur.execute(RESET_DDL)
        conn.commit()
        logger.info("Schemas dropped.\n")

    # 1 — DDL
    logger.info("── Creating schemas + tables …")
    ok, err = run_sql_block(cur, DDL, "DDL")
    conn.commit()
    logger.info("   %d statements OK, %d errors\n", ok, err)

    # 2 — Load CSVs into raw
    logger.info("── Loading sample CSVs into raw …")
    load_csvs(cur)
    conn.commit()
    logger.info("")

    # 3 — Staging transforms
    logger.info("── Running raw → staging transforms …")
    ok, err = run_sql_block(cur, STAGING_SQL, "staging")
    conn.commit()
    logger.info("   %d statements OK, %d errors\n", ok, err)

    # 4 — Warehouse load
    logger.info("── Loading warehouse (dims + facts) …")
    ok, err = run_sql_block(cur, WAREHOUSE_SQL, "warehouse")
    conn.commit()
    logger.info("   %d statements OK, %d errors\n", ok, err)

    # 5 — Mart views
    logger.info("── Creating mart views …")
    ok, err = run_sql_block(cur, MART_VIEWS_SQL, "mart")
    conn.commit()
    logger.info("   %d statements OK, %d errors\n", ok, err)

    # 6 — Summary
    logger.info("── Row counts ──")
    show_counts(cur)

    cur.close()
    conn.close()
    logger.info("Migration complete.")


if __name__ == "__main__":
    main()
