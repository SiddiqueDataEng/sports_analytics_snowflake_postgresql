"""
local_db.py
===========
Creates and populates a local SQLite database from the sample CSV files.
Useful for development and testing without a live Snowflake connection.

Usage
-----
    python deploy/local_db.py                    # create db, load all tables
    python deploy/local_db.py --db my_test.db    # custom db path
    python deploy/local_db.py --query "SELECT * FROM raw_customers"

The SQLite schema mirrors the RAW + STAGING layers of the Snowflake project.
"""

import argparse
import csv
import logging
import sqlite3
import sys
from pathlib import Path

DEPLOY_DIR  = Path(__file__).parent
PROJECT_DIR = DEPLOY_DIR.parent
SAMPLE_DIR  = PROJECT_DIR / "01_raw" / "04_sample_data"
DEFAULT_DB  = PROJECT_DIR / "local_dev.db"

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# DDL – mirrors RAW layer (SQLite-compatible types)
# ---------------------------------------------------------------------------

RAW_DDL = """
CREATE TABLE IF NOT EXISTS raw_customers (
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
    _load_ts        TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS raw_products (
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
    _load_ts        TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS raw_stores (
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
    _load_ts        TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS raw_orders (
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
    _load_ts        TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS raw_order_items (
    order_item_id   TEXT,
    order_id        TEXT,
    prd_id          TEXT,
    qty             TEXT,
    unit_price      TEXT,
    discount_pct    TEXT,
    line_total      TEXT,
    _src_file       TEXT,
    _load_ts        TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS raw_inventory (
    inventory_id    TEXT,
    prd_id          TEXT,
    store_id        TEXT,
    snapshot_date   TEXT,
    qty_on_hand     TEXT,
    qty_reserved    TEXT,
    reorder_point   TEXT,
    inv_status      TEXT,
    _src_file       TEXT,
    _load_ts        TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS raw_promotions (
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
    _load_ts        TEXT DEFAULT (datetime('now'))
);
"""

# ---------------------------------------------------------------------------
# Staging DDL (typed SQLite)
# ---------------------------------------------------------------------------

STAGING_DDL = """
CREATE TABLE IF NOT EXISTS stg_customers (
    cust_id         TEXT NOT NULL,
    first_name      TEXT,
    last_name       TEXT,
    full_name       TEXT,
    email           TEXT,
    phone           TEXT,
    date_of_birth   TEXT,
    age_band        TEXT,
    gender          TEXT,
    address_line1   TEXT,
    address_line2   TEXT,
    city            TEXT,
    state           TEXT,
    zip_code        TEXT,
    country         TEXT,
    loyalty_tier    TEXT,
    signup_date     TEXT,
    is_active       INTEGER,
    eff_start_dt    TEXT,
    eff_end_dt      TEXT,
    is_current      INTEGER DEFAULT 1,
    _load_ts        TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS stg_products (
    prd_id          TEXT NOT NULL,
    prd_name        TEXT,
    prd_category    TEXT,
    prd_subcategory TEXT,
    brand           TEXT,
    unit_price      REAL,
    unit_cost       REAL,
    gross_margin_pct REAL,
    sku             TEXT,
    supplier_id     TEXT,
    is_active       INTEGER,
    launch_date     TEXT,
    discontinue_dt  TEXT,
    _load_ts        TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS stg_orders (
    order_id        TEXT NOT NULL,
    order_date      TEXT,
    order_year      INTEGER,
    order_month     INTEGER,
    order_quarter   INTEGER,
    cust_id         TEXT,
    store_id        TEXT,
    channel         TEXT,
    order_status    TEXT,
    promo_code      TEXT,
    total_amt       REAL,
    discount_amt    REAL,
    net_amt         REAL,
    is_discounted   INTEGER,
    _load_ts        TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS stg_order_items (
    order_item_id   TEXT NOT NULL,
    order_id        TEXT,
    prd_id          TEXT,
    qty             INTEGER,
    unit_price      REAL,
    discount_pct    REAL,
    line_total      REAL,
    discount_amt    REAL,
    _load_ts        TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS stg_inventory (
    inventory_id    TEXT NOT NULL,
    prd_id          TEXT,
    store_id        TEXT,
    snapshot_date   TEXT,
    qty_on_hand     INTEGER,
    qty_reserved    INTEGER,
    qty_available   INTEGER,
    reorder_point   INTEGER,
    inv_status      TEXT,
    needs_reorder   INTEGER,
    _load_ts        TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS stg_promotions (
    promo_id        TEXT NOT NULL,
    promo_code      TEXT,
    promo_name      TEXT,
    promo_type      TEXT,
    discount_value  REAL,
    start_date      TEXT,
    end_date        TEXT,
    min_order_amt   REAL,
    is_active       INTEGER,
    _load_ts        TEXT DEFAULT (datetime('now'))
);
"""

# ---------------------------------------------------------------------------
# Warehouse DDL (star schema in SQLite)
# ---------------------------------------------------------------------------

WAREHOUSE_DDL = """
CREATE TABLE IF NOT EXISTS dim_date (
    date_key        INTEGER PRIMARY KEY,
    full_date       TEXT,
    day_of_week     INTEGER,
    day_name        TEXT,
    day_of_month    INTEGER,
    day_of_year     INTEGER,
    week_of_year    INTEGER,
    month_num       INTEGER,
    month_name      TEXT,
    quarter         INTEGER,
    quarter_name    TEXT,
    year            INTEGER,
    is_weekend      INTEGER,
    is_holiday      INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS dim_customer (
    cust_sk         INTEGER PRIMARY KEY AUTOINCREMENT,
    cust_id         TEXT NOT NULL,
    first_name      TEXT,
    last_name       TEXT,
    full_name       TEXT,
    email           TEXT,
    phone           TEXT,
    date_of_birth   TEXT,
    age_band        TEXT,
    gender          TEXT,
    city            TEXT,
    state           TEXT,
    country         TEXT,
    loyalty_tier    TEXT,
    signup_date     TEXT,
    is_active       INTEGER,
    eff_start_dt    TEXT,
    eff_end_dt      TEXT,
    is_current      INTEGER DEFAULT 1
);

CREATE TABLE IF NOT EXISTS dim_product (
    prd_sk          INTEGER PRIMARY KEY AUTOINCREMENT,
    prd_id          TEXT NOT NULL,
    prd_name        TEXT,
    prd_category    TEXT,
    prd_subcategory TEXT,
    brand           TEXT,
    sku             TEXT,
    unit_price      REAL,
    unit_cost       REAL,
    gross_margin_pct REAL,
    price_tier      TEXT,
    is_active       INTEGER,
    launch_date     TEXT,
    discontinue_dt  TEXT,
    eff_start_dt    TEXT,
    eff_end_dt      TEXT,
    is_current      INTEGER DEFAULT 1
);

CREATE TABLE IF NOT EXISTS dim_store (
    store_sk        INTEGER PRIMARY KEY AUTOINCREMENT,
    store_id        TEXT NOT NULL,
    store_name      TEXT,
    store_type      TEXT,
    region          TEXT,
    city            TEXT,
    state           TEXT,
    country         TEXT,
    open_date       TEXT,
    manager_name    TEXT
);

CREATE TABLE IF NOT EXISTS dim_promotion (
    promo_sk        INTEGER PRIMARY KEY AUTOINCREMENT,
    promo_id        TEXT,
    promo_code      TEXT,
    promo_name      TEXT,
    promo_type      TEXT,
    discount_value  REAL,
    start_date      TEXT,
    end_date        TEXT,
    min_order_amt   REAL,
    is_active       INTEGER
);

CREATE TABLE IF NOT EXISTS fact_sales (
    sales_sk        INTEGER PRIMARY KEY AUTOINCREMENT,
    order_date_key  INTEGER,
    cust_sk         INTEGER,
    prd_sk          INTEGER,
    store_sk        INTEGER,
    promo_sk        INTEGER,
    order_id        TEXT,
    order_item_id   TEXT UNIQUE,
    channel         TEXT,
    order_status    TEXT,
    qty_ordered     INTEGER,
    unit_price      REAL,
    unit_cost       REAL,
    gross_margin_pct REAL,
    discount_pct    REAL,
    discount_amt    REAL,
    gross_revenue   REAL,
    net_revenue     REAL,
    cogs            REAL,
    gross_profit    REAL
);

CREATE TABLE IF NOT EXISTS fact_inventory (
    inventory_sk    INTEGER PRIMARY KEY AUTOINCREMENT,
    snapshot_date_key INTEGER,
    prd_sk          INTEGER,
    store_sk        INTEGER,
    inventory_id    TEXT,
    qty_on_hand     INTEGER,
    qty_reserved    INTEGER,
    qty_available   INTEGER,
    reorder_point   INTEGER,
    inv_status      TEXT,
    needs_reorder   INTEGER,
    stock_value     REAL
);
"""


# ---------------------------------------------------------------------------
# CSV → table mapping  (delimiter aware)
# ---------------------------------------------------------------------------

CSV_MAP = [
    ("customers.csv",   "raw_customers",   ","),
    ("products.csv",    "raw_products",    ","),
    ("stores.csv",      "raw_stores",      ","),
    ("orders.csv",      "raw_orders",      "|"),
    ("order_items.csv", "raw_order_items", "|"),
    ("inventory.csv",   "raw_inventory",   ","),
    ("promotions.csv",  "raw_promotions",  ","),
]


def load_csv(conn: sqlite3.Connection, csv_path: Path, table: str, delimiter: str):
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter=delimiter)
        rows = list(reader)

    if not rows:
        logger.warning("  %s is empty", csv_path.name)
        return

    cols = [c.lower() for c in rows[0].keys()]
    placeholders = ", ".join("?" * len(cols))
    col_names    = ", ".join(cols)

    sql = f"INSERT OR IGNORE INTO {table} ({col_names}, _src_file) VALUES ({placeholders}, ?)"
    data = [tuple(r[c] for c in rows[0].keys()) + (csv_path.name,) for r in rows]

    conn.executemany(sql, data)
    conn.commit()
    logger.info("  %-20s → %-20s  %d rows", csv_path.name, table, len(data))


def run_staging_transforms(conn: sqlite3.Connection):
    """Simple Python-driven transforms from raw → staging (SQLite doesn't have TRY_TO_DATE etc.)."""
    from datetime import date

    def to_date(s):
        try:
            return str(date.fromisoformat(s.strip())) if s and s.strip() not in ("NULL","null","") else None
        except Exception:
            return None

    def to_float(s):
        try:
            return float(s.strip()) if s and s.strip() not in ("NULL","null","") else None
        except Exception:
            return None

    def to_int(s):
        try:
            return int(s.strip()) if s and s.strip() not in ("NULL","null","") else None
        except Exception:
            return None

    def yn_bool(s):
        return 1 if str(s).strip().upper() in ("Y","1","TRUE","YES") else 0

    def age_band(dob_str):
        if not dob_str:
            return "UNKNOWN"
        try:
            from datetime import date
            dob  = date.fromisoformat(dob_str)
            age  = (date.today() - dob).days // 365
            if age < 25: return "18-24"
            if age < 35: return "25-34"
            if age < 45: return "35-44"
            if age < 55: return "45-54"
            return "55+"
        except Exception:
            return "UNKNOWN"

    # ---- STG_CUSTOMERS ----
    cur = conn.execute("SELECT * FROM raw_customers")
    cols = [d[0] for d in cur.description]
    rows = [dict(zip(cols, r)) for r in cur.fetchall()]

    seen_ids = set()
    stg_customers = []
    for r in rows:
        cid = (r.get("cust_id") or "").strip()
        if not cid or cid in seen_ids:
            continue
        seen_ids.add(cid)
        fn = (r.get("first_name") or "").strip().title()
        ln = (r.get("last_name")  or "").strip().title()
        dob = to_date(r.get("date_of_birth",""))
        stg_customers.append((
            cid, fn, ln, f"{fn} {ln}",
            (r.get("email") or "").strip().lower(),
            (r.get("phone") or "").strip(),
            dob, age_band(dob),
            (r.get("gender") or "U").strip().upper()[:1],
            (r.get("address_line1") or "").strip(),
            (r.get("address_line2") or "").strip(),
            (r.get("city")    or "").strip().title(),
            (r.get("state")   or "").strip().upper(),
            (r.get("zip_code")or "").strip(),
            (r.get("country") or "").strip().upper(),
            (r.get("loyalty_tier") or "").strip().upper(),
            to_date(r.get("signup_date","")),
            yn_bool(r.get("is_active","N")),
            str(date.today()), None, 1
        ))

    conn.executemany("""
        INSERT OR IGNORE INTO stg_customers
        (cust_id,first_name,last_name,full_name,email,phone,date_of_birth,age_band,
         gender,address_line1,address_line2,city,state,zip_code,country,loyalty_tier,
         signup_date,is_active,eff_start_dt,eff_end_dt,is_current)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, stg_customers)
    logger.info("  stg_customers: %d rows", len(stg_customers))

    # ---- STG_PRODUCTS ----
    cur = conn.execute("SELECT * FROM raw_products")
    cols = [d[0] for d in cur.description]
    rows = [dict(zip(cols, r)) for r in cur.fetchall()]
    stg_products = []
    for r in rows:
        pid  = (r.get("prd_id") or "").strip()
        if not pid: continue
        price = to_float(r.get("unit_price",""))
        cost  = to_float(r.get("unit_cost",""))
        margin = round((price - cost) / price * 100, 2) if price and cost and price > 0 else None
        stg_products.append((
            pid,
            (r.get("prd_name")        or "").strip(),
            (r.get("prd_category")    or "").strip().upper(),
            (r.get("prd_subcategory") or "").strip().upper(),
            (r.get("brand")           or "").strip(),
            price, cost, margin,
            (r.get("sku")             or "").strip().upper(),
            (r.get("supplier_id")     or "").strip(),
            yn_bool(r.get("is_active","N")),
            to_date(r.get("launch_date","")),
            to_date(r.get("discontinue_dt","")),
        ))
    conn.executemany("""
        INSERT OR IGNORE INTO stg_products
        (prd_id,prd_name,prd_category,prd_subcategory,brand,unit_price,unit_cost,
         gross_margin_pct,sku,supplier_id,is_active,launch_date,discontinue_dt)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, stg_products)
    logger.info("  stg_products: %d rows", len(stg_products))

    # ---- STG_ORDERS ----
    cur = conn.execute("SELECT * FROM raw_orders")
    cols = [d[0] for d in cur.description]
    rows = [dict(zip(cols, r)) for r in cur.fetchall()]
    stg_orders = []
    for r in rows:
        oid = (r.get("order_id") or "").strip()
        if not oid: continue
        od  = to_date(r.get("order_date",""))
        disc = to_float(r.get("discount_amt","")) or 0.0
        stg_orders.append((
            oid, od,
            int(od[:4])        if od else None,
            int(od[5:7])       if od else None,
            (int(od[5:7])-1)//3+1 if od else None,
            (r.get("cust_id")  or "").strip(),
            (r.get("store_id") or "").strip(),
            (r.get("channel")  or "").strip().upper(),
            (r.get("order_status") or "").strip().upper(),
            (r.get("promo_code")   or "").strip() or None,
            to_float(r.get("total_amt","")),
            disc,
            to_float(r.get("net_amt","")),
            1 if disc > 0 else 0,
        ))
    conn.executemany("""
        INSERT OR IGNORE INTO stg_orders
        (order_id,order_date,order_year,order_month,order_quarter,
         cust_id,store_id,channel,order_status,promo_code,
         total_amt,discount_amt,net_amt,is_discounted)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, stg_orders)
    logger.info("  stg_orders: %d rows", len(stg_orders))

    # ---- STG_ORDER_ITEMS ----
    cur = conn.execute("SELECT * FROM raw_order_items")
    cols = [d[0] for d in cur.description]
    rows = [dict(zip(cols, r)) for r in cur.fetchall()]
    stg_items = []
    for r in rows:
        iid = (r.get("order_item_id") or "").strip()
        if not iid: continue
        price = to_float(r.get("unit_price","")) or 0
        qty   = to_int(r.get("qty",""))          or 0
        dpct  = to_float(r.get("discount_pct","")) or 0
        stg_items.append((
            iid,
            (r.get("order_id") or "").strip(),
            (r.get("prd_id")   or "").strip(),
            qty, price, dpct,
            to_float(r.get("line_total","")),
            round(price * qty * dpct / 100, 2),
        ))
    conn.executemany("""
        INSERT OR IGNORE INTO stg_order_items
        (order_item_id,order_id,prd_id,qty,unit_price,discount_pct,line_total,discount_amt)
        VALUES (?,?,?,?,?,?,?,?)
    """, stg_items)
    logger.info("  stg_order_items: %d rows", len(stg_items))

    # ---- STG_INVENTORY ----
    cur = conn.execute("SELECT * FROM raw_inventory")
    cols = [d[0] for d in cur.description]
    rows = [dict(zip(cols, r)) for r in cur.fetchall()]
    stg_inv = []
    for r in rows:
        iid = (r.get("inventory_id") or "").strip()
        if not iid: continue
        on_hand    = to_int(r.get("qty_on_hand","")) or 0
        reserved   = to_int(r.get("qty_reserved","")) or 0
        reorder_pt = to_int(r.get("reorder_point","")) or 0
        stg_inv.append((
            iid,
            (r.get("prd_id")   or "").strip(),
            (r.get("store_id") or "").strip(),
            to_date(r.get("snapshot_date","")),
            on_hand, reserved, on_hand - reserved, reorder_pt,
            (r.get("inv_status") or "").strip().upper(),
            1 if on_hand <= reorder_pt else 0,
        ))
    conn.executemany("""
        INSERT OR IGNORE INTO stg_inventory
        (inventory_id,prd_id,store_id,snapshot_date,qty_on_hand,qty_reserved,
         qty_available,reorder_point,inv_status,needs_reorder)
        VALUES (?,?,?,?,?,?,?,?,?,?)
    """, stg_inv)
    logger.info("  stg_inventory: %d rows", len(stg_inv))

    # ---- STG_PROMOTIONS ----
    cur = conn.execute("SELECT * FROM raw_promotions")
    cols = [d[0] for d in cur.description]
    rows = [dict(zip(cols, r)) for r in cur.fetchall()]
    stg_promos = []
    for r in rows:
        pid = (r.get("promo_id") or "").strip()
        if not pid: continue
        stg_promos.append((
            pid,
            (r.get("promo_code") or "").strip().upper(),
            (r.get("promo_name") or "").strip(),
            (r.get("promo_type") or "").strip().upper(),
            to_float(r.get("discount_value","")),
            to_date(r.get("start_date","")),
            to_date(r.get("end_date","")),
            to_float(r.get("min_order_amt","")),
            yn_bool(r.get("is_active","N")),
        ))
    conn.executemany("""
        INSERT OR IGNORE INTO stg_promotions
        (promo_id,promo_code,promo_name,promo_type,discount_value,
         start_date,end_date,min_order_amt,is_active)
        VALUES (?,?,?,?,?,?,?,?,?)
    """, stg_promos)
    logger.info("  stg_promotions: %d rows", len(stg_promos))

    conn.commit()


def run_warehouse_load(conn: sqlite3.Connection):
    """Load dimensions and facts from staging tables."""
    from datetime import date

    today = str(date.today())

    # ---- dim_customer (insert unknown row first) ----
    conn.execute("""
        INSERT OR IGNORE INTO dim_customer
        (cust_sk,cust_id,full_name,eff_start_dt,is_current)
        VALUES (-1,'UNKNOWN','Unknown Customer','1900-01-01',1)
    """)

    conn.execute(f"""
        INSERT OR IGNORE INTO dim_customer
        (cust_id,first_name,last_name,full_name,email,phone,date_of_birth,
         age_band,gender,city,state,country,loyalty_tier,
         signup_date,is_active,eff_start_dt,is_current)
        SELECT cust_id,first_name,last_name,full_name,email,phone,date_of_birth,
               age_band,gender,city,state,country,loyalty_tier,
               signup_date,is_active,'{today}',1
        FROM stg_customers WHERE is_current=1
    """)
    logger.info("  dim_customer loaded")

    # ---- dim_product ----
    conn.execute("""
        INSERT OR IGNORE INTO dim_product
        (prd_sk,prd_id,prd_name,eff_start_dt,is_current)
        VALUES (-1,'UNKNOWN','Unknown Product','1900-01-01',1)
    """)
    conn.execute(f"""
        INSERT OR IGNORE INTO dim_product
        (prd_id,prd_name,prd_category,prd_subcategory,brand,sku,
         unit_price,unit_cost,gross_margin_pct,
         price_tier,is_active,launch_date,discontinue_dt,eff_start_dt,is_current)
        SELECT prd_id,prd_name,prd_category,prd_subcategory,brand,sku,
               unit_price,unit_cost,gross_margin_pct,
               CASE WHEN unit_price < 50 THEN 'BUDGET'
                    WHEN unit_price < 120 THEN 'MID'
                    ELSE 'PREMIUM' END,
               is_active,launch_date,discontinue_dt,'{today}',1
        FROM stg_products
    """)
    logger.info("  dim_product loaded")

    # ---- dim_store ----
    conn.execute("INSERT OR IGNORE INTO dim_store (store_sk,store_id,store_name) VALUES (-1,'UNKNOWN','Unknown Store')")
    conn.execute("""
        INSERT OR IGNORE INTO dim_store
        (store_id,store_name,store_type,region,city,state,country,open_date,manager_name)
        SELECT store_id,store_name,store_type,region,city,state,country,open_date,manager_name
        FROM raw_stores
    """)
    logger.info("  dim_store loaded")

    # ---- dim_promotion ----
    conn.execute("INSERT OR IGNORE INTO dim_promotion (promo_sk,promo_id,promo_code,promo_name) VALUES (-1,'NONE','NONE','No Promotion')")
    conn.execute("""
        INSERT OR IGNORE INTO dim_promotion
        (promo_id,promo_code,promo_name,promo_type,discount_value,start_date,end_date,min_order_amt,is_active)
        SELECT promo_id,promo_code,promo_name,promo_type,discount_value,start_date,end_date,min_order_amt,is_active
        FROM stg_promotions
    """)
    logger.info("  dim_promotion loaded")

    # ---- fact_sales ----
    conn.execute("""
        INSERT OR IGNORE INTO fact_sales
        (order_date_key,cust_sk,prd_sk,store_sk,promo_sk,
         order_id,order_item_id,channel,order_status,
         qty_ordered,unit_price,unit_cost,gross_margin_pct,
         discount_pct,discount_amt,gross_revenue,net_revenue,cogs,gross_profit)
        SELECT
            CAST(REPLACE(o.order_date,'-','') AS INTEGER) AS order_date_key,
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
            oi.qty * COALESCE(dp.unit_cost,0),
            oi.line_total - oi.qty * COALESCE(dp.unit_cost,0)
        FROM stg_order_items oi
        JOIN stg_orders o       ON oi.order_id   = o.order_id
        LEFT JOIN dim_customer dc  ON o.cust_id   = dc.cust_id  AND dc.is_current=1
        LEFT JOIN dim_product  dp  ON oi.prd_id   = dp.prd_id   AND dp.is_current=1
        LEFT JOIN dim_store    ds  ON o.store_id  = ds.store_id
        LEFT JOIN dim_promotion dpr ON COALESCE(o.promo_code,'NONE') = dpr.promo_code
    """)
    logger.info("  fact_sales loaded")

    # ---- fact_inventory ----
    conn.execute("""
        INSERT OR IGNORE INTO fact_inventory
        (snapshot_date_key,prd_sk,store_sk,inventory_id,
         qty_on_hand,qty_reserved,qty_available,reorder_point,
         inv_status,needs_reorder,stock_value)
        SELECT
            CAST(REPLACE(si.snapshot_date,'-','') AS INTEGER),
            COALESCE(dp.prd_sk,  -1),
            COALESCE(ds.store_sk,-1),
            si.inventory_id,
            si.qty_on_hand, si.qty_reserved, si.qty_available, si.reorder_point,
            si.inv_status, si.needs_reorder,
            si.qty_on_hand * COALESCE(dp.unit_cost,0)
        FROM stg_inventory si
        LEFT JOIN dim_product dp  ON si.prd_id   = dp.prd_id   AND dp.is_current=1
        LEFT JOIN dim_store   ds  ON si.store_id = ds.store_id
    """)
    logger.info("  fact_inventory loaded")

    conn.commit()


def build_local_db(db_path: Path):
    logger.info("Creating local SQLite DB: %s", db_path)
    conn = sqlite3.connect(db_path)

    logger.info("Creating RAW schema …")
    conn.executescript(RAW_DDL)
    logger.info("Creating STAGING schema …")
    conn.executescript(STAGING_DDL)
    logger.info("Creating WAREHOUSE schema …")
    conn.executescript(WAREHOUSE_DDL)
    conn.commit()

    logger.info("\nLoading sample CSV files into RAW tables …")
    for csv_file, table, delimiter in CSV_MAP:
        csv_path = SAMPLE_DIR / csv_file
        if csv_path.exists():
            load_csv(conn, csv_path, table, delimiter)
        else:
            logger.warning("  Not found: %s", csv_path)

    logger.info("\nRunning RAW → STAGING transforms …")
    run_staging_transforms(conn)

    logger.info("\nRunning STAGING → WAREHOUSE load …")
    run_warehouse_load(conn)

    # ---- Summary ----
    logger.info("\n── Row counts ──")
    tables = [
        "raw_customers","raw_products","raw_stores","raw_orders","raw_order_items",
        "raw_inventory","raw_promotions",
        "stg_customers","stg_products","stg_orders","stg_order_items",
        "stg_inventory","stg_promotions",
        "dim_customer","dim_product","dim_store","dim_promotion",
        "fact_sales","fact_inventory",
    ]
    for t in tables:
        cnt = conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
        logger.info("  %-25s  %d", t, cnt)

    conn.close()
    logger.info("\nLocal database ready: %s", db_path)


def run_query(db_path: Path, sql: str):
    conn = sqlite3.connect(db_path)
    cur  = conn.execute(sql)
    cols = [d[0] for d in cur.description]
    rows = cur.fetchall()
    conn.close()

    print("  " + " | ".join(f"{c:<20}" for c in cols))
    print("  " + "-" * (23 * len(cols)))
    for row in rows:
        print("  " + " | ".join(f"{str(v):<20}" for v in row))
    print(f"\n  {len(rows)} row(s)")


def parse_args():
    p = argparse.ArgumentParser(description="Build local SQLite dev database")
    p.add_argument("--db", default=str(DEFAULT_DB), help="SQLite database file path")
    p.add_argument("--query", help="Run a SQL query against the local DB")
    return p.parse_args()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-7s  %(message)s",
        datefmt="%H:%M:%S",
    )
    args = parse_args()
    db_path = Path(args.db)

    if args.query:
        if not db_path.exists():
            logger.error("DB not found: %s  (run without --query first)", db_path)
            sys.exit(1)
        run_query(db_path, args.query)
    else:
        build_local_db(db_path)
