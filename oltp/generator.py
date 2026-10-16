"""
generator.py
============
OLTP data generator for SportZone retail.
Generates realistic, referentially consistent transactional data into Neon Postgres.

Architecture
------------
  reference data  →  customers  →  orders + items  →  payments  →  events
  (categories, brands, suppliers, stores, products, promotions)

Usage
-----
    # Choose PostgreSQL, CSV files, or both; enter a date range when prompted
    python oltp/generator.py

    # Generate an all-table CSV dataset without connecting to PostgreSQL
    python oltp/generator.py --destination csv --start-date 2023-01-01 --end-date 2023-12-31

    # Seed reference data only
    python oltp/generator.py --seed-only

    # Generate 500 customers + 2000 orders (one-shot)
    python oltp/generator.py --customers 500 --orders 2000

    # Generate historical orders and save their headers to CSV
    python oltp/generator.py --orders 2000 --start-date 2023-01-01 --end-date 2023-12-31 --csv-output output/historical_orders.csv

    # Continuous mode: generate new orders every 10 seconds
    python oltp/generator.py --continuous --interval 10 --batch 20

    # Reset and regenerate everything
    python oltp/generator.py --reset --customers 1000 --orders 5000
"""

import argparse
import csv
import json
import logging
import random
import re
import sys
import time
import uuid
from datetime import date, datetime, timedelta
from pathlib import Path

import psycopg
from faker import Faker

# Ensure project root is on sys.path before importing db
PROJECT_DIR = Path(__file__).parent.parent
DEPLOY_DIR  = PROJECT_DIR / "deploy"
sys.path.insert(0, str(PROJECT_DIR))

from db import get_neon_conn

logger = logging.getLogger(__name__)
fake   = Faker("en_US")
Faker.seed(42)
random.seed(42)


# ---------------------------------------------------------------------------
# Connection
# ---------------------------------------------------------------------------

def get_conn():
    return get_neon_conn()


# ---------------------------------------------------------------------------
# Reference / seed data
# ---------------------------------------------------------------------------

CATEGORIES = [
    (1, "FOOTWEAR",     None),
    (2, "RUNNING",      1),
    (3, "TRAINING",     1),
    (4, "BASKETBALL",   1),
    (5, "APPAREL",      None),
    (6, "TOPS",         5),
    (7, "BOTTOMS",      5),
    (8, "OUTERWEAR",    5),
    (9, "EQUIPMENT",    None),
    (10,"TENNIS",       9),
    (11,"FITNESS",      9),
    (12,"ACCESSORIES",  None),
    (13,"HYDRATION",    12),
    (14,"PROTECTION",   12),
]

BRANDS = [
    (1, "SportZone",  "CA"),
    (2, "ProStep",    "US"),
    (3, "FlexFit",    "US"),
    (4, "ZenFit",     "CA"),
    (5, "ProRacket",  "DE"),
    (6, "SwiftRun",   "GB"),
    (7, "IronClad",   "US"),
]

SUPPLIERS = [
    (1, "Global Sports Supply",   "supply1@gss.com",   "+1-800-555-0001", "US"),
    (2, "ProStep Manufacturing",  "orders@prostep.com","+1-800-555-0002", "US"),
    (3, "FlexFit Textiles",       "b2b@flexfit.com",   "+1-416-555-0003", "CA"),
    (4, "OuterWear Co",           "ops@outerwear.ca",  "+1-604-555-0004", "CA"),
    (5, "Racket World",           "sales@rworld.de",   "+49-30-555-0005", "DE"),
    (6, "ZenFit Wellness",        "supply@zenfit.com", "+1-800-555-0006", "US"),
    (7, "HydroGoods Ltd",         "orders@hydro.com",  "+44-20-555-0007", "GB"),
]

PRODUCTS = [
    # (sku, name, cat_id, brand_id, supp_id, cost, price, weight_kg)
    ("SZ-FW-RN-001", "AirMax Pro Running Shoe",      2, 1, 1, 55.00,  129.99, 0.320),
    ("SZ-FW-RN-002", "SwiftRun Elite Shoe",           2, 6, 1, 60.00,  149.99, 0.295),
    ("SZ-FW-TR-001", "UltraFlex Training Shoe",       3, 1, 1, 42.00,   99.99, 0.340),
    ("SZ-FW-BK-001", "Classic Basketball Hi-Top",     4, 2, 2, 65.00,  149.99, 0.450),
    ("SZ-FW-BK-002", "Pro Jump Basketball Shoe",      4, 2, 2, 70.00,  169.99, 0.430),
    ("SZ-AP-TO-001", "Performance Running Shirt",     6, 1, 3, 14.00,   39.99, 0.180),
    ("SZ-AP-TO-002", "Compression Training Top",      6, 3, 3, 12.00,   34.99, 0.160),
    ("SZ-AP-BO-001", "Compression Leggings",          7, 3, 3, 20.00,   54.99, 0.220),
    ("SZ-AP-BO-002", "Training Shorts",               7, 1, 3, 10.00,   29.99, 0.140),
    ("SZ-AP-OW-001", "Waterproof Running Jacket",     8, 1, 4, 78.00,  189.99, 0.580),
    ("SZ-EQ-TN-001", "Pro Tennis Racket 300g",       10, 5, 5, 95.00,  219.99, 0.310),
    ("SZ-EQ-TN-002", "Junior Tennis Racket",         10, 5, 5, 45.00,   99.99, 0.260),
    ("SZ-EQ-FT-001", "Yoga Mat Premium 6mm",         11, 4, 6, 18.00,   49.99, 1.200),
    ("SZ-EQ-FT-002", "Resistance Band Set",          11, 3, 6, 12.00,   34.99, 0.400),
    ("SZ-EQ-BK-001", "Basketball Official Size 7",    4, 2, 2, 15.00,   39.99, 0.620),
    ("SZ-AC-PR-001", "Pro Gym Gloves",               14, 3, 6,  8.00,   24.99, 0.120),
    ("SZ-AC-HY-001", "Water Bottle 1L BPA-Free",     13, 1, 7,  5.00,   19.99, 0.280),
    ("SZ-AC-HY-002", "Insulated Sports Flask 750ml", 13, 4, 7,  9.00,   29.99, 0.350),
    ("SZ-FW-RN-003", "IronClad Trail Running Shoe",   2, 7, 1, 68.00,  159.99, 0.390),
    ("SZ-AP-TO-003", "Moisture-Wick Training Tee",    6, 7, 3, 11.00,   32.99, 0.170),
]

STORES = [
    # (code, name, type, region, address, city, state, zip, country, open_date, manager)
    ("TOR-DT",  "SportZone Toronto Downtown",    "PHYSICAL", "EAST",    "100 Queen St W",      "Toronto",    "ON", "M5H2N2", "CA", "2015-03-01", "Alex Turner"),
    ("VAN-PC",  "SportZone Vancouver Pacific",   "PHYSICAL", "WEST",    "1000 Robson St",      "Vancouver",  "BC", "V6E1B5", "CA", "2016-07-15", "Maria Santos"),
    ("NYC-5A",  "SportZone New York 5th Ave",    "PHYSICAL", "NE",      "500 5th Ave",         "New York",   "NY", "10110",  "US", "2014-11-01", "John Lee"),
    ("CHI-MI",  "SportZone Chicago Michigan",    "PHYSICAL", "MIDWEST", "900 N Michigan Ave",  "Chicago",    "IL", "60611",  "US", "2017-04-20", "Susan Kim"),
    ("MIA-BR",  "SportZone Miami Brickell",      "PHYSICAL", "SOUTH",   "1200 Brickell Ave",   "Miami",      "FL", "33131",  "US", "2018-09-10", "Carlos Reyes"),
    ("DAL-UP",  "SportZone Dallas Uptown",       "PHYSICAL", "SOUTH",   "3699 McKinney Ave",   "Dallas",     "TX", "75204",  "US", "2019-02-14", "Priya Patel"),
    ("SEA-WA",  "SportZone Seattle Waterfront",  "PHYSICAL", "WEST",    "1201 Western Ave",    "Seattle",    "WA", "98101",  "US", "2020-06-01", "Ryan O'Brien"),
    ("ONL-CA",  "SportZone Online Canada",       "ONLINE",   "NATIONAL","",                    "",           "",   "",       "CA", "2013-01-01", "Digital Team CA"),
    ("ONL-US",  "SportZone Online USA",          "ONLINE",   "NATIONAL","",                    "",           "",   "",       "US", "2013-01-01", "Digital Team US"),
    ("MOB-CA",  "SportZone Mobile Canada",       "ONLINE",   "NATIONAL","",                    "",           "",   "",       "CA", "2021-03-01", "Mobile Team CA"),
    ("MOB-US",  "SportZone Mobile USA",          "ONLINE",   "NATIONAL","",                    "",           "",   "",       "US", "2021-03-01", "Mobile Team US"),
]

PROMOTIONS = [
    # (code, name, type, value, min_order, max_uses, start, end)
    ("SAVE10",    "10% Off Sitewide",           "PERCENTAGE", 10.0,  50.0,  None,  "2024-01-01", "2024-12-31"),
    ("GOLD20",    "Gold/Platinum Member 20%",   "PERCENTAGE", 20.0, 100.0,  None,  "2024-01-01", "2026-12-31"),
    ("WINTER30",  "Winter Clearance 30%",       "PERCENTAGE", 30.0,  75.0,  5000,  "2024-01-01", "2024-02-28"),
    ("BOGO50",    "Buy One Get One 50%",        "PERCENTAGE", 50.0,   0.0,  2000,  "2024-01-10", "2024-01-31"),
    ("FREESHIP",  "Free Shipping",              "FREESHIP",    0.0,   0.0,  None,  "2024-01-01", "2026-12-31"),
    ("SUMMER15",  "Summer Sale 15%",            "PERCENTAGE", 15.0,  60.0,  None,  "2024-06-01", "2024-08-31"),
    ("NEWCUST25", "New Customer 25% Off",       "PERCENTAGE", 25.0,   0.0, 10000,  "2024-01-01", "2026-12-31"),
    ("FLASH40",   "Flash Sale 40%",             "PERCENTAGE", 40.0,   0.0,  1000,  "2024-11-29", "2024-11-30"),
    ("LOYALTY15", "Loyalty Reward 15%",         "PERCENTAGE", 15.0,  50.0,  None,  "2024-01-01", "2026-12-31"),
    ("FIXED15",   "\\$15 Off Over \\$100",      "FIXED",      15.0, 100.0,  3000,  "2024-03-01", "2024-05-31"),
]

CA_CITIES = [("Toronto","ON","M5V","CA"),("Vancouver","BC","V6B","CA"),("Montreal","QC","H2Y","CA"),
             ("Calgary","AB","T2P","CA"),("Ottawa","ON","K1A","CA"),("Edmonton","AB","T5J","CA")]
US_CITIES = [("New York","NY","10001","US"),("Los Angeles","CA","90001","US"),("Chicago","IL","60601","US"),
             ("Houston","TX","77001","US"),("Miami","FL","33101","US"),("Seattle","WA","98101","US"),
             ("Dallas","TX","75201","US"),("Denver","CO","80201","US"),("Boston","MA","02101","US")]
ALL_CITIES = CA_CITIES + US_CITIES

CHANNELS   = ["ONLINE","IN-STORE","ONLINE","MOBILE","IN-STORE","ONLINE"]   # weighted
PAY_METHODS = ["CREDIT_CARD","CREDIT_CARD","DEBIT_CARD","PAYPAL","APPLE_PAY","GOOGLE_PAY","CREDIT_CARD"]


# ---------------------------------------------------------------------------
# Seed reference data
# ---------------------------------------------------------------------------

def seed_reference(cur):
    logger.info("Seeding reference data …")

    # Only run DDL if tables don't exist yet
    cur.execute("""
        SELECT COUNT(*) FROM information_schema.tables
        WHERE table_schema = 'oltp' AND table_name = 'product'
    """)
    if cur.fetchone()[0] == 0:
        logger.info("  Creating OLTP schema …")
        schema_sql = (PROJECT_DIR / "oltp" / "schema.sql").read_text(encoding="utf-8")
        for stmt in [s.strip() for s in schema_sql.split(";") if s.strip()]:
            try:
                cur.execute(stmt)
            except Exception as e:
                if "already exists" not in str(e).lower():
                    raise
        cur.connection.commit()

    # Categories
    cur.executemany("""
        INSERT INTO oltp.category (category_id, name, parent_id)
        VALUES (%s,%s,%s) ON CONFLICT DO NOTHING
    """, CATEGORIES)
    cur.execute("SELECT setval('oltp.category_category_id_seq', (SELECT MAX(category_id) FROM oltp.category))")

    # Brands
    cur.executemany("""
        INSERT INTO oltp.brand (brand_id, name, country_origin)
        VALUES (%s,%s,%s) ON CONFLICT DO NOTHING
    """, BRANDS)
    cur.execute("SELECT setval('oltp.brand_brand_id_seq', (SELECT MAX(brand_id) FROM oltp.brand))")

    # Suppliers
    cur.executemany("""
        INSERT INTO oltp.supplier (supplier_id, name, contact_email, phone, country)
        VALUES (%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING
    """, SUPPLIERS)
    cur.execute("SELECT setval('oltp.supplier_supplier_id_seq', (SELECT MAX(supplier_id) FROM oltp.supplier))")

    # Products
    cur.executemany("""
        INSERT INTO oltp.product (sku, name, category_id, brand_id, supplier_id,
                                  unit_cost, unit_price, weight_kg)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (sku) DO NOTHING
    """, PRODUCTS)

    # Stores
    cur.executemany("""
        INSERT INTO oltp.store (code, name, store_type, region, address,
                                city, state, zip_code, country, open_date, manager_name)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (code) DO NOTHING
    """, STORES)

    # Promotions
    cur.executemany("""
        INSERT INTO oltp.promotion (code, name, promo_type, discount_value,
                                    min_order_amt, max_uses, start_date, end_date)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (code) DO NOTHING
    """, PROMOTIONS)

    # ETL watermarks
    tables = ["customer","order_header","order_item","payment","inventory","customer_event"]
    for t in tables:
        cur.execute("""
            INSERT INTO oltp.etl_watermark (table_name) VALUES (%s)
            ON CONFLICT DO NOTHING
        """, (t,))

    logger.info("  Reference data seeded.")


# ---------------------------------------------------------------------------
# Customer generator
# ---------------------------------------------------------------------------

def generate_customers(cur, n: int) -> list[int]:
    logger.info("Generating %d customers …", n)
    ids = []

    for _ in range(n):
        gender  = random.choice(["M","F","M","F","U"])
        if gender == "M":
            fn = fake.first_name_male()
        elif gender == "F":
            fn = fake.first_name_female()
        else:
            fn = fake.first_name()

        city, state, zip_pfx, country = random.choice(ALL_CITIES)
        zip_code  = zip_pfx + str(random.randint(100,999))
        dob       = fake.date_of_birth(minimum_age=18, maximum_age=70)
        signup_dt = fake.date_between(start_date=date(2019,1,1), end_date=date.today())

        # Assign loyalty tier based on age on platform
        months_active = (date.today() - signup_dt).days // 30
        if months_active < 3:
            tier = "BRONZE"
        elif months_active < 12:
            tier = random.choice(["BRONZE","SILVER"])
        elif months_active < 24:
            tier = random.choice(["SILVER","GOLD"])
        else:
            tier = random.choice(["GOLD","GOLD","PLATINUM","SILVER"])

        try:
            cur.execute("""
                INSERT INTO oltp.customer
                    (first_name, last_name, email, phone, date_of_birth, gender,
                     address_line1, city, state, zip_code, country,
                     loyalty_tier, signup_date)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                RETURNING customer_id
            """, (
                fn, fake.last_name(),
                f"{fake.user_name()}-{uuid.uuid4().hex[:12]}@example.com",
                fake.numerify("###-###-####"),
                dob, gender,
                fake.street_address(),
                city, state, zip_code, country,
                tier, signup_dt
            ))
            ids.append(cur.fetchone()[0])
        except psycopg.errors.UniqueViolation:
            cur.connection.rollback()
            # Skip duplicate email

    logger.info("  Inserted %d customers.", len(ids))
    return ids


# ---------------------------------------------------------------------------
# Order generator
# ---------------------------------------------------------------------------

def generate_orders(cur, customer_ids: list[int], n_orders: int,
                    start_date: date = None, end_date: date = None,
                    csv_output: Path | None = None):
    if start_date is None:
        start_date = date(2024, 1, 1)
    if end_date is None:
        end_date = date.today()

    start_dt = datetime.combine(start_date, datetime.min.time())
    end_dt = datetime.combine(end_date + timedelta(days=1), datetime.min.time()) - timedelta(microseconds=1)

    # Fetch product info
    cur.execute("SELECT product_id, unit_price, unit_cost, category_id FROM oltp.product WHERE is_active=TRUE")
    products = [(r[0], float(r[1]), float(r[2]), r[3]) for r in cur.fetchall()]

    # Fetch stores
    cur.execute("SELECT store_id, store_type FROM oltp.store WHERE is_active=TRUE")
    stores = cur.fetchall()
    physical_stores = [s for s in stores if s[1] == "PHYSICAL"]
    online_stores   = [s for s in stores if s[1] == "ONLINE"]

    # Fetch active promotions
    cur.execute("SELECT promotion_id, code, promo_type, discount_value, min_order_amt FROM oltp.promotion WHERE is_active=TRUE")
    promotions = [(r[0], r[1], r[2], float(r[3]), float(r[4])) for r in cur.fetchall()]

    logger.info("Generating %d orders (date range %s → %s) …", n_orders, start_date, end_date)

    # Build all data in memory first, then bulk insert
    order_rows = []
    item_rows  = []
    pay_rows   = []

    for i in range(n_orders):
        cust_id  = random.choice(customer_ids)
        channel  = random.choice(CHANNELS)
        store    = random.choice(online_stores if channel in ("ONLINE","MOBILE") else physical_stores)
        store_id = store[0]
        order_dt = fake.date_time_between(start_date=start_dt, end_date=end_dt)
        order_ref = f"ORD-{order_dt.strftime('%Y%m')}-{uuid.uuid4().hex[:12]}"

        n_items    = random.choices([1,2,3,4], weights=[50,30,15,5])[0]
        order_prods = random.sample(products, min(n_items, len(products)))

        subtotal = sum(p[1] * random.randint(1,2) for p in order_prods)

        promo_id = None
        discount_amt = 0.0
        eligible_promos = [p for p in promotions if p[4] <= subtotal]
        if eligible_promos and random.random() < 0.35:
            promo = random.choice(eligible_promos)
            promo_id = promo[0]
            ptype, pval = promo[2], promo[3]
            if ptype == "PERCENTAGE":
                discount_amt = round(subtotal * pval / 100, 2)
            elif ptype == "FIXED":
                discount_amt = min(pval, subtotal)

        tax_rate   = 0.13 if random.random() < 0.5 else 0.08
        shipping   = 0.0 if channel == "IN-STORE" else (0.0 if subtotal > 75 else 9.99)
        net_amt    = round(subtotal - discount_amt, 2)
        tax_amt    = round(net_amt * tax_rate, 2)
        total_amt  = round(net_amt + tax_amt + shipping, 2)

        days_ago = (date.today() - order_dt.date()).days
        if days_ago > 30:
            status = random.choices(["COMPLETED","CANCELLED","RETURNED","COMPLETED"], weights=[75,10,8,7])[0]
        elif days_ago > 7:
            status = random.choices(["COMPLETED","SHIPPED","CONFIRMED","CANCELLED"], weights=[60,20,15,5])[0]
        else:
            status = random.choices(["PENDING","CONFIRMED","SHIPPED"], weights=[30,40,30])[0]

        shipped_dt   = order_dt + timedelta(days=random.randint(1,3)) if status in ("SHIPPED","COMPLETED","RETURNED") else None
        delivered_dt = (shipped_dt + timedelta(days=random.randint(1,5))) if shipped_dt and status in ("COMPLETED","RETURNED") else None

        order_rows.append((
            order_ref, cust_id, store_id, promo_id, channel,
            status, order_dt, shipped_dt, delivered_dt,
            round(subtotal,2), discount_amt, tax_amt, shipping, total_amt
        ))
        # Store items and payment for this order index
        item_rows.append((i, order_prods, discount_amt, subtotal))
        pay_rows.append((i, status, total_amt))

    # Bulk-insert orders
    logger.info("  Inserting %d order headers …", len(order_rows))
    cur.executemany("""
        INSERT INTO oltp.order_header
            (order_ref, customer_id, store_id, promotion_id, channel,
             order_status, order_date, shipped_date, delivered_date,
             subtotal_amt, discount_amt, tax_amt, shipping_amt, total_amt)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (order_ref) DO NOTHING
    """, order_rows)
    cur.connection.commit()

    # Fetch back generated order_ids by order_ref
    order_refs = [r[0] for r in order_rows]
    cur.execute("""
        SELECT order_ref, order_id FROM oltp.order_header
        WHERE order_ref = ANY(%s)
    """, (order_refs,))
    ref_to_id = {r[0]: r[1] for r in cur.fetchall()}

    # Build item + payment rows with real order_ids
    all_item_rows = []
    all_pay_rows  = []
    for i, (idx, prods, disc_amt, subtotal) in enumerate(item_rows):
        order_ref = order_rows[idx][0]
        order_id  = ref_to_id.get(order_ref)
        if not order_id:
            continue
        for prod_id, unit_price, unit_cost, _ in prods:
            qty       = random.randint(1,2)
            disc_pct  = round(disc_amt / subtotal * 100, 2) if subtotal > 0 else 0
            line_sub  = round(unit_price * qty, 2)
            line_disc = round(line_sub * disc_pct / 100, 2)
            line_tot  = round(line_sub - line_disc, 2)
            all_item_rows.append((order_id, prod_id, qty, unit_price, unit_cost,
                                   disc_pct, line_sub, line_disc, line_tot))

    for i, (idx, status, total_amt) in enumerate(pay_rows):
        order_ref = order_rows[idx][0]
        order_id  = ref_to_id.get(order_ref)
        if not order_id:
            continue
        pay_method = random.choice(PAY_METHODS)
        pay_status = ("REFUNDED" if status == "RETURNED"
                      else "CAPTURED" if status not in ("PENDING","CANCELLED")
                      else "PENDING" if status == "PENDING" else "FAILED")
        all_pay_rows.append((order_id, pay_method, pay_status, total_amt,
                              f"TXN-{uuid.uuid4().hex.upper()}"))

    logger.info("  Inserting %d order items …", len(all_item_rows))
    cur.executemany("""
        INSERT INTO oltp.order_item
            (order_id, product_id, qty, unit_price, unit_cost,
             discount_pct, line_subtotal, line_discount, line_total)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """, all_item_rows)

    logger.info("  Inserting %d payments …", len(all_pay_rows))
    cur.executemany("""
        INSERT INTO oltp.payment
            (order_id, payment_method, payment_status, amount, transaction_ref)
        VALUES (%s,%s,%s,%s,%s)
    """, all_pay_rows)

    cur.connection.commit()
    logger.info("  Generated %d orders total.", len(ref_to_id))
    if csv_output is not None:
        write_orders_csv(order_rows, csv_output)


ORDER_CSV_COLUMNS = [
    "order_ref", "customer_id", "store_id", "promotion_id", "channel",
    "order_status", "order_date", "shipped_date", "delivered_date",
    "subtotal_amt", "discount_amt", "tax_amt", "shipping_amt", "total_amt",
]


def write_orders_csv(order_rows: list[tuple], output_path: Path):
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", newline="", encoding="utf-8") as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(ORDER_CSV_COLUMNS)
        writer.writerows(order_rows)
    logger.info("  Wrote %d order headers to %s.", len(order_rows), output_path)


# ---------------------------------------------------------------------------
# Inventory snapshot
# ---------------------------------------------------------------------------

def generate_inventory(cur):
    logger.info("Seeding inventory …")
    # Skip if already seeded
    cur.execute("SELECT COUNT(*) FROM oltp.inventory")
    if cur.fetchone()[0] > 0:
        logger.info("  Inventory already seeded — skipping.")
        return

    cur.execute("SELECT product_id FROM oltp.product WHERE is_active=TRUE")
    products = [r[0] for r in cur.fetchall()]
    cur.execute("SELECT store_id FROM oltp.store WHERE is_active=TRUE")
    stores   = [r[0] for r in cur.fetchall()]

    rows = []
    for pid in products:
        for sid in stores:
            qty = random.randint(0, 200)
            reo = random.randint(10, 30)
            rows.append((pid, sid, qty, reo))

    cur.executemany("""
        INSERT INTO oltp.inventory (product_id, store_id, qty_on_hand, reorder_point)
        VALUES (%s,%s,%s,%s)
        ON CONFLICT (product_id, store_id) DO UPDATE
        SET qty_on_hand = EXCLUDED.qty_on_hand, last_updated = NOW()
    """, rows)
    logger.info("  Inventory seeded (%d products × %d stores)", len(products), len(stores))


# ---------------------------------------------------------------------------
# Customer events
# ---------------------------------------------------------------------------

def generate_events(cur, customer_ids: list[int], n: int = 2000,
                    start_date: date | None = None,
                    end_date: date | None = None):
    logger.info("Generating %d customer events …", n)
    start_date = start_date or date(2024, 1, 1)
    end_date = end_date or date.today()
    cur.execute("SELECT product_id FROM oltp.product WHERE is_active=TRUE")
    products = [r[0] for r in cur.fetchall()]

    events = []
    for _ in range(n):
        cid      = random.choice(customer_ids)
        etype    = random.choice(["PAGE_VIEW","PAGE_VIEW","ADD_TO_CART","ADD_TO_CART",
                                  "CHECKOUT_START","PURCHASE","REMOVE_FROM_CART","REVIEW"])
        prod_id  = random.choice(products) if etype != "PAGE_VIEW" else None
        ts       = fake.date_time_between(
            start_date=datetime.combine(start_date, datetime.min.time()),
            end_date=datetime.combine(end_date + timedelta(days=1), datetime.min.time())
            - timedelta(microseconds=1),
        )
        evt_data = {}
        if etype == "PAGE_VIEW":
            evt_data = {"page": random.choice(["/products","/category","/sale","/new-arrivals"])}
        elif etype in ("ADD_TO_CART","REMOVE_FROM_CART"):
            evt_data = {"qty": random.randint(1,3)}
        elif etype == "REVIEW":
            evt_data = {"rating": random.randint(3,5), "verified": True}

        events.append((cid, f"SESS-{uuid.uuid4().hex}",
                        etype, prod_id, json.dumps(evt_data), ts))

    cur.executemany("""
        INSERT INTO oltp.customer_event
            (customer_id, session_id, event_type, product_id, event_data, occurred_at)
        VALUES (%s,%s,%s,%s,%s,%s)
    """, events)
    logger.info("  Inserted %d events.", len(events))


# ---------------------------------------------------------------------------
# Stats
# ---------------------------------------------------------------------------

def print_stats(cur):
    tables = [
        "oltp.category","oltp.brand","oltp.supplier","oltp.product",
        "oltp.store","oltp.promotion","oltp.customer","oltp.inventory",
        "oltp.order_header","oltp.order_item","oltp.payment","oltp.customer_event"
    ]
    print(f"\n{'Table':<35} {'Rows':>8}")
    print("-" * 45)
    for t in tables:
        cur.execute(f"SELECT COUNT(*) FROM {t}")
        print(f"  {t:<33} {cur.fetchone()[0]:>8,}")
    print()


# ---------------------------------------------------------------------------
# CSV-only generation and export
# ---------------------------------------------------------------------------

CSV_TABLE_KEYS = {
    "category": "category_id",
    "brand": "brand_id",
    "supplier": "supplier_id",
    "product": "product_id",
    "store": "store_id",
    "promotion": "promotion_id",
    "customer": "customer_id",
    "inventory": "inventory_id",
    "order_header": "order_id",
    "order_item": "order_item_id",
    "payment": "payment_id",
    "customer_event": "event_id",
}

CSV_TABLE_COLUMNS = {
    "category": ["category_id", "name", "parent_id"],
    "brand": ["brand_id", "name", "country_origin", "is_active"],
    "supplier": ["supplier_id", "name", "contact_email", "phone", "country",
                 "is_active", "created_at"],
    "product": ["product_id", "sku", "name", "category_id", "brand_id",
                "supplier_id", "unit_cost", "unit_price", "weight_kg",
                "is_active", "launch_date", "discontinue_dt", "created_at",
                "updated_at"],
    "store": ["store_id", "code", "name", "store_type", "region", "address",
              "city", "state", "zip_code", "country", "open_date",
              "manager_name", "is_active", "created_at"],
    "promotion": ["promotion_id", "code", "name", "promo_type",
                  "discount_value", "min_order_amt", "max_uses", "uses_count",
                  "start_date", "end_date", "is_active", "created_at"],
    "customer": ["customer_id", "external_id", "first_name", "last_name",
                 "email", "phone", "date_of_birth", "gender", "address_line1",
                 "address_line2", "city", "state", "zip_code", "country",
                 "loyalty_tier", "loyalty_points", "signup_date", "is_active",
                 "created_at", "updated_at"],
    "inventory": ["inventory_id", "product_id", "store_id", "qty_on_hand",
                  "qty_reserved", "reorder_point", "last_updated"],
    "order_header": ["order_id", "order_ref", "customer_id", "store_id",
                     "promotion_id", "channel", "order_status", "order_date",
                     "shipped_date", "delivered_date", "subtotal_amt",
                     "discount_amt", "tax_amt", "shipping_amt", "total_amt",
                     "currency", "notes", "created_at", "updated_at"],
    "order_item": ["order_item_id", "order_id", "product_id", "qty",
                   "unit_price", "unit_cost", "discount_pct", "line_subtotal",
                   "line_discount", "line_total", "created_at"],
    "payment": ["payment_id", "order_id", "payment_method", "payment_status",
                "amount", "currency", "transaction_ref", "processed_at"],
    "customer_event": ["event_id", "customer_id", "session_id", "event_type",
                       "product_id", "event_data", "occurred_at"],
}


def generate_csv_dataset(n_customers: int, n_orders: int, n_events: int,
                         start_date: date, end_date: date) -> dict[str, list[dict]]:
    now = datetime.now().astimezone().isoformat()
    categories = [
        {"category_id": row[0], "name": row[1], "parent_id": row[2]}
        for row in CATEGORIES
    ]
    brands = [
        {"brand_id": row[0], "name": row[1], "country_origin": row[2],
         "is_active": True}
        for row in BRANDS
    ]
    suppliers = [
        {"supplier_id": i, "name": row[0], "contact_email": row[1],
         "phone": row[2], "country": row[3], "is_active": True,
         "created_at": now}
        for i, row in enumerate(SUPPLIERS, 1)
    ]
    products = [
        {"product_id": i, "sku": row[0], "name": row[1],
         "category_id": row[2], "brand_id": row[3], "supplier_id": row[4],
         "unit_cost": row[5], "unit_price": row[6], "weight_kg": row[7],
         "is_active": True, "launch_date": start_date.isoformat(),
         "discontinue_dt": None, "created_at": now, "updated_at": now}
        for i, row in enumerate(PRODUCTS, 1)
    ]
    stores = [
        {"store_id": i, "code": row[0], "name": row[1], "store_type": row[2],
         "region": row[3], "address": row[4], "city": row[5], "state": row[6],
         "zip_code": row[7], "country": row[8], "open_date": row[9],
         "manager_name": row[10], "is_active": True, "created_at": now}
        for i, row in enumerate(STORES, 1)
    ]
    promotions = [
        {"promotion_id": i, "code": row[0], "name": row[1],
         "promo_type": row[2], "discount_value": row[3],
         "min_order_amt": row[4], "max_uses": row[5], "uses_count": 0,
         "start_date": row[6], "end_date": row[7], "is_active": True,
         "created_at": now}
        for i, row in enumerate(PROMOTIONS, 1)
    ]

    customers = []
    for i in range(1, n_customers + 1):
        gender = random.choice(["M", "F", "M", "F", "U"])
        first_name = (fake.first_name_male() if gender == "M" else
                      fake.first_name_female() if gender == "F" else fake.first_name())
        city, state, zip_prefix, country = random.choice(ALL_CITIES)
        signup_date = fake.date_between(min(date(2019, 1, 1), end_date), end_date)
        active_months = (end_date - signup_date).days // 30
        if active_months < 3:
            tier = "BRONZE"
        elif active_months < 12:
            tier = random.choice(["BRONZE", "SILVER"])
        elif active_months < 24:
            tier = random.choice(["SILVER", "GOLD"])
        else:
            tier = random.choice(["GOLD", "GOLD", "PLATINUM", "SILVER"])
        customers.append({
            "customer_id": i, "external_id": f"CRM-{uuid.uuid4().hex[:20]}",
            "first_name": first_name, "last_name": fake.last_name(),
            "email": f"{fake.user_name()}-{uuid.uuid4().hex[:12]}@example.com",
            "phone": fake.numerify("###-###-####"),
            "date_of_birth": fake.date_of_birth(minimum_age=18, maximum_age=70).isoformat(),
            "gender": gender, "address_line1": fake.street_address(),
            "address_line2": None, "city": city, "state": state,
            "zip_code": f"{zip_prefix}{random.randint(100, 999)}",
            "country": country, "loyalty_tier": tier, "loyalty_points": 0,
            "signup_date": signup_date.isoformat(), "is_active": True,
            "created_at": now, "updated_at": now,
        })

    physical_stores = [row for row in stores if row["store_type"] == "PHYSICAL"]
    online_stores = [row for row in stores if row["store_type"] == "ONLINE"]
    order_headers = []
    order_items = []
    payments = []
    for order_id in range(1, n_orders + 1):
        customer = random.choice(customers)
        channel = random.choice(CHANNELS)
        store = random.choice(online_stores if channel in ("ONLINE", "MOBILE")
                              else physical_stores)
        order_dt = fake.date_time_between(
            start_date=datetime.combine(start_date, datetime.min.time()),
            end_date=datetime.combine(end_date + timedelta(days=1), datetime.min.time())
            - timedelta(microseconds=1),
        )
        chosen_products = random.sample(products, random.randint(1, min(4, len(products))))
        quantities = {p["product_id"]: random.randint(1, 2) for p in chosen_products}
        subtotal = round(sum(p["unit_price"] * quantities[p["product_id"]]
                             for p in chosen_products), 2)
        promo = next((p for p in promotions
                      if p["min_order_amt"] <= subtotal
                      and p["start_date"] <= order_dt.date().isoformat()
                      <= p["end_date"]), None)
        promotion_id = None
        discount = 0.0
        if promo and random.random() < 0.35:
            promotion_id = promo["promotion_id"]
            discount = (round(subtotal * promo["discount_value"] / 100, 2)
                        if promo["promo_type"] == "PERCENTAGE"
                        else min(promo["discount_value"], subtotal)
                        if promo["promo_type"] == "FIXED" else 0.0)
        tax = round((subtotal - discount) * random.choice([0.08, 0.13]), 2)
        shipping = (0.0 if channel == "IN-STORE" or subtotal > 75 else 9.99)
        total = round(subtotal - discount + tax + shipping, 2)
        days_old = (date.today() - order_dt.date()).days
        status = (random.choice(["COMPLETED", "CANCELLED", "RETURNED"])
                  if days_old > 30 else
                  random.choice(["COMPLETED", "SHIPPED", "CONFIRMED", "CANCELLED"])
                  if days_old > 7 else
                  random.choice(["PENDING", "CONFIRMED", "SHIPPED"]))
        shipped = (order_dt + timedelta(days=random.randint(1, 3))
                   if status in ("SHIPPED", "COMPLETED", "RETURNED") else None)
        delivered = (shipped + timedelta(days=random.randint(1, 5))
                     if shipped and status in ("COMPLETED", "RETURNED") else None)
        order_headers.append({
            "order_id": order_id,
            "order_ref": f"ORD-{order_dt:%Y%m}-{uuid.uuid4().hex[:12]}",
            "customer_id": customer["customer_id"], "store_id": store["store_id"],
            "promotion_id": promotion_id, "channel": channel,
            "order_status": status, "order_date": order_dt.isoformat(),
            "shipped_date": shipped.isoformat() if shipped else None,
            "delivered_date": delivered.isoformat() if delivered else None,
            "subtotal_amt": subtotal, "discount_amt": discount, "tax_amt": tax,
            "shipping_amt": shipping, "total_amt": total, "currency": "USD",
            "notes": None, "created_at": now, "updated_at": now,
        })
        discount_pct = round(discount / subtotal * 100, 2) if subtotal else 0
        for product in chosen_products:
            qty = quantities[product["product_id"]]
            line_subtotal = round(product["unit_price"] * qty, 2)
            line_discount = round(line_subtotal * discount_pct / 100, 2)
            order_items.append({
                "order_item_id": len(order_items) + 1, "order_id": order_id,
                "product_id": product["product_id"], "qty": qty,
                "unit_price": product["unit_price"], "unit_cost": product["unit_cost"],
                "discount_pct": discount_pct, "line_subtotal": line_subtotal,
                "line_discount": line_discount,
                "line_total": round(line_subtotal - line_discount, 2),
                "created_at": now,
            })
        payment_status = ("REFUNDED" if status == "RETURNED" else
                          "CAPTURED" if status not in ("PENDING", "CANCELLED") else
                          "PENDING" if status == "PENDING" else "FAILED")
        payments.append({
            "payment_id": order_id, "order_id": order_id,
            "payment_method": random.choice(PAY_METHODS),
            "payment_status": payment_status, "amount": total, "currency": "USD",
            "transaction_ref": f"TXN-{uuid.uuid4().hex.upper()}",
            "processed_at": now,
        })

    events = []
    event_types = ["PAGE_VIEW", "ADD_TO_CART", "REMOVE_FROM_CART",
                   "CHECKOUT_START", "PURCHASE", "REVIEW"]
    for event_id in range(1, n_events + 1):
        event_type = random.choice(event_types)
        event_data = (
            {"page": random.choice(["/products", "/category", "/sale", "/new-arrivals"])}
            if event_type == "PAGE_VIEW" else
            {"qty": random.randint(1, 3)}
            if event_type in ("ADD_TO_CART", "REMOVE_FROM_CART") else
            {"rating": random.randint(3, 5), "verified": True}
            if event_type == "REVIEW" else {}
        )
        event_dt = fake.date_time_between(
            start_date=datetime.combine(start_date, datetime.min.time()),
            end_date=datetime.combine(end_date + timedelta(days=1), datetime.min.time())
            - timedelta(microseconds=1),
        )
        events.append({
            "event_id": event_id, "customer_id": random.choice(customers)["customer_id"],
            "session_id": f"SESS-{uuid.uuid4().hex}", "event_type": event_type,
            "product_id": random.choice(products)["product_id"] if event_type != "PAGE_VIEW" else None,
            "event_data": json.dumps(event_data), "occurred_at": event_dt.isoformat(),
        })

    inventory = [
        {"inventory_id": i, "product_id": product["product_id"],
         "store_id": store["store_id"], "qty_on_hand": random.randint(0, 200),
         "qty_reserved": 0, "reorder_point": random.randint(10, 30),
         "last_updated": now}
        for i, (product, store) in enumerate(
            ((p, s) for p in products for s in stores), 1)
    ]
    return {
        "category": categories, "brand": brands, "supplier": suppliers,
        "product": products, "store": stores, "promotion": promotions,
        "customer": customers, "inventory": inventory,
        "order_header": order_headers, "order_item": order_items,
        "payment": payments, "customer_event": events,
    }


def apply_csv_quality_issues(tables: dict[str, list[dict]]) -> int:
    issues = 0
    customers = tables["customer"]
    if customers:
        customers[0]["email"] = "invalid-email"
        issues += 1
    if len(customers) > 1:
        customers[1]["phone"] = ""
        issues += 1
    items = tables["order_item"]
    if items:
        items[0]["qty"] = 0
        issues += 1
    events = tables["customer_event"]
    if events:
        events[0]["event_type"] = "UNKNOWN_EVENT"
        issues += 1
    return issues


def write_oltp_csvs(tables: dict[str, list[dict]], output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    issue_count = apply_csv_quality_issues(tables)
    for table_name, primary_key in CSV_TABLE_KEYS.items():
        rows = tables.get(table_name, [])
        columns = CSV_TABLE_COLUMNS[table_name]
        seen = set()
        for row in rows:
            key = row[primary_key]
            if key in seen:
                raise ValueError(f"Duplicate {primary_key}={key!r} in oltp.{table_name}")
            seen.add(key)
            if set(row) != set(columns):
                raise ValueError(f"Inconsistent columns in oltp.{table_name}")
        path = output_dir / f"{table_name}.csv"
        with path.open("w", newline="", encoding="utf-8") as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames=columns)
            writer.writeheader()
            writer.writerows({
                key: json.dumps(value) if isinstance(value, (dict, list)) else value
                for key, value in row.items()
            } for row in rows)
        logger.info("  Wrote %d unique rows to %s.", len(rows), path)
    logger.warning("Injected %d controlled data-quality issues into CSV copies only.",
                   issue_count)


def export_postgres_csvs(cur, output_dir: Path) -> None:
    tables = {}
    for table_name in CSV_TABLE_KEYS:
        cur.execute(f"SELECT * FROM oltp.{table_name}")
        columns = [column.name for column in cur.description]
        tables[table_name] = [
            dict(zip(columns, row)) for row in cur.fetchall()
        ]
    write_oltp_csvs(tables, output_dir)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def parse_args():
    p = argparse.ArgumentParser(description="SportZone OLTP data generator")
    p.add_argument("--seed-only",   action="store_true", help="Only seed reference data")
    p.add_argument("--customers",   type=int, default=500,  help="Number of customers to generate")
    p.add_argument("--orders",      type=int, default=2000, help="Number of orders to generate")
    p.add_argument("--events",      type=int, default=3000, help="Number of customer events")
    p.add_argument("--destination", choices=("postgres", "csv", "both"),
                   help="Write generated data to PostgreSQL, CSV files, or both")
    p.add_argument("--start-date",  default=None,           help="Order start date YYYY-MM-DD")
    p.add_argument("--end-date",    default=None,           help="Order end date YYYY-MM-DD")
    p.add_argument("--csv-output", type=Path, default=None,
                   help="Legacy option: also write generated order headers to this CSV path")
    p.add_argument("--csv-output-dir", type=Path, default=Path("output/oltp_csv"),
                   help="Directory for one CSV per generated OLTP table")
    p.add_argument("--reset",       action="store_true",    help="Drop oltp schema and rebuild")
    p.add_argument("--continuous",  action="store_true",    help="Continuous mode: keep inserting")
    p.add_argument("--interval",    type=int, default=15,   help="Continuous mode: seconds between batches")
    p.add_argument("--batch",       type=int, default=25,   help="Continuous mode: orders per batch")
    p.add_argument("--stats",       action="store_true",    help="Print table stats and exit")
    return p.parse_args()


def prompt_destination() -> str:
    options = {"1": "postgres", "2": "csv", "3": "both"}
    print("Choose data destination:\n  1) PostgreSQL\n  2) CSV files\n  3) Both")
    while True:
        choice = input("Destination [1-3]: ").strip()
        if choice in options:
            return options[choice]
        print("Enter 1, 2, or 3.")


def prompt_date(label: str) -> date:
    while True:
        value = input(f"{label} (YYYY-MM-DD): ").strip()
        try:
            return date.fromisoformat(value)
        except ValueError:
            print("Enter a valid date in YYYY-MM-DD format.")


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-7s  %(message)s",
        datefmt="%H:%M:%S",
    )
    args = parse_args()

    if args.stats:
        conn = get_conn()
        cur = conn.cursor()
        print_stats(cur)
        cur.close()
        conn.close()
        return

    destination = args.destination
    if destination is None:
        destination = "both" if args.csv_output else prompt_destination()
    if args.continuous and destination != "postgres":
        raise SystemExit("--continuous supports PostgreSQL output only")
    if args.csv_output is not None and destination == "csv":
        raise SystemExit("--csv-output requires PostgreSQL or both; use --csv-output-dir for CSV-only")
    if destination == "csv" and args.seed_only:
        raise SystemExit("--seed-only is not applicable to CSV-only generation")

    start_dt = (date.fromisoformat(args.start_date) if args.start_date
                else prompt_date("Start date"))
    end_dt = (date.fromisoformat(args.end_date) if args.end_date
              else prompt_date("End date"))
    if start_dt > end_dt:
        raise SystemExit("--start-date must be on or before --end-date")
    if args.customers < 1 or args.orders < 1 or args.events < 0:
        raise SystemExit("--customers and --orders must be positive; --events cannot be negative")

    if destination == "csv":
        tables = generate_csv_dataset(
            args.customers, args.orders, args.events, start_dt, end_dt
        )
        write_oltp_csvs(tables, args.csv_output_dir)
        logger.info("CSV dataset generated in %s.", args.csv_output_dir)
        return

    conn = get_conn()
    cur  = conn.cursor()

    if args.reset:
        logger.warning("Dropping oltp schema …")
        cur.execute("DROP SCHEMA IF EXISTS oltp CASCADE")
        conn.commit()

    # Always ensure schema + reference data
    cur.execute("CREATE SCHEMA IF NOT EXISTS oltp")
    conn.commit()
    seed_reference(cur)
    conn.commit()

    if args.seed_only:
        print_stats(cur)
        cur.close(); conn.close()
        return

    # Customers
    generate_inventory(cur)
    conn.commit()

    # Check existing customers
    cur.execute("SELECT customer_id FROM oltp.customer")
    existing_ids = [r[0] for r in cur.fetchall()]

    if len(existing_ids) < args.customers:
        new_ids = generate_customers(cur, args.customers - len(existing_ids))
        conn.commit()
        all_ids = existing_ids + new_ids
    else:
        all_ids = existing_ids

    if args.continuous:
        logger.info("Continuous mode — batch=%d every %ds. Ctrl+C to stop.", args.batch, args.interval)
        iteration = 0
        try:
            while True:
                iteration += 1
                batch_end = datetime.now()
                generate_orders(cur, all_ids, args.batch,
                                start_date=batch_end.date() - timedelta(days=1),
                                end_date=batch_end.date())
                generate_events(cur, all_ids, n=args.batch * 5)
                conn.commit()
                logger.info("Batch #%d committed. Sleeping %ds …", iteration, args.interval)
                time.sleep(args.interval)
        except KeyboardInterrupt:
            logger.info("Stopped.")
    else:
        generate_orders(cur, all_ids, args.orders, start_date=start_dt, end_date=end_dt,
                        csv_output=args.csv_output)
        conn.commit()
        generate_events(cur, all_ids, n=args.events,
                        start_date=start_dt, end_date=end_dt)
        conn.commit()

    if destination == "both":
        export_postgres_csvs(cur, args.csv_output_dir)

    print_stats(cur)
    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
