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
    # Seed reference data only
    python oltp/generator.py --seed-only

    # Generate 500 customers + 2000 orders (one-shot)
    python oltp/generator.py --customers 500 --orders 2000

    # Continuous mode: generate new orders every 10 seconds
    python oltp/generator.py --continuous --interval 10 --batch 20

    # Reset and regenerate everything
    python oltp/generator.py --reset --customers 1000 --orders 5000
"""

import argparse
import json
import logging
import random
import re
import sys
import time
from datetime import date, datetime, timedelta
from pathlib import Path

import psycopg2
import psycopg2.extras
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
    psycopg2.extras.execute_batch(cur, """
        INSERT INTO oltp.store (code, name, store_type, region, address,
                                city, state, zip_code, country, open_date, manager_name)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (code) DO NOTHING
    """, STORES)

    # Promotions
    psycopg2.extras.execute_batch(cur, """
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
                fake.unique.email(),
                fake.numerify("###-###-####"),
                dob, gender,
                fake.street_address(),
                city, state, zip_code, country,
                tier, signup_dt
            ))
            ids.append(cur.fetchone()[0])
        except psycopg2.errors.UniqueViolation:
            cur.connection.rollback()
            # Skip duplicate email

    logger.info("  Inserted %d customers.", len(ids))
    return ids


# ---------------------------------------------------------------------------
# Order generator
# ---------------------------------------------------------------------------

def generate_orders(cur, customer_ids: list[int], n_orders: int,
                    start_date: date = None, end_date: date = None):
    if start_date is None:
        start_date = date(2024, 1, 1)
    if end_date is None:
        end_date = date.today()

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
        order_dt = fake.date_time_between(start_date=start_date, end_date=end_date)
        order_ref = f"ORD-{order_dt.strftime('%Y%m')}-{str(i+1).zfill(6)}"

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
    psycopg2.extras.execute_batch(cur, """
        INSERT INTO oltp.order_header
            (order_ref, customer_id, store_id, promotion_id, channel,
             order_status, order_date, shipped_date, delivered_date,
             subtotal_amt, discount_amt, tax_amt, shipping_amt, total_amt)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (order_ref) DO NOTHING
    """, order_rows, page_size=200)
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
                              fake.bothify("TXN-########-????").upper()))

    logger.info("  Inserting %d order items …", len(all_item_rows))
    psycopg2.extras.execute_batch(cur, """
        INSERT INTO oltp.order_item
            (order_id, product_id, qty, unit_price, unit_cost,
             discount_pct, line_subtotal, line_discount, line_total)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """, all_item_rows, page_size=500)

    logger.info("  Inserting %d payments …", len(all_pay_rows))
    psycopg2.extras.execute_batch(cur, """
        INSERT INTO oltp.payment
            (order_id, payment_method, payment_status, amount, transaction_ref)
        VALUES (%s,%s,%s,%s,%s)
    """, all_pay_rows, page_size=500)

    cur.connection.commit()
    logger.info("  Generated %d orders total.", len(ref_to_id))


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

    psycopg2.extras.execute_batch(cur, """
        INSERT INTO oltp.inventory (product_id, store_id, qty_on_hand, reorder_point)
        VALUES (%s,%s,%s,%s)
        ON CONFLICT (product_id, store_id) DO UPDATE
        SET qty_on_hand = EXCLUDED.qty_on_hand, last_updated = NOW()
    """, rows, page_size=500)
    logger.info("  Inventory seeded (%d products × %d stores)", len(products), len(stores))


# ---------------------------------------------------------------------------
# Customer events
# ---------------------------------------------------------------------------

def generate_events(cur, customer_ids: list[int], n: int = 2000):
    logger.info("Generating %d customer events …", n)
    cur.execute("SELECT product_id FROM oltp.product WHERE is_active=TRUE")
    products = [r[0] for r in cur.fetchall()]

    events = []
    for _ in range(n):
        cid      = random.choice(customer_ids)
        etype    = random.choice(["PAGE_VIEW","PAGE_VIEW","ADD_TO_CART","ADD_TO_CART",
                                  "CHECKOUT_START","PURCHASE","REMOVE_FROM_CART","REVIEW"])
        prod_id  = random.choice(products) if etype != "PAGE_VIEW" else None
        ts       = fake.date_time_between(start_date=date(2024,1,1), end_date=datetime.now())
        evt_data = {}
        if etype == "PAGE_VIEW":
            evt_data = {"page": random.choice(["/products","/category","/sale","/new-arrivals"])}
        elif etype in ("ADD_TO_CART","REMOVE_FROM_CART"):
            evt_data = {"qty": random.randint(1,3)}
        elif etype == "REVIEW":
            evt_data = {"rating": random.randint(3,5), "verified": True}

        events.append((cid, fake.bothify("SESS-####-????").upper(),
                        etype, prod_id, json.dumps(evt_data), ts))

    psycopg2.extras.execute_batch(cur, """
        INSERT INTO oltp.customer_event
            (customer_id, session_id, event_type, product_id, event_data, occurred_at)
        VALUES (%s,%s,%s,%s,%s,%s)
    """, events, page_size=500)
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
# Main
# ---------------------------------------------------------------------------

def parse_args():
    p = argparse.ArgumentParser(description="SportZone OLTP data generator")
    p.add_argument("--seed-only",   action="store_true", help="Only seed reference data")
    p.add_argument("--customers",   type=int, default=500,  help="Number of customers to generate")
    p.add_argument("--orders",      type=int, default=2000, help="Number of orders to generate")
    p.add_argument("--events",      type=int, default=3000, help="Number of customer events")
    p.add_argument("--start-date",  default="2024-01-01",   help="Order start date YYYY-MM-DD")
    p.add_argument("--end-date",    default=None,           help="Order end date YYYY-MM-DD")
    p.add_argument("--reset",       action="store_true",    help="Drop oltp schema and rebuild")
    p.add_argument("--continuous",  action="store_true",    help="Continuous mode: keep inserting")
    p.add_argument("--interval",    type=int, default=15,   help="Continuous mode: seconds between batches")
    p.add_argument("--batch",       type=int, default=25,   help="Continuous mode: orders per batch")
    p.add_argument("--stats",       action="store_true",    help="Print table stats and exit")
    return p.parse_args()


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-7s  %(message)s",
        datefmt="%H:%M:%S",
    )
    args = parse_args()

    conn = get_conn()
    cur  = conn.cursor()

    if args.reset:
        logger.warning("Dropping oltp schema …")
        cur.execute("DROP SCHEMA IF EXISTS oltp CASCADE")
        conn.commit()

    if args.stats:
        print_stats(cur)
        cur.close(); conn.close()
        return

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

    start_dt = date.fromisoformat(args.start_date)
    end_dt   = date.fromisoformat(args.end_date) if args.end_date else date.today()

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
        generate_orders(cur, all_ids, args.orders, start_date=start_dt, end_date=end_dt)
        conn.commit()
        generate_events(cur, all_ids, n=args.events)
        conn.commit()

    print_stats(cur)
    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
