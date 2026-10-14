"""
loader.py
=========
Uploads sample CSV files from 01_raw/04_sample_data/ to Snowflake internal
stages and then runs COPY INTO to populate the RAW layer tables.

Called by deploy.py --load-data, or run standalone:
    python deploy/loader.py
"""

import logging
import os
import sys
from pathlib import Path

import snowflake.connector

DEPLOY_DIR   = Path(__file__).parent
PROJECT_DIR  = DEPLOY_DIR.parent
SAMPLE_DIR   = PROJECT_DIR / "01_raw" / "04_sample_data"

sys.path.insert(0, str(DEPLOY_DIR))
from config import load_config

logger = logging.getLogger(__name__)

# Map: csv filename → (internal_stage, target_table, file_format)
FILE_MAP = {
    "customers.csv":   ("SPORTZONE_DEV.RAW.stg_customers",   "SPORTZONE_DEV.RAW.RAW_CUSTOMERS",   "SPORTZONE_DEV.RAW.csv_comma_fmt"),
    "products.csv":    ("SPORTZONE_DEV.RAW.stg_products",    "SPORTZONE_DEV.RAW.RAW_PRODUCTS",    "SPORTZONE_DEV.RAW.csv_comma_fmt"),
    "stores.csv":      ("SPORTZONE_DEV.RAW.stg_stores",      "SPORTZONE_DEV.RAW.RAW_STORES",      "SPORTZONE_DEV.RAW.csv_comma_fmt"),
    "orders.csv":      ("SPORTZONE_DEV.RAW.stg_orders",      "SPORTZONE_DEV.RAW.RAW_ORDERS",      "SPORTZONE_DEV.RAW.csv_pipe_fmt"),
    "order_items.csv": ("SPORTZONE_DEV.RAW.stg_orders",      "SPORTZONE_DEV.RAW.RAW_ORDER_ITEMS", "SPORTZONE_DEV.RAW.csv_pipe_fmt"),
    "inventory.csv":   ("SPORTZONE_DEV.RAW.stg_inventory",   "SPORTZONE_DEV.RAW.RAW_INVENTORY",   "SPORTZONE_DEV.RAW.csv_comma_fmt"),
    "promotions.csv":  ("SPORTZONE_DEV.RAW.stg_promotions",  "SPORTZONE_DEV.RAW.RAW_PROMOTIONS",  "SPORTZONE_DEV.RAW.csv_comma_fmt"),
}

# Column mappings for COPY INTO (positional $N → column name)
COPY_SQLS = {
    "customers.csv": """
        COPY INTO SPORTZONE_DEV.RAW.RAW_CUSTOMERS
        (CUST_ID,FIRST_NAME,LAST_NAME,EMAIL,PHONE,DATE_OF_BIRTH,
         GENDER,ADDRESS_LINE1,ADDRESS_LINE2,CITY,STATE,ZIP_CODE,
         COUNTRY,LOYALTY_TIER,SIGNUP_DATE,IS_ACTIVE,_SRC_FILE)
        FROM (SELECT $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,
                     METADATA$FILENAME
              FROM @SPORTZONE_DEV.RAW.stg_customers/customers.csv.gz)
        FILE_FORMAT = SPORTZONE_DEV.RAW.csv_comma_fmt
        ON_ERROR = 'CONTINUE'
        PURGE = FALSE
    """,
    "products.csv": """
        COPY INTO SPORTZONE_DEV.RAW.RAW_PRODUCTS
        (PRD_ID,PRD_NAME,PRD_CATEGORY,PRD_SUBCATEGORY,BRAND,
         UNIT_PRICE,UNIT_COST,SKU,SUPPLIER_ID,IS_ACTIVE,
         LAUNCH_DATE,DISCONTINUE_DT,_SRC_FILE)
        FROM (SELECT $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,
                     METADATA$FILENAME
              FROM @SPORTZONE_DEV.RAW.stg_products/products.csv.gz)
        FILE_FORMAT = SPORTZONE_DEV.RAW.csv_comma_fmt
        ON_ERROR = 'CONTINUE'
        PURGE = FALSE
    """,
    "stores.csv": """
        COPY INTO SPORTZONE_DEV.RAW.RAW_STORES
        (STORE_ID,STORE_NAME,STORE_TYPE,REGION,ADDRESS,
         CITY,STATE,ZIP_CODE,COUNTRY,OPEN_DATE,MANAGER_NAME,_SRC_FILE)
        FROM (SELECT $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,
                     METADATA$FILENAME
              FROM @SPORTZONE_DEV.RAW.stg_stores/stores.csv.gz)
        FILE_FORMAT = SPORTZONE_DEV.RAW.csv_comma_fmt
        ON_ERROR = 'CONTINUE'
        PURGE = FALSE
    """,
    "orders.csv": """
        COPY INTO SPORTZONE_DEV.RAW.RAW_ORDERS
        (ORDER_ID,ORDER_DATE,CUST_ID,STORE_ID,CHANNEL,
         ORDER_STATUS,PROMO_CODE,TOTAL_AMT,DISCOUNT_AMT,NET_AMT,_SRC_FILE)
        FROM (SELECT $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,
                     METADATA$FILENAME
              FROM @SPORTZONE_DEV.RAW.stg_orders/orders.csv.gz)
        FILE_FORMAT = SPORTZONE_DEV.RAW.csv_pipe_fmt
        ON_ERROR = 'CONTINUE'
        PURGE = FALSE
    """,
    "order_items.csv": """
        COPY INTO SPORTZONE_DEV.RAW.RAW_ORDER_ITEMS
        (ORDER_ITEM_ID,ORDER_ID,PRD_ID,QTY,
         UNIT_PRICE,DISCOUNT_PCT,LINE_TOTAL,_SRC_FILE)
        FROM (SELECT $1,$2,$3,$4,$5,$6,$7,
                     METADATA$FILENAME
              FROM @SPORTZONE_DEV.RAW.stg_orders/order_items.csv.gz)
        FILE_FORMAT = SPORTZONE_DEV.RAW.csv_pipe_fmt
        ON_ERROR = 'CONTINUE'
        PURGE = FALSE
    """,
    "inventory.csv": """
        COPY INTO SPORTZONE_DEV.RAW.RAW_INVENTORY
        (INVENTORY_ID,PRD_ID,STORE_ID,SNAPSHOT_DATE,
         QTY_ON_HAND,QTY_RESERVED,REORDER_POINT,INV_STATUS,_SRC_FILE)
        FROM (SELECT $1,$2,$3,$4,$5,$6,$7,$8,
                     METADATA$FILENAME
              FROM @SPORTZONE_DEV.RAW.stg_inventory/inventory.csv.gz)
        FILE_FORMAT = SPORTZONE_DEV.RAW.csv_comma_fmt
        ON_ERROR = 'CONTINUE'
        PURGE = FALSE
    """,
    "promotions.csv": """
        COPY INTO SPORTZONE_DEV.RAW.RAW_PROMOTIONS
        (PROMO_ID,PROMO_CODE,PROMO_NAME,PROMO_TYPE,DISCOUNT_VALUE,
         START_DATE,END_DATE,MIN_ORDER_AMT,IS_ACTIVE,_SRC_FILE)
        FROM (SELECT $1,$2,$3,$4,$5,$6,$7,$8,$9,
                     METADATA$FILENAME
              FROM @SPORTZONE_DEV.RAW.stg_promotions/promotions.csv.gz)
        FILE_FORMAT = SPORTZONE_DEV.RAW.csv_comma_fmt
        ON_ERROR = 'CONTINUE'
        PURGE = FALSE
    """,
}


def load_sample_data(conn: snowflake.connector.SnowflakeConnection):
    """Upload CSVs via PUT then load into RAW tables via COPY INTO."""
    logger.info("Sample data directory: %s", SAMPLE_DIR)

    for filename, (stage, table, _fmt) in FILE_MAP.items():
        csv_path = SAMPLE_DIR / filename
        if not csv_path.exists():
            logger.warning("  CSV not found, skipping: %s", csv_path)
            continue

        # PUT uploads and auto-gzips the file
        put_sql = f"PUT file://{csv_path.as_posix()} @{stage} AUTO_COMPRESS=TRUE OVERWRITE=TRUE"
        logger.info("  PUT  %s  →  @%s", filename, stage)
        try:
            with conn.cursor() as cur:
                cur.execute(put_sql)
                rows = cur.fetchall()
                for row in rows:
                    logger.debug("       %s", row)
        except Exception as exc:
            logger.error("  PUT failed for %s: %s", filename, exc)
            continue

        # COPY INTO
        copy_sql = COPY_SQLS.get(filename, "")
        if not copy_sql:
            logger.warning("  No COPY SQL defined for %s, skipping load.", filename)
            continue

        logger.info("  COPY %s  →  %s", filename, table)
        try:
            with conn.cursor() as cur:
                cur.execute(copy_sql)
                rows = cur.fetchall()
                loaded = sum(int(r[3]) for r in rows if len(r) > 3 and str(r[3]).isdigit())
                logger.info("       Loaded %d row(s)", loaded)
        except Exception as exc:
            logger.error("  COPY failed for %s: %s", filename, exc)

    # ---- Run staging transforms ----
    logger.info("\n  Running RAW → STAGING transforms …")
    stg_path = PROJECT_DIR / "02_staging" / "02_staging_transforms.sql"
    if stg_path.exists():
        from runner import execute_file
        ok, err = execute_file(conn, stg_path, stop_on_error=False)
        logger.info("  Staging transforms: %d ok, %d errors", ok, err)

    # ---- Run warehouse load ----
    logger.info("  Running STAGING → WAREHOUSE load …")
    wh_path = PROJECT_DIR / "03_warehouse" / "03_load_warehouse.sql"
    if wh_path.exists():
        from runner import execute_file
        ok, err = execute_file(conn, wh_path, stop_on_error=False)
        logger.info("  Warehouse load: %d ok, %d errors", ok, err)

    logger.info("  Sample data pipeline complete.\n")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-7s  %(message)s",
        datefmt="%H:%M:%S",
    )
    conn_params = load_config()
    logger.info("Connecting for data load …")
    conn = snowflake.connector.connect(**conn_params)
    load_sample_data(conn)
    conn.close()
