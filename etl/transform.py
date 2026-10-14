"""
transform.py
============
Transform extracted Parquet files into DWH-ready DataFrames.
Applies cleansing, type casting, business rules, and derives warehouse columns.

Input  : dict of {table_name: parquet_path}  (from extract.py)
Output : dict of {target_table: DataFrame}    (passed to load.py)

Target tables map:
    customers      → RAW.RAW_CUSTOMERS      + STAGING.STG_CUSTOMERS
    products       → RAW.RAW_PRODUCTS       + STAGING.STG_PRODUCTS
    stores         → RAW.RAW_STORES         + STAGING.STG_STORES
    promotions     → RAW.RAW_PROMOTIONS     + STAGING.STG_PROMOTIONS
    orders         → RAW.RAW_ORDERS         + STAGING.STG_ORDERS
    order_items    → RAW.RAW_ORDER_ITEMS    + STAGING.STG_ORDER_ITEMS
    inventory      → RAW.RAW_INVENTORY      + STAGING.STG_INVENTORY
"""

import logging
from datetime import date, datetime, timezone
from pathlib import Path

import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def safe_date(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce").dt.date


def safe_ts(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce")


def yn_to_bool(series: pd.Series) -> pd.Series:
    return series.map(lambda x: True if str(x).upper() in ("TRUE","1","Y","YES","T") else False)


def age_band(dob_series: pd.Series) -> pd.Series:
    today = date.today()
    def _band(dob):
        if pd.isna(dob):
            return "UNKNOWN"
        try:
            age = (today - dob).days // 365
            if age < 25: return "18-24"
            if age < 35: return "25-34"
            if age < 45: return "35-44"
            if age < 55: return "45-54"
            return "55+"
        except Exception:
            return "UNKNOWN"
    return dob_series.map(_band)


def price_tier(price_series: pd.Series) -> pd.Series:
    return price_series.map(
        lambda p: "BUDGET" if p < 50 else ("MID" if p < 120 else "PREMIUM")
    )


def load_parquet(path: Path) -> pd.DataFrame:
    return pd.read_parquet(path)


# ---------------------------------------------------------------------------
# Per-table transforms
# ---------------------------------------------------------------------------

def transform_customers(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Returns (raw_df, staging_df)"""
    df = df.copy()
    df["_src_file"]   = "oltp.customer"
    df["_load_ts"]    = datetime.now(timezone.utc)

    # --- RAW ---
    raw = df.rename(columns={
        "customer_id":  "CUST_ID",
        "first_name":   "FIRST_NAME",
        "last_name":    "LAST_NAME",
        "email":        "EMAIL",
        "phone":        "PHONE",
        "date_of_birth":"DATE_OF_BIRTH",
        "gender":       "GENDER",
        "address_line1":"ADDRESS_LINE1",
        "address_line2":"ADDRESS_LINE2",
        "city":         "CITY",
        "state":        "STATE",
        "zip_code":     "ZIP_CODE",
        "country":      "COUNTRY",
        "loyalty_tier": "LOYALTY_TIER",
        "signup_date":  "SIGNUP_DATE",
        "is_active":    "IS_ACTIVE",
        "_src_file":    "_SRC_FILE",
        "_load_ts":     "_LOAD_TS",
    }).astype({"CUST_ID": str})

    raw_cols = ["CUST_ID","FIRST_NAME","LAST_NAME","EMAIL","PHONE","DATE_OF_BIRTH",
                "GENDER","ADDRESS_LINE1","ADDRESS_LINE2","CITY","STATE","ZIP_CODE",
                "COUNTRY","LOYALTY_TIER","SIGNUP_DATE","IS_ACTIVE","_SRC_FILE","_LOAD_TS"]
    raw = raw[[c for c in raw_cols if c in raw.columns]]

    # --- STAGING ---
    stg = df.copy()
    stg["CUST_ID"]       = stg["customer_id"].astype(str)
    stg["FIRST_NAME"]    = stg["first_name"].str.strip().str.title()
    stg["LAST_NAME"]     = stg["last_name"].str.strip().str.title()
    stg["FULL_NAME"]     = stg["FIRST_NAME"] + " " + stg["LAST_NAME"]
    stg["EMAIL"]         = stg["email"].str.lower().str.strip()
    stg["PHONE"]         = stg["phone"].fillna("").str.strip()
    stg["DATE_OF_BIRTH"] = safe_date(stg["date_of_birth"])
    stg["AGE_BAND"]      = age_band(stg["DATE_OF_BIRTH"])
    stg["GENDER"]        = stg["gender"].fillna("U").str.upper().str[:1]
    stg["ADDRESS_LINE1"] = stg["address_line1"].fillna("").str.strip()
    stg["ADDRESS_LINE2"] = stg["address_line2"].fillna("").str.strip()
    stg["CITY"]          = stg["city"].fillna("").str.strip().str.title()
    stg["STATE"]         = stg["state"].fillna("").str.upper()
    stg["ZIP_CODE"]      = stg["zip_code"].fillna("").str.strip()
    stg["COUNTRY"]       = stg["country"].fillna("").str.upper()
    stg["LOYALTY_TIER"]  = stg["loyalty_tier"].fillna("BRONZE").str.upper()
    stg["SIGNUP_DATE"]   = safe_date(stg["signup_date"])
    stg["IS_ACTIVE"]     = stg["is_active"].astype(bool)
    stg["EFF_START_DT"]  = date.today()
    stg["EFF_END_DT"]    = None
    stg["IS_CURRENT"]    = True
    stg["_LOAD_TS"]      = datetime.now(timezone.utc)

    stg_cols = ["CUST_ID","FIRST_NAME","LAST_NAME","FULL_NAME","EMAIL","PHONE",
                "DATE_OF_BIRTH","AGE_BAND","GENDER","ADDRESS_LINE1","ADDRESS_LINE2",
                "CITY","STATE","ZIP_CODE","COUNTRY","LOYALTY_TIER","SIGNUP_DATE",
                "IS_ACTIVE","EFF_START_DT","EFF_END_DT","IS_CURRENT","_LOAD_TS"]
    stg = stg[[c for c in stg_cols if c in stg.columns]]
    return raw, stg


def transform_products(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    df = df.copy()

    # RAW — build explicitly to avoid duplicate columns
    raw = pd.DataFrame()
    raw["PRD_ID"]        = df["product_id"].astype(str)
    raw["PRD_NAME"]      = df["product_name"]
    raw["PRD_CATEGORY"]  = df["parent_category"]
    raw["PRD_SUBCATEGORY"] = df["category"]
    raw["BRAND"]         = df["brand"]
    raw["UNIT_PRICE"]    = df["unit_price"]
    raw["UNIT_COST"]     = df["unit_cost"]
    raw["SKU"]           = df["sku"]
    raw["SUPPLIER_ID"]   = df["supplier_name"] if "supplier_name" in df.columns else ""
    raw["IS_ACTIVE"]     = df["is_active"]
    raw["LAUNCH_DATE"]   = df["launch_date"] if "launch_date" in df.columns else None
    raw["DISCONTINUE_DT"]= df["discontinue_dt"] if "discontinue_dt" in df.columns else None
    raw["_SRC_FILE"]     = "oltp.product"
    raw["_LOAD_TS"]      = datetime.now(timezone.utc)

    # STAGING
    stg = pd.DataFrame()
    stg["PRD_ID"]          = df["product_id"].astype(str)
    stg["PRD_NAME"]        = df["product_name"].str.strip()
    stg["PRD_CATEGORY"]    = df["parent_category"].fillna(df["category"]).str.upper()
    stg["PRD_SUBCATEGORY"] = df["category"].str.upper()
    stg["BRAND"]           = df["brand"].str.strip()
    stg["SKU"]             = df["sku"].str.upper()
    stg["UNIT_PRICE"]      = pd.to_numeric(df["unit_price"], errors="coerce").round(2)
    stg["UNIT_COST"]       = pd.to_numeric(df["unit_cost"],  errors="coerce").round(2)
    stg["GROSS_MARGIN_PCT"]= pd.to_numeric(df["gross_margin_pct"], errors="coerce").round(2)
    stg["PRICE_TIER"]      = price_tier(stg["UNIT_PRICE"])
    stg["SUPPLIER_ID"]     = df["supplier_name"]
    stg["IS_ACTIVE"]       = df["is_active"].astype(bool)
    stg["LAUNCH_DATE"]     = safe_date(df["launch_date"])
    stg["DISCONTINUE_DT"]  = safe_date(df["discontinue_dt"])
    stg["EFF_START_DT"]    = date.today()
    stg["EFF_END_DT"]      = None
    stg["IS_CURRENT"]      = True
    stg["_LOAD_TS"]        = datetime.now(timezone.utc)
    return raw, stg


def transform_stores(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    df = df.copy()
    # Build explicitly — OLTP may have extra columns (e.g. code) not in DWH schema
    raw = pd.DataFrame()
    raw["STORE_ID"]    = df["store_id"].astype(str)
    raw["STORE_NAME"]  = df["store_name"]
    raw["STORE_TYPE"]  = df["store_type"]
    raw["REGION"]      = df["region"] if "region" in df.columns else ""
    raw["ADDRESS"]     = df["address"] if "address" in df.columns else ""
    raw["CITY"]        = df["city"] if "city" in df.columns else ""
    raw["STATE"]       = df["state"] if "state" in df.columns else ""
    raw["ZIP_CODE"]    = df["zip_code"] if "zip_code" in df.columns else ""
    raw["COUNTRY"]     = df["country"] if "country" in df.columns else ""
    raw["OPEN_DATE"]   = df["open_date"] if "open_date" in df.columns else None
    raw["MANAGER_NAME"]= df["manager_name"] if "manager_name" in df.columns else ""
    raw["_SRC_FILE"]   = "oltp.store"
    raw["_LOAD_TS"]    = datetime.now(timezone.utc)

    stg = raw.copy()
    stg["STORE_TYPE"] = stg["STORE_TYPE"].str.upper()
    stg["REGION"]     = stg["REGION"].fillna("").str.upper()
    stg["COUNTRY"]    = stg["COUNTRY"].fillna("").str.upper()
    return raw, stg


def transform_promotions(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    df = df.copy()
    # Build explicitly — OLTP may have extra columns (e.g. max_uses) not in DWH schema
    raw = pd.DataFrame()
    raw["PROMO_ID"]      = df["promotion_id"].astype(str) if "promotion_id" in df.columns else df["promo_id"].astype(str)
    raw["PROMO_CODE"]    = df["promo_code"]
    raw["PROMO_NAME"]    = df["promo_name"]
    raw["PROMO_TYPE"]    = df["promo_type"]
    raw["DISCOUNT_VALUE"]= df["discount_value"] if "discount_value" in df.columns else None
    raw["MIN_ORDER_AMT"] = df["min_order_amt"] if "min_order_amt" in df.columns else None
    raw["START_DATE"]    = df["start_date"] if "start_date" in df.columns else None
    raw["END_DATE"]      = df["end_date"] if "end_date" in df.columns else None
    raw["IS_ACTIVE"]     = df["is_active"]
    raw["_SRC_FILE"]     = "oltp.promotion"
    raw["_LOAD_TS"]      = datetime.now(timezone.utc)

    stg = pd.DataFrame()
    stg["PROMO_ID"]       = raw["PROMO_ID"]
    stg["PROMO_CODE"]     = raw["PROMO_CODE"].str.upper()
    stg["PROMO_NAME"]     = raw["PROMO_NAME"]
    stg["PROMO_TYPE"]     = raw["PROMO_TYPE"].str.upper()
    stg["DISCOUNT_VALUE"] = raw["DISCOUNT_VALUE"]
    stg["START_DATE"]     = raw["START_DATE"]
    stg["END_DATE"]       = raw["END_DATE"]
    stg["MIN_ORDER_AMT"]  = raw["MIN_ORDER_AMT"]
    stg["IS_ACTIVE"]      = raw["IS_ACTIVE"].astype(bool)
    stg["_LOAD_TS"]       = datetime.now(timezone.utc)
    return raw, stg


def transform_orders(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    df = df.copy()
    raw = pd.DataFrame()
    raw["ORDER_ID"]      = df["order_id"].astype(str)
    raw["ORDER_DATE"]    = safe_ts(df["order_date"])
    raw["CUST_ID"]       = df["customer_id"].astype(str)
    raw["STORE_ID"]      = df["store_id"].astype(str)
    raw["CHANNEL"]       = df["channel"]
    raw["ORDER_STATUS"]  = df["order_status"]
    # promo_code may be absent if the orders query was simplified
    raw["PROMO_CODE"]    = df["promo_code"].fillna("") if "promo_code" in df.columns else ""
    raw["TOTAL_AMT"]     = pd.to_numeric(df["total_amt"],    errors="coerce").round(2)
    raw["DISCOUNT_AMT"]  = pd.to_numeric(df["discount_amt"], errors="coerce").round(2)
    raw["NET_AMT"]       = (raw["TOTAL_AMT"] - raw["DISCOUNT_AMT"]).round(2)
    raw["_SRC_FILE"]     = "oltp.order_header"
    raw["_LOAD_TS"]      = datetime.now(timezone.utc)

    stg = pd.DataFrame()
    stg["ORDER_ID"]      = raw["ORDER_ID"]
    stg["ORDER_DATE"]    = raw["ORDER_DATE"].dt.date
    stg["ORDER_YEAR"]    = raw["ORDER_DATE"].dt.year.astype("Int64")
    stg["ORDER_MONTH"]   = raw["ORDER_DATE"].dt.month.astype("Int64")
    stg["ORDER_QUARTER"] = raw["ORDER_DATE"].dt.quarter.astype("Int64")
    stg["CUST_ID"]       = raw["CUST_ID"]
    stg["STORE_ID"]      = raw["STORE_ID"]
    stg["CHANNEL"]       = raw["CHANNEL"].str.upper()
    stg["ORDER_STATUS"]  = raw["ORDER_STATUS"].str.upper()
    stg["PROMO_CODE"]    = raw["PROMO_CODE"].replace("", None)
    stg["TOTAL_AMT"]     = raw["TOTAL_AMT"]
    stg["DISCOUNT_AMT"]  = raw["DISCOUNT_AMT"]
    stg["NET_AMT"]       = raw["NET_AMT"]
    stg["IS_DISCOUNTED"] = (raw["DISCOUNT_AMT"] > 0)
    stg["_LOAD_TS"]      = datetime.now(timezone.utc)
    return raw, stg


def transform_order_items(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    df = df.copy()
    raw = pd.DataFrame()
    raw["ORDER_ITEM_ID"] = df["order_item_id"].astype(str)
    raw["ORDER_ID"]      = df["order_id"].astype(str)
    raw["PRD_ID"]        = df["product_id"].astype(str)
    raw["QTY"]           = pd.to_numeric(df["qty"],          errors="coerce")
    raw["UNIT_PRICE"]    = pd.to_numeric(df["unit_price"],   errors="coerce").round(2)
    raw["DISCOUNT_PCT"]  = pd.to_numeric(df["discount_pct"], errors="coerce").round(2)
    raw["LINE_TOTAL"]    = pd.to_numeric(df["line_total"],   errors="coerce").round(2)
    raw["_SRC_FILE"]     = "oltp.order_item"
    raw["_LOAD_TS"]      = datetime.now(timezone.utc)

    # STG_ORDER_ITEMS does NOT have _SRC_FILE — select only schema columns
    stg = pd.DataFrame()
    stg["ORDER_ITEM_ID"] = raw["ORDER_ITEM_ID"]
    stg["ORDER_ID"]      = raw["ORDER_ID"]
    stg["PRD_ID"]        = raw["PRD_ID"]
    stg["QTY"]           = raw["QTY"]
    stg["UNIT_PRICE"]    = raw["UNIT_PRICE"]
    stg["DISCOUNT_PCT"]  = raw["DISCOUNT_PCT"]
    stg["LINE_TOTAL"]    = raw["LINE_TOTAL"]
    stg["DISCOUNT_AMT"]  = (raw["UNIT_PRICE"] * raw["QTY"] * raw["DISCOUNT_PCT"] / 100).round(2)
    stg["GROSS_REVENUE"] = (raw["UNIT_PRICE"] * raw["QTY"]).round(2)
    stg["_LOAD_TS"]      = datetime.now(timezone.utc)
    return raw, stg


def transform_inventory(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    df = df.copy()
    raw = pd.DataFrame()
    raw["INVENTORY_ID"]  = df["inventory_id"].astype(str)
    raw["PRD_ID"]        = df["product_id"].astype(str)
    raw["STORE_ID"]      = df["store_id"].astype(str)
    raw["SNAPSHOT_DATE"] = safe_date(df["updated_at"])
    raw["QTY_ON_HAND"]   = pd.to_numeric(df["qty_on_hand"],   errors="coerce")
    raw["QTY_RESERVED"]  = pd.to_numeric(df["qty_reserved"],  errors="coerce")
    raw["REORDER_POINT"] = pd.to_numeric(df["reorder_point"], errors="coerce")
    raw["INV_STATUS"]    = df["inv_status"].str.upper()
    raw["_SRC_FILE"]     = "oltp.inventory"
    raw["_LOAD_TS"]      = datetime.now(timezone.utc)

    # STG_INVENTORY does NOT have _SRC_FILE — select only schema columns
    stg = pd.DataFrame()
    stg["INVENTORY_ID"]  = raw["INVENTORY_ID"]
    stg["PRD_ID"]        = raw["PRD_ID"]
    stg["STORE_ID"]      = raw["STORE_ID"]
    stg["SNAPSHOT_DATE"] = raw["SNAPSHOT_DATE"]
    stg["QTY_ON_HAND"]   = raw["QTY_ON_HAND"]
    stg["QTY_RESERVED"]  = raw["QTY_RESERVED"]
    stg["QTY_AVAILABLE"] = (raw["QTY_ON_HAND"] - raw["QTY_RESERVED"]).clip(lower=0)
    stg["REORDER_POINT"] = raw["REORDER_POINT"]
    stg["INV_STATUS"]    = raw["INV_STATUS"]
    stg["NEEDS_REORDER"] = raw["QTY_ON_HAND"] <= raw["REORDER_POINT"]
    stg["_LOAD_TS"]      = datetime.now(timezone.utc)
    return raw, stg


# ---------------------------------------------------------------------------
# Dispatch
# ---------------------------------------------------------------------------

TRANSFORM_MAP = {
    "customers":   transform_customers,
    "products":    transform_products,
    "stores":      transform_stores,
    "promotions":  transform_promotions,
    "orders":      transform_orders,
    "order_items": transform_order_items,
    "inventory":   transform_inventory,
}

# Snowflake target table names
TARGET_MAP = {
    "customers":   ("RAW.RAW_CUSTOMERS",   "STAGING.STG_CUSTOMERS"),
    "products":    ("RAW.RAW_PRODUCTS",    "STAGING.STG_PRODUCTS"),
    "stores":      ("RAW.RAW_STORES",      "STAGING.STG_STORES"),
    "promotions":  ("RAW.RAW_PROMOTIONS",  "STAGING.STG_PROMOTIONS"),
    "orders":      ("RAW.RAW_ORDERS",      "STAGING.STG_ORDERS"),
    "order_items": ("RAW.RAW_ORDER_ITEMS", "STAGING.STG_ORDER_ITEMS"),
    "inventory":   ("RAW.RAW_INVENTORY",   "STAGING.STG_INVENTORY"),
}


def run_transform(extracted: dict[str, Path]) -> dict[str, pd.DataFrame]:
    """
    Transforms each extracted Parquet file.
    Returns {snowflake_table_name: dataframe}.
    """
    logger.info("=== TRANSFORM phase ===")
    result = {}

    for name, path in extracted.items():
        if name not in TRANSFORM_MAP:
            logger.debug("  No transform defined for %s — skipping.", name)
            continue

        logger.info("  Transforming %-20s (%s) …", name, path.name)
        df = load_parquet(path)

        if df.empty:
            logger.info("  %-20s  empty — skip.", name)
            continue

        raw_df, stg_df = TRANSFORM_MAP[name](df)
        raw_tbl, stg_tbl = TARGET_MAP[name]

        result[raw_tbl] = raw_df
        result[stg_tbl] = stg_df
        logger.info("  %-20s → raw=%d rows, stg=%d rows",
                    name, len(raw_df), len(stg_df))

    logger.info("Transform complete: %d target tables.\n", len(result))
    return result
