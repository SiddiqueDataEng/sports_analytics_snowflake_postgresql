"""
load_from_parquet.py
====================
Loads the LATEST Parquet staging files directly into Neon DWH
without re-extracting from OLTP (fast — no network queries).

Usage
-----
    python etl/load_from_parquet.py
    python etl/load_from_parquet.py --status
"""

import logging
import sys
from pathlib import Path

import pandas as pd

PROJECT_DIR = Path(__file__).parent.parent
STAGING_DIR = Path(__file__).parent / "staging"
sys.path.insert(0, str(PROJECT_DIR))
sys.path.insert(0, str(Path(__file__).parent))

from transform  import run_transform
from neon_load  import run_neon_load, show_status
from db         import get_neon_conn

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


def load_latest_parquet() -> dict:
    """Read the most recent Parquet file per table from etl/staging/."""
    tables = [
        "customers","products","stores","promotions",
        "orders","order_items","payments","inventory","customer_events"
    ]
    extracted = {}
    for table in tables:
        table_dir = STAGING_DIR / table
        if not table_dir.exists():
            continue
        files = sorted(table_dir.glob("*.parquet"))
        if not files:
            continue
        latest = files[-1]
        extracted[table] = latest
        logger.info("  %-20s  ← %s", table, latest.name)
    return extracted


def parse_args():
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--status", action="store_true", help="Show DWH row counts only")
    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()
    conn = get_neon_conn(direct=True)

    if args.status:
        show_status(conn.cursor())
        conn.close()
        sys.exit(0)

    logger.info("Loading latest Parquet files into Neon DWH …")
    extracted   = load_latest_parquet()
    transformed = run_transform(extracted)
    run_neon_load(transformed, conn=conn)

    logger.info("Showing final row counts …")
    show_status(conn.cursor())
    conn.close()
