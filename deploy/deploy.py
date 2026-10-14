"""
deploy.py
=========
End-to-end deployment script for the SportZone Retail Analytics Platform.

Usage
-----
    # Full deploy (all layers in order)
    python deploy/deploy.py

    # Deploy only specific layers
    python deploy/deploy.py --layers 00_setup 01_raw 02_staging

    # Dry-run (print statements, don't execute)
    python deploy/deploy.py --dry-run

    # Skip a layer
    python deploy/deploy.py --skip 06_governance 07_sharing

    # Load sample data after schema deploy
    python deploy/deploy.py --load-data

Layers executed in order
------------------------
    00_setup      → roles, users, warehouses, databases, schemas
    01_raw        → file formats, stages, raw tables + COPY INTO
    02_staging    → staging tables + transforms (raw → staging)
    03_warehouse  → dimensions, facts, load merges
    04_mart       → business mart views
    05_pipelines  → streams + tasks
    06_governance → masking policies + tags
    07_sharing    → secure views + shares
    08_operations → cloning + monitoring queries
"""

import argparse
import logging
import sys
import time
from pathlib import Path

import snowflake.connector

# Resolve paths relative to this script
DEPLOY_DIR  = Path(__file__).parent
PROJECT_DIR = DEPLOY_DIR.parent
sys.path.insert(0, str(DEPLOY_DIR))

from config  import load_config
from runner  import execute_file
from loader  import load_sample_data   # defined in loader.py

# ---------------------------------------------------------------------------
# Ordered deployment pipeline
# ---------------------------------------------------------------------------

LAYERS = [
    ("00_setup",      ["01_account_setup.sql", "02_database_setup.sql"]),
    ("01_raw",        ["01_file_formats.sql", "02_stages.sql", "03_raw_tables.sql"]),
    ("02_staging",    ["01_staging_tables.sql", "02_staging_transforms.sql"]),
    ("03_warehouse",  ["01_dimensions.sql", "02_facts.sql", "03_load_warehouse.sql"]),
    ("04_mart",       ["01_mart_views.sql"]),
    ("05_pipelines",  ["01_streams.sql", "02_tasks.sql"]),
    ("06_governance", ["01_masking_policies.sql", "02_tags.sql"]),
    ("07_sharing",    ["01_data_shares.sql"]),
    ("08_operations", ["01_cloning.sql", "02_monitoring.sql"]),
]

# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args():
    parser = argparse.ArgumentParser(description="Deploy SportZone to Snowflake")
    parser.add_argument(
        "--layers", nargs="*",
        help="Layer folder names to deploy (default: all)"
    )
    parser.add_argument(
        "--skip", nargs="*", default=[],
        help="Layer folder names to skip"
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Print statements without executing"
    )
    parser.add_argument(
        "--load-data", action="store_true",
        help="Upload and load sample CSV data after schema deploy"
    )
    parser.add_argument(
        "--stop-on-error", action="store_true", default=False,
        help="Abort on first error (default: continue)"
    )
    return parser.parse_args()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    args = parse_args()

    # ---- Determine which layers to run ----
    requested = set(args.layers) if args.layers else {name for name, _ in LAYERS}
    skipped   = set(args.skip or [])
    active    = [(name, files) for name, files in LAYERS
                 if name in requested and name not in skipped]

    if not active:
        logger.error("No layers selected. Check --layers / --skip arguments.")
        sys.exit(1)

    logger.info("=" * 60)
    logger.info("SportZone Retail Analytics Platform — Deployment")
    logger.info("Layers : %s", ", ".join(n for n, _ in active))
    logger.info("Dry-run: %s", args.dry_run)
    logger.info("=" * 60)

    # ---- Connect ----
    if not args.dry_run:
        conn_params = load_config()
        logger.info("Connecting to Snowflake account %s as %s …",
                    conn_params["account"], conn_params["user"])
        logger.info("Browser window will open for SSO authentication.")
        try:
            conn = snowflake.connector.connect(**conn_params)
            logger.info("Connected successfully.\n")
        except Exception as exc:
            logger.error("Connection failed: %s", exc)
            sys.exit(1)
    else:
        conn = None
        logger.info("(dry-run — no connection made)\n")

    # ---- Execute layers ----
    total_success = total_error = 0
    start_time = time.time()

    for layer_name, sql_files in active:
        layer_dir = PROJECT_DIR / layer_name
        logger.info("── Layer: %s ──", layer_name)

        for sql_file in sql_files:
            sql_path = layer_dir / sql_file
            if not sql_path.exists():
                logger.warning("  File not found, skipping: %s", sql_path)
                continue

            try:
                ok, err = execute_file(
                    conn, sql_path,
                    dry_run=args.dry_run,
                    stop_on_error=args.stop_on_error,
                )
                total_success += ok
                total_error   += err
            except Exception as exc:
                logger.error("  Aborted on %s: %s", sql_file, exc)
                if not args.dry_run and conn:
                    conn.close()
                sys.exit(1)

        logger.info("")

    # ---- Sample data load ----
    if args.load_data and not args.dry_run:
        logger.info("── Loading sample CSV data ──")
        try:
            load_sample_data(conn)
        except Exception as exc:
            logger.error("Sample data load failed: %s", exc)
            total_error += 1

    # ---- Summary ----
    elapsed = time.time() - start_time
    logger.info("=" * 60)
    logger.info("Done in %.1fs  |  ✓ %d statements  |  ✗ %d errors",
                elapsed, total_success, total_error)
    logger.info("=" * 60)

    if not args.dry_run and conn:
        conn.close()

    sys.exit(0 if total_error == 0 else 1)


if __name__ == "__main__":
    main()
