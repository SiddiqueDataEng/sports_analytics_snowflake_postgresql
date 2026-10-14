"""
migrate.py
==========
Idempotent migration runner with a Snowflake-backed migration log table.

How it works
------------
1. Creates SPORTZONE_DEV.RAW.DEPLOY_MIGRATIONS table on first run.
2. For each .sql file in the ordered LAYERS list:
   - Hash the file content (SHA-256).
   - If hash already in migration table → skip (already applied).
   - Execute the file → on success, record it in the migration table.
3. Supports --status  to list applied migrations without running anything.
4. Supports --rollback N  to mark the last N migrations as pending
   (does NOT reverse the SQL — Snowflake DDL is not transactional in that way,
    but lets you re-run changed files after fixing them).

Usage
-----
    python deploy/migrate.py                  # apply all pending
    python deploy/migrate.py --status         # show migration history
    python deploy/migrate.py --rollback 3     # unmark last 3 migrations
    python deploy/migrate.py --layers 00_setup 01_raw
    python deploy/migrate.py --dry-run
"""

import argparse
import hashlib
import logging
import sys
import time
from pathlib import Path
from datetime import datetime, timezone

import snowflake.connector

DEPLOY_DIR  = Path(__file__).parent
PROJECT_DIR = DEPLOY_DIR.parent
sys.path.insert(0, str(DEPLOY_DIR))

from config import load_config
from runner import execute_file

logger = logging.getLogger(__name__)

MIGRATION_TABLE = "SPORTZONE_DEV.RAW.DEPLOY_MIGRATIONS"

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
# Helpers
# ---------------------------------------------------------------------------

def file_hash(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def ensure_migration_table(conn):
    """Create the migrations tracking table if it doesn't exist."""
    ddl = f"""
    CREATE TABLE IF NOT EXISTS {MIGRATION_TABLE} (
        MIGRATION_ID    INTEGER AUTOINCREMENT PRIMARY KEY,
        LAYER           VARCHAR(50)  NOT NULL,
        FILENAME        VARCHAR(200) NOT NULL,
        FILE_HASH       VARCHAR(64)  NOT NULL,
        APPLIED_AT      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
        DURATION_SEC    FLOAT,
        STATUS          VARCHAR(20)  DEFAULT 'APPLIED',
        UNIQUE (LAYER, FILENAME, FILE_HASH)
    )
    """
    with conn.cursor() as cur:
        cur.execute(ddl)


def get_applied(conn) -> set[tuple[str, str, str]]:
    """Return set of (layer, filename, file_hash) that are already applied."""
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT LAYER, FILENAME, FILE_HASH FROM {MIGRATION_TABLE} WHERE STATUS = 'APPLIED'"
        )
        return {(r[0], r[1], r[2]) for r in cur.fetchall()}


def record_migration(conn, layer: str, filename: str, fhash: str, duration: float):
    with conn.cursor() as cur:
        cur.execute(
            f"""
            INSERT INTO {MIGRATION_TABLE} (LAYER, FILENAME, FILE_HASH, DURATION_SEC, STATUS)
            VALUES (%s, %s, %s, %s, 'APPLIED')
            """,
            (layer, filename, fhash, round(duration, 3)),
        )


def rollback_last_n(conn, n: int):
    """Mark last N migrations as PENDING so they will re-run next time."""
    with conn.cursor() as cur:
        cur.execute(
            f"""
            UPDATE {MIGRATION_TABLE}
            SET STATUS = 'PENDING'
            WHERE MIGRATION_ID IN (
                SELECT MIGRATION_ID FROM {MIGRATION_TABLE}
                WHERE STATUS = 'APPLIED'
                ORDER BY MIGRATION_ID DESC
                LIMIT {n}
            )
            """
        )
        logger.info("Marked last %d migration(s) as PENDING.", n)


def show_status(conn):
    """Print all migration history."""
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT MIGRATION_ID, LAYER, FILENAME, STATUS,
                   APPLIED_AT, DURATION_SEC
            FROM {MIGRATION_TABLE}
            ORDER BY MIGRATION_ID
            """
        )
        rows = cur.fetchall()

    if not rows:
        print("No migrations applied yet.")
        return

    print(f"\n{'ID':>4}  {'LAYER':<14} {'FILE':<35} {'STATUS':<10} {'APPLIED AT':<22} {'SEC':>6}")
    print("-" * 95)
    for mid, layer, fname, status, applied_at, dur in rows:
        dur_s = f"{dur:.1f}" if dur else "—"
        print(f"{mid:>4}  {layer:<14} {fname:<35} {status:<10} {str(applied_at)[:19]:<22} {dur_s:>6}")
    print()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def parse_args():
    p = argparse.ArgumentParser(description="SportZone migration runner")
    p.add_argument("--layers", nargs="*", help="Only run these layers")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--status",  action="store_true", help="Show migration history")
    p.add_argument("--rollback", type=int, metavar="N",
                   help="Mark last N migrations as pending")
    p.add_argument("--force", action="store_true",
                   help="Re-run even if already applied (ignores hash check)")
    return p.parse_args()


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-7s  %(message)s",
        datefmt="%H:%M:%S",
    )
    args = parse_args()

    conn_params = load_config()
    logger.info("Connecting to Snowflake …")
    conn = snowflake.connector.connect(**conn_params)
    logger.info("Connected.\n")

    ensure_migration_table(conn)

    if args.status:
        show_status(conn)
        conn.close()
        return

    if args.rollback:
        rollback_last_n(conn, args.rollback)
        conn.close()
        return

    requested = set(args.layers) if args.layers else {n for n, _ in LAYERS}
    active    = [(n, fs) for n, fs in LAYERS if n in requested]

    applied = get_applied(conn)
    pending_count = 0

    for layer_name, sql_files in active:
        layer_dir = PROJECT_DIR / layer_name
        logger.info("── Layer: %s ──", layer_name)

        for sql_file in sql_files:
            sql_path = layer_dir / sql_file
            if not sql_path.exists():
                logger.warning("  Not found: %s", sql_path)
                continue

            fhash = file_hash(sql_path)
            key   = (layer_name, sql_file, fhash)

            if key in applied and not args.force:
                logger.info("  %-40s  [SKIP — already applied]", sql_file)
                continue

            pending_count += 1
            if args.dry_run:
                logger.info("  %-40s  [DRY-RUN — would apply]", sql_file)
                continue

            logger.info("  %-40s  applying …", sql_file)
            t0 = time.time()
            try:
                ok, err = execute_file(conn, sql_path, stop_on_error=False)
                duration = time.time() - t0
                if err == 0:
                    record_migration(conn, layer_name, sql_file, fhash, duration)
                    logger.info("  %-40s  [OK  — %.1fs  %d stmts]", sql_file, duration, ok)
                else:
                    logger.warning("  %-40s  [WARN — %d errors, not recorded]",
                                   sql_file, err)
            except Exception as exc:
                logger.error("  %-40s  [FAIL — %s]", sql_file, exc)

        logger.info("")

    if pending_count == 0 and not args.dry_run:
        logger.info("Everything is up to date. Nothing to migrate.")

    conn.close()


if __name__ == "__main__":
    main()
