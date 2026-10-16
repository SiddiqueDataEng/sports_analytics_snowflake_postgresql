"""
run_all.py
==========
Single entry-point to run the complete SportZone pipeline:

    1. Generate OLTP data  (Neon Postgres)
    2. Run ETL pipeline    (Neon → Snowflake DWH)
    3. Print final counts

Usage
-----
    # Full demo: generate + ETL + load into Snowflake
    python run_all.py

    # Only generate OLTP data (no Snowflake)
    python run_all.py --generate-only

    # Only run ETL (data already generated)
    python run_all.py --etl-only

    # Generate 500 customers / 3000 orders then ETL
    python run_all.py --customers 500 --orders 3000

    # Scheduled: generate new orders every 5 min then ETL
    python run_all.py --schedule 5 --batch 50
"""

import argparse
import logging
import subprocess
import sys
import time
from pathlib import Path

PROJECT_DIR = Path(__file__).parent
sys.path.insert(0, str(PROJECT_DIR / "deploy"))
sys.path.insert(0, str(PROJECT_DIR / "etl"))

logger = logging.getLogger(__name__)


def run_cmd(cmd: list[str], label: str) -> bool:
    logger.info("── %s ──", label)
    result = subprocess.run(cmd, cwd=str(PROJECT_DIR))
    if result.returncode not in (0, 1):   # 1 = unicode stderr noise on Windows
        logger.error("%s failed (exit %d)", label, result.returncode)
        return False
    return True


def generate(customers: int = 300, orders: int = 1500,
             events: int = 2000, batch: bool = False,
             batch_size: int = 50):
    cmd = [
        sys.executable, "oltp/generator.py",
        "--customers", str(customers),
        "--orders",    str(orders),
        "--events",    str(events),
        "--start-date","2024-01-01",
    ]
    return run_cmd(cmd, f"OLTP Generate ({customers} customers, {orders} orders)")


def run_etl(full_load: bool = False, dry_run: bool = False):
    cmd = [sys.executable, "etl/pipeline.py"]
    if full_load:
        cmd.append("--full-load")
    if dry_run:
        cmd.append("--dry-run")
    return run_cmd(cmd, "ETL Pipeline (Neon → Snowflake)")


def parse_args():
    p = argparse.ArgumentParser(description="SportZone end-to-end runner")
    p.add_argument("--generate-only", action="store_true")
    p.add_argument("--etl-only",      action="store_true")
    p.add_argument("--customers",     type=int, default=300)
    p.add_argument("--orders",        type=int, default=1500)
    p.add_argument("--events",        type=int, default=2000)
    p.add_argument("--full-load",     action="store_true",
                   help="ETL full reload (ignore watermarks)")
    p.add_argument("--dry-run",       action="store_true",
                   help="ETL dry-run (no Snowflake load)")
    p.add_argument("--schedule",      type=int, default=None,
                   help="Run generate+ETL loop every N minutes")
    p.add_argument("--batch",         type=int, default=50,
                   help="Orders per scheduled batch")
    return p.parse_args()


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-7s  %(message)s",
        datefmt="%H:%M:%S",
    )
    args = parse_args()

    if args.schedule:
        logger.info("Scheduled mode: every %d minutes, %d orders/batch", args.schedule, args.batch)
        iteration = 0
        while True:
            iteration += 1
            logger.info("═" * 55)
            logger.info("Cycle #%d", iteration)
            logger.info("═" * 55)

            if not args.etl_only:
                generate(customers=0, orders=args.batch, events=args.batch * 3)
            if not args.generate_only:
                run_etl(dry_run=args.dry_run)

            logger.info("Sleeping %d minutes …", args.schedule)
            time.sleep(args.schedule * 60)
        return

    logger.info("═" * 55)
    logger.info("SportZone — End-to-End Pipeline")
    logger.info("═" * 55)

    if not args.etl_only:
        ok = generate(args.customers, args.orders, args.events)
        if not ok:
            logger.error("Generation failed. Stopping.")
            sys.exit(1)

    if not args.generate_only:
        ok = run_etl(full_load=args.full_load, dry_run=args.dry_run)
        if not ok:
            logger.error("ETL failed.")
            sys.exit(1)

    logger.info("═" * 55)
    logger.info("Done.")
    logger.info("═" * 55)


if __name__ == "__main__":
    main()
