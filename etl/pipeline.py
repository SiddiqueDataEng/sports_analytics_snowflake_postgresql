"""
pipeline.py
===========
Orchestrates the full ETL pipeline:
    Neon Postgres OLTP  →  Extract  →  Transform  →  Load  →  Snowflake DWH

Usage
-----
    # Single run
    python etl/pipeline.py

    # Full reload (ignore watermarks)
    python etl/pipeline.py --full-load

    # Specific tables only
    python etl/pipeline.py --tables customers orders order_items

    # Continuous scheduler (every N minutes)
    python etl/pipeline.py --schedule 30

    # Dry-run (extract + transform only, skip Snowflake load)
    python etl/pipeline.py --dry-run

Pipeline DAG
------------
    [OLTP Neon Postgres]
           │
      extract.py          ← incremental via watermarks
           │  Parquet files  (etl/staging/<table>/<ts>.parquet)
           ▼
      transform.py         ← cleanse, type-cast, derive columns
           │  DataFrames
           ▼
        load.py            ← write_pandas into RAW + STAGING
           │
           ▼
     Snowflake MERGE       ← DIM SCD2 + FACT incremental merge
           │
           ▼
    [SPORTZONE_DEV warehouse layer]
           │
           ▼
    [MART views — always fresh]
"""

import argparse
import logging
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

PROJECT_DIR = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_DIR / "etl"))
sys.path.insert(0, str(PROJECT_DIR / "deploy"))

from extract   import run_extract
from transform import run_transform
from load      import run_load

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Run log — persisted to etl/pipeline_runs.log
# ---------------------------------------------------------------------------

RUN_LOG = PROJECT_DIR / "etl" / "pipeline_runs.log"


def log_run(status: str, tables: list, duration: float, rows: dict):
    entry = {
        "ts":       datetime.now(timezone.utc).isoformat(),
        "status":   status,
        "tables":   tables,
        "duration": round(duration, 2),
        "rows":     rows,
    }
    with open(RUN_LOG, "a", encoding="utf-8") as f:
        import json
        f.write(json.dumps(entry) + "\n")


# ---------------------------------------------------------------------------
# Single pipeline run
# ---------------------------------------------------------------------------

def run_pipeline(
    tables: list[str] = None,
    full_load: bool   = False,
    dry_run: bool     = False,
) -> bool:
    """
    Execute one full ETL cycle.
    Returns True if successful.
    """
    start = time.time()
    run_ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    logger.info("━" * 60)
    logger.info("Pipeline run  [%s]", run_ts)
    logger.info("  full_load=%s  dry_run=%s  tables=%s",
                full_load, dry_run, tables or "ALL")
    logger.info("━" * 60)

    try:
        # ── EXTRACT ─────────────────────────────────────────────
        extracted = run_extract(tables=tables, full_load=full_load)

        if not extracted:
            logger.info("Nothing new to process. Pipeline complete.")
            log_run("NOOP", tables or [], time.time()-start, {})
            return True

        # ── TRANSFORM ───────────────────────────────────────────
        transformed = run_transform(extracted)

        if not transformed:
            logger.warning("Transform produced no output. Check extract data.")
            log_run("WARN", list(extracted.keys()), time.time()-start, {})
            return True

        if dry_run:
            logger.info("[DRY-RUN] Skipping Snowflake load.")
            for tbl, df in transformed.items():
                logger.info("  Would load %-40s  %d rows", tbl, len(df))
            log_run("DRY-RUN", list(extracted.keys()), time.time()-start, {})
            return True

        # ── LOAD ────────────────────────────────────────────────
        row_counts = run_load(transformed)

        duration = time.time() - start
        total_rows = sum(v for v in row_counts.values() if v > 0)

        logger.info("━" * 60)
        logger.info("Pipeline complete in %.1fs  |  %d tables  |  %d rows total",
                    duration, len(row_counts), total_rows)
        logger.info("━" * 60)

        log_run("OK", list(extracted.keys()), duration, row_counts)
        return True

    except Exception as exc:
        duration = time.time() - start
        logger.error("Pipeline FAILED after %.1fs: %s", duration, exc, exc_info=True)
        log_run("FAILED", tables or [], duration, {})
        return False


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args():
    p = argparse.ArgumentParser(description="SportZone ETL pipeline")
    p.add_argument("--tables",    nargs="*",
                   help="Specific tables to process (default: all)")
    p.add_argument("--full-load", action="store_true",
                   help="Ignore watermarks and reload everything")
    p.add_argument("--dry-run",   action="store_true",
                   help="Extract+transform only — do not load into Snowflake")
    p.add_argument("--schedule",  type=int, default=None,
                   help="Run continuously every N minutes")
    p.add_argument("--history",   action="store_true",
                   help="Print recent pipeline run history")
    return p.parse_args()


def print_history():
    if not RUN_LOG.exists():
        print("No pipeline runs recorded yet.")
        return
    import json
    lines = RUN_LOG.read_text().strip().splitlines()[-20:]
    print(f"\n{'Timestamp':<24} {'Status':<10} {'Dur(s)':>6} {'Rows':>8}  Tables")
    print("-" * 80)
    for line in lines:
        try:
            e = json.loads(line)
            tables_str = ",".join(e.get("tables",[]))[:35]
            rows = sum(v for v in e.get("rows",{}).values() if isinstance(v,int) and v>0)
            print(f"  {e['ts'][:19]:<24} {e['status']:<10} {e['duration']:>6} {rows:>8}  {tables_str}")
        except Exception:
            pass
    print()


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-7s  %(message)s",
        datefmt="%H:%M:%S",
    )
    args = parse_args()

    if args.history:
        print_history()
        return

    if args.schedule:
        logger.info("Scheduled mode: every %d minutes. Ctrl+C to stop.", args.schedule)
        iteration = 0
        while True:
            iteration += 1
            logger.info("─── Scheduled run #%d ───", iteration)
            run_pipeline(
                tables=args.tables,
                full_load=args.full_load and iteration == 1,
                dry_run=args.dry_run,
            )
            logger.info("Next run in %d minutes …", args.schedule)
            time.sleep(args.schedule * 60)
    else:
        ok = run_pipeline(
            tables=args.tables,
            full_load=args.full_load,
            dry_run=args.dry_run,
        )
        sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
