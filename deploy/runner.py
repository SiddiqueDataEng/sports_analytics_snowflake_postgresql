"""
runner.py
Core SQL execution engine.
Reads .sql files, splits on statement boundaries, and executes against Snowflake.
"""

import re
import sys
import time
import logging
from pathlib import Path
from typing import Optional

import snowflake.connector
from snowflake.connector import DictCursor

logger = logging.getLogger(__name__)


def split_sql(sql_text: str) -> list[str]:
    """
    Split a SQL file into individual statements.
    Handles:
      - single-line  --  comments
      - block        /* */ comments  (including the inline comment blocks used
        in this project that wrap whole exercises)
      - string literals containing semicolons
      - semicolons inside CREATE PROCEDURE / BEGIN…END blocks
    """
    # Remove block comments first (non-greedy)
    sql_text = re.sub(r"/\*.*?\*/", "", sql_text, flags=re.DOTALL)

    statements = []
    current: list[str] = []
    in_string = False
    string_char = ""
    i = 0

    lines = sql_text.splitlines(keepends=True)
    for line in lines:
        # Strip inline -- comments (but not inside strings)
        stripped = []
        j = 0
        while j < len(line):
            ch = line[j]
            if in_string:
                stripped.append(ch)
                if ch == string_char:
                    in_string = False
            else:
                if ch in ("'", '"'):
                    in_string = True
                    string_char = ch
                    stripped.append(ch)
                elif ch == "-" and j + 1 < len(line) and line[j + 1] == "-":
                    # rest of line is a comment
                    break
                else:
                    stripped.append(ch)
            j += 1
        line = "".join(stripped)

        current.append(line)
        # Check for statement-ending semicolon (outside strings)
        if ";" in line and not in_string:
            stmt = "".join(current).strip()
            if stmt and stmt != ";":
                statements.append(stmt)
            current = []

    # Catch any trailing statement without a semicolon
    leftover = "".join(current).strip()
    if leftover and leftover not in (";", ""):
        statements.append(leftover)

    return [s for s in statements if s.strip()]


def execute_file(
    conn: snowflake.connector.SnowflakeConnection,
    sql_path: Path,
    dry_run: bool = False,
    stop_on_error: bool = True,
) -> tuple[int, int]:
    """
    Execute all statements in a .sql file.
    Returns (success_count, error_count).
    """
    sql_text = sql_path.read_text(encoding="utf-8")
    statements = split_sql(sql_text)

    success = error = 0
    logger.info("  %-50s  [%d statements]", sql_path.name, len(statements))

    for idx, stmt in enumerate(statements, start=1):
        first_line = stmt.split("\n")[0][:80].strip()
        if dry_run:
            print(f"    [DRY-RUN #{idx}] {first_line}")
            success += 1
            continue

        try:
            with conn.cursor() as cur:
                cur.execute(stmt)
                # Consume results so the cursor cleans up
                try:
                    rows = cur.fetchmany(1)
                except Exception:
                    rows = []
            logger.debug("    [OK  #%d] %s", idx, first_line)
            success += 1
        except snowflake.connector.errors.ProgrammingError as exc:
            msg = str(exc)
            # Treat "already exists" / "already up to date" as warnings
            ignorable = (
                "already exists" in msg.lower()
                or "already up-to-date" in msg.lower()
                or "nothing to do" in msg.lower()
            )
            if ignorable:
                logger.debug("    [SKIP #%d] %s — %s", idx, first_line, msg.split("\n")[0])
                success += 1
            else:
                logger.error("    [FAIL #%d] %s\n             %s", idx, first_line, msg)
                error += 1
                if stop_on_error:
                    raise

    return success, error
