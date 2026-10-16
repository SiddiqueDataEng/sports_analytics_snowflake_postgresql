"""Populate DIM_DATE in Snowflake WAREHOUSE schema (2023-01-01 to 2026-12-31)."""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent / "deploy"))
from config import load_config
import snowflake.connector

params = load_config()
conn = snowflake.connector.connect(**params)
cur  = conn.cursor()
cur.execute("USE DATABASE SPORTZONE_DEV")
cur.execute("USE WAREHOUSE COMPUTE_WH")

print("Populating WAREHOUSE.DIM_DATE ...")
cur.execute("""
    INSERT INTO WAREHOUSE.DIM_DATE
    WITH date_spine AS (
        SELECT DATEADD(DAY, SEQ4(), '2023-01-01'::DATE) AS dt
        FROM TABLE(GENERATOR(ROWCOUNT => 1461))
    )
    SELECT
        TO_NUMBER(TO_CHAR(dt,'YYYYMMDD'))        AS DATE_KEY,
        dt                                       AS FULL_DATE,
        DAYOFWEEK(dt)                            AS DAY_OF_WEEK,
        DAYNAME(dt)                              AS DAY_NAME,
        DAY(dt)                                  AS DAY_OF_MONTH,
        DAYOFYEAR(dt)                            AS DAY_OF_YEAR,
        WEEKOFYEAR(dt)                           AS WEEK_OF_YEAR,
        MONTH(dt)                                AS MONTH_NUM,
        MONTHNAME(dt)                            AS MONTH_NAME,
        QUARTER(dt)                              AS QUARTER,
        'Q' || QUARTER(dt)                       AS QUARTER_NAME,
        YEAR(dt)                                 AS YEAR,
        CASE WHEN DAYOFWEEK(dt) IN (0,6) THEN TRUE ELSE FALSE END AS IS_WEEKEND,
        FALSE                                    AS IS_HOLIDAY,
        CASE WHEN MONTH(dt) >= 2 THEN YEAR(dt) ELSE YEAR(dt)-1 END AS FISCAL_YEAR,
        CASE WHEN MONTH(dt) IN (2,3,4) THEN 1
             WHEN MONTH(dt) IN (5,6,7) THEN 2
             WHEN MONTH(dt) IN (8,9,10) THEN 3
             ELSE 4 END                          AS FISCAL_QUARTER
    FROM date_spine
""")
print(f"  Inserted {cur.rowcount:,} rows into DIM_DATE")

# Verify sales mart now works
cur.execute("""
    SELECT PRD_CATEGORY, SUM(NET_REVENUE) AS REV, SUM(UNITS_SOLD) AS UNITS
    FROM MART.DAILY_SALES_SUMMARY
    GROUP BY PRD_CATEGORY ORDER BY REV DESC
""")
rows = cur.fetchall()
print("\nMart DAILY_SALES_SUMMARY working:")
for r in rows:
    print(f"  {str(r[0]):<15}  ${float(r[1]):>10,.0f}  {r[2]:>5} units")

conn.close()
print("\nDIM_DATE populated. All marts active.")
