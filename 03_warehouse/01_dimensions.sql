-- =============================================================================
-- SportZone Retail Analytics Platform
-- 03_warehouse / 01_dimensions.sql
-- Purpose : Star schema dimension tables (SCD Type 1 & Type 2)
-- Run as  : SPORTZONE_DATA_ENG
-- =============================================================================

USE ROLE SPORTZONE_DATA_ENG;
USE DATABASE SPORTZONE_DEV;
USE SCHEMA WAREHOUSE;
USE WAREHOUSE DEV_LOAD_WH;

-- ---------------------------------------------------------------------------
-- DIM_DATE  (static calendar table – pre-populated)
-- ---------------------------------------------------------------------------

CREATE OR REPLACE TABLE DIM_DATE (
    DATE_KEY        INTEGER         NOT NULL PRIMARY KEY,  -- YYYYMMDD
    FULL_DATE       DATE            NOT NULL,
    DAY_OF_WEEK     INTEGER,                               -- 1=Sun … 7=Sat
    DAY_NAME        VARCHAR(10),
    DAY_OF_MONTH    INTEGER,
    DAY_OF_YEAR     INTEGER,
    WEEK_OF_YEAR    INTEGER,
    MONTH_NUM       INTEGER,
    MONTH_NAME      VARCHAR(10),
    QUARTER         INTEGER,
    QUARTER_NAME    VARCHAR(6),
    YEAR            INTEGER,
    IS_WEEKEND      BOOLEAN,
    IS_HOLIDAY      BOOLEAN DEFAULT FALSE,
    FISCAL_YEAR     INTEGER,                               -- Feb–Jan fiscal year
    FISCAL_QUARTER  INTEGER
);

-- Populate 2023-01-01 to 2026-12-31
INSERT INTO DIM_DATE
WITH date_spine AS (
    SELECT DATEADD(DAY, SEQ4(), '2023-01-01') AS dt
    FROM TABLE(GENERATOR(ROWCOUNT => 1461))   -- 4 years
)
SELECT
    TO_NUMBER(TO_CHAR(dt, 'YYYYMMDD'))          AS DATE_KEY,
    dt                                          AS FULL_DATE,
    DAYOFWEEK(dt)                               AS DAY_OF_WEEK,
    DAYNAME(dt)                                 AS DAY_NAME,
    DAY(dt)                                     AS DAY_OF_MONTH,
    DAYOFYEAR(dt)                               AS DAY_OF_YEAR,
    WEEKOFYEAR(dt)                              AS WEEK_OF_YEAR,
    MONTH(dt)                                   AS MONTH_NUM,
    MONTHNAME(dt)                               AS MONTH_NAME,
    QUARTER(dt)                                 AS QUARTER,
    'Q' || QUARTER(dt)                          AS QUARTER_NAME,
    YEAR(dt)                                    AS YEAR,
    CASE WHEN DAYOFWEEK(dt) IN (1,7) THEN TRUE ELSE FALSE END AS IS_WEEKEND,
    FALSE                                       AS IS_HOLIDAY,
    CASE WHEN MONTH(dt) >= 2
         THEN YEAR(dt)
         ELSE YEAR(dt) - 1 END                  AS FISCAL_YEAR,
    CASE WHEN MONTH(dt) IN (2,3,4)   THEN 1
         WHEN MONTH(dt) IN (5,6,7)   THEN 2
         WHEN MONTH(dt) IN (8,9,10)  THEN 3
         ELSE 4 END                             AS FISCAL_QUARTER
FROM date_spine;

-- ---------------------------------------------------------------------------
-- DIM_CUSTOMER  (SCD Type 2 – tracks loyalty tier and address changes)
-- ---------------------------------------------------------------------------

CREATE OR REPLACE TABLE DIM_CUSTOMER (
    CUST_SK         INTEGER         NOT NULL AUTOINCREMENT PRIMARY KEY,
    CUST_ID         VARCHAR(20)     NOT NULL,              -- natural key
    FIRST_NAME      VARCHAR(100),
    LAST_NAME       VARCHAR(100),
    FULL_NAME       VARCHAR(200),
    EMAIL           VARCHAR(255),
    PHONE           VARCHAR(30),
    DATE_OF_BIRTH   DATE,
    AGE_BAND        VARCHAR(20),                           -- derived
    GENDER          VARCHAR(1),
    CITY            VARCHAR(100),
    STATE           VARCHAR(50),
    COUNTRY         VARCHAR(2),
    LOYALTY_TIER    VARCHAR(20),
    SIGNUP_DATE     DATE,
    IS_ACTIVE       BOOLEAN,
    -- SCD2
    EFF_START_DT    DATE            NOT NULL,
    EFF_END_DT      DATE,
    IS_CURRENT      BOOLEAN         DEFAULT TRUE,
    -- Audit
    _LOAD_TS        TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP()
);

-- ---------------------------------------------------------------------------
-- DIM_PRODUCT  (SCD Type 1 – overwrite; Type 2 for price changes)
-- ---------------------------------------------------------------------------

CREATE OR REPLACE TABLE DIM_PRODUCT (
    PRD_SK          INTEGER         NOT NULL AUTOINCREMENT PRIMARY KEY,
    PRD_ID          VARCHAR(20)     NOT NULL,
    PRD_NAME        VARCHAR(255),
    PRD_CATEGORY    VARCHAR(100),
    PRD_SUBCATEGORY VARCHAR(100),
    BRAND           VARCHAR(100),
    SKU             VARCHAR(50),
    UNIT_PRICE      NUMBER(10,2),
    UNIT_COST       NUMBER(10,2),
    GROSS_MARGIN_PCT NUMBER(5,2),
    SUPPLIER_ID     VARCHAR(20),
    PRICE_TIER      VARCHAR(20),    -- BUDGET | MID | PREMIUM
    IS_ACTIVE       BOOLEAN,
    LAUNCH_DATE     DATE,
    DISCONTINUE_DT  DATE,
    -- SCD2
    EFF_START_DT    DATE            NOT NULL DEFAULT CURRENT_DATE(),
    EFF_END_DT      DATE,
    IS_CURRENT      BOOLEAN         DEFAULT TRUE,
    -- Audit
    _LOAD_TS        TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP()
);

-- ---------------------------------------------------------------------------
-- DIM_STORE  (SCD Type 1 – stores rarely change)
-- ---------------------------------------------------------------------------

CREATE OR REPLACE TABLE DIM_STORE (
    STORE_SK        INTEGER         NOT NULL AUTOINCREMENT PRIMARY KEY,
    STORE_ID        VARCHAR(20)     NOT NULL,
    STORE_NAME      VARCHAR(200),
    STORE_TYPE      VARCHAR(20),
    REGION          VARCHAR(50),
    CITY            VARCHAR(100),
    STATE           VARCHAR(50),
    COUNTRY         VARCHAR(2),
    OPEN_DATE       DATE,
    MANAGER_NAME    VARCHAR(200),
    -- Audit
    _LOAD_TS        TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP()
);

-- ---------------------------------------------------------------------------
-- DIM_PROMOTION
-- ---------------------------------------------------------------------------

CREATE OR REPLACE TABLE DIM_PROMOTION (
    PROMO_SK        INTEGER         NOT NULL AUTOINCREMENT PRIMARY KEY,
    PROMO_ID        VARCHAR(20),
    PROMO_CODE      VARCHAR(50),
    PROMO_NAME      VARCHAR(200),
    PROMO_TYPE      VARCHAR(30),
    DISCOUNT_VALUE  NUMBER(10,2),
    START_DATE      DATE,
    END_DATE        DATE,
    MIN_ORDER_AMT   NUMBER(10,2),
    IS_ACTIVE       BOOLEAN,
    -- Audit
    _LOAD_TS        TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP()
);

-- Unknown member rows (for FK integrity)
INSERT INTO DIM_CUSTOMER (CUST_SK, CUST_ID, FULL_NAME, EFF_START_DT, IS_CURRENT)
VALUES (-1, 'UNKNOWN', 'Unknown Customer', '1900-01-01', TRUE);

INSERT INTO DIM_PRODUCT (PRD_SK, PRD_ID, PRD_NAME, EFF_START_DT, IS_CURRENT)
VALUES (-1, 'UNKNOWN', 'Unknown Product', '1900-01-01', TRUE);

INSERT INTO DIM_STORE (STORE_SK, STORE_ID, STORE_NAME)
VALUES (-1, 'UNKNOWN', 'Unknown Store');

INSERT INTO DIM_PROMOTION (PROMO_SK, PROMO_ID, PROMO_CODE, PROMO_NAME)
VALUES (-1, 'NONE', 'NONE', 'No Promotion');
