-- =============================================================================
-- SportZone Retail Analytics Platform
-- 00_setup / 01_account_setup.sql
-- Purpose : Roles, users, and warehouses
-- Run as  : ACCOUNTADMIN
-- =============================================================================

USE ROLE ACCOUNTADMIN;

-- ---------------------------------------------------------------------------
-- ROLES
-- ---------------------------------------------------------------------------

-- Platform roles
CREATE ROLE IF NOT EXISTS SPORTZONE_SYSADMIN   COMMENT = 'Top-level admin for SportZone objects';
CREATE ROLE IF NOT EXISTS SPORTZONE_DATA_ENG   COMMENT = 'Data engineers – build & run ELT pipelines';
CREATE ROLE IF NOT EXISTS SPORTZONE_ANALYST    COMMENT = 'Analysts – read mart & reporting layer';
CREATE ROLE IF NOT EXISTS SPORTZONE_MASKING_ADMIN COMMENT = 'Manages masking & tagging policies';
CREATE ROLE IF NOT EXISTS SPORTZONE_SHARE_ADMIN   COMMENT = 'Manages outbound data shares';

-- Role hierarchy (child → parent)
GRANT ROLE SPORTZONE_DATA_ENG     TO ROLE SPORTZONE_SYSADMIN;
GRANT ROLE SPORTZONE_ANALYST      TO ROLE SPORTZONE_SYSADMIN;
GRANT ROLE SPORTZONE_MASKING_ADMIN TO ROLE SPORTZONE_SYSADMIN;
GRANT ROLE SPORTZONE_SHARE_ADMIN   TO ROLE SPORTZONE_SYSADMIN;
GRANT ROLE SPORTZONE_SYSADMIN     TO ROLE SYSADMIN;

-- ---------------------------------------------------------------------------
-- WAREHOUSES
-- ---------------------------------------------------------------------------

-- DEV warehouses (small, auto-suspend in 60s)
CREATE OR REPLACE WAREHOUSE DEV_LOAD_WH
    WAREHOUSE_SIZE = 'X-SMALL'
    AUTO_SUSPEND   = 60
    AUTO_RESUME    = TRUE
    COMMENT        = 'DEV – ELT loading';

CREATE OR REPLACE WAREHOUSE DEV_QUERY_WH
    WAREHOUSE_SIZE = 'X-SMALL'
    AUTO_SUSPEND   = 60
    AUTO_RESUME    = TRUE
    COMMENT        = 'DEV – analyst queries';

-- PROD warehouses
CREATE OR REPLACE WAREHOUSE PROD_LOAD_WH
    WAREHOUSE_SIZE = 'SMALL'
    AUTO_SUSPEND   = 120
    AUTO_RESUME    = TRUE
    COMMENT        = 'PROD – ELT loading';

CREATE OR REPLACE WAREHOUSE PROD_QUERY_WH
    WAREHOUSE_SIZE = 'SMALL'
    AUTO_SUSPEND   = 120
    AUTO_RESUME    = TRUE
    COMMENT        = 'PROD – analyst queries';

-- ---------------------------------------------------------------------------
-- USERS
-- ---------------------------------------------------------------------------

CREATE USER IF NOT EXISTS sz_data_engineer
    PASSWORD            = 'SportZone_Eng#2024'
    DEFAULT_ROLE        = SPORTZONE_DATA_ENG
    DEFAULT_WAREHOUSE   = DEV_LOAD_WH
    MUST_CHANGE_PASSWORD = FALSE
    COMMENT             = 'Data Engineer service account';

CREATE USER IF NOT EXISTS sz_analyst
    PASSWORD            = 'SportZone_Ana#2024'
    DEFAULT_ROLE        = SPORTZONE_ANALYST
    DEFAULT_WAREHOUSE   = DEV_QUERY_WH
    MUST_CHANGE_PASSWORD = FALSE
    COMMENT             = 'Analyst service account';

-- Assign roles to users
GRANT ROLE SPORTZONE_DATA_ENG  TO USER sz_data_engineer;
GRANT ROLE SPORTZONE_ANALYST   TO USER sz_analyst;

-- ---------------------------------------------------------------------------
-- WAREHOUSE GRANTS
-- ---------------------------------------------------------------------------

GRANT USAGE ON WAREHOUSE DEV_LOAD_WH   TO ROLE SPORTZONE_DATA_ENG;
GRANT USAGE ON WAREHOUSE DEV_QUERY_WH  TO ROLE SPORTZONE_DATA_ENG;
GRANT USAGE ON WAREHOUSE DEV_QUERY_WH  TO ROLE SPORTZONE_ANALYST;
GRANT USAGE ON WAREHOUSE PROD_LOAD_WH  TO ROLE SPORTZONE_DATA_ENG;
GRANT USAGE ON WAREHOUSE PROD_QUERY_WH TO ROLE SPORTZONE_ANALYST;
