-- =============================================================================
-- SportZone Retail Analytics Platform
-- 00_setup / 02_database_setup.sql
-- Purpose : Databases and schemas for DEV and PROD environments
-- Run as  : ACCOUNTADMIN
-- =============================================================================

USE ROLE ACCOUNTADMIN;

-- ---------------------------------------------------------------------------
-- DATABASES
-- ---------------------------------------------------------------------------

CREATE DATABASE IF NOT EXISTS SPORTZONE_DEV
    COMMENT = 'Development environment for SportZone retail platform';

CREATE DATABASE IF NOT EXISTS SPORTZONE_PROD
    COMMENT = 'Production environment for SportZone retail platform';

-- ---------------------------------------------------------------------------
-- SCHEMAS — DEV
-- ---------------------------------------------------------------------------

USE DATABASE SPORTZONE_DEV;

CREATE SCHEMA IF NOT EXISTS RAW       COMMENT = 'Raw landing zone – data as-is from sources';
CREATE SCHEMA IF NOT EXISTS STAGING   COMMENT = 'Cleansed and conformed data';
CREATE SCHEMA IF NOT EXISTS WAREHOUSE COMMENT = 'Star schema – dimensions and facts';
CREATE SCHEMA IF NOT EXISTS MART      COMMENT = 'Business-specific aggregated views';
CREATE SCHEMA IF NOT EXISTS REPORTING COMMENT = 'Secure views shared with consumers';

-- ---------------------------------------------------------------------------
-- SCHEMAS — PROD
-- ---------------------------------------------------------------------------

USE DATABASE SPORTZONE_PROD;

CREATE SCHEMA IF NOT EXISTS RAW       COMMENT = 'Raw landing zone';
CREATE SCHEMA IF NOT EXISTS STAGING   COMMENT = 'Cleansed and conformed data';
CREATE SCHEMA IF NOT EXISTS WAREHOUSE COMMENT = 'Star schema';
CREATE SCHEMA IF NOT EXISTS MART      COMMENT = 'Business marts';
CREATE SCHEMA IF NOT EXISTS REPORTING COMMENT = 'Secure views';

-- ---------------------------------------------------------------------------
-- DATABASE-LEVEL GRANTS
-- ---------------------------------------------------------------------------

-- DEV – data engineers can create/manage objects
GRANT ALL PRIVILEGES ON DATABASE SPORTZONE_DEV  TO ROLE SPORTZONE_DATA_ENG;
GRANT ALL PRIVILEGES ON ALL SCHEMAS IN DATABASE SPORTZONE_DEV TO ROLE SPORTZONE_DATA_ENG;

-- DEV – analysts get read-only on MART and REPORTING
GRANT USAGE ON DATABASE SPORTZONE_DEV TO ROLE SPORTZONE_ANALYST;
GRANT USAGE ON SCHEMA SPORTZONE_DEV.MART      TO ROLE SPORTZONE_ANALYST;
GRANT USAGE ON SCHEMA SPORTZONE_DEV.REPORTING TO ROLE SPORTZONE_ANALYST;
GRANT SELECT ON ALL TABLES IN SCHEMA SPORTZONE_DEV.MART      TO ROLE SPORTZONE_ANALYST;
GRANT SELECT ON ALL VIEWS  IN SCHEMA SPORTZONE_DEV.REPORTING TO ROLE SPORTZONE_ANALYST;

-- PROD
GRANT ALL PRIVILEGES ON DATABASE SPORTZONE_PROD TO ROLE SPORTZONE_DATA_ENG;
GRANT ALL PRIVILEGES ON ALL SCHEMAS IN DATABASE SPORTZONE_PROD TO ROLE SPORTZONE_DATA_ENG;
GRANT USAGE ON DATABASE SPORTZONE_PROD TO ROLE SPORTZONE_ANALYST;
GRANT USAGE ON SCHEMA SPORTZONE_PROD.MART      TO ROLE SPORTZONE_ANALYST;
GRANT USAGE ON SCHEMA SPORTZONE_PROD.REPORTING TO ROLE SPORTZONE_ANALYST;
GRANT SELECT ON ALL TABLES IN SCHEMA SPORTZONE_PROD.MART      TO ROLE SPORTZONE_ANALYST;
GRANT SELECT ON ALL VIEWS  IN SCHEMA SPORTZONE_PROD.REPORTING TO ROLE SPORTZONE_ANALYST;

-- Masking admin
GRANT CREATE MASKING POLICY ON SCHEMA SPORTZONE_DEV.RAW     TO ROLE SPORTZONE_MASKING_ADMIN;
GRANT CREATE MASKING POLICY ON SCHEMA SPORTZONE_DEV.STAGING TO ROLE SPORTZONE_MASKING_ADMIN;
GRANT APPLY MASKING POLICY ON ACCOUNT                       TO ROLE SPORTZONE_MASKING_ADMIN;
