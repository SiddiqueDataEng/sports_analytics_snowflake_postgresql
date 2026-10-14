-- =============================================================================
-- SportZone Retail Analytics Platform
-- 06_governance / 01_masking_policies.sql
-- Purpose : Dynamic data masking for PII columns
-- Run as  : ACCOUNTADMIN (create), SPORTZONE_MASKING_ADMIN (apply)
-- =============================================================================

-- Step 1: Grant masking privileges (run as ACCOUNTADMIN)
USE ROLE ACCOUNTADMIN;
GRANT CREATE MASKING POLICY ON SCHEMA SPORTZONE_DEV.STAGING  TO ROLE SPORTZONE_MASKING_ADMIN;
GRANT CREATE MASKING POLICY ON SCHEMA SPORTZONE_DEV.WAREHOUSE TO ROLE SPORTZONE_MASKING_ADMIN;
GRANT APPLY  MASKING POLICY ON ACCOUNT                        TO ROLE SPORTZONE_MASKING_ADMIN;

-- Step 2: Create masking policies (run as SPORTZONE_MASKING_ADMIN)
USE ROLE SPORTZONE_MASKING_ADMIN;
USE DATABASE SPORTZONE_DEV;
USE SCHEMA STAGING;

-- ---------------------------------------------------------------------------
-- Email masking: full email visible to ACCOUNTADMIN / DATA_ENG only
-- ---------------------------------------------------------------------------

CREATE OR REPLACE MASKING POLICY mask_email
AS (val STRING) RETURNS STRING ->
    CASE
        WHEN CURRENT_ROLE() IN ('ACCOUNTADMIN', 'SPORTZONE_DATA_ENG')
            THEN val
        WHEN CURRENT_ROLE() = 'SPORTZONE_ANALYST'
            THEN REGEXP_REPLACE(val, '^(.{2}).*(@.*)$', '\\1***\\2')
        ELSE '***@***.***'
    END
COMMENT = 'Mask email – full to admins, partial to analysts, hidden to others';

-- ---------------------------------------------------------------------------
-- Phone masking
-- ---------------------------------------------------------------------------

CREATE OR REPLACE MASKING POLICY mask_phone
AS (val STRING) RETURNS STRING ->
    CASE
        WHEN CURRENT_ROLE() IN ('ACCOUNTADMIN', 'SPORTZONE_DATA_ENG')
            THEN val
        ELSE REGEXP_REPLACE(val, '\\d(?=\\d{4})', '*')
    END
COMMENT = 'Mask phone – last 4 digits visible to analysts';

-- ---------------------------------------------------------------------------
-- Full PII masking (used for SSN-like fields in account tables)
-- ---------------------------------------------------------------------------

CREATE OR REPLACE MASKING POLICY mask_full_pii
AS (val STRING) RETURNS STRING ->
    CASE
        WHEN CURRENT_ROLE() = 'ACCOUNTADMIN' THEN val
        ELSE '**REDACTED**'
    END
COMMENT = 'Full redaction for highly sensitive fields';

-- ---------------------------------------------------------------------------
-- Date of birth – return only year to non-admins
-- ---------------------------------------------------------------------------

CREATE OR REPLACE MASKING POLICY mask_dob
AS (val DATE) RETURNS DATE ->
    CASE
        WHEN CURRENT_ROLE() IN ('ACCOUNTADMIN', 'SPORTZONE_DATA_ENG')
            THEN val
        ELSE DATE_FROM_PARTS(YEAR(val), 1, 1)   -- return Jan 1 of birth year
    END
COMMENT = 'DOB – full date to admins, year-only to others';

-- ---------------------------------------------------------------------------
-- Apply policies to STAGING tables
-- ---------------------------------------------------------------------------

-- STG_CUSTOMERS
ALTER TABLE STAGING.STG_CUSTOMERS
    MODIFY COLUMN EMAIL         SET MASKING POLICY mask_email;

ALTER TABLE STAGING.STG_CUSTOMERS
    MODIFY COLUMN PHONE         SET MASKING POLICY mask_phone;

ALTER TABLE STAGING.STG_CUSTOMERS
    MODIFY COLUMN DATE_OF_BIRTH SET MASKING POLICY mask_dob;

-- ---------------------------------------------------------------------------
-- Apply policies to WAREHOUSE dim table
-- ---------------------------------------------------------------------------

ALTER TABLE WAREHOUSE.DIM_CUSTOMER
    MODIFY COLUMN EMAIL         SET MASKING POLICY mask_email;

ALTER TABLE WAREHOUSE.DIM_CUSTOMER
    MODIFY COLUMN PHONE         SET MASKING POLICY mask_phone;

ALTER TABLE WAREHOUSE.DIM_CUSTOMER
    MODIFY COLUMN DATE_OF_BIRTH SET MASKING POLICY mask_dob;

-- ---------------------------------------------------------------------------
-- Validate – switch roles to test masking
-- ---------------------------------------------------------------------------

USE ROLE SPORTZONE_DATA_ENG;
USE DATABASE SPORTZONE_DEV;
USE WAREHOUSE DEV_QUERY_WH;
SELECT CUST_ID, FULL_NAME, EMAIL, PHONE, DATE_OF_BIRTH
FROM STAGING.STG_CUSTOMERS LIMIT 5;
-- Expected: unmasked (DATA_ENG role)

USE ROLE SPORTZONE_ANALYST;
SELECT CUST_ID, FULL_NAME, EMAIL, PHONE, DATE_OF_BIRTH
FROM SPORTZONE_DEV.STAGING.STG_CUSTOMERS LIMIT 5;
-- Expected: email partially masked, phone last-4 visible, DOB = Jan 1

USE ROLE ACCOUNTADMIN;
SELECT CUST_ID, FULL_NAME, EMAIL, PHONE, DATE_OF_BIRTH
FROM SPORTZONE_DEV.STAGING.STG_CUSTOMERS LIMIT 5;
-- Expected: full data visible
