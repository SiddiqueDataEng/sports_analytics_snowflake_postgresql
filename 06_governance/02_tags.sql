-- =============================================================================
-- SportZone Retail Analytics Platform
-- 06_governance / 02_tags.sql
-- Purpose : Object tagging for data classification and PII tracking
-- Run as  : ACCOUNTADMIN
-- =============================================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE SPORTZONE_DEV;

-- ---------------------------------------------------------------------------
-- Create classification tags
-- ---------------------------------------------------------------------------

CREATE TAG IF NOT EXISTS SPORTZONE_DEV.STAGING.PII
    COMMENT = 'Personally Identifiable Information – GDPR/CCPA applicable';

CREATE TAG IF NOT EXISTS SPORTZONE_DEV.STAGING.FINANCIAL
    COMMENT = 'Financial data – revenue, costs, pricing';

CREATE TAG IF NOT EXISTS SPORTZONE_DEV.STAGING.SENSITIVE
    COMMENT = 'General sensitive data requiring access control review';

CREATE TAG IF NOT EXISTS SPORTZONE_DEV.STAGING.DATA_DOMAIN
    ALLOWED_VALUES = 'CUSTOMER', 'PRODUCT', 'ORDER', 'INVENTORY', 'PROMOTION'
    COMMENT = 'Business domain classification for data catalog';

-- ---------------------------------------------------------------------------
-- Tag masking policies to PII tag
-- ---------------------------------------------------------------------------

ALTER TAG SPORTZONE_DEV.STAGING.PII
    SET MASKING POLICY SPORTZONE_DEV.STAGING.mask_email;

-- ---------------------------------------------------------------------------
-- Apply tags to tables
-- ---------------------------------------------------------------------------

-- Customer table – PII domain
ALTER TABLE SPORTZONE_DEV.STAGING.STG_CUSTOMERS
    SET TAG SPORTZONE_DEV.STAGING.DATA_DOMAIN  = 'CUSTOMER',
            SPORTZONE_DEV.STAGING.SENSITIVE     = 'Contains PII – handle per privacy policy';

-- Tag individual PII columns
ALTER TABLE SPORTZONE_DEV.STAGING.STG_CUSTOMERS
    MODIFY COLUMN EMAIL         SET TAG SPORTZONE_DEV.STAGING.PII = 'email-address';

ALTER TABLE SPORTZONE_DEV.STAGING.STG_CUSTOMERS
    MODIFY COLUMN PHONE         SET TAG SPORTZONE_DEV.STAGING.PII = 'phone-number';

ALTER TABLE SPORTZONE_DEV.STAGING.STG_CUSTOMERS
    MODIFY COLUMN DATE_OF_BIRTH SET TAG SPORTZONE_DEV.STAGING.PII = 'date-of-birth';

-- Product table
ALTER TABLE SPORTZONE_DEV.STAGING.STG_PRODUCTS
    SET TAG SPORTZONE_DEV.STAGING.DATA_DOMAIN  = 'PRODUCT',
            SPORTZONE_DEV.STAGING.FINANCIAL     = 'Contains pricing and cost data';

ALTER TABLE SPORTZONE_DEV.STAGING.STG_PRODUCTS
    MODIFY COLUMN UNIT_PRICE    SET TAG SPORTZONE_DEV.STAGING.FINANCIAL = 'retail-price';

ALTER TABLE SPORTZONE_DEV.STAGING.STG_PRODUCTS
    MODIFY COLUMN UNIT_COST     SET TAG SPORTZONE_DEV.STAGING.FINANCIAL = 'cost-price – confidential';

-- Orders table
ALTER TABLE SPORTZONE_DEV.STAGING.STG_ORDERS
    SET TAG SPORTZONE_DEV.STAGING.DATA_DOMAIN  = 'ORDER',
            SPORTZONE_DEV.STAGING.FINANCIAL     = 'Transaction financial data';

-- Warehouse DIM_CUSTOMER
ALTER TABLE SPORTZONE_DEV.WAREHOUSE.DIM_CUSTOMER
    SET TAG SPORTZONE_DEV.STAGING.DATA_DOMAIN = 'CUSTOMER',
            SPORTZONE_DEV.STAGING.PII         = 'Contains PII – SCD2 history';

ALTER TABLE SPORTZONE_DEV.WAREHOUSE.DIM_CUSTOMER
    MODIFY COLUMN EMAIL         SET TAG SPORTZONE_DEV.STAGING.PII = 'email-address';
ALTER TABLE SPORTZONE_DEV.WAREHOUSE.DIM_CUSTOMER
    MODIFY COLUMN PHONE         SET TAG SPORTZONE_DEV.STAGING.PII = 'phone-number';

-- ---------------------------------------------------------------------------
-- Query tag assignments for governance audit
-- ---------------------------------------------------------------------------

-- All PII-tagged columns in the account
SELECT
    TAG_DATABASE,
    TAG_SCHEMA,
    TAG_NAME,
    OBJECT_DATABASE,
    OBJECT_SCHEMA,
    OBJECT_NAME,
    COLUMN_NAME,
    TAG_VALUE
FROM TABLE(SPORTZONE_DEV.INFORMATION_SCHEMA.TAG_REFERENCES_ALL_COLUMNS(
    'SPORTZONE_DEV.STAGING.STG_CUSTOMERS', 'table'
));

-- All tags on a specific table
SELECT *
FROM TABLE(SPORTZONE_DEV.INFORMATION_SCHEMA.TAG_REFERENCES(
    'SPORTZONE_DEV.STAGING.STG_CUSTOMERS', 'TABLE'
));
