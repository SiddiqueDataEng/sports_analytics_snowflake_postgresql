-- =============================================================================
-- SportZone Retail Analytics Platform
-- 01_raw / 02_stages.sql
-- Purpose : Internal and external stages for source data loading
-- Run as  : SPORTZONE_DATA_ENG
-- =============================================================================

USE ROLE SPORTZONE_DATA_ENG;
USE DATABASE SPORTZONE_DEV;
USE SCHEMA RAW;
USE WAREHOUSE DEV_LOAD_WH;

-- ---------------------------------------------------------------------------
-- INTERNAL STAGES (no cloud credentials needed – great for dev/testing)
-- ---------------------------------------------------------------------------

-- Customers source files
CREATE STAGE IF NOT EXISTS stg_customers
    FILE_FORMAT = csv_comma_fmt
    COMMENT = 'Customer master data – CSV';

-- Products source files
CREATE STAGE IF NOT EXISTS stg_products
    FILE_FORMAT = csv_comma_fmt
    COMMENT = 'Product catalog – CSV';

-- Orders source files (ERP pipe-delimited)
CREATE STAGE IF NOT EXISTS stg_orders
    FILE_FORMAT = csv_pipe_fmt
    COMMENT = 'Orders from ERP system – pipe CSV';

-- Inventory source files
CREATE STAGE IF NOT EXISTS stg_inventory
    FILE_FORMAT = csv_comma_fmt
    COMMENT = 'Inventory snapshots – CSV';

-- Promotions source files
CREATE STAGE IF NOT EXISTS stg_promotions
    FILE_FORMAT = csv_comma_fmt
    COMMENT = 'Promotions and discounts – CSV';

-- Stores source files
CREATE STAGE IF NOT EXISTS stg_stores
    FILE_FORMAT = csv_comma_fmt
    COMMENT = 'Store master data – CSV';

-- ---------------------------------------------------------------------------
-- EXTERNAL STAGE (AWS S3 – production source)
-- NOTE: Replace ARN and bucket path with your actual AWS details
--       before running in a real account.
-- ---------------------------------------------------------------------------

/*
-- Step 1: Create storage integration (run once as ACCOUNTADMIN)
USE ROLE ACCOUNTADMIN;

CREATE STORAGE INTEGRATION sportzone_s3_int
    TYPE                      = EXTERNAL_STAGE
    STORAGE_PROVIDER          = 'S3'
    STORAGE_AWS_ROLE_ARN      = 'arn:aws:iam::123456789012:role/snowflake-sportzone-role'
    ENABLED                   = TRUE
    STORAGE_ALLOWED_LOCATIONS = ('s3://sportzone-datalake/raw/');

-- Step 2: Get the IAM values to trust in AWS
DESC INTEGRATION sportzone_s3_int;

-- Step 3: Create external stage
USE ROLE SPORTZONE_DATA_ENG;
USE DATABASE SPORTZONE_DEV;
USE SCHEMA RAW;

CREATE STAGE IF NOT EXISTS stg_s3_raw
    URL                  = 's3://sportzone-datalake/raw/'
    STORAGE_INTEGRATION  = sportzone_s3_int
    FILE_FORMAT          = csv_comma_fmt
    COMMENT              = 'S3 external stage – production raw data';
*/

-- List internal stages
LIST @stg_customers;
LIST @stg_products;
LIST @stg_orders;

-- ---------------------------------------------------------------------------
-- HOW TO UPLOAD sample files (run from SnowSQL CLI):
--   PUT file:///path/to/customers.csv     @stg_customers;
--   PUT file:///path/to/products.csv      @stg_products;
--   PUT file:///path/to/orders.csv        @stg_orders;
--   PUT file:///path/to/inventory.csv     @stg_inventory;
--   PUT file:///path/to/promotions.csv    @stg_promotions;
--   PUT file:///path/to/stores.csv        @stg_stores;
-- ---------------------------------------------------------------------------
