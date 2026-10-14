-- =============================================================================
-- SportZone Retail Analytics Platform
-- 01_raw / 01_file_formats.sql
-- Purpose : File formats for all inbound source files
-- Run as  : SPORTZONE_DATA_ENG
-- =============================================================================

USE ROLE SPORTZONE_DATA_ENG;
USE DATABASE SPORTZONE_DEV;
USE SCHEMA RAW;
USE WAREHOUSE DEV_LOAD_WH;

-- ---------------------------------------------------------------------------
-- CSV – comma delimited (most source files)
-- ---------------------------------------------------------------------------

CREATE OR REPLACE FILE FORMAT csv_comma_fmt
    TYPE              = CSV
    FIELD_DELIMITER   = ','
    RECORD_DELIMITER  = '\n'
    SKIP_HEADER       = 1
    NULL_IF           = ('NULL', 'null', 'N/A', '')
    EMPTY_FIELD_AS_NULL = TRUE
    TRIM_SPACE        = TRUE
    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
    COMMENT = 'Standard comma-delimited CSV with header';

-- ---------------------------------------------------------------------------
-- CSV – pipe delimited (legacy ERP exports)
-- ---------------------------------------------------------------------------

CREATE OR REPLACE FILE FORMAT csv_pipe_fmt
    TYPE              = CSV
    FIELD_DELIMITER   = '|'
    RECORD_DELIMITER  = '\n'
    SKIP_HEADER       = 1
    NULL_IF           = ('NULL', 'null', '')
    EMPTY_FIELD_AS_NULL = TRUE
    TRIM_SPACE        = TRUE
    COMMENT = 'Pipe-delimited CSV – ERP exports';

-- ---------------------------------------------------------------------------
-- JSON – event / clickstream data
-- ---------------------------------------------------------------------------

CREATE OR REPLACE FILE FORMAT json_fmt
    TYPE        = JSON
    STRIP_OUTER_ARRAY = TRUE
    COMMENT = 'JSON arrays – web clickstream / API events';

-- ---------------------------------------------------------------------------
-- Parquet – analytics / ML feature files
-- ---------------------------------------------------------------------------

CREATE OR REPLACE FILE FORMAT parquet_snappy_fmt
    TYPE        = PARQUET
    COMPRESSION = SNAPPY
    COMMENT = 'Parquet with Snappy compression';

-- Verify
SHOW FILE FORMATS;
