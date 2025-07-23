/* ================================================================================
INSURANCE WORKSHOP - AUTOMATED PIPELINE SETUP
================================================================================
Purpose: Foundation setup for three-entity insurance data model
Entities: CUSTOMERS (1,200) → BROKER_ID → BROKERS (20), CLAIMS (1,001)
Scope: Database infrastructure, automated ingestion, initial data loading
================================================================================
*/

USE ROLE ACCOUNTADMIN;
ALTER ACCOUNT SET CORTEX_ENABLED_CROSS_REGION = 'ANY_REGION';

USE SECONDARY ROLES NONE;

-- Clean start for workshop consistency
DROP DATABASE IF EXISTS INSURANCE_WORKSHOP_DB;

/* ================================================================================
DATABASE AND WAREHOUSE CREATION
================================================================================
*/

CREATE DATABASE INSURANCE_WORKSHOP_DB
    COMMENT = 'Insurance Workshop - Enhanced Demo with Broker Integration';

CREATE SCHEMA INSURANCE_WORKSHOP_DB.RAW_DATA 
    COMMENT = 'Raw insurance data from CSV and JSON sources';

CREATE SCHEMA INSURANCE_WORKSHOP_DB.ANALYTICS 
    COMMENT = 'Business analytics with Dynamic Tables and UDFs';
    
CREATE SCHEMA INSURANCE_WORKSHOP_DB.GOVERNANCE 
    COMMENT = 'Data governance policies and access controls';
    
CREATE SCHEMA INSURANCE_WORKSHOP_DB.SHARING 
    COMMENT = 'Secure views for external data sharing';

-- Set working context
USE DATABASE INSURANCE_WORKSHOP_DB;
USE SCHEMA RAW_DATA;

-- Create optimized warehouses for workshop operations
CREATE OR REPLACE WAREHOUSE WORKSHOP_COMPUTE_WH
    WAREHOUSE_SIZE = XSMALL
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    RESOURCE_CONSTRAINT = 'STANDARD_GEN_2'
    COMMENT = 'Main compute for workshop analytics operations';

CREATE OR REPLACE WAREHOUSE WORKSHOP_OPS_WH
    WAREHOUSE_SIZE = XSMALL
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    COMMENT = 'Dedicated warehouse for pipeline operations';

USE WAREHOUSE WORKSHOP_COMPUTE_WH;

/* ================================================================================
RBAC SETUP FOR WORKSHOP
================================================================================
*/

-- Create analyst role for workshop demonstrations
USE ROLE USERADMIN;
CREATE OR REPLACE ROLE WORKSHOP_ANALYST
    COMMENT = 'Workshop analyst role for demonstrations';

CREATE OR REPLACE ROLE BROKER_CONSUMER
    COMMENT = 'External broker role for data sharing demonstrations';

-- Grant comprehensive privileges
USE ROLE SECURITYADMIN;
GRANT USAGE, OPERATE ON WAREHOUSE WORKSHOP_COMPUTE_WH TO ROLE WORKSHOP_ANALYST;
GRANT USAGE, OPERATE ON WAREHOUSE WORKSHOP_OPS_WH TO ROLE WORKSHOP_ANALYST;
GRANT USAGE, OPERATE ON WAREHOUSE WORKSHOP_COMPUTE_WH TO ROLE BROKER_CONSUMER;

GRANT USAGE ON DATABASE INSURANCE_WORKSHOP_DB TO ROLE WORKSHOP_ANALYST;
GRANT USAGE ON DATABASE INSURANCE_WORKSHOP_DB TO ROLE BROKER_CONSUMER;
GRANT USAGE ON ALL SCHEMAS IN DATABASE INSURANCE_WORKSHOP_DB TO ROLE WORKSHOP_ANALYST;
GRANT USAGE ON SCHEMA INSURANCE_WORKSHOP_DB.SHARING TO ROLE BROKER_CONSUMER;

-- Grant object creation privileges for workshop operations
GRANT CREATE TABLE, CREATE VIEW, CREATE DYNAMIC TABLE, CREATE FUNCTION ON SCHEMA INSURANCE_WORKSHOP_DB.RAW_DATA TO ROLE WORKSHOP_ANALYST;
GRANT CREATE TABLE, CREATE VIEW, CREATE DYNAMIC TABLE, CREATE FUNCTION ON SCHEMA INSURANCE_WORKSHOP_DB.ANALYTICS TO ROLE WORKSHOP_ANALYST;
GRANT CREATE VIEW, CREATE MASKING POLICY, CREATE ROW ACCESS POLICY ON SCHEMA INSURANCE_WORKSHOP_DB.GOVERNANCE TO ROLE WORKSHOP_ANALYST;
GRANT CREATE VIEW ON SCHEMA INSURANCE_WORKSHOP_DB.SHARING TO ROLE WORKSHOP_ANALYST;

-- Grant analyst role to current user
SET MY_USER_ID = CURRENT_USER();
GRANT ROLE WORKSHOP_ANALYST TO USER identifier($MY_USER_ID);

USE ROLE ACCOUNTADMIN;

/* ================================================================================
AUTOMATED PIPELINE INFRASTRUCTURE WITH GIT INTEGRATION
================================================================================
*/

-- Create Git integration for repository data access
CREATE OR REPLACE API INTEGRATION INSURANCE_WORKSHOP_GIT_INTEGRATION
    API_PROVIDER = git_https_api
    API_ALLOWED_PREFIXES = ('https://github.com')
    ENABLED = TRUE
    COMMENT = 'Git integration for automated workshop data pipeline';

-- Connect to demo repository
CREATE OR REPLACE GIT REPOSITORY INSURANCE_WORKSHOP_DEMO_REPO
    API_INTEGRATION = INSURANCE_WORKSHOP_GIT_INTEGRATION
    ORIGIN = 'https://github.com/MrHarBear/SRG_Workshop_2025_07.git'
    GIT_CREDENTIALS = NULL
    COMMENT = 'Repository with SRG Workshop 2025 insurance demo data and additional files';

-- Refresh repository to access latest files
ALTER GIT REPOSITORY INSURANCE_WORKSHOP_DEMO_REPO FETCH;

-- List files to verify the repository connection
SHOW GIT BRANCHES IN GIT REPOSITORY INSURANCE_WORKSHOP_DEMO_REPO;
LS @INSURANCE_WORKSHOP_DEMO_REPO/branches/main;

-- Create simplified stage structure for workshop operations
-- DATA_STAGE: All CSV and JSON data files (customers, claims, brokers)
-- DOCUMENT_STAGE: Claims Documents
-- POLICY_WORDING_DOCUMENTS: Policy Wording Document Landing zone
CREATE OR REPLACE STAGE RAW_DATA.DATA_STAGE
    DIRECTORY = ( ENABLE = true )
    ENCRYPTION = ( TYPE = 'SNOWFLAKE_SSE' )
    COMMENT = 'Unified stage for all CSV and JSON data files';

CREATE OR REPLACE STAGE RAW_DATA.DOCUMENT_STAGE
    DIRECTORY = ( ENABLE = true )
    ENCRYPTION = ( TYPE = 'SNOWFLAKE_SSE' )
    COMMENT = 'Stage for documentation and reference files';

CREATE OR REPLACE STAGE RAW_DATA.POLICY_WORDING_DOCUMENTS
    DIRECTORY = ( ENABLE = true )
    ENCRYPTION = ( TYPE = 'SNOWFLAKE_SSE' )
    COMMENT = 'Stage for documentation and reference files';

-- Create file formats for different data types
CREATE OR REPLACE FILE FORMAT RAW_DATA.CSV_FORMAT
    TYPE = CSV
    PARSE_HEADER = TRUE
    FIELD_DELIMITER = ','
    RECORD_DELIMITER = '\n'
    TRIM_SPACE = TRUE
    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
    REPLACE_INVALID_CHARACTERS = TRUE
    DATE_FORMAT = 'YYYY-MM-DD'
    TIMESTAMP_FORMAT = 'YYYY-MM-DD HH24:MI:SS.FF3'
    COMMENT = 'Standard CSV format for customer and claims data';

CREATE OR REPLACE FILE FORMAT RAW_DATA.JSON_FORMAT
    TYPE = JSON
    STRIP_OUTER_ARRAY = TRUE
    COMMENT = 'JSON format for broker profile data';

/* ================================================================================
INITIAL DATA LOADING FROM GIT REPOSITORY
================================================================================
*/

-- Load all data files from Git repository to unified data stage
COPY FILES
    INTO @DATA_STAGE
    FROM '@INSURANCE_WORKSHOP_DEMO_REPO/branches/main/datasets/'
    PATTERN='.*claim_data.csv';

COPY FILES
    INTO @DATA_STAGE
    FROM '@INSURANCE_WORKSHOP_DEMO_REPO/branches/main/datasets/'
    PATTERN='.*customer_data.csv';

COPY FILES
    INTO @DATA_STAGE
    FROM '@INSURANCE_WORKSHOP_DEMO_REPO/branches/main/datasets/'
    PATTERN='.*broker_profiles.json';

-- Verify files are staged
LIST @DATA_STAGE;

/* ================================================================================
SCHEMA DETECTION FOR AUTOMATED TABLE CREATION
================================================================================
*/

-- Use schema detection for Claims data
SELECT * FROM TABLE(
    INFER_SCHEMA(
        LOCATION=>'@DATA_STAGE',
        FILE_FORMAT=>'CSV_FORMAT',
        FILES=>'claim_data.csv'
    )
);

-- Use schema detection for Broker JSON data
SELECT 
    $1 as RAW_JSON_RECORD,
FROM @DATA_STAGE/broker_profiles.json
(FILE_FORMAT => 'JSON_FORMAT')
LIMIT 10;


-- Detect schema from JSON broker profiles
SELECT * FROM TABLE(
    INFER_SCHEMA(
        LOCATION=>'@DATA_STAGE',
        FILE_FORMAT=>'JSON_FORMAT',
        FILES=>'broker_profiles.json',
        IGNORE_CASE => TRUE
    )
);

/* ================================================================================
RAW DATA TABLES - THREE ENTITY MODEL WITH SCHEMA DETECTION
================================================================================
CUSTOMERS_RAW: Manual definition for workshop consistency and broker relationships
CLAIMS_RAW: Auto-created using CSV schema detection
BROKERS_RAW: Auto-created using JSON schema detection
================================================================================
*/

-- Customers table with broker relationships (manual definition for workshop consistency)
CREATE OR REPLACE TABLE RAW_DATA.CUSTOMERS_RAW (
    POLICY_NUMBER VARCHAR(50),
    BROKER_ID VARCHAR(10),
    AGE NUMBER,
    POLICY_START_DATE DATE,
    POLICY_LENGTH_MONTH NUMBER,
    POLICY_DEDUCTABLE NUMBER(10,2),
    POLICY_ANNUAL_PREMIUM NUMBER(10,2),
    INSURED_SEX VARCHAR(10),
    INSURED_EDUCATION_LEVEL VARCHAR(50),
    INSURED_OCCUPATION VARCHAR(100)
) COMMENT = 'Customer data with broker assignments';

-- Claims table using schema detection
CREATE OR REPLACE TABLE RAW_DATA.CLAIMS_RAW 
USING TEMPLATE (
    SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
    FROM TABLE(
        INFER_SCHEMA(
            LOCATION=>'@DATA_STAGE',
            FILE_FORMAT=>'CSV_FORMAT',
            FILES=>'claim_data.csv'
        )
    )
);

-- Set table comment for schema-detected claims table
ALTER TABLE RAW_DATA.CLAIMS_RAW SET COMMENT = 'Claims data with policy relationships - schema auto-detected';

-- Brokers table using schema detection from JSON source
CREATE OR REPLACE TABLE RAW_DATA.BROKERS_RAW 
USING TEMPLATE (
    SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
    FROM TABLE(
        INFER_SCHEMA(
            LOCATION=>'@DATA_STAGE',
            FILE_FORMAT=>'JSON_FORMAT',
            FILES=>'broker_profiles.json',
            IGNORE_CASE => TRUE
        )
    )
);

-- Set table comment for schema-detected brokers table
ALTER TABLE RAW_DATA.BROKERS_RAW SET COMMENT = 'Broker profile data from JSON source - schema auto-detected';

/* ================================================================================
INITIAL DATA LOADING FROM GIT REPOSITORY
================================================================================
Load data into schema-detected tables with automatic column mapping
================================================================================
*/

/* ================================================================================
SNOWPIPE SETUP FOR AUTOMATED LOADING
================================================================================
All Snowpipes configured to monitor DATA_STAGE for incoming files
*/

-- Snowpipe for automated claims data loading with schema detection support
CREATE OR REPLACE PIPE RAW_DATA.CLAIMS_DATA_PIPE
    AUTO_INGEST = TRUE
    AS
    COPY INTO RAW_DATA.CLAIMS_RAW 
    FROM @DATA_STAGE
    PATTERN = '.*claim_data.*\.csv'
    FILE_FORMAT = (FORMAT_NAME = 'CSV_FORMAT')
    MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
    ON_ERROR = CONTINUE;
-- Manually refresh pipes to process any staged files
ALTER PIPE CLAIMS_DATA_PIPE REFRESH;


-- Load customer data from Git repository
COPY INTO RAW_DATA.CUSTOMERS_RAW
FROM @DATA_STAGE/customer_data.csv
FILE_FORMAT = (FORMAT_NAME = 'CSV_FORMAT')
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

-- -- Load claims data using schema detection (auto-mapped columns)
-- COPY INTO RAW_DATA.CLAIMS_RAW
-- FROM @DATA_STAGE/claim_data.csv
-- FILE_FORMAT = (FORMAT_NAME = 'CSV_FORMAT')
-- MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

-- Load broker profiles using schema detection (auto-mapped columns)
COPY INTO RAW_DATA.BROKERS_RAW
FROM @DATA_STAGE/broker_profiles.json
FILE_FORMAT = (FORMAT_NAME = 'JSON_FORMAT')
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;


select * from CUSTOMERS_RAW;
select * from BROKERS_RAW;
select * from CLAIMS_RAW;

/* ================================================================================
PRIVILEGE GRANTS FOR WORKSHOP ROLES
================================================================================
*/
use role workshop_analyst;

select * from claims_raw;

-- Grant privileges
USE ROLE ACCOUNTADMIN;

-- Grant access to raw data tables
GRANT SELECT, INSERT ON TABLE RAW_DATA.CUSTOMERS_RAW TO ROLE WORKSHOP_ANALYST;
GRANT SELECT, INSERT ON TABLE RAW_DATA.CLAIMS_RAW TO ROLE WORKSHOP_ANALYST;
GRANT SELECT, INSERT ON TABLE RAW_DATA.BROKERS_RAW TO ROLE WORKSHOP_ANALYST;

use role workshop_analyst;
select * from claims_raw;

/* ================================================================================
DATA VALIDATION AND RELATIONSHIP VERIFICATION
================================================================================
*/
USE ROLE ACCOUNTADMIN;
USE SECONDARY ROLES ALL;

SELECT 
    'CUSTOMERS' as ENTITY,
    COUNT(*) as TOTAL_RECORDS,
    COUNT(DISTINCT POLICY_NUMBER) as UNIQUE_POLICIES,
    COUNT(DISTINCT BROKER_ID) as UNIQUE_BROKERS
FROM RAW_DATA.CUSTOMERS_RAW

UNION ALL

SELECT 
    'CLAIMS' as ENTITY,
    COUNT(*) as TOTAL_RECORDS,
    COUNT(DISTINCT POLICY_NUMBER) as UNIQUE_POLICIES,
    NULL as UNIQUE_BROKERS
FROM RAW_DATA.CLAIMS_RAW

UNION ALL

SELECT 
    'BROKERS' as ENTITY,
    COUNT(*) as TOTAL_RECORDS,
    COUNT(DISTINCT BROKER_ID) as UNIQUE_POLICIES,
    NULL as UNIQUE_BROKERS
FROM RAW_DATA.BROKERS_RAW;

-- Verify referential integrity
SELECT 
    'CUSTOMER-BROKER RELATIONSHIP' as CHECK_TYPE,
    COUNT(c.POLICY_NUMBER) as CUSTOMERS_WITH_BROKERS,
    COUNT(b.BROKER_ID) as MATCHED_BROKERS
FROM RAW_DATA.CUSTOMERS_RAW c
LEFT JOIN RAW_DATA.BROKERS_RAW b ON c.BROKER_ID = b.BROKER_ID;

SELECT 
    'CUSTOMER-CLAIMS RELATIONSHIP' as CHECK_TYPE,
    COUNT(DISTINCT c.POLICY_NUMBER) as CUSTOMERS_WITH_CLAIMS,
    COUNT(cl.POLICY_NUMBER) as TOTAL_CLAIMS
FROM RAW_DATA.CUSTOMERS_RAW c
INNER JOIN RAW_DATA.CLAIMS_RAW cl ON c.POLICY_NUMBER = cl.POLICY_NUMBER;


/* =============================================================================
ADDITIONAL DATA LOADING FROM GIT REPOSITORY
============================================================================= */
-- At this point, feel free to use the Git Integration to add the Git repository to the Snowflake workspace.
-- https://github.com/MrHarBear/SRG_Workshop_2025_07

/* =============================================================================
ADDITIONAL DATA LOADING FROM GIT REPOSITORY
================================================================================
Load data into schema-detected tables with automatic column mapping and Snowpipe
================================================================================
*/
-- -- Check pipe status
-- SELECT SYSTEM$PIPE_STATUS('CLAIMS_DATA_PIPE') as CLAIMS_PIPE_STATUS;

-- USE ROLE ACCOUNTADMIN;
-- USE DATABASE INSURANCE_WORKSHOP_DB;
-- USE SCHEMA RAW_DATA;
-- -- Load all data files from Git repository to unified data stage
-- COPY FILES
--     INTO @DATA_STAGE
--     FROM '@INSURANCE_WORKSHOP_DEMO_REPO/branches/main/datasets/'
--     PATTERN='.*claim_data_.*.csv';

-- COPY FILES
--     INTO @DATA_STAGE
--     FROM '@INSURANCE_WORKSHOP_DEMO_REPO/branches/main/datasets/'
--     PATTERN='.*customer_data_.*.csv';

-- COPY INTO RAW_DATA.CUSTOMERS_RAW
-- FROM @DATA_STAGE/customer_data_with_errors.csv
-- FILE_FORMAT = (FORMAT_NAME = 'CSV_FORMAT')
-- MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;