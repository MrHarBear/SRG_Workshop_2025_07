/* ================================================================================
INSURANCE WORKSHOP - AUTOMATED PIPELINE SETUP
================================================================================
Purpose: Foundation setup for three-entity insurance data model
Entities: CUSTOMERS (1,200) → BROKER_ID → BROKERS (20), CLAIMS (1,001)
Scope: Database infrastructure, automated ingestion, initial data loading
================================================================================
*/

USE ROLE ACCOUNTADMIN;

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
    AUTO_SUSPEND = 30
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
DATA INGESTION INFRASTRUCTURE
================================================================================
*/

-- Create internal stages for data loading
CREATE OR REPLACE STAGE RAW_DATA.WORKSHOP_CSV_STAGE
    DIRECTORY = ( ENABLE = true )
    ENCRYPTION = ( TYPE = 'SNOWFLAKE_SSE' )
    COMMENT = 'Stage for CSV data files (customers and claims)';

CREATE OR REPLACE STAGE RAW_DATA.WORKSHOP_JSON_STAGE
    DIRECTORY = ( ENABLE = true )
    ENCRYPTION = ( TYPE = 'SNOWFLAKE_SSE' )
    COMMENT = 'Stage for JSON data files (broker profiles)';

CREATE OR REPLACE STAGE RAW_DATA.WORKSHOP_PIPELINE_STAGE
    DIRECTORY = ( ENABLE = true )
    ENCRYPTION = ( TYPE = 'SNOWFLAKE_SSE' )
    COMMENT = 'Working stage for pipeline operations';

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
RAW DATA TABLES - THREE ENTITY MODEL
================================================================================
*/

-- Customers table with broker relationships
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
    INSURED_OCCUPATION VARCHAR(100),
    -- Pipeline tracking columns
    LOAD_TIMESTAMP TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
    FILE_NAME STRING DEFAULT 'MANUAL_LOAD'
) COMMENT = 'Customer data with broker assignments';

-- Claims table with policy relationships
CREATE OR REPLACE TABLE RAW_DATA.CLAIMS_RAW (
    POLICY_NUMBER VARCHAR(50),
    INCIDENT_DATE DATE,
    INCIDENT_TYPE VARCHAR(100),
    INCIDENT_SEVERITY VARCHAR(50),
    AUTHORITIES_CONTACTED VARCHAR(50),
    INCIDENT_HOUR_OF_THE_DAY NUMBER,
    NUMBER_OF_VEHICLES_INVOLVED NUMBER,
    BODILY_INJURIES NUMBER,
    WITNESSES NUMBER,
    POLICE_REPORT_AVAILABLE VARCHAR(10),
    CLAIM_AMOUNT NUMBER(10,2),
    FRAUD_REPORTED BOOLEAN,
    -- Pipeline tracking columns
    LOAD_TIMESTAMP TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
    FILE_NAME STRING DEFAULT 'MANUAL_LOAD'
) COMMENT = 'Claims data with policy relationships';

-- Brokers table from JSON source
CREATE OR REPLACE TABLE RAW_DATA.BROKERS_RAW (
    BROKER_ID VARCHAR(10),
    FIRST_NAME VARCHAR(50),
    LAST_NAME VARCHAR(50),
    EMAIL VARCHAR(100),
    OFFICE_LOCATION VARCHAR(100),
    HIRE_DATE DATE,
    SPECIALIZATIONS ARRAY,
    TERRITORY ARRAY,
    CUSTOMER_SATISFACTION NUMBER(3,1),
    YEARS_EXPERIENCE NUMBER,
    TRAINING_HOURS_COMPLETED NUMBER,
    CERTIFICATIONS ARRAY,
    ACTIVE BOOLEAN,
    -- Pipeline tracking columns
    LOAD_TIMESTAMP TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
    FILE_NAME STRING DEFAULT 'MANUAL_LOAD'
) COMMENT = 'Broker profile data from JSON source';

/* ================================================================================
INITIAL DATA LOADING
================================================================================
*/

-- Load customer data
PUT file://datasets/customer_data.csv @WORKSHOP_CSV_STAGE;

COPY INTO RAW_DATA.CUSTOMERS_RAW (
    POLICY_NUMBER, BROKER_ID, AGE, POLICY_START_DATE, POLICY_LENGTH_MONTH,
    POLICY_DEDUCTABLE, POLICY_ANNUAL_PREMIUM, INSURED_SEX,
    INSURED_EDUCATION_LEVEL, INSURED_OCCUPATION
)
FROM @WORKSHOP_CSV_STAGE/customer_data.csv
FILE_FORMAT = (FORMAT_NAME = 'CSV_FORMAT')
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

-- Load claims data
PUT file://datasets/claim_data.csv @WORKSHOP_CSV_STAGE;

COPY INTO RAW_DATA.CLAIMS_RAW (
    POLICY_NUMBER, INCIDENT_DATE, INCIDENT_TYPE, INCIDENT_SEVERITY,
    AUTHORITIES_CONTACTED, INCIDENT_HOUR_OF_THE_DAY, NUMBER_OF_VEHICLES_INVOLVED,
    BODILY_INJURIES, WITNESSES, POLICE_REPORT_AVAILABLE, CLAIM_AMOUNT, FRAUD_REPORTED
)
FROM @WORKSHOP_CSV_STAGE/claim_data.csv
FILE_FORMAT = (FORMAT_NAME = 'CSV_FORMAT')
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

-- Load broker profiles from JSON
PUT file://datasets/broker_profiles.json @WORKSHOP_JSON_STAGE;

COPY INTO RAW_DATA.BROKERS_RAW (
    BROKER_ID, FIRST_NAME, LAST_NAME, EMAIL, OFFICE_LOCATION, HIRE_DATE,
    SPECIALIZATIONS, TERRITORY, CUSTOMER_SATISFACTION, YEARS_EXPERIENCE,
    TRAINING_HOURS_COMPLETED, CERTIFICATIONS, ACTIVE
)
FROM (
    SELECT 
        $1:broker_id::VARCHAR(10),
        $1:first_name::VARCHAR(50),
        $1:last_name::VARCHAR(50),
        $1:email::VARCHAR(100),
        $1:office_location::VARCHAR(100),
        $1:hire_date::DATE,
        $1:specializations::ARRAY,
        $1:territory::ARRAY,
        $1:performance_metrics.customer_satisfaction::NUMBER(3,1),
        $1:performance_metrics.years_experience::NUMBER,
        $1:performance_metrics.training_hours_completed::NUMBER,
        $1:certifications::ARRAY,
        $1:active::BOOLEAN
    FROM @WORKSHOP_JSON_STAGE/broker_profiles.json
)
FILE_FORMAT = (FORMAT_NAME = 'JSON_FORMAT');

/* ================================================================================
SNOWPIPE SETUP FOR AUTOMATED LOADING
================================================================================
*/

-- Snowpipe for automated customer data loading
CREATE OR REPLACE PIPE RAW_DATA.CUSTOMERS_DATA_PIPE
    AUTO_INGEST = TRUE
    AS
    COPY INTO RAW_DATA.CUSTOMERS_RAW 
    FROM @WORKSHOP_PIPELINE_STAGE
    PATTERN = '.*CUSTOMER_DATA.*\.csv'
    FILE_FORMAT = (FORMAT_NAME = 'CSV_FORMAT')
    MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
    ON_ERROR = CONTINUE;

-- Snowpipe for automated claims data loading
CREATE OR REPLACE PIPE RAW_DATA.CLAIMS_DATA_PIPE
    AUTO_INGEST = TRUE
    AS
    COPY INTO RAW_DATA.CLAIMS_RAW 
    FROM @WORKSHOP_PIPELINE_STAGE
    PATTERN = '.*CLAIM_DATA.*\.csv'
    FILE_FORMAT = (FORMAT_NAME = 'CSV_FORMAT')
    MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
    ON_ERROR = CONTINUE;

-- Snowpipe for automated broker profile loading
CREATE OR REPLACE PIPE RAW_DATA.BROKERS_DATA_PIPE
    AUTO_INGEST = TRUE
    AS
    COPY INTO RAW_DATA.BROKERS_RAW 
    FROM @WORKSHOP_PIPELINE_STAGE
    PATTERN = '.*BROKER_PROFILES.*\.json'
    FILE_FORMAT = (FORMAT_NAME = 'JSON_FORMAT')
    ON_ERROR = CONTINUE;

/* ================================================================================
PRIVILEGE GRANTS FOR WORKSHOP ROLES
================================================================================
*/

-- Grant access to raw data tables
GRANT SELECT, INSERT ON TABLE RAW_DATA.CUSTOMERS_RAW TO ROLE WORKSHOP_ANALYST;
GRANT SELECT, INSERT ON TABLE RAW_DATA.CLAIMS_RAW TO ROLE WORKSHOP_ANALYST;
GRANT SELECT, INSERT ON TABLE RAW_DATA.BROKERS_RAW TO ROLE WORKSHOP_ANALYST;

-- Grant stage access
GRANT READ, WRITE ON STAGE RAW_DATA.WORKSHOP_CSV_STAGE TO ROLE WORKSHOP_ANALYST;
GRANT READ, WRITE ON STAGE RAW_DATA.WORKSHOP_JSON_STAGE TO ROLE WORKSHOP_ANALYST;
GRANT READ, WRITE ON STAGE RAW_DATA.WORKSHOP_PIPELINE_STAGE TO ROLE WORKSHOP_ANALYST;

-- Grant file format usage
GRANT USAGE ON FILE FORMAT RAW_DATA.CSV_FORMAT TO ROLE WORKSHOP_ANALYST;
GRANT USAGE ON FILE FORMAT RAW_DATA.JSON_FORMAT TO ROLE WORKSHOP_ANALYST;

/* ================================================================================
DATA VALIDATION AND RELATIONSHIP VERIFICATION
================================================================================
*/

-- Validate data loading and relationships
SELECT 
    'CUSTOMERS' as ENTITY,
    COUNT(*) as TOTAL_RECORDS,
    COUNT(DISTINCT POLICY_NUMBER) as UNIQUE_POLICIES,
    COUNT(DISTINCT BROKER_ID) as UNIQUE_BROKERS,
    MIN(LOAD_TIMESTAMP) as FIRST_LOADED,
    MAX(LOAD_TIMESTAMP) as LAST_LOADED
FROM RAW_DATA.CUSTOMERS_RAW

UNION ALL

SELECT 
    'CLAIMS' as ENTITY,
    COUNT(*) as TOTAL_RECORDS,
    COUNT(DISTINCT POLICY_NUMBER) as UNIQUE_POLICIES,
    NULL as UNIQUE_BROKERS,
    MIN(LOAD_TIMESTAMP) as FIRST_LOADED,
    MAX(LOAD_TIMESTAMP) as LAST_LOADED
FROM RAW_DATA.CLAIMS_RAW

UNION ALL

SELECT 
    'BROKERS' as ENTITY,
    COUNT(*) as TOTAL_RECORDS,
    COUNT(DISTINCT BROKER_ID) as UNIQUE_POLICIES,
    NULL as UNIQUE_BROKERS,
    MIN(LOAD_TIMESTAMP) as FIRST_LOADED,
    MAX(LOAD_TIMESTAMP) as LAST_LOADED
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

-- Check pipe status
SELECT SYSTEM$PIPE_STATUS('CUSTOMERS_DATA_PIPE') as CUSTOMERS_PIPE_STATUS;
SELECT SYSTEM$PIPE_STATUS('CLAIMS_DATA_PIPE') as CLAIMS_PIPE_STATUS;
SELECT SYSTEM$PIPE_STATUS('BROKERS_DATA_PIPE') as BROKERS_PIPE_STATUS;

/* ================================================================================
FOUNDATION SETUP COMPLETE
================================================================================
Setup Complete:
• Database: INSURANCE_WORKSHOP_DB with 4 schemas
• Warehouses: WORKSHOP_COMPUTE_WH, WORKSHOP_OPS_WH  
• Roles: WORKSHOP_ANALYST, BROKER_CONSUMER
• Tables: CUSTOMERS_RAW (1,200), CLAIMS_RAW (1,001), BROKERS_RAW (20)
• Automation: 3 Snowpipes for continuous data loading
• Validation: Referential integrity confirmed

Ready for: Phase 2 - Data Quality Monitoring implementation
================================================================================
*/ 