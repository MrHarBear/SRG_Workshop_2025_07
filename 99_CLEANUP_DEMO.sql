/* ================================================================================
INSURANCE WORKSHOP - CLEANUP AND RESET
================================================================================
Purpose: Complete cleanup and reset procedures for workshop environment
Scope: Optional cleanup with data preservation, demo reset, resource management
Note: Use with caution - this script removes workshop components
================================================================================
*/

-- Confirm current session context
SELECT CURRENT_DATABASE(), CURRENT_SCHEMA(), CURRENT_ROLE(), CURRENT_WAREHOUSE();

USE ROLE ACCOUNTADMIN;

/* ================================================================================
CLEANUP OPTIONS AND CONFIRMATION
================================================================================
*/

-- Safety check: Ensure we're working with the correct database
SET DATABASE_NAME = 'INSURANCE_WORKSHOP_DB';

-- Verify database exists before cleanup
SELECT 
    'Database exists: ' || COALESCE(DATABASE_NAME, 'NOT FOUND') as STATUS
FROM INFORMATION_SCHEMA.DATABASES 
WHERE DATABASE_NAME = $DATABASE_NAME;

/* ================================================================================
CLEANUP PHASE 1: STOP AUTOMATED PROCESSES
================================================================================
*/

-- Stop all Data Metric Function schedules
USE DATABASE INSURANCE_WORKSHOP_DB;
USE SCHEMA RAW_DATA;

-- Suspend DMF schedules for controlled cleanup
ALTER DATA METRIC FUNCTION IF EXISTS CUSTOMER_COMPLETENESS_CHECK SUSPEND;
ALTER DATA METRIC FUNCTION IF EXISTS CUSTOMER_VALIDITY_CHECK SUSPEND;
ALTER DATA METRIC FUNCTION IF EXISTS CLAIM_AMOUNT_VALIDATION SUSPEND;
ALTER DATA METRIC FUNCTION IF EXISTS CLAIM_RELATIONSHIP_INTEGRITY SUSPEND;
ALTER DATA METRIC FUNCTION IF EXISTS BROKER_PROFILE_COMPLETENESS SUSPEND;
ALTER DATA METRIC FUNCTION IF EXISTS CROSS_ENTITY_CONSISTENCY SUSPEND;

-- Stop any running streams or tasks
SHOW STREAMS IN DATABASE INSURANCE_WORKSHOP_DB;
SHOW TASKS IN DATABASE INSURANCE_WORKSHOP_DB;

-- Stop streams if they exist
ALTER STREAM IF EXISTS RAW_DATA.CUSTOMER_CHANGES SUSPEND;
ALTER STREAM IF EXISTS RAW_DATA.CLAIM_CHANGES SUSPEND;
ALTER STREAM IF EXISTS RAW_DATA.BROKER_CHANGES SUSPEND;

-- Stop tasks if they exist
ALTER TASK IF EXISTS RAW_DATA.DATA_QUALITY_MONITORING_TASK SUSPEND;
ALTER TASK IF EXISTS ANALYTICS.RISK_INTELLIGENCE_REFRESH_TASK SUSPEND;
ALTER TASK IF EXISTS GOVERNANCE.POLICY_ENFORCEMENT_TASK SUSPEND;

/* ================================================================================
CLEANUP PHASE 2: DATA PRESERVATION OPTIONS
================================================================================
*/

-- Option 1: Preserve data in temporary location (uncomment if needed)
/*
CREATE OR REPLACE DATABASE INSURANCE_WORKSHOP_BACKUP;

CREATE OR REPLACE TABLE INSURANCE_WORKSHOP_BACKUP.PUBLIC.CUSTOMERS_BACKUP AS
SELECT * FROM INSURANCE_WORKSHOP_DB.RAW_DATA.CUSTOMERS_RAW;

CREATE OR REPLACE TABLE INSURANCE_WORKSHOP_BACKUP.PUBLIC.CLAIMS_BACKUP AS
SELECT * FROM INSURANCE_WORKSHOP_DB.RAW_DATA.CLAIMS_RAW;

CREATE OR REPLACE TABLE INSURANCE_WORKSHOP_BACKUP.PUBLIC.BROKERS_BACKUP AS
SELECT * FROM INSURANCE_WORKSHOP_DB.RAW_DATA.BROKERS_RAW;

SELECT 'Data backed up successfully' as BACKUP_STATUS;
*/

-- Option 2: Export summary statistics for reference
CREATE OR REPLACE TEMPORARY TABLE WORKSHOP_SUMMARY AS
SELECT 
    'CUSTOMERS' as ENTITY,
    COUNT(*) as RECORD_COUNT,
    COUNT(DISTINCT CUSTOMER_REGION) as REGIONS,
    COUNT(DISTINCT BROKER_ID) as BROKERS,
    CURRENT_TIMESTAMP() as CLEANUP_TIME
FROM INSURANCE_WORKSHOP_DB.RAW_DATA.CUSTOMERS_RAW
UNION ALL
SELECT 
    'CLAIMS',
    COUNT(*),
    COUNT(DISTINCT CASE WHEN CLAIM_AMOUNT_FILLED > 0 THEN 1 END),
    COUNT(DISTINCT POLICY_NUMBER),
    CURRENT_TIMESTAMP()
FROM INSURANCE_WORKSHOP_DB.RAW_DATA.CLAIMS_RAW
UNION ALL
SELECT 
    'BROKERS',
    COUNT(*),
    COUNT(DISTINCT BROKER_OFFICE),
    COUNT(DISTINCT BROKER_TIER),
    CURRENT_TIMESTAMP()
FROM INSURANCE_WORKSHOP_DB.RAW_DATA.BROKERS_RAW;

-- Display summary before cleanup
SELECT * FROM WORKSHOP_SUMMARY;

/* ================================================================================
CLEANUP PHASE 3: REMOVE GOVERNANCE POLICIES
================================================================================
*/

-- Remove masking policies
DROP MASKING POLICY IF EXISTS INSURANCE_WORKSHOP_DB.GOVERNANCE.CUSTOMER_PII_MASK;
DROP MASKING POLICY IF EXISTS INSURANCE_WORKSHOP_DB.GOVERNANCE.CUSTOMER_EMAIL_MASK;
DROP MASKING POLICY IF EXISTS INSURANCE_WORKSHOP_DB.GOVERNANCE.CUSTOMER_PHONE_MASK;
DROP MASKING POLICY IF EXISTS INSURANCE_WORKSHOP_DB.GOVERNANCE.CLAIM_AMOUNT_MASK;

-- Remove row access policies
DROP ROW ACCESS POLICY IF EXISTS INSURANCE_WORKSHOP_DB.GOVERNANCE.BROKER_DATA_ACCESS;
DROP ROW ACCESS POLICY IF EXISTS INSURANCE_WORKSHOP_DB.GOVERNANCE.REGIONAL_DATA_ACCESS;

-- Remove classification tags
DROP TAG IF EXISTS INSURANCE_WORKSHOP_DB.GOVERNANCE.DATA_CLASSIFICATION;
DROP TAG IF EXISTS INSURANCE_WORKSHOP_DB.GOVERNANCE.SENSITIVITY_LEVEL;
DROP TAG IF EXISTS INSURANCE_WORKSHOP_DB.GOVERNANCE.RETENTION_PERIOD;

/* ================================================================================
CLEANUP PHASE 4: REMOVE USER-DEFINED FUNCTIONS
================================================================================
*/

-- Remove Python UDFs
DROP FUNCTION IF EXISTS INSURANCE_WORKSHOP_DB.ANALYTICS.ANALYZE_BROKER_PERFORMANCE(OBJECT);
DROP FUNCTION IF EXISTS INSURANCE_WORKSHOP_DB.ANALYTICS.PREDICT_RISK_TRAJECTORY(NUMBER, VARCHAR, NUMBER, NUMBER);
DROP FUNCTION IF EXISTS INSURANCE_WORKSHOP_DB.ANALYTICS.CALCULATE_TERRITORY_RISK(VARCHAR, NUMBER, NUMBER);
DROP FUNCTION IF EXISTS INSURANCE_WORKSHOP_DB.ANALYTICS.GENERATE_CUSTOMER_INSIGHTS(OBJECT);

-- Remove SQL UDFs
DROP FUNCTION IF EXISTS INSURANCE_WORKSHOP_DB.ANALYTICS.CALCULATE_CUSTOMER_RISK_SCORE(NUMBER, NUMBER, NUMBER);
DROP FUNCTION IF EXISTS INSURANCE_WORKSHOP_DB.ANALYTICS.DETERMINE_BROKER_TIER(NUMBER, NUMBER, NUMBER);
DROP FUNCTION IF EXISTS INSURANCE_WORKSHOP_DB.ANALYTICS.CLASSIFY_CLAIM_RISK(NUMBER, VARCHAR, VARCHAR);
DROP FUNCTION IF EXISTS INSURANCE_WORKSHOP_DB.ANALYTICS.VALIDATE_CUSTOMER_PROFILE(OBJECT);

/* ================================================================================
CLEANUP PHASE 5: REMOVE DYNAMIC TABLES
================================================================================
*/

-- Remove Dynamic Tables in dependency order
DROP DYNAMIC TABLE IF EXISTS INSURANCE_WORKSHOP_DB.ANALYTICS.RISK_INTELLIGENCE_DASHBOARD;
DROP DYNAMIC TABLE IF EXISTS INSURANCE_WORKSHOP_DB.ANALYTICS.BROKER_PERFORMANCE_MATRIX;
DROP DYNAMIC TABLE IF EXISTS INSURANCE_WORKSHOP_DB.ANALYTICS.CUSTOMER_RISK_PROFILES;
DROP DYNAMIC TABLE IF EXISTS INSURANCE_WORKSHOP_DB.ANALYTICS.INTEGRATED_CUSTOMER_INTELLIGENCE;

/* ================================================================================
CLEANUP PHASE 6: REMOVE VIEWS AND TABLES
================================================================================
*/

-- Remove secure sharing views
DROP VIEW IF EXISTS INSURANCE_WORKSHOP_DB.SHARING.SECURE_CUSTOMER_VIEW;
DROP VIEW IF EXISTS INSURANCE_WORKSHOP_DB.SHARING.SECURE_CLAIM_VIEW;
DROP VIEW IF EXISTS INSURANCE_WORKSHOP_DB.SHARING.SECURE_BROKER_VIEW;
DROP VIEW IF EXISTS INSURANCE_WORKSHOP_DB.SHARING.ANONYMOUS_ANALYTICS_VIEW;

-- Remove analytics tables
DROP TABLE IF EXISTS INSURANCE_WORKSHOP_DB.RAW_DATA.ENTITY_QUALITY_SCORES;
DROP TABLE IF EXISTS INSURANCE_WORKSHOP_DB.RAW_DATA.QUALITY_MONITORING_SUMMARY;
DROP TABLE IF EXISTS INSURANCE_WORKSHOP_DB.RAW_DATA.RELATIONSHIP_QUALITY_METRICS;
DROP TABLE IF EXISTS INSURANCE_WORKSHOP_DB.GOVERNANCE.POLICY_ENFORCEMENT_LOG;

-- Remove staging tables
DROP TABLE IF EXISTS INSURANCE_WORKSHOP_DB.RAW_DATA.CUSTOMERS_STAGING;
DROP TABLE IF EXISTS INSURANCE_WORKSHOP_DB.RAW_DATA.CLAIMS_STAGING;
DROP TABLE IF EXISTS INSURANCE_WORKSHOP_DB.RAW_DATA.BROKERS_STAGING;

-- Remove main data tables
DROP TABLE IF EXISTS INSURANCE_WORKSHOP_DB.RAW_DATA.CUSTOMERS_RAW;
DROP TABLE IF EXISTS INSURANCE_WORKSHOP_DB.RAW_DATA.CLAIMS_RAW;
DROP TABLE IF EXISTS INSURANCE_WORKSHOP_DB.RAW_DATA.BROKERS_RAW;

/* ================================================================================
CLEANUP PHASE 7: REMOVE FILE FORMATS AND STAGES
================================================================================
*/

-- Remove file formats
DROP FILE FORMAT IF EXISTS INSURANCE_WORKSHOP_DB.RAW_DATA.CSV_FORMAT;
DROP FILE FORMAT IF EXISTS INSURANCE_WORKSHOP_DB.RAW_DATA.JSON_FORMAT;

-- Remove internal stages
DROP STAGE IF EXISTS INSURANCE_WORKSHOP_DB.RAW_DATA.CUSTOMER_STAGE;
DROP STAGE IF EXISTS INSURANCE_WORKSHOP_DB.RAW_DATA.CLAIM_STAGE;
DROP STAGE IF EXISTS INSURANCE_WORKSHOP_DB.RAW_DATA.BROKER_STAGE;

/* ================================================================================
CLEANUP PHASE 8: REMOVE SCHEMAS
================================================================================
*/

-- Remove schemas in proper order
DROP SCHEMA IF EXISTS INSURANCE_WORKSHOP_DB.SHARING;
DROP SCHEMA IF EXISTS INSURANCE_WORKSHOP_DB.GOVERNANCE;
DROP SCHEMA IF EXISTS INSURANCE_WORKSHOP_DB.ANALYTICS;
DROP SCHEMA IF EXISTS INSURANCE_WORKSHOP_DB.RAW_DATA;

/* ================================================================================
CLEANUP PHASE 9: REMOVE DATABASE AND WAREHOUSES
================================================================================
*/

-- Remove warehouses
DROP WAREHOUSE IF EXISTS WORKSHOP_COMPUTE_WH;
DROP WAREHOUSE IF EXISTS WORKSHOP_OPS_WH;

-- Remove database (final step)
DROP DATABASE IF EXISTS INSURANCE_WORKSHOP_DB;

/* ================================================================================
CLEANUP PHASE 10: ROLE AND USER CLEANUP (OPTIONAL)
================================================================================
*/

-- Remove workshop-specific roles (uncomment if created)
/*
DROP ROLE IF EXISTS INSURANCE_ANALYST;
DROP ROLE IF EXISTS INSURANCE_BROKER;
DROP ROLE IF EXISTS DATA_ENGINEER;
DROP ROLE IF EXISTS COMPLIANCE_OFFICER;
*/

-- Remove workshop users (uncomment if created)
/*
DROP USER IF EXISTS WORKSHOP_USER_1;
DROP USER IF EXISTS WORKSHOP_USER_2;
DROP USER IF EXISTS WORKSHOP_USER_3;
*/

/* ================================================================================
CLEANUP VERIFICATION
================================================================================
*/

-- Verify cleanup completion
SELECT 
    'Cleanup completed at: ' || CURRENT_TIMESTAMP() as CLEANUP_STATUS,
    'Database removed: INSURANCE_WORKSHOP_DB' as DATABASE_STATUS,
    'Warehouses removed: WORKSHOP_COMPUTE_WH, WORKSHOP_OPS_WH' as WAREHOUSE_STATUS;

-- Check for any remaining objects
SELECT 
    'Remaining databases with INSURANCE in name:' as CHECK_TYPE,
    COUNT(*) as OBJECT_COUNT
FROM INFORMATION_SCHEMA.DATABASES 
WHERE DATABASE_NAME LIKE '%INSURANCE%';

SELECT 
    'Remaining warehouses with WORKSHOP in name:' as CHECK_TYPE,
    COUNT(*) as OBJECT_COUNT
FROM INFORMATION_SCHEMA.WAREHOUSES 
WHERE WAREHOUSE_NAME LIKE '%WORKSHOP%';

-- Check account usage for any remaining references
SELECT 
    'Recent queries against removed database (last 1 hour):' as CHECK_TYPE,
    COUNT(*) as QUERY_COUNT
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY 
WHERE DATABASE_NAME = 'INSURANCE_WORKSHOP_DB'
    AND START_TIME >= DATEADD('hour', -1, CURRENT_TIMESTAMP());

/* ================================================================================
RESET DEMONSTRATION CAPABILITY
================================================================================
*/

-- Create simple reset script for quick demo restart
CREATE OR REPLACE PROCEDURE RESET_INSURANCE_WORKSHOP()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    -- This procedure can be called to quickly reset the workshop
    -- It assumes the 00_AUTOMATED_PIPELINE_SETUP.sql will be re-run
    
    LET status STRING := 'Workshop environment cleared and ready for reset';
    
    -- Log the reset action
    INSERT INTO SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY 
    SELECT 
        CURRENT_TIMESTAMP() as reset_time,
        'WORKSHOP_RESET' as action_type,
        'Insurance Workshop environment reset completed' as description;
    
    RETURN status;
END;
$$;

/* ================================================================================
CLEANUP SUMMARY AND RECOMMENDATIONS
================================================================================
*/

SELECT 
    '================================================================================' as SECTION,
    'INSURANCE WORKSHOP CLEANUP COMPLETED' as TITLE,
    '================================================================================' as SECTION2;

SELECT 
    'Next Steps:' as RECOMMENDATIONS,
    '1. Re-run 00_AUTOMATED_PIPELINE_SETUP.sql to restart the workshop' as STEP_1,
    '2. Upload data files to recreate the demo environment' as STEP_2,
    '3. Execute 01_DATA_QUALITY.sql for quality monitoring setup' as STEP_3,
    '4. Run 02_RISK_ANALYTICS_GOVERNANCE.sql for analytics and governance' as STEP_4,
    '5. Launch Streamlit dashboards for interactive demonstrations' as STEP_5;

SELECT 
    'Workshop Components Removed:' as REMOVED_COMPONENTS,
    '• Database: INSURANCE_WORKSHOP_DB' as COMPONENT_1,
    '• Warehouses: WORKSHOP_COMPUTE_WH, WORKSHOP_OPS_WH' as COMPONENT_2,
    '• All tables, views, and Dynamic Tables' as COMPONENT_3,
    '• Python and SQL UDFs' as COMPONENT_4,
    '• Data Metric Functions and schedules' as COMPONENT_5,
    '• Masking and Row Access Policies' as COMPONENT_6,
    '• Classification tags and governance structures' as COMPONENT_7,
    '• File formats and internal stages' as COMPONENT_8;

-- Final confirmation
SELECT 
    'CLEANUP STATUS: COMPLETE' as FINAL_STATUS,
    'Environment ready for fresh workshop setup' as READY_STATE,
    CURRENT_TIMESTAMP() as COMPLETION_TIME; 