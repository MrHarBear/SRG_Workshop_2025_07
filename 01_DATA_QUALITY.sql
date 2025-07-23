/* ================================================================================
INSURANCE WORKSHOP - DATA QUALITY MONITORING
================================================================================
Purpose: Comprehensive data quality monitoring with DMFs for two-entity model
Scope: Custom and system DMFs, automated monitoring, quality scoring
Entities: CUSTOMERS, CLAIMS with cross-entity validation
================================================================================
*/

USE DATABASE INSURANCE_WORKSHOP_DB;
USE SCHEMA RAW_DATA;
USE WAREHOUSE WORKSHOP_COMPUTE_WH;
USE ROLE ACCOUNTADMIN;

/* ================================================================================
CUSTOM DATA METRIC FUNCTIONS - MIXED LANGUAGE APPROACH
================================================================================
*/

-- SQL UDFs for simple validation logic (high performance)

-- Customer age validation using SQL for efficiency
CREATE OR REPLACE DATA METRIC FUNCTION RAW_DATA.INVALID_CUSTOMER_AGE_COUNT(
    INPUT_TABLE TABLE(AGE NUMBER)
)
RETURNS NUMBER
LANGUAGE SQL
COMMENT = 'Count of customers with invalid age (outside 18-85 range)'
AS
'SELECT COUNT_IF(
    AGE IS NOT NULL 
    AND (AGE < 18 OR AGE > 85)
) FROM INPUT_TABLE';

-- Broker ID format validation using SQL for pattern checking
CREATE OR REPLACE DATA METRIC FUNCTION RAW_DATA.INVALID_BROKER_ID_COUNT(
    INPUT_TABLE TABLE(BROKER_ID VARCHAR)
)
RETURNS NUMBER
LANGUAGE SQL
COMMENT = 'Count of broker IDs that do not follow BRK### format'
AS
'SELECT COUNT_IF(
    BROKER_ID IS NOT NULL 
    AND NOT REGEXP_LIKE(BROKER_ID, ''^BRK[0-9]{3}$'')
) FROM INPUT_TABLE';


-- AD-HOC QUALITY TESTS
-- Test specific data quality scenarios manually
SELECT 
    'AGE_DISTRIBUTION_ANALYSIS' as analysis_type,
    COUNT(*) as total_customers,
    COUNT(CASE WHEN AGE < 18 THEN 1 END) as under_age_customers,
    COUNT(CASE WHEN AGE > 85 THEN 1 END) as over_age_customers,
    COUNT(CASE WHEN AGE IS NULL THEN 1 END) as null_age_customers,
    RAW_DATA.INVALID_CUSTOMER_AGE_COUNT(SELECT AGE FROM RAW_DATA.CUSTOMERS_RAW) as dmf_invalid_age_count
FROM RAW_DATA.CUSTOMERS_RAW;

-- Test broker ID format patterns manually
SELECT 
    'BROKER_ID_PATTERN_ANALYSIS' as analysis_type,
    COUNT(*) as total_broker_ids,
    COUNT(CASE WHEN REGEXP_LIKE(BROKER_ID, '^BRK[0-9]{3}$') THEN 1 END) as valid_format_ids,
    COUNT(CASE WHEN NOT REGEXP_LIKE(BROKER_ID, '^BRK[0-9]{3}$') AND BROKER_ID IS NOT NULL THEN 1 END) as invalid_format_ids,
    COUNT(CASE WHEN BROKER_ID IS NULL THEN 1 END) as null_broker_ids,
    RAW_DATA.INVALID_BROKER_ID_COUNT(SELECT BROKER_ID FROM RAW_DATA.CUSTOMERS_RAW) as dmf_invalid_broker_count
FROM RAW_DATA.CUSTOMERS_RAW;

/* ================================================================================
SYSTEM DMF APPLICATION TO TABLES
===================================================================================
*/
-- Set automated monitoring schedule for customer and claims tables only
ALTER TABLE RAW_DATA.CUSTOMERS_RAW SET DATA_METRIC_SCHEDULE = '5 minute';
-- Set the data metric function schedule to run at 8:00 AM on weekdays only: 'USING CRON 0 8 * * MON,TUE,WED,THU,FRI UTC';
-- DATA_METRIC_SCHEDULE = 'TRIGGER_ON_CHANGES';
ALTER TABLE RAW_DATA.CLAIMS_RAW SET DATA_METRIC_SCHEDULE = '5 minute';

-- CUSTOMERS TABLE: Custom + System DMFs
ALTER TABLE RAW_DATA.CUSTOMERS_RAW ADD DATA METRIC FUNCTION RAW_DATA.INVALID_CUSTOMER_AGE_COUNT ON (AGE);
ALTER TABLE RAW_DATA.CUSTOMERS_RAW ADD DATA METRIC FUNCTION RAW_DATA.INVALID_BROKER_ID_COUNT ON (BROKER_ID);
ALTER TABLE RAW_DATA.CUSTOMERS_RAW ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.NULL_COUNT ON (POLICY_NUMBER);
ALTER TABLE RAW_DATA.CUSTOMERS_RAW ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.DUPLICATE_COUNT ON (POLICY_NUMBER);
ALTER TABLE RAW_DATA.CUSTOMERS_RAW ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.ROW_COUNT ON ();

-- CLAIMS TABLE: System DMFs only
ALTER TABLE RAW_DATA.CLAIMS_RAW ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.NULL_COUNT ON (POLICY_NUMBER);
ALTER TABLE RAW_DATA.CLAIMS_RAW ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.DUPLICATE_COUNT ON (POLICY_NUMBER);
ALTER TABLE RAW_DATA.CLAIMS_RAW ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.ROW_COUNT ON ();

/* ================================================================================
QUALITY DASHBOARD DATA PREPARATION
================================================================================
*/

-- Create view for real-time quality monitoring dashboard
CREATE OR REPLACE VIEW RAW_DATA.QUALITY_MONITORING_SUMMARY AS
SELECT 
    table_name,
    metric_name,
    value as metric_value,
    measurement_time,
    change_commit_time,
    CASE 
        WHEN metric_name LIKE '%NULL_COUNT%' THEN 
            CASE WHEN value = 0 THEN 'EXCELLENT' 
                 WHEN value <= 5 THEN 'GOOD'
                 WHEN value <= 20 THEN 'WARNING'
                 ELSE 'CRITICAL' END
        WHEN metric_name LIKE '%DUPLICATE_COUNT%' THEN
            CASE WHEN value = 0 THEN 'EXCELLENT'
                 WHEN value <= 3 THEN 'GOOD'
                 WHEN value <= 10 THEN 'WARNING'
                 ELSE 'CRITICAL' END
        WHEN metric_name LIKE '%INVALID%' THEN
            CASE WHEN value = 0 THEN 'EXCELLENT'
                 WHEN value <= 2 THEN 'GOOD'
                 WHEN value <= 10 THEN 'WARNING'
                 ELSE 'CRITICAL' END
        ELSE 'UNKNOWN'
    END as quality_status
FROM SNOWFLAKE.LOCAL.DATA_QUALITY_MONITORING_RESULTS
WHERE table_database = 'INSURANCE_WORKSHOP_DB'
    AND table_schema = 'RAW_DATA'
    AND table_name IN ('CUSTOMERS_RAW', 'CLAIMS_RAW');

-- Create aggregated quality score view
CREATE OR REPLACE VIEW RAW_DATA.ENTITY_QUALITY_SCORES AS
SELECT 
    table_name as entity_name,
    COUNT(*) as total_metrics,
    COUNT(CASE WHEN quality_status = 'EXCELLENT' THEN 1 END) as excellent_count,
    COUNT(CASE WHEN quality_status = 'GOOD' THEN 1 END) as good_count,
    COUNT(CASE WHEN quality_status = 'WARNING' THEN 1 END) as warning_count,
    COUNT(CASE WHEN quality_status = 'CRITICAL' THEN 1 END) as critical_count,
    ROUND(
        (COUNT(CASE WHEN quality_status = 'EXCELLENT' THEN 1 END) * 100 +
         COUNT(CASE WHEN quality_status = 'GOOD' THEN 1 END) * 80 +
         COUNT(CASE WHEN quality_status = 'WARNING' THEN 1 END) * 60 +
         COUNT(CASE WHEN quality_status = 'CRITICAL' THEN 1 END) * 20) / 
        (COUNT(*) * 100.0) * 100, 1
    ) as overall_quality_score,
    MAX(measurement_time) as last_measured
FROM RAW_DATA.QUALITY_MONITORING_SUMMARY
GROUP BY table_name;

-- Create cross-entity relationship quality view
CREATE OR REPLACE VIEW RAW_DATA.RELATIONSHIP_QUALITY_METRICS AS
SELECT 
    'CUSTOMER_CLAIMS_INTEGRITY' as relationship_type,
    COUNT(DISTINCT c.POLICY_NUMBER) as total_customers,
    COUNT(DISTINCT cl.POLICY_NUMBER) as valid_relationships,
    COUNT(DISTINCT c.POLICY_NUMBER) - COUNT(DISTINCT cl.POLICY_NUMBER) as missing_relationships,
    ROUND((COUNT(DISTINCT cl.POLICY_NUMBER) * 100.0) / COUNT(DISTINCT c.POLICY_NUMBER), 2) as integrity_percentage,
    CURRENT_TIMESTAMP() as measured_at
FROM RAW_DATA.CUSTOMERS_RAW c
LEFT JOIN RAW_DATA.CLAIMS_RAW cl ON c.POLICY_NUMBER = cl.POLICY_NUMBER;

/* ================================================================================
ENHANCED DMF MONITORING AND DATA REMEDIATION
================================================================================
*/

-- List all DMFs in the account for comprehensive overview
SHOW DATA METRIC FUNCTIONS IN ACCOUNT;

-- Advanced monitoring: Show specific DMF assignments per table
SELECT * FROM TABLE(
    INFORMATION_SCHEMA.DATA_METRIC_FUNCTION_REFERENCES(
        REF_ENTITY_NAME => 'INSURANCE_WORKSHOP_DB.RAW_DATA.CUSTOMERS_RAW',
        REF_ENTITY_DOMAIN => 'TABLE'
    )
);

/* ================================================================================
DATA QUALITY ISSUE IDENTIFICATION USING SYSTEM$DATA_METRIC_SCAN
================================================================================
Demonstrate how to identify specific problematic records using Snowflake's 
SYSTEM$DATA_METRIC_SCAN function for data remediation
*/

-- Example 1: Find specific records with NULL policy numbers
CREATE OR REPLACE VIEW RAW_DATA.CUSTOMERS_WITH_NULL_POLICY_NUMBERS AS
SELECT *
FROM TABLE(SYSTEM$DATA_METRIC_SCAN(
    REF_ENTITY_NAME => 'INSURANCE_WORKSHOP_DB.RAW_DATA.CUSTOMERS_RAW',
    METRIC_NAME => 'snowflake.core.null_count',
    ARGUMENT_NAME => 'POLICY_NUMBER'
));

-- Example 2: Find duplicate policy numbers in customers table
CREATE OR REPLACE VIEW RAW_DATA.CUSTOMERS_WITH_DUPLICATE_POLICIES AS
SELECT *
FROM TABLE(SYSTEM$DATA_METRIC_SCAN(
    REF_ENTITY_NAME => 'INSURANCE_WORKSHOP_DB.RAW_DATA.CUSTOMERS_RAW',
    METRIC_NAME => 'snowflake.core.duplicate_count',
    ARGUMENT_NAME => 'POLICY_NUMBER'
));

-- Example 3: Find claims with NULL policy numbers
CREATE OR REPLACE VIEW RAW_DATA.CLAIMS_WITH_NULL_POLICY_NUMBERS AS
SELECT *
FROM TABLE(SYSTEM$DATA_METRIC_SCAN(
    REF_ENTITY_NAME => 'INSURANCE_WORKSHOP_DB.RAW_DATA.CLAIMS_RAW',
    METRIC_NAME => 'snowflake.core.null_count',
    ARGUMENT_NAME => 'POLICY_NUMBER'
));

-- Example 4: Find duplicate policy numbers in claims table
CREATE OR REPLACE VIEW RAW_DATA.CLAIMS_WITH_DUPLICATE_POLICIES AS
SELECT *
FROM TABLE(SYSTEM$DATA_METRIC_SCAN(
    REF_ENTITY_NAME => 'INSURANCE_WORKSHOP_DB.RAW_DATA.CLAIMS_RAW',
    METRIC_NAME => 'snowflake.core.duplicate_count',
    ARGUMENT_NAME => 'POLICY_NUMBER'
));

/* ================================================================================
MONITOR DATA METRIC FUNCTION RESULTS
================================================================================
*/
-- View current quality monitoring results
SELECT 
    measurement_time,
    change_commit_time,
    table_name,
    metric_name,
    value,
    CASE 
        WHEN metric_name LIKE '%INVALID%' AND value > 0 THEN 'ATTENTION_REQUIRED'
        WHEN metric_name LIKE '%NULL_COUNT%' AND value > 0 THEN 'DATA_COMPLETENESS_ISSUE'
        WHEN metric_name LIKE '%DUPLICATE%' AND value > 0 THEN 'DATA_UNIQUENESS_ISSUE'
        ELSE 'OK'
    END as status_flag
FROM SNOWFLAKE.LOCAL.DATA_QUALITY_MONITORING_RESULTS
WHERE table_database = 'INSURANCE_WORKSHOP_DB'
    AND table_schema = 'RAW_DATA'
    AND table_name IN ('CUSTOMERS_RAW', 'CLAIMS_RAW')
ORDER BY measurement_time DESC, table_name, metric_name;

/* ================================================================================
QUALITY VALIDATION REPORT
================================================================================
*/

-- Generate initial quality assessment
SELECT 
    'DATA_QUALITY_ASSESSMENT' as report_type,
    entity_name,
    total_metrics,
    overall_quality_score,
    CASE 
        WHEN overall_quality_score >= 90 THEN 'EXCELLENT'
        WHEN overall_quality_score >= 75 THEN 'GOOD'
        WHEN overall_quality_score >= 60 THEN 'NEEDS_ATTENTION'
        ELSE 'CRITICAL'
    END as quality_grade,
    last_measured
FROM RAW_DATA.ENTITY_QUALITY_SCORES
ORDER BY overall_quality_score DESC;

-- Show relationship integrity status
SELECT 
    relationship_type,
    total_customers,
    valid_relationships,
    missing_relationships,
    integrity_percentage,
    CASE 
        WHEN integrity_percentage >= 98 THEN 'EXCELLENT'
        WHEN integrity_percentage >= 95 THEN 'GOOD'
        WHEN integrity_percentage >= 90 THEN 'NEEDS_ATTENTION'
        ELSE 'CRITICAL'
    END as integrity_grade
FROM RAW_DATA.RELATIONSHIP_QUALITY_METRICS;