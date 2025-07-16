/* ================================================================================
INSURANCE WORKSHOP - DATA QUALITY MONITORING
================================================================================
Purpose: Comprehensive data quality monitoring with DMFs for three-entity model
Scope: Custom and system DMFs, automated monitoring, quality scoring
Entities: CUSTOMERS, CLAIMS, BROKERS with cross-entity validation
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



/* ================================================================================
SYSTEM DMF APPLICATION TO TABLES
===================================================================================
*/
-- Set automated monitoring schedule for all tables
-- Set the data metric function to run when a general DML operation, such as inserting a new row, modifies the table:
ALTER TABLE RAW_DATA.CUSTOMERS_RAW SET DATA_METRIC_SCHEDULE = 'TRIGGER_ON_CHANGES';
                                                            -- Time interval:
                                                            -- '5 minute';
                                                            -- At 8:00 AM on weekdays only:
                                                            -- 'USING CRON 0 8 * * MON,TUE,WED,THU,FRI UTC';
ALTER TABLE RAW_DATA.CLAIMS_RAW SET DATA_METRIC_SCHEDULE = 'TRIGGER_ON_CHANGES';
ALTER TABLE RAW_DATA.BROKERS_RAW SET DATA_METRIC_SCHEDULE = 'TRIGGER_ON_CHANGES';

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

-- BROKERS TABLE: Custom + System DMFs
ALTER TABLE RAW_DATA.BROKERS_RAW ADD DATA METRIC FUNCTION RAW_DATA.INVALID_BROKER_ID_COUNT ON (BROKER_ID);
ALTER TABLE RAW_DATA.BROKERS_RAW ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.NULL_COUNT ON (BROKER_ID);
ALTER TABLE RAW_DATA.BROKERS_RAW ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.DUPLICATE_COUNT ON (BROKER_ID);
ALTER TABLE RAW_DATA.BROKERS_RAW ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.ROW_COUNT ON ();

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
    AND table_name IN ('CUSTOMERS_RAW', 'CLAIMS_RAW', 'BROKERS_RAW');

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
    'CUSTOMER_BROKER_INTEGRITY' as relationship_type,
    COUNT(c.POLICY_NUMBER) as total_customers,
    COUNT(b.BROKER_ID) as valid_relationships,
    COUNT(c.POLICY_NUMBER) - COUNT(b.BROKER_ID) as missing_relationships,
    ROUND((COUNT(b.BROKER_ID) * 100.0) / COUNT(c.POLICY_NUMBER), 2) as integrity_percentage,
    CURRENT_TIMESTAMP() as measured_at
FROM RAW_DATA.CUSTOMERS_RAW c
LEFT JOIN RAW_DATA.BROKERS_RAW b ON c.BROKER_ID = b.BROKER_ID

UNION ALL

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

-- Example 4: Find duplicate broker IDs
CREATE OR REPLACE VIEW RAW_DATA.BROKERS_WITH_DUPLICATE_IDS AS
SELECT *
FROM TABLE(SYSTEM$DATA_METRIC_SCAN(
    REF_ENTITY_NAME => 'INSURANCE_WORKSHOP_DB.RAW_DATA.BROKERS_RAW',
    METRIC_NAME => 'snowflake.core.duplicate_count',
    ARGUMENT_NAME => 'BROKER_ID'
));

/* ================================================================================
DATA REMEDIATION EXAMPLES - DEMO PURPOSES
================================================================================
Show how SYSTEM$DATA_METRIC_SCAN can be used for data fixing
Note: These are demo queries - run carefully in production
*/

-- Demo: Count of records that would be affected by remediation
SELECT 
    'NULL_POLICY_NUMBERS_CUSTOMERS' as issue_type,
    COUNT(*) as affected_records
FROM RAW_DATA.CUSTOMERS_WITH_NULL_POLICY_NUMBERS

UNION ALL

SELECT 
    'DUPLICATE_POLICY_NUMBERS_CUSTOMERS' as issue_type,
    COUNT(*) as affected_records
FROM RAW_DATA.CUSTOMERS_WITH_DUPLICATE_POLICIES

UNION ALL

SELECT 
    'NULL_POLICY_NUMBERS_CLAIMS' as issue_type,
    COUNT(*) as affected_records
FROM RAW_DATA.CLAIMS_WITH_NULL_POLICY_NUMBERS

UNION ALL

SELECT 
    'DUPLICATE_BROKER_IDS' as issue_type,
    COUNT(*) as affected_records
FROM RAW_DATA.BROKERS_WITH_DUPLICATE_IDS;

-- Example remediation query (commented for safety)
/*
-- Demo: How to fix NULL policy numbers (example only)
UPDATE INSURANCE_WORKSHOP_DB.RAW_DATA.CUSTOMERS_RAW
SET POLICY_NUMBER = 'POL_' || UNIFORM(1000000, 9999999, RANDOM())::STRING
WHERE POLICY_NUMBER IN (
    SELECT POLICY_NUMBER 
    FROM RAW_DATA.CUSTOMERS_WITH_NULL_POLICY_NUMBERS
);
*/

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
    AND table_name IN ('CUSTOMERS_RAW', 'CLAIMS_RAW', 'BROKERS_RAW')
ORDER BY measurement_time DESC, table_name, metric_name;

/* ================================================================================
GRANT ACCESS FOR WORKSHOP ROLES
================================================================================
*/

-- Grant access to quality monitoring views
GRANT SELECT ON VIEW RAW_DATA.QUALITY_MONITORING_SUMMARY TO ROLE WORKSHOP_ANALYST;
GRANT SELECT ON VIEW RAW_DATA.ENTITY_QUALITY_SCORES TO ROLE WORKSHOP_ANALYST;
GRANT SELECT ON VIEW RAW_DATA.RELATIONSHIP_QUALITY_METRICS TO ROLE WORKSHOP_ANALYST;

-- Grant access to issue identification views
GRANT SELECT ON VIEW RAW_DATA.CUSTOMERS_WITH_NULL_POLICY_NUMBERS TO ROLE WORKSHOP_ANALYST;
GRANT SELECT ON VIEW RAW_DATA.CUSTOMERS_WITH_DUPLICATE_POLICIES TO ROLE WORKSHOP_ANALYST;
GRANT SELECT ON VIEW RAW_DATA.CLAIMS_WITH_NULL_POLICY_NUMBERS TO ROLE WORKSHOP_ANALYST;
GRANT SELECT ON VIEW RAW_DATA.BROKERS_WITH_DUPLICATE_IDS TO ROLE WORKSHOP_ANALYST;

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
    customers_with_valid_brokers as valid_relationships,
    customers_missing_brokers as missing_relationships,
    integrity_percentage,
    CASE 
        WHEN integrity_percentage >= 98 THEN 'EXCELLENT'
        WHEN integrity_percentage >= 95 THEN 'GOOD'
        WHEN integrity_percentage >= 90 THEN 'NEEDS_ATTENTION'
        ELSE 'CRITICAL'
    END as integrity_grade
FROM RAW_DATA.RELATIONSHIP_QUALITY_METRICS;

/* ================================================================================
DATA QUALITY MONITORING SETUP COMPLETE - ENHANCED WITH REMEDIATION
================================================================================
Setup Complete:
• Custom DMFs: 2 SQL functions for essential business validation
• System DMFs: 9 functions across 3 tables for core data quality
• Monitoring: TRIGGER_ON_CHANGES scheduling for real-time quality checks
• Views: 7 views (3 quality scoring + 4 issue identification) for comprehensive monitoring
• Coverage: Customers (5 DMFs), Claims (3 DMFs), Brokers (4 DMFs)

Enhanced Quality Framework:
• Business Rules: Customer age validation, broker ID format validation
• Data Integrity: NULL checks, duplicate detection with SYSTEM$DATA_METRIC_SCAN
• Real-time Scoring: Automated quality scoring with status classification
• Data Remediation: Record-level issue identification for targeted fixes

Custom DMF Focus:
• INVALID_CUSTOMER_AGE_COUNT: Validates customer age within 18-85 range
• INVALID_BROKER_ID_COUNT: Ensures broker IDs follow BRK### format pattern

Enhanced Capabilities (NEW):
• SYSTEM$DATA_METRIC_SCAN: Identify specific problematic records for remediation
• SHOW DATA METRIC FUNCTIONS: Comprehensive DMF inventory and management
• Issue Identification Views: Pre-built views for common data quality problems
• Remediation Examples: Sample SQL for fixing identified data quality issues

Quality Issue Detection Views:
• CUSTOMERS_WITH_NULL_POLICY_NUMBERS: Records needing policy number assignment
• CUSTOMERS_WITH_DUPLICATE_POLICIES: Duplicate policy records for cleanup
• CLAIMS_WITH_NULL_POLICY_NUMBERS: Claims missing policy associations
• BROKERS_WITH_DUPLICATE_IDS: Duplicate broker records for resolution

Demo Features:
• Record-level issue identification using SYSTEM$DATA_METRIC_SCAN
• Targeted remediation capabilities with example SQL
• Real-time monitoring with TRIGGER_ON_CHANGES scheduling
• Comprehensive quality scoring and status classification

Ready for: Phase 3 - Advanced Analytics and Governance implementation
================================================================================
*/ 