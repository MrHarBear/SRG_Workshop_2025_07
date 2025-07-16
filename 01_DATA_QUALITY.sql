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

-- Python UDF for complex geographic territory validation
CREATE OR REPLACE DATA METRIC FUNCTION RAW_DATA.TERRITORY_COVERAGE_GAPS(
    INPUT_TABLE TABLE(TERRITORY ARRAY, OFFICE_LOCATION VARCHAR)
)
RETURNS NUMBER
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
HANDLER = 'calculate_coverage_gaps'
COMMENT = 'Count of brokers with geographic coverage gaps'
AS
$$
def calculate_coverage_gaps(input_table):
    import pandas as pd
    
    gap_count = 0
    
    for row in input_table.itertuples():
        territory = row.TERRITORY if row.TERRITORY else []
        office_location = row.OFFICE_LOCATION if row.OFFICE_LOCATION else ""
        
        # Check if office location is covered in territory
        if territory and office_location:
            territory_list = [str(t).strip() for t in territory if t]
            office_covered = any(office_location.lower() in t.lower() for t in territory_list)
            
            # Check for basic coverage requirements
            has_minimum_coverage = len(territory_list) >= 1
            
            if not office_covered or not has_minimum_coverage:
                gap_count += 1
    
    return gap_count
$$;

/* ================================================================================
SYSTEM DMF APPLICATION TO TABLES
================================================================================
*/

-- Set automated monitoring schedule for all tables
ALTER TABLE RAW_DATA.CUSTOMERS_RAW SET DATA_METRIC_SCHEDULE = '5 minute';
ALTER TABLE RAW_DATA.CLAIMS_RAW SET DATA_METRIC_SCHEDULE = '5 minute';
ALTER TABLE RAW_DATA.BROKERS_RAW SET DATA_METRIC_SCHEDULE = '5 minute';

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
ALTER TABLE RAW_DATA.BROKERS_RAW ADD DATA METRIC FUNCTION RAW_DATA.TERRITORY_COVERAGE_GAPS ON (TERRITORY, OFFICE_LOCATION);
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
        WHEN metric_name LIKE '%COVERAGE_GAPS%' THEN
            CASE WHEN value = 0 THEN 'EXCELLENT'
                 WHEN value <= 1 THEN 'GOOD'
                 WHEN value <= 3 THEN 'WARNING'
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
    COUNT(b.BROKER_ID) as customers_with_valid_brokers,
    COUNT(c.POLICY_NUMBER) - COUNT(b.BROKER_ID) as customers_missing_brokers,
    ROUND((COUNT(b.BROKER_ID) * 100.0) / COUNT(c.POLICY_NUMBER), 2) as integrity_percentage,
    CURRENT_TIMESTAMP() as measured_at
FROM RAW_DATA.CUSTOMERS_RAW c
LEFT JOIN RAW_DATA.BROKERS_RAW b ON c.BROKER_ID = b.BROKER_ID

UNION ALL

SELECT 
    'CUSTOMER_CLAIMS_INTEGRITY' as relationship_type,
    COUNT(DISTINCT c.POLICY_NUMBER) as total_customers,
    COUNT(DISTINCT cl.POLICY_NUMBER) as customers_with_claims,
    COUNT(DISTINCT c.POLICY_NUMBER) - COUNT(DISTINCT cl.POLICY_NUMBER) as customers_without_claims,
    ROUND((COUNT(DISTINCT cl.POLICY_NUMBER) * 100.0) / COUNT(DISTINCT c.POLICY_NUMBER), 2) as integrity_percentage,
    CURRENT_TIMESTAMP() as measured_at
FROM RAW_DATA.CUSTOMERS_RAW c
LEFT JOIN RAW_DATA.CLAIMS_RAW cl ON c.POLICY_NUMBER = cl.POLICY_NUMBER;

/* ================================================================================
MONITOR DATA METRIC FUNCTION RESULTS
================================================================================
*/

-- View active DMF configurations
SELECT 
    ref_entity_name as table_name,
    metric_name,
    schedule,
    schedule_status,
    arguments
FROM TABLE(INFORMATION_SCHEMA.DATA_METRIC_FUNCTION_REFERENCES(
    ref_entity_name => 'INSURANCE_WORKSHOP_DB.RAW_DATA.CUSTOMERS_RAW',
    ref_entity_domain => 'TABLE'
))
UNION ALL
SELECT 
    ref_entity_name as table_name,
    metric_name,
    schedule,
    schedule_status,
    arguments
FROM TABLE(INFORMATION_SCHEMA.DATA_METRIC_FUNCTION_REFERENCES(
    ref_entity_name => 'INSURANCE_WORKSHOP_DB.RAW_DATA.CLAIMS_RAW',
    ref_entity_domain => 'TABLE'
))
UNION ALL
SELECT 
    ref_entity_name as table_name,
    metric_name,
    schedule,
    schedule_status,
    arguments
FROM TABLE(INFORMATION_SCHEMA.DATA_METRIC_FUNCTION_REFERENCES(
    ref_entity_name => 'INSURANCE_WORKSHOP_DB.RAW_DATA.BROKERS_RAW',
    ref_entity_domain => 'TABLE'
));

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
        WHEN metric_name LIKE '%COVERAGE_GAPS%' AND value > 0 THEN 'TERRITORY_COVERAGE_ISSUE'
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

-- Grant access to monitor DMF results
GRANT USAGE ON SCHEMA SNOWFLAKE.LOCAL TO ROLE WORKSHOP_ANALYST;

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
DATA QUALITY MONITORING SETUP COMPLETE - STREAMLINED VERSION
================================================================================
Setup Complete:
• Custom DMFs: 3 functions (2 SQL + 1 Python) for essential business validation
• System DMFs: 9 functions across 3 tables for core data quality
• Monitoring: Automated 5-minute scheduling for all quality checks
• Views: 3 summary views for quality scoring and relationship validation
• Coverage: Customers (5 DMFs), Claims (3 DMFs), Brokers (5 DMFs)

Streamlined Quality Framework:
• Business Rules: Customer age validation, broker ID format validation
• Data Integrity: NULL checks, duplicate detection
• Operational Quality: Territory coverage validation
• Real-time Scoring: Automated quality scoring with status classification

Custom DMF Focus:
• INVALID_CUSTOMER_AGE_COUNT: Validates customer age within 18-85 range
• INVALID_BROKER_ID_COUNT: Ensures broker IDs follow BRK### format pattern
• TERRITORY_COVERAGE_GAPS: Complex Python validation for geographic coverage

Ready for: Phase 3 - Advanced Analytics and Governance implementation
================================================================================
*/ 