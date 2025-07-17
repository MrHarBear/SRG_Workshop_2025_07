/* ================================================================================
INSURANCE WORKSHOP - GOVERNANCE DEVOPS AND MONITORING
================================================================================
Purpose: Advanced governance operations including alerts, monitoring, and DevOps workflows
Scope: Email notifications, time travel, zero-copy cloning, governance auditing
Dependencies: Requires completion of 03_GOVERNANCE_POLICIES.sql
================================================================================
*/

USE DATABASE INSURANCE_WORKSHOP_DB;
USE SCHEMA GOVERNANCE;
USE WAREHOUSE WORKSHOP_COMPUTE_WH;
USE ROLE ACCOUNTADMIN;

/* ================================================================================
EMAIL NOTIFICATION INTEGRATION SETUP
================================================================================
*/

-- Create email notification integration for governance alerts
CREATE OR REPLACE NOTIFICATION INTEGRATION INSURANCE_EMAIL_ALERTS
    TYPE = EMAIL
    ENABLED = TRUE
    COMMENT = 'Email notification system for insurance workshop governance alerts';

-- Grant usage to appropriate roles
GRANT USAGE ON INTEGRATION INSURANCE_EMAIL_ALERTS TO ROLE ACCOUNTADMIN;
GRANT USAGE ON INTEGRATION INSURANCE_EMAIL_ALERTS TO ROLE WORKSHOP_ANALYST;

-- Grant alert execution privileges
GRANT EXECUTE ALERT ON ACCOUNT TO ROLE ACCOUNTADMIN;
GRANT EXECUTE MANAGED ALERT ON ACCOUNT TO ROLE ACCOUNTADMIN;

/* ================================================================================
CRITICAL TABLE MONITORING ALERT
================================================================================
*/

-- Create alert to monitor critical analytics table existence
CREATE OR REPLACE ALERT GOVERNANCE.RISK_DASHBOARD_MONITOR
    SCHEDULE = '1 MINUTE'  -- Check every 2 minutes for demo purposes
    IF (EXISTS (
        SELECT 1 
        FROM (SELECT 0 AS table_missing)  -- Always returns 1 row
        WHERE (
            SELECT COUNT(*) 
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_SCHEMA = 'ANALYTICS' 
            AND TABLE_NAME = 'RISK_INTELLIGENCE_DASHBOARD'
        ) = 0
    ))
    THEN
        BEGIN
            -- Get current timestamp for the alert
            LET alert_time STRING := CURRENT_TIMESTAMP()::STRING;
            
            -- Send email notification using the integration
            CALL SYSTEM$SEND_EMAIL(
                'INSURANCE_EMAIL_ALERTS',
                'harley.chen@snowflake.com',  -- ensure to have the correct email here (must be verified)
                'CRITICAL ALERT: Risk Intelligence Dashboard Missing',
                'ALERT TRIGGERED AT: ' || :alert_time || CHR(10) || CHR(10) ||
                'The critical RISK_INTELLIGENCE_DASHBOARD table has been dropped or is missing.' || CHR(10) || 
                'This table contains essential insurance analytics and governance data.' || CHR(10) || CHR(10) ||
                'IMMEDIATE ACTION REQUIRED:' || CHR(10) ||
                '1. Investigate the cause of the table drop' || CHR(10) ||
                '2. Use Time Travel to recover the table if accidentally dropped' || CHR(10) ||
                '3. Verify data integrity and governance policies after recovery' || CHR(10) || CHR(10) ||
                'Database: INSURANCE_WORKSHOP_DB' || CHR(10) ||
                'Schema: ANALYTICS' || CHR(10) ||
                'Table: RISK_INTELLIGENCE_DASHBOARD' || CHR(10) || CHR(10) ||
                'This is an automated alert from the Insurance Workshop governance system.'
            );
        END;

-- Resume the alert to make it active
ALTER ALERT GOVERNANCE.RISK_DASHBOARD_MONITOR RESUME;

-- Verify alert is active
SHOW ALERTS IN SCHEMA GOVERNANCE;

/* ================================================================================
ZERO-COPY CLONE FOR GOVERNANCE TESTING
================================================================================
*/

-- Create development environment using zero-copy cloning
CREATE OR REPLACE DATABASE INSURANCE_WORKSHOP_DEV 
    CLONE INSURANCE_WORKSHOP_DB
    COMMENT = 'Development environment clone for governance testing';

-- Create developer role for testing governance policies
CREATE ROLE IF NOT EXISTS GOVERNANCE_DEVELOPER
    COMMENT = 'Developer role for governance testing in cloned environment';

-- Grant basic privileges to developer role
GRANT USAGE ON WAREHOUSE WORKSHOP_COMPUTE_WH TO ROLE GOVERNANCE_DEVELOPER;

GRANT USAGE ON DATABASE INSURANCE_WORKSHOP_DEV TO ROLE GOVERNANCE_DEVELOPER;
GRANT USAGE ON ALL SCHEMAS IN DATABASE INSURANCE_WORKSHOP_DEV TO ROLE GOVERNANCE_DEVELOPER;
GRANT USAGE ON FUTURE SCHEMAS IN DATABASE INSURANCE_WORKSHOP_DEV TO ROLE GOVERNANCE_DEVELOPER;
-- Existing objects
GRANT SELECT ON ALL TABLES IN DATABASE INSURANCE_WORKSHOP_DEV TO ROLE GOVERNANCE_DEVELOPER;
GRANT ALL PRIVILEGES ON ALL TABLES IN DATABASE INSURANCE_WORKSHOP_DEV TO ROLE GOVERNANCE_DEVELOPER;
GRANT ALL PRIVILEGES ON ALL VIEWS IN DATABASE INSURANCE_WORKSHOP_DEV TO ROLE GOVERNANCE_DEVELOPER;

-- Capture the current Snowflake user in a session variable
SET CURRENT_EXEC_USER = CURRENT_USER();
GRANT ROLE GOVERNANCE_DEVELOPER TO USER IDENTIFIER($CURRENT_EXEC_USER);

-- Verify clone storage efficiency (should show minimal initial storage usage)
-- here we are showing the sizes of the tables that we've just created, as well as the cloned table
-- we can see that the cloned tables takes up no storage!
SELECT 
    TABLE_NAME, 
    CLONE_GROUP_ID, 
    TABLE_CREATED,
    ((ACTIVE_BYTES/1024)) AS STORAGE_USAGE_KB
FROM INSURANCE_WORKSHOP_DEV.INFORMATION_SCHEMA.TABLE_STORAGE_METRICS
WHERE TABLE_NAME LIKE 'RISK_INTELLIGENCE_DASHBOARD'
ORDER BY TABLE_CREATED DESC
LIMIT 1;

/* ================================================================================
DROP/UNDROP DEMONSTRATION WITH TIME TRAVEL
================================================================================
*/

-- Verify current table status before demonstration
SELECT 
    'Current Dashboard Status' as CHECK_TYPE,
    COUNT(*) as RECORD_COUNT,
    MAX(LAST_UPDATED) as LAST_REFRESH
FROM ANALYTICS.RISK_INTELLIGENCE_DASHBOARD;

-- Record timestamp for time travel reference
SET drop_demonstration_time = CURRENT_TIMESTAMP();

-- WARNING: This drops the table for demonstration purposes
-- In production, never intentionally drop critical tables
DROP TABLE ANALYTICS.RISK_INTELLIGENCE_DASHBOARD;

-- Verify table no longer exists
SELECT COUNT(*) AS table_exists_count
FROM INFORMATION_SCHEMA.TABLES 
WHERE TABLE_SCHEMA = 'ANALYTICS' 
    AND TABLE_NAME = 'RISK_INTELLIGENCE_DASHBOARD';

-- Monitor alert execution (wait 2-3 minutes for alert to trigger)
SELECT *
FROM TABLE(INFORMATION_SCHEMA.ALERT_HISTORY(
    SCHEDULED_TIME_RANGE_START => DATEADD('minute', -5, CURRENT_TIMESTAMP())
))
WHERE NAME = 'RISK_DASHBOARD_MONITOR'
ORDER BY SCHEDULED_TIME DESC;

-- RECOVERY: Restore using UNDROP TABLE
UNDROP TABLE ANALYTICS.RISK_INTELLIGENCE_DASHBOARD;

-- Verify successful recovery
SELECT 
*
FROM ANALYTICS.RISK_INTELLIGENCE_DASHBOARD;

/* ================================================================================
TIME TRAVEL FOR GOVERNANCE AUDITING
================================================================================
*/

-- Query historical governance policy applications using time travel
SELECT 
    'Before Drop' as TIME_PERIOD,
    COUNT(*) as RECORDS_COUNT
FROM ANALYTICS.RISK_INTELLIGENCE_DASHBOARD 
AT(TIMESTAMP => $drop_demonstration_time);

-- Time travel query to see data at specific point in time
SELECT 
    CUSTOMER_SEGMENT,
    COUNT(*) as CUSTOMER_COUNT,
    AVG(CUSTOMER_RISK_SCORE) as AVG_RISK_SCORE
FROM ANALYTICS.RISK_INTELLIGENCE_DASHBOARD 
AT(TIMESTAMP => $drop_demonstration_time)
GROUP BY CUSTOMER_SEGMENT
ORDER BY AVG_RISK_SCORE DESC;

-- Additional time travel example using BEFORE statement (for reference)
-- This shows how to query data before a specific operation
-- (Uncomment to use after identifying a specific query_id)
/*
SET query_id = 
    (
    SELECT TOP 1
        query_id
    FROM TABLE(INSURANCE_WORKSHOP_DB.INFORMATION_SCHEMA.QUERY_HISTORY())
    WHERE 1=1
        AND query_type = 'DROP_TABLE'
        AND query_text LIKE '%RISK_INTELLIGENCE_DASHBOARD%'
    ORDER BY start_time DESC
    );

-- Query data as it existed before the DROP statement
SELECT COUNT(*) as records_before_drop
FROM ANALYTICS.RISK_INTELLIGENCE_DASHBOARD
BEFORE(STATEMENT => $query_id);
*/

/* ================================================================================
CLEANUP PROCEDURES (OPTIONAL)
================================================================================
*/

-- Suspend and clean up alert (uncomment to execute)
-- ALTER ALERT GOVERNANCE.RISK_DASHBOARD_MONITOR SUSPEND;
-- DROP ALERT GOVERNANCE.RISK_DASHBOARD_MONITOR;

-- Clean up development environment (uncomment to execute)
-- DROP DATABASE INSURANCE_WORKSHOP_DEV;
-- DROP ROLE GOVERNANCE_DEVELOPER;

-- Clean up email integration (uncomment to execute)
-- DROP INTEGRATION INSURANCE_EMAIL_ALERTS;