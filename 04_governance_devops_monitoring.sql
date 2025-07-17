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
    SCHEDULE = '2 MINUTE'  -- Check every 2 minutes for demo purposes
    IF (EXISTS (
        SELECT 1 
        FROM (SELECT 0 AS table_missing)  -- Always returns 1 row
        WHERE (
            SELECT COUNT(*) 
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_SCHEMA = 'ANALYTICS' 
            AND TABLE_NAME = 'RISK_INTELLIGENCE_DASHBOARD'
            AND TABLE_TYPE = 'DYNAMIC TABLE'
        ) = 0
    ))
    THEN
        BEGIN
            -- Get current timestamp for the alert
            LET alert_time STRING := CURRENT_TIMESTAMP()::STRING;
            
            -- Send email notification using the integration
            CALL SYSTEM$SEND_EMAIL(
                'INSURANCE_EMAIL_ALERTS',
                <EMAIL@DOMAIN.COM>,  -- ensure to have the correct email here (must be verified)
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
    'Recovered Dashboard Status' as CHECK_TYPE,
    COUNT(*) as RECORD_COUNT,
    MAX(LAST_UPDATED) as LAST_REFRESH
FROM ANALYTICS.RISK_INTELLIGENCE_DASHBOARD;

/* ================================================================================
TIME TRAVEL FOR GOVERNANCE AUDITING
================================================================================
*/

-- Query historical governance policy applications using time travel
SELECT 
    'Before Drop' as TIME_PERIOD,
    COUNT(*) as RECORDS_COUNT,
    MAX(LAST_UPDATED) as LAST_UPDATE
FROM ANALYTICS.RISK_INTELLIGENCE_DASHBOARD 
AT(TIMESTAMP => $drop_demonstration_time);

-- Analyze query history for governance-related operations
SELECT 
    USER_NAME,
    ROLE_NAME,
    QUERY_TYPE,
    START_TIME,
    TOTAL_ELAPSED_TIME/1000 AS ELAPSED_SECONDS,
    EXECUTION_STATUS,
    LEFT(QUERY_TEXT, 100) AS QUERY_PREVIEW
FROM TABLE(INSURANCE_WORKSHOP_DB.INFORMATION_SCHEMA.QUERY_HISTORY())
WHERE QUERY_TEXT ILIKE '%RISK_INTELLIGENCE_DASHBOARD%'
    AND START_TIME > DATEADD('hour', -2, CURRENT_TIMESTAMP())
ORDER BY START_TIME DESC
LIMIT 10;

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
GOVERNANCE TESTING IN DEVELOPMENT ENVIRONMENT
================================================================================
*/

-- Test governance policies in development environment
USE ROLE GOVERNANCE_DEVELOPER;
USE DATABASE INSURANCE_WORKSHOP_DEV;

-- Test masking policies are inherited in clone
SELECT TOP 10
    BROKER_ID,
    POLICY_ANNUAL_PREMIUM,  -- Should be masked based on role
    CLAIM_AMOUNT_FILLED,    -- Should be masked based on role
    CUSTOMER_REGION
FROM ANALYTICS.RISK_INTELLIGENCE_DASHBOARD;

-- Test row access policies are inherited in clone
SELECT 
    CUSTOMER_REGION,
    COUNT(*) as VISIBLE_RECORDS
FROM ANALYTICS.RISK_INTELLIGENCE_DASHBOARD
GROUP BY CUSTOMER_REGION;

-- Switch back to admin for full view comparison
USE ROLE ACCOUNTADMIN;
USE DATABASE INSURANCE_WORKSHOP_DB;

SELECT 
    CUSTOMER_REGION,
    COUNT(*) as TOTAL_RECORDS
FROM ANALYTICS.RISK_INTELLIGENCE_DASHBOARD
GROUP BY CUSTOMER_REGION;

/* ================================================================================
GOVERNANCE MONITORING AND VALIDATION
================================================================================
*/

-- Monitor governance policy effectiveness
SELECT 
    'GOVERNANCE_POLICIES' as MONITORING_CATEGORY,
    COUNT(*) as TOTAL_POLICIES,
    'ACTIVE' as STATUS,
    CURRENT_TIMESTAMP() as LAST_CHECK
FROM INFORMATION_SCHEMA.POLICY_REFERENCES
WHERE POLICY_SCHEMA = 'GOVERNANCE'

UNION ALL

SELECT 
    'EMAIL_ALERTS' as MONITORING_CATEGORY,
    COUNT(*) as TOTAL_ALERTS,
    'ACTIVE' as STATUS,
    CURRENT_TIMESTAMP() as LAST_CHECK
FROM INFORMATION_SCHEMA.ALERTS
WHERE ALERT_SCHEMA = 'GOVERNANCE'

UNION ALL

SELECT 
    'DEV_ENVIRONMENTS' as MONITORING_CATEGORY,
    COUNT(*) as TOTAL_CLONES,
    'ACTIVE' as STATUS,
    CURRENT_TIMESTAMP() as LAST_CHECK
FROM INFORMATION_SCHEMA.DATABASES
WHERE DATABASE_NAME LIKE '%_DEV';

-- Validate time travel retention settings
SHOW PARAMETERS LIKE 'DATA_RETENTION_TIME_IN_DAYS' IN ACCOUNT;

-- Check recent governance-related queries
SELECT 
    QUERY_TYPE,
    COUNT(*) as QUERY_COUNT,
    MAX(START_TIME) as LAST_EXECUTION
FROM TABLE(INSURANCE_WORKSHOP_DB.INFORMATION_SCHEMA.QUERY_HISTORY())
WHERE QUERY_TEXT ILIKE '%GOVERNANCE%'
    AND START_TIME > DATEADD('day', -1, CURRENT_TIMESTAMP())
GROUP BY QUERY_TYPE
ORDER BY QUERY_COUNT DESC;

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

/* ================================================================================
GOVERNANCE DEVOPS AND MONITORING SETUP COMPLETE
================================================================================
Implementation Complete:
• Email Alerts: Automated monitoring of critical analytics tables with email notifications
• Time Travel: Drop/undrop demonstration with complete data recovery capabilities
• Zero-Copy Cloning: Cost-effective development environment with governance policy inheritance
• Governance Auditing: Historical query analysis and time-based data access for compliance

DevOps Capabilities:
• Environment Management: Instant development environment creation with zero storage cost
• Change Management: Time travel-based recovery and audit trails for all modifications
• Policy Testing: Safe governance policy testing in cloned environments
• Monitoring Integration: Automated alerting for critical infrastructure changes

Business Continuity Features:
• Proactive Monitoring: Real-time alerts for critical table drops or modifications
• Rapid Recovery: Sub-minute recovery using UNDROP and time travel capabilities
• Audit Compliance: Complete query history and time-based access for regulatory requirements
• Cost Efficiency: Zero-copy cloning minimizes development and testing costs

Workshop Demonstration Value:
• Live Email Alerts: Participants receive actual email notifications during demo
• Immediate Recovery: Real-time demonstration of data loss recovery without backups
• Cost Transparency: Clear visibility into storage efficiency of cloned environments
• Governance Inheritance: Show how policies automatically apply to development environments

Ready for: Complete workshop demonstration with live audience interaction
================================================================================
*/ 