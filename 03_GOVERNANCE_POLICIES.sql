/* ================================================================================
INSURANCE WORKSHOP - GOVERNANCE POLICIES
================================================================================
Purpose: Comprehensive governance implementation for insurance analytics platform
Scope: Classification, masking policies, row access policies, secure data sharing
Dependencies: Requires completion of 01_DATA_QUALITY.sql and 02_RISK_ANALYTICS.sql
================================================================================
*/

USE DATABASE INSURANCE_WORKSHOP_DB;
USE SCHEMA GOVERNANCE;
USE WAREHOUSE WORKSHOP_COMPUTE_WH;
USE ROLE ACCOUNTADMIN;

/* ================================================================================
AUTOMATED CLASSIFICATION AND TAGGING
================================================================================
*/

-- Create classification profile for automated sensitive data detection
CREATE OR REPLACE SNOWFLAKE.DATA_PRIVACY.CLASSIFICATION_PROFILE INSURANCE_AUTO_CLASSIFICATION(
    {
        'minimum_object_age_for_classification_days': 0,
        'maximum_classification_validity_days': 30,
        'auto_tag': true
    }
);

-- Apply classification to analytics schema
ALTER SCHEMA ANALYTICS SET CLASSIFICATION_PROFILE = 'GOVERNANCE.INSURANCE_AUTO_CLASSIFICATION';

-- Run classification on key analytics tables
CALL SYSTEM$CLASSIFY(
    'ANALYTICS.RISK_INTELLIGENCE_DASHBOARD',
    'GOVERNANCE.INSURANCE_AUTO_CLASSIFICATION'
);

CALL SYSTEM$CLASSIFY(
    'ANALYTICS.BROKER_PERFORMANCE_MATRIX',
    'GOVERNANCE.INSURANCE_AUTO_CLASSIFICATION'
);

CALL SYSTEM$CLASSIFY(
    'ANALYTICS.CUSTOMER_RISK_PROFILE',
    'GOVERNANCE.INSURANCE_AUTO_CLASSIFICATION'
);

/* ================================================================================
PROGRESSIVE GOVERNANCE - MASKING POLICIES
================================================================================
*/

-- Financial data masking policy with progressive access levels
CREATE OR REPLACE MASKING POLICY GOVERNANCE.FINANCIAL_DATA_MASK AS 
    (financial_value NUMBER) RETURNS NUMBER ->
    CASE
        -- Internal admin roles get full access
        WHEN CURRENT_ROLE() IN ('ACCOUNTADMIN') THEN financial_value
        -- Workshop analysts get rounded values for demonstration purposes
        WHEN CURRENT_ROLE() IN ('WORKSHOP_ANALYST') THEN FLOOR(financial_value / 5000) * 5000
        -- External brokers get heavily masked values for competitive protection
        WHEN CURRENT_ROLE() IN ('BROKER_CONSUMER') OR 
             CURRENT_ACCOUNT_NAME() LIKE '%CONSUMER%' THEN FLOOR(financial_value / 10000) * 10000
        -- Default to highest masking level
        ELSE FLOOR(financial_value / 10000) * 10000
    END
    COMMENT = 'Progressive masking for financial data based on role and account context';

-- Broker contact information masking policy
CREATE OR REPLACE MASKING POLICY GOVERNANCE.BROKER_CONTACT_MASK AS 
    (contact_info STRING) RETURNS STRING ->
    CASE
        -- Full access for internal administration
        WHEN CURRENT_ROLE() IN ('ACCOUNTADMIN') THEN contact_info
        -- Partial masking for workshop analysts (mask domain but keep structure)
        WHEN CURRENT_ROLE() IN ('WORKSHOP_ANALYST') THEN 
            REGEXP_REPLACE(contact_info, '(@[^.]+)', '@***')
        -- Complete masking for external access
        ELSE 'MASKED'
    END
    COMMENT = 'Mask broker contact information for external access while preserving internal operations';

-- Performance metrics masking for competitive protection
CREATE OR REPLACE MASKING POLICY GOVERNANCE.PERFORMANCE_METRICS_MASK AS 
    (performance_data OBJECT) RETURNS OBJECT ->
    CASE
        -- Internal roles get full performance analytics
        WHEN CURRENT_ROLE() IN ('ACCOUNTADMIN', 'WORKSHOP_ANALYST') THEN performance_data
        -- External access gets masked indicators only
        ELSE OBJECT_CONSTRUCT('masked', true, 'access_level', 'restricted')
    END
    COMMENT = 'Mask detailed performance metrics for competitive protection';

-- Personal information masking for privacy compliance
CREATE OR REPLACE MASKING POLICY GOVERNANCE.PERSONAL_INFO_MASK AS 
    (personal_data STRING) RETURNS STRING ->
    CASE
        WHEN CURRENT_ROLE() IN ('ACCOUNTADMIN') THEN personal_data
        WHEN CURRENT_ROLE() IN ('WORKSHOP_ANALYST') THEN 
            CASE 
                WHEN personal_data IS NULL THEN NULL
                WHEN LENGTH(personal_data) <= 3 THEN '***'
                ELSE CONCAT(LEFT(personal_data, 2), REPEAT('*', LENGTH(personal_data) - 2))
            END
        ELSE 'REDACTED'
    END
    COMMENT = 'Privacy-compliant masking for personal information';

-- Apply masking policies to analytics tables
ALTER TABLE ANALYTICS.RISK_INTELLIGENCE_DASHBOARD 
    MODIFY COLUMN POLICY_ANNUAL_PREMIUM 
    SET MASKING POLICY GOVERNANCE.FINANCIAL_DATA_MASK;

ALTER TABLE ANALYTICS.RISK_INTELLIGENCE_DASHBOARD 
    MODIFY COLUMN CLAIM_AMOUNT_FILLED 
    SET MASKING POLICY GOVERNANCE.FINANCIAL_DATA_MASK;

ALTER TABLE ANALYTICS.BROKER_PERFORMANCE_MATRIX 
    MODIFY COLUMN BROKER_PERFORMANCE_ANALYSIS 
    SET MASKING POLICY GOVERNANCE.PERFORMANCE_METRICS_MASK;

ALTER TABLE ANALYTICS.BROKER_PERFORMANCE_MATRIX 
    MODIFY COLUMN AVG_CUSTOMER_PREMIUM 
    SET MASKING POLICY GOVERNANCE.FINANCIAL_DATA_MASK;

ALTER TABLE ANALYTICS.BROKER_PERFORMANCE_MATRIX 
    MODIFY COLUMN TOTAL_PREMIUM_VOLUME 
    SET MASKING POLICY GOVERNANCE.FINANCIAL_DATA_MASK;

/* ================================================================================
PROGRESSIVE GOVERNANCE - ROW ACCESS POLICIES
================================================================================
*/

-- Broker territory access policy for geographic restrictions
CREATE OR REPLACE ROW ACCESS POLICY GOVERNANCE.BROKER_TERRITORY_ACCESS AS
    (customer_region STRING, broker_id STRING) RETURNS BOOLEAN ->
    CASE
        -- Internal admin roles see all regions for oversight
        WHEN CURRENT_ROLE() IN ('ACCOUNTADMIN') THEN TRUE
        -- Workshop analysts see all regions for demonstration purposes
        WHEN CURRENT_ROLE() IN ('WORKSHOP_ANALYST') THEN TRUE
        -- Broker consumers limited to specific regions based on business rules
        WHEN CURRENT_ROLE() IN ('BROKER_CONSUMER') THEN 
            customer_region IN ('London Region', 'Manchester Region')
        -- Default to no access
        ELSE FALSE
    END
    COMMENT = 'Restrict broker access to assigned geographic territories';

-- Performance tier access policy for competitive protection
CREATE OR REPLACE ROW ACCESS POLICY GOVERNANCE.PERFORMANCE_TIER_ACCESS AS
    (broker_tier STRING, customer_segment STRING) RETURNS BOOLEAN ->
    CASE
        -- Internal roles have unrestricted access
        WHEN CURRENT_ROLE() IN ('ACCOUNTADMIN', 'WORKSHOP_ANALYST') THEN TRUE
        -- External brokers can only see specific tiers and segments
        WHEN CURRENT_ROLE() IN ('BROKER_CONSUMER') THEN 
            broker_tier IN ('GOLD', 'SILVER') AND customer_segment NOT LIKE '%PREMIUM%'
        -- Default to restricted access
        ELSE FALSE
    END
    COMMENT = 'Limit access based on broker performance tier and customer segment sensitivity';

-- Risk level access policy for data sensitivity
CREATE OR REPLACE ROW ACCESS POLICY GOVERNANCE.RISK_LEVEL_ACCESS AS
    (final_risk_level STRING, customer_segment STRING) RETURNS BOOLEAN ->
    CASE
        -- Internal roles see all risk levels
        WHEN CURRENT_ROLE() IN ('ACCOUNTADMIN', 'WORKSHOP_ANALYST') THEN TRUE
        -- External access limited to non-sensitive risk data
        WHEN CURRENT_ROLE() IN ('BROKER_CONSUMER') THEN 
            final_risk_level IN ('LOW', 'MEDIUM') AND customer_segment NOT LIKE '%PREMIUM%'
        ELSE FALSE
    END
    COMMENT = 'Control access to sensitive risk assessment data';

-- Apply row access policies to analytics tables
ALTER TABLE ANALYTICS.RISK_INTELLIGENCE_DASHBOARD
    ADD ROW ACCESS POLICY GOVERNANCE.BROKER_TERRITORY_ACCESS ON (CUSTOMER_REGION, BROKER_ID);

ALTER TABLE ANALYTICS.RISK_INTELLIGENCE_DASHBOARD
    ADD ROW ACCESS POLICY GOVERNANCE.RISK_LEVEL_ACCESS ON (FINAL_RISK_LEVEL, CUSTOMER_SEGMENT);

ALTER TABLE ANALYTICS.BROKER_PERFORMANCE_MATRIX
    ADD ROW ACCESS POLICY GOVERNANCE.PERFORMANCE_TIER_ACCESS ON (BROKER_TIER, 'STANDARD');

/* ================================================================================
SECURE DATA SHARING ARCHITECTURE
================================================================================
*/

USE SCHEMA SHARING;

-- Broker portal view for individual broker access with governance
CREATE OR REPLACE SECURE VIEW SHARING.BROKER_PORTAL_VIEW 
    COMMENT = 'Individual broker performance and customer portfolio view with applied governance'
    AS
SELECT 
    b.BROKER_ID,
    b.BROKER_FIRST_NAME,
    b.BROKER_LAST_NAME,
    b.BROKER_TIER,
    b.TOTAL_CUSTOMERS,
    b.AVG_CUSTOMER_PREMIUM,
    b.AVG_CUSTOMER_RISK,
    r.CUSTOMER_SEGMENT,
    r.FINAL_RISK_LEVEL,
    r.POLICY_ANNUAL_PREMIUM,
    r.CLAIM_AMOUNT_FILLED,
    r.CUSTOMER_REGION,
    r.LAST_UPDATED
FROM ANALYTICS.BROKER_PERFORMANCE_MATRIX b
LEFT JOIN ANALYTICS.RISK_INTELLIGENCE_DASHBOARD r ON b.BROKER_ID = r.BROKER_ID
WHERE b.BROKER_ACTIVE = TRUE;

-- Regional manager view for territory-wide analytics
CREATE OR REPLACE SECURE VIEW SHARING.REGIONAL_MANAGER_VIEW 
    COMMENT = 'Territory-wide analytics for regional management with aggregated data'
    AS
SELECT 
    CUSTOMER_REGION,
    COUNT(DISTINCT BROKER_ID) as ACTIVE_BROKERS,
    COUNT(*) as TOTAL_CUSTOMERS,
    AVG(POLICY_ANNUAL_PREMIUM) as AVG_REGION_PREMIUM,
    SUM(CLAIM_AMOUNT_FILLED) as TOTAL_REGION_CLAIMS,
    AVG(CUSTOMER_RISK_SCORE) as AVG_REGION_RISK,
    COUNT(CASE WHEN FINAL_RISK_LEVEL = 'HIGH' THEN 1 END) as HIGH_RISK_CUSTOMERS,
    COUNT(CASE WHEN CUSTOMER_SEGMENT LIKE '%PREMIUM%' THEN 1 END) as PREMIUM_CUSTOMERS,
    MAX(LAST_UPDATED) as LAST_REFRESH
FROM ANALYTICS.RISK_INTELLIGENCE_DASHBOARD
GROUP BY CUSTOMER_REGION;

-- Executive dashboard view for high-level KPIs without sensitive details
CREATE OR REPLACE SECURE VIEW SHARING.EXECUTIVE_DASHBOARD_VIEW 
    COMMENT = 'Executive KPIs without sensitive operational details'
    AS
SELECT 
    COUNT(DISTINCT BROKER_ID) as TOTAL_BROKERS,
    COUNT(DISTINCT POLICY_NUMBER) as TOTAL_CUSTOMERS,
    COUNT(DISTINCT CUSTOMER_REGION) as ACTIVE_REGIONS,
    AVG(CUSTOMER_RISK_SCORE) as OVERALL_RISK_SCORE,
    COUNT(CASE WHEN FINAL_RISK_LEVEL = 'HIGH' THEN 1 END) as HIGH_RISK_COUNT,
    COUNT(CASE WHEN FINAL_RISK_LEVEL = 'MEDIUM' THEN 1 END) as MEDIUM_RISK_COUNT,
    COUNT(CASE WHEN FINAL_RISK_LEVEL = 'LOW' THEN 1 END) as LOW_RISK_COUNT,
    COUNT(CASE WHEN CUSTOMER_SEGMENT LIKE '%PREMIUM%' THEN 1 END) as PREMIUM_CUSTOMERS,
    AVG(CASE WHEN CUSTOMER_SEGMENT LIKE '%PREMIUM%' THEN POLICY_ANNUAL_PREMIUM END) as AVG_PREMIUM_VALUE,
    MAX(LAST_UPDATED) as LAST_REFRESH
FROM ANALYTICS.RISK_INTELLIGENCE_DASHBOARD;

-- Data quality summary view for governance monitoring
CREATE OR REPLACE SECURE VIEW SHARING.GOVERNANCE_MONITORING_VIEW 
    COMMENT = 'Data governance and quality monitoring summary'
    AS
SELECT 
    'ANALYTICS_TABLES' as MONITORING_CATEGORY,
    COUNT(*) as TOTAL_OBJECTS,
    'ACTIVE' as STATUS,
    CURRENT_TIMESTAMP() as LAST_CHECK
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA = 'ANALYTICS' AND TABLE_TYPE = 'DYNAMIC TABLE'

UNION ALL

SELECT 
    'MASKING_POLICIES' as MONITORING_CATEGORY,
    COUNT(*) as TOTAL_OBJECTS,
    'APPLIED' as STATUS,
    CURRENT_TIMESTAMP() as LAST_CHECK
FROM INFORMATION_SCHEMA.POLICY_REFERENCES
WHERE POLICY_SCHEMA = 'GOVERNANCE' AND POLICY_KIND = 'MASKING_POLICY'

UNION ALL

SELECT 
    'ROW_ACCESS_POLICIES' as MONITORING_CATEGORY,
    COUNT(*) as TOTAL_OBJECTS,
    'APPLIED' as STATUS,
    CURRENT_TIMESTAMP() as LAST_CHECK
FROM INFORMATION_SCHEMA.POLICY_REFERENCES
WHERE POLICY_SCHEMA = 'GOVERNANCE' AND POLICY_KIND = 'ROW_ACCESS_POLICY';

/* ================================================================================
DATA SHARING SETUP
================================================================================
*/

-- Create secure data shares for different stakeholder groups
CREATE OR REPLACE SHARE BROKER_PORTAL_SHARE
    COMMENT = 'Individual broker performance data share with governance controls';

CREATE OR REPLACE SHARE REGIONAL_ANALYTICS_SHARE
    COMMENT = 'Regional management analytics share with aggregated insights';

CREATE OR REPLACE SHARE EXECUTIVE_METRICS_SHARE
    COMMENT = 'Executive dashboard metrics share with high-level KPIs';

CREATE OR REPLACE SHARE GOVERNANCE_MONITORING_SHARE
    COMMENT = 'Data governance monitoring and compliance reporting share';

-- Grant database and schema access to shares
GRANT USAGE ON DATABASE INSURANCE_WORKSHOP_DB TO SHARE BROKER_PORTAL_SHARE;
GRANT USAGE ON SCHEMA SHARING TO SHARE BROKER_PORTAL_SHARE;
GRANT SELECT ON VIEW SHARING.BROKER_PORTAL_VIEW TO SHARE BROKER_PORTAL_SHARE;

GRANT USAGE ON DATABASE INSURANCE_WORKSHOP_DB TO SHARE REGIONAL_ANALYTICS_SHARE;
GRANT USAGE ON SCHEMA SHARING TO SHARE REGIONAL_ANALYTICS_SHARE;
GRANT SELECT ON VIEW SHARING.REGIONAL_MANAGER_VIEW TO SHARE REGIONAL_ANALYTICS_SHARE;

GRANT USAGE ON DATABASE INSURANCE_WORKSHOP_DB TO SHARE EXECUTIVE_METRICS_SHARE;
GRANT USAGE ON SCHEMA SHARING TO SHARE EXECUTIVE_METRICS_SHARE;
GRANT SELECT ON VIEW SHARING.EXECUTIVE_DASHBOARD_VIEW TO SHARE EXECUTIVE_METRICS_SHARE;

GRANT USAGE ON DATABASE INSURANCE_WORKSHOP_DB TO SHARE GOVERNANCE_MONITORING_SHARE;
GRANT USAGE ON SCHEMA SHARING TO SHARE GOVERNANCE_MONITORING_SHARE;
GRANT SELECT ON VIEW SHARING.GOVERNANCE_MONITORING_VIEW TO SHARE GOVERNANCE_MONITORING_SHARE;

/* ================================================================================
GOVERNANCE ROLES AND PERMISSIONS
================================================================================
*/

-- Grant comprehensive access to workshop roles
GRANT SELECT ON ALL VIEWS IN SCHEMA SHARING TO ROLE WORKSHOP_ANALYST;
GRANT SELECT ON VIEW SHARING.BROKER_PORTAL_VIEW TO ROLE BROKER_CONSUMER;
GRANT SELECT ON VIEW SHARING.GOVERNANCE_MONITORING_VIEW TO ROLE WORKSHOP_ANALYST;

-- Grant governance administration privileges
GRANT ALL PRIVILEGES ON SCHEMA GOVERNANCE TO ROLE WORKSHOP_ANALYST;

/* ================================================================================
GOVERNANCE VALIDATION AND MONITORING
================================================================================
*/

-- Governance implementation status check
SELECT 
    'MASKING_POLICIES' as GOVERNANCE_COMPONENT,
    COUNT(*) as POLICIES_APPLIED,
    'ACTIVE' as STATUS
FROM INFORMATION_SCHEMA.POLICY_REFERENCES
WHERE POLICY_SCHEMA = 'GOVERNANCE' AND POLICY_KIND = 'MASKING_POLICY'

UNION ALL

SELECT 
    'ROW_ACCESS_POLICIES' as GOVERNANCE_COMPONENT,
    COUNT(*) as POLICIES_APPLIED,
    'ACTIVE' as STATUS
FROM INFORMATION_SCHEMA.POLICY_REFERENCES
WHERE POLICY_SCHEMA = 'GOVERNANCE' AND POLICY_KIND = 'ROW_ACCESS_POLICY'

UNION ALL

SELECT 
    'DATA_SHARES' as GOVERNANCE_COMPONENT,
    COUNT(*) as POLICIES_APPLIED,
    'ACTIVE' as STATUS
FROM INFORMATION_SCHEMA.OUTBOUND_SHARES
WHERE SHARE_NAME LIKE '%SHARE'

UNION ALL

SELECT 
    'SECURE_VIEWS' as GOVERNANCE_COMPONENT,
    COUNT(*) as POLICIES_APPLIED,
    'ACTIVE' as STATUS
FROM INFORMATION_SCHEMA.VIEWS
WHERE TABLE_SCHEMA = 'SHARING' AND SECURITY_TYPE = 'SECURE';

/* ================================================================================
GOVERNANCE POLICIES SETUP COMPLETE
================================================================================
Implementation Complete:
• Classification: Automated PII and financial data discovery with 30-day refresh
• Masking Policies: 4 progressive policies for financial, contact, performance, and personal data
• Row Access Policies: 3 territorial and tier-based access controls
• Secure Data Sharing: 4 governed shares for different stakeholder groups
• Monitoring Views: Governance compliance and status monitoring

Progressive Governance Architecture:
• Role-Based Access: ACCOUNTADMIN → WORKSHOP_ANALYST → BROKER_CONSUMER hierarchy
• Data Sensitivity Levels: Full → Rounded → Masked → Redacted progression
• Geographic Controls: Territory-based row-level security for broker access
• Competitive Protection: Performance metrics and premium customer data masking

Data Sharing Strategy:
• Broker Portal: Individual performance with governance controls
• Regional Analytics: Territory-wide insights with aggregated data
• Executive Dashboard: High-level KPIs without operational sensitivity
• Governance Monitoring: Compliance reporting and policy status tracking

Security Features:
• Automated Classification: SNOWFLAKE.DATA_PRIVACY.CLASSIFICATION_PROFILE
• Dynamic Masking: Context-aware based on role and account
• Row-Level Security: Multi-dimensional access controls
• Secure Views: Governed data distribution with audit trails

Ready for: Phase 4 - Visualization Dashboards and Phase 5 - Complete Workshop Integration
================================================================================
*/ 