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

CALL SYSTEM$CLASSIFY(
    'ANALYTICS.BROKER_PERFORMANCE_MATRIX',
    'GOVERNANCE.INSURANCE_AUTO_CLASSIFICATION'
);

-- View classification results
SELECT SYSTEM$GET_CLASSIFICATION_RESULT('ANALYTICS.BROKER_PERFORMANCE_MATRIX');

SELECT *
FROM TABLE(
  INFORMATION_SCHEMA.TAG_REFERENCES_ALL_COLUMNS(
    'ANALYTICS.BROKER_PERFORMANCE_MATRIX',
    'table'
));
/* ================================================================================
PROGRESSIVE GOVERNANCE - MASKING POLICIES
================================================================================
*/

-- Mask competitive financial data (premium volumes)
CREATE OR REPLACE MASKING POLICY GOVERNANCE.COMPETITIVE_FINANCIAL_MASK AS 
    (financial_value NUMBER) RETURNS NUMBER ->
    CASE
        -- Internal admin and analysts see full values
        WHEN CURRENT_ROLE() IN ('ACCOUNTADMIN') THEN financial_value
        -- External brokers see rounded values for competitive protection
        WHEN CURRENT_ROLE() IN ('WORKSHOP_ANALYST') 
        -- Future: Consumer accounts will see masked values (uncomment during workshop)
        -- OR CURRENT_ACCOUNT_NAME() LIKE '%DATASHARING_CONSUMER%'
        THEN FLOOR(financial_value / 10000) * 10000
        ELSE FLOOR(financial_value / 10000) * 10000
    END
    COMMENT = 'Mask competitive financial data for external broker access';

-- Mask detailed performance analytics
CREATE OR REPLACE MASKING POLICY GOVERNANCE.PERFORMANCE_ANALYTICS_MASK AS 
    (performance_data OBJECT) RETURNS OBJECT ->
    CASE
        -- Internal roles see full performance breakdown
        WHEN CURRENT_ROLE() IN ('ACCOUNTADMIN') THEN performance_data
        -- External brokers see simplified performance indicator
        WHEN CURRENT_ROLE() IN ('WORKSHOP_ANALYST') 
        -- Future: Consumer accounts will see masked version (uncomment during workshop)
        -- OR CURRENT_ACCOUNT_NAME() LIKE '%DATASHARING_CONSUMER%'  
        THEN OBJECT_CONSTRUCT('performance_tier', performance_data:performance_tier, 'access_level', 'external_view')
        ELSE OBJECT_CONSTRUCT('masked', TRUE, 'access_level', 'restricted')
    END
    COMMENT = 'Mask detailed performance analytics for competitive protection';

-- Apply masking policies to broker performance table
ALTER TABLE ANALYTICS.BROKER_PERFORMANCE_MATRIX 
    MODIFY COLUMN TOTAL_PREMIUM_VOLUME 
    SET MASKING POLICY GOVERNANCE.COMPETITIVE_FINANCIAL_MASK;

ALTER TABLE ANALYTICS.BROKER_PERFORMANCE_MATRIX 
    MODIFY COLUMN AVG_CUSTOMER_PREMIUM 
    SET MASKING POLICY GOVERNANCE.COMPETITIVE_FINANCIAL_MASK;

ALTER TABLE ANALYTICS.BROKER_PERFORMANCE_MATRIX 
    MODIFY COLUMN BROKER_PERFORMANCE_ANALYSIS 
    SET MASKING POLICY GOVERNANCE.PERFORMANCE_ANALYTICS_MASK;



select * from ANALYTICS.BROKER_PERFORMANCE_MATRIX; 
use role WORKSHOP_ANALYST;
select * from ANALYTICS.BROKER_PERFORMANCE_MATRIX; 
use role accountadmin;

/* ================================================================================
STEP 3: ROW ACCESS POLICY
================================================================================
*/

-- Ensure brokers can only see their own performance data
CREATE OR REPLACE ROW ACCESS POLICY GOVERNANCE.BROKER_ISOLATION_POLICY AS
    (broker_id STRING) RETURNS BOOLEAN ->
    CASE
        -- Internal roles see all brokers for oversight
        WHEN CURRENT_ROLE() IN ('ACCOUNTADMIN') THEN TRUE
        -- External brokers only see their own performance
        WHEN CURRENT_ROLE() IN ('WORKSHOP_ANALYST')  
        -- Future: Consumer accounts will have restricted access (modify during workshop)
        OR CURRENT_ACCOUNT_NAME() LIKE '%DATASHARING_CONSUMER%' 
        THEN broker_id IN ('BRK001', 'BRK002', 'BRK003')
        ELSE TRUE
    END
    COMMENT = 'Isolate broker performance data - brokers see only their own records';

-- Apply row access policy to broker performance table
ALTER TABLE ANALYTICS.BROKER_PERFORMANCE_MATRIX
    ADD ROW ACCESS POLICY GOVERNANCE.BROKER_ISOLATION_POLICY ON (BROKER_ID);

select * from ANALYTICS.BROKER_PERFORMANCE_MATRIX;
use role workshop_analyst;
select * from ANALYTICS.BROKER_PERFORMANCE_MATRIX;
use role accountadmin;

/* ================================================================================
STEP 4: SECURE DATA PRODUCT FOR SHARING
================================================================================
*/

USE SCHEMA SHARING;

-- Create broker portal view with governance controls applied
CREATE OR REPLACE SECURE VIEW SHARING.BROKER_PERFORMANCE_PORTAL 
    COMMENT = 'Governed broker performance data product for external sharing'
    AS
SELECT 
    BROKER_ID,
    BROKER_FIRST_NAME,
    BROKER_LAST_NAME,
    BROKER_TIER,
    TOTAL_CUSTOMERS,
    AVG_CUSTOMER_PREMIUM,      -- Masked for competitive protection
    AVG_CUSTOMER_RISK,
    TOTAL_PREMIUM_VOLUME,      -- Masked for competitive protection
    BROKER_PERFORMANCE_ANALYSIS, -- Masked to show only tier
    BROKER_ACTIVE,
    CURRENT_TIMESTAMP() as DATA_ACCESS_TIME
FROM ANALYTICS.BROKER_PERFORMANCE_MATRIX
WHERE BROKER_ACTIVE = TRUE;

use role workshop_analyst;
select * from SHARING.BROKER_PERFORMANCE_PORTAL;

/* ================================================================================
WORKSHOP MODIFICATION INSTRUCTIONS
================================================================================

During the workshop:

1. CREATE CONSUMER ACCOUNT in Snowflake UI
2. ADD the BROKER_PERFORMANCE_SHARE to the consumer account
3. LOGIN to consumer account and verify data access
4. RETURN to this script and UNCOMMENT the account-based logic:
   - Line 40: WHEN CURRENT_ACCOUNT_NAME() LIKE '%CONSUMER%' THEN...
   - Line 52: WHEN CURRENT_ACCOUNT_NAME() LIKE '%CONSUMER%' THEN...
   - Line 75: WHEN CURRENT_ACCOUNT_NAME() LIKE '%CONSUMER%' THEN...
5. RERUN the policy creation commands
6. RETURN to consumer account to see masked data

This demonstrates progressive governance and secure data sharing capabilities.

================================================================================
*/ 
use role accountadmin;
-- Apply masking policies to broker performance table
ALTER TABLE ANALYTICS.BROKER_PERFORMANCE_MATRIX 
    MODIFY COLUMN TOTAL_PREMIUM_VOLUME 
    UNSET MASKING POLICY;

ALTER TABLE ANALYTICS.BROKER_PERFORMANCE_MATRIX 
    MODIFY COLUMN AVG_CUSTOMER_PREMIUM 
    UNSET MASKING POLICY;

ALTER TABLE ANALYTICS.BROKER_PERFORMANCE_MATRIX 
    MODIFY COLUMN BROKER_PERFORMANCE_ANALYSIS 
    UNSET MASKING POLICY;

ALTER TABLE ANALYTICS.BROKER_PERFORMANCE_MATRIX 
    DROP ROW ACCESS POLICY GOVERNANCE.BROKER_ISOLATION_POLICY;