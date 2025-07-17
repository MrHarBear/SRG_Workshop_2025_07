/* ================================================================================
INSURANCE WORKSHOP - RISK ANALYTICS
================================================================================
Purpose: Advanced analytics with mixed UDFs and Dynamic Tables for three-entity model
Scope: Streamlined SQL/Python UDFs, two-level Dynamic Tables, real-time analytics
Entities: Comprehensive broker-customer-claims risk intelligence platform
================================================================================
*/

USE DATABASE INSURANCE_WORKSHOP_DB;
USE SCHEMA ANALYTICS;
USE WAREHOUSE WORKSHOP_COMPUTE_WH;
USE ROLE ACCOUNTADMIN;

/* ================================================================================
STREAMLINED ANALYTICS UDFS - MIXED LANGUAGE APPROACH
================================================================================
*/

-- SQL UDF for customer risk scoring (high performance)
CREATE OR REPLACE FUNCTION ANALYTICS.CALCULATE_CUSTOMER_RISK_SCORE(AGE NUMBER, PREMIUM NUMBER, CLAIM_AMOUNT NUMBER)
RETURNS NUMBER
LANGUAGE SQL
COMMENT = 'Calculate customer risk score based on age, premium, and claim history'
AS
$$
    CASE 
        WHEN AGE IS NULL THEN 50
        WHEN AGE < 25 THEN 
            CASE WHEN CLAIM_AMOUNT > 50000 THEN 85 ELSE 65 END
        WHEN AGE < 35 THEN 
            CASE WHEN CLAIM_AMOUNT > 75000 THEN 70 ELSE 45 END
        WHEN AGE < 55 THEN 
            CASE WHEN CLAIM_AMOUNT > 100000 THEN 55 ELSE 25 END
        ELSE 
            CASE WHEN CLAIM_AMOUNT > 50000 THEN 40 ELSE 20 END
    END +
    CASE 
        WHEN PREMIUM > 2000 THEN 10
        WHEN PREMIUM > 1500 THEN 5
        ELSE 0
    END
$$;

-- SQL UDF for broker performance tier classification
CREATE OR REPLACE FUNCTION ANALYTICS.DETERMINE_BROKER_TIER(SATISFACTION NUMBER, EXPERIENCE NUMBER, TRAINING NUMBER)
RETURNS VARCHAR(20)
LANGUAGE SQL
COMMENT = 'Classify broker performance tier based on key metrics'
AS
$$
    CASE 
        WHEN SATISFACTION >= 4.8 AND EXPERIENCE >= 10 AND TRAINING >= 40 THEN 'PLATINUM'
        WHEN SATISFACTION >= 4.5 AND EXPERIENCE >= 5 AND TRAINING >= 30 THEN 'GOLD'
        WHEN SATISFACTION >= 4.2 AND EXPERIENCE >= 3 AND TRAINING >= 20 THEN 'SILVER'
        ELSE 'BRONZE'
    END
$$;

-- Python UDF for comprehensive broker performance analysis
CREATE OR REPLACE FUNCTION ANALYTICS.ANALYZE_BROKER_PERFORMANCE(
    SATISFACTION NUMBER, EXPERIENCE NUMBER, TRAINING NUMBER, 
    CUSTOMER_COUNT NUMBER, AVG_CLAIM_AMOUNT NUMBER
)
RETURNS OBJECT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.12'
HANDLER = 'analyze_performance'
COMMENT = 'Comprehensive broker performance analysis with multiple factors'
AS
$$
def analyze_performance(satisfaction, experience, training, customer_count, avg_claim_amount):
    import math
    
    # Handle null values
    satisfaction = satisfaction or 0
    experience = experience or 0
    training = training or 0
    customer_count = customer_count or 0
    avg_claim_amount = avg_claim_amount or 0
    
    # Performance scoring algorithm
    satisfaction_score = min(satisfaction * 20, 100)  # Scale to 100
    experience_score = min(experience * 8, 80)        # Cap at 80
    training_score = min(training * 2, 60)            # Cap at 60
    
    # Portfolio management score
    portfolio_score = 0
    if customer_count > 0:
        portfolio_score = min(math.log(customer_count) * 15, 50)
    
    # Risk management score (lower avg claims = better)
    risk_score = 0
    if avg_claim_amount > 0:
        risk_score = max(50 - (avg_claim_amount / 2000), 0)
    
    total_score = satisfaction_score + experience_score + training_score + portfolio_score + risk_score
    
    # Performance classification
    if total_score >= 250:
        tier = "ELITE"
    elif total_score >= 200:
        tier = "SUPERIOR"
    elif total_score >= 150:
        tier = "PROFICIENT"
    else:
        tier = "DEVELOPING"
    
    return {
        "total_score": round(total_score, 1),
        "performance_tier": tier,
        "satisfaction_component": round(satisfaction_score, 1),
        "experience_component": round(experience_score, 1),
        "training_component": round(training_score, 1),
        "portfolio_component": round(portfolio_score, 1),
        "risk_management_component": round(risk_score, 1)
    }
$$;

/* ================================================================================
DYNAMIC TABLES ARCHITECTURE - TWO LEVELS
================================================================================
*/

-- Level 1: Data Integration Layer
CREATE OR REPLACE DYNAMIC TABLE ANALYTICS.CUSTOMER_BROKER_CLAIMS_INTEGRATED
    TARGET_LAG = '1 minute'
    WAREHOUSE = WORKSHOP_COMPUTE_WH
    COMMENT = 'Level 1: Complete integration of customers, brokers, and claims'
    AS
SELECT 
    -- Customer core data
    c.POLICY_NUMBER,
    c.AGE,
    c.INSURED_SEX,
    c.INSURED_EDUCATION_LEVEL,
    c.INSURED_OCCUPATION,
    c.POLICY_START_DATE,
    c.POLICY_LENGTH_MONTH,
    c.POLICY_DEDUCTABLE,
    c.POLICY_ANNUAL_PREMIUM,
    
    -- Broker information
    c.BROKER_ID,
    b.FIRST_NAME as BROKER_FIRST_NAME,
    b.LAST_NAME as BROKER_LAST_NAME,
    b.EMAIL as BROKER_EMAIL,
    b.OFFICE_LOCATION as BROKER_OFFICE,
    b.SPECIALIZATIONS as BROKER_SPECIALIZATIONS,
    b.TERRITORY as BROKER_TERRITORY,
    b.PERFORMANCE_METRICS:customer_satisfaction::NUMBER as BROKER_SATISFACTION,
    b.PERFORMANCE_METRICS:years_experience::NUMBER as BROKER_EXPERIENCE,
    b.PERFORMANCE_METRICS:training_hours_completed::NUMBER as BROKER_TRAINING,
    b.ACTIVE as BROKER_ACTIVE,
    
    -- Claims data (NULL if no claims)
    cl.INCIDENT_DATE,
    cl.INCIDENT_TYPE,
    cl.INCIDENT_SEVERITY,
    cl.AUTHORITIES_CONTACTED,
    cl.INCIDENT_HOUR_OF_THE_DAY,
    cl.NUMBER_OF_VEHICLES_INVOLVED,
    cl.BODILY_INJURIES,
    cl.WITNESSES,
    cl.POLICE_REPORT_AVAILABLE,
    cl.CLAIM_AMOUNT,
    cl.FRAUD_REPORTED,
    
    -- Derived fields
    CASE WHEN cl.POLICY_NUMBER IS NOT NULL THEN 1 ELSE 0 END as HAS_CLAIM,
    COALESCE(cl.CLAIM_AMOUNT, 0) as CLAIM_AMOUNT_FILLED,
    COALESCE(cl.FRAUD_REPORTED, FALSE) as FRAUD_REPORTED_FILLED
    
FROM RAW_DATA.CUSTOMERS_RAW c
LEFT JOIN RAW_DATA.BROKERS_RAW b ON c.BROKER_ID = b.BROKER_ID
LEFT JOIN RAW_DATA.CLAIMS_RAW cl ON c.POLICY_NUMBER = cl.POLICY_NUMBER;

-- Level 2: Risk Intelligence Dashboard Layer
CREATE OR REPLACE DYNAMIC TABLE ANALYTICS.RISK_INTELLIGENCE_DASHBOARD
    TARGET_LAG = '1 minute'
    WAREHOUSE = WORKSHOP_COMPUTE_WH
    COMMENT = 'Level 2: Executive intelligence layer with UDF-driven analytics'
    AS
SELECT 
    -- Customer identification and core data
    POLICY_NUMBER,
    AGE,
    INSURED_SEX,
    INSURED_OCCUPATION,
    POLICY_ANNUAL_PREMIUM,
    POLICY_LENGTH_MONTH,
    CLAIM_AMOUNT_FILLED,
    FRAUD_REPORTED_FILLED,
    HAS_CLAIM,
    
    -- Broker identification and metrics
    BROKER_ID,
    BROKER_FIRST_NAME,
    BROKER_LAST_NAME,
    BROKER_OFFICE,
    BROKER_SATISFACTION,
    BROKER_EXPERIENCE,
    BROKER_TRAINING,
    
    -- Regional information for governance
    CASE 
        WHEN BROKER_OFFICE LIKE '%London%' THEN 'London Region'
        WHEN BROKER_OFFICE LIKE '%Manchester%' THEN 'Manchester Region'
        WHEN BROKER_OFFICE LIKE '%Birmingham%' THEN 'Birmingham Region'
        WHEN BROKER_OFFICE LIKE '%Leeds%' THEN 'Northern Region'
        ELSE 'Other Region'
    END as CUSTOMER_REGION,
    
    -- Customer segment classification
    CASE 
        WHEN POLICY_ANNUAL_PREMIUM > 2500 THEN 'PREMIUM'
        WHEN POLICY_ANNUAL_PREMIUM > 1500 THEN 'STANDARD'
        ELSE 'BASIC'
    END as CUSTOMER_SEGMENT,
    
    -- Broker portfolio metrics
    COUNT(*) OVER (PARTITION BY BROKER_ID) as BROKER_CUSTOMER_COUNT,
    AVG(CLAIM_AMOUNT_FILLED) OVER (PARTITION BY BROKER_ID) as BROKER_AVG_CLAIM,
    
    -- SQL UDF risk and performance calculations
    ANALYTICS.CALCULATE_CUSTOMER_RISK_SCORE(AGE, POLICY_ANNUAL_PREMIUM, CLAIM_AMOUNT_FILLED) as CUSTOMER_RISK_SCORE,
    ANALYTICS.DETERMINE_BROKER_TIER(BROKER_SATISFACTION, BROKER_EXPERIENCE, BROKER_TRAINING) as BROKER_TIER,
    
    -- Python UDF advanced broker performance analysis
    ANALYTICS.ANALYZE_BROKER_PERFORMANCE(
        BROKER_SATISFACTION, 
        BROKER_EXPERIENCE, 
        BROKER_TRAINING,
        COUNT(*) OVER (PARTITION BY BROKER_ID),
        AVG(CLAIM_AMOUNT_FILLED) OVER (PARTITION BY BROKER_ID)
    ) as BROKER_PERFORMANCE_ANALYSIS,
    
    -- Final risk classification
    CASE 
        WHEN ANALYTICS.CALCULATE_CUSTOMER_RISK_SCORE(AGE, POLICY_ANNUAL_PREMIUM, CLAIM_AMOUNT_FILLED) >= 75 THEN 'HIGH'
        WHEN ANALYTICS.CALCULATE_CUSTOMER_RISK_SCORE(AGE, POLICY_ANNUAL_PREMIUM, CLAIM_AMOUNT_FILLED) >= 50 THEN 'MEDIUM'
        ELSE 'LOW'
    END as FINAL_RISK_LEVEL
    
FROM ANALYTICS.CUSTOMER_BROKER_CLAIMS_INTEGRATED
WHERE BROKER_ID IS NOT NULL;

-- Level 3: Broker Performance Matrix for Governance
CREATE OR REPLACE DYNAMIC TABLE ANALYTICS.BROKER_PERFORMANCE_MATRIX
    TARGET_LAG = '1 minute'
    WAREHOUSE = WORKSHOP_COMPUTE_WH
    COMMENT = 'Level 3: Broker-centric performance metrics for governance and sharing'
    AS
SELECT 
    BROKER_ID,
    MAX(BROKER_FIRST_NAME) as BROKER_FIRST_NAME,
    MAX(BROKER_LAST_NAME) as BROKER_LAST_NAME,
    MAX(BROKER_TIER) as BROKER_TIER,
    COUNT(*) as TOTAL_CUSTOMERS,
    AVG(POLICY_ANNUAL_PREMIUM) as AVG_CUSTOMER_PREMIUM,
    AVG(CUSTOMER_RISK_SCORE) as AVG_CUSTOMER_RISK,
    SUM(POLICY_ANNUAL_PREMIUM) as TOTAL_PREMIUM_VOLUME,
    ANY_VALUE(BROKER_PERFORMANCE_ANALYSIS) as BROKER_PERFORMANCE_ANALYSIS,
    MAX(CASE WHEN BROKER_TIER IS NOT NULL THEN TRUE ELSE FALSE END) as BROKER_ACTIVE
FROM ANALYTICS.RISK_INTELLIGENCE_DASHBOARD
GROUP BY BROKER_ID;

/* ================================================================================
GRANT PERMISSIONS FOR WORKSHOP ROLES
================================================================================
*/

-- Grant access to analytics tables and UDFs
GRANT SELECT ON ALL DYNAMIC TABLES IN SCHEMA ANALYTICS TO ROLE WORKSHOP_ANALYST;
GRANT USAGE ON ALL FUNCTIONS IN SCHEMA ANALYTICS TO ROLE WORKSHOP_ANALYST;

/* ================================================================================
STREAMLINED RISK ANALYTICS SETUP COMPLETE
================================================================================
Implementation Complete:
• Streamlined UDFs: 2 SQL functions + 1 Python function for optimal demo balance
• Dynamic Tables: 3-level architecture with 1-minute refresh for real-time insights
• Analytics Coverage: Customer risk profiling and broker performance analysis
• Integration Layer: Complete customer-broker-claims data fusion
• Intelligence Layer: UDF-driven analytics for business insights
• Performance Matrix: Broker-centric aggregations for governance and sharing

Analytics Functions Delivered:
• SQL UDFs: High-performance risk scoring and tier classification
• Python UDF: Multi-factor broker performance analysis with detailed scoring
• Incremental Processing: Optimized for incremental refreshes without timestamps
• Scalable Architecture: Three-level Dynamic Tables design for complete coverage

Business Intelligence:
• Customer Risk Scoring: Age, premium, and claim-based risk assessment
• Broker Performance Analysis: Comprehensive multi-factor evaluation
• Performance Tiers: Clear broker classification system
• Risk Classification: Simple three-tier risk categorization
• Broker Aggregations: Portfolio metrics and governance-ready data

Ready for: Phase 3 - Governance Implementation and Phase 4 - Visualization Dashboards
================================================================================
*/ 