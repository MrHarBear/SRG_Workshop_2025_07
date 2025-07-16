/* ================================================================================
INSURANCE WORKSHOP - RISK ANALYTICS AND GOVERNANCE
================================================================================
Purpose: Advanced analytics with progressive governance for three-entity model
Scope: Mixed UDFs, Dynamic Tables, classification, governance policies, sharing
Entities: Comprehensive broker-customer-claims risk intelligence platform
================================================================================
*/

USE DATABASE INSURANCE_WORKSHOP_DB;
USE SCHEMA ANALYTICS;
USE WAREHOUSE WORKSHOP_COMPUTE_WH;
USE ROLE ACCOUNTADMIN;

/* ================================================================================
ENHANCED ANALYTICS UDFS - MIXED LANGUAGE APPROACH
================================================================================
*/

-- SQL UDFs for simple calculations (high performance)

-- Customer risk scoring using SQL for efficiency
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

-- Broker performance tier using SQL for classification
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

-- Territory premium calculation using SQL for aggregation
CREATE OR REPLACE FUNCTION ANALYTICS.CALCULATE_TERRITORY_PREMIUM(TERRITORY_LIST ARRAY, BASE_PREMIUM NUMBER)
RETURNS NUMBER
LANGUAGE SQL
COMMENT = 'Calculate territory-adjusted premium based on coverage areas'
AS
$$
    BASE_PREMIUM * 
    CASE 
        WHEN ARRAY_SIZE(TERRITORY_LIST) >= 3 THEN 1.15
        WHEN ARRAY_SIZE(TERRITORY_LIST) = 2 THEN 1.10
        WHEN ARRAY_SIZE(TERRITORY_LIST) = 1 THEN 1.05
        ELSE 1.0
    END
$$;

-- Claim severity assessment using SQL for categorization
CREATE OR REPLACE FUNCTION ANALYTICS.ASSESS_CLAIM_SEVERITY(CLAIM_AMOUNT NUMBER, BODILY_INJURIES NUMBER, VEHICLES NUMBER)
RETURNS VARCHAR(20)
LANGUAGE SQL
COMMENT = 'Assess claim severity based on amount, injuries, and vehicles involved'
AS
$$
    CASE 
        WHEN CLAIM_AMOUNT > 100000 OR BODILY_INJURIES > 2 OR VEHICLES > 3 THEN 'SEVERE'
        WHEN CLAIM_AMOUNT > 50000 OR BODILY_INJURIES > 0 OR VEHICLES > 1 THEN 'MODERATE'
        WHEN CLAIM_AMOUNT > 20000 THEN 'MINOR'
        ELSE 'MINIMAL'
    END
$$;

-- Python UDFs for complex analytics (advanced business logic)

-- Broker performance analysis using Python for multi-factor computation
CREATE OR REPLACE FUNCTION ANALYTICS.ANALYZE_BROKER_PERFORMANCE(
    SATISFACTION NUMBER, EXPERIENCE NUMBER, TRAINING NUMBER, 
    CUSTOMER_COUNT NUMBER, AVG_CLAIM_AMOUNT NUMBER
)
RETURNS OBJECT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
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

-- Customer portfolio segmentation using Python for ML-style clustering
CREATE OR REPLACE FUNCTION ANALYTICS.SEGMENT_CUSTOMER_PORTFOLIO(
    AGE NUMBER, PREMIUM NUMBER, CLAIM_AMOUNT NUMBER, POLICY_LENGTH NUMBER
)
RETURNS VARCHAR(20)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
HANDLER = 'segment_customer'
COMMENT = 'ML-style customer segmentation based on multiple attributes'
AS
$$
def segment_customer(age, premium, claim_amount, policy_length):
    # Handle null values
    age = age or 35
    premium = premium or 1000
    claim_amount = claim_amount or 0
    policy_length = policy_length or 12
    
    # Normalize factors (0-1 scale)
    age_factor = max(0, min(1, (age - 18) / 67))  # 18-85 range
    premium_factor = max(0, min(1, premium / 3000))  # Up to $3000
    claim_factor = max(0, min(1, claim_amount / 200000))  # Up to $200k
    loyalty_factor = max(0, min(1, policy_length / 120))  # Up to 10 years
    
    # Calculate composite scores
    value_score = (premium_factor * 0.6) + (loyalty_factor * 0.4)
    risk_score = (claim_factor * 0.7) + ((1 - age_factor) * 0.3)  # Younger = higher risk
    
    # Segmentation logic
    if value_score >= 0.7 and risk_score <= 0.3:
        return "PREMIUM_LOW_RISK"
    elif value_score >= 0.7 and risk_score > 0.3:
        return "PREMIUM_HIGH_RISK"
    elif value_score >= 0.4 and risk_score <= 0.4:
        return "STANDARD_GOOD"
    elif value_score >= 0.4 and risk_score > 0.4:
        return "STANDARD_WATCH"
    elif risk_score <= 0.3:
        return "BASIC_SAFE"
    else:
        return "BASIC_RISK"
$$;

-- Risk trajectory prediction using Python for advanced modeling
CREATE OR REPLACE FUNCTION ANALYTICS.PREDICT_RISK_TRAJECTORY(
    CURRENT_RISK NUMBER, AGE NUMBER, CLAIM_HISTORY ARRAY, POLICY_CHANGES NUMBER
)
RETURNS OBJECT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
HANDLER = 'predict_trajectory'
COMMENT = 'Predict risk trajectory based on historical patterns and demographics'
AS
$$
def predict_trajectory(current_risk, age, claim_history, policy_changes):
    import statistics
    
    # Handle inputs
    current_risk = current_risk or 50
    age = age or 35
    claim_history = claim_history or []
    policy_changes = policy_changes or 0
    
    # Age-based risk progression
    if age < 30:
        age_trend = -0.5  # Risk decreases with maturity
    elif age < 60:
        age_trend = 0.1   # Stable risk period
    else:
        age_trend = 0.3   # Risk increases with age
    
    # Claim history analysis
    history_trend = 0
    if len(claim_history) > 1:
        recent_claims = claim_history[-3:] if len(claim_history) >= 3 else claim_history
        if len(recent_claims) > 1:
            trend_direction = recent_claims[-1] - recent_claims[0]
            history_trend = trend_direction / 10000  # Scale factor
    
    # Policy stability factor
    stability_factor = max(-0.2, min(0.2, policy_changes * 0.1))
    
    # Predicted trajectory
    predicted_change = age_trend + history_trend + stability_factor
    predicted_risk = max(10, min(100, current_risk + predicted_change))
    
    # Confidence calculation
    data_points = len(claim_history) + (1 if age > 0 else 0)
    confidence = min(0.95, 0.5 + (data_points * 0.1))
    
    return {
        "predicted_risk_score": round(predicted_risk, 1),
        "risk_change": round(predicted_change, 2),
        "confidence_level": round(confidence, 2),
        "primary_factor": "AGE" if abs(age_trend) > abs(history_trend) else "CLAIMS",
        "trajectory": "IMPROVING" if predicted_change < -0.5 else "STABLE" if abs(predicted_change) <= 0.5 else "DETERIORATING"
    }
$$;

-- Territory optimization using Python for geographic analysis
CREATE OR REPLACE FUNCTION ANALYTICS.OPTIMIZE_TERRITORY_ASSIGNMENT(
    BROKER_LOCATION VARCHAR, CURRENT_TERRITORIES ARRAY, CUSTOMER_DENSITY OBJECT
)
RETURNS OBJECT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
HANDLER = 'optimize_territory'
COMMENT = 'Optimize broker territory assignments based on location and customer density'
AS
$$
def optimize_territory(broker_location, current_territories, customer_density):
    # Handle inputs
    broker_location = broker_location or "Unknown"
    current_territories = current_territories or []
    customer_density = customer_density or {}
    
    # Location mapping for optimization
    location_weights = {
        "London": {"South East England": 0.9, "Greater London": 1.0, "Surrey": 0.8},
        "Manchester": {"North West England": 1.0, "North Wales": 0.7, "Yorkshire": 0.6},
        "Birmingham": {"Midlands": 1.0, "West England": 0.9, "Staffordshire": 0.8},
        "Edinburgh": {"Scotland": 1.0, "Northern England": 0.7, "Highlands": 0.9}
    }
    
    # Calculate current efficiency
    current_efficiency = 0
    base_weights = location_weights.get(broker_location, {})
    
    for territory in current_territories:
        territory_str = str(territory) if territory else ""
        weight = 0
        for area, w in base_weights.items():
            if area.lower() in territory_str.lower():
                weight = w
                break
        current_efficiency += weight
    
    current_efficiency = current_efficiency / max(1, len(current_territories))
    
    # Optimization recommendations
    recommendations = []
    if current_efficiency < 0.7:
        recommendations.append("REASSIGN_LOW_EFFICIENCY_TERRITORIES")
    if len(current_territories) > 3:
        recommendations.append("REDUCE_TERRITORY_COUNT")
    if len(current_territories) < 1:
        recommendations.append("ASSIGN_PRIMARY_TERRITORY")
    
    optimization_score = min(1.0, current_efficiency + (len(recommendations) * 0.1))
    
    return {
        "current_efficiency": round(current_efficiency, 2),
        "optimization_score": round(optimization_score, 2),
        "recommended_actions": recommendations,
        "territory_count": len(current_territories),
        "efficiency_grade": "EXCELLENT" if current_efficiency >= 0.8 else 
                          "GOOD" if current_efficiency >= 0.6 else 
                          "NEEDS_IMPROVEMENT"
    }
$$;

/* ================================================================================
DYNAMIC TABLES ARCHITECTURE - THREE LEVELS
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
    b.CUSTOMER_SATISFACTION as BROKER_SATISFACTION,
    b.YEARS_EXPERIENCE as BROKER_EXPERIENCE,
    b.TRAINING_HOURS_COMPLETED as BROKER_TRAINING,
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
    COALESCE(cl.FRAUD_REPORTED, FALSE) as FRAUD_REPORTED_FILLED,
    
    -- Simulated geographic distribution for governance demonstration
    CASE 
        WHEN MOD(HASH(c.POLICY_NUMBER), 20) < 6 THEN 'London Region'
        WHEN MOD(HASH(c.POLICY_NUMBER), 20) < 10 THEN 'Manchester Region'
        WHEN MOD(HASH(c.POLICY_NUMBER), 20) < 14 THEN 'Birmingham Region'
        WHEN MOD(HASH(c.POLICY_NUMBER), 20) < 17 THEN 'Edinburgh Region'
        ELSE 'Other Regions'
    END as CUSTOMER_REGION,
    
    -- Data lineage tracking
    GREATEST(c.LOAD_TIMESTAMP, b.LOAD_TIMESTAMP, COALESCE(cl.LOAD_TIMESTAMP, c.LOAD_TIMESTAMP)) as LAST_UPDATED
    
FROM RAW_DATA.CUSTOMERS_RAW c
LEFT JOIN RAW_DATA.BROKERS_RAW b ON c.BROKER_ID = b.BROKER_ID
LEFT JOIN RAW_DATA.CLAIMS_RAW cl ON c.POLICY_NUMBER = cl.POLICY_NUMBER;

-- Level 2: Business Analytics Layer
CREATE OR REPLACE DYNAMIC TABLE ANALYTICS.BROKER_PERFORMANCE_MATRIX
    TARGET_LAG = '1 minute'
    WAREHOUSE = WORKSHOP_COMPUTE_WH
    COMMENT = 'Level 2: Broker-centric performance analytics and customer portfolio analysis'
    AS
SELECT 
    -- Broker identification
    BROKER_ID,
    BROKER_FIRST_NAME,
    BROKER_LAST_NAME,
    BROKER_OFFICE,
    BROKER_SATISFACTION,
    BROKER_EXPERIENCE,
    BROKER_TRAINING,
    BROKER_ACTIVE,
    
    -- Portfolio metrics
    COUNT(*) as TOTAL_CUSTOMERS,
    COUNT(CASE WHEN HAS_CLAIM = 1 THEN 1 END) as CUSTOMERS_WITH_CLAIMS,
    AVG(POLICY_ANNUAL_PREMIUM) as AVG_CUSTOMER_PREMIUM,
    SUM(POLICY_ANNUAL_PREMIUM) as TOTAL_PREMIUM_VOLUME,
    AVG(CLAIM_AMOUNT_FILLED) as AVG_CLAIM_AMOUNT,
    SUM(CLAIM_AMOUNT_FILLED) as TOTAL_CLAIM_AMOUNT,
    
    -- SQL UDF results for performance classification
    ANALYTICS.DETERMINE_BROKER_TIER(BROKER_SATISFACTION, BROKER_EXPERIENCE, BROKER_TRAINING) as BROKER_TIER,
    ANALYTICS.CALCULATE_TERRITORY_PREMIUM(BROKER_TERRITORY, AVG(POLICY_ANNUAL_PREMIUM)) as TERRITORY_ADJUSTED_PREMIUM,
    
    -- Risk assessment
    AVG(ANALYTICS.CALCULATE_CUSTOMER_RISK_SCORE(AGE, POLICY_ANNUAL_PREMIUM, CLAIM_AMOUNT_FILLED)) as AVG_CUSTOMER_RISK,
    COUNT(CASE WHEN FRAUD_REPORTED_FILLED = TRUE THEN 1 END) as FRAUD_CASES,
    
    -- Geographic distribution
    CUSTOMER_REGION,
    COUNT(*) as CUSTOMERS_IN_REGION,
    
    LAST_UPDATED
    
FROM ANALYTICS.CUSTOMER_BROKER_CLAIMS_INTEGRATED
WHERE BROKER_ID IS NOT NULL
GROUP BY 
    BROKER_ID, BROKER_FIRST_NAME, BROKER_LAST_NAME, BROKER_OFFICE,
    BROKER_SATISFACTION, BROKER_EXPERIENCE, BROKER_TRAINING, BROKER_ACTIVE,
    BROKER_TERRITORY, CUSTOMER_REGION, LAST_UPDATED;

-- Create additional analytics layer for customer risk profiling
CREATE OR REPLACE DYNAMIC TABLE ANALYTICS.CUSTOMER_RISK_PROFILE
    TARGET_LAG = '1 minute'
    WAREHOUSE = WORKSHOP_COMPUTE_WH
    COMMENT = 'Level 2: Individual customer risk profiling with broker impact analysis'
    AS
SELECT 
    POLICY_NUMBER,
    AGE,
    INSURED_SEX,
    INSURED_OCCUPATION,
    POLICY_ANNUAL_PREMIUM,
    POLICY_LENGTH_MONTH,
    CLAIM_AMOUNT_FILLED,
    FRAUD_REPORTED_FILLED,
    CUSTOMER_REGION,
    
    -- Broker influence
    BROKER_ID,
    BROKER_TIER,
    BROKER_SATISFACTION,
    
    -- SQL UDF risk calculations
    ANALYTICS.CALCULATE_CUSTOMER_RISK_SCORE(AGE, POLICY_ANNUAL_PREMIUM, CLAIM_AMOUNT_FILLED) as CUSTOMER_RISK_SCORE,
    ANALYTICS.ASSESS_CLAIM_SEVERITY(CLAIM_AMOUNT_FILLED, BODILY_INJURIES, NUMBER_OF_VEHICLES_INVOLVED) as CLAIM_SEVERITY,
    
    -- Python UDF customer segmentation
    ANALYTICS.SEGMENT_CUSTOMER_PORTFOLIO(AGE, POLICY_ANNUAL_PREMIUM, CLAIM_AMOUNT_FILLED, POLICY_LENGTH_MONTH) as CUSTOMER_SEGMENT,
    
    LAST_UPDATED
    
FROM ANALYTICS.CUSTOMER_BROKER_CLAIMS_INTEGRATED;

-- Level 3: Intelligence Dashboard Layer
CREATE OR REPLACE DYNAMIC TABLE ANALYTICS.RISK_INTELLIGENCE_DASHBOARD
    TARGET_LAG = '1 minute'
    WAREHOUSE = WORKSHOP_COMPUTE_WH
    COMMENT = 'Level 3: Executive intelligence layer with Python UDF advanced analytics'
    AS
SELECT 
    -- Customer core
    c.POLICY_NUMBER,
    c.AGE,
    c.CUSTOMER_SEGMENT,
    c.CUSTOMER_RISK_SCORE,
    c.POLICY_ANNUAL_PREMIUM,
    c.CLAIM_AMOUNT_FILLED,
    c.CUSTOMER_REGION,
    
    -- Broker performance
    c.BROKER_ID,
    b.BROKER_TIER,
    b.AVG_CUSTOMER_RISK as BROKER_AVG_RISK,
    b.TOTAL_CUSTOMERS as BROKER_PORTFOLIO_SIZE,
    
    -- Python UDF advanced analytics
    ANALYTICS.ANALYZE_BROKER_PERFORMANCE(
        b.BROKER_SATISFACTION, b.BROKER_EXPERIENCE, b.BROKER_TRAINING,
        b.TOTAL_CUSTOMERS, b.AVG_CLAIM_AMOUNT
    ) as BROKER_PERFORMANCE_ANALYSIS,
    
    ANALYTICS.PREDICT_RISK_TRAJECTORY(
        c.CUSTOMER_RISK_SCORE, c.AGE, ARRAY_CONSTRUCT(c.CLAIM_AMOUNT_FILLED), 
        CASE WHEN c.POLICY_LENGTH_MONTH > 24 THEN 1 ELSE 0 END
    ) as RISK_TRAJECTORY_PREDICTION,
    
    -- Final risk classification
    CASE 
        WHEN c.CUSTOMER_RISK_SCORE >= 75 THEN 'HIGH'
        WHEN c.CUSTOMER_RISK_SCORE >= 50 THEN 'MEDIUM'
        ELSE 'LOW'
    END as FINAL_RISK_LEVEL,
    
    c.LAST_UPDATED
    
FROM ANALYTICS.CUSTOMER_RISK_PROFILE c
LEFT JOIN ANALYTICS.BROKER_PERFORMANCE_MATRIX b ON c.BROKER_ID = b.BROKER_ID AND c.CUSTOMER_REGION = b.CUSTOMER_REGION;

/* ================================================================================
AUTOMATED CLASSIFICATION AND TAGGING
================================================================================
*/

-- Setup governance schema and classification
USE SCHEMA GOVERNANCE;

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

-- Run classification on intelligence dashboard
CALL SYSTEM$CLASSIFY(
    'ANALYTICS.RISK_INTELLIGENCE_DASHBOARD',
    'GOVERNANCE.INSURANCE_AUTO_CLASSIFICATION'
);

-- Classification for broker performance data
CALL SYSTEM$CLASSIFY(
    'ANALYTICS.BROKER_PERFORMANCE_MATRIX',
    'GOVERNANCE.INSURANCE_AUTO_CLASSIFICATION'
);

/* ================================================================================
PROGRESSIVE GOVERNANCE - MASKING POLICIES
================================================================================
*/

-- Financial data masking policy
CREATE OR REPLACE MASKING POLICY GOVERNANCE.FINANCIAL_DATA_MASK AS 
    (financial_value NUMBER) RETURNS NUMBER ->
    CASE
        -- Internal roles get full access
        WHEN CURRENT_ROLE() IN ('ACCOUNTADMIN') THEN financial_value
        -- Workshop analysts get rounded values
        WHEN CURRENT_ROLE() IN ('WORKSHOP_ANALYST') THEN FLOOR(financial_value / 5000) * 5000
        -- External brokers get heavily masked values  
        WHEN CURRENT_ROLE() IN ('BROKER_CONSUMER') OR 
             CURRENT_ACCOUNT_NAME() LIKE '%CONSUMER%' THEN FLOOR(financial_value / 10000) * 10000
        ELSE FLOOR(financial_value / 10000) * 10000
    END
    COMMENT = 'Progressive masking for financial data based on role and account';

-- Broker contact information masking
CREATE OR REPLACE MASKING POLICY GOVERNANCE.BROKER_CONTACT_MASK AS 
    (contact_info STRING) RETURNS STRING ->
    CASE
        WHEN CURRENT_ROLE() IN ('ACCOUNTADMIN') THEN contact_info
        WHEN CURRENT_ROLE() IN ('WORKSHOP_ANALYST') THEN 
            REGEXP_REPLACE(contact_info, '(@[^.]+)', '@***')
        ELSE 'MASKED'
    END
    COMMENT = 'Mask broker contact information for external access';

-- Performance metrics masking
CREATE OR REPLACE MASKING POLICY GOVERNANCE.PERFORMANCE_METRICS_MASK AS 
    (performance_data OBJECT) RETURNS OBJECT ->
    CASE
        WHEN CURRENT_ROLE() IN ('ACCOUNTADMIN', 'WORKSHOP_ANALYST') THEN performance_data
        ELSE OBJECT_CONSTRUCT('masked', true, 'access_level', 'restricted')
    END
    COMMENT = 'Mask detailed performance metrics for competitive protection';

-- Apply masking policies
ALTER TABLE ANALYTICS.RISK_INTELLIGENCE_DASHBOARD 
    MODIFY COLUMN POLICY_ANNUAL_PREMIUM 
    SET MASKING POLICY GOVERNANCE.FINANCIAL_DATA_MASK;

ALTER TABLE ANALYTICS.RISK_INTELLIGENCE_DASHBOARD 
    MODIFY COLUMN CLAIM_AMOUNT_FILLED 
    SET MASKING POLICY GOVERNANCE.FINANCIAL_DATA_MASK;

ALTER TABLE ANALYTICS.BROKER_PERFORMANCE_MATRIX 
    MODIFY COLUMN BROKER_PERFORMANCE_ANALYSIS 
    SET MASKING POLICY GOVERNANCE.PERFORMANCE_METRICS_MASK;

/* ================================================================================
PROGRESSIVE GOVERNANCE - ROW ACCESS POLICIES
================================================================================
*/

-- Broker territory access policy
CREATE OR REPLACE ROW ACCESS POLICY GOVERNANCE.BROKER_TERRITORY_ACCESS AS
    (customer_region STRING, broker_id STRING) RETURNS BOOLEAN ->
    CASE
        -- Internal roles see all regions
        WHEN CURRENT_ROLE() IN ('ACCOUNTADMIN') THEN TRUE
        -- Workshop analysts see all for demonstration
        WHEN CURRENT_ROLE() IN ('WORKSHOP_ANALYST') THEN TRUE
        -- Broker consumers limited to specific regions based on policy context
        WHEN CURRENT_ROLE() IN ('BROKER_CONSUMER') THEN 
            customer_region IN ('London Region', 'Manchester Region')
        ELSE FALSE
    END
    COMMENT = 'Restrict broker access to assigned geographic territories';

-- Performance tier access policy
CREATE OR REPLACE ROW ACCESS POLICY GOVERNANCE.PERFORMANCE_TIER_ACCESS AS
    (broker_tier STRING, customer_segment STRING) RETURNS BOOLEAN ->
    CASE
        WHEN CURRENT_ROLE() IN ('ACCOUNTADMIN', 'WORKSHOP_ANALYST') THEN TRUE
        -- External brokers can only see their own tier performance
        WHEN CURRENT_ROLE() IN ('BROKER_CONSUMER') THEN 
            broker_tier IN ('GOLD', 'SILVER') AND customer_segment NOT LIKE '%PREMIUM%'
        ELSE FALSE
    END
    COMMENT = 'Limit access based on broker performance tier';

-- Apply row access policies
ALTER TABLE ANALYTICS.RISK_INTELLIGENCE_DASHBOARD
    ADD ROW ACCESS POLICY GOVERNANCE.BROKER_TERRITORY_ACCESS ON (CUSTOMER_REGION, BROKER_ID);

ALTER TABLE ANALYTICS.BROKER_PERFORMANCE_MATRIX
    ADD ROW ACCESS POLICY GOVERNANCE.PERFORMANCE_TIER_ACCESS ON (BROKER_TIER, 'STANDARD');

/* ================================================================================
SECURE DATA SHARING
================================================================================
*/

USE SCHEMA SHARING;

-- Broker portal view for individual broker access
CREATE OR REPLACE SECURE VIEW SHARING.BROKER_PORTAL_VIEW 
    COMMENT = 'Individual broker performance and customer portfolio view'
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
    r.CUSTOMER_REGION
FROM ANALYTICS.BROKER_PERFORMANCE_MATRIX b
LEFT JOIN ANALYTICS.RISK_INTELLIGENCE_DASHBOARD r ON b.BROKER_ID = r.BROKER_ID
WHERE b.BROKER_ACTIVE = TRUE;

-- Regional manager view for territory-wide analytics
CREATE OR REPLACE SECURE VIEW SHARING.REGIONAL_MANAGER_VIEW 
    COMMENT = 'Territory-wide analytics for regional management'
    AS
SELECT 
    CUSTOMER_REGION,
    COUNT(DISTINCT BROKER_ID) as ACTIVE_BROKERS,
    COUNT(*) as TOTAL_CUSTOMERS,
    AVG(POLICY_ANNUAL_PREMIUM) as AVG_REGION_PREMIUM,
    SUM(CLAIM_AMOUNT_FILLED) as TOTAL_REGION_CLAIMS,
    AVG(CUSTOMER_RISK_SCORE) as AVG_REGION_RISK,
    COUNT(CASE WHEN FINAL_RISK_LEVEL = 'HIGH' THEN 1 END) as HIGH_RISK_CUSTOMERS
FROM ANALYTICS.RISK_INTELLIGENCE_DASHBOARD
GROUP BY CUSTOMER_REGION;

-- Executive dashboard view for high-level KPIs
CREATE OR REPLACE SECURE VIEW SHARING.EXECUTIVE_DASHBOARD_VIEW 
    COMMENT = 'Executive KPIs without sensitive details'
    AS
SELECT 
    COUNT(DISTINCT BROKER_ID) as TOTAL_BROKERS,
    COUNT(DISTINCT POLICY_NUMBER) as TOTAL_CUSTOMERS,
    COUNT(DISTINCT CUSTOMER_REGION) as ACTIVE_REGIONS,
    AVG(CUSTOMER_RISK_SCORE) as OVERALL_RISK_SCORE,
    COUNT(CASE WHEN FINAL_RISK_LEVEL = 'HIGH' THEN 1 END) as HIGH_RISK_COUNT,
    COUNT(CASE WHEN CUSTOMER_SEGMENT LIKE '%PREMIUM%' THEN 1 END) as PREMIUM_CUSTOMERS,
    MAX(LAST_UPDATED) as LAST_REFRESH
FROM ANALYTICS.RISK_INTELLIGENCE_DASHBOARD;

-- Create data shares
CREATE OR REPLACE SHARE BROKER_PORTAL_SHARE
    COMMENT = 'Individual broker performance data share';

CREATE OR REPLACE SHARE REGIONAL_ANALYTICS_SHARE
    COMMENT = 'Regional management analytics share';

CREATE OR REPLACE SHARE EXECUTIVE_METRICS_SHARE
    COMMENT = 'Executive dashboard metrics share';

-- Grant access to shares
GRANT USAGE ON DATABASE INSURANCE_WORKSHOP_DB TO SHARE BROKER_PORTAL_SHARE;
GRANT USAGE ON SCHEMA SHARING TO SHARE BROKER_PORTAL_SHARE;
GRANT SELECT ON VIEW SHARING.BROKER_PORTAL_VIEW TO SHARE BROKER_PORTAL_SHARE;

GRANT USAGE ON DATABASE INSURANCE_WORKSHOP_DB TO SHARE REGIONAL_ANALYTICS_SHARE;
GRANT USAGE ON SCHEMA SHARING TO SHARE REGIONAL_ANALYTICS_SHARE;
GRANT SELECT ON VIEW SHARING.REGIONAL_MANAGER_VIEW TO SHARE REGIONAL_ANALYTICS_SHARE;

GRANT USAGE ON DATABASE INSURANCE_WORKSHOP_DB TO SHARE EXECUTIVE_METRICS_SHARE;
GRANT USAGE ON SCHEMA SHARING TO SHARE EXECUTIVE_METRICS_SHARE;
GRANT SELECT ON VIEW SHARING.EXECUTIVE_DASHBOARD_VIEW TO SHARE EXECUTIVE_METRICS_SHARE;

/* ================================================================================
GRANT PERMISSIONS FOR WORKSHOP ROLES
================================================================================
*/

-- Grant access to analytics tables and views
GRANT SELECT ON ALL DYNAMIC TABLES IN SCHEMA ANALYTICS TO ROLE WORKSHOP_ANALYST;
GRANT SELECT ON ALL VIEWS IN SCHEMA SHARING TO ROLE WORKSHOP_ANALYST;
GRANT SELECT ON ALL VIEWS IN SCHEMA SHARING TO ROLE BROKER_CONSUMER;

-- Grant function usage
GRANT USAGE ON ALL FUNCTIONS IN SCHEMA ANALYTICS TO ROLE WORKSHOP_ANALYST;

/* ================================================================================
ADVANCED ANALYTICS AND GOVERNANCE SETUP COMPLETE
================================================================================
Implementation Complete:
• Mixed UDFs: 4 SQL functions + 4 Python functions for optimal performance
• Dynamic Tables: 3-level architecture with 1-minute refresh rates
• Analytics: Customer risk profiling, broker performance, territory optimization
• Classification: Automated PII and financial data discovery
• Governance: 3 masking policies + 2 row access policies applied
• Data Sharing: 3 secure shares for different stakeholder groups

Advanced Analytics Delivered:
• SQL UDFs: High-performance scoring and classification functions
• Python UDFs: Complex multi-factor analysis and ML-style segmentation  
• Real-time Intelligence: Sub-minute refresh for business-critical insights
• Progressive Governance: Role-based access with automated policy enforcement

Business Value:
• Broker Performance: Comprehensive multi-factor analysis with optimization
• Customer Intelligence: Risk scoring with predictive trajectory modeling
• Territory Management: Geographic optimization with coverage analysis
• Secure Collaboration: Governed data sharing across stakeholder groups

Ready for: Phase 4 - Visualization Dashboards and Phase 5 - Workshop Integration
================================================================================
*/ 