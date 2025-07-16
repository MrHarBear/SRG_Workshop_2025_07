/* ================================================================================
INSURANCE WORKSHOP - RISK ANALYTICS
================================================================================
Purpose: Advanced analytics with mixed UDFs and Dynamic Tables for three-entity model
Scope: SQL/Python UDFs, multi-level Dynamic Tables, real-time analytics
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
    CURRENT_TIMESTAMP() as LAST_UPDATED
    
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
    
    CURRENT_TIMESTAMP() as LAST_UPDATED
    
FROM ANALYTICS.CUSTOMER_BROKER_CLAIMS_INTEGRATED
WHERE BROKER_ID IS NOT NULL
GROUP BY 
    BROKER_ID, BROKER_FIRST_NAME, BROKER_LAST_NAME, BROKER_OFFICE,
    BROKER_SATISFACTION, BROKER_EXPERIENCE, BROKER_TRAINING, BROKER_ACTIVE,
    BROKER_TERRITORY, CUSTOMER_REGION;

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
    ANALYTICS.DETERMINE_BROKER_TIER(
        (SELECT BROKER_SATISFACTION FROM ANALYTICS.CUSTOMER_BROKER_CLAIMS_INTEGRATED b WHERE b.BROKER_ID = CUSTOMER_BROKER_CLAIMS_INTEGRATED.BROKER_ID LIMIT 1),
        (SELECT BROKER_EXPERIENCE FROM ANALYTICS.CUSTOMER_BROKER_CLAIMS_INTEGRATED b WHERE b.BROKER_ID = CUSTOMER_BROKER_CLAIMS_INTEGRATED.BROKER_ID LIMIT 1),
        (SELECT BROKER_TRAINING FROM ANALYTICS.CUSTOMER_BROKER_CLAIMS_INTEGRATED b WHERE b.BROKER_ID = CUSTOMER_BROKER_CLAIMS_INTEGRATED.BROKER_ID LIMIT 1)
    ) as BROKER_TIER,
    BROKER_SATISFACTION,
    
    -- SQL UDF risk calculations
    ANALYTICS.CALCULATE_CUSTOMER_RISK_SCORE(AGE, POLICY_ANNUAL_PREMIUM, CLAIM_AMOUNT_FILLED) as CUSTOMER_RISK_SCORE,
    ANALYTICS.ASSESS_CLAIM_SEVERITY(CLAIM_AMOUNT_FILLED, BODILY_INJURIES, NUMBER_OF_VEHICLES_INVOLVED) as CLAIM_SEVERITY,
    
    -- Python UDF customer segmentation
    ANALYTICS.SEGMENT_CUSTOMER_PORTFOLIO(AGE, POLICY_ANNUAL_PREMIUM, CLAIM_AMOUNT_FILLED, POLICY_LENGTH_MONTH) as CUSTOMER_SEGMENT,
    
    CURRENT_TIMESTAMP() as LAST_UPDATED
    
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
    
    CURRENT_TIMESTAMP() as LAST_UPDATED
    
FROM ANALYTICS.CUSTOMER_RISK_PROFILE c
LEFT JOIN ANALYTICS.BROKER_PERFORMANCE_MATRIX b ON c.BROKER_ID = b.BROKER_ID AND c.CUSTOMER_REGION = b.CUSTOMER_REGION;

/* ================================================================================
GRANT PERMISSIONS FOR WORKSHOP ROLES
================================================================================
*/

-- Grant access to analytics tables and UDFs
GRANT SELECT ON ALL DYNAMIC TABLES IN SCHEMA ANALYTICS TO ROLE WORKSHOP_ANALYST;
GRANT USAGE ON ALL FUNCTIONS IN SCHEMA ANALYTICS TO ROLE WORKSHOP_ANALYST;

/* ================================================================================
RISK ANALYTICS SETUP COMPLETE
================================================================================
Implementation Complete:
• Mixed UDFs: 4 SQL functions + 4 Python functions for optimal performance balance
• Dynamic Tables: 3-level architecture with 1-minute refresh rates for real-time insights
• Analytics Coverage: Customer risk profiling, broker performance, territory optimization
• Integration Layer: Complete customer-broker-claims data fusion
• Business Layer: Performance matrices and risk profiling with UDF calculations
• Intelligence Layer: Advanced Python analytics for predictive insights

Analytics Functions Delivered:
• SQL UDFs: High-performance risk scoring, tier classification, territory premiums, claim severity
• Python UDFs: Multi-factor broker analysis, ML-style customer segmentation, risk prediction, territory optimization
• Real-time Processing: Sub-minute refresh for business-critical analytics
• Scalable Architecture: Three-level Dynamic Tables design for performance and maintainability

Business Intelligence:
• Customer Risk Scoring: Age, premium, and claim-based risk assessment
• Broker Performance Analysis: Comprehensive multi-factor evaluation with tier classification
• Territory Optimization: Geographic efficiency analysis with actionable recommendations
• Predictive Analytics: Risk trajectory modeling with confidence levels

Ready for: Phase 3 - Governance Implementation and Phase 4 - Visualization Dashboards
================================================================================
*/ 