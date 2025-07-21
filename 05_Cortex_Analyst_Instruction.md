# Cortex Analyst Setup and Demonstration Guide

## Overview

This guide demonstrates Snowflake Cortex Analyst's natural language query capabilities using our Risk Intelligence Dashboard. You'll learn how to create a semantic model, ask natural language questions, and see the power of filters and metrics in action.

## Prerequisites

- Access to Snowflake with Cortex Analyst enabled
- Completed execution of `02_RISK_ANALYTICS_GOVERNANCE.sql` (RISK_INTELLIGENCE_DASHBOARD table exists)
- ACCOUNTADMIN or appropriate privileges to create Cortex Analyst

---

## Step 1: Create Cortex Analyst in Snowflake UI

### 1.1 Navigate to Cortex Analyst
1. Log into your Snowflake account
2. In the left navigation, click **"AI & ML"**
3. Select **"Cortex Analyst"**
4. Click **"+ Analyst"** button

### 1.2 Basic Configuration
1. **Name**: `Risk_Intelligence_Analyst`
2. **Database**: `INSURANCE_WORKSHOP_DB`
3. **Schema**: `ANALYTICS`
4. **Warehouse**: `WORKSHOP_COMPUTE_WH`

### 1.3 Add Semantic Model
1. Click **"Add Tables"**
2. Select `RISK_INTELLIGENCE_DASHBOARD` table
3. Click **"Edit Semantic Model"**
4. Replace the auto-generated YAML with the semantic model below

---

## Step 2: Semantic Model YAML Configuration

Copy and paste this complete semantic model:

```yaml
name: analyst_dashboard
tables:
  - name: RISK_INTELLIGENCE_DASHBOARD
    base_table:
      database: INSURANCE_WORKSHOP_DB
      schema: ANALYTICS
      table: RISK_INTELLIGENCE_DASHBOARD
    dimensions:
      - name: POLICY_NUMBER
        expr: POLICY_NUMBER
        data_type: VARCHAR(50)
        sample_values:
          - '521585'
          - '342868'
          - '687698'
        description: Unique identifier for a specific insurance policy.
        synonyms:
          - policy_id
          - policy_reference
          - policy_code
          - contract_number
          - insurance_policy_number
          - policy_identifier
      - name: INSURED_SEX
        expr: INSURED_SEX
        data_type: VARCHAR(10)
        sample_values:
          - M
          - F
        description: The sex of the insured individual, either Male (M) or Female (F).
        synonyms:
          - gender
          - insured_gender
          - policyholder_sex
          - insured_person_gender
          - policyholder_gender
      - name: INSURED_OCCUPATION
        expr: INSURED_OCCUPATION
        data_type: VARCHAR(100)
        sample_values:
          - craft-repair
          - machine-op-inspct
          - sales
        description: 'The occupation of the insured individual, categorizing their job type into one of the following categories: craft or repair work, machine operation or inspection, or sales.'
        synonyms:
          - insured_job
          - insured_profession
          - insured_role
          - insured_work
          - insured_position
          - insured_title
          - insured_vocation
          - insured_employment
      - name: FRAUD_REPORTED_FILLED
        expr: FRAUD_REPORTED_FILLED
        data_type: BOOLEAN
        sample_values:
          - 'TRUE'
          - 'FALSE'
        description: Indicates whether a fraud report has been filed for a specific transaction or account.
        synonyms:
          - fraudulent_claims
          - reported_fraud
          - filled_fraudulent_claims
          - claims_marked_as_fraud
          - fraudulent_claims_filled
      - name: BROKER_ID
        expr: BROKER_ID
        data_type: VARCHAR(10)
        sample_values:
          - BRK008
          - BRK017
          - BRK003
        description: Unique identifier for the broker firm that executed the trade.
        synonyms:
          - broker_identifier
          - agent_id
          - insurance_agent_id
          - intermediary_id
          - broker_code
          - insurance_broker_id
          - agent_code
      - name: BROKER_FIRST_NAME
        expr: BROKER_FIRST_NAME
        data_type: VARCHAR(16777216)
        sample_values:
          - Helen
          - Christopher
          - David
        description: The first name of the broker associated with the risk intelligence data.
        synonyms:
          - broker_given_name
          - first_name_of_broker
          - broker_forename
          - first_name_broker
          - broker_firstname
      - name: BROKER_LAST_NAME
        expr: BROKER_LAST_NAME
        data_type: VARCHAR(16777216)
        sample_values:
          - Clark
          - Campbell
          - Thompson
        description: The last name of the broker associated with the risk intelligence data.
        synonyms:
          - broker_surname
          - broker_family_name
          - broker_last_name_on_record
          - broker_full_last_name
          - broker_registered_last_name
      - name: BROKER_OFFICE
        expr: BROKER_OFFICE
        data_type: VARCHAR(16777216)
        sample_values:
          - Nottingham
          - Gloucester
          - Birmingham
        description: The location of the insurance broker's office.
        synonyms:
          - broker_location
          - office_location
          - broker_branch
          - agency_location
          - brokerage_office
          - office_name
      - name: CUSTOMER_REGION
        expr: CUSTOMER_REGION
        data_type: VARCHAR(17)
        sample_values:
          - Other Region
          - Birmingham Region
          - London Region
        description: Geographic region where the customer is located, categorized into distinct areas for analysis and reporting purposes.
        synonyms:
          - geographic_area
          - customer_location
          - regional_area
          - area_of_operation
          - customer_territory
          - sales_region
          - market_area
      - name: CUSTOMER_SEGMENT
        expr: CUSTOMER_SEGMENT
        data_type: VARCHAR(8)
        sample_values:
          - BASIC
          - STANDARD
        description: 'The CUSTOMER_SEGMENT column categorizes customers into distinct groups based on their product or service usage patterns, with two defined segments: BASIC and STANDARD, which likely reflect varying levels of service or feature access, with BASIC typically representing a more limited or entry-level offering and STANDARD representing a more comprehensive or premium offering.'
        synonyms:
          - customer_category
          - policy_holder_group
          - client_tier
          - customer_type
          - policy_segment
          - client_classification
      - name: BROKER_TIER
        expr: BROKER_TIER
        data_type: VARCHAR(8)
        sample_values:
          - GOLD
          - PLATINUM
          - BRONZE
        description: The tier level of the broker, indicating their level of service and benefits, with GOLD being the highest, PLATINUM being the middle, and BRONZE being the lowest.
        synonyms:
          - broker_level
          - broker_rank
          - broker_grade
          - broker_classification
          - broker_category
          - broker_rating
      - name: BROKER_PERFORMANCE_ANALYSIS
        expr: BROKER_PERFORMANCE_ANALYSIS
        data_type: OBJECT
        sample_values:
          - |-
            {
              "experience_component": 56,
              "performance_tier": "ELITE",
              "portfolio_component": 50,
              "risk_management_component": 2.570000000000000e+01,
              "satisfaction_component": 100,
              "total_score": 2.917000000000000e+02,
              "training_component": 60
            }
          - |-
            {
              "experience_component": 64,
              "performance_tier": "ELITE",
              "portfolio_component": 50,
              "risk_management_component": 2.180000000000000e+01,
              "satisfaction_component": 100,
              "total_score": 2.958000000000000e+02,
              "training_component": 60
            }
          - |-
            {
              "experience_component": 80,
              "performance_tier": "ELITE",
              "portfolio_component": 50,
              "risk_management_component": 2.180000000000000e+01,
              "satisfaction_component": 100,
              "total_score": 3.118000000000000e+02,
              "training_component": 60
            }
        description: This column, BROKER_PERFORMANCE_ANALYSIS, provides a comprehensive assessment of a broker's performance, breaking it down into various components such as experience, portfolio management, risk management, customer satisfaction, and training, with a total score that reflects their overall performance tier, which can be categorized as "ELITE" in this case.
        synonyms:
          - broker_performance_rating
          - broker_success_evaluation
          - broker_efficiency_assessment
          - broker_quality_measurement
          - broker_productivity_analysis
      - name: FINAL_RISK_LEVEL
        expr: FINAL_RISK_LEVEL
        data_type: VARCHAR(6)
        sample_values:
          - LOW
          - MEDIUM
          - HIGH
        description: 'The FINAL_RISK_LEVEL column represents the overall risk level of a particular entity or situation, categorized into three distinct levels: LOW, MEDIUM, and HIGH, indicating the severity of the risk and the potential impact on the organization.'
        synonyms:
          - risk_classification
          - risk_category
          - risk_rating
          - risk_status
          - risk_level
          - risk_assessment
          - risk_profile
          - risk_grade
    facts:
      - name: AGE
        expr: AGE
        data_type: NUMBER(38,0)
        sample_values:
          - '48'
          - '42'
          - '29'
        description: The age of the individual or entity associated with the risk intelligence data.
        synonyms:
          - years_old
          - years_of_age
          - age_in_years
          - years_lived
          - birth_age
      - name: POLICY_ANNUAL_PREMIUM
        expr: POLICY_ANNUAL_PREMIUM
        data_type: NUMBER(10,2)
        sample_values:
          - '1406.91'
          - '1197.22'
          - '1413.14'
        description: The annual premium amount paid by policyholders for their insurance coverage.
        synonyms:
          - annual_policy_cost
          - yearly_premium
          - policy_yearly_fee
          - annual_insurance_cost
          - yearly_insurance_premium
      - name: POLICY_LENGTH_MONTH
        expr: POLICY_LENGTH_MONTH
        data_type: NUMBER(38,0)
        sample_values:
          - '328'
          - '228'
          - '134'
        description: The length of time, in months, that a policy has been in effect.
        synonyms:
          - policy_duration
          - coverage_period
          - policy_term
          - coverage_length
          - policy_tenure
          - contract_length
          - insurance_term
          - coverage_duration
      - name: CLAIM_AMOUNT_FILLED
        expr: CLAIM_AMOUNT_FILLED
        data_type: NUMBER(6,0)
        sample_values:
          - '71610'
          - '5070'
          - '34650'
        description: The total amount claimed by the policyholder for a specific insurance claim.
        synonyms:
          - amount_paid
          - claim_payout
          - filled_claim_amount
          - paid_claim_value
          - settlement_amount
          - claim_settlement
          - insurance_payout
          - claim_reimbursement
          - paid_insurance_claim
      - name: HAS_CLAIM
        expr: HAS_CLAIM
        data_type: NUMBER(1,0)
        sample_values:
          - '1'
          - '0'
        description: Indicates whether a claim has been made against the policy, where 1 represents a claim has been made and 0 represents no claim has been made.
        synonyms:
          - CLAIM_EXISTS
          - CLAIM_PRESENT
          - CLAIM_STATUS
          - CLAIM_INDICATOR
          - CLAIM_FLAG
      - name: BROKER_SATISFACTION
        expr: BROKER_SATISFACTION
        data_type: NUMBER(38,0)
        sample_values:
          - '5'
          - '4'
        description: Broker Satisfaction is a measure of the level of satisfaction that brokers have with the services provided by the organization, with higher values indicating greater satisfaction.
        synonyms:
          - broker_happiness
          - broker_contentment
          - broker_approval_rating
          - broker_service_quality
          - broker_performance_rating
          - broker_evaluation_score
      - name: BROKER_EXPERIENCE
        expr: BROKER_EXPERIENCE
        data_type: NUMBER(38,0)
        sample_values:
          - '7'
          - '8'
          - '15'
        description: The number of years of experience the broker has in the industry.
        synonyms:
          - broker_tenure
          - years_of_service
          - broker_service_length
          - experience_level
          - broker_time_in_industry
          - industry_experience
      - name: BROKER_TRAINING
        expr: BROKER_TRAINING
        data_type: NUMBER(38,0)
        sample_values:
          - '39'
          - '43'
          - '52'
        description: The number of training sessions completed by brokers to enhance their knowledge and skills in identifying and mitigating potential risks.
        synonyms:
          - broker_certification
          - broker_development
          - broker_education
          - broker_professional_development
          - broker_skills_training
      - name: BROKER_CUSTOMER_COUNT
        expr: BROKER_CUSTOMER_COUNT
        data_type: NUMBER(18,0)
        sample_values:
          - '97'
          - '74'
          - '43'
        description: The total number of customers that a broker has.
        synonyms:
          - broker_portfolio_size
          - broker_client_count
          - broker_policyholder_count
          - broker_customer_base
          - broker_client_base
      - name: BROKER_AVG_CLAIM
        expr: BROKER_AVG_CLAIM
        data_type: NUMBER(21,3)
        sample_values:
          - '48581.907'
          - '56429.851'
          - '56461.069'
        description: The average claim amount paid out by brokers.
        synonyms:
          - average_claim_per_broker
          - broker_average_claim_amount
          - average_broker_claim
          - broker_claim_average
          - mean_claim_per_broker
      - name: CUSTOMER_RISK_SCORE
        expr: CUSTOMER_RISK_SCORE
        data_type: NUMBER(3,0)
        sample_values:
          - '25'
          - '45'
          - '30'
        description: A numerical score indicating the level of risk associated with a customer, ranging from 0 to 100, with higher scores representing a higher risk, used to evaluate and prioritize customer relationships based on their potential impact on the organization.
        synonyms:
          - customer_risk_rating
          - risk_level
          - policyholder_risk_assessment
          - client_risk_grade
          - risk_profile_score
```

### 1.4 Save and Publish
1. Click **"Save"** to save your semantic model
2. Click **"Publish"** to make the analyst available
3. Wait for the publishing process to complete

---

## Step 3: Initial Natural Language Questions

Test your Cortex Analyst with these questions to demonstrate its capabilities:

### Basic Questions (Easy)
```
How many policies in our portfolio have been flagged as HIGH risk level, and what percentage of our total policies does this represent?
```

```
Show me the total claim amounts for policies where fraud has been reported versus those without fraud reports. What's the average claim size difference?
```

### Intermediate Questions (Medium)
```
Which broker tier (GOLD, PLATINUM, BRONZE) has the best risk management performance based on their average customer risk scores and fraud reporting rates?
```

```
Analyze the relationship between customer regions and final risk levels. Which regions show the highest concentration of MEDIUM and HIGH risk customers, and how does this correlate with broker experience levels in those areas?
```

### Advanced Questions (Difficult)
```
Identify patterns between broker characteristics (experience, training, satisfaction scores) and their ability to manage risk effectively. Specifically, analyze how broker performance metrics correlate with their customers' risk scores, claim amounts, and fraud rates to determine the optimal broker profile for high-risk customer segments.
```

---

## Step 4: Filter Demonstration (Before & After)

### 4.1 Ask Question BEFORE Adding Filter

First, ask this question to establish baseline behavior:
```
Show me the broker performance analysis for high-risk policies. Which brokers are managing the most high-risk policies and what are their satisfaction scores?
```

**Observe**: Note how Cortex Analyst handles the concept of "high-risk policies" without a predefined filter.

### 4.2 Add Filter to Semantic Model

1. Go back to your Cortex Analyst
2. Click **"Edit Semantic Model"**
3. Add this filter section under the `RISK_INTELLIGENCE_DASHBOARD` table (after the `facts` section):

```yaml
    filters:
      - name: high_risk_policies
        expr: (CUSTOMER_RISK_SCORE >= 50 OR FRAUD_REPORTED_FILLED = TRUE)
        description: Filters for policies that are considered high-risk, either due to customer risk score of 50 or above, or having fraud reports filed
        synonyms:
          - risky_policies
          - elevated_risk_policies
          - high_risk_portfolio
          - problematic_policies
          - flagged_policies
```

4. Save and republish the semantic model

### 4.3 Ask Question AFTER Adding Filter

Now ask the same question again:
```
Show me the broker performance analysis for high-risk policies. Which brokers are managing the most high-risk policies and what are their satisfaction scores?
```

**Compare Results**: Notice how the filter provides more precise and consistent filtering logic.

---

## Step 5: Metric Demonstration (Before & After)

### 5.1 Ask Question BEFORE Adding Metric

Ask this question to see how Cortex Analyst handles complex calculations:
```
What is the claim to premium ratio by broker tier, and which tier has the most efficient claims management?
```

**Observe**: Note how Cortex Analyst attempts to calculate this ratio without a predefined metric.

### 5.2 Add Metric to Semantic Model

1. Edit your semantic model again
2. Add this metrics section under the `RISK_INTELLIGENCE_DASHBOARD` table (after the `filters` section):

```yaml
    metrics:
      - name: claim_to_premium_ratio
        expr: AVG(CLAIM_AMOUNT_FILLED / POLICY_ANNUAL_PREMIUM)
        data_type: NUMBER(10,4)
        description: Average ratio of claim amounts to annual premiums, indicating the claims efficiency across the portfolio
        synonyms:
          - loss_ratio
          - claim_efficiency_ratio
          - payout_ratio
          - claims_percentage
          - loss_percentage
          - claims_to_premium_percentage
```

3. Save and republish the semantic model

### 5.3 Ask Question AFTER Adding Metric

Ask the same question again:
```
What is the claim to premium ratio by broker tier, and which tier has the most efficient claims management?
```

**Compare Results**: Notice how the predefined metric provides more accurate and consistent calculations.

---

## Step 6: Advanced Demonstration Questions

Now that you have filters and metrics configured, try these advanced questions:

```
Using our high-risk policies filter, what is the average claim to premium ratio for each broker tier?
```

```
Show me the brokers with the highest satisfaction scores who are managing high-risk policies, and compare their claim efficiency ratios.
```

```
Which customer regions have the worst claim to premium ratios when we filter for high-risk policies only?
```

---

## Key Takeaways

### Benefits Demonstrated:
1. **No SQL Required**: Business users can ask complex questions in natural language
2. **Consistent Definitions**: Filters and metrics ensure everyone uses the same business logic
3. **Rich Context**: Synonyms and descriptions help Cortex Analyst understand intent
4. **Self-Service Analytics**: Reduces dependency on technical teams for ad-hoc analysis

### Business Value for Specialty Risk Group:
- **Risk Assessment**: Instantly identify and analyze high-risk policies
- **Broker Performance**: Evaluate broker effectiveness across multiple dimensions
- **Operational Efficiency**: Quick answers to complex risk management questions
- **Decision Support**: Data-driven insights for risk mitigation strategies

---

## Troubleshooting

### Common Issues:
1. **"Table not found"**: Ensure `02_RISK_ANALYTICS_GOVERNANCE.sql` has been executed
2. **Semantic model errors**: Validate YAML syntax and table references
3. **Slow responses**: Check warehouse size and table optimization
4. **Unexpected results**: Review synonyms and descriptions for clarity

### Best Practices:
- Use descriptive names for dimensions, facts, and metrics
- Include comprehensive synonyms for business terms
- Test questions incrementally, from simple to complex
- Document your semantic model for future maintenance

---

## Next Steps

After completing this demonstration:
1. Explore additional questions specific to your risk management needs
2. Consider adding more filters for different risk categories
3. Create additional metrics for key performance indicators
4. Share the Cortex Analyst with business stakeholders for self-service analytics 