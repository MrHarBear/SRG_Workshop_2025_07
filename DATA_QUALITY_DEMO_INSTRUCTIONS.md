# Data Quality Monitoring Demo - Error-Prone Datasets

## Overview

This demo has been enhanced to focus specifically on **CUSTOMERS_RAW** and **CLAIMS_RAW** data quality monitoring using Snowflake's Data Metric Functions (DMFs). All BROKERS_RAW related monitoring has been removed for a streamlined demonstration.

## What Was Modified

### 1. SQL Configuration (`01_DATA_QUALITY.sql`)
- **Removed**: All BROKERS_RAW table DMF configurations
- **Kept**: CUSTOMERS_RAW and CLAIMS_RAW monitoring only
- **Updated**: Views and monitoring queries to focus on two-entity model

### 2. Dashboard Updates (`01_data_quality_dashboard.py`)
- **Removed**: All BROKERS_RAW references from dashboard
- **Updated**: DMF status display to show only CUSTOMERS_RAW and CLAIMS_RAW
- **Simplified**: Relationship integrity to focus on customer-claims relationships only

### 3. Error-Prone Datasets Created

#### `datasets/customer_data_with_errors.csv` (100 rows)
**Intentional Data Quality Issues:**
- **NULL Policy Numbers**: 5 records (5%) - triggers `SNOWFLAKE.CORE.NULL_COUNT`
- **Duplicate Policy Numbers**: 20 records (15%) - triggers `SNOWFLAKE.CORE.DUPLICATE_COUNT`
- **Invalid Ages**: 11 records (11%) with ages < 18 or > 85 - triggers `RAW_DATA.INVALID_CUSTOMER_AGE_COUNT`
- **Invalid Broker IDs**: 9 records (9%) with wrong format - triggers `RAW_DATA.INVALID_BROKER_ID_COUNT`

#### `datasets/claim_data_with_errors.csv` (100 rows)
**Intentional Data Quality Issues:**
- **NULL Policy Numbers**: 17 records (17%) - triggers `SNOWFLAKE.CORE.NULL_COUNT`
- **Duplicate Policy Numbers**: 38 records (38%) - triggers `SNOWFLAKE.CORE.DUPLICATE_COUNT`

## DMF Configuration Summary

### CUSTOMERS_RAW Table (5 DMFs)
1. `RAW_DATA.INVALID_CUSTOMER_AGE_COUNT` - Custom DMF for age validation
2. `RAW_DATA.INVALID_BROKER_ID_COUNT` - Custom DMF for broker ID format validation
3. `SNOWFLAKE.CORE.NULL_COUNT` - System DMF for NULL policy numbers
4. `SNOWFLAKE.CORE.DUPLICATE_COUNT` - System DMF for duplicate policy numbers
5. `SNOWFLAKE.CORE.ROW_COUNT` - System DMF for volume monitoring

### CLAIMS_RAW Table (3 DMFs)
1. `SNOWFLAKE.CORE.NULL_COUNT` - System DMF for NULL policy numbers
2. `SNOWFLAKE.CORE.DUPLICATE_COUNT` - System DMF for duplicate policy numbers
3. `SNOWFLAKE.CORE.ROW_COUNT` - System DMF for volume monitoring

## How to Use the Error-Prone Datasets

### Step 1: Load Error-Prone Data
```sql
-- Load customer data with errors
COPY INTO RAW_DATA.CUSTOMERS_RAW 
FROM @DATA_STAGE/customer_data_with_errors.csv
FILE_FORMAT = (FORMAT_NAME = 'CSV_FORMAT')
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

-- Load claims data with errors  
COPY INTO RAW_DATA.CLAIMS_RAW
FROM @DATA_STAGE/claim_data_with_errors.csv
FILE_FORMAT = (FORMAT_NAME = 'CSV_FORMAT') 
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;
```

### Step 2: Monitor DMF Results
```sql
-- View real-time quality monitoring results
SELECT 
    measurement_time,
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
    AND table_name IN ('CUSTOMERS_RAW', 'CLAIMS_RAW')
ORDER BY measurement_time DESC;
```

### Step 3: View Quality Dashboard
Launch the Streamlit dashboard to see real-time quality monitoring:
```bash
streamlit run 01_data_quality_dashboard.py
```

## Expected Dashboard Results

When the error-prone datasets are loaded, you should see:

### Entity Quality Overview
- **CUSTOMERS_RAW**: Critical quality score due to multiple DMF violations
- **CLAIMS_RAW**: Critical quality score due to NULL and duplicate policy numbers

### Quality Metrics Breakdown
- Multiple "CRITICAL" and "WARNING" status indicators
- Clear visualization of data quality issues by entity

### Quality Issue Detection
- Specific counts of problematic records
- SYSTEM$DATA_METRIC_SCAN results showing exact records needing remediation

## DMF Triggers You'll See

1. **INVALID_CUSTOMER_AGE_COUNT**: 11 customers with invalid ages
2. **INVALID_BROKER_ID_COUNT**: 9 customers with malformed broker IDs
3. **NULL_COUNT (Customers)**: 5 customers with missing policy numbers
4. **DUPLICATE_COUNT (Customers)**: 20 customers with duplicate policy numbers
5. **NULL_COUNT (Claims)**: 17 claims with missing policy numbers
6. **DUPLICATE_COUNT (Claims)**: 38 claims with duplicate policy numbers

## Benefits Demonstrated

1. **Real-time Detection**: DMFs automatically detect quality issues within 5 minutes
2. **Business Rule Validation**: Custom DMFs enforce business-specific rules
3. **Record-level Identification**: SYSTEM$DATA_METRIC_SCAN pinpoints exact problematic records
4. **Automated Scoring**: Quality scores automatically calculated and categorized
5. **Visual Monitoring**: Streamlit dashboard provides intuitive quality oversight

## Clean Up After Demo

To reset for next demonstration:
```sql
-- Clear the tables
TRUNCATE TABLE RAW_DATA.CUSTOMERS_RAW;
TRUNCATE TABLE RAW_DATA.CLAIMS_RAW;

-- Reload clean data
COPY INTO RAW_DATA.CUSTOMERS_RAW FROM @DATA_STAGE/customer_data.csv ...
COPY INTO RAW_DATA.CLAIMS_RAW FROM @DATA_STAGE/claim_data.csv ...
```

This focused demonstration showcases Snowflake's comprehensive data quality monitoring capabilities in a clear, actionable way! 