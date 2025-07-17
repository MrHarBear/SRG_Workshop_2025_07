# Broker Distribution Update - Customer Data

## Overview
Modified the original `customer_data.csv` to create a more realistic, uneven broker distribution that better reflects real-world scenarios where brokers have varying client portfolios.

## Changes Made

### Before (Even Distribution)
- **All 20 brokers**: 60 customers each
- **Distribution**: Perfectly even (unrealistic)

### After (Realistic Distribution)

#### Top Performers (High Client Volume)
- **BRK001**: 200 customers ⭐ (Top performer - as requested)
- **BRK004**: 120 customers (High performer)
- **BRK008**: 95 customers (Strong performer)
- **BRK006**: 85 customers (Good performer)

#### Low Performers (Small Client Base)
- **BRK002**: 10 customers ⚠️ (Lowest - as requested)
- **BRK005**: 15 customers (Poor performer)
- **BRK007**: 25 customers (Struggling)
- **BRK011**: 26 customers (Below average)

#### Medium Range (Typical Performance)
- **BRK003**: 40 customers (As requested)
- **BRK009**: 75 customers
- **BRK012**: 72 customers  
- **BRK017**: 72 customers
- **BRK020**: 72 customers
- **BRK019**: 68 customers
- **BRK010**: 40 customers
- **BRK013**: 42 customers
- **BRK014**: 40 customers
- **BRK015**: 39 customers
- **BRK016**: 33 customers
- **BRK018**: 31 customers

## Benefits of New Distribution

1. **Realistic Variance**: Mimics real-world broker performance differences
2. **Demo Value**: Better showcases analytics that identify high/low performers
3. **Risk Analysis**: Enables concentration risk analysis (BRK001 has 16.7% of all customers)
4. **Business Intelligence**: More interesting patterns for dashboards and governance policies

## Impact on Analytics

### Risk Analytics
- **Concentration Risk**: BRK001 represents significant concentration with 200 customers
- **Performance Tiers**: Clear segmentation between high, medium, and low performers
- **Territory Analysis**: More realistic for demonstrating territory optimization

### Governance Scenarios  
- **Access Policies**: Can demonstrate broker-specific data access based on portfolio size
- **Monitoring**: Different thresholds for different broker tiers
- **Compliance**: Risk-based monitoring based on client concentration

## Total Verification
- **Total Customers**: 1,200 (unchanged)
- **Broker Count**: 20 (unchanged)
- **Data Integrity**: All relationships maintained
- **Statistical Range**: 10 to 200 customers per broker (realistic spread)

This updated distribution provides a much more realistic foundation for demonstrating Snowflake's analytics and governance capabilities in the insurance workshop! 