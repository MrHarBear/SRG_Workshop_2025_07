import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import plotly.figure_factory as ff
from datetime import datetime
import time
import json
import numpy as np
from snowflake.snowpark.context import get_active_session

# Insurance Workshop Risk Analytics Dashboard
# Purpose: Multi-dimensional risk analysis with UDF-driven insights
# Scope: Customer risk profiling, broker performance analysis, geographic intelligence

st.set_page_config(
    page_title="Insurance Workshop - Risk Analytics",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Snowflake Brand Colors from ProjectPlan.md
COLORS = {
    'main': '#29B5E8',           # Main Snowflake Blue
    'midnight': '#000000',       # Titles
    'mid_blue': '#11567F',       # Sections
    'medium_gray': '#5B5B5B',    # Body Copy
    'star_blue': '#75CDD7',      # Star Blue
    'valencia_orange': '#FF9F36', # Valencia Orange
    'first_light': '#D45B90',    # First Light
    'purple_moon': '#7254A3'     # Purple Moon
}

# Professional styling with Snowflake branding
st.markdown(f"""
<style>
    .main-header {{
        color: {COLORS['midnight']};
        font-size: 32px;
        font-weight: bold;
        text-align: center;
        padding: 20px 0;
        border-bottom: 3px solid {COLORS['main']};
        margin-bottom: 30px;
    }}
    .section-header {{
        color: {COLORS['mid_blue']};
        font-size: 22px;
        font-weight: bold;
        margin: 25px 0 15px 0;
        padding: 12px 0;
        border-left: 4px solid {COLORS['main']};
        padding-left: 20px;
    }}
    .risk-card {{
        background: linear-gradient(135deg, #f8f9fa 0%, #e9ecef 100%);
        padding: 25px;
        border-radius: 15px;
        margin: 20px 0;
        box-shadow: 0 6px 12px rgba(0,0,0,0.1);
    }}
    .risk-high {{ border-left: 6px solid {COLORS['first_light']}; }}
    .risk-medium {{ border-left: 6px solid {COLORS['valencia_orange']}; }}
    .risk-low {{ border-left: 6px solid {COLORS['star_blue']}; }}
    .metric-card {{
        background: white;
        padding: 20px;
        border-radius: 12px;
        border: 2px solid {COLORS['main']};
        margin: 15px 0;
        box-shadow: 0 4px 8px rgba(0,0,0,0.1);
    }}
    .udf-analysis {{ 
        background-color: #f0f8ff;
        padding: 15px;
        border-radius: 8px;
        border-left: 4px solid {COLORS['purple_moon']};
        margin: 15px 0;
        color: {COLORS['medium_gray']};
    }}
</style>
""", unsafe_allow_html=True)

# Get active Snowflake session
session = get_active_session()

@st.cache_data(ttl=60)
def get_risk_analytics_data():
    """Fetch comprehensive risk analytics from Dynamic Tables and UDFs"""
    
    try:
        results = {}
        
        # Risk intelligence dashboard with corrected column names
        results['risk_dashboard'] = session.sql("""
            SELECT 
                POLICY_NUMBER,
                AGE,
                CUSTOMER_SEGMENT,
                CUSTOMER_RISK_SCORE,
                POLICY_ANNUAL_PREMIUM,
                CLAIM_AMOUNT_FILLED,
                CUSTOMER_REGION,
                BROKER_ID,
                BROKER_TIER,
                BROKER_CUSTOMER_COUNT,
                BROKER_AVG_CLAIM,
                FINAL_RISK_LEVEL,
                BROKER_PERFORMANCE_ANALYSIS,
                INSURED_SEX,
                INSURED_OCCUPATION,
                HAS_CLAIM,
                FRAUD_REPORTED_FILLED
            FROM INSURANCE_WORKSHOP_DB.ANALYTICS.RISK_INTELLIGENCE_DASHBOARD
            WHERE BROKER_ID IS NOT NULL
        """).to_pandas()
        
        # Customer risk profile aggregations
        results['risk_profiles'] = session.sql("""
            SELECT 
                CUSTOMER_SEGMENT,
                CUSTOMER_REGION,
                COUNT(*) as CUSTOMER_COUNT,
                AVG(CUSTOMER_RISK_SCORE) as AVG_RISK_SCORE,
                AVG(POLICY_ANNUAL_PREMIUM) as AVG_PREMIUM,
                COUNT(CASE WHEN FINAL_RISK_LEVEL = 'HIGH' THEN 1 END) as HIGH_RISK_COUNT,
                COUNT(CASE WHEN FINAL_RISK_LEVEL = 'MEDIUM' THEN 1 END) as MEDIUM_RISK_COUNT,
                COUNT(CASE WHEN FINAL_RISK_LEVEL = 'LOW' THEN 1 END) as LOW_RISK_COUNT,
                SUM(CLAIM_AMOUNT_FILLED) as TOTAL_CLAIMS
            FROM INSURANCE_WORKSHOP_DB.ANALYTICS.RISK_INTELLIGENCE_DASHBOARD
            GROUP BY CUSTOMER_SEGMENT, CUSTOMER_REGION
        """).to_pandas()
        
        # Broker correlation analysis with correct column names
        results['broker_correlation'] = session.sql("""
            SELECT 
                BROKER_ID,
                BROKER_TIER,
                BROKER_CUSTOMER_COUNT,
                BROKER_AVG_CLAIM,
                COUNT(*) as MANAGED_CUSTOMERS,
                AVG(CUSTOMER_RISK_SCORE) as PORTFOLIO_RISK_SCORE,
                COUNT(CASE WHEN FINAL_RISK_LEVEL = 'HIGH' THEN 1 END) as HIGH_RISK_CUSTOMERS,
                AVG(POLICY_ANNUAL_PREMIUM) as AVG_PORTFOLIO_PREMIUM,
                SUM(CLAIM_AMOUNT_FILLED) as TOTAL_PORTFOLIO_CLAIMS,
                MAX(CUSTOMER_REGION) as PRIMARY_REGION,
                ANY_VALUE(BROKER_PERFORMANCE_ANALYSIS) as BROKER_PERFORMANCE_ANALYSIS
            FROM INSURANCE_WORKSHOP_DB.ANALYTICS.RISK_INTELLIGENCE_DASHBOARD
            WHERE BROKER_ID IS NOT NULL
            GROUP BY BROKER_ID, BROKER_TIER, BROKER_CUSTOMER_COUNT, BROKER_AVG_CLAIM
        """).to_pandas()
        
        # Geographic risk distribution
        results['geographic_risk'] = session.sql("""
            SELECT 
                CUSTOMER_REGION,
                COUNT(*) as TOTAL_CUSTOMERS,
                AVG(CUSTOMER_RISK_SCORE) as REGION_AVG_RISK,
                COUNT(CASE WHEN FINAL_RISK_LEVEL = 'HIGH' THEN 1 END) as HIGH_RISK_COUNT,
                COUNT(DISTINCT BROKER_ID) as ACTIVE_BROKERS,
                AVG(POLICY_ANNUAL_PREMIUM) as REGION_AVG_PREMIUM,
                SUM(CLAIM_AMOUNT_FILLED) as REGION_TOTAL_CLAIMS,
                ROUND(AVG(CUSTOMER_RISK_SCORE), 1) as RISK_SCORE_ROUNDED
            FROM INSURANCE_WORKSHOP_DB.ANALYTICS.RISK_INTELLIGENCE_DASHBOARD
            GROUP BY CUSTOMER_REGION
            ORDER BY REGION_AVG_RISK DESC
        """).to_pandas()
        
        # Broker performance matrix aggregated data
        results['broker_matrix'] = session.sql("""
            SELECT 
                BROKER_ID,
                BROKER_FIRST_NAME,
                BROKER_LAST_NAME,
                BROKER_TIER,
                TOTAL_CUSTOMERS,
                AVG_CUSTOMER_PREMIUM,
                AVG_CUSTOMER_RISK,
                TOTAL_PREMIUM_VOLUME,
                BROKER_PERFORMANCE_ANALYSIS,
                BROKER_ACTIVE
            FROM INSURANCE_WORKSHOP_DB.ANALYTICS.BROKER_PERFORMANCE_MATRIX
            WHERE BROKER_ACTIVE = TRUE
            ORDER BY TOTAL_PREMIUM_VOLUME DESC
        """).to_pandas()
        
        return results
        
    except Exception as e:
        st.error(f"Error fetching risk analytics data: {str(e)}")
        return {}

def parse_broker_performance_analysis(performance_json):
    """Parse Python UDF broker performance analysis results"""
    try:
        if isinstance(performance_json, str):
            performance = json.loads(performance_json)
        elif isinstance(performance_json, dict):
            performance = performance_json
        else:
            return None
        
        return {
            'total_score': performance.get('total_score', 0),
            'performance_tier': performance.get('performance_tier', 'UNKNOWN'),
            'satisfaction_component': performance.get('satisfaction_component', 0),
            'experience_component': performance.get('experience_component', 0),
            'training_component': performance.get('training_component', 0),
            'portfolio_component': performance.get('portfolio_component', 0),
            'risk_management_component': performance.get('risk_management_component', 0)
        }
    except:
        return None

# Dashboard Header
st.markdown('<div class="main-header">Insurance Workshop - Risk Analytics Intelligence</div>', 
           unsafe_allow_html=True)

col1, col2, col3 = st.columns([2, 1, 1])
with col1:
    st.markdown("**UDF-driven risk analysis with Dynamic Tables and broker performance intelligence**")
with col2:
    auto_refresh = st.checkbox("Auto-refresh (60s)", value=False)
with col3:
    if st.button("Refresh Analytics"):
        st.cache_data.clear()
        st.rerun()

# Auto-refresh logic
if auto_refresh:
    time.sleep(60)
    st.rerun()

# Fetch data
risk_data = get_risk_analytics_data()

if not risk_data:
    st.error("Unable to load risk analytics data. Please check your session context.")
    st.stop()

# Risk Overview Dashboard
st.markdown('<div class="section-header">Risk Overview Dashboard</div>', unsafe_allow_html=True)

if 'risk_dashboard' in risk_data and not risk_data['risk_dashboard'].empty:
    dashboard_data = risk_data['risk_dashboard']
    
    # Overall risk metrics
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        total_customers = len(dashboard_data)
        st.markdown(f"""
        <div class="metric-card">
            <h4 style="color: {COLORS['mid_blue']}; margin: 0;">Total Customers</h4>
            <h2 style="color: {COLORS['midnight']}; margin: 10px 0;">{total_customers:,}</h2>
            <p style="color: {COLORS['medium_gray']}; margin: 0;">Under risk analysis</p>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        high_risk_count = len(dashboard_data[dashboard_data['FINAL_RISK_LEVEL'] == 'HIGH'])
        high_risk_pct = (high_risk_count / total_customers) * 100 if total_customers > 0 else 0
        st.markdown(f"""
        <div class="metric-card">
            <h4 style="color: {COLORS['mid_blue']}; margin: 0;">High Risk Customers</h4>
            <h2 style="color: {COLORS['first_light']}; margin: 10px 0;">{high_risk_count:,}</h2>
            <p style="color: {COLORS['medium_gray']}; margin: 0;">{high_risk_pct:.1f}% of portfolio</p>
        </div>
        """, unsafe_allow_html=True)
    
    with col3:
        avg_risk_score = dashboard_data['CUSTOMER_RISK_SCORE'].mean()
        risk_color = COLORS['first_light'] if avg_risk_score > 60 else COLORS['valencia_orange'] if avg_risk_score > 40 else COLORS['star_blue']
        st.markdown(f"""
        <div class="metric-card">
            <h4 style="color: {COLORS['mid_blue']}; margin: 0;">Average Risk Score</h4>
            <h2 style="color: {risk_color}; margin: 10px 0;">{avg_risk_score:.1f}</h2>
            <p style="color: {COLORS['medium_gray']}; margin: 0;">SQL UDF calculation</p>
        </div>
        """, unsafe_allow_html=True)
    
    with col4:
        total_exposure = dashboard_data['CLAIM_AMOUNT_FILLED'].sum()
        st.markdown(f"""
        <div class="metric-card">
            <h4 style="color: {COLORS['mid_blue']}; margin: 0;">Total Exposure</h4>
            <h2 style="color: {COLORS['purple_moon']}; margin: 10px 0;">${total_exposure:,.0f}</h2>
            <p style="color: {COLORS['medium_gray']}; margin: 0;">Claims exposure</p>
        </div>
        """, unsafe_allow_html=True)

# UDF Results Analysis
st.markdown('<div class="section-header">UDF-Driven Analytics</div>', unsafe_allow_html=True)

st.markdown(f"""
<div class="udf-analysis">
<strong>Mixed UDF Architecture:</strong> This dashboard showcases both SQL UDFs (CALCULATE_CUSTOMER_RISK_SCORE, DETERMINE_BROKER_TIER) 
and Python UDFs (ANALYZE_BROKER_PERFORMANCE) working together in Dynamic Tables for real-time analytics.
</div>
""", unsafe_allow_html=True)

if 'risk_dashboard' in risk_data and not risk_data['risk_dashboard'].empty:
    col1, col2 = st.columns(2)
    
    with col1:
        # Customer Risk Score Distribution (SQL UDF)
        risk_levels = dashboard_data['FINAL_RISK_LEVEL'].value_counts()
        fig_risk_dist = px.pie(
            values=risk_levels.values,
            names=risk_levels.index,
            color_discrete_map={
                'HIGH': COLORS['first_light'],
                'MEDIUM': COLORS['valencia_orange'],
                'LOW': COLORS['star_blue']
            },
            title="Customer Risk Distribution (SQL UDF)"
        )
        fig_risk_dist.update_layout(
            title_font_color=COLORS['mid_blue'],
            height=400
        )
        st.plotly_chart(fig_risk_dist, use_container_width=True)
    
    with col2:
        # Broker Tier Distribution (SQL UDF)
        broker_tiers = dashboard_data['BROKER_TIER'].value_counts()
        fig_broker_tiers = px.bar(
            x=broker_tiers.index,
            y=broker_tiers.values,
            color=broker_tiers.index,
            color_discrete_map={
                'PLATINUM': COLORS['purple_moon'],
                'GOLD': COLORS['valencia_orange'],
                'SILVER': COLORS['medium_gray'],
                'BRONZE': COLORS['first_light']
            },
            title="Broker Tier Distribution (SQL UDF)",
            labels={'x': 'Broker Tier', 'y': 'Number of Customers'}
        )
        fig_broker_tiers.update_layout(
            title_font_color=COLORS['mid_blue'],
            height=400,
            showlegend=False
        )
        st.plotly_chart(fig_broker_tiers, use_container_width=True)

# Python UDF Broker Performance Analysis
st.markdown('<div class="section-header">Python UDF Broker Performance Analysis</div>', unsafe_allow_html=True)

if 'broker_matrix' in risk_data and not risk_data['broker_matrix'].empty:
    broker_matrix = risk_data['broker_matrix']
    
    # Parse Python UDF results
    performance_analyses = []
    for idx, row in broker_matrix.iterrows():
        if pd.notna(row['BROKER_PERFORMANCE_ANALYSIS']):
            parsed = parse_broker_performance_analysis(row['BROKER_PERFORMANCE_ANALYSIS'])
            if parsed:
                parsed['broker_id'] = row['BROKER_ID']
                parsed['broker_name'] = f"{row['BROKER_FIRST_NAME']} {row['BROKER_LAST_NAME']}"
                parsed['total_customers'] = row['TOTAL_CUSTOMERS']
                performance_analyses.append(parsed)
    
    if performance_analyses:
        performance_df = pd.DataFrame(performance_analyses)
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Performance Score vs Customer Count
            fig_performance = px.scatter(
                performance_df,
                x='total_customers',
                y='total_score',
                size='portfolio_component',
                color='performance_tier',
                hover_data=['broker_name'],
                color_discrete_map={
                    'ELITE': COLORS['purple_moon'],
                    'SUPERIOR': COLORS['star_blue'],
                    'PROFICIENT': COLORS['valencia_orange'],
                    'DEVELOPING': COLORS['first_light']
                },
                title="Broker Performance Analysis (Python UDF)",
                labels={'total_customers': 'Portfolio Size', 'total_score': 'Total Performance Score'}
            )
            fig_performance.update_layout(
                title_font_color=COLORS['mid_blue'],
                height=400
            )
            st.plotly_chart(fig_performance, use_container_width=True)
        
        with col2:
            # Performance Component Breakdown
            component_cols = ['satisfaction_component', 'experience_component', 'training_component', 
                            'portfolio_component', 'risk_management_component']
            avg_components = performance_df[component_cols].mean()
            
            fig_components = px.bar(
                x=avg_components.values,
                y=[col.replace('_component', '').title() for col in avg_components.index],
                orientation='h',
                color=avg_components.values,
                color_continuous_scale=[[0, COLORS['first_light']], [0.5, COLORS['valencia_orange']], [1, COLORS['star_blue']]],
                title="Average Performance Components (Python UDF)"
            )
            fig_components.update_layout(
                title_font_color=COLORS['mid_blue'],
                height=400,
                showlegend=False
            )
            st.plotly_chart(fig_components, use_container_width=True)
        
        # Top performing brokers table
        st.markdown("**Top Performing Brokers (Python UDF Analysis)**")
        top_brokers = performance_df.nlargest(10, 'total_score')[
            ['broker_name', 'performance_tier', 'total_score', 'total_customers']
        ].round(1)
        st.dataframe(top_brokers, use_container_width=True)

# Multi-dimensional Risk Analysis
st.markdown('<div class="section-header">Multi-dimensional Risk Analysis</div>', unsafe_allow_html=True)

if 'risk_profiles' in risk_data and not risk_data['risk_profiles'].empty:
    risk_profiles = risk_data['risk_profiles']
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Customer segment risk distribution
        segment_risk = risk_profiles.groupby('CUSTOMER_SEGMENT').agg({
            'CUSTOMER_COUNT': 'sum',
            'AVG_RISK_SCORE': 'mean',
            'HIGH_RISK_COUNT': 'sum'
        }).reset_index()
        
        fig_segment_risk = px.scatter(
            segment_risk,
            x='AVG_RISK_SCORE',
            y='HIGH_RISK_COUNT',
            size='CUSTOMER_COUNT',
            color='CUSTOMER_SEGMENT',
            color_discrete_sequence=[COLORS['main'], COLORS['star_blue'], COLORS['valencia_orange']],
            title="Risk Profile by Customer Segment",
            labels={'AVG_RISK_SCORE': 'Average Risk Score', 'HIGH_RISK_COUNT': 'High Risk Customer Count'}
        )
        fig_segment_risk.update_layout(
            title_font_color=COLORS['mid_blue'],
            height=400
        )
        st.plotly_chart(fig_segment_risk, use_container_width=True)
    
    with col2:
        # Regional risk heatmap
        region_pivot = risk_profiles.pivot_table(
            index='CUSTOMER_REGION',
            columns='CUSTOMER_SEGMENT',
            values='AVG_RISK_SCORE',
            fill_value=0
        )
        
        if not region_pivot.empty:
            fig_heatmap = px.imshow(
                region_pivot.values,
                x=region_pivot.columns,
                y=region_pivot.index,
                color_continuous_scale=[[0, COLORS['star_blue']], [0.5, COLORS['valencia_orange']], [1, COLORS['first_light']]],
                title="Risk Score Heatmap: Region vs Segment",
                labels={'color': 'Risk Score'}
            )
            fig_heatmap.update_layout(
                title_font_color=COLORS['mid_blue'],
                height=400
            )
            st.plotly_chart(fig_heatmap, use_container_width=True)

# Broker-Customer Risk Correlation
st.markdown('<div class="section-header">Broker-Customer Risk Correlation</div>', unsafe_allow_html=True)

if 'broker_correlation' in risk_data and not risk_data['broker_correlation'].empty:
    broker_correlation = risk_data['broker_correlation']
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Broker tier vs portfolio risk
        fig_broker_risk = px.box(
            broker_correlation,
            x='BROKER_TIER',
            y='PORTFOLIO_RISK_SCORE',
            color='BROKER_TIER',
            color_discrete_map={
                'PLATINUM': COLORS['purple_moon'],
                'GOLD': COLORS['valencia_orange'],
                'SILVER': COLORS['medium_gray'],
                'BRONZE': COLORS['first_light']
            },
            title="Portfolio Risk Distribution by Broker Tier"
        )
        fig_broker_risk.update_layout(
            title_font_color=COLORS['mid_blue'],
            height=400,
            showlegend=False
        )
        st.plotly_chart(fig_broker_risk, use_container_width=True)
    
    with col2:
        # Portfolio size vs risk correlation
        fig_size_risk = px.scatter(
            broker_correlation,
            x='BROKER_CUSTOMER_COUNT',
            y='PORTFOLIO_RISK_SCORE',
            size='TOTAL_PORTFOLIO_CLAIMS',
            color='BROKER_TIER',
            color_discrete_map={
                'PLATINUM': COLORS['purple_moon'],
                'GOLD': COLORS['valencia_orange'],
                'SILVER': COLORS['medium_gray'],
                'BRONZE': COLORS['first_light']
            },
            title="Portfolio Size vs Risk Correlation",
            labels={'BROKER_CUSTOMER_COUNT': 'Portfolio Size', 'PORTFOLIO_RISK_SCORE': 'Portfolio Risk Score'}
        )
        fig_size_risk.update_layout(
            title_font_color=COLORS['mid_blue'],
            height=400
        )
        st.plotly_chart(fig_size_risk, use_container_width=True)

# Geographic Risk Analysis
st.markdown('<div class="section-header">Geographic Risk Distribution</div>', unsafe_allow_html=True)

if 'geographic_risk' in risk_data and not risk_data['geographic_risk'].empty:
    geographic_data = risk_data['geographic_risk']
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Regional risk comparison
        fig_regional_risk = px.bar(
            geographic_data,
            x='CUSTOMER_REGION',
            y='REGION_AVG_RISK',
            color='HIGH_RISK_COUNT',
            color_continuous_scale=[[0, COLORS['star_blue']], [0.5, COLORS['valencia_orange']], [1, COLORS['first_light']]],
            title="Average Risk Score by Region",
            labels={'REGION_AVG_RISK': 'Average Risk Score', 'CUSTOMER_REGION': 'Region'}
        )
        fig_regional_risk.update_layout(
            title_font_color=COLORS['mid_blue'],
            height=400
        )
        st.plotly_chart(fig_regional_risk, use_container_width=True)
    
    with col2:
        # Risk vs customer density
        fig_density_risk = px.scatter(
            geographic_data,
            x='TOTAL_CUSTOMERS',
            y='REGION_AVG_RISK',
            size='REGION_TOTAL_CLAIMS',
            color='ACTIVE_BROKERS',
            hover_data=['CUSTOMER_REGION'],
            color_continuous_scale=[[0, COLORS['first_light']], [0.5, COLORS['valencia_orange']], [1, COLORS['star_blue']]],
            title="Customer Density vs Regional Risk",
            labels={'TOTAL_CUSTOMERS': 'Customer Count', 'REGION_AVG_RISK': 'Average Risk Score'}
        )
        fig_density_risk.update_layout(
            title_font_color=COLORS['mid_blue'],
            height=400
        )
        st.plotly_chart(fig_density_risk, use_container_width=True)

# Risk Factor Analysis
st.markdown('<div class="section-header">Risk Factor Analysis</div>', unsafe_allow_html=True)

if 'risk_dashboard' in risk_data and not risk_data['risk_dashboard'].empty:
    dashboard_data = risk_data['risk_dashboard']
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Age vs Risk correlation - using histogram for WebGL compatibility
        fig_age_risk = px.histogram(
            dashboard_data,
            x='AGE',
            color='FINAL_RISK_LEVEL',
            color_discrete_map={
                'HIGH': COLORS['first_light'],
                'MEDIUM': COLORS['valencia_orange'],
                'LOW': COLORS['star_blue']
            },
            title="Age Distribution by Risk Level (SQL UDF)",
            labels={'AGE': 'Customer Age', 'count': 'Number of Customers'},
            opacity=0.7
        )
        fig_age_risk.update_layout(
            title_font_color=COLORS['mid_blue'],
            height=400,
            barmode='overlay'
        )
        st.plotly_chart(fig_age_risk, use_container_width=True)
    
    with col2:
        # Premium vs Risk analysis using box plot for WebGL compatibility
        fig_premium_risk = px.box(
            dashboard_data,
            x='FINAL_RISK_LEVEL',
            y='POLICY_ANNUAL_PREMIUM',
            color='FINAL_RISK_LEVEL',
            color_discrete_map={
                'HIGH': COLORS['first_light'],
                'MEDIUM': COLORS['valencia_orange'],
                'LOW': COLORS['star_blue']
            },
            title="Premium Distribution by Risk Level",
            labels={'FINAL_RISK_LEVEL': 'Risk Level', 'POLICY_ANNUAL_PREMIUM': 'Annual Premium ($)'}
        )
        fig_premium_risk.update_layout(
            title_font_color=COLORS['mid_blue'],
            height=400,
            showlegend=False
        )
        st.plotly_chart(fig_premium_risk, use_container_width=True)

# Analytics Summary
st.markdown('<div class="section-header">Analytics Summary</div>', unsafe_allow_html=True)

if risk_data:
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        if 'geographic_risk' in risk_data:
            regions_analyzed = len(risk_data['geographic_risk'])
            st.metric("Regions Analyzed", regions_analyzed)
    
    with col2:
        if 'broker_correlation' in risk_data:
            brokers_analyzed = len(risk_data['broker_correlation'])
            st.metric("Brokers Analyzed", brokers_analyzed)
    
    with col3:
        if 'risk_profiles' in risk_data:
            segments_analyzed = risk_data['risk_profiles']['CUSTOMER_SEGMENT'].nunique()
            st.metric("Customer Segments", segments_analyzed)
    
    with col4:
        if performance_analyses:
            udf_calculations = len(performance_analyses)
            st.metric("Python UDF Results", udf_calculations)

# Footer
st.markdown("---")
st.markdown(f"""
<div style='text-align: center; color: {COLORS['medium_gray']}; padding: 20px;'>
    <p><strong>Insurance Workshop - Risk Analytics Intelligence Dashboard</strong></p>
    <p>Powered by Snowflake Dynamic Tables, SQL UDFs, and Python UDFs</p>
    <p>UDF-Driven Analytics • Geographic Intelligence • Broker Performance • Real-time Insights</p>
</div>
""", unsafe_allow_html=True) 