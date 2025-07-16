import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
import time
import json
from snowflake.snowpark.context import get_active_session

# Insurance Workshop Broker Performance Dashboard
# Purpose: Individual broker scorecards and territory analytics
# Scope: Broker performance analysis with customer portfolio insights

st.set_page_config(
    page_title="Insurance Workshop - Broker Performance",
    page_icon="🏆",
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
    .broker-card {{
        background: linear-gradient(135deg, #f8f9fa 0%, #e9ecef 100%);
        padding: 25px;
        border-radius: 15px;
        border-left: 6px solid {COLORS['main']};
        margin: 20px 0;
        box-shadow: 0 6px 12px rgba(0,0,0,0.1);
    }}
    .performance-card {{
        background: white;
        padding: 20px;
        border-radius: 10px;
        border: 2px solid {COLORS['star_blue']};
        margin: 15px 0;
        box-shadow: 0 3px 6px rgba(0,0,0,0.1);
    }}
    .tier-platinum {{ border-left-color: {COLORS['purple_moon']}; }}
    .tier-gold {{ border-left-color: {COLORS['valencia_orange']}; }}
    .tier-silver {{ border-left-color: {COLORS['medium_gray']}; }}
    .tier-bronze {{ border-left-color: {COLORS['first_light']}; }}
    .metric-excellent {{ color: {COLORS['star_blue']}; font-weight: bold; }}
    .metric-good {{ color: {COLORS['main']}; font-weight: bold; }}
    .metric-average {{ color: {COLORS['valencia_orange']}; font-weight: bold; }}
    .metric-poor {{ color: {COLORS['first_light']}; font-weight: bold; }}
</style>
""", unsafe_allow_html=True)

# Get active Snowflake session
session = get_active_session()

@st.cache_data(ttl=60)
def get_broker_performance_data():
    """Fetch comprehensive broker performance analytics"""
    
    try:
        results = {}
        
        # Broker performance matrix with Python UDF results
        results['broker_matrix'] = session.sql("""
            SELECT 
                BROKER_ID,
                BROKER_FIRST_NAME,
                BROKER_LAST_NAME,
                BROKER_OFFICE,
                BROKER_SATISFACTION,
                BROKER_EXPERIENCE,
                BROKER_TRAINING,
                BROKER_TIER,
                BROKER_ACTIVE,
                TOTAL_CUSTOMERS,
                CUSTOMERS_WITH_CLAIMS,
                AVG_CUSTOMER_PREMIUM,
                TOTAL_PREMIUM_VOLUME,
                AVG_CLAIM_AMOUNT,
                TOTAL_CLAIM_AMOUNT,
                TERRITORY_ADJUSTED_PREMIUM,
                AVG_CUSTOMER_RISK,
                FRAUD_CASES,
                CUSTOMER_REGION,
                CUSTOMERS_IN_REGION
            FROM INSURANCE_WORKSHOP_DB.ANALYTICS.BROKER_PERFORMANCE_MATRIX
            WHERE BROKER_ACTIVE = TRUE
            ORDER BY BROKER_TIER DESC, TOTAL_PREMIUM_VOLUME DESC
        """).to_pandas()
        
        # Individual broker intelligence with Python UDF analytics
        results['broker_intelligence'] = session.sql("""
            SELECT 
                BROKER_ID,
                BROKER_PERFORMANCE_ANALYSIS,
                COUNT(*) as CUSTOMER_COUNT,
                AVG(CUSTOMER_RISK_SCORE) as AVG_CUSTOMER_RISK_SCORE,
                COUNT(CASE WHEN FINAL_RISK_LEVEL = 'HIGH' THEN 1 END) as HIGH_RISK_CUSTOMERS,
                COUNT(CASE WHEN CUSTOMER_SEGMENT LIKE '%PREMIUM%' THEN 1 END) as PREMIUM_CUSTOMERS,
                AVG(POLICY_ANNUAL_PREMIUM) as AVG_PREMIUM,
                SUM(CLAIM_AMOUNT_FILLED) as TOTAL_CLAIMS
            FROM INSURANCE_WORKSHOP_DB.ANALYTICS.RISK_INTELLIGENCE_DASHBOARD
            WHERE BROKER_ID IS NOT NULL
            GROUP BY BROKER_ID, BROKER_PERFORMANCE_ANALYSIS
        """).to_pandas()
        
        # Territory performance summary
        results['territory_performance'] = session.sql("""
            SELECT 
                CUSTOMER_REGION,
                COUNT(DISTINCT BROKER_ID) as ACTIVE_BROKERS,
                COUNT(*) as TOTAL_CUSTOMERS,
                AVG(AVG_CUSTOMER_PREMIUM) as REGION_AVG_PREMIUM,
                SUM(TOTAL_PREMIUM_VOLUME) as REGION_PREMIUM_VOLUME,
                AVG(AVG_CUSTOMER_RISK) as REGION_RISK_SCORE,
                SUM(FRAUD_CASES) as REGION_FRAUD_CASES,
                AVG(BROKER_SATISFACTION) as REGION_SATISFACTION
            FROM INSURANCE_WORKSHOP_DB.ANALYTICS.BROKER_PERFORMANCE_MATRIX
            WHERE BROKER_ACTIVE = TRUE
            GROUP BY CUSTOMER_REGION
            ORDER BY REGION_PREMIUM_VOLUME DESC
        """).to_pandas()
        
        # Broker ranking and comparison
        results['broker_rankings'] = session.sql("""
            SELECT 
                BROKER_ID,
                BROKER_FIRST_NAME || ' ' || BROKER_LAST_NAME as BROKER_NAME,
                BROKER_TIER,
                TOTAL_PREMIUM_VOLUME,
                AVG_CUSTOMER_RISK,
                BROKER_SATISFACTION,
                CUSTOMERS_WITH_CLAIMS,
                TOTAL_CUSTOMERS,
                ROUND((CUSTOMERS_WITH_CLAIMS * 100.0) / TOTAL_CUSTOMERS, 1) as CLAIMS_RATIO,
                RANK() OVER (ORDER BY TOTAL_PREMIUM_VOLUME DESC) as VOLUME_RANK,
                RANK() OVER (ORDER BY AVG_CUSTOMER_RISK ASC) as RISK_RANK,
                RANK() OVER (ORDER BY BROKER_SATISFACTION DESC) as SATISFACTION_RANK
            FROM INSURANCE_WORKSHOP_DB.ANALYTICS.BROKER_PERFORMANCE_MATRIX
            WHERE BROKER_ACTIVE = TRUE AND TOTAL_CUSTOMERS > 0
        """).to_pandas()
        
        return results
        
    except Exception as e:
        st.error(f"Error fetching broker performance data: {str(e)}")
        return {}

@st.cache_data(ttl=120)
def get_detailed_broker_analytics(broker_id):
    """Get detailed analytics for a specific broker"""
    
    try:
        # Customer portfolio breakdown
        portfolio_data = session.sql(f"""
            SELECT 
                CUSTOMER_SEGMENT,
                COUNT(*) as CUSTOMER_COUNT,
                AVG(CUSTOMER_RISK_SCORE) as AVG_RISK_SCORE,
                AVG(POLICY_ANNUAL_PREMIUM) as AVG_PREMIUM,
                SUM(CLAIM_AMOUNT_FILLED) as TOTAL_CLAIMS,
                COUNT(CASE WHEN FINAL_RISK_LEVEL = 'HIGH' THEN 1 END) as HIGH_RISK_COUNT
            FROM INSURANCE_WORKSHOP_DB.ANALYTICS.RISK_INTELLIGENCE_DASHBOARD
            WHERE BROKER_ID = '{broker_id}'
            GROUP BY CUSTOMER_SEGMENT
            ORDER BY CUSTOMER_COUNT DESC
        """).to_pandas()
        
        # Risk trajectory insights
        risk_trends = session.sql(f"""
            SELECT 
                FINAL_RISK_LEVEL,
                COUNT(*) as CUSTOMER_COUNT,
                AVG(CUSTOMER_RISK_SCORE) as AVG_SCORE,
                RISK_TRAJECTORY_PREDICTION
            FROM INSURANCE_WORKSHOP_DB.ANALYTICS.RISK_INTELLIGENCE_DASHBOARD
            WHERE BROKER_ID = '{broker_id}'
            GROUP BY FINAL_RISK_LEVEL, RISK_TRAJECTORY_PREDICTION
        """).to_pandas()
        
        return {'portfolio': portfolio_data, 'risk_trends': risk_trends}
        
    except Exception as e:
        st.warning(f"Detailed analytics not available for broker {broker_id}: {str(e)}")
        return {'portfolio': pd.DataFrame(), 'risk_trends': pd.DataFrame()}

# Dashboard Header
st.markdown('<div class="main-header">Insurance Workshop - Broker Performance Analytics</div>', 
           unsafe_allow_html=True)

col1, col2, col3 = st.columns([2, 1, 1])
with col1:
    st.markdown("**Individual broker scorecards with comprehensive performance analysis**")
with col2:
    auto_refresh = st.checkbox("Auto-refresh (60s)", value=False)
with col3:
    if st.button("Refresh Data"):
        st.cache_data.clear()
        st.rerun()

# Auto-refresh logic
if auto_refresh:
    time.sleep(60)
    st.rerun()

# Fetch data
broker_data = get_broker_performance_data()

if not broker_data:
    st.error("Unable to load broker performance data. Please check your session context.")
    st.stop()

# Broker Selection and Overview
st.markdown('<div class="section-header">Broker Selection and Overview</div>', unsafe_allow_html=True)

if 'broker_matrix' in broker_data and not broker_data['broker_matrix'].empty:
    broker_matrix = broker_data['broker_matrix']
    
    col1, col2 = st.columns([1, 2])
    
    with col1:
        # Broker selection
        broker_options = broker_matrix[['BROKER_ID', 'BROKER_FIRST_NAME', 'BROKER_LAST_NAME', 'BROKER_TIER']].copy()
        broker_options['DISPLAY_NAME'] = (
            broker_options['BROKER_FIRST_NAME'] + ' ' + 
            broker_options['BROKER_LAST_NAME'] + ' (' + 
            broker_options['BROKER_TIER'] + ')'
        )
        
        selected_broker_display = st.selectbox(
            "Select Broker for Detailed Analysis",
            broker_options['DISPLAY_NAME'].tolist()
        )
        
        selected_broker_id = broker_options[
            broker_options['DISPLAY_NAME'] == selected_broker_display
        ]['BROKER_ID'].iloc[0]
    
    with col2:
        # Performance tier distribution
        tier_counts = broker_matrix['BROKER_TIER'].value_counts()
        
        fig_tiers = px.pie(
            values=tier_counts.values,
            names=tier_counts.index,
            color_discrete_map={
                'PLATINUM': COLORS['purple_moon'],
                'GOLD': COLORS['valencia_orange'],
                'SILVER': COLORS['medium_gray'],
                'BRONZE': COLORS['first_light']
            },
            title="Broker Performance Tier Distribution"
        )
        fig_tiers.update_layout(
            title_font_color=COLORS['mid_blue'],
            height=300
        )
        st.plotly_chart(fig_tiers, use_container_width=True)

# Individual Broker Scorecard
st.markdown('<div class="section-header">Individual Broker Scorecard</div>', unsafe_allow_html=True)

if selected_broker_id and 'broker_matrix' in broker_data:
    broker_info = broker_matrix[broker_matrix['BROKER_ID'] == selected_broker_id].iloc[0]
    
    # Get detailed analytics for selected broker
    detailed_analytics = get_detailed_broker_analytics(selected_broker_id)
    
    # Extract Python UDF performance analysis if available
    performance_analysis = {}
    if 'broker_intelligence' in broker_data:
        intel_data = broker_data['broker_intelligence']
        broker_intel = intel_data[intel_data['BROKER_ID'] == selected_broker_id]
        if not broker_intel.empty:
            try:
                performance_json = broker_intel['BROKER_PERFORMANCE_ANALYSIS'].iloc[0]
                if isinstance(performance_json, str):
                    performance_analysis = json.loads(performance_json)
                elif isinstance(performance_json, dict):
                    performance_analysis = performance_json
            except:
                performance_analysis = {}
    
    # Broker header card
    tier_class = f"tier-{broker_info['BROKER_TIER'].lower()}"
    st.markdown(f"""
    <div class="broker-card {tier_class}">
        <h2 style="color: {COLORS['midnight']}; margin: 0;">
            {broker_info['BROKER_FIRST_NAME']} {broker_info['BROKER_LAST_NAME']}
        </h2>
        <h3 style="color: {COLORS['mid_blue']}; margin: 5px 0;">
            {broker_info['BROKER_TIER']} Tier • {broker_info['BROKER_OFFICE']}
        </h3>
        <p style="color: {COLORS['medium_gray']}; margin: 0;">
            {broker_info['TOTAL_CUSTOMERS']} customers • ${broker_info['TOTAL_PREMIUM_VOLUME']:,.0f} premium volume
        </p>
    </div>
    """, unsafe_allow_html=True)
    
    # Performance metrics grid
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        satisfaction = broker_info['BROKER_SATISFACTION']
        sat_color = COLORS['star_blue'] if satisfaction >= 4.5 else COLORS['valencia_orange'] if satisfaction >= 4.0 else COLORS['first_light']
        st.markdown(f"""
        <div class="performance-card">
            <h4 style="color: {COLORS['mid_blue']}; margin: 0;">Customer Satisfaction</h4>
            <h2 style="color: {sat_color}; margin: 10px 0;">{satisfaction:.1f}/5.0</h2>
            <p style="color: {COLORS['medium_gray']}; margin: 0;">
                {broker_info['BROKER_EXPERIENCE']} years experience
            </p>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        claims_ratio = (broker_info['CUSTOMERS_WITH_CLAIMS'] / broker_info['TOTAL_CUSTOMERS']) * 100
        claims_color = COLORS['first_light'] if claims_ratio > 30 else COLORS['valencia_orange'] if claims_ratio > 20 else COLORS['star_blue']
        st.markdown(f"""
        <div class="performance-card">
            <h4 style="color: {COLORS['mid_blue']}; margin: 0;">Claims Ratio</h4>
            <h2 style="color: {claims_color}; margin: 10px 0;">{claims_ratio:.1f}%</h2>
            <p style="color: {COLORS['medium_gray']}; margin: 0;">
                {broker_info['CUSTOMERS_WITH_CLAIMS']} of {broker_info['TOTAL_CUSTOMERS']} customers
            </p>
        </div>
        """, unsafe_allow_html=True)
    
    with col3:
        risk_score = broker_info['AVG_CUSTOMER_RISK']
        risk_color = COLORS['star_blue'] if risk_score < 30 else COLORS['valencia_orange'] if risk_score < 50 else COLORS['first_light']
        st.markdown(f"""
        <div class="performance-card">
            <h4 style="color: {COLORS['mid_blue']}; margin: 0;">Portfolio Risk</h4>
            <h2 style="color: {risk_color}; margin: 10px 0;">{risk_score:.1f}</h2>
            <p style="color: {COLORS['medium_gray']}; margin: 0;">
                Average customer risk score
            </p>
        </div>
        """, unsafe_allow_html=True)
    
    with col4:
        training_hours = broker_info['BROKER_TRAINING']
        training_color = COLORS['star_blue'] if training_hours >= 40 else COLORS['valencia_orange'] if training_hours >= 30 else COLORS['first_light']
        st.markdown(f"""
        <div class="performance-card">
            <h4 style="color: {COLORS['mid_blue']}; margin: 0;">Training Hours</h4>
            <h2 style="color: {training_color}; margin: 10px 0;">{training_hours}</h2>
            <p style="color: {COLORS['medium_gray']}; margin: 0;">
                Completed this year
            </p>
        </div>
        """, unsafe_allow_html=True)
    
    # Python UDF Performance Analysis
    if performance_analysis:
        st.markdown("**Advanced Performance Analysis (Python UDF Results)**")
        
        col1, col2 = st.columns(2)
        
        with col1:
            total_score = performance_analysis.get('total_score', 0)
            performance_tier = performance_analysis.get('performance_tier', 'UNKNOWN')
            
            st.markdown(f"""
            <div class="performance-card">
                <h4 style="color: {COLORS['mid_blue']};">Overall Performance Score</h4>
                <h2 style="color: {COLORS['main']};">{total_score}/300</h2>
                <h3 style="color: {COLORS['purple_moon']};">{performance_tier}</h3>
            </div>
            """, unsafe_allow_html=True)
        
        with col2:
            components = {
                'Customer Satisfaction': performance_analysis.get('satisfaction_component', 0),
                'Experience': performance_analysis.get('experience_component', 0),
                'Training': performance_analysis.get('training_component', 0),
                'Portfolio Management': performance_analysis.get('portfolio_component', 0),
                'Risk Management': performance_analysis.get('risk_management_component', 0)
            }
            
            fig_components = px.bar(
                x=list(components.values()),
                y=list(components.keys()),
                orientation='h',
                color=list(components.values()),
                color_continuous_scale=[[0, COLORS['first_light']], [0.5, COLORS['valencia_orange']], [1, COLORS['star_blue']]],
                title="Performance Component Breakdown"
            )
            fig_components.update_layout(
                title_font_color=COLORS['mid_blue'],
                height=300,
                showlegend=False
            )
            st.plotly_chart(fig_components, use_container_width=True)

# Customer Portfolio Analysis
st.markdown('<div class="section-header">Customer Portfolio Analysis</div>', unsafe_allow_html=True)

if detailed_analytics['portfolio'] is not None and not detailed_analytics['portfolio'].empty:
    portfolio_data = detailed_analytics['portfolio']
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Portfolio composition
        fig_portfolio = px.pie(
            portfolio_data,
            values='CUSTOMER_COUNT',
            names='CUSTOMER_SEGMENT',
            color_discrete_sequence=[COLORS['main'], COLORS['star_blue'], COLORS['valencia_orange'], COLORS['purple_moon']],
            title=f"Customer Portfolio Composition"
        )
        fig_portfolio.update_layout(
            title_font_color=COLORS['mid_blue'],
            height=350
        )
        st.plotly_chart(fig_portfolio, use_container_width=True)
    
    with col2:
        # Risk vs Premium analysis
        fig_risk_premium = px.scatter(
            portfolio_data,
            x='AVG_PREMIUM',
            y='AVG_RISK_SCORE',
            size='CUSTOMER_COUNT',
            color='CUSTOMER_SEGMENT',
            color_discrete_sequence=[COLORS['main'], COLORS['star_blue'], COLORS['valencia_orange'], COLORS['purple_moon']],
            title="Risk vs Premium by Segment",
            labels={'AVG_PREMIUM': 'Average Premium ($)', 'AVG_RISK_SCORE': 'Average Risk Score'}
        )
        fig_risk_premium.update_layout(
            title_font_color=COLORS['mid_blue'],
            height=350
        )
        st.plotly_chart(fig_risk_premium, use_container_width=True)

# Territory Performance Comparison
st.markdown('<div class="section-header">Territory Performance Comparison</div>', unsafe_allow_html=True)

if 'territory_performance' in broker_data and not broker_data['territory_performance'].empty:
    territory_data = broker_data['territory_performance']
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Territory premium volume
        fig_territory_volume = px.bar(
            territory_data,
            x='CUSTOMER_REGION',
            y='REGION_PREMIUM_VOLUME',
            color='REGION_SATISFACTION',
            color_continuous_scale=[[0, COLORS['first_light']], [0.5, COLORS['valencia_orange']], [1, COLORS['star_blue']]],
            title="Premium Volume by Territory",
            labels={'REGION_PREMIUM_VOLUME': 'Total Premium Volume ($)', 'CUSTOMER_REGION': 'Territory'}
        )
        fig_territory_volume.update_layout(
            title_font_color=COLORS['mid_blue'],
            height=400
        )
        st.plotly_chart(fig_territory_volume, use_container_width=True)
    
    with col2:
        # Territory risk analysis
        fig_territory_risk = px.scatter(
            territory_data,
            x='ACTIVE_BROKERS',
            y='REGION_RISK_SCORE',
            size='TOTAL_CUSTOMERS',
            color='REGION_SATISFACTION',
            hover_data=['CUSTOMER_REGION'],
            color_continuous_scale=[[0, COLORS['first_light']], [0.5, COLORS['valencia_orange']], [1, COLORS['star_blue']]],
            title="Territory Risk vs Broker Count",
            labels={'ACTIVE_BROKERS': 'Number of Brokers', 'REGION_RISK_SCORE': 'Average Risk Score'}
        )
        fig_territory_risk.update_layout(
            title_font_color=COLORS['mid_blue'],
            height=400
        )
        st.plotly_chart(fig_territory_risk, use_container_width=True)

# Broker Rankings and Competition
st.markdown('<div class="section-header">Broker Rankings and Competition</div>', unsafe_allow_html=True)

if 'broker_rankings' in broker_data and not broker_data['broker_rankings'].empty:
    rankings_data = broker_data['broker_rankings']
    
    # Top performers summary
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown("**Top Volume Generators**")
        top_volume = rankings_data.nsmallest(5, 'VOLUME_RANK')[['BROKER_NAME', 'TOTAL_PREMIUM_VOLUME', 'BROKER_TIER']]
        for idx, broker in top_volume.iterrows():
            tier_color = {
                'PLATINUM': COLORS['purple_moon'],
                'GOLD': COLORS['valencia_orange'],
                'SILVER': COLORS['medium_gray'],
                'BRONZE': COLORS['first_light']
            }.get(broker['BROKER_TIER'], COLORS['main'])
            
            st.markdown(f"""
            <div style="padding: 10px; margin: 5px 0; border-left: 3px solid {tier_color}; background: #f8f9fa;">
                <strong style="color: {COLORS['mid_blue']};">{broker['BROKER_NAME']}</strong><br>
                <span style="color: {COLORS['medium_gray']};">${broker['TOTAL_PREMIUM_VOLUME']:,.0f}</span>
            </div>
            """, unsafe_allow_html=True)
    
    with col2:
        st.markdown("**Lowest Risk Portfolios**")
        top_risk = rankings_data.nsmallest(5, 'RISK_RANK')[['BROKER_NAME', 'AVG_CUSTOMER_RISK', 'BROKER_TIER']]
        for idx, broker in top_risk.iterrows():
            tier_color = {
                'PLATINUM': COLORS['purple_moon'],
                'GOLD': COLORS['valencia_orange'],
                'SILVER': COLORS['medium_gray'],
                'BRONZE': COLORS['first_light']
            }.get(broker['BROKER_TIER'], COLORS['main'])
            
            st.markdown(f"""
            <div style="padding: 10px; margin: 5px 0; border-left: 3px solid {tier_color}; background: #f8f9fa;">
                <strong style="color: {COLORS['mid_blue']};">{broker['BROKER_NAME']}</strong><br>
                <span style="color: {COLORS['medium_gray']};">Risk Score: {broker['AVG_CUSTOMER_RISK']:.1f}</span>
            </div>
            """, unsafe_allow_html=True)
    
    with col3:
        st.markdown("**Highest Satisfaction**")
        top_satisfaction = rankings_data.nsmallest(5, 'SATISFACTION_RANK')[['BROKER_NAME', 'BROKER_SATISFACTION', 'BROKER_TIER']]
        for idx, broker in top_satisfaction.iterrows():
            tier_color = {
                'PLATINUM': COLORS['purple_moon'],
                'GOLD': COLORS['valencia_orange'],
                'SILVER': COLORS['medium_gray'],
                'BRONZE': COLORS['first_light']
            }.get(broker['BROKER_TIER'], COLORS['main'])
            
            st.markdown(f"""
            <div style="padding: 10px; margin: 5px 0; border-left: 3px solid {tier_color}; background: #f8f9fa;">
                <strong style="color: {COLORS['mid_blue']};">{broker['BROKER_NAME']}</strong><br>
                <span style="color: {COLORS['medium_gray']};">Satisfaction: {broker['BROKER_SATISFACTION']:.1f}/5.0</span>
            </div>
            """, unsafe_allow_html=True)

# Performance Analytics Summary
st.markdown('<div class="section-header">Performance Analytics Summary</div>', unsafe_allow_html=True)

if broker_data:
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        if 'broker_matrix' in broker_data:
            total_brokers = len(broker_data['broker_matrix'])
            st.metric("Active Brokers", total_brokers)
    
    with col2:
        if 'broker_matrix' in broker_data:
            total_customers = broker_data['broker_matrix']['TOTAL_CUSTOMERS'].sum()
            st.metric("Total Customers", f"{total_customers:,}")
    
    with col3:
        if 'territory_performance' in broker_data:
            total_regions = len(broker_data['territory_performance'])
            st.metric("Active Territories", total_regions)
    
    with col4:
        if 'broker_matrix' in broker_data:
            total_volume = broker_data['broker_matrix']['TOTAL_PREMIUM_VOLUME'].sum()
            st.metric("Total Premium Volume", f"${total_volume:,.0f}")

# Footer
st.markdown("---")
st.markdown(f"""
<div style='text-align: center; color: {COLORS['medium_gray']}; padding: 20px;'>
    <p><strong>Insurance Workshop - Broker Performance Dashboard</strong></p>
    <p>Powered by Snowflake Dynamic Tables and Python UDFs for Advanced Analytics</p>
    <p>Individual Scorecards • Territory Analysis • Performance Ranking • Risk-Adjusted Metrics</p>
</div>
""", unsafe_allow_html=True) 