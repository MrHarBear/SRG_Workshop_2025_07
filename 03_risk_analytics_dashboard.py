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
# Purpose: Multi-dimensional risk analysis with predictive insights
# Scope: Customer risk profiling, geographic analysis, broker correlation

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
    .prediction-improving {{ color: {COLORS['star_blue']}; font-weight: bold; }}
    .prediction-stable {{ color: {COLORS['valencia_orange']}; font-weight: bold; }}
    .prediction-deteriorating {{ color: {COLORS['first_light']}; font-weight: bold; }}
    .analytics-note {{
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
        
        # Risk intelligence dashboard with Python UDF results
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
                BROKER_AVG_RISK,
                BROKER_PORTFOLIO_SIZE,
                FINAL_RISK_LEVEL,
                BROKER_PERFORMANCE_ANALYSIS,
                RISK_TRAJECTORY_PREDICTION
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
        
        # Broker risk correlation analysis
        results['broker_risk_correlation'] = session.sql("""
            SELECT 
                BROKER_ID,
                BROKER_TIER,
                BROKER_PORTFOLIO_SIZE,
                BROKER_AVG_RISK,
                COUNT(*) as MANAGED_CUSTOMERS,
                AVG(CUSTOMER_RISK_SCORE) as PORTFOLIO_RISK_SCORE,
                COUNT(CASE WHEN FINAL_RISK_LEVEL = 'HIGH' THEN 1 END) as HIGH_RISK_CUSTOMERS,
                AVG(POLICY_ANNUAL_PREMIUM) as AVG_PORTFOLIO_PREMIUM,
                SUM(CLAIM_AMOUNT_FILLED) as TOTAL_PORTFOLIO_CLAIMS,
                CUSTOMER_REGION
            FROM INSURANCE_WORKSHOP_DB.ANALYTICS.RISK_INTELLIGENCE_DASHBOARD
            WHERE BROKER_ID IS NOT NULL
            GROUP BY BROKER_ID, BROKER_TIER, BROKER_PORTFOLIO_SIZE, BROKER_AVG_RISK, CUSTOMER_REGION
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
        
        # Risk trajectory analysis (Python UDF results)
        results['risk_trajectories'] = session.sql("""
            SELECT 
                CUSTOMER_SEGMENT,
                FINAL_RISK_LEVEL,
                RISK_TRAJECTORY_PREDICTION,
                COUNT(*) as CUSTOMER_COUNT,
                AVG(CUSTOMER_RISK_SCORE) as AVG_CURRENT_RISK
            FROM INSURANCE_WORKSHOP_DB.ANALYTICS.RISK_INTELLIGENCE_DASHBOARD
            WHERE RISK_TRAJECTORY_PREDICTION IS NOT NULL
            GROUP BY CUSTOMER_SEGMENT, FINAL_RISK_LEVEL, RISK_TRAJECTORY_PREDICTION
        """).to_pandas()
        
        return results
        
    except Exception as e:
        st.error(f"Error fetching risk analytics data: {str(e)}")
        return {}

@st.cache_data(ttl=120)
def get_predictive_analytics():
    """Extract and analyze Python UDF predictive results"""
    
    try:
        # Parse risk trajectory predictions
        trajectory_data = session.sql("""
            SELECT 
                CUSTOMER_SEGMENT,
                CUSTOMER_REGION,
                FINAL_RISK_LEVEL,
                RISK_TRAJECTORY_PREDICTION,
                COUNT(*) as PREDICTION_COUNT
            FROM INSURANCE_WORKSHOP_DB.ANALYTICS.RISK_INTELLIGENCE_DASHBOARD
            WHERE RISK_TRAJECTORY_PREDICTION IS NOT NULL
            GROUP BY CUSTOMER_SEGMENT, CUSTOMER_REGION, FINAL_RISK_LEVEL, RISK_TRAJECTORY_PREDICTION
        """).to_pandas()
        
        return trajectory_data
        
    except Exception as e:
        st.warning(f"Predictive analytics not available: {str(e)}")
        return pd.DataFrame()

def parse_trajectory_prediction(prediction_json):
    """Parse Python UDF trajectory prediction results"""
    try:
        if isinstance(prediction_json, str):
            prediction = json.loads(prediction_json)
        elif isinstance(prediction_json, dict):
            prediction = prediction_json
        else:
            return None
        
        return {
            'predicted_risk': prediction.get('predicted_risk_score', 0),
            'risk_change': prediction.get('risk_change', 0),
            'confidence': prediction.get('confidence_level', 0),
            'primary_factor': prediction.get('primary_factor', 'UNKNOWN'),
            'trajectory': prediction.get('trajectory', 'STABLE')
        }
    except:
        return None

# Dashboard Header
st.markdown('<div class="main-header">Insurance Workshop - Risk Analytics Intelligence</div>', 
           unsafe_allow_html=True)

col1, col2, col3 = st.columns([2, 1, 1])
with col1:
    st.markdown("**Multi-dimensional risk analysis with predictive intelligence powered by Python UDFs**")
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
predictive_data = get_predictive_analytics()

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
        high_risk_pct = (high_risk_count / total_customers) * 100
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
            <p style="color: {COLORS['medium_gray']}; margin: 0;">Portfolio average</p>
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
            color_discrete_sequence=[COLORS['main'], COLORS['star_blue'], COLORS['valencia_orange'], COLORS['purple_moon'], COLORS['first_light']],
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

if 'broker_risk_correlation' in risk_data and not risk_data['broker_risk_correlation'].empty:
    broker_correlation = risk_data['broker_risk_correlation']
    
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
            x='MANAGED_CUSTOMERS',
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
            labels={'MANAGED_CUSTOMERS': 'Portfolio Size', 'PORTFOLIO_RISK_SCORE': 'Portfolio Risk Score'}
        )
        fig_size_risk.update_layout(
            title_font_color=COLORS['mid_blue'],
            height=400
        )
        st.plotly_chart(fig_size_risk, use_container_width=True)

# Geographic Risk Visualization
st.markdown('<div class="section-header">Geographic Risk Visualization</div>', unsafe_allow_html=True)

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

# Predictive Analytics Display
st.markdown('<div class="section-header">Predictive Analytics Intelligence</div>', unsafe_allow_html=True)

st.markdown(f"""
<div class="analytics-note">
<strong>Python UDF Predictions:</strong> This section displays risk trajectory predictions 
generated by Python UDFs analyzing historical patterns and demographic factors.
</div>
""", unsafe_allow_html=True)

if 'risk_trajectories' in risk_data and not risk_data['risk_trajectories'].empty:
    trajectory_data = risk_data['risk_trajectories']
    
    # Parse trajectory predictions for sample display
    if 'risk_dashboard' in risk_data:
        sample_predictions = risk_data['risk_dashboard'].head(10).copy()
        parsed_predictions = []
        
        for idx, row in sample_predictions.iterrows():
            if pd.notna(row['RISK_TRAJECTORY_PREDICTION']):
                parsed = parse_trajectory_prediction(row['RISK_TRAJECTORY_PREDICTION'])
                if parsed:
                    parsed['customer_segment'] = row['CUSTOMER_SEGMENT']
                    parsed['current_risk'] = row['CUSTOMER_RISK_SCORE']
                    parsed_predictions.append(parsed)
        
        if parsed_predictions:
            col1, col2 = st.columns(2)
            
            with col1:
                # Trajectory distribution
                trajectory_counts = {}
                for pred in parsed_predictions:
                    traj = pred['trajectory']
                    trajectory_counts[traj] = trajectory_counts.get(traj, 0) + 1
                
                if trajectory_counts:
                    fig_trajectory = px.pie(
                        values=list(trajectory_counts.values()),
                        names=list(trajectory_counts.keys()),
                        color_discrete_map={
                            'IMPROVING': COLORS['star_blue'],
                            'STABLE': COLORS['valencia_orange'],
                            'DETERIORATING': COLORS['first_light']
                        },
                        title="Risk Trajectory Predictions"
                    )
                    fig_trajectory.update_layout(
                        title_font_color=COLORS['mid_blue'],
                        height=350
                    )
                    st.plotly_chart(fig_trajectory, use_container_width=True)
            
            with col2:
                # Prediction confidence analysis
                if parsed_predictions:
                    confidence_data = pd.DataFrame(parsed_predictions)
                    
                    fig_confidence = px.scatter(
                        confidence_data,
                        x='current_risk',
                        y='predicted_risk',
                        size='confidence',
                        color='trajectory',
                        color_discrete_map={
                            'IMPROVING': COLORS['star_blue'],
                            'STABLE': COLORS['valencia_orange'],
                            'DETERIORATING': COLORS['first_light']
                        },
                        title="Current vs Predicted Risk Scores",
                        labels={'current_risk': 'Current Risk Score', 'predicted_risk': 'Predicted Risk Score'}
                    )
                    fig_confidence.update_layout(
                        title_font_color=COLORS['mid_blue'],
                        height=350
                    )
                    st.plotly_chart(fig_confidence, use_container_width=True)
            
            # Sample prediction details
            st.markdown("**Sample Risk Trajectory Predictions**")
            prediction_display = []
            for i, pred in enumerate(parsed_predictions[:5]):
                trajectory_class = f"prediction-{pred['trajectory'].lower()}"
                prediction_display.append({
                    'Customer Segment': pred['customer_segment'],
                    'Current Risk': f"{pred['current_risk']:.1f}",
                    'Predicted Risk': f"{pred['predicted_risk']:.1f}",
                    'Risk Change': f"{pred['risk_change']:+.2f}",
                    'Confidence': f"{pred['confidence']:.2f}",
                    'Trajectory': pred['trajectory'],
                    'Primary Factor': pred['primary_factor']
                })
            
            if prediction_display:
                prediction_df = pd.DataFrame(prediction_display)
                st.dataframe(prediction_df, use_container_width=True)

# Risk Factor Analysis
st.markdown('<div class="section-header">Risk Factor Analysis</div>', unsafe_allow_html=True)

if 'risk_dashboard' in risk_data and not risk_data['risk_dashboard'].empty:
    dashboard_data = risk_data['risk_dashboard']
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Age vs Risk correlation
        fig_age_risk = px.scatter(
            dashboard_data,
            x='AGE',
            y='CUSTOMER_RISK_SCORE',
            color='FINAL_RISK_LEVEL',
            size='POLICY_ANNUAL_PREMIUM',
            color_discrete_map={
                'HIGH': COLORS['first_light'],
                'MEDIUM': COLORS['valencia_orange'],
                'LOW': COLORS['star_blue']
            },
            title="Age vs Risk Score Correlation",
            labels={'AGE': 'Customer Age', 'CUSTOMER_RISK_SCORE': 'Risk Score'}
        )
        fig_age_risk.update_layout(
            title_font_color=COLORS['mid_blue'],
            height=400
        )
        st.plotly_chart(fig_age_risk, use_container_width=True)
    
    with col2:
        # Premium vs Claims analysis
        fig_premium_claims = px.scatter(
            dashboard_data,
            x='POLICY_ANNUAL_PREMIUM',
            y='CLAIM_AMOUNT_FILLED',
            color='CUSTOMER_RISK_SCORE',
            size='AGE',
            color_continuous_scale=[[0, COLORS['star_blue']], [0.5, COLORS['valencia_orange']], [1, COLORS['first_light']]],
            title="Premium vs Claims Relationship",
            labels={'POLICY_ANNUAL_PREMIUM': 'Annual Premium ($)', 'CLAIM_AMOUNT_FILLED': 'Claim Amount ($)'}
        )
        fig_premium_claims.update_layout(
            title_font_color=COLORS['mid_blue'],
            height=400
        )
        st.plotly_chart(fig_premium_claims, use_container_width=True)

# Risk Analytics Summary
st.markdown('<div class="section-header">Risk Analytics Summary</div>', unsafe_allow_html=True)

if risk_data:
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        if 'geographic_risk' in risk_data:
            regions_analyzed = len(risk_data['geographic_risk'])
            st.metric("Regions Analyzed", regions_analyzed)
    
    with col2:
        if 'broker_risk_correlation' in risk_data:
            brokers_analyzed = len(risk_data['broker_risk_correlation'])
            st.metric("Brokers Analyzed", brokers_analyzed)
    
    with col3:
        if 'risk_profiles' in risk_data:
            segments_analyzed = risk_data['risk_profiles']['CUSTOMER_SEGMENT'].nunique()
            st.metric("Customer Segments", segments_analyzed)
    
    with col4:
        if parsed_predictions:
            predictions_generated = len(parsed_predictions)
            st.metric("Predictions Generated", predictions_generated)

# Footer
st.markdown("---")
st.markdown(f"""
<div style='text-align: center; color: {COLORS['medium_gray']}; padding: 20px;'>
    <p><strong>Insurance Workshop - Risk Analytics Intelligence Dashboard</strong></p>
    <p>Powered by Snowflake Dynamic Tables and Python UDFs for Predictive Analytics</p>
    <p>Multi-dimensional Analysis • Geographic Intelligence • Broker Correlation • Predictive Insights</p>
</div>
""", unsafe_allow_html=True) 