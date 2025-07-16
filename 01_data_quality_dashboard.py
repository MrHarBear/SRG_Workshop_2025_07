import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import time
from snowflake.snowpark.context import get_active_session

# Insurance Workshop Data Quality Dashboard
# Purpose: Real-time data quality monitoring with DMF results
# Scope: Three-entity quality tracking with Snowflake branding

st.set_page_config(
    page_title="Insurance Workshop - Data Quality Monitoring",
    page_icon="⚡",
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
    .metric-card {{
        background: linear-gradient(135deg, #f8f9fa 0%, #e9ecef 100%);
        padding: 20px;
        border-radius: 12px;
        border-left: 5px solid {COLORS['main']};
        margin: 15px 0;
        box-shadow: 0 4px 6px rgba(0,0,0,0.1);
    }}
    .quality-excellent {{ color: {COLORS['star_blue']}; font-weight: bold; }}
    .quality-good {{ color: {COLORS['main']}; font-weight: bold; }}
    .quality-warning {{ color: {COLORS['valencia_orange']}; font-weight: bold; }}
    .quality-critical {{ color: {COLORS['first_light']}; font-weight: bold; }}
    .dmf-note {{
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

@st.cache_data(ttl=30)
def get_quality_monitoring_data():
    """Fetch real-time quality monitoring data from DMF results"""
    
    try:
        results = {}
        
        # Entity quality scores
        results['entity_scores'] = session.sql("""
            SELECT 
                entity_name,
                total_metrics,
                excellent_count,
                good_count,
                warning_count,
                critical_count,
                overall_quality_score,
                last_measured
            FROM INSURANCE_WORKSHOP_DB.RAW_DATA.ENTITY_QUALITY_SCORES
            ORDER BY overall_quality_score DESC
        """).to_pandas()
        
        # Detailed quality monitoring summary
        results['quality_summary'] = session.sql("""
            SELECT 
                table_name,
                metric_name,
                metric_value,
                quality_status,
                measurement_time
            FROM INSURANCE_WORKSHOP_DB.RAW_DATA.QUALITY_MONITORING_SUMMARY
            ORDER BY measurement_time DESC
        """).to_pandas()
        
        # Relationship integrity metrics
        results['relationship_metrics'] = session.sql("""
            SELECT 
                relationship_type,
                total_customers,
                valid_relationships,
                missing_relationships,
                integrity_percentage,
                CASE 
                    WHEN integrity_percentage >= 98 THEN 'EXCELLENT'
                    WHEN integrity_percentage >= 95 THEN 'GOOD'
                    WHEN integrity_percentage >= 90 THEN 'NEEDS_ATTENTION'
                    ELSE 'CRITICAL'
                END as integrity_grade
            FROM INSURANCE_WORKSHOP_DB.RAW_DATA.RELATIONSHIP_QUALITY_METRICS
        """).to_pandas()
        
        # DMF configuration status
        results['dmf_status'] = session.sql("""
            SELECT 
                REPLACE(ref_entity_name, 'INSURANCE_WORKSHOP_DB.RAW_DATA.', '') as table_name,
                metric_name,
                schedule,
                schedule_status
            FROM TABLE(INFORMATION_SCHEMA.DATA_METRIC_FUNCTION_REFERENCES(
                ref_entity_name => 'INSURANCE_WORKSHOP_DB.RAW_DATA.CUSTOMERS_RAW',
                ref_entity_domain => 'TABLE'
            ))
            UNION ALL
            SELECT 
                REPLACE(ref_entity_name, 'INSURANCE_WORKSHOP_DB.RAW_DATA.', '') as table_name,
                metric_name,
                schedule,
                schedule_status
            FROM TABLE(INFORMATION_SCHEMA.DATA_METRIC_FUNCTION_REFERENCES(
                ref_entity_name => 'INSURANCE_WORKSHOP_DB.RAW_DATA.CLAIMS_RAW',
                ref_entity_domain => 'TABLE'
            ))
            UNION ALL
            SELECT 
                REPLACE(ref_entity_name, 'INSURANCE_WORKSHOP_DB.RAW_DATA.', '') as table_name,
                metric_name,
                schedule,
                schedule_status
            FROM TABLE(INFORMATION_SCHEMA.DATA_METRIC_FUNCTION_REFERENCES(
                ref_entity_name => 'INSURANCE_WORKSHOP_DB.RAW_DATA.BROKERS_RAW',
                ref_entity_domain => 'TABLE'
            ))
        """).to_pandas()
        
        return results
        
    except Exception as e:
        st.error(f"Error fetching quality monitoring data: {str(e)}")
        return {}

@st.cache_data(ttl=60)
def get_historical_trends():
    """Get historical quality trends for trend analysis"""
    
    try:
        # Simulate historical trend data
        historical_data = session.sql("""
            SELECT 
                table_name,
                DATE_TRUNC('hour', measurement_time) as hour_bucket,
                metric_name,
                AVG(metric_value) as avg_metric_value,
                COUNT(*) as measurement_count
            FROM SNOWFLAKE.LOCAL.DATA_QUALITY_MONITORING_RESULTS
            WHERE table_database = 'INSURANCE_WORKSHOP_DB'
                AND table_schema = 'RAW_DATA'
                AND measurement_time >= DATEADD('hour', -24, CURRENT_TIMESTAMP())
            GROUP BY table_name, hour_bucket, metric_name
            ORDER BY hour_bucket DESC
        """).to_pandas()
        
        return historical_data
        
    except Exception as e:
        st.warning(f"Historical trend data not available: {str(e)}")
        return pd.DataFrame()

# Dashboard Header
st.markdown('<div class="main-header">Insurance Workshop - Data Quality Monitoring</div>', 
           unsafe_allow_html=True)

col1, col2, col3 = st.columns([2, 1, 1])
with col1:
    st.markdown("**Real-time data quality monitoring powered by Snowflake Data Metric Functions**")
with col2:
    auto_refresh = st.checkbox("Auto-refresh (30s)", value=False)
with col3:
    if st.button("Refresh Data"):
        st.cache_data.clear()
        st.rerun()

# Auto-refresh logic
if auto_refresh:
    time.sleep(30)
    st.rerun()

# Fetch data
quality_data = get_quality_monitoring_data()
historical_data = get_historical_trends()

if not quality_data:
    st.error("Unable to load quality monitoring data. Please check your session context.")
    st.stop()

# Entity Quality Overview
st.markdown('<div class="section-header">Entity Quality Overview</div>', unsafe_allow_html=True)

if 'entity_scores' in quality_data and not quality_data['entity_scores'].empty:
    entity_scores = quality_data['entity_scores']
    
    col1, col2, col3 = st.columns(3)
    
    for idx, entity in entity_scores.iterrows():
        with [col1, col2, col3][idx % 3]:
            score = entity['overall_quality_score']
            if score >= 90:
                score_color = COLORS['star_blue']
                grade = 'EXCELLENT'
            elif score >= 75:
                score_color = COLORS['main']
                grade = 'GOOD'
            elif score >= 60:
                score_color = COLORS['valencia_orange']
                grade = 'NEEDS ATTENTION'
            else:
                score_color = COLORS['first_light']
                grade = 'CRITICAL'
            
            st.markdown(f"""
            <div class="metric-card">
                <h3 style="color: {COLORS['mid_blue']}; margin: 0;">{entity['entity_name'].replace('_RAW', '')}</h3>
                <h1 style="color: {score_color}; margin: 10px 0;">{score}%</h1>
                <p style="color: {COLORS['medium_gray']}; margin: 0;">{grade} - {entity['total_metrics']} metrics</p>
                <small style="color: {COLORS['medium_gray']};">Last measured: {entity['last_measured']}</small>
            </div>
            """, unsafe_allow_html=True)

# Quality Metrics Breakdown
st.markdown('<div class="section-header">Quality Metrics Breakdown</div>', unsafe_allow_html=True)

if 'quality_summary' in quality_data and not quality_data['quality_summary'].empty:
    quality_summary = quality_data['quality_summary']
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("**Quality Status Distribution**")
        
        status_counts = quality_summary['quality_status'].value_counts()
        
        # Create pie chart with Snowflake colors
        fig_status = px.pie(
            values=status_counts.values,
            names=status_counts.index,
            color_discrete_map={
                'EXCELLENT': COLORS['star_blue'],
                'GOOD': COLORS['main'],
                'WARNING': COLORS['valencia_orange'],
                'CRITICAL': COLORS['first_light']
            },
            title="Current Quality Status Distribution"
        )
        fig_status.update_layout(
            title_font_color=COLORS['mid_blue'],
            height=350
        )
        st.plotly_chart(fig_status, use_container_width=True)
    
    with col2:
        st.markdown("**Metrics by Entity**")
        
        # Group by table and count metrics
        entity_metrics = quality_summary.groupby(['table_name', 'quality_status']).size().reset_index(name='count')
        
        fig_entity = px.bar(
            entity_metrics,
            x='table_name',
            y='count',
            color='quality_status',
            color_discrete_map={
                'EXCELLENT': COLORS['star_blue'],
                'GOOD': COLORS['main'],
                'WARNING': COLORS['valencia_orange'],
                'CRITICAL': COLORS['first_light']
            },
            title="Quality Metrics by Entity",
            labels={'table_name': 'Entity', 'count': 'Metric Count'}
        )
        fig_entity.update_layout(
            title_font_color=COLORS['mid_blue'],
            height=350
        )
        st.plotly_chart(fig_entity, use_container_width=True)

# Detailed Quality Metrics
st.markdown('<div class="section-header">Detailed Quality Metrics</div>', unsafe_allow_html=True)

if 'quality_summary' in quality_data and not quality_data['quality_summary'].empty:
    
    # Filter controls
    col1, col2, col3 = st.columns(3)
    with col1:
        selected_entity = st.selectbox(
            "Select Entity",
            ['All'] + list(quality_summary['table_name'].unique())
        )
    with col2:
        selected_status = st.selectbox(
            "Filter by Status",
            ['All', 'CRITICAL', 'WARNING', 'GOOD', 'EXCELLENT']
        )
    with col3:
        show_recent = st.checkbox("Show only recent (last hour)", value=True)
    
    # Apply filters
    filtered_data = quality_summary.copy()
    
    if selected_entity != 'All':
        filtered_data = filtered_data[filtered_data['table_name'] == selected_entity]
    
    if selected_status != 'All':
        filtered_data = filtered_data[filtered_data['quality_status'] == selected_status]
    
    if show_recent:
        one_hour_ago = datetime.now() - timedelta(hours=1)
        filtered_data = filtered_data[pd.to_datetime(filtered_data['measurement_time']) >= one_hour_ago]
    
    # Display filtered results
    if not filtered_data.empty:
        # Style the dataframe
        def style_quality_status(val):
            if val == 'EXCELLENT':
                return f'background-color: {COLORS["star_blue"]}; color: white'
            elif val == 'GOOD':
                return f'background-color: {COLORS["main"]}; color: white'
            elif val == 'WARNING':
                return f'background-color: {COLORS["valencia_orange"]}; color: white'
            elif val == 'CRITICAL':
                return f'background-color: {COLORS["first_light"]}; color: white'
            return ''
        
        styled_df = filtered_data.style.applymap(style_quality_status, subset=['quality_status'])
        st.dataframe(styled_df, use_container_width=True)
    else:
        st.info("No data matches the selected filters.")

# Relationship Integrity Analysis
st.markdown('<div class="section-header">Relationship Integrity Analysis</div>', unsafe_allow_html=True)

if 'relationship_metrics' in quality_data and not quality_data['relationship_metrics'].empty:
    rel_metrics = quality_data['relationship_metrics']
    
    col1, col2 = st.columns(2)
    
    for idx, relationship in rel_metrics.iterrows():
        with [col1, col2][idx % 2]:
            rel_type = relationship['relationship_type'].replace('_', ' ').title()
            integrity_pct = relationship['integrity_percentage']
            grade = relationship['integrity_grade']
            
            if grade == 'EXCELLENT':
                grade_color = COLORS['star_blue']
            elif grade == 'GOOD':
                grade_color = COLORS['main']
            elif grade == 'NEEDS_ATTENTION':
                grade_color = COLORS['valencia_orange']
            else:
                grade_color = COLORS['first_light']
            
            st.markdown(f"""
            <div class="metric-card">
                <h3 style="color: {COLORS['mid_blue']}; margin: 0;">{rel_type}</h3>
                <h1 style="color: {grade_color}; margin: 10px 0;">{integrity_pct}%</h1>
                <p style="color: {COLORS['medium_gray']}; margin: 0;">{grade}</p>
                <small style="color: {COLORS['medium_gray']};">
                    {relationship['valid_relationships']:,} valid / {relationship['total_customers']:,} total
                </small>
            </div>
            """, unsafe_allow_html=True)

# DMF Configuration Status
st.markdown('<div class="section-header">Data Metric Function Status</div>', unsafe_allow_html=True)

if 'dmf_status' in quality_data and not quality_data['dmf_status'].empty:
    dmf_status = quality_data['dmf_status']
    
    st.markdown(f"""
    <div class="dmf-note">
    <strong>DMF Configuration:</strong> This section shows the status of all Data Metric Functions 
    configured for automated quality monitoring across the three-entity model.
    </div>
    """, unsafe_allow_html=True)
    
    # Group by table for better display
    for table_name in dmf_status['table_name'].unique():
        table_dmfs = dmf_status[dmf_status['table_name'] == table_name]
        
        st.markdown(f"**{table_name.upper()}**")
        
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Total DMFs", len(table_dmfs))
        with col2:
            active_count = len(table_dmfs[table_dmfs['schedule_status'] == 'STARTED'])
            st.metric("Active DMFs", active_count)
        with col3:
            st.metric("Schedule", table_dmfs['schedule'].iloc[0] if not table_dmfs.empty else "N/A")
        
        # Display DMF details
        dmf_display = table_dmfs[['metric_name', 'schedule_status']].copy()
        dmf_display['metric_name'] = dmf_display['metric_name'].str.replace('INSURANCE_WORKSHOP_DB.RAW_DATA.', '')
        st.dataframe(dmf_display, use_container_width=True, hide_index=True)

# Historical Trends (if available)
if not historical_data.empty:
    st.markdown('<div class="section-header">Quality Trends</div>', unsafe_allow_html=True)
    
    # Simple trend visualization
    fig_trend = px.line(
        historical_data,
        x='hour_bucket',
        y='avg_metric_value',
        color='table_name',
        facet_col='metric_name',
        title="Quality Metrics Trend (Last 24 Hours)",
        color_discrete_sequence=[COLORS['main'], COLORS['valencia_orange'], COLORS['purple_moon']]
    )
    fig_trend.update_layout(
        title_font_color=COLORS['mid_blue'],
        height=400
    )
    st.plotly_chart(fig_trend, use_container_width=True)

# System Health Summary
st.markdown('<div class="section-header">System Health Summary</div>', unsafe_allow_html=True)

if quality_data:
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        total_entities = len(quality_data.get('entity_scores', []))
        st.metric("Monitored Entities", total_entities)
    
    with col2:
        if 'dmf_status' in quality_data:
            total_dmfs = len(quality_data['dmf_status'])
            st.metric("Active DMFs", total_dmfs)
    
    with col3:
        if 'quality_summary' in quality_data:
            critical_count = len(quality_data['quality_summary'][quality_data['quality_summary']['quality_status'] == 'CRITICAL'])
            st.metric("Critical Issues", critical_count, delta=None if critical_count == 0 else f"-{critical_count}")
    
    with col4:
        current_time = datetime.now().strftime("%H:%M:%S")
        st.metric("Last Update", current_time)

# Footer
st.markdown("---")
st.markdown(f"""
<div style='text-align: center; color: {COLORS['medium_gray']}; padding: 20px;'>
    <p><strong>Insurance Workshop - Data Quality Monitoring Dashboard</strong></p>
    <p>Powered by Snowflake Data Metric Functions with Real-time Quality Scoring</p>
    <p>Three-Entity Model: Customers • Claims • Brokers with Cross-Entity Validation</p>
</div>
""", unsafe_allow_html=True) 