import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import plotly.figure_factory as ff
from datetime import datetime, timedelta
import time
import networkx as nx
from snowflake.snowpark.context import get_active_session

# Insurance Workshop Governance Dashboard
# Purpose: Policy enforcement validation and compliance monitoring
# Scope: Data governance, masking policies, tag compliance, lineage tracking

st.set_page_config(
    page_title="Insurance Workshop - Governance Dashboard",
    page_icon="🔒",
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
    .governance-card {{
        background: linear-gradient(135deg, #f8f9fa 0%, #e9ecef 100%);
        padding: 25px;
        border-radius: 15px;
        margin: 20px 0;
        box-shadow: 0 6px 12px rgba(0,0,0,0.1);
    }}
    .policy-compliant {{ border-left: 6px solid {COLORS['star_blue']}; }}
    .policy-partial {{ border-left: 6px solid {COLORS['valencia_orange']}; }}
    .policy-violation {{ border-left: 6px solid {COLORS['first_light']}; }}
    .compliance-card {{
        background: white;
        padding: 20px;
        border-radius: 12px;
        border: 2px solid {COLORS['main']};
        margin: 15px 0;
        box-shadow: 0 4px 8px rgba(0,0,0,0.1);
    }}
    .status-compliant {{ color: {COLORS['star_blue']}; font-weight: bold; }}
    .status-warning {{ color: {COLORS['valencia_orange']}; font-weight: bold; }}
    .status-violation {{ color: {COLORS['first_light']}; font-weight: bold; }}
    .governance-note {{
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
def get_governance_data():
    """Fetch comprehensive governance and compliance data"""
    
    try:
        results = {}
        
        # Policy enforcement monitoring from the governance schema
        results['policy_enforcement'] = session.sql("""
            SELECT 
                POLICY_NAME,
                ENTITY_TYPE,
                ENTITY_NAME,
                ENFORCEMENT_STATUS,
                LAST_EVALUATED,
                COMPLIANCE_SCORE,
                VIOLATION_COUNT,
                POLICY_CATEGORY
            FROM INSURANCE_WORKSHOP_DB.GOVERNANCE.POLICY_ENFORCEMENT_LOG
            WHERE LAST_EVALUATED >= DATEADD('day', -7, CURRENT_TIMESTAMP())
            ORDER BY LAST_EVALUATED DESC
        """).to_pandas()
        
        # Dynamic masking policy status
        results['masking_policies'] = session.sql("""
            SELECT 
                POLICY_NAME,
                POLICY_KIND,
                POLICY_BODY,
                POLICY_SIGNATURE,
                CREATED,
                LAST_ALTERED,
                COMMENT
            FROM INFORMATION_SCHEMA.MASKING_POLICIES
            WHERE POLICY_DATABASE = 'INSURANCE_WORKSHOP_DB'
            ORDER BY CREATED DESC
        """).to_pandas()
        
        # Tag governance and compliance
        results['tag_compliance'] = session.sql("""
            SELECT 
                TAG_NAME,
                TAG_VALUE,
                OBJECT_TYPE,
                OBJECT_NAME,
                DOMAIN,
                APPLIED_AT
            FROM SNOWFLAKE.ACCOUNT_USAGE.TAG_REFERENCES
            WHERE TAG_DATABASE = 'INSURANCE_WORKSHOP_DB'
                AND DELETED IS NULL
                AND APPLIED_AT >= DATEADD('day', -30, CURRENT_TIMESTAMP())
            ORDER BY APPLIED_AT DESC
        """).to_pandas()
        
        # Row access policy monitoring
        results['row_access_policies'] = session.sql("""
            SELECT 
                POLICY_NAME,
                POLICY_KIND,
                POLICY_BODY,
                POLICY_SIGNATURE,
                CREATED,
                LAST_ALTERED,
                COMMENT
            FROM INFORMATION_SCHEMA.ROW_ACCESS_POLICIES
            WHERE POLICY_DATABASE = 'INSURANCE_WORKSHOP_DB'
            ORDER BY CREATED DESC
        """).to_pandas()
        
        # Data classification summary
        results['classification_summary'] = session.sql("""
            SELECT 
                CLASSIFICATION_CATEGORY,
                CLASSIFICATION_NAME,
                COUNT(*) as OBJECT_COUNT,
                COUNT(DISTINCT OBJECT_DATABASE) as DATABASE_COUNT,
                COUNT(DISTINCT OBJECT_SCHEMA) as SCHEMA_COUNT
            FROM SNOWFLAKE.ACCOUNT_USAGE.TAG_REFERENCES
            WHERE TAG_NAME = 'DATA_CLASSIFICATION'
                AND DELETED IS NULL
                AND APPLIED_AT >= DATEADD('day', -30, CURRENT_TIMESTAMP())
            GROUP BY CLASSIFICATION_CATEGORY, CLASSIFICATION_NAME
            ORDER BY OBJECT_COUNT DESC
        """).to_pandas()
        
        # Governance monitoring for the three-entity model
        results['entity_governance'] = session.sql("""
            SELECT 
                'CUSTOMERS' as ENTITY_NAME,
                COUNT(*) as TOTAL_RECORDS,
                COUNT(CASE WHEN CUSTOMER_FIRST_NAME LIKE 'masked_%' THEN 1 END) as MASKED_RECORDS,
                COUNT(CASE WHEN CUSTOMER_EMAIL IS NOT NULL THEN 1 END) as EMAIL_RECORDS,
                'PII_PROTECTED' as GOVERNANCE_STATUS
            FROM INSURANCE_WORKSHOP_DB.SHARING.SECURE_CUSTOMER_VIEW
            UNION ALL
            SELECT 
                'CLAIMS' as ENTITY_NAME,
                COUNT(*) as TOTAL_RECORDS,
                COUNT(CASE WHEN CLAIM_AMOUNT_FILLED > 0 THEN 1 END) as MASKED_RECORDS,
                COUNT(CASE WHEN POLICY_NUMBER IS NOT NULL THEN 1 END) as EMAIL_RECORDS,
                'FINANCIAL_PROTECTED' as GOVERNANCE_STATUS
            FROM INSURANCE_WORKSHOP_DB.SHARING.SECURE_CLAIM_VIEW
            UNION ALL
            SELECT 
                'BROKERS' as ENTITY_NAME,
                COUNT(*) as TOTAL_RECORDS,
                COUNT(CASE WHEN BROKER_ACTIVE = TRUE THEN 1 END) as MASKED_RECORDS,
                COUNT(CASE WHEN BROKER_ID IS NOT NULL THEN 1 END) as EMAIL_RECORDS,
                'ROLE_PROTECTED' as GOVERNANCE_STATUS
            FROM INSURANCE_WORKSHOP_DB.SHARING.SECURE_BROKER_VIEW
        """).to_pandas()
        
        return results
        
    except Exception as e:
        st.error(f"Error fetching governance data: {str(e)}")
        return {}

@st.cache_data(ttl=120)
def get_access_monitoring():
    """Monitor access patterns and compliance"""
    
    try:
        # Access pattern analysis
        access_data = session.sql("""
            SELECT 
                DATE_TRUNC('hour', START_TIME) as ACCESS_HOUR,
                USER_NAME,
                ROLE_NAME,
                WAREHOUSE_NAME,
                DATABASE_NAME,
                SCHEMA_NAME,
                QUERY_TYPE,
                COUNT(*) as QUERY_COUNT,
                SUM(TOTAL_ELAPSED_TIME) as TOTAL_TIME_MS
            FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
            WHERE START_TIME >= DATEADD('day', -7, CURRENT_TIMESTAMP())
                AND DATABASE_NAME = 'INSURANCE_WORKSHOP_DB'
                AND QUERY_TYPE IN ('SELECT', 'INSERT', 'UPDATE', 'DELETE')
            GROUP BY ACCESS_HOUR, USER_NAME, ROLE_NAME, WAREHOUSE_NAME, 
                     DATABASE_NAME, SCHEMA_NAME, QUERY_TYPE
            ORDER BY ACCESS_HOUR DESC
        """).to_pandas()
        
        return access_data
        
    except Exception as e:
        st.warning(f"Access monitoring data not available: {str(e)}")
        return pd.DataFrame()

def calculate_compliance_score(policy_data):
    """Calculate overall compliance score"""
    
    if policy_data.empty:
        return 0
    
    total_policies = len(policy_data)
    compliant_policies = len(policy_data[policy_data['ENFORCEMENT_STATUS'] == 'ENFORCED'])
    
    return (compliant_policies / total_policies) * 100 if total_policies > 0 else 0

# Dashboard Header
st.markdown('<div class="main-header">Insurance Workshop - Governance Dashboard</div>', 
           unsafe_allow_html=True)

col1, col2, col3 = st.columns([2, 1, 1])
with col1:
    st.markdown("**Policy enforcement validation and compliance monitoring for data governance**")
with col2:
    auto_refresh = st.checkbox("Auto-refresh (60s)", value=False)
with col3:
    if st.button("Refresh Governance"):
        st.cache_data.clear()
        st.rerun()

# Auto-refresh logic
if auto_refresh:
    time.sleep(60)
    st.rerun()

# Fetch data
governance_data = get_governance_data()
access_data = get_access_monitoring()

if not governance_data:
    st.error("Unable to load governance data. Please check your session context.")
    st.stop()

# Governance Overview
st.markdown('<div class="section-header">Governance Overview</div>', unsafe_allow_html=True)

if governance_data:
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        policy_count = len(governance_data.get('masking_policies', []))
        st.markdown(f"""
        <div class="compliance-card">
            <h4 style="color: {COLORS['mid_blue']}; margin: 0;">Masking Policies</h4>
            <h2 style="color: {COLORS['main']}; margin: 10px 0;">{policy_count}</h2>
            <p style="color: {COLORS['medium_gray']}; margin: 0;">Active policies</p>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        row_policy_count = len(governance_data.get('row_access_policies', []))
        st.markdown(f"""
        <div class="compliance-card">
            <h4 style="color: {COLORS['mid_blue']}; margin: 0;">Row Access Policies</h4>
            <h2 style="color: {COLORS['purple_moon']}; margin: 10px 0;">{row_policy_count}</h2>
            <p style="color: {COLORS['medium_gray']}; margin: 0;">Access controls</p>
        </div>
        """, unsafe_allow_html=True)
    
    with col3:
        tag_count = len(governance_data.get('tag_compliance', []))
        st.markdown(f"""
        <div class="compliance-card">
            <h4 style="color: {COLORS['mid_blue']}; margin: 0;">Applied Tags</h4>
            <h2 style="color: {COLORS['star_blue']}; margin: 10px 0;">{tag_count}</h2>
            <p style="color: {COLORS['medium_gray']}; margin: 0;">Classification tags</p>
        </div>
        """, unsafe_allow_html=True)
    
    with col4:
        if 'policy_enforcement' in governance_data and not governance_data['policy_enforcement'].empty:
            compliance_score = calculate_compliance_score(governance_data['policy_enforcement'])
            score_color = COLORS['star_blue'] if compliance_score >= 95 else COLORS['valencia_orange'] if compliance_score >= 85 else COLORS['first_light']
            st.markdown(f"""
            <div class="compliance-card">
                <h4 style="color: {COLORS['mid_blue']}; margin: 0;">Compliance Score</h4>
                <h2 style="color: {score_color}; margin: 10px 0;">{compliance_score:.1f}%</h2>
                <p style="color: {COLORS['medium_gray']}; margin: 0;">Overall compliance</p>
            </div>
            """, unsafe_allow_html=True)

# Policy Enforcement Monitoring
st.markdown('<div class="section-header">Policy Enforcement Monitoring</div>', unsafe_allow_html=True)

if 'policy_enforcement' in governance_data and not governance_data['policy_enforcement'].empty:
    policy_data = governance_data['policy_enforcement']
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Policy enforcement status distribution
        status_counts = policy_data['ENFORCEMENT_STATUS'].value_counts()
        
        fig_enforcement = px.pie(
            values=status_counts.values,
            names=status_counts.index,
            color_discrete_map={
                'ENFORCED': COLORS['star_blue'],
                'PENDING': COLORS['valencia_orange'],
                'FAILED': COLORS['first_light'],
                'DISABLED': COLORS['medium_gray']
            },
            title="Policy Enforcement Status Distribution"
        )
        fig_enforcement.update_layout(
            title_font_color=COLORS['mid_blue'],
            height=350
        )
        st.plotly_chart(fig_enforcement, use_container_width=True)
    
    with col2:
        # Compliance scores by policy category
        category_compliance = policy_data.groupby('POLICY_CATEGORY').agg({
            'COMPLIANCE_SCORE': 'mean',
            'VIOLATION_COUNT': 'sum'
        }).reset_index()
        
        fig_category_compliance = px.bar(
            category_compliance,
            x='POLICY_CATEGORY',
            y='COMPLIANCE_SCORE',
            color='VIOLATION_COUNT',
            color_continuous_scale=[[0, COLORS['star_blue']], [0.5, COLORS['valencia_orange']], [1, COLORS['first_light']]],
            title="Compliance Score by Policy Category",
            labels={'COMPLIANCE_SCORE': 'Average Compliance Score', 'POLICY_CATEGORY': 'Policy Category'}
        )
        fig_category_compliance.update_layout(
            title_font_color=COLORS['mid_blue'],
            height=350
        )
        st.plotly_chart(fig_category_compliance, use_container_width=True)
    
    # Policy enforcement details table
    st.markdown("**Policy Enforcement Details**")
    
    # Style the enforcement status
    def style_enforcement_status(val):
        if val == 'ENFORCED':
            return f'background-color: {COLORS["star_blue"]}; color: white'
        elif val == 'PENDING':
            return f'background-color: {COLORS["valencia_orange"]}; color: white'
        elif val == 'FAILED':
            return f'background-color: {COLORS["first_light"]}; color: white'
        return ''
    
    display_policy = policy_data[['POLICY_NAME', 'ENTITY_TYPE', 'ENTITY_NAME', 'ENFORCEMENT_STATUS', 'COMPLIANCE_SCORE', 'VIOLATION_COUNT', 'LAST_EVALUATED']].copy()
    styled_policy = display_policy.style.applymap(style_enforcement_status, subset=['ENFORCEMENT_STATUS'])
    st.dataframe(styled_policy, use_container_width=True)

# Dynamic Masking Policy Analysis
st.markdown('<div class="section-header">Dynamic Masking Policy Analysis</div>', unsafe_allow_html=True)

if 'masking_policies' in governance_data and not governance_data['masking_policies'].empty:
    masking_data = governance_data['masking_policies']
    
    st.markdown(f"""
    <div class="governance-note">
    <strong>Active Masking Policies:</strong> {len(masking_data)} policies configured for 
    progressive data protection across the three-entity insurance model.
    </div>
    """, unsafe_allow_html=True)
    
    # Policy creation timeline
    if 'CREATED' in masking_data.columns:
        masking_data['CREATED_DATE'] = pd.to_datetime(masking_data['CREATED']).dt.date
        policy_timeline = masking_data.groupby('CREATED_DATE').size().reset_index(name='POLICIES_CREATED')
        
        fig_timeline = px.line(
            policy_timeline,
            x='CREATED_DATE',
            y='POLICIES_CREATED',
            title="Masking Policy Creation Timeline",
            line_shape='spline',
            color_discrete_sequence=[COLORS['main']]
        )
        fig_timeline.update_layout(
            title_font_color=COLORS['mid_blue'],
            height=300
        )
        st.plotly_chart(fig_timeline, use_container_width=True)
    
    # Masking policy details
    st.markdown("**Masking Policy Configuration**")
    policy_display = masking_data[['POLICY_NAME', 'POLICY_KIND', 'CREATED', 'LAST_ALTERED', 'COMMENT']].copy()
    st.dataframe(policy_display, use_container_width=True)

# Three-Entity Model Governance
st.markdown('<div class="section-header">Three-Entity Model Governance</div>', unsafe_allow_html=True)

if 'entity_governance' in governance_data and not governance_data['entity_governance'].empty:
    entity_gov = governance_data['entity_governance']
    
    col1, col2, col3 = st.columns(3)
    
    for idx, entity in entity_gov.iterrows():
        with [col1, col2, col3][idx % 3]:
            entity_name = entity['ENTITY_NAME']
            total_records = entity['TOTAL_RECORDS']
            masked_records = entity['MASKED_RECORDS']
            governance_status = entity['GOVERNANCE_STATUS']
            
            masking_percentage = (masked_records / total_records * 100) if total_records > 0 else 0
            
            # Determine protection level color
            if governance_status == 'PII_PROTECTED':
                protection_color = COLORS['star_blue']
            elif governance_status == 'FINANCIAL_PROTECTED':
                protection_color = COLORS['purple_moon']
            else:
                protection_color = COLORS['valencia_orange']
            
            st.markdown(f"""
            <div class="governance-card policy-compliant">
                <h3 style="color: {COLORS['mid_blue']}; margin: 0;">{entity_name}</h3>
                <h2 style="color: {protection_color}; margin: 10px 0;">{masking_percentage:.1f}%</h2>
                <p style="color: {COLORS['medium_gray']}; margin: 5px 0;">
                    {masked_records:,} of {total_records:,} records protected
                </p>
                <p style="color: {protection_color}; margin: 0; font-weight: bold;">
                    {governance_status.replace('_', ' ').title()}
                </p>
            </div>
            """, unsafe_allow_html=True)

# Tag Compliance Tracking
st.markdown('<div class="section-header">Tag Compliance Tracking</div>', unsafe_allow_html=True)

if 'tag_compliance' in governance_data and not governance_data['tag_compliance'].empty:
    tag_data = governance_data['tag_compliance']
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Tag distribution by object type
        tag_by_type = tag_data.groupby(['OBJECT_TYPE', 'TAG_NAME']).size().reset_index(name='TAG_COUNT')
        
        fig_tag_type = px.bar(
            tag_by_type,
            x='OBJECT_TYPE',
            y='TAG_COUNT',
            color='TAG_NAME',
            color_discrete_sequence=[COLORS['main'], COLORS['star_blue'], COLORS['valencia_orange'], COLORS['purple_moon']],
            title="Tag Distribution by Object Type"
        )
        fig_tag_type.update_layout(
            title_font_color=COLORS['mid_blue'],
            height=350
        )
        st.plotly_chart(fig_tag_type, use_container_width=True)
    
    with col2:
        # Tag application timeline
        if 'APPLIED_AT' in tag_data.columns:
            tag_data['APPLIED_DATE'] = pd.to_datetime(tag_data['APPLIED_AT']).dt.date
            tag_timeline = tag_data.groupby('APPLIED_DATE').size().reset_index(name='TAGS_APPLIED')
            
            fig_tag_timeline = px.area(
                tag_timeline,
                x='APPLIED_DATE',
                y='TAGS_APPLIED',
                title="Tag Application Timeline",
                color_discrete_sequence=[COLORS['star_blue']]
            )
            fig_tag_timeline.update_layout(
                title_font_color=COLORS['mid_blue'],
                height=350
            )
            st.plotly_chart(fig_tag_timeline, use_container_width=True)

# Data Classification Summary
st.markdown('<div class="section-header">Data Classification Summary</div>', unsafe_allow_html=True)

if 'classification_summary' in governance_data and not governance_data['classification_summary'].empty:
    classification_data = governance_data['classification_summary']
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Classification category distribution
        fig_classification = px.pie(
            classification_data,
            values='OBJECT_COUNT',
            names='CLASSIFICATION_CATEGORY',
            color_discrete_sequence=[COLORS['main'], COLORS['star_blue'], COLORS['valencia_orange'], COLORS['purple_moon']],
            title="Data Classification Distribution"
        )
        fig_classification.update_layout(
            title_font_color=COLORS['mid_blue'],
            height=350
        )
        st.plotly_chart(fig_classification, use_container_width=True)
    
    with col2:
        # Classification scope
        fig_scope = px.bar(
            classification_data,
            x='CLASSIFICATION_NAME',
            y='OBJECT_COUNT',
            color='DATABASE_COUNT',
            color_continuous_scale=[[0, COLORS['star_blue']], [1, COLORS['purple_moon']]],
            title="Classification Scope by Type",
            labels={'OBJECT_COUNT': 'Number of Objects', 'CLASSIFICATION_NAME': 'Classification Type'}
        )
        fig_scope.update_layout(
            title_font_color=COLORS['mid_blue'],
            height=350
        )
        st.plotly_chart(fig_scope, use_container_width=True)

# Access Pattern Monitoring
st.markdown('<div class="section-header">Access Pattern Monitoring</div>', unsafe_allow_html=True)

if not access_data.empty:
    col1, col2 = st.columns(2)
    
    with col1:
        # Query type distribution
        query_distribution = access_data.groupby('QUERY_TYPE')['QUERY_COUNT'].sum().reset_index()
        
        fig_query_types = px.bar(
            query_distribution,
            x='QUERY_TYPE',
            y='QUERY_COUNT',
            color='QUERY_TYPE',
            color_discrete_map={
                'SELECT': COLORS['star_blue'],
                'INSERT': COLORS['valencia_orange'],
                'UPDATE': COLORS['purple_moon'],
                'DELETE': COLORS['first_light']
            },
            title="Query Type Distribution (Last 7 Days)"
        )
        fig_query_types.update_layout(
            title_font_color=COLORS['mid_blue'],
            height=350,
            showlegend=False
        )
        st.plotly_chart(fig_query_types, use_container_width=True)
    
    with col2:
        # Access by schema
        schema_access = access_data.groupby('SCHEMA_NAME')['QUERY_COUNT'].sum().reset_index()
        
        fig_schema_access = px.pie(
            schema_access,
            values='QUERY_COUNT',
            names='SCHEMA_NAME',
            color_discrete_sequence=[COLORS['main'], COLORS['star_blue'], COLORS['valencia_orange'], COLORS['purple_moon']],
            title="Schema Access Distribution"
        )
        fig_schema_access.update_layout(
            title_font_color=COLORS['mid_blue'],
            height=350
        )
        st.plotly_chart(fig_schema_access, use_container_width=True)
else:
    st.info("Access monitoring data is not available in the current session context.")

# Governance Health Summary
st.markdown('<div class="section-header">Governance Health Summary</div>', unsafe_allow_html=True)

if governance_data:
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        if 'entity_governance' in governance_data:
            entities_protected = len(governance_data['entity_governance'])
            st.metric("Protected Entities", entities_protected)
    
    with col2:
        if 'policy_enforcement' in governance_data and not governance_data['policy_enforcement'].empty:
            active_policies = len(governance_data['policy_enforcement'][governance_data['policy_enforcement']['ENFORCEMENT_STATUS'] == 'ENFORCED'])
            st.metric("Active Policies", active_policies)
    
    with col3:
        if 'tag_compliance' in governance_data:
            total_tags = len(governance_data['tag_compliance'])
            st.metric("Applied Tags", total_tags)
    
    with col4:
        if 'policy_enforcement' in governance_data and not governance_data['policy_enforcement'].empty:
            violations = governance_data['policy_enforcement']['VIOLATION_COUNT'].sum()
            st.metric("Total Violations", violations, delta=f"-{violations}" if violations > 0 else None)

# Footer
st.markdown("---")
st.markdown(f"""
<div style='text-align: center; color: {COLORS['medium_gray']}; padding: 20px;'>
    <p><strong>Insurance Workshop - Governance Dashboard</strong></p>
    <p>Powered by Snowflake Governance Features with Policy Enforcement Validation</p>
    <p>Dynamic Masking • Row Access Policies • Tag Compliance • Access Monitoring</p>
</div>
""", unsafe_allow_html=True) 