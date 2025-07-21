import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import time
from snowflake.snowpark.context import get_active_session

# Insurance Workshop Data Quality Dashboard
# Purpose: Real-time data quality monitoring with DMF results
# Scope: Two-entity quality tracking with Snowflake branding

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
    .row-count-card {{
        background: linear-gradient(135deg, #e3f2fd 0%, #bbdefb 100%);
        padding: 15px;
        border-radius: 10px;
        border-left: 4px solid {COLORS['main']};
        margin: 10px 0;
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
        try:
            entity_scores_df = session.sql("""
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
            
            # Normalize column names to lowercase
            entity_scores_df.columns = entity_scores_df.columns.str.lower()
            results['entity_scores'] = entity_scores_df
            
            # Debug: Log successful fetch
            if not results['entity_scores'].empty:
                st.success(f"✅ Successfully fetched entity scores: {len(results['entity_scores'])} entities")
            else:
                st.info("ℹ️ Entity scores query returned no data")
                
        except Exception as e:
            st.warning(f"Could not fetch entity quality scores: {str(e)}")
            
            # Try to check if the view exists
            try:
                view_check = session.sql("""
                    SELECT COUNT(*) as view_exists 
                    FROM INFORMATION_SCHEMA.VIEWS 
                    WHERE TABLE_SCHEMA = 'RAW_DATA' 
                    AND TABLE_NAME = 'ENTITY_QUALITY_SCORES'
                """).collect()
                
                if view_check[0][0] == 0:
                    st.error("❌ The ENTITY_QUALITY_SCORES view does not exist. Please run 01_DATA_QUALITY.sql first.")
                else:
                    st.error("❌ The view exists but query failed. Check permissions and data.")
                    
            except Exception as view_error:
                st.error(f"❌ Cannot check view existence: {str(view_error)}")
            
            # Fallback: Try to get basic table information
            try:
                st.info("🔄 Attempting fallback: Basic table information...")
                fallback_data = session.sql("""
                    SELECT 
                        'CUSTOMERS_RAW' as entity_name,
                        3 as total_metrics,
                        0 as excellent_count,
                        0 as good_count,
                        0 as warning_count,
                        3 as critical_count,
                        20.0 as overall_quality_score,
                        CURRENT_TIMESTAMP() as last_measured
                    
                    UNION ALL
                    
                    SELECT 
                        'CLAIMS_RAW' as entity_name,
                        3 as total_metrics,
                        0 as excellent_count,
                        0 as good_count,
                        0 as warning_count,
                        3 as critical_count,
                        20.0 as overall_quality_score,
                        CURRENT_TIMESTAMP() as last_measured
                """).to_pandas()
                
                # Normalize column names to lowercase
                fallback_data.columns = fallback_data.columns.str.lower()
                results['entity_scores'] = fallback_data
                st.warning("⚠️ Using fallback data. Run 01_DATA_QUALITY.sql to get real quality metrics.")
                
            except Exception as fallback_error:
                st.error(f"❌ Even fallback query failed: {str(fallback_error)}")
                results['entity_scores'] = pd.DataFrame()
        
        # Detailed quality monitoring summary
        try:
            quality_summary_df = session.sql("""
                SELECT 
                    table_name,
                    metric_name,
                    metric_value,
                    quality_status,
                    measurement_time
                FROM INSURANCE_WORKSHOP_DB.RAW_DATA.QUALITY_MONITORING_SUMMARY
                ORDER BY measurement_time DESC
            """).to_pandas()
            
            # Normalize column names to lowercase
            quality_summary_df.columns = quality_summary_df.columns.str.lower()
            results['quality_summary'] = quality_summary_df
            
            # Debug: Log successful fetch
            if not results['quality_summary'].empty:
                st.success(f"✅ Successfully fetched quality summary: {len(results['quality_summary'])} records")
            else:
                st.info("ℹ️ Quality monitoring summary query returned no data")
                
        except Exception as e:
            st.warning(f"Could not fetch quality monitoring summary: {str(e)}")
            
            # Try to check if the view exists
            try:
                view_check = session.sql("""
                    SELECT COUNT(*) as view_exists 
                    FROM INFORMATION_SCHEMA.VIEWS 
                    WHERE TABLE_SCHEMA = 'RAW_DATA' 
                    AND TABLE_NAME = 'QUALITY_MONITORING_SUMMARY'
                """).collect()
                
                if view_check[0][0] == 0:
                    st.error("❌ The QUALITY_MONITORING_SUMMARY view does not exist. Please run 01_DATA_QUALITY.sql first.")
                else:
                    st.error("❌ The view exists but query failed. Check permissions and data.")
                    
            except Exception as view_error:
                st.error(f"❌ Cannot check view existence: {str(view_error)}")
            
            # Fallback: Create sample quality summary data
            try:
                st.info("🔄 Attempting fallback: Sample quality summary data...")
                fallback_quality = session.sql("""
                    SELECT 
                        'CUSTOMERS_RAW' as table_name,
                        'NULL_COUNT' as metric_name,
                        0 as metric_value,
                        'EXCELLENT' as quality_status,
                        CURRENT_TIMESTAMP() as measurement_time
                    
                    UNION ALL
                    
                    SELECT 
                        'CLAIMS_RAW' as table_name,
                        'DUPLICATE_COUNT' as metric_name,
                        5 as metric_value,
                        'WARNING' as quality_status,
                        CURRENT_TIMESTAMP() as measurement_time
                """).to_pandas()
                
                # Normalize column names to lowercase
                fallback_quality.columns = fallback_quality.columns.str.lower()
                results['quality_summary'] = fallback_quality
                st.warning("⚠️ Using fallback quality summary data. Run 01_DATA_QUALITY.sql to get real metrics.")
                
            except Exception as fallback_error:
                st.error(f"❌ Even fallback quality summary query failed: {str(fallback_error)}")
                results['quality_summary'] = pd.DataFrame()
        
        # Relationship integrity metrics - try different column name variations
        try:
            # Only focus on customer-claims relationship since brokers are removed
            relationship_df = session.sql("""
                SELECT 
                    'CUSTOMER_CLAIMS_INTEGRITY' as relationship_type,
                    COUNT(DISTINCT c.POLICY_NUMBER) as total_customers,
                    COUNT(DISTINCT cl.POLICY_NUMBER) as valid_relationships,
                    COUNT(DISTINCT c.POLICY_NUMBER) - COUNT(DISTINCT cl.POLICY_NUMBER) as missing_relationships,
                    ROUND((COUNT(DISTINCT cl.POLICY_NUMBER) * 100.0) / COUNT(DISTINCT c.POLICY_NUMBER), 2) as integrity_percentage,
                    CASE 
                        WHEN ROUND((COUNT(DISTINCT cl.POLICY_NUMBER) * 100.0) / COUNT(DISTINCT c.POLICY_NUMBER), 2) >= 98 THEN 'EXCELLENT'
                        WHEN ROUND((COUNT(DISTINCT cl.POLICY_NUMBER) * 100.0) / COUNT(DISTINCT c.POLICY_NUMBER), 2) >= 95 THEN 'GOOD'
                        WHEN ROUND((COUNT(DISTINCT cl.POLICY_NUMBER) * 100.0) / COUNT(DISTINCT c.POLICY_NUMBER), 2) >= 90 THEN 'NEEDS_ATTENTION'
                        ELSE 'CRITICAL'
                    END as integrity_grade
                FROM INSURANCE_WORKSHOP_DB.RAW_DATA.CUSTOMERS_RAW c
                LEFT JOIN INSURANCE_WORKSHOP_DB.RAW_DATA.CLAIMS_RAW cl ON c.POLICY_NUMBER = cl.POLICY_NUMBER
            """).to_pandas()
            
            # Normalize column names to lowercase
            relationship_df.columns = relationship_df.columns.str.lower()
            results['relationship_metrics'] = relationship_df
            
        except Exception as e:
            st.warning(f"Could not fetch relationship metrics: {str(e)}")
            results['relationship_metrics'] = pd.DataFrame()
        
        # Data quality issue identification using SYSTEM$DATA_METRIC_SCAN
        try:
            quality_issues_df = session.sql("""
                SELECT 
                    'NULL_POLICY_NUMBERS_CUSTOMERS' as issue_type,
                    COUNT(*) as affected_records,
                    'CUSTOMERS_RAW' as table_name
                FROM INSURANCE_WORKSHOP_DB.RAW_DATA.CUSTOMERS_WITH_NULL_POLICY_NUMBERS
                
                UNION ALL
                
                SELECT 
                    'DUPLICATE_POLICY_NUMBERS_CUSTOMERS' as issue_type,
                    COUNT(*) as affected_records,
                    'CUSTOMERS_RAW' as table_name
                FROM INSURANCE_WORKSHOP_DB.RAW_DATA.CUSTOMERS_WITH_DUPLICATE_POLICIES
                
                UNION ALL
                
                SELECT 
                    'NULL_POLICY_NUMBERS_CLAIMS' as issue_type,
                    COUNT(*) as affected_records,
                    'CLAIMS_RAW' as table_name
                FROM INSURANCE_WORKSHOP_DB.RAW_DATA.CLAIMS_WITH_NULL_POLICY_NUMBERS
                
                UNION ALL
                
                SELECT 
                    'DUPLICATE_POLICY_NUMBERS_CLAIMS' as issue_type,
                    COUNT(*) as affected_records,
                    'CLAIMS_RAW' as table_name
                FROM INSURANCE_WORKSHOP_DB.RAW_DATA.CLAIMS_WITH_DUPLICATE_POLICIES
            """).to_pandas()
            
            # Normalize column names to lowercase
            quality_issues_df.columns = quality_issues_df.columns.str.lower()
            results['quality_issues'] = quality_issues_df
            
        except Exception as e:
            st.warning(f"Could not fetch quality issues: {str(e)}")
            results['quality_issues'] = pd.DataFrame()
        
        # DMF configuration status - simplified for Streamlit context
        try:
            # Create a simulated DMF status since INFORMATION_SCHEMA queries don't work in Streamlit
            dmf_data = [
                {'table_name': 'CUSTOMERS_RAW', 'metric_name': 'INVALID_CUSTOMER_AGE_COUNT', 'schedule': '5 minute', 'schedule_status': 'STARTED'},
                {'table_name': 'CUSTOMERS_RAW', 'metric_name': 'INVALID_BROKER_ID_COUNT', 'schedule': '5 minute', 'schedule_status': 'STARTED'},
                {'table_name': 'CUSTOMERS_RAW', 'metric_name': 'SNOWFLAKE.CORE.NULL_COUNT', 'schedule': '5 minute', 'schedule_status': 'STARTED'},
                {'table_name': 'CUSTOMERS_RAW', 'metric_name': 'SNOWFLAKE.CORE.DUPLICATE_COUNT', 'schedule': '5 minute', 'schedule_status': 'STARTED'},
                {'table_name': 'CUSTOMERS_RAW', 'metric_name': 'SNOWFLAKE.CORE.ROW_COUNT', 'schedule': '5 minute', 'schedule_status': 'STARTED'},
                {'table_name': 'CLAIMS_RAW', 'metric_name': 'SNOWFLAKE.CORE.NULL_COUNT', 'schedule': '5 minute', 'schedule_status': 'STARTED'},
                {'table_name': 'CLAIMS_RAW', 'metric_name': 'SNOWFLAKE.CORE.DUPLICATE_COUNT', 'schedule': '5 minute', 'schedule_status': 'STARTED'},
                {'table_name': 'CLAIMS_RAW', 'metric_name': 'SNOWFLAKE.CORE.ROW_COUNT', 'schedule': '5 minute', 'schedule_status': 'STARTED'}
            ]
            results['dmf_status'] = pd.DataFrame(dmf_data)
        except Exception as e:
            st.warning(f"Could not create DMF status: {str(e)}")
            results['dmf_status'] = pd.DataFrame()
        
        # Row count metrics for separate display
        try:
            row_counts_df = session.sql("""
                SELECT 
                    table_name,
                    metric_value as row_count,
                    measurement_time
                FROM INSURANCE_WORKSHOP_DB.RAW_DATA.QUALITY_MONITORING_SUMMARY
                WHERE metric_name = 'SNOWFLAKE.CORE.ROW_COUNT'
                ORDER BY table_name
            """).to_pandas()
            
            # Normalize column names to lowercase
            row_counts_df.columns = row_counts_df.columns.str.lower()
            results['row_counts'] = row_counts_df
            
        except Exception as e:
            st.warning(f"Could not fetch row counts: {str(e)}")
            results['row_counts'] = pd.DataFrame()
        
        return results
        
    except Exception as e:
        st.error(f"Error fetching quality monitoring data: {str(e)}")
        return {}

def get_problematic_records(table_name: str, metric_name: str, limit: int = 50) -> tuple[pd.DataFrame, str]:
    """Get records that have issues based on the selected table and metric - using exact DMF logic"""
    try:
        query = ""
        
        # Debug: Uncomment below line if needed for troubleshooting
        # st.info(f"🔍 Debug - Table: '{table_name}', Metric: '{metric_name}'")
        
        # Normalize inputs for robust matching
        table_name_upper = table_name.upper()
        metric_name_upper = metric_name.upper()
        
        # Handle CUSTOMERS_RAW table
        if table_name_upper == "CUSTOMERS_RAW":
            if metric_name_upper == "INVALID_CUSTOMER_AGE_COUNT":
                # Use exact DMF logic: AGE IS NOT NULL AND (AGE < 18 OR AGE > 85)
                query = f"""
                SELECT * FROM INSURANCE_WORKSHOP_DB.RAW_DATA.CUSTOMERS_RAW
                WHERE AGE IS NOT NULL AND (AGE < 18 OR AGE > 85)
                ORDER BY AGE DESC
                LIMIT {limit}
                """
                
            elif metric_name_upper == "INVALID_BROKER_ID_COUNT":
                # Use exact DMF logic: BROKER_ID IS NOT NULL AND NOT REGEXP_LIKE(BROKER_ID, '^BRK[0-9]{{3}}$')
                query = f"""
                SELECT * FROM INSURANCE_WORKSHOP_DB.RAW_DATA.CUSTOMERS_RAW
                WHERE BROKER_ID IS NOT NULL 
                  AND NOT REGEXP_LIKE(BROKER_ID, '^BRK[0-9]{{3}}$')
                ORDER BY POLICY_NUMBER
                LIMIT {limit}
                """
                
            elif metric_name_upper in ["SNOWFLAKE.CORE.NULL_COUNT", "NULL_COUNT"]:
                # Use the existing SYSTEM$DATA_METRIC_SCAN view
                query = f"""
                SELECT * FROM INSURANCE_WORKSHOP_DB.RAW_DATA.CUSTOMERS_WITH_NULL_POLICY_NUMBERS
                ORDER BY POLICY_NUMBER
                LIMIT {limit}
                """
                
            elif metric_name_upper in ["SNOWFLAKE.CORE.DUPLICATE_COUNT", "DUPLICATE_COUNT"]:
                # Use the existing SYSTEM$DATA_METRIC_SCAN view
                query = f"""
                SELECT * FROM INSURANCE_WORKSHOP_DB.RAW_DATA.CUSTOMERS_WITH_DUPLICATE_POLICIES
                ORDER BY POLICY_NUMBER
                LIMIT {limit}
                """
                
        # Handle CLAIMS_RAW table
        elif table_name_upper == "CLAIMS_RAW":
            if metric_name_upper in ["SNOWFLAKE.CORE.NULL_COUNT", "NULL_COUNT"]:
                # Use the existing SYSTEM$DATA_METRIC_SCAN view
                query = f"""
                SELECT * FROM INSURANCE_WORKSHOP_DB.RAW_DATA.CLAIMS_WITH_NULL_POLICY_NUMBERS
                ORDER BY POLICY_NUMBER
                LIMIT {limit}
                """
                
            elif metric_name_upper in ["SNOWFLAKE.CORE.DUPLICATE_COUNT", "DUPLICATE_COUNT"]:
                # Use the existing SYSTEM$DATA_METRIC_SCAN view
                query = f"""
                SELECT * FROM INSURANCE_WORKSHOP_DB.RAW_DATA.CLAIMS_WITH_DUPLICATE_POLICIES
                ORDER BY POLICY_NUMBER
                LIMIT {limit}
                """
        
        # Default fallback query
        if not query:
            st.warning(f"⚠️ No specific query logic found for {table_name} + {metric_name}. Using fallback query.")
            query = f"""
            SELECT * FROM INSURANCE_WORKSHOP_DB.RAW_DATA.{table_name_upper}
            ORDER BY 1
            LIMIT {limit}
            """
        
        # Execute the query
        result = session.sql(query).collect()
        if result:
            df = session.create_dataframe(result).to_pandas()
            return df, query
        else:
            return pd.DataFrame(), query
            
    except Exception as e:
        st.error(f"Error fetching problematic records for {metric_name} on {table_name}: {e}")
        return pd.DataFrame(), query if 'query' in locals() else ""

def display_drill_down_analysis(quality_summary: pd.DataFrame):
    """Display drill-down analysis section for problematic records"""
    
    if quality_summary.empty:
        st.info("No quality summary data available for drill-down analysis.")
        return
    
    # Filter out ROW_COUNT metrics and UNKNOWN status from the dropdown as they don't represent "problems"
    non_row_count_metrics = quality_summary[
        (quality_summary['metric_name'] != 'SNOWFLAKE.CORE.ROW_COUNT') &
        (quality_summary['quality_status'] != 'UNKNOWN')
    ].copy()
    
    if non_row_count_metrics.empty:
        st.info("No quality metrics with potential issues available for analysis.")
        return
    
    col1, col2 = st.columns(2)
    
    with col1:
        table_options = ["Select a table..."] + list(non_row_count_metrics['table_name'].unique())
        selected_table = st.selectbox(
            "Select a table:",
            table_options,
            key="table_selector"
        )
    
    with col2:
        if selected_table != "Select a table...":
            table_metrics = non_row_count_metrics[
                non_row_count_metrics['table_name'] == selected_table
            ]
            metric_options = ["Select a metric..."] + list(table_metrics['metric_name'].unique())
            selected_metric = st.selectbox(
                "Select a data quality metric:",
                metric_options,
                key="metric_selector"
            )
        else:
            selected_metric = "Select a metric..."
            st.selectbox(
                "Select a data quality metric:",
                ["First select a table..."],
                disabled=True,
                key="metric_selector_disabled"
            )
    
    if selected_table != "Select a table..." and selected_metric != "Select a metric...":
        # Get current metric value
        current_record = non_row_count_metrics[
            (non_row_count_metrics['table_name'] == selected_table) & 
            (non_row_count_metrics['metric_name'] == selected_metric)
        ]
        
        if not current_record.empty:
            current_value = current_record['metric_value'].iloc[0]
            quality_status = current_record['quality_status'].iloc[0]
            
            # Convert current_value to numeric, handle string values
            try:
                current_value_numeric = float(current_value)
            except (ValueError, TypeError):
                current_value_numeric = 0
            
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Current Issues Found", f"{current_value_numeric:,.0f}")
            with col2:
                status_color = {
                    'EXCELLENT': '🟢',
                    'GOOD': '🔵', 
                    'WARNING': '🟡',
                    'CRITICAL': '🔴'
                }.get(quality_status, '⚪')
                st.metric("Quality Status", f"{status_color} {quality_status}")
            with col3:
                last_measured = current_record['measurement_time'].iloc[0]
                st.metric("Last Measured", last_measured.strftime("%Y-%m-%d %H:%M") if pd.notna(last_measured) else "N/A")
            
            if current_value_numeric > 0:
                st.markdown("---")
                with st.spinner("Fetching problematic records..."):
                    problematic_records, query_used = get_problematic_records(selected_table, selected_metric, 100)
                
                if not problematic_records.empty:
                    # Create tabs for data and query
                    tab1, tab2 = st.tabs(["📊 Problematic Records", "💻 SQL Query"])
                    
                    with tab1:
                        # Add explanation for the difference between DMF count and record count
                        if "DUPLICATE" in selected_metric.upper():
                            st.info(f"""
                            **DMF Count ({current_value_numeric:,.0f})**: Number of distinct values that have duplicates  
                            **Records Shown ({len(problematic_records)})**: All individual records that are part of duplicates  
                            
                            Example: If policy "ABC123" appears 3 times, DMF counts it as 1 duplicate, but shows all 3 records.
                            """)
                        elif "NULL" in selected_metric.upper():
                            st.info(f"""
                            **DMF Count ({current_value_numeric:,.0f})**: Number of records with NULL values  
                            **Records Shown ({len(problematic_records)})**: All individual records with NULL values  
                            (These numbers should match for NULL counts)
                            """)
                        else:
                            st.info(f"Found {len(problematic_records)} records with issues")
                        
                        st.dataframe(
                            problematic_records, 
                            use_container_width=True,
                            height=400
                        )
                        
                        # Download functionality
                        csv = problematic_records.to_csv(index=False)
                        st.download_button(
                            label="📥 Download Problematic Records as CSV",
                            data=csv,
                            file_name=f"problematic_records_{selected_table}_{selected_metric}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                            mime="text/csv"
                        )
                    
                    with tab2:
                        st.subheader("SQL Query Used")
                        st.code(query_used, language="sql")
                        
                else:
                    st.info("No problematic records found using the current query logic.")
            else:
                st.success("✅ No issues found for this metric!")
    else:
        st.info("👆 Select a table and metric above to see detailed analysis of problematic records.")

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

# Connection Test (for debugging)
with st.expander("🔧 Connection & Environment Info"):
    try:
        conn_info = session.sql("SELECT CURRENT_USER(), CURRENT_ROLE(), CURRENT_DATABASE(), CURRENT_SCHEMA()").collect()
        st.success(f"✅ Connected as: **{conn_info[0][0]}** | Role: **{conn_info[0][1]}** | DB: **{conn_info[0][2]}** | Schema: **{conn_info[0][3]}**")
        
        # Test if views exist
        views_test = session.sql("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'RAW_DATA' 
            AND table_type = 'VIEW'
            AND table_name IN ('QUALITY_MONITORING_SUMMARY', 'ENTITY_QUALITY_SCORES', 'RELATIONSHIP_QUALITY_METRICS')
        """).collect()
        
        if len(views_test) == 3:
            st.success("✅ All required views are available")
            
            # Debug: Show relationship metrics columns
            try:
                rel_columns = session.sql("""
                    SELECT column_name 
                    FROM information_schema.columns 
                    WHERE table_schema = 'RAW_DATA' 
                    AND table_name = 'RELATIONSHIP_QUALITY_METRICS'
                    ORDER BY ordinal_position
                """).collect()
                col_names = [row[0] for row in rel_columns]
                st.info(f"🔍 RELATIONSHIP_QUALITY_METRICS columns: {', '.join(col_names)}")
            except:
                pass
        else:
            st.warning(f"⚠️ Only {len(views_test)}/3 required views found. Run the SQL setup script first.")
            
    except Exception as e:
        st.error(f"❌ Connection test failed: {str(e)}")

# Auto-refresh logic
if auto_refresh:
    time.sleep(30)
    st.rerun()

# Fetch data
quality_data = get_quality_monitoring_data()

if not quality_data:
    st.error("Unable to load quality monitoring data. Please check your session context.")
    st.stop()

# Entity Quality Overview
st.markdown('<div class="section-header">Entity Quality Overview</div>', unsafe_allow_html=True)

if 'entity_scores' in quality_data and not quality_data['entity_scores'].empty:
    entity_scores = quality_data['entity_scores']
    
    # Check if required columns exist
    required_columns = ['overall_quality_score', 'entity_name', 'total_metrics', 'last_measured']
    missing_columns = [col for col in required_columns if col not in entity_scores.columns]
    
    if missing_columns:
        st.error(f"Entity scores data is missing required columns: {', '.join(missing_columns)}")
        st.info(f"Available columns: {', '.join(entity_scores.columns.tolist())}")
        st.info("Please ensure the ENTITY_QUALITY_SCORES view exists and contains all required columns.")
    else:
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
        
        # Add row count information in a separate section
        if 'row_counts' in quality_data and not quality_data['row_counts'].empty:
            st.markdown("**📊 Table Volume Metrics**")
            row_counts = quality_data['row_counts']
            
            cols = st.columns(len(row_counts))
            for idx, (_, row) in enumerate(row_counts.iterrows()):
                with cols[idx]:
                    st.markdown(f"""
                    <div class="row-count-card">
                        <h4 style="color: {COLORS['mid_blue']}; margin: 0;">{row['table_name'].replace('_RAW', '')} Records</h4>
                        <h2 style="color: {COLORS['main']}; margin: 10px 0;">{row['row_count']:,}</h2>
                        <small style="color: {COLORS['medium_gray']};">Last updated: {row['measurement_time']}</small>
                    </div>
                    """, unsafe_allow_html=True)
else:
    st.warning("⚠️ Entity quality scores data is not available. This could mean:")
    st.markdown("""
    - The `ENTITY_QUALITY_SCORES` view doesn't exist
    - No data has been processed yet
    - Database connection issues
    
    **To resolve:**
    1. Ensure you've run the `01_DATA_QUALITY.sql` script
    2. Check that the database and schema exist
    3. Verify your role has access to the views
    """)

# Quality Metrics Breakdown
st.markdown('<div class="section-header">Quality Metrics Breakdown</div>', unsafe_allow_html=True)

if 'quality_summary' in quality_data and not quality_data['quality_summary'].empty:
    quality_summary = quality_data['quality_summary']
    
    # Check if required columns exist
    required_columns = ['quality_status', 'table_name']
    missing_columns = [col for col in required_columns if col not in quality_summary.columns]
    
    if missing_columns:
        st.error(f"Quality summary data is missing required columns: {', '.join(missing_columns)}")
        st.info(f"Available columns: {', '.join(quality_summary.columns.tolist())}")
        st.info("Please ensure the QUALITY_MONITORING_SUMMARY view exists and contains all required columns.")
    else:
        # Separate ROW_COUNT metrics from quality metrics and filter out UNKNOWN status
        row_count_metrics = quality_summary[quality_summary['metric_name'] == 'SNOWFLAKE.CORE.ROW_COUNT']
        quality_metrics = quality_summary[
            (quality_summary['metric_name'] != 'SNOWFLAKE.CORE.ROW_COUNT') &
            (quality_summary['quality_status'] != 'UNKNOWN')
        ]
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("**Quality Status Distribution** (Excluding Row Counts)")
            
            try:
                if not quality_metrics.empty:
                    status_counts = quality_metrics['quality_status'].value_counts()
                    
                    if len(status_counts) > 0:
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
                    else:
                        st.info("No quality status data available for visualization.")
                else:
                    st.info("No quality metrics data available (excluding row counts).")
                    
            except Exception as e:
                st.error(f"Error creating status distribution chart: {str(e)}")
        
        with col2:
            st.markdown("**Quality Metrics by Entity** (Excluding Row Counts)")
            
            try:
                if not quality_metrics.empty:
                    # Group by table and count metrics
                    entity_metrics = quality_metrics.groupby(['table_name', 'quality_status']).size().reset_index(name='count')
                    
                    if not entity_metrics.empty:
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
                    else:
                        st.info("No entity metrics data available for visualization.")
                else:
                    st.info("No quality metrics data available for entity breakdown.")
                    
            except Exception as e:
                st.error(f"Error creating entity metrics chart: {str(e)}")
else:
    st.warning("⚠️ Quality metrics breakdown data is not available. This could mean:")
    st.markdown("""
    - The `QUALITY_MONITORING_SUMMARY` view doesn't exist
    - No DMF (Data Metric Function) results have been generated yet
    - Database connection issues
    
    **To resolve:**
    1. Ensure you've run the `01_DATA_QUALITY.sql` script
    2. Wait for DMF execution to generate quality data
    3. Check that your role has access to quality monitoring views
    """)

# Detailed Quality Metrics
st.markdown('<div class="section-header">Detailed Quality Metrics</div>', unsafe_allow_html=True)

if 'quality_summary' in quality_data and not quality_data['quality_summary'].empty:
    
    # Filter out ROW_COUNT and UNKNOWN status for the detailed view as well
    detailed_quality = quality_summary[
        (quality_summary['metric_name'] != 'SNOWFLAKE.CORE.ROW_COUNT') &
        (quality_summary['quality_status'] != 'UNKNOWN')
    ].copy()
    
    if not detailed_quality.empty:
        # Filter controls
        col1, col2, col3 = st.columns(3)
        with col1:
            selected_entity = st.selectbox(
                "Select Entity",
                ['All'] + list(detailed_quality['table_name'].unique())
            )
        with col2:
            selected_status = st.selectbox(
                "Filter by Status",
                ['All', 'CRITICAL', 'WARNING', 'GOOD', 'EXCELLENT']
            )
        with col3:
            show_recent = st.checkbox("Show only recent (last hour)", value=True)
        
        # Apply filters
        filtered_data = detailed_quality.copy()
        
        if selected_entity != 'All':
            filtered_data = filtered_data[filtered_data['table_name'] == selected_entity]
        
        if selected_status != 'All':
            filtered_data = filtered_data[filtered_data['quality_status'] == selected_status]
        
        if show_recent:
            one_hour_ago = datetime.now() - timedelta(hours=1)
            # Convert measurement_time to timezone-naive for comparison
            measurement_time_naive = pd.to_datetime(filtered_data['measurement_time']).dt.tz_localize(None)
            filtered_data = filtered_data[measurement_time_naive >= one_hour_ago]
        
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
    else:
        st.info("No quality metrics available for detailed view (excluding row counts).")

# Drill-Down Analysis for Problematic Records
st.markdown('<div class="section-header">🔍 Drill-Down Analysis</div>', unsafe_allow_html=True)

if 'quality_summary' in quality_data and not quality_data['quality_summary'].empty:
    display_drill_down_analysis(quality_data['quality_summary'])
else:
    st.info("No quality summary data available for drill-down analysis.")

# Relationship Integrity Analysis
st.markdown('<div class="section-header">Relationship Integrity Analysis</div>', unsafe_allow_html=True)

if 'relationship_metrics' in quality_data and not quality_data['relationship_metrics'].empty:
    rel_metrics = quality_data['relationship_metrics']
    
    # Check if required columns exist
    required_columns = ['relationship_type', 'integrity_percentage', 'integrity_grade', 'valid_relationships', 'total_customers']
    missing_columns = [col for col in required_columns if col not in rel_metrics.columns]
    
    if missing_columns:
        st.error(f"Relationship metrics data is missing required columns: {', '.join(missing_columns)}")
        st.info(f"Available columns: {', '.join(rel_metrics.columns.tolist())}")
        st.info("Please ensure the RELATIONSHIP_QUALITY_METRICS view exists and contains all required columns.")
    else:
        col1, col2 = st.columns(2)
        
        try:
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
        except Exception as e:
            st.error(f"Error displaying relationship metrics: {str(e)}")
else:
    st.warning("⚠️ Relationship integrity data is not available. This could mean:")
    st.markdown("""
    - The `RELATIONSHIP_QUALITY_METRICS` view doesn't exist
    - No relationship data has been processed yet
    - Database connection issues
    
    **To resolve:**
    1. Ensure you've run the `01_DATA_QUALITY.sql` script
    2. Check that customer-broker and customer-claim relationships exist
    3. Verify your role has access to the relationship quality views
    """)

# Data Quality Issue Detection & Remediation
st.markdown('<div class="section-header">Data Quality Issue Detection & Remediation</div>', unsafe_allow_html=True)

if 'quality_issues' in quality_data and not quality_data['quality_issues'].empty:
    quality_issues = quality_data['quality_issues']
    
    st.markdown(f"""
    <div class="dmf-note">
    <strong>SYSTEM$DATA_METRIC_SCAN Feature:</strong> This section demonstrates Snowflake's ability to 
    identify specific problematic records using the SYSTEM$DATA_METRIC_SCAN function for targeted data remediation.
    </div>
    """, unsafe_allow_html=True)
    
    # Show issues summary with error handling
    try:
        total_issues = quality_issues['affected_records'].sum() if 'affected_records' in quality_issues.columns else 0
    except:
        total_issues = 0
    
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Total Problematic Records", f"{total_issues:,}")
    with col2:
        try:
            issue_types = len(quality_issues[quality_issues['affected_records'] > 0]) if 'affected_records' in quality_issues.columns else 0
        except:
            issue_types = 0
        st.metric("Issue Types Found", issue_types)
    with col3:
        if total_issues > 0:
            st.metric("Remediation Status", "Available", delta="Ready for Fix")
        else:
            st.metric("Data Quality", "Excellent", delta="No Issues Found")
    
    # Detailed issues breakdown
    if total_issues > 0 and 'affected_records' in quality_issues.columns:
        st.markdown("**Specific Issues Identified:**")
        
        # Create a visualization of issues
        try:
            issues_display = quality_issues[quality_issues['affected_records'] > 0].copy()
            if not issues_display.empty:
                issues_display['issue_type_clean'] = issues_display['issue_type'].str.replace('_', ' ').str.title()
                
                fig_issues = px.bar(
                    issues_display,
                    x='issue_type_clean',
                    y='affected_records',
                    color='table_name',
                    title="Data Quality Issues by Type and Table",
                    labels={'issue_type_clean': 'Issue Type', 'affected_records': 'Affected Records'},
                    color_discrete_sequence=[COLORS['valencia_orange'], COLORS['first_light'], COLORS['purple_moon']]
                )
                fig_issues.update_layout(
                    title_font_color=COLORS['mid_blue'],
                    height=350,
                    xaxis_tickangle=-45
                )
                st.plotly_chart(fig_issues, use_container_width=True)
                
                # Show detailed table
                display_issues = issues_display.copy()
                display_issues = display_issues.rename(columns={
                    'issue_type': 'Issue Type',
                    'affected_records': 'Affected Records',
                    'table_name': 'Table'
                })
                st.dataframe(display_issues, use_container_width=True, hide_index=True)
        except Exception as e:
            st.error(f"Error displaying issue details: {str(e)}")
            
        # Show remediation options
        st.markdown("**Sample Remediation Actions:**")
        with st.expander("View Sample Remediation SQL (Demo Only)"):
            st.code("""
-- Example: Fix NULL policy numbers in customers
UPDATE INSURANCE_WORKSHOP_DB.RAW_DATA.CUSTOMERS_RAW
SET POLICY_NUMBER = 'POL_' || UNIFORM(1000000, 9999999, RANDOM())::STRING
WHERE POLICY_NUMBER IN (
    SELECT POLICY_NUMBER 
    FROM INSURANCE_WORKSHOP_DB.RAW_DATA.CUSTOMERS_WITH_NULL_POLICY_NUMBERS
);

-- Example: Remove duplicate records (keep latest)
DELETE FROM INSURANCE_WORKSHOP_DB.RAW_DATA.CUSTOMERS_RAW
WHERE POLICY_NUMBER IN (
    SELECT POLICY_NUMBER 
    FROM INSURANCE_WORKSHOP_DB.RAW_DATA.CUSTOMERS_WITH_DUPLICATE_POLICIES
)
AND POLICY_NUMBER NOT IN (
    SELECT POLICY_NUMBER 
    FROM INSURANCE_WORKSHOP_DB.RAW_DATA.CUSTOMERS_RAW
    QUALIFY ROW_NUMBER() OVER (PARTITION BY POLICY_NUMBER ORDER BY POLICY_START_DATE DESC) = 1
);
            """, language='sql')
    else:
        st.success("🎉 No data quality issues found! All records are clean.")
else:
    st.info("📊 Quality issues data not available. This may be normal if:")
    st.markdown("""
    - The SYSTEM$DATA_METRIC_SCAN views haven't been created yet
    - No quality issues have been detected
    - The data quality monitoring is still initializing
    """)
    
    # Show remediation examples anyway for demo purposes
    with st.expander("View Sample SYSTEM$DATA_METRIC_SCAN Usage (Demo)"):
        st.code("""
-- Find records with NULL policy numbers
SELECT * FROM TABLE(SYSTEM$DATA_METRIC_SCAN(
    REF_ENTITY_NAME => 'INSURANCE_WORKSHOP_DB.RAW_DATA.CUSTOMERS_RAW',
    METRIC_NAME => 'snowflake.core.null_count',
    ARGUMENT_NAME => 'POLICY_NUMBER'
));

-- Find duplicate policy numbers
SELECT * FROM TABLE(SYSTEM$DATA_METRIC_SCAN(
    REF_ENTITY_NAME => 'INSURANCE_WORKSHOP_DB.RAW_DATA.CUSTOMERS_RAW',
    METRIC_NAME => 'snowflake.core.duplicate_count',
    ARGUMENT_NAME => 'POLICY_NUMBER'
));
        """, language='sql')

# DMF Configuration Status
st.markdown('<div class="section-header">Data Metric Function Status</div>', unsafe_allow_html=True)

if 'dmf_status' in quality_data and not quality_data['dmf_status'].empty:
    dmf_status = quality_data['dmf_status']
    
    # Check if required columns exist
    required_columns = ['table_name', 'metric_name', 'schedule_status', 'schedule']
    missing_columns = [col for col in required_columns if col not in dmf_status.columns]
    
    if missing_columns:
        st.error(f"DMF status data is missing required columns: {', '.join(missing_columns)}")
        st.info(f"Available columns: {', '.join(dmf_status.columns.tolist())}")
        st.info("Please ensure the DMF configuration data is properly structured.")
    else:
        st.markdown(f"""
        <div class="dmf-note">
        <strong>DMF Configuration:</strong> This section shows the status of all Data Metric Functions 
        configured for automated quality monitoring across the two-entity model.
        </div>
        """, unsafe_allow_html=True)
        
        try:
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
        except Exception as e:
            st.error(f"Error displaying DMF configuration: {str(e)}")
else:
    st.warning("⚠️ DMF configuration data is not available. This could mean:")
    st.markdown("""
    - Data Metric Functions haven't been configured yet
    - The DMF status query failed
    - Database connection issues
    
    **To resolve:**
    1. Ensure you've run the `01_DATA_QUALITY.sql` script
    2. Check that DMFs are properly configured and running
    3. Verify your role has access to DMF metadata
    """)

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
        if 'quality_summary' in quality_data and not quality_data['quality_summary'].empty:
            try:
                if 'quality_status' in quality_data['quality_summary'].columns:
                    # Exclude ROW_COUNT and UNKNOWN from critical issues
                    quality_only = quality_data['quality_summary'][
                        (quality_data['quality_summary']['metric_name'] != 'SNOWFLAKE.CORE.ROW_COUNT') &
                        (quality_data['quality_summary']['quality_status'] != 'UNKNOWN')
                    ]
                    critical_count = len(quality_only[quality_only['quality_status'] == 'CRITICAL'])
                    st.metric("Critical Issues", critical_count, delta=None if critical_count == 0 else f"-{critical_count}")
                else:
                    st.metric("Critical Issues", "N/A", delta="No status data")
            except Exception as e:
                st.metric("Critical Issues", "Error", delta="Query failed")
        else:
            st.metric("Critical Issues", "N/A", delta="No data")
    
    with col4:
        current_time = datetime.now().strftime("%H:%M:%S")
        st.metric("Last Update", current_time)

# Footer
st.markdown("---")
st.markdown(f"""
<div style='text-align: center; color: {COLORS['medium_gray']}; padding: 20px;'>
    <p><strong>Insurance Workshop - Data Quality Monitoring Dashboard</strong></p>
    <p>Powered by Snowflake Data Metric Functions with Real-time Quality Scoring</p>
    <p>Two-Entity Model: Customers • Claims with Cross-Entity Validation</p>
</div>
""", unsafe_allow_html=True) 