"""
GA-SF Mapping Streamlit Application
Main application file with auth, tabs for GA-SF and NE mapped data, BQ reset with password
"""
import streamlit as st
import pandas as pd
from datetime import datetime, timedelta

from modules.config import SF_REQUIRED_COLUMNS, BQ_TABLE_GA_SF, BQ_TABLE_GA_SF_NE
from modules.ga4_client import get_ga4_client
from modules.data_processor import process_ga_data, map_ne_data, get_bimonth_date_range
from modules.bigquery_manager import upload_ga_sf_data, upload_ga_sf_ne_data, reset_table
from modules.auth import Authenticator
from modules.email_alerts import send_security_alert


# Page Configuration
st.set_page_config(
    page_title="GA-SF Data Mapping Dashboard",
    page_icon="📊",
    layout="wide"
)

# Custom CSS for dark theme compatible tabs and metric cards
st.markdown("""
<style>
    /* Metric cards */
    .metric-card {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 20px;
        border-radius: 15px;
        color: white;
        text-align: center;
        box-shadow: 0 4px 15px rgba(0, 0, 0, 0.2);
    }
    .metric-value {
        font-size: 36px;
        font-weight: bold;
        margin: 10px 0;
    }
    .metric-label {
        font-size: 14px;
        opacity: 0.9;
        text-transform: uppercase;
        letter-spacing: 1px;
    }
    
    /* Dark theme compatible tabs */
    .stTabs [data-baseweb="tab-list"] {
        gap: 8px;
        background-color: rgba(255, 255, 255, 0.05);
        padding: 10px;
        border-radius: 10px;
    }
    .stTabs [data-baseweb="tab"] {
        height: 50px;
        padding-left: 25px;
        padding-right: 25px;
        background-color: rgba(255, 255, 255, 0.1);
        border-radius: 8px;
        color: inherit;
        font-weight: 500;
        border: 1px solid rgba(255, 255, 255, 0.1);
    }
    .stTabs [aria-selected="true"] {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white !important;
        border: none;
    }
    .stTabs [data-baseweb="tab"]:hover {
        background-color: rgba(255, 255, 255, 0.15);
    }
    .stTabs [aria-selected="true"]:hover {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
    }
    
    /* Tab panel styling */
    .stTabs [data-baseweb="tab-panel"] {
        padding-top: 20px;
    }
</style>
""", unsafe_allow_html=True)


def display_metric_card(label: str, value, color: str = "#667eea"):
    """Display a styled metric card"""
    st.markdown(f"""
        <div style="
            background: linear-gradient(135deg, {color} 0%, {color}99 100%);
            padding: 20px;
            border-radius: 15px;
            color: white;
            text-align: center;
            box-shadow: 0 4px 15px rgba(0, 0, 0, 0.2);
            margin-bottom: 10px;
        ">
            <div style="font-size: 14px; opacity: 0.9; text-transform: uppercase; letter-spacing: 1px;">
                {label}
            </div>
            <div style="font-size: 36px; font-weight: bold; margin: 10px 0;">
                {value:,}
            </div>
        </div>
    """, unsafe_allow_html=True)


def display_percentage_card(label: str, value: float, color: str = "#10b981"):
    """Display a styled percentage metric card"""
    st.markdown(f"""
        <div style="
            background: linear-gradient(135deg, {color} 0%, {color}99 100%);
            padding: 20px;
            border-radius: 15px;
            color: white;
            text-align: center;
            box-shadow: 0 4px 15px rgba(0, 0, 0, 0.2);
            margin-bottom: 10px;
        ">
            <div style="font-size: 14px; opacity: 0.9; text-transform: uppercase; letter-spacing: 1px;">
                {label}
            </div>
            <div style="font-size: 36px; font-weight: bold; margin: 10px 0;">
                {value:.1f}%
            </div>
        </div>
    """, unsafe_allow_html=True)


def apply_filters(df: pd.DataFrame, filters: dict) -> pd.DataFrame:
    """Apply filters to DataFrame"""
    filtered_df = df.copy()
    
    # Date filter
    if filters.get('start_date') and filters.get('end_date'):
        filtered_df['Date'] = pd.to_datetime(filtered_df['Date'])
        start = pd.Timestamp(filters['start_date'])
        end = pd.Timestamp(filters['end_date'])
        filtered_df = filtered_df[(filtered_df['Date'] >= start) & (filtered_df['Date'] <= end)]
    
    # Campaign filter (multiselect)
    if filters.get('campaigns') and 'Final_Source' in filtered_df.columns:
        if len(filters['campaigns']) > 0:
            filtered_df = filtered_df[filtered_df['Final_Source'].isin(filters['campaigns'])]
    
    # Source Medium filter (multiselect)
    if filters.get('source_mediums') and 'Source_Medium' in filtered_df.columns:
        if len(filters['source_mediums']) > 0:
            filtered_df = filtered_df[filtered_df['Source_Medium'].isin(filters['source_mediums'])]
    
    # Operating System filter (multiselect)
    if filters.get('operating_systems') and 'Operating_System' in filtered_df.columns:
        if len(filters['operating_systems']) > 0:
            filtered_df = filtered_df[filtered_df['Operating_System'].isin(filters['operating_systems'])]
    
    # Shifting Type filter (multiselect)
    if filters.get('shifting_types') and 'Shifting_Type' in filtered_df.columns:
        if len(filters['shifting_types']) > 0:
            filtered_df = filtered_df[filtered_df['Shifting_Type'].isin(filters['shifting_types'])]
    
    return filtered_df


def calculate_metrics(df: pd.DataFrame) -> dict:
    """Calculate metrics from DataFrame"""
    metrics = {
        'total_leads': 0,
        'total_conversions': 0,
        'conversion_rate': 0.0,
        'not_found_count': 0
    }
    
    if df.empty:
        return metrics
    
    # Total unique leads (mobile numbers)
    metrics['total_leads'] = df['Mobile'].nunique()
    
    # Total conversions (Status = Converted)
    if 'Status' in df.columns:
        metrics['total_conversions'] = df[df['Status'] == 'Converted']['Mobile'].nunique()
        metrics['not_found_count'] = df[df['Status'] == 'Not Found']['Mobile'].nunique()
    
    # Conversion rate
    if metrics['total_leads'] > 0:
        metrics['conversion_rate'] = (metrics['total_conversions'] / metrics['total_leads']) * 100
    
    return metrics


def handle_reset_with_password(table_name: str, table_display_name: str):
    """Handle BQ table reset with password protection and email alerts"""
    user_email = st.session_state.get('user_email', 'Unknown')
    
    # Initialize attempt counter for this table
    attempt_key = f'reset_attempts_{table_name}'
    if attempt_key not in st.session_state:
        st.session_state[attempt_key] = 0
    
    max_attempts = st.secrets.get("security", {}).get("max_password_attempts", 3)
    
    # Check if locked out
    if st.session_state[attempt_key] >= max_attempts:
        st.error("🔒 You have been locked out due to too many failed attempts. Contact administrator.")
        return
    
    # Password input
    password = st.text_input(
        f"Enter reset password for {table_display_name}:", 
        type="password",
        key=f"reset_pwd_{table_name}"
    )
    
    col1, col2 = st.columns(2)
    with col1:
        if st.button(f"🔴 Confirm Reset {table_display_name}", key=f"confirm_reset_{table_name}"):
            correct_password = st.secrets.get("security", {}).get("reset_password", "")
            
            if password == correct_password:
                # Reset successful
                st.session_state[attempt_key] = 0
                with st.spinner(f"Resetting {table_display_name}..."):
                    success, message = reset_table(table_name)
                    if success:
                        st.success(f"✅ {message}")
                    else:
                        st.error(message)
            else:
                # Wrong password
                st.session_state[attempt_key] += 1
                remaining = max_attempts - st.session_state[attempt_key]
                
                st.error(f"❌ Incorrect password. {remaining} attempts remaining.")
                
                # Send security alert
                send_security_alert(
                    user_email=user_email,
                    attempt_count=st.session_state[attempt_key],
                    max_attempts_reached=(st.session_state[attempt_key] >= max_attempts)
                )
                
                if st.session_state[attempt_key] >= max_attempts:
                    st.error("🔒 Maximum attempts reached. You have been locked out.")
                    st.rerun()
    
    with col2:
        if st.button("Cancel", key=f"cancel_reset_{table_name}"):
            st.rerun()


def main():
    # Initialize authenticator
    auth = Authenticator()
    
    # Check authentication using Streamlit's built-in auth
    if not auth.is_authenticated():
        auth.show_login_page()
        return
    
    # Get user email and check domain access
    user_email = auth.get_user_email()
    if not auth.check_email_access(user_email):
        st.error(f"❌ Access denied for {user_email}")
        st.warning("Your email domain is not authorized to use this application.")
        st.button("🚪 Logout", on_click=st.logout)
        return
    
    # Store user email in session state for security alerts
    st.session_state.user_email = user_email
    
    # Show user info in sidebar
    auth.show_user_info()
    
    st.title("📊 GA-SF Data Mapping Dashboard")
    st.markdown("---")
    
    # Initialize session state
    if 'ga_sf_data' not in st.session_state:
        st.session_state.ga_sf_data = None
    if 'ga_sf_ne_data' not in st.session_state:
        st.session_state.ga_sf_ne_data = None
    if 'data_loaded' not in st.session_state:
        st.session_state.data_loaded = False
    if 'show_reset_ga_sf' not in st.session_state:
        st.session_state.show_reset_ga_sf = False
    if 'show_reset_ne' not in st.session_state:
        st.session_state.show_reset_ne = False
    
    # Sidebar
    with st.sidebar:
        st.header("📅 Date Range")
        
        default_end = datetime.now() - timedelta(days=1)
        default_start = default_end - timedelta(days=30)
        
        col1, col2 = st.columns(2)
        with col1:
            start_date = st.date_input("Start Date", value=default_start, max_value=datetime.now())
        with col2:
            end_date = st.date_input("End Date", value=default_end, max_value=datetime.now())
        
        st.markdown("---")
        st.header("📁 File Uploads")
        
        # Salesforce file upload
        sf_file = st.file_uploader(
            "Upload Salesforce CSV", 
            type=['csv'],
            help="Required columns: " + ", ".join(SF_REQUIRED_COLUMNS)
        )
        
        # NE file upload (optional)
        ne_file = st.file_uploader(
            "Upload NE Data (Optional)", 
            type=['csv'],
            help="File with Mobile/LEAD_MOBILE, LEAD_TYPE, Revenue columns"
        )
        
        st.markdown("---")
        
        # Fetch Data button
        if st.button("🔄 Fetch & Process Data", type="primary", width="stretch"):
            if sf_file is None:
                st.error("Please upload Salesforce CSV file first!")
            elif start_date > end_date:
                st.error("End date must be after start date!")
            else:
                with st.spinner("Fetching GA4 data..."):
                    try:
                        # Load Salesforce data
                        sf_df = pd.read_csv(sf_file, encoding='latin1')
                        
                        # Validate SF columns
                        missing_cols = [col for col in SF_REQUIRED_COLUMNS if col not in sf_df.columns]
                        if missing_cols:
                            st.error(f"Missing required columns in Salesforce file: {missing_cols}")
                            st.stop()
                        
                        # Fetch GA4 data for expanded bi-month range
                        ga_client = get_ga4_client()
                        expanded_start, expanded_end = get_bimonth_date_range(
                            start_date.strftime('%Y-%m-%d'),
                            end_date.strftime('%Y-%m-%d')
                        )
                        ga_data = ga_client.fetch_data(expanded_start, expanded_end)
                        
                        if not ga_data:
                            st.warning("No GA4 data found for the selected date range.")
                            st.stop()
                        
                        # Process GA data and map Salesforce
                        st.session_state.ga_sf_data = process_ga_data(ga_data, sf_df)
                        
                        # Process NE data if uploaded
                        if ne_file is not None:
                            ne_df = pd.read_csv(ne_file, encoding='latin1')
                            # Handle LEAD_MOBILE column name
                            if 'LEAD_MOBILE' in ne_df.columns and 'Mobile' not in ne_df.columns:
                                ne_df = ne_df.rename(columns={'LEAD_MOBILE': 'Mobile'})
                            if 'Mobile' in ne_df.columns:
                                st.session_state.ga_sf_ne_data = map_ne_data(
                                    st.session_state.ga_sf_data, 
                                    ne_df
                                )
                            else:
                                st.warning("NE file must contain 'Mobile' or 'LEAD_MOBILE' column for mapping.")
                        
                        st.session_state.data_loaded = True
                        st.success("Data processed successfully!")
                        st.rerun()
                        
                    except Exception as e:
                        st.error(f"Error processing data: {str(e)}")
        
        # Separate button to process NE data (when GA-SF is already loaded)
        if st.session_state.data_loaded and st.session_state.ga_sf_data is not None:
            if st.button("📁 Map NE Data", width="stretch"):
                if ne_file is None:
                    st.error("Please upload NE data file first!")
                else:
                    with st.spinner("Mapping NE data..."):
                        try:
                            ne_df = pd.read_csv(ne_file, encoding='latin1')
                            # Handle LEAD_MOBILE column name
                            if 'LEAD_MOBILE' in ne_df.columns and 'Mobile' not in ne_df.columns:
                                ne_df = ne_df.rename(columns={'LEAD_MOBILE': 'Mobile'})
                            if 'Mobile' in ne_df.columns:
                                st.session_state.ga_sf_ne_data = map_ne_data(
                                    st.session_state.ga_sf_data, 
                                    ne_df
                                )
                                st.success("NE data mapped successfully!")
                                st.rerun()
                            else:
                                st.error("NE file must contain 'Mobile' or 'LEAD_MOBILE' column for mapping.")
                        except Exception as e:
                            st.error(f"Error mapping NE data: {str(e)}")
        
        # Clear cache button
        if st.button("🗑️ Clear Cache", width="stretch"):
            st.cache_data.clear()
            st.cache_resource.clear()
            st.session_state.ga_sf_data = None
            st.session_state.ga_sf_ne_data = None
            st.session_state.data_loaded = False
            st.rerun()
    
    # Main Content
    if not st.session_state.data_loaded or st.session_state.ga_sf_data is None:
        st.info("👈 Upload Salesforce data and click 'Fetch & Process Data' to begin.")
        return
    
    # Filters
    st.subheader("🔍 Filters")
    
    df = st.session_state.ga_sf_data
    
    # Date filters
    filter_row1 = st.columns(4)
    with filter_row1[0]:
        filter_start = st.date_input("Filter Start", value=start_date, key="filter_start")
    with filter_row1[1]:
        filter_end = st.date_input("Filter End", value=end_date, key="filter_end")
    
    # Multiselect filters
    filter_row2 = st.columns(4)
    with filter_row2[0]:
        campaigns = sorted(df['Final_Source'].dropna().unique().tolist())
        selected_campaigns = st.multiselect("Campaign/Source", campaigns, default=[])
    
    with filter_row2[1]:
        source_mediums = sorted(df['Source_Medium'].dropna().unique().tolist())
        selected_source_mediums = st.multiselect("Source/Medium", source_mediums, default=[])
    
    with filter_row2[2]:
        operating_systems = sorted(df['Operating_System'].dropna().unique().tolist())
        selected_os = st.multiselect("Operating System", operating_systems, default=[])
    
    with filter_row2[3]:
        shifting_types = sorted(df['Shifting_Type'].dropna().unique().tolist())
        selected_shifting = st.multiselect("Shifting Type", shifting_types, default=[])
    
    # Apply filters
    filters = {
        'start_date': filter_start,
        'end_date': filter_end,
        'campaigns': selected_campaigns,
        'source_mediums': selected_source_mediums,
        'operating_systems': selected_os,
        'shifting_types': selected_shifting
    }
    
    filtered_df = apply_filters(df, filters)
    
    # Metrics Cards
    st.markdown("---")
    metrics = calculate_metrics(filtered_df)
    
    metric_cols = st.columns(4)
    with metric_cols[0]:
        display_metric_card("Total Leads", metrics['total_leads'], "#667eea")
    with metric_cols[1]:
        display_metric_card("Conversions", metrics['total_conversions'], "#10b981")
    with metric_cols[2]:
        display_percentage_card("Conversion Rate", metrics['conversion_rate'], "#f59e0b")
    with metric_cols[3]:
        display_metric_card("Not Found in SF", metrics['not_found_count'], "#ef4444")
    
    st.markdown("---")
    
    # Tabs
    tab1, tab2 = st.tabs(["📊 GA-SF Mapped Data", "📁 NE Mapped Data"])
    
    with tab1:
        st.subheader("GA-SF Mapped Data")
        
        if filtered_df.empty:
            st.warning("No data available for the selected filters.")
        else:
            # Data table
            st.dataframe(filtered_df, width="stretch", height=400)
            
            # Action buttons
            col1, col2, col3 = st.columns([1, 1, 1])
            
            with col1:
                st.download_button(
                    label="📥 Download CSV",
                    data=filtered_df.to_csv(index=False),
                    file_name=f"GA_SF_Mapped_{datetime.now().strftime('%Y%m%d')}.csv",
                    mime="text/csv",
                    width="stretch"
                )
            
            with col2:
                if st.button("☁️ Upload to BigQuery", key="upload_ga_sf", width="stretch"):
                    with st.spinner("Uploading to BigQuery..."):
                        success, status_updates, message = upload_ga_sf_data(st.session_state.ga_sf_data)
                        if success:
                            st.success(message)
                            if status_updates > 0:
                                st.info(f"🔄 {status_updates} status updates detected and applied.")
                        else:
                            st.error(message)
            
            with col3:
                if st.button("🔴 Reset BQ Table", key="reset_ga_sf_btn", width="stretch"):
                    st.session_state.show_reset_ga_sf = True
            
            # Show reset dialog
            if st.session_state.show_reset_ga_sf:
                st.warning("⚠️ This will DELETE all existing data in the GA-SF table!")
                handle_reset_with_password(BQ_TABLE_GA_SF, "GA-SF Table")
    
    with tab2:
        st.subheader("NE Mapped Data")
        
        if st.session_state.ga_sf_ne_data is None:
            st.info("No NE data uploaded. Upload NE data file in the sidebar and click 'Map NE Data'.")
        else:
            ne_filtered = apply_filters(st.session_state.ga_sf_ne_data, filters)
            
            if ne_filtered.empty:
                st.warning("No data available for the selected filters.")
            else:
                # Data table
                st.dataframe(ne_filtered, width="stretch", height=400)
                
                # Action buttons
                col1, col2, col3 = st.columns([1, 1, 1])
                
                with col1:
                    st.download_button(
                        label="📥 Download CSV",
                        data=ne_filtered.to_csv(index=False),
                        file_name=f"GA_SF_NE_Mapped_{datetime.now().strftime('%Y%m%d')}.csv",
                        mime="text/csv",
                        width="stretch"
                    )
                
                with col2:
                    if st.button("☁️ Upload to BigQuery", key="upload_ga_sf_ne", width="stretch"):
                        with st.spinner("Uploading to BigQuery..."):
                            success, status_updates, message = upload_ga_sf_ne_data(st.session_state.ga_sf_ne_data)
                            if success:
                                st.success(message)
                                if status_updates > 0:
                                    st.info(f"🔄 {status_updates} status updates detected and applied.")
                            else:
                                st.error(message)
                
                with col3:
                    if st.button("🔴 Reset BQ Table", key="reset_ne_btn", width="stretch"):
                        st.session_state.show_reset_ne = True
                
                # Show reset dialog
                if st.session_state.show_reset_ne:
                    st.warning("⚠️ This will DELETE all existing data in the NE table!")
                    handle_reset_with_password(BQ_TABLE_GA_SF_NE, "NE Table")


if __name__ == "__main__":
    main()
