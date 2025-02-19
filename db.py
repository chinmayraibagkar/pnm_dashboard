import streamlit as st
from google.oauth2.service_account import Credentials
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import DateRange, Metric, Dimension, RunReportRequest
import pandas as pd
from datetime import datetime, timedelta
from functools import lru_cache
import concurrent.futures
import re
import os
import json
from google.ads.googleads.client import GoogleAdsClient
import requests
from io import StringIO
import time
import tempfile

# Configure page settings
st.set_page_config(page_title="Google Analytics Dashboard", layout="wide")
st.title("Google Analytics 4 Data Dashboard")

PROPERTY_ID = st.secrets["ga_property"]["property_id"]
SCOPES = ['https://www.googleapis.com/auth/analytics.readonly']
BATCH_SIZE = 100000
TOKEN = st.secrets["facebook"]["access_token"]

@st.cache_resource
def initialize_ga4_client():
    """Initialize and return GA4 client with proper authentication"""
    try:
        credentials = {
            "type": st.secrets["gcp_service_account"]["type"],
            "project_id": st.secrets["gcp_service_account"]["project_id"],
            "private_key_id": st.secrets["gcp_service_account"]["private_key_id"],
            "private_key": st.secrets["gcp_service_account"]["private_key"],
            "client_email": st.secrets["gcp_service_account"]["client_email"],
            "client_id": st.secrets["gcp_service_account"]["client_id"],
            "auth_uri": st.secrets["gcp_service_account"]["auth_uri"],
            "token_uri": st.secrets["gcp_service_account"]["token_uri"],
            "auth_provider_x509_cert_url": st.secrets["gcp_service_account"]["auth_provider_x509_cert_url"],
            "client_x509_cert_url": st.secrets["gcp_service_account"]["client_x509_cert_url"],
            "universe_domain": st.secrets["gcp_service_account"]["universe_domain"]
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as temp:
            json.dump(credentials, temp)
            temp_path = temp.name

        credentials = Credentials.from_service_account_file(temp_path, scopes=SCOPES)
        return BetaAnalyticsDataClient(credentials=credentials)
    except Exception as e:
        st.error(f"Failed to initialize GA4 client: {str(e)}")
        return None


def create_request(property_id, start_date, end_date, offset=0):
    """Create a RunReportRequest object"""
    return RunReportRequest(
        property=f"properties/{property_id}",
        date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
        dimensions=[
            Dimension(name="customEvent:PnM_parameter"),
            Dimension(name="date"),
            Dimension(name="firstUserCampaignName"),
            Dimension(name="sessionSource"),
            Dimension(name="sessionMedium"),
            Dimension(name="sessionCampaignName"),
            Dimension(name="customEvent:GTES_mobile"),
        ],
        metrics=[
            Metric(name="sessions"),
            Metric(name="engagedSessions"),
        ],
        offset=offset,
        limit=BATCH_SIZE
    )


def extract_mobile_numbers(df, column_name):
    # Regular expression to match a 10-digit mobile number
    pattern = r'\d{10}'
    
    # Extracting mobile numbers from the specified column
    df['Mobile'] = df[column_name].astype(str).apply(lambda x: re.search(pattern, x).group() if re.search(pattern, x) else None)
    
    return df


def process_response_batch(batch):
    """Process a batch of GA4 response rows into a list of dictionaries"""
    return [{
        'PnM Parameter': row.dimension_values[0].value,
        'Date': datetime.strptime(row.dimension_values[1].value, '%Y%m%d').strftime('%Y-%m-%d'),
        'First User Campaign': row.dimension_values[2].value,
        'Source': row.dimension_values[3].value,
        'Medium': row.dimension_values[4].value,
        'Session Campaign': row.dimension_values[5].value,
        'Sessions': int(row.metric_values[0].value),
        'Engaged Sessions': int(row.metric_values[1].value)
    } for row in batch]


@st.cache_data(ttl=86400)  # Use cache_data for serializable objects (e.g., dataframes)
def fetch_ga4_data(_client, start_date, end_date):  # Prefix client with underscore
    """
    Fetch data from GA4 with specified metrics and dimensions
    Returns a pandas DataFrame with the requested data
    """
    try:
        all_rows = []
        offset = 0
        limit = 100000  # Maximum allowed by API per request

        while True:
            request = {
                "property": f"properties/{PROPERTY_ID}",
                "date_ranges": [DateRange(start_date=start_date, end_date=end_date)],
                "dimensions": [
                    Dimension(name="customEvent:PnM_parameter"),
                    Dimension(name="date"),
                    Dimension(name="firstUserCampaignName"),
                    Dimension(name="sessionSource"),
                    Dimension(name="sessionMedium"),
                    Dimension(name="sessionCampaignName"),
                    Dimension(name="customEvent:GTES_mobile"),
                ],
                "metrics": [
                    Metric(name="sessions"),
                    Metric(name="engagedSessions"),
                ],
                "offset": offset,
                "limit": limit
            }

            response = _client.run_report(request)  # Use _client instead of client
            current_rows = response.rows
            
            if not current_rows:
                break
                
            all_rows.extend(current_rows)
            
            # Check if we've received all available rows
            if len(current_rows) < limit:
                break
                
            offset += limit

        # Process the response into a DataFrame
        data = []
        for row in all_rows:
            data.append({
                'PnM Parameter': row.dimension_values[0].value,
                'Date': datetime.strptime(row.dimension_values[1].value, '%Y%m%d').strftime('%Y-%m-%d'),
                'First User Campaign': row.dimension_values[2].value,
                'Source': row.dimension_values[3].value,
                'Medium': row.dimension_values[4].value,
                'Session Campaign': row.dimension_values[5].value,
                'Sessions': int(row.metric_values[0].value),
                'Engaged Sessions': int(row.metric_values[1].value)
            })
        
        # Create DataFrame and combine source/medium
        df = pd.DataFrame(data)
        if not df.empty:
            df['Source/Medium'] = df['Source'] + ' / ' + df['Medium']
            df = df.drop(['Source', 'Medium'], axis=1)
            column_order = [
                'PnM Parameter', 'Date', 'First User Campaign', 
                'Sessions', 'Source/Medium', 'Session Campaign', 'Engaged Sessions'
            ]
            df = df[column_order]
        
        return df
    
    except Exception as e:
        st.error(f"Error fetching GA4 data: {str(e)}")
        return None


@st.cache_data(ttl=86400)  # Cache for 1 day
def remove_duplicates(df):
    """Remove duplicates within a 60-day window, keeping the oldest occurrence first"""
    df['Date'] = pd.to_datetime(df['Date'])
    df = df.sort_values(['Mobile', 'Date'], ascending=[True, True])  # Sort by PnM Parameter and Date (oldest first)

    filtered_rows = []  # List to store filtered rows
    seen_dates = {}  # Dictionary to track last kept date for each 'PnM Parameter'

    for _, row in df.iterrows():
        pnm = row['Mobile']
        date = row['Date']

        if pnm not in seen_dates or (date - seen_dates[pnm]).days > 60:
            filtered_rows.append(row)
            seen_dates[pnm] = date  # Update last kept date

    df_filtered = pd.DataFrame(filtered_rows)
    return df_filtered.sort_values('Date').reset_index(drop=True)  # Sort back to chronological order


def get_google_ads_client():
    # Get credentials from Streamlit secrets
    credentials = {
        'developer_token': st.secrets['google_ads']['developer_token'],
        'client_id': st.secrets['google_ads']['client_id'],
        'client_secret': st.secrets['google_ads']['client_secret'],
        'refresh_token': st.secrets['google_ads']['refresh_token'],
        'login_customer_id': st.secrets['google_ads']['login_customer_id'],
        'use_proto_plus': st.secrets['google_ads']['use_proto_plus'],
    }
    
    # Create a temporary yaml file
    with open('temp_credentials.yaml', 'w') as f:
        import yaml
        yaml.dump(credentials, f)
    
    # Load client from temporary file
    client = GoogleAdsClient.load_from_storage('temp_credentials.yaml')
    
    # Remove temporary file
    import os
    os.remove('temp_credentials.yaml')
    
    return client


def get_google_ads_data(client, customer_id, start_date, end_date):
    ga_service = client.get_service("GoogleAdsService", version="v17")

    # Constructing the query
    query = f"""
    SELECT
        segments.date,
        campaign.name,
        metrics.clicks,
        metrics.impressions,
        metrics.cost_micros,
        metrics.conversions
    FROM
        campaign
    WHERE
        segments.date BETWEEN '{start_date}' AND '{end_date}'
    """

    response = ga_service.search_stream(customer_id=customer_id, query=query)

    data = []
    for batch in response:
        for row in batch.results:
            data.append({
                "Date": row.segments.date if hasattr(row.segments, 'date') else 'NA',
                "Campaign Name": row.campaign.name if hasattr(row.campaign, 'name') else 'NA',
                "clicks": row.metrics.clicks if hasattr(row.metrics, 'clicks') else 'NA',
                "impressions": row.metrics.impressions if hasattr(row.metrics, 'impressions') else 'NA',
                "cost": row.metrics.cost_micros / 1e6 if hasattr(row.metrics, 'cost_micros') else 'NA', # Converting micros to standard currency unit
                "conversions": row.metrics.conversions if hasattr(row.metrics, 'conversions') else 'NA'
            })

    return pd.DataFrame(data)

@st.cache_data(ttl=86400)  # Cache for 1 day
def get_facebook_data(start_date, end_date):
    # Validate date format
    try:
        datetime.strptime(start_date, '%Y-%m-%d')
        datetime.strptime(end_date, '%Y-%m-%d')
    except ValueError:
        print("Error: Dates must be in YYYY-MM-DD format")
        return

    url = "https://graph.facebook.com/v18.0/act_547569015598645/insights"
    access_token = st.secrets["facebook"]["access_token"]
    
    all_data = []
    total_records = 0

    params = {
        "level": "adset",
        "fields": "account_name,campaign_name,adset_name,impressions,clicks,spend",
        "time_range[since]": start_date,
        "time_range[until]": end_date,
        "time_increment": 1,
        "access_token": access_token,
        "limit": 500
    }

    while True:
        response = requests.get(url, params=params)

        if response.status_code == 200:
            data = response.json()
            if "data" in data:
                all_data.extend(data["data"])
                total_records += len(data["data"])
                print(f"Fetched {len(data['data'])} records. Total so far: {total_records}")
                time.sleep(1)
            else:
                print("No data found.")
                break

            if "paging" in data and "next" in data["paging"]:
                url = data["paging"]["next"]
            else:
                break
        else:
            print(f"Error: {response} - {response.text}")
            break

        time.sleep(1)

    # Convert data to DataFrame
    df = pd.json_normalize(all_data)
    return df


def GA4_data_preprocessing(df, GA_client, start_date_str, end_date_str):
    df = fetch_ga4_data(GA_client, start_date_str, end_date_str) 
    if df is not None and not df.empty:
        # Filter out 'NA' values
        df = df[(df['PnM Parameter'] != 'NA')]
        
        # Create mapping dictionaries for First User Campaign and Session Campaign
        first_user_mapping = {
            '(not set)': 'NA',
            '(direct)': 'NA',
            '(organic)': 'NA',
            '(referral)': 'NA',
            'bangalore': 'NA',
            'surat': 'NA',
            'chennai': 'NA', 
            'mumbai': 'NA',
            'kopar-khairane': 'NA',
            'pune': 'NA',
            'indore': 'NA',
            'default_trucks_fare_estimate': 'NA',
            'jaipur': 'NA',
            'shriramgroup_banner': 'NA',
            'default_fare_estimate_booking_flow': 'NA',
            'default_home_2W': 'NA',
            'delhi': 'NA',
            'kolkata': 'NA',
            'hyderabad': 'NA',
            'default_two_wheelers_fare_estimate': 'NA',
            'default_home_Trucks': 'NA',
            'Open Targeting Bangalore Sept9th2024': 'NA',
            'ahmedabad': 'NA',
            'coimbatore': 'NA',
            'invite_code': 'NA',
            'footer-links': 'NA',
            'Kochi': 'NA',
            'broker_network': 'NA',
            'header-logo': 'NA',
            'geoID15': 'NA',
            'santacruz': 'NA',
            'vadavalli': 'NA',
            'south-delhi': 'NA',
            'confirmation_instructions_parent': 'NA',
            'geoID7': 'NA',
            'Nagpur': 'NA',
            'Referral_v2': 'NA',
            'peenya': 'NA'
        }

        # Replace values using the mapping
        df['First User Campaign'] = df['First User Campaign'].replace(first_user_mapping)
        df['Session Campaign'] = df['Session Campaign'].replace(first_user_mapping)

        # create a column final source where =IF(D2="(not set)", IF(ISNUMBER(SEARCH("Brand", G2)), "FT_Organic", G2), D2) logic is applied, where D is First user campaign and G is Session Campaign
        df['Final Source'] = df.apply(lambda x: 'FT_Organic' if x['First User Campaign'] == 'NA' and 'Brand' in x['Session Campaign'] 
                                    else x['Session Campaign'] if x['First User Campaign'] == 'NA' and 'Brand' not in x['Session Campaign'] and x['Session Campaign'] != 'NA'
                                    else x['First User Campaign'], axis=1)
        
        # Extract mobile numbers from 'PnM Parameter' column
        df = extract_mobile_numbers(df, 'PnM Parameter')
        df_filtered = remove_duplicates(df)

    return df_filtered


def map_google_leads(df_filtered, google_ads_data):
    if google_ads_data is not None and df_filtered is not None:
        # convert datr to datetime
        google_ads_data['Date'] = pd.to_datetime(google_ads_data['Date'])
        
        # Calculate intercity leads
        inter_city = df_filtered[df_filtered['Shifting Type'] == 'inter_city']
        inter_leads = inter_city.groupby(['Date', 'Final Source'])['Mobile'].nunique().reset_index()
        inter_leads = inter_leads.rename(columns={'Mobile': 'inter_city_leads'})
        
        # Calculate intracity leads
        intra_city = df_filtered[df_filtered['Shifting Type'] == 'intra_city']
        intra_leads = intra_city.groupby(['Date', 'Final Source'])['Mobile'].nunique().reset_index()
        intra_leads = intra_leads.rename(columns={'Mobile': 'intra_city_leads'})
        
        result = google_ads_data.merge(
            inter_leads,
            left_on=['Date', 'Campaign Name'],
            right_on=['Date', 'Final Source'],
            how='left'
        ).drop('Final Source', axis=1)
        
        result = result.merge(
            intra_leads,
            left_on=['Date', 'Campaign Name'],
            right_on=['Date', 'Final Source'],
            how='left'
        ).drop('Final Source', axis=1)
        
        # Fill NaN values with 0
        result['inter_city_leads'] = result['inter_city_leads'].fillna(0)
        result['intra_city_leads'] = result['intra_city_leads'].fillna(0)
        result['SF_Leads'] = result['inter_city_leads'] + result['intra_city_leads']
        
        return result
    else:
        return google_ads_data
    

def map_meta_leads(df_filtered, meta_ads_data):
    if meta_ads_data is not None and df_filtered is not None:
        # convert datr to datetime
        meta_ads_data['Date'] = pd.to_datetime(meta_ads_data['Date'])
        
        # Calculate intercity leads
        inter_city = df_filtered[df_filtered['Shifting Type'] == 'inter_city']
        inter_leads = inter_city.groupby(['Date', 'Final Source'])['Mobile'].nunique().reset_index()
        inter_leads = inter_leads.rename(columns={'Mobile': 'inter_city_leads'})
        
        # Calculate intracity leads
        intra_city = df_filtered[df_filtered['Shifting Type'] == 'intra_city']
        intra_leads = intra_city.groupby(['Date', 'Final Source'])['Mobile'].nunique().reset_index()
        intra_leads = intra_leads.rename(columns={'Mobile': 'intra_city_leads'})
        
        result = meta_ads_data.merge(
            inter_leads,
            left_on=['Date', 'adset_name'],
            right_on=['Date', 'Final Source'],
            how='left'
        ).drop('Final Source', axis=1)
        
        result = result.merge(
            intra_leads,
            left_on=['Date', 'adset_name'],
            right_on=['Date', 'Final Source'],
            how='left'
        ).drop('Final Source', axis=1)
        
        # Fill NaN values with 0
        result['inter_city_leads'] = result['inter_city_leads'].fillna(0)
        result['intra_city_leads'] = result['intra_city_leads'].fillna(0)
        result['SF_Leads'] = result['inter_city_leads'] + result['intra_city_leads']
        
        return result
    else:
        return meta_ads_data


def map_google_conversions(df_filtered, google_ads_data):
    if google_ads_data is not None and df_filtered is not None:
        google_ads_data['Date'] = pd.to_datetime(google_ads_data['Date'])
        
        # Filter for converted status
        converted = df_filtered[df_filtered['Status'] == 'Converted']
        
        # Calculate total conversions
        conversions = converted.groupby(['Date', 'Final Source'])['Mobile'].nunique().reset_index()
        conversions = conversions.rename(columns={'Mobile': 'SF_conversions'})
        
        # Calculate intercity conversions
        inter_conv = converted[converted['Shifting Type'] == 'inter_city']
        inter_conversions = inter_conv.groupby(['Date', 'Final Source'])['Mobile'].nunique().reset_index()
        inter_conversions = inter_conversions.rename(columns={'Mobile': 'inter_city_conversions'})
        
        # Calculate intracity conversions
        intra_conv = converted[converted['Shifting Type'] == 'intra_city']
        intra_conversions = intra_conv.groupby(['Date', 'Final Source'])['Mobile'].nunique().reset_index()
        intra_conversions = intra_conversions.rename(columns={'Mobile': 'intra_city_conversions'})
        
        # Merge all conversion counts with Google Ads data
        result = google_ads_data.merge(
            conversions,
            left_on=['Date', 'Campaign Name'],
            right_on=['Date', 'Final Source'],
            how='left'
        ).drop('Final Source', axis=1)
        
        result = result.merge(
            inter_conversions,
            left_on=['Date', 'Campaign Name'],
            right_on=['Date', 'Final Source'],
            how='left'
        ).drop('Final Source', axis=1)
        
        result = result.merge(
            intra_conversions,
            left_on=['Date', 'Campaign Name'],
            right_on=['Date', 'Final Source'],
            how='left'
        ).drop('Final Source', axis=1)
        
        # Fill NaN values with 0
        result['SF_conversions'] = result['SF_conversions'].fillna(0)
        result['inter_city_conversions'] = result['inter_city_conversions'].fillna(0)
        result['intra_city_conversions'] = result['intra_city_conversions'].fillna(0)
        
        return result
    else:
        return google_ads_data


def map_meta_conversions(df_filtered, meta_ads_data):
    if meta_ads_data is not None and df_filtered is not None:
        meta_ads_data['Date'] = pd.to_datetime(meta_ads_data['Date'])
        
        # Filter for converted status
        converted = df_filtered[df_filtered['Status'] == 'Converted']
        
        # Calculate total conversions
        conversions = converted.groupby(['Date', 'Final Source'])['Mobile'].nunique().reset_index()
        conversions = conversions.rename(columns={'Mobile': 'SF_conversions'})
        
        # Calculate intercity conversions
        inter_conv = converted[converted['Shifting Type'] == 'inter_city']
        inter_conversions = inter_conv.groupby(['Date', 'Final Source'])['Mobile'].nunique().reset_index()
        inter_conversions = inter_conversions.rename(columns={'Mobile': 'inter_city_conversions'})
        
        # Calculate intracity conversions
        intra_conv = converted[converted['Shifting Type'] == 'intra_city']
        intra_conversions = intra_conv.groupby(['Date', 'Final Source'])['Mobile'].nunique().reset_index()
        intra_conversions = intra_conversions.rename(columns={'Mobile': 'intra_city_conversions'})
        
        # Merge all conversion counts with Google Ads data
        result = meta_ads_data.merge(
            conversions,
            left_on=['Date', 'adset_name'],
            right_on=['Date', 'Final Source'],
            how='left'
        ).drop('Final Source', axis=1)
        
        result = result.merge(
            inter_conversions,
            left_on=['Date', 'adset_name'],
            right_on=['Date', 'Final Source'],
            how='left'
        ).drop('Final Source', axis=1)
        
        result = result.merge(
            intra_conversions,
            left_on=['Date', 'adset_name'],
            right_on=['Date', 'Final Source'],
            how='left'
        ).drop('Final Source', axis=1)
        
        # Fill NaN values with 0
        result['SF_conversions'] = result['SF_conversions'].fillna(0)
        result['inter_city_conversions'] = result['inter_city_conversions'].fillna(0)
        result['intra_city_conversions'] = result['intra_city_conversions'].fillna(0)
        
        return result
    else:
        return meta_ads_data


@st.cache_data(ttl=86400)  # Cache for 1 day
def map_salesforce_data(df_filtered, salesforce_data):
    if salesforce_data is not None:
        # Convert mobile to string
        salesforce_data['Mobile'] = salesforce_data['Mobile'].astype(str)

        # Create lookup dictionaries for status and shifting type
        status_dict = salesforce_data.groupby('Mobile')['Status'].last().to_dict()
        shifting_type_dict = salesforce_data.groupby('Mobile')['Shifting Type'].last().to_dict()
        
        # Add new columns with default value 'NA'
        df_filtered['Status'] = 'NA'
        df_filtered['Shifting Type'] = 'NA'
        
        # Update values where mobile numbers match
        matching_mobiles = df_filtered['Mobile'].isin(status_dict.keys())
        df_filtered.loc[matching_mobiles, 'Status'] = df_filtered.loc[matching_mobiles, 'Mobile'].map(status_dict)
        df_filtered.loc[matching_mobiles, 'Shifting Type'] = df_filtered.loc[matching_mobiles, 'Mobile'].map(shifting_type_dict)
        
    return df_filtered


def map_cities(df, column_name):
    # List of cities to map
    cities = [
        "Surat", "Ahmedabad", "Coimbatore", "Jaipur", "Indore", "Mumbai", 
        "Bangalore", "Delhi", "Hyderabad", "Chennai", "Pune", "Kolkata", 
        "Lucknow", "Nagpur", "NCR"
    ]
    
    # Create a mapping of city names
    city_mapping = {
        "NCR": "Delhi",  # Mapping NCR to Delhi
    }

    # Function to check if a city is in the column text
    def find_city(value):
        for city in cities:
            if city in str(value):
                return city_mapping.get(city, city)  # Return mapped city or city itself
        return "Others"  # If no city is found, return 'Others'

    # Apply the function to the column and create a new column 'City'
    df['City'] = df[column_name].apply(find_city)
    
    return df


def main():
    GA_client = initialize_ga4_client()
    Ads_client = get_google_ads_client()
    if not GA_client and Ads_client:
        return

    st.sidebar.header("Date Range Selection")
    
    # Default date range
    default_end_date = datetime.now() - timedelta(days=1)
    default_start_date = default_end_date - timedelta(days=30)
    
    # Date input widgets
    col1, col2 = st.sidebar.columns(2)
    with col1:
        start_date = st.date_input("Start Date", value=default_start_date, max_value=datetime.now())
    with col2:
        end_date = st.date_input("End Date", value=default_end_date, max_value=datetime.now())

    # Add file uploader to sidebar for Salesforce data
    with st.sidebar:
        uploaded_file = st.file_uploader("Upload Salesforce CSV file")
        if uploaded_file is not None:
            st.session_state.salesforce_data = pd.read_csv(uploaded_file, encoding='latin1')
        else:
            st.session_state.salesforce_data = None

    if start_date <= end_date:
        start_date_str = start_date.strftime('%Y-%m-%d')
        end_date_str = end_date.strftime('%Y-%m-%d')
        
        # Show loading spinner
        if 'processed_GA_data' not in st.session_state:
            with st.spinner('Fetching data...'):
                st.session_state.Google_ads_data = get_google_ads_data(Ads_client, st.secrets['google_ads']['customer_id'], start_date_str, end_date_str)
                st.session_state.Meta_ads_data = get_facebook_data(start_date_str, end_date_str)
                st.session_state.GA_data = fetch_ga4_data(GA_client, start_date_str, end_date_str)
                st.session_state.processed_GA_data = GA4_data_preprocessing(st.session_state.GA_data, GA_client, start_date_str, end_date_str)

    df_filtered = st.session_state.processed_GA_data.copy()
    salesforce_data = st.session_state.salesforce_data.copy()
    google_ads_data = st.session_state.Google_ads_data.copy()
    meta_ads_data = st.session_state.Meta_ads_data.copy()

    salesforce_data = salesforce_data[['House Shifting Opportunity: Created Date', 'Mobile', 'Status', 'Shifting Type']]
    google_ads_data = google_ads_data[google_ads_data['Campaign Name'].str.contains('packer', case=False, na=False)]
    meta_ads_data = meta_ads_data[meta_ads_data['campaign_name'].str.contains('pnm', case=False, na=False)]
    meta_ads_data = meta_ads_data.rename(columns={'date_stop': 'Date'})

    if st.checkbox("Show Raw Data"):
        st.header("Raw Data")

        st.subheader("Salesforce Data")
        st.dataframe(salesforce_data)

        st.subheader("Google Ads Data")
        st.dataframe(google_ads_data)

        st.subheader("Meta Ads Data")
        st.dataframe(meta_ads_data)

        st.subheader("GA4 Data")
        st.dataframe(df_filtered)

    if df_filtered is not None:
        df_filtered = map_salesforce_data(df_filtered, salesforce_data)
        google_final_raw = map_google_leads(df_filtered, google_ads_data)
        google_final_raw = map_google_conversions(df_filtered, google_final_raw)
        #meta_final_raw = map_meta_leads(df_filtered, meta_ads_data)
        #meta_final_raw = map_meta_conversions(df_filtered, meta_final_raw)

        # map cities
        google_final_raw = map_cities(google_final_raw, 'Campaign Name')
        #meta_final_raw = map_cities(meta_final_raw, 'adset_name')

        if st.checkbox("Show Mapped Data"):
            st.header("Mapped Data")
            
            #st.dataframe(df_filtered)
            st.subheader("Google Mapped")
            st.dataframe(google_final_raw)
        #st.dataframe(meta_final_raw)

        # Monthly view for Spends, Leads Conversions, CPL & CAC

        # Create monthly view
        if not google_final_raw.empty:
            # Convert Date to datetime if not already
            google_final_raw['Date'] = pd.to_datetime(google_final_raw['Date'])
            
            # Create month-year column
            google_final_raw['Month-Year'] = google_final_raw['Date'].dt.strftime('%B-%Y')
            
            # First create the overall monthly view
            monthly_data = google_final_raw.groupby('Month-Year').agg({
                'cost': 'sum',
                'SF_Leads': 'sum',
                'SF_conversions': 'sum'
            }).reset_index()
            
            # Calculate CPL and CAC
            monthly_data['CPL'] = monthly_data['cost'] / monthly_data['SF_Leads']
            monthly_data['CAC'] = monthly_data['cost'] / monthly_data['SF_conversions']
            
            # Round numeric columns
            monthly_data['cost'] = monthly_data['cost'].round()
            monthly_data['CPL'] = monthly_data['CPL'].round()
            monthly_data['CAC'] = monthly_data['CAC'].round()
            
            # Sort monthly data
            monthly_data['Sort_Date'] = pd.to_datetime(monthly_data['Month-Year'], format='%B-%Y')
            monthly_data = monthly_data.sort_values('Sort_Date')
            monthly_data = monthly_data.drop('Sort_Date', axis=1)
            
            # Now create the city-wise monthly view
            city_monthly = google_final_raw.groupby(['Month-Year', 'City']).agg({
                'cost': 'sum',
                'SF_Leads': 'sum',
                'SF_conversions': 'sum'
            }).reset_index()
            
            # Calculate city-wise CPL and CAC
            city_monthly['CPL'] = city_monthly['cost'] / city_monthly['SF_Leads']
            city_monthly['CAC'] = city_monthly['cost'] / city_monthly['SF_conversions']
            
            # Round numeric columns
            city_monthly['cost'] = city_monthly['cost'].round()
            city_monthly['CPL'] = city_monthly['CPL'].round()
            city_monthly['CAC'] = city_monthly['CAC'].round()
            
            # Sort data by date
            city_monthly['Sort_Date'] = pd.to_datetime(city_monthly['Month-Year'], format='%B-%Y')
            city_monthly = city_monthly.sort_values(['Sort_Date', 'City'])
            city_monthly = city_monthly.drop('Sort_Date', axis=1)

            # Display overall monthly performance
            st.subheader("Overall Monthly Performance")
            st.dataframe(monthly_data.set_index('Month-Year').T)
            
            # Create and display separate tables for each metric
            st.subheader("Month-on-Month City-wise Performance")

            # Create two columns
            col1, col2 = st.columns(2)

            # Column 1
            with col1:
                # Leads table
                st.write("Monthly Leads by City")
                leads_pivot = pd.pivot_table(
                    city_monthly, 
                    values='SF_Leads',
                    index='City',
                    columns='Month-Year',
                    aggfunc='sum'
                ).round(0)
                st.dataframe(leads_pivot)

                # Conversions table
                st.write("Monthly Conversions by City")
                conv_pivot = pd.pivot_table(
                    city_monthly, 
                    values='SF_conversions',
                    index='City',
                    columns='Month-Year',
                    aggfunc='sum'
                ).round(0)
                st.dataframe(conv_pivot)

                # Spends table
                st.write("Monthly Spends by City")
                spends_pivot = pd.pivot_table(
                    city_monthly, 
                    values='cost',
                    index='City',
                    columns='Month-Year',
                    aggfunc='sum'
                ).round(2)
                st.dataframe(spends_pivot)

            # Column 2
            with col2:
                # CPL table
                st.write("Monthly CPL by City")
                cpl_pivot = pd.pivot_table(
                    city_monthly, 
                    values='CPL',
                    index='City',
                    columns='Month-Year',
                    aggfunc='mean'
                ).round(2)
                st.dataframe(cpl_pivot)

                # CAC table
                st.write("Monthly CAC by City")
                cac_pivot = pd.pivot_table(
                    city_monthly, 
                    values='CAC',
                    index='City',
                    columns='Month-Year',
                    aggfunc='mean'
                ).round(2)
                st.dataframe(cac_pivot)

        
        #st.dataframe(df_filtered)
        # Add download button
        # csv = df_filtered.to_csv(index=False).encode('utf-8')
        # st.download_button(
        #     "Download Data as CSV",
        #     csv,
        #     "ga4_data.csv",
        #     "text/csv",
        #     key='download-csv'
        # )
    else:
        st.error("Error: End date must be after start date")

if __name__ == "__main__":
    main()
