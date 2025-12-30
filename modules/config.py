"""
Configuration module for GA-SF Mapping App
"""
import streamlit as st

# BigQuery Configuration
BQ_PROJECT_ID = "dashbords-450707"
BQ_DATASET_ID = "porter_pnm"
BQ_TABLE_GA_SF = "GA_SF_Mapped"
BQ_TABLE_GA_SF_NE = "GA_SF_NE_Mapped"

# GA4 Configuration
GA_SCOPES = ['https://www.googleapis.com/auth/analytics.readonly']

# Status Priority Mapping (higher = higher priority)
STATUS_PRIORITY = {
    'Open': 1,
    'Prospect': 2,
    'Quoted': 3,
    'Closed': 4,
    'Converted': 5
}

# Campaign Mapping - values to be replaced with 'NA'
CAMPAIGN_NA_MAPPING = {
    '(not set)', '(direct)', '(organic)', '(referral)',
    'bangalore', 'surat', 'chennai', 'mumbai', 'kopar-khairane',
    'pune', 'indore', 'default_trucks_fare_estimate', 'jaipur',
    'shriramgroup_banner', 'default_fare_estimate_booking_flow',
    'default_home_2W', 'delhi', 'kolkata', 'hyderabad',
    'default_two_wheelers_fare_estimate', 'default_home_Trucks',
    'Open Targeting Bangalore Sept9th2024', 'ahmedabad', 'coimbatore',
    'invite_code', 'footer-links', 'Kochi', 'broker_network',
    'header-logo', 'geoID15', 'santacruz', 'vadavalli', 'south-delhi',
    'confirmation_instructions_parent', 'geoID7', 'Nagpur',
    'Referral_v2', 'peenya'
}

# City Mapping
CITIES = [
    "Surat", "Ahmedabad", "Coimbatore", "Jaipur", "Indore", "Mumbai",
    "Bangalore", "Delhi", "Hyderabad", "Chennai", "Pune", "Kolkata",
    "Lucknow", "Nagpur", "NCR"
]

CITY_MAPPING = {
    "NCR": "Delhi"
}

# Required Salesforce Columns  
SF_REQUIRED_COLUMNS = [
    'House Shifting Opportunity: Created Date',
    'Mobile',
    'Status',
    'Shifting Type'
]

# GA4 Dimensions to fetch
GA4_DIMENSIONS = [
    "customEvent:PnM_parameter",
    "date",
    "firstUserCampaignName",
    "sessionSource",
    "sessionMedium",
    "sessionCampaignName",
    "customEvent:GTES_mobile",
    "sessionManualAdContent",
    "operatingSystem"
]

# GA4 Metrics to fetch
GA4_METRICS = [
    "sessions",
    "engagedSessions"
]


def get_ga_property_id():
    """Get GA Property ID from secrets"""
    return st.secrets["ga_property"]["property_id"]


def get_gcp_credentials():
    """Get GCP service account credentials from secrets"""
    return {
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
