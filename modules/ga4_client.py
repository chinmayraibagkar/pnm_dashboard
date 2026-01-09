"""
GA4 Client module for fetching Google Analytics data
"""
import json
import tempfile
from datetime import datetime
from typing import List, Dict, Any

import streamlit as st
from google.oauth2.service_account import Credentials
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import (
    DateRange, Metric, Dimension, RunReportRequest,
    FilterExpression, Filter, FilterExpressionList
)

from .config import get_gcp_credentials, get_ga_property_id, GA_SCOPES


class GA4Client:
    """Client for fetching data from Google Analytics 4"""
    
    def __init__(self):
        """Initialize GA4 client with credentials from secrets"""
        self.property_id = get_ga_property_id()
        self.client = self._initialize_client()
    
    def _initialize_client(self) -> BetaAnalyticsDataClient:
        """Initialize and return GA4 client with proper authentication"""
        try:
            credentials_dict = get_gcp_credentials()
            
            # Create temporary file for credentials
            with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as temp:
                json.dump(credentials_dict, temp)
                temp_path = temp.name
            
            credentials = Credentials.from_service_account_file(temp_path, scopes=GA_SCOPES)
            return BetaAnalyticsDataClient(credentials=credentials)
        except Exception as e:
            st.error(f"Failed to initialize GA4 client: {str(e)}")
            raise
    
    def fetch_data(self, start_date: str, end_date: str) -> List[Dict[str, Any]]:
        """
        Fetch GA4 data with dimension filter to exclude blank or '(not set)' PnM_parameters.
        Uses 2 API calls due to GA4's 9 dimension limit, then merges the results.
        
        Args:
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            
        Returns:
            List of dictionaries containing GA4 data
        """
        # Define Filter: PnM_parameter is NOT "(not set)" AND NOT ""
        filter_ex = FilterExpression(
            and_group=FilterExpressionList(
                expressions=[
                    FilterExpression(
                        not_expression=FilterExpression(
                            filter=Filter(
                                field_name="customEvent:PnM_parameter",
                                string_filter=Filter.StringFilter(value="(not set)")
                            )
                        )
                    ),
                    FilterExpression(
                        not_expression=FilterExpression(
                            filter=Filter(
                                field_name="customEvent:PnM_parameter",
                                string_filter=Filter.StringFilter(value="")
                            )
                        )
                    )
                ]
            )
        )
        
        # First API call: Main dimensions (9 dimensions - within limit)
        # PnM_parameter, date, firstUserCampaignName, sessionSource, sessionMedium,
        # sessionCampaignName, GTES_mobile, sessionManualAdContent, operatingSystem
        all_rows_main = []
        offset = 0
        limit = 100000
        
        while True:
            request = RunReportRequest(
                property=f"properties/{self.property_id}",
                date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
                dimensions=[
                    Dimension(name="customEvent:PnM_parameter"),
                    Dimension(name="date"),
                    Dimension(name="firstUserCampaignName"),
                    Dimension(name="sessionSource"),
                    Dimension(name="sessionMedium"),
                    Dimension(name="sessionCampaignName"),
                    Dimension(name="customEvent:GTES_mobile"),
                    Dimension(name="sessionManualAdContent"),
                    Dimension(name="operatingSystem"),
                ],
                metrics=[
                    Metric(name="sessions"),
                    Metric(name="engagedSessions"),
                ],
                dimension_filter=filter_ex,
                offset=offset,
                limit=limit
            )
            
            response = self.client.run_report(request)
            
            if not response.rows:
                break
            
            all_rows_main.extend(response.rows)
            
            if len(response.rows) < limit:
                break
            
            offset += limit
        
        # Second API call: Campaign IDs (4 dimensions - for ID lookup)
        # firstUserCampaignName, firstUserCampaignId, sessionCampaignName, sessionCampaignId
        campaign_id_map = {}  # (firstUserCampaign, sessionCampaign) -> (firstUserId, sessionId)
        offset = 0
        
        while True:
            request = RunReportRequest(
                property=f"properties/{self.property_id}",
                date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
                dimensions=[
                    Dimension(name="firstUserCampaignName"),
                    Dimension(name="firstUserCampaignId"),
                    Dimension(name="sessionCampaignName"),
                    Dimension(name="sessionCampaignId"),
                ],
                metrics=[
                    Metric(name="sessions"),
                ],
                dimension_filter=filter_ex,
                offset=offset,
                limit=limit
            )
            
            response = self.client.run_report(request)
            
            if not response.rows:
                break
            
            for row in response.rows:
                first_user_campaign = row.dimension_values[0].value
                first_user_campaign_id = row.dimension_values[1].value
                session_campaign = row.dimension_values[2].value
                session_campaign_id = row.dimension_values[3].value
                
                # Store mapping - use campaign names as keys to look up IDs
                if first_user_campaign not in campaign_id_map:
                    campaign_id_map[first_user_campaign] = first_user_campaign_id
                # Also create a separate map for session campaigns
                session_key = f"session_{session_campaign}"
                if session_key not in campaign_id_map:
                    campaign_id_map[session_key] = session_campaign_id
            
            if len(response.rows) < limit:
                break
            
            offset += limit
        
        # Process main rows and add campaign IDs from lookup
        data = []
        for row in all_rows_main:
            date_str = datetime.strptime(
                row.dimension_values[1].value, '%Y%m%d'
            ).strftime('%Y-%m-%d')
            
            # Dimension indices for main request:
            # 0: PnM_parameter, 1: date, 2: firstUserCampaignName,
            # 3: sessionSource, 4: sessionMedium, 5: sessionCampaignName,
            # 6: GTES_mobile, 7: sessionManualAdContent, 8: operatingSystem
            first_user_campaign = row.dimension_values[2].value
            session_campaign = row.dimension_values[5].value
            
            source = row.dimension_values[3].value
            medium = row.dimension_values[4].value
            source_medium = f"{source} / {medium}"
            
            # Handle Operating System - categorize as iOS, Windows, Android, or Others
            os_value = row.dimension_values[8].value
            operating_system = os_value if os_value in ["iOS", "Windows", "Android"] else "Others"
            
            # Look up campaign IDs from the second API call
            first_user_campaign_id = campaign_id_map.get(first_user_campaign, "")
            session_campaign_id = campaign_id_map.get(f"session_{session_campaign}", "")
            
            item = {
                'PnM_Parameter': row.dimension_values[0].value,
                'Date': date_str,
                'First_User_Campaign': first_user_campaign,
                'First_User_Campaign_ID': first_user_campaign_id,
                'Sessions': int(row.metric_values[0].value),
                'Source_Medium': source_medium,
                'Session_Campaign': session_campaign,
                'Session_Campaign_ID': session_campaign_id,
                'Engaged_Sessions': int(row.metric_values[1].value),
                'Keyword': row.dimension_values[7].value,
                'Operating_System': operating_system
            }
            data.append(item)
        
        return data


@st.cache_resource
def get_ga4_client() -> GA4Client:
    """Get cached GA4 client instance"""
    return GA4Client()
