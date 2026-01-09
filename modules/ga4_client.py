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
        
        all_rows = []
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
                    Dimension(name="firstUserCampaignId"),
                    Dimension(name="sessionSource"),
                    Dimension(name="sessionMedium"),
                    Dimension(name="sessionCampaignName"),
                    Dimension(name="sessionCampaignId"),
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
            
            all_rows.extend(response.rows)
            
            if len(response.rows) < limit:
                break
            
            offset += limit
        
        # Process rows into dictionaries
        data = []
        for row in all_rows:
            date_str = datetime.strptime(
                row.dimension_values[1].value, '%Y%m%d'
            ).strftime('%Y-%m-%d')
            
            # Dimension indices after adding campaign IDs:
            # 0: PnM_parameter, 1: date, 2: firstUserCampaignName, 3: firstUserCampaignId,
            # 4: sessionSource, 5: sessionMedium, 6: sessionCampaignName, 7: sessionCampaignId,
            # 8: GTES_mobile, 9: sessionManualAdContent, 10: operatingSystem
            source = row.dimension_values[4].value
            medium = row.dimension_values[5].value
            source_medium = f"{source} / {medium}"
            
            # Handle Operating System - categorize as iOS, Windows, Android, or Others
            os_value = row.dimension_values[10].value
            operating_system = os_value if os_value in ["iOS", "Windows", "Android"] else "Others"
            
            item = {
                'PnM_Parameter': row.dimension_values[0].value,
                'Date': date_str,
                'First_User_Campaign': row.dimension_values[2].value,
                'First_User_Campaign_ID': row.dimension_values[3].value,
                'Sessions': int(row.metric_values[0].value),
                'Source_Medium': source_medium,
                'Session_Campaign': row.dimension_values[6].value,
                'Session_Campaign_ID': row.dimension_values[7].value,
                'Engaged_Sessions': int(row.metric_values[1].value),
                'Keyword': row.dimension_values[9].value,
                'Operating_System': operating_system
            }
            data.append(item)
        
        return data


@st.cache_resource
def get_ga4_client() -> GA4Client:
    """Get cached GA4 client instance"""
    return GA4Client()
