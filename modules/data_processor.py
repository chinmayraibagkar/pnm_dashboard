"""
Data Processor module for GA-SF data processing
"""
import re
import pandas as pd
from typing import Optional

from .config import CAMPAIGN_NA_MAPPING, STATUS_PRIORITY, SF_REQUIRED_COLUMNS


def extract_mobile_numbers(df: pd.DataFrame, column_name: str) -> pd.DataFrame:
    """
    Extract 10-digit mobile numbers from the specified column.
    
    Args:
        df: DataFrame containing the column
        column_name: Name of the column to extract mobile numbers from
        
    Returns:
        DataFrame with 'Mobile' column added
    """
    pattern = r'\d{10}'
    
    df = df.copy()
    df['Mobile'] = df[column_name].astype(str).apply(
        lambda x: re.search(pattern, x).group() if re.search(pattern, x) else None
    )
    
    return df


def get_bimonth_period(month: int) -> int:
    """
    Get the 2-month period number (1-6) for a given month.
    Jan-Feb=1, Mar-Apr=2, May-Jun=3, Jul-Aug=4, Sep-Oct=5, Nov-Dec=6
    
    Args:
        month: Month number (1-12)
        
    Returns:
        Period number (1-6)
    """
    return (month - 1) // 2 + 1


def get_bimonth_date_range(start_date, end_date):
    """
    Expand a date range to cover the full bi-month bucket(s).
    This ensures deduplication is consistent regardless of selected date range.
    
    Bi-month buckets: Jan-Feb, Mar-Apr, May-Jun, Jul-Aug, Sep-Oct, Nov-Dec.
    
    Args:
        start_date: Start date (datetime.date or string 'YYYY-MM-DD')
        end_date: End date (datetime.date or string 'YYYY-MM-DD')
        
    Returns:
        Tuple of (expanded_start_date, expanded_end_date) as strings 'YYYY-MM-DD'
    """
    from datetime import datetime
    from calendar import monthrange
    
    # Parse dates if strings
    if isinstance(start_date, str):
        start_date = datetime.strptime(start_date, '%Y-%m-%d').date()
    if isinstance(end_date, str):
        end_date = datetime.strptime(end_date, '%Y-%m-%d').date()
    
    # Get bi-month period boundaries for start date
    start_period = get_bimonth_period(start_date.month)
    start_bucket_first_month = (start_period - 1) * 2 + 1  # 1, 3, 5, 7, 9, 11
    expanded_start = start_date.replace(month=start_bucket_first_month, day=1)
    
    # Get bi-month period boundaries for end date
    end_period = get_bimonth_period(end_date.month)
    end_bucket_last_month = end_period * 2  # 2, 4, 6, 8, 10, 12
    last_day = monthrange(end_date.year, end_bucket_last_month)[1]
    expanded_end = end_date.replace(month=end_bucket_last_month, day=last_day)
    
    return expanded_start.strftime('%Y-%m-%d'), expanded_end.strftime('%Y-%m-%d')


def remove_duplicates_bimonth_ga(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove duplicates within 2-month calendar windows for GA data.
    Keeps the OLDEST occurrence (earliest date) for each mobile within each period-year.
    
    Args:
        df: DataFrame with GA data containing 'Mobile' and 'Date' columns
        
    Returns:
        Deduplicated DataFrame
    """
    if df.empty:
        return df
    
    df = df.copy()
    df['Date'] = pd.to_datetime(df['Date'])
    
    # Create period-year columns
    df['_Year'] = df['Date'].dt.year
    df['_Period'] = df['Date'].dt.month.apply(get_bimonth_period)
    
    # Sort by Mobile, Year, Period, Date (ascending to get oldest first)
    df = df.sort_values(['Mobile', '_Year', '_Period', 'Date'], ascending=True)
    
    # Keep first (oldest) occurrence for each Mobile within each Period-Year
    df_filtered = df.drop_duplicates(
        subset=['Mobile', '_Year', '_Period'], 
        keep='first'
    )
    
    # Drop helper columns
    df_filtered = df_filtered.drop(columns=['_Year', '_Period'])
    
    # Sort back to chronological order
    return df_filtered.sort_values('Date').reset_index(drop=True)


def dedupe_salesforce_by_priority(sf_df: pd.DataFrame) -> pd.DataFrame:
    """
    Deduplicate Salesforce data within 2-month calendar windows.
    Keeps the HIGHEST PRIORITY status for each mobile within each period-year.
    Priority: Converted > Closed > Quoted > Prospect > Open
    
    Args:
        sf_df: Salesforce DataFrame
        
    Returns:
        Deduplicated DataFrame with highest priority status per mobile per period
    """
    if sf_df.empty:
        return sf_df
    
    sf_df = sf_df.copy()
    
    # Parse date column
    date_col = 'House Shifting Opportunity: Created Date'
    sf_df[date_col] = pd.to_datetime(sf_df[date_col], errors='coerce')
    
    # Create period-year columns
    sf_df['_Year'] = sf_df[date_col].dt.year
    sf_df['_Period'] = sf_df[date_col].dt.month.apply(get_bimonth_period)
    
    # Add priority score
    sf_df['_Priority'] = sf_df['Status'].map(STATUS_PRIORITY).fillna(0)
    
    # Sort by Mobile, Year, Period, Priority (desc), Date (desc for most recent if same priority)
    sf_df = sf_df.sort_values(
        ['Mobile', '_Year', '_Period', '_Priority', date_col], 
        ascending=[True, True, True, False, False]
    )
    
    # Keep first (highest priority, most recent) occurrence for each Mobile within each Period-Year
    sf_filtered = sf_df.drop_duplicates(
        subset=['Mobile', '_Year', '_Period'], 
        keep='first'
    )
    
    # Drop helper columns
    sf_filtered = sf_filtered.drop(columns=['_Year', '_Period', '_Priority'])
    
    return sf_filtered.reset_index(drop=True)


def apply_campaign_mapping(df: pd.DataFrame) -> pd.DataFrame:
    """
    Map certain campaign values to 'NA' based on predefined list.
    
    Args:
        df: DataFrame with campaign columns
        
    Returns:
        DataFrame with mapped campaign values
    """
    df = df.copy()
    
    # Create replacement function
    def replace_if_in_list(value):
        return 'NA' if value in CAMPAIGN_NA_MAPPING else value
    
    # Apply to both campaign columns
    if 'First_User_Campaign' in df.columns:
        df['First_User_Campaign'] = df['First_User_Campaign'].apply(replace_if_in_list)
    
    if 'Session_Campaign' in df.columns:
        df['Session_Campaign'] = df['Session_Campaign'].apply(replace_if_in_list)
    
    return df


def calculate_final_source(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate Final_Source based on campaign logic.
    
    Logic:
    - If First_User_Campaign is "NA":
        - If "Brand" in Session_Campaign -> "FT_Organic"
        - Else if Session_Campaign != "NA" -> Session_Campaign
        - Else -> First_User_Campaign (which is "NA")
    - Else -> First_User_Campaign
    
    Args:
        df: DataFrame with First_User_Campaign and Session_Campaign columns
        
    Returns:
        DataFrame with Final_Source column added
    """
    df = df.copy()
    
    def get_final_source(row):
        first_user = row.get('First_User_Campaign', 'NA')
        session = row.get('Session_Campaign', 'NA')
        
        if first_user == 'NA':
            if 'Brand' in str(session):
                return 'FT_Organic'
            elif session != 'NA':
                return session
            else:
                return first_user
        else:
            return first_user
    
    df['Final_Source'] = df.apply(get_final_source, axis=1)
    
    return df


def map_salesforce_data(ga_df: pd.DataFrame, sf_df: pd.DataFrame) -> pd.DataFrame:
    """
    Map Salesforce Status and Shifting Type to GA data based on mobile number.
    Uses left join to keep all GA records.
    Unmatched records are marked as 'Not Found'.
    
    Args:
        ga_df: GA DataFrame with Mobile column
        sf_df: Salesforce DataFrame with Mobile, Status, Shifting Type columns
        
    Returns:
        GA DataFrame with Status and Shifting_Type columns added
    """
    ga_df = ga_df.copy()
    
    if sf_df is None or sf_df.empty:
        ga_df['Status'] = 'Not Found'
        ga_df['Shifting_Type'] = 'Not Found'
        return ga_df
    
    # Validate required columns
    missing_cols = [col for col in SF_REQUIRED_COLUMNS if col not in sf_df.columns]
    if missing_cols:
        raise ValueError(f"Missing required Salesforce columns: {missing_cols}")
    
    sf_df = sf_df.copy()
    
    # Ensure Mobile is string type
    sf_df['Mobile'] = sf_df['Mobile'].astype(str)
    ga_df['Mobile'] = ga_df['Mobile'].astype(str)
    
    # Deduplicate SF data by priority
    sf_deduped = dedupe_salesforce_by_priority(sf_df)
    
    # Create lookup dictionaries
    status_dict = sf_deduped.set_index('Mobile')['Status'].to_dict()
    shifting_dict = sf_deduped.set_index('Mobile')['Shifting Type'].to_dict()
    
    # Map values - use 'Not Found' for unmatched
    ga_df['Status'] = ga_df['Mobile'].map(status_dict).fillna('Not Found')
    ga_df['Shifting_Type'] = ga_df['Mobile'].map(shifting_dict).fillna('Not Found')
    
    return ga_df


def map_ne_data(mapped_df: pd.DataFrame, ne_df: pd.DataFrame) -> pd.DataFrame:
    """
    Map NE data columns to GA-SF mapped data based on mobile number.
    Uses left join to keep all GA-SF records.
    
    Args:
        mapped_df: GA-SF mapped DataFrame with Mobile column
        ne_df: NE DataFrame with Mobile and other columns
        
    Returns:
        DataFrame with NE columns mapped
    """
    if ne_df is None or ne_df.empty:
        return mapped_df
    
    mapped_df = mapped_df.copy()
    ne_df = ne_df.copy()
    
    # Ensure Mobile is string
    ne_df['Mobile'] = ne_df['Mobile'].astype(str)
    
    # Get columns to map (exclude date-like columns)
    date_patterns = ['date', 'Date', 'DATE', 'created', 'Created', 'CREATED']
    cols_to_map = [
        col for col in ne_df.columns 
        if col != 'Mobile' and not any(pat in col for pat in date_patterns)
    ]
    
    # Create lookup DataFramefor merging
    ne_lookup = ne_df[['Mobile'] + cols_to_map].drop_duplicates(subset=['Mobile'], keep='first')
    
    # Merge using left join
    result = mapped_df.merge(ne_lookup, on='Mobile', how='left', suffixes=('', '_NE'))
    
    return result


def add_month_year_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add Month and Year columns for BigQuery unique key.
    
    Args:
        df: DataFrame with Date column
        
    Returns:
        DataFrame with Month and Year columns added
    """
    df = df.copy()
    df['Date'] = pd.to_datetime(df['Date'])
    df['Month'] = df['Date'].dt.month
    df['Year'] = df['Date'].dt.year
    return df


def process_ga_data(ga_data: list, sf_df: Optional[pd.DataFrame] = None) -> pd.DataFrame:
    """
    Full processing pipeline for GA data.
    
    Args:
        ga_data: List of dictionaries from GA4 client
        sf_df: Optional Salesforce DataFrame
        
    Returns:
        Fully processed and mapped DataFrame
    """
    if not ga_data:
        return pd.DataFrame()
    
    # Create DataFrame
    df = pd.DataFrame(ga_data)
    
    # Extract mobile numbers
    df = extract_mobile_numbers(df, 'PnM_Parameter')
    
    # Remove rows without valid mobile
    df = df.dropna(subset=['Mobile'])
    
    # Apply campaign mapping
    df = apply_campaign_mapping(df)
    
    # Calculate final source
    df = calculate_final_source(df)
    
    # Remove duplicates using 2-month window
    df = remove_duplicates_bimonth_ga(df)
    
    # Map Salesforce data
    df = map_salesforce_data(df, sf_df)
    
    # Add month/year columns for BQ key
    df = add_month_year_columns(df)
    
    return df
