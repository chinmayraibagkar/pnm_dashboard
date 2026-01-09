"""
BigQuery Manager module for data storage and retrieval
"""
import json
import tempfile
from typing import Tuple, Optional

import pandas as pd
import streamlit as st
from google.cloud import bigquery
from google.oauth2 import service_account
from google.api_core.exceptions import NotFound

from .config import (
    BQ_PROJECT_ID, BQ_DATASET_ID, BQ_TABLE_GA_SF, BQ_TABLE_GA_SF_NE,
    get_gcp_credentials
)


# Schema for GA_SF_Mapped table
GA_SF_SCHEMA = [
    bigquery.SchemaField("PnM_Parameter", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("Date", "DATE", mode="REQUIRED"),
    bigquery.SchemaField("First_User_Campaign", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("First_User_Campaign_ID", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("Sessions", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("Source_Medium", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("Session_Campaign", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("Session_Campaign_ID", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("Engaged_Sessions", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("Keyword", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("Operating_System", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("Mobile", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("Final_Source", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("Final_Source_Campaign_ID", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("Status", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("Shifting_Type", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("Month", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("Year", "INTEGER", mode="NULLABLE"),
]


def get_bq_client() -> bigquery.Client:
    """
    Initialize and return BigQuery client using service account credentials.
    
    Returns:
        BigQuery Client instance
    """
    credentials_dict = get_gcp_credentials()
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as temp:
        json.dump(credentials_dict, temp)
        temp_path = temp.name
    
    credentials = service_account.Credentials.from_service_account_file(temp_path)
    return bigquery.Client(credentials=credentials, project=BQ_PROJECT_ID)


def ensure_dataset_exists(client: bigquery.Client) -> None:
    """
    Ensure the dataset exists, create if not.
    
    Args:
        client: BigQuery client
    """
    dataset_ref = f"{client.project}.{BQ_DATASET_ID}"
    
    try:
        client.get_dataset(dataset_ref)
    except NotFound:
        dataset = bigquery.Dataset(dataset_ref)
        client.create_dataset(dataset)


def ensure_table_exists(
    client: bigquery.Client, 
    table_name: str, 
    schema: list
) -> str:
    """
    Ensure the table exists, create if not.
    
    Args:
        client: BigQuery client
        table_name: Name of the table
        schema: Schema for the table
        
    Returns:
        Full table ID
    """
    ensure_dataset_exists(client)
    
    full_table_id = f"{client.project}.{BQ_DATASET_ID}.{table_name}"
    
    try:
        client.get_table(full_table_id)
    except NotFound:
        table = bigquery.Table(full_table_id, schema=schema)
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="Date"
        )
        client.create_table(table)
    
    return full_table_id


def reset_table(table_name: str) -> Tuple[bool, str]:
    """
    Drop and recreate a BigQuery table.
    
    Args:
        table_name: Name of the table to reset
        
    Returns:
        Tuple of (success, message)
    """
    try:
        client = get_bq_client()
        full_table_id = f"{client.project}.{BQ_DATASET_ID}.{table_name}"
        
        # Determine schema based on table name
        if table_name == BQ_TABLE_GA_SF:
            schema = GA_SF_SCHEMA
        else:
            # For NE table, use GA_SF schema as base
            schema = GA_SF_SCHEMA
        
        # Drop table if exists
        try:
            client.delete_table(full_table_id)
        except NotFound:
            pass  # Table doesn't exist, that's fine
        
        # Create fresh table
        table = bigquery.Table(full_table_id, schema=schema)
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="Date"
        )
        client.create_table(table)
        
        return True, f"Successfully reset table {table_name}"
        
    except Exception as e:
        return False, f"Error resetting table: {str(e)}"


def get_existing_data(client: bigquery.Client, table_name: str) -> pd.DataFrame:
    """
    Fetch existing data from BigQuery table.
    
    Args:
        client: BigQuery client
        table_name: Name of the table
        
    Returns:
        DataFrame with existing data
    """
    full_table_id = f"{client.project}.{BQ_DATASET_ID}.{table_name}"
    
    try:
        query = f"SELECT * FROM `{full_table_id}`"
        return client.query(query).to_dataframe()
    except NotFound:
        return pd.DataFrame()
    except Exception as e:
        st.warning(f"Could not fetch existing data: {str(e)}")
        return pd.DataFrame()


def merge_with_existing_data(
    new_df: pd.DataFrame, 
    existing_df: pd.DataFrame
) -> Tuple[pd.DataFrame, int, int]:
    """
    Merge new data with existing BigQuery data using date-based comparison.
    For each Mobile-Month-Year key, keeps the record with the LATEST date.
    Uses vectorized operations for performance.
    
    Args:
        new_df: New data to upload
        existing_df: Existing data in BigQuery
        
    Returns:
        Tuple of (merged_df, new_records_count, status_updates_count)
    """
    if existing_df.empty:
        return new_df, len(new_df), 0
    
    new_df = new_df.copy()
    existing_df = existing_df.copy()
    
    # Ensure proper types
    for df in [new_df, existing_df]:
        df['Mobile'] = df['Mobile'].astype(str)
        df['Month'] = df['Month'].astype(int)
        df['Year'] = df['Year'].astype(int)
        df['Date'] = pd.to_datetime(df['Date'])
    
    # Create unique key: Mobile_Month_Year
    new_df['_key'] = new_df['Mobile'] + '_' + new_df['Month'].astype(str) + '_' + new_df['Year'].astype(str)
    existing_df['_key'] = existing_df['Mobile'] + '_' + existing_df['Month'].astype(str) + '_' + existing_df['Year'].astype(str)
    
    # Find key sets
    existing_keys = set(existing_df['_key'].unique())
    new_keys = set(new_df['_key'].unique())
    
    # Keys only in new data (new records)
    only_new_keys = new_keys - existing_keys
    new_records_count = len(only_new_keys)
    
    # Keys only in existing (keep as-is)
    only_existing_keys = existing_keys - new_keys
    
    # Keys in both (need date-based comparison)
    overlapping_keys = existing_keys.intersection(new_keys)
    
    result_parts = []
    
    # 1. Add new-only records
    new_only_df = new_df[new_df['_key'].isin(only_new_keys)]
    if not new_only_df.empty:
        result_parts.append(new_only_df)
    
    # 2. Add existing-only records
    existing_only_df = existing_df[existing_df['_key'].isin(only_existing_keys)]
    if not existing_only_df.empty:
        result_parts.append(existing_only_df)
    
    # 3. For overlapping keys - vectorized date comparison
    status_updates_count = 0
    if overlapping_keys:
        # Get overlapping records from both dataframes
        new_overlap = new_df[new_df['_key'].isin(overlapping_keys)].copy()
        existing_overlap = existing_df[existing_df['_key'].isin(overlapping_keys)].copy()
        
        # Create lookup dicts for dates and status
        new_dates = new_overlap.set_index('_key')['Date'].to_dict()
        existing_dates = existing_overlap.set_index('_key')['Date'].to_dict()
        
        # Determine which source to use for each key
        keys_use_new = []
        keys_use_existing = []
        
        for key in overlapping_keys:
            if new_dates.get(key, pd.Timestamp.min) >= existing_dates.get(key, pd.Timestamp.min):
                keys_use_new.append(key)
            else:
                keys_use_existing.append(key)
        
        # Add records from new where new date is >= existing date
        if keys_use_new:
            records_from_new = new_overlap[new_overlap['_key'].isin(keys_use_new)]
            result_parts.append(records_from_new)
            
            # Count status updates (where status changed and we used new data)
            if 'Status' in new_overlap.columns and 'Status' in existing_overlap.columns:
                new_status = new_overlap.set_index('_key')['Status'].to_dict()
                existing_status = existing_overlap.set_index('_key')['Status'].to_dict()
                for key in keys_use_new:
                    if new_status.get(key) != existing_status.get(key):
                        status_updates_count += 1
        
        # Add records from existing where existing date is > new date
        if keys_use_existing:
            records_from_existing = existing_overlap[existing_overlap['_key'].isin(keys_use_existing)]
            result_parts.append(records_from_existing)
    
    # Combine all results
    if result_parts:
        merged_df = pd.concat(result_parts, ignore_index=True)
    else:
        merged_df = pd.DataFrame()
    
    # Drop helper column
    if '_key' in merged_df.columns:
        merged_df = merged_df.drop(columns=['_key'])
    
    return merged_df, new_records_count, status_updates_count


def prepare_upload_df(df: pd.DataFrame, target_columns: list) -> pd.DataFrame:
    """
    Prepare DataFrame for upload by selecting and ordering columns.
    
    Args:
        df: DataFrame to prepare
        target_columns: List of column names to include
        
    Returns:
        Prepared DataFrame
    """
    # Create copy with only target columns that exist
    available_cols = [col for col in target_columns if col in df.columns]
    upload_df = df[available_cols].copy()
    
    # Add missing columns with None
    for col in target_columns:
        if col not in upload_df.columns:
            upload_df[col] = None
    
    # Reorder columns
    upload_df = upload_df[target_columns]
    
    # Fix Date type for PyArrow
    if 'Date' in upload_df.columns:
        upload_df['Date'] = pd.to_datetime(upload_df['Date']).dt.date
    
    return upload_df


def upload_ga_sf_data(
    df: pd.DataFrame
) -> Tuple[bool, dict, str]:
    """
    Upload GA-SF mapped data to BigQuery with date-based merge.
    Uses WRITE_TRUNCATE to replace existing data.
    For each Mobile-Month-Year key, keeps the record with the LATEST date.
    
    Args:
        df: DataFrame to upload
        
    Returns:
        Tuple of (success, stats_dict, message)
        stats_dict contains: new_records, status_updates, total_rows
    """
    try:
        client = get_bq_client()
        
        # Ensure table exists
        full_table_id = ensure_table_exists(client, BQ_TABLE_GA_SF, GA_SF_SCHEMA)
        
        # Get existing data for comparison
        existing_df = get_existing_data(client, BQ_TABLE_GA_SF)
        
        # Merge with existing data using date-based comparison
        target_columns = [field.name for field in GA_SF_SCHEMA]
        upload_df = prepare_upload_df(df, target_columns)
        
        merged_df, new_records, status_updates = merge_with_existing_data(upload_df, existing_df)
        
        # Prepare final upload DataFrame
        final_df = prepare_upload_df(merged_df, target_columns)
        
        # Upload to BigQuery
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE",
            schema=GA_SF_SCHEMA
        )
        
        job = client.load_table_from_dataframe(final_df, full_table_id, job_config=job_config)
        job.result()
        
        total_rows = len(final_df)
        
        stats = {
            'new_records': new_records,
            'status_updates': status_updates,
            'total_rows': total_rows
        }
        
        message = f"Successfully uploaded to {BQ_TABLE_GA_SF}"
        
        return True, stats, message
        
    except Exception as e:
        return False, {'new_records': 0, 'status_updates': 0, 'total_rows': 0}, f"Error uploading to BigQuery: {str(e)}"


def upload_ga_sf_ne_data(
    df: pd.DataFrame
) -> Tuple[bool, dict, str]:
    """
    Upload GA-SF-NE mapped data to BigQuery with date-based merge.
    Dynamically creates schema based on DataFrame columns.
    For each Mobile-Month-Year key, keeps the record with the LATEST date.
    
    Args:
        df: DataFrame to upload
        
    Returns:
        Tuple of (success, stats_dict, message)
        stats_dict contains: new_records, status_updates, total_rows
    """
    try:
        client = get_bq_client()
        
        # Create dynamic schema based on DataFrame columns
        schema = []
        for col in df.columns:
            dtype = df[col].dtype
            if col == 'Date':
                schema.append(bigquery.SchemaField(col, "DATE", mode="REQUIRED"))
            elif dtype in ['int64', 'int32']:
                schema.append(bigquery.SchemaField(col, "INTEGER", mode="NULLABLE"))
            elif dtype == 'float64':
                schema.append(bigquery.SchemaField(col, "FLOAT", mode="NULLABLE"))
            else:
                schema.append(bigquery.SchemaField(col, "STRING", mode="NULLABLE"))
        
        # Ensure table exists
        full_table_id = ensure_table_exists(client, BQ_TABLE_GA_SF_NE, schema)
        
        # Get existing data for comparison
        existing_df = get_existing_data(client, BQ_TABLE_GA_SF_NE)
        
        # Prepare upload DataFrame
        upload_df = df.copy()
        if 'Date' in upload_df.columns:
            upload_df['Date'] = pd.to_datetime(upload_df['Date']).dt.date
        
        # Merge with existing data using date-based comparison
        if 'Mobile' in df.columns and 'Month' in df.columns and 'Year' in df.columns:
            merged_df, new_records, status_updates = merge_with_existing_data(upload_df, existing_df)
        else:
            merged_df = upload_df
            new_records = len(upload_df)
            status_updates = 0
        
        # Upload to BigQuery
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE",
            schema=schema
        )
        
        job = client.load_table_from_dataframe(merged_df, full_table_id, job_config=job_config)
        job.result()
        
        total_rows = len(merged_df)
        
        stats = {
            'new_records': new_records,
            'status_updates': status_updates,
            'total_rows': total_rows
        }
        
        message = f"Successfully uploaded to {BQ_TABLE_GA_SF_NE}"
        
        return True, stats, message
        
    except Exception as e:
        return False, {'new_records': 0, 'status_updates': 0, 'total_rows': 0}, f"Error uploading to BigQuery: {str(e)}"
