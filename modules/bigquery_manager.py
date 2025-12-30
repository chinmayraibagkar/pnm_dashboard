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
    bigquery.SchemaField("Sessions", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("Source_Medium", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("Session_Campaign", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("Engaged_Sessions", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("Keyword", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("Operating_System", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("Mobile", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("Final_Source", "STRING", mode="NULLABLE"),
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


def find_status_updates(
    new_df: pd.DataFrame, 
    existing_df: pd.DataFrame
) -> Tuple[int, pd.DataFrame]:
    """
    Find records where status has changed for the same Mobile-Month-Year key.
    
    Args:
        new_df: New data to upload
        existing_df: Existing data in BigQuery
        
    Returns:
        Tuple of (status_update_count, merged_df_with_latest_status)
    """
    if existing_df.empty:
        return 0, new_df
    
    # Ensure proper types
    for df in [new_df, existing_df]:
        df['Mobile'] = df['Mobile'].astype(str)
        df['Month'] = df['Month'].astype(int)
        df['Year'] = df['Year'].astype(int)
    
    # Create unique key
    new_df['_key'] = new_df['Mobile'] + '_' + new_df['Month'].astype(str) + '_' + new_df['Year'].astype(str)
    existing_df['_key'] = existing_df['Mobile'] + '_' + existing_df['Month'].astype(str) + '_' + existing_df['Year'].astype(str)
    
    # Find overlapping keys
    existing_keys = set(existing_df['_key'].unique())
    new_keys = set(new_df['_key'].unique())
    overlapping_keys = existing_keys.intersection(new_keys)
    
    if not overlapping_keys:
        new_df = new_df.drop(columns=['_key'])
        return 0, new_df
    
    # Check for status changes
    status_updates = 0
    
    existing_status = existing_df[existing_df['_key'].isin(overlapping_keys)][['_key', 'Status']].drop_duplicates(subset=['_key'])
    new_status = new_df[new_df['_key'].isin(overlapping_keys)][['_key', 'Status']].drop_duplicates(subset=['_key'])
    
    merged_status = existing_status.merge(new_status, on='_key', suffixes=('_old', '_new'))
    status_updates = (merged_status['Status_old'] != merged_status['Status_new']).sum()
    
    # Clean up
    new_df = new_df.drop(columns=['_key'])
    
    return status_updates, new_df


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
) -> Tuple[bool, int, str]:
    """
    Upload GA-SF mapped data to BigQuery with status update detection.
    Uses WRITE_TRUNCATE to replace existing data.
    
    Args:
        df: DataFrame to upload
        
    Returns:
        Tuple of (success, status_updates_count, message)
    """
    try:
        client = get_bq_client()
        
        # Ensure table exists
        full_table_id = ensure_table_exists(client, BQ_TABLE_GA_SF, GA_SF_SCHEMA)
        
        # Get existing data for comparison
        existing_df = get_existing_data(client, BQ_TABLE_GA_SF)
        
        # Find status updates
        status_updates, df = find_status_updates(df, existing_df)
        
        # Prepare upload DataFrame
        target_columns = [field.name for field in GA_SF_SCHEMA]
        upload_df = prepare_upload_df(df, target_columns)
        
        # Merge with existing data (keep latest status for each key)
        if not existing_df.empty:
            existing_df['_key'] = existing_df['Mobile'].astype(str) + '_' + existing_df['Month'].astype(str) + '_' + existing_df['Year'].astype(str)
            upload_df['_key'] = upload_df['Mobile'].astype(str) + '_' + upload_df['Month'].astype(str) + '_' + upload_df['Year'].astype(str)
            
            # Get keys that are only in existing (not in new)
            new_keys = set(upload_df['_key'].unique())
            only_existing_keys = set(existing_df['_key'].unique()) - new_keys
            
            # Keep records from existing that are not in new
            existing_to_keep = existing_df[existing_df['_key'].isin(only_existing_keys)]
            if not existing_to_keep.empty:
                existing_to_keep = prepare_upload_df(existing_to_keep.drop(columns=['_key']), target_columns)
                upload_df = upload_df.drop(columns=['_key'])
                upload_df = pd.concat([upload_df, existing_to_keep], ignore_index=True)
            else:
                upload_df = upload_df.drop(columns=['_key'])
        
        # Upload to BigQuery
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE",
            schema=GA_SF_SCHEMA
        )
        
        job = client.load_table_from_dataframe(upload_df, full_table_id, job_config=job_config)
        job.result()
        
        message = f"Successfully uploaded {len(upload_df)} rows to {BQ_TABLE_GA_SF}"
        if status_updates > 0:
            message += f" ({status_updates} status updates detected)"
        
        return True, status_updates, message
        
    except Exception as e:
        return False, 0, f"Error uploading to BigQuery: {str(e)}"


def upload_ga_sf_ne_data(
    df: pd.DataFrame
) -> Tuple[bool, int, str]:
    """
    Upload GA-SF-NE mapped data to BigQuery with status update detection.
    Dynamically creates schema based on DataFrame columns.
    
    Args:
        df: DataFrame to upload
        
    Returns:
        Tuple of (success, status_updates_count, message)
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
        
        # Find status updates (if Status column exists)
        status_updates = 0
        if 'Status' in df.columns and not existing_df.empty and 'Status' in existing_df.columns:
            status_updates, df = find_status_updates(df, existing_df)
        
        # Prepare upload DataFrame
        upload_df = df.copy()
        if 'Date' in upload_df.columns:
            upload_df['Date'] = pd.to_datetime(upload_df['Date']).dt.date
        
        # Merge with existing data
        if not existing_df.empty and 'Mobile' in df.columns and 'Month' in df.columns and 'Year' in df.columns:
            existing_df['_key'] = existing_df['Mobile'].astype(str) + '_' + existing_df['Month'].astype(str) + '_' + existing_df['Year'].astype(str)
            upload_df['_key'] = upload_df['Mobile'].astype(str) + '_' + upload_df['Month'].astype(str) + '_' + upload_df['Year'].astype(str)
            
            new_keys = set(upload_df['_key'].unique())
            only_existing_keys = set(existing_df['_key'].unique()) - new_keys
            
            existing_to_keep = existing_df[existing_df['_key'].isin(only_existing_keys)]
            if not existing_to_keep.empty:
                existing_to_keep = existing_to_keep.drop(columns=['_key'])
                if 'Date' in existing_to_keep.columns:
                    existing_to_keep['Date'] = pd.to_datetime(existing_to_keep['Date']).dt.date
                upload_df = upload_df.drop(columns=['_key'])
                upload_df = pd.concat([upload_df, existing_to_keep], ignore_index=True)
            else:
                upload_df = upload_df.drop(columns=['_key'])
        
        # Upload to BigQuery
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE",
            schema=schema
        )
        
        job = client.load_table_from_dataframe(upload_df, full_table_id, job_config=job_config)
        job.result()
        
        message = f"Successfully uploaded {len(upload_df)} rows to {BQ_TABLE_GA_SF_NE}"
        if status_updates > 0:
            message += f" ({status_updates} status updates detected)"
        
        return True, status_updates, message
        
    except Exception as e:
        return False, 0, f"Error uploading to BigQuery: {str(e)}"
