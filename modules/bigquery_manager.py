"""
BigQuery Manager module for data storage and retrieval.
Uses SQL MERGE with temporary staging tables for efficient upserts.
"""
import json
import tempfile
from typing import Tuple, Optional, List

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

# SQL fragment: maps a Status string to its numeric priority
_STATUS_PRIORITY_SQL = """
    CASE {col}
        WHEN 'Converted' THEN 5
        WHEN 'Closed'    THEN 4
        WHEN 'Quoted'    THEN 3
        WHEN 'Open'      THEN 2
        WHEN 'Prospect'  THEN 1
        ELSE 0
    END
"""


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


# ---------------------------------------------------------------------------
# Internal helpers for temp-table MERGE approach
# ---------------------------------------------------------------------------

def _upload_to_temp_table(
    client: bigquery.Client,
    df: pd.DataFrame,
    temp_table_id: str,
    schema: list
) -> None:
    """
    Upload a DataFrame to a temporary staging table using WRITE_TRUNCATE.
    Creates the table if it doesn't exist.

    Args:
        client: BigQuery client
        df: DataFrame to upload
        temp_table_id: Full table ID for the temp table
        schema: Schema for the temp table
    """
    # Drop temp table if it already exists (clean slate)
    try:
        client.delete_table(temp_table_id)
    except NotFound:
        pass

    # Create temp table
    table = bigquery.Table(temp_table_id, schema=schema)
    client.create_table(table)

    # Upload
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        schema=schema
    )
    job = client.load_table_from_dataframe(df, temp_table_id, job_config=job_config)
    job.result()


def _get_merge_stats(
    client: bigquery.Client,
    main_table: str,
    temp_table: str,
    has_status_col: bool = True
) -> dict:
    """
    Compute merge statistics BEFORE the merge executes:
    - new_records: rows in temp that don't exist in main (by Mobile+Month+Year key)
    - status_updates: breakdown of status transitions that will occur
    - total_rows: projected total after merge

    Args:
        client: BigQuery client
        main_table: Full ID of the main table
        temp_table: Full ID of the temp staging table
        has_status_col: Whether the table has a Status column

    Returns:
        stats dict with keys: new_records, status_updates, total_rows
    """
    stats = {
        'new_records': 0,
        'status_updates': {},
        'total_rows': 0
    }

    # Check if main table has data
    count_query = f"SELECT COUNT(*) as cnt FROM `{main_table}`"
    try:
        count_result = client.query(count_query).result()
        main_count = list(count_result)[0].cnt
    except Exception:
        main_count = 0

    # If main table is empty, all temp rows are new
    if main_count == 0:
        temp_count_query = f"SELECT COUNT(*) as cnt FROM `{temp_table}`"
        temp_result = client.query(temp_count_query).result()
        temp_count = list(temp_result)[0].cnt
        stats['new_records'] = temp_count
        stats['total_rows'] = temp_count
        return stats

    # Count new records (in temp but not in main)
    new_records_query = f"""
        SELECT COUNT(*) as new_records
        FROM `{temp_table}` t
        LEFT JOIN `{main_table}` m
            ON t.Mobile = m.Mobile AND t.Month = m.Month AND t.Year = m.Year
        WHERE m.Mobile IS NULL
    """
    result = client.query(new_records_query).result()
    stats['new_records'] = list(result)[0].new_records

    # Count status transitions (only for records where source will overwrite target)
    if has_status_col:
        status_priority_source = _STATUS_PRIORITY_SQL.format(col="t.Status")
        status_priority_target = _STATUS_PRIORITY_SQL.format(col="m.Status")

        status_query = f"""
            SELECT
                m.Status as old_status,
                t.Status as new_status,
                COUNT(*) as cnt
            FROM `{temp_table}` t
            JOIN `{main_table}` m
                ON t.Mobile = m.Mobile AND t.Month = m.Month AND t.Year = m.Year
            WHERE m.Status != t.Status
              AND (
                  t.Date > m.Date
                  OR (t.Date = m.Date AND ({status_priority_source}) >= ({status_priority_target}))
              )
            GROUP BY 1, 2
        """
        try:
            rows = client.query(status_query).result()
            for row in rows:
                change_key = f"{row.old_status} → {row.new_status}"
                stats['status_updates'][change_key] = row.cnt
        except Exception:
            pass  # Status tracking is best-effort

    # Projected total rows after merge
    # = existing rows that won't be touched + new rows from temp
    # (updates replace existing rows, so count stays same for those)
    stats['total_rows'] = main_count + stats['new_records']

    return stats


def _build_merge_sql(
    main_table: str,
    temp_table: str,
    columns: List[str],
    has_status_col: bool = True
) -> str:
    """
    Build the SQL MERGE statement that implements:
    - Key: Mobile + Month + Year
    - If dates differ: keep the record with the LATEST date
    - If dates are same: keep the record with higher STATUS PRIORITY
    - New records (not in main): INSERT

    Args:
        main_table: Full ID of the main table
        temp_table: Full ID of the temp staging table
        columns: List of column names
        has_status_col: Whether the table has a Status column

    Returns:
        SQL MERGE string
    """
    # Build the UPDATE SET clause (all columns except the key columns)
    key_cols = {'Mobile', 'Month', 'Year'}
    update_cols = [c for c in columns if c not in key_cols]
    update_set = ",\n        ".join(
        [f"target.`{c}` = source.`{c}`" for c in update_cols]
    )

    # Build the INSERT columns and values
    insert_cols = ", ".join([f"`{c}`" for c in columns])
    insert_vals = ", ".join([f"source.`{c}`" for c in columns])

    # Build the WHEN MATCHED condition
    if has_status_col:
        status_priority_source = _STATUS_PRIORITY_SQL.format(col="source.Status")
        status_priority_target = _STATUS_PRIORITY_SQL.format(col="target.Status")

        match_condition = f"""
        source.Date > target.Date
        OR (
            source.Date = target.Date
            AND ({status_priority_source}) >= ({status_priority_target})
        )
        """
    else:
        # Without Status column, just use date comparison
        match_condition = "source.Date > target.Date"

    merge_sql = f"""
    MERGE `{main_table}` AS target
    USING `{temp_table}` AS source
    ON target.Mobile = source.Mobile
       AND target.Month = source.Month
       AND target.Year = source.Year

    WHEN MATCHED AND (
        {match_condition}
    ) THEN UPDATE SET
        {update_set}

    WHEN NOT MATCHED BY TARGET THEN
        INSERT ({insert_cols})
        VALUES ({insert_vals})
    """

    return merge_sql


def _execute_merge_via_temp_table(
    client: bigquery.Client,
    df: pd.DataFrame,
    table_name: str,
    schema: list,
    columns: List[str],
    has_status_col: bool = True
) -> Tuple[bool, dict, str]:
    """
    Core merge routine shared by both upload functions.

    1. Upload df to a temp staging table
    2. Compute merge stats (new records, status changes)
    3. Execute SQL MERGE from temp → main
    4. Drop temp table
    5. Return stats

    Args:
        client: BigQuery client
        df: Prepared DataFrame to upload
        table_name: Name of the main table
        schema: BigQuery schema
        columns: Ordered list of column names
        has_status_col: Whether Status column exists

    Returns:
        Tuple of (success, stats_dict, message)
    """
    main_table_id = ensure_table_exists(client, table_name, schema)
    temp_table_name = f"_temp_{table_name}"
    temp_table_id = f"{client.project}.{BQ_DATASET_ID}.{temp_table_name}"

    try:
        # Step 1: Upload new data to temp table
        _upload_to_temp_table(client, df, temp_table_id, schema)

        # Step 2: Get merge stats before executing
        stats = _get_merge_stats(client, main_table_id, temp_table_id, has_status_col)

        # Step 3: Execute MERGE
        merge_sql = _build_merge_sql(main_table_id, temp_table_id, columns, has_status_col)
        merge_job = client.query(merge_sql)
        merge_job.result()  # Wait for completion

        # Update total_rows with actual count after merge
        count_query = f"SELECT COUNT(*) as cnt FROM `{main_table_id}`"
        count_result = client.query(count_query).result()
        stats['total_rows'] = list(count_result)[0].cnt

        # Step 4: Cleanup temp table
        try:
            client.delete_table(temp_table_id)
        except NotFound:
            pass

        message = f"Successfully uploaded to {table_name}"
        return True, stats, message

    except Exception as e:
        # Cleanup temp table on error
        try:
            client.delete_table(temp_table_id)
        except Exception:
            pass
        return False, {'new_records': 0, 'status_updates': {}, 'total_rows': 0}, f"Error uploading to BigQuery: {str(e)}"


# ---------------------------------------------------------------------------
# Public upload functions (same signatures as before)
# ---------------------------------------------------------------------------

def upload_ga_sf_data(
    df: pd.DataFrame
) -> Tuple[bool, dict, str]:
    """
    Upload GA-SF mapped data to BigQuery using SQL MERGE with a temp table.
    For each Mobile-Month-Year key, keeps the record with the LATEST date.
    On same date, keeps the record with the higher STATUS PRIORITY.

    Args:
        df: DataFrame to upload

    Returns:
        Tuple of (success, stats_dict, message)
        stats_dict contains: new_records, status_updates, total_rows
    """
    try:
        client = get_bq_client()

        target_columns = [field.name for field in GA_SF_SCHEMA]
        upload_df = prepare_upload_df(df, target_columns)

        return _execute_merge_via_temp_table(
            client=client,
            df=upload_df,
            table_name=BQ_TABLE_GA_SF,
            schema=GA_SF_SCHEMA,
            columns=target_columns,
            has_status_col=True
        )

    except Exception as e:
        return False, {'new_records': 0, 'status_updates': {}, 'total_rows': 0}, f"Error uploading to BigQuery: {str(e)}"


def upload_ga_sf_ne_data(
    df: pd.DataFrame
) -> Tuple[bool, dict, str]:
    """
    Upload GA-SF-NE mapped data to BigQuery using SQL MERGE with a temp table.
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
        ensure_table_exists(client, BQ_TABLE_GA_SF_NE, schema)

        # Prepare upload DataFrame
        upload_df = df.copy()
        if 'Date' in upload_df.columns:
            upload_df['Date'] = pd.to_datetime(upload_df['Date']).dt.date

        columns = list(upload_df.columns)
        has_status = 'Status' in columns
        has_merge_keys = all(c in columns for c in ['Mobile', 'Month', 'Year'])

        if has_merge_keys:
            return _execute_merge_via_temp_table(
                client=client,
                df=upload_df,
                table_name=BQ_TABLE_GA_SF_NE,
                schema=schema,
                columns=columns,
                has_status_col=has_status
            )
        else:
            # Fallback: no merge keys available, do a simple append
            full_table_id = f"{client.project}.{BQ_DATASET_ID}.{BQ_TABLE_GA_SF_NE}"
            job_config = bigquery.LoadJobConfig(
                write_disposition="WRITE_TRUNCATE",
                schema=schema
            )
            job = client.load_table_from_dataframe(upload_df, full_table_id, job_config=job_config)
            job.result()

            stats = {
                'new_records': len(upload_df),
                'status_updates': {},
                'total_rows': len(upload_df)
            }
            return True, stats, f"Successfully uploaded to {BQ_TABLE_GA_SF_NE}"

    except Exception as e:
        return False, {'new_records': 0, 'status_updates': {}, 'total_rows': 0}, f"Error uploading to BigQuery: {str(e)}"
