import pandas as pd
from typing import List, Optional, Dict
import logging
from datetime import datetime
import io
import json
from pathlib import Path

logger = logging.getLogger(__name__)


def read_csv_from_s3(
    s3_path: str,
    boto3_session=None,
    **kwargs
) -> pd.DataFrame:
    """Read CSV file from S3 using boto3"""
    try:
        # Parse S3 path
        parts = s3_path.replace("s3://", "").split("/", 1)
        bucket = parts[0]
        key = parts[1] if len(parts) > 1 else ""
        
        logger.info(f"Reading CSV from: {s3_path}")
        
        # Get S3 client
        if boto3_session:
            s3_client = boto3_session.client('s3')
        else:
            from airflow.hooks.base import BaseHook
            import boto3
            conn = BaseHook.get_connection('aws_source')
            s3_client = boto3.client(
                's3',
                aws_access_key_id=conn.login,
                aws_secret_access_key=conn.password,
                region_name='eu-north-1'
            )
        
        # Download file
        obj = s3_client.get_object(Bucket=bucket, Key=key)
        df = pd.read_csv(io.BytesIO(obj['Body'].read()), **kwargs)
        
        logger.info(f"Successfully read {len(df)} rows from {s3_path}")
        return df
        
    except Exception as e:
        logger.error(f"Error reading CSV from {s3_path}: {str(e)}")
        raise


def read_json_from_s3(
    s3_path: str,
    boto3_session=None
) -> pd.DataFrame:
    """Read JSON file from S3 using boto3"""
    try:
        # Parse S3 path
        if isinstance(s3_path, list):
            # If list, read first file (or loop through all)
            return read_multiple_jsons_from_s3(s3_path, boto3_session)
        
        parts = s3_path.replace("s3://", "").split("/", 1)
        bucket = parts[0]
        key = parts[1] if len(parts) > 1 else ""
        
        logger.info(f"Reading JSON from: {s3_path}")
        
        # Get S3 client
        if boto3_session:
            s3_client = boto3_session.client('s3')
        else:
            from airflow.hooks.base import BaseHook
            import boto3
            conn = BaseHook.get_connection('aws_source')
            s3_client = boto3.client(
                's3',
                aws_access_key_id=conn.login,
                aws_secret_access_key=conn.password,
                region_name='eu-north-1'
            )
        
        # Download file
        obj = s3_client.get_object(Bucket=bucket, Key=key)
        json_data = json.loads(obj['Body'].read().decode('utf-8'))
        
        # Convert to DataFrame
        if isinstance(json_data, dict):
            # If dict of records, normalize it
            records = list(json_data.values())
            df = pd.json_normalize(records)
        elif isinstance(json_data, list):
            df = pd.DataFrame(json_data)
        else:
            df = pd.DataFrame([json_data])
        
        logger.info(f"Successfully read {len(df)} rows from {s3_path}")
        return df
        
    except Exception as e:
        logger.error(f"Error reading JSON from {s3_path}: {str(e)}")
        raise


def read_multiple_jsons_from_s3(
    s3_paths: List[str],
    boto3_session=None
) -> pd.DataFrame:
    """Read multiple JSON files and combine"""
    dfs = []
    for path in s3_paths:
        logger.info(f"Reading JSON: {path}")
        df = read_json_from_s3(path, boto3_session=boto3_session)
        if not df.empty:
            dfs.append(df)
    
    if not dfs:
        raise ValueError("No data read from JSON files")
    return pd.concat(dfs, ignore_index=True)


def read_multiple_csvs_from_s3(
    s3_paths: List[str],
    boto3_session=None
) -> pd.DataFrame:
    """Read multiple CSV files and combine"""
    try:
        logger.info(f"Reading {len(s3_paths)} CSV files from S3")
        dfs = []
        for path in s3_paths:
            df = read_csv_from_s3(path, boto3_session=boto3_session)
            dfs.append(df)
        
        combined_df = pd.concat(dfs, ignore_index=True)
        logger.info(f"Successfully combined {len(combined_df)} total rows")
        return combined_df
        
    except Exception as e:
        logger.error(f"Error reading multiple CSVs: {str(e)}")
        raise


def list_s3_objects(bucket: str, prefix: str = "", suffix: str = "", boto3_session=None) -> List[str]:
    """List objects in S3 bucket"""
    try:
        # Get S3 client
        if boto3_session:
            s3_client = boto3_session.client('s3')
        else:
            from airflow.hooks.base import BaseHook
            import boto3
            conn = BaseHook.get_connection('aws_source')
            s3_client = boto3.client(
                's3',
                aws_access_key_id=conn.login,
                aws_secret_access_key=conn.password,
                region_name='eu-north-1'
            )
        
        # List objects
        paginator = s3_client.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=bucket, Prefix=prefix)
        
        objects = []
        for page in pages:
            if 'Contents' in page:
                for obj in page['Contents']:
                    key = obj['Key']
                    if not suffix or key.endswith(suffix):
                        objects.append(f"s3://{bucket}/{key}")
        
        return objects
        
    except Exception as e:
        logger.error(f"Error listing S3 objects: {str(e)}")
        raise


def extract_customers_from_s3(
    bucket: str,
    key: Optional[str] = None,
    boto3_session=None
) -> pd.DataFrame:
    """Extract customer data from S3"""
    try:
        logger.info("Extracting customer data from S3...")
        
        if key:
            s3_path = f"s3://{bucket}/{key}"
        else:
            # Search for customers file
            objects = list_s3_objects(bucket, suffix='.csv', boto3_session=boto3_session)
            customer_files = [obj for obj in objects if 'customers' in obj.lower()]
            
            if not customer_files:
                raise FileNotFoundError("No customer CSV files found in bucket")
            
            s3_path = customer_files[0]
        
        # Read CSV
        df = read_csv_from_s3(s3_path, boto3_session=boto3_session)
        
        # Clean column names
        df.columns = df.columns.str.lower().str.strip().str.replace(' ', '_')
        
        # Add metadata
        df = add_extraction_metadata(df, 's3_customers', s3_path)
        
        logger.info(f"Extracted {len(df)} customer records")
        return df
        
    except Exception as e:
        logger.error(f"Error extracting customer data: {str(e)}")
        raise


def extract_call_logs_from_s3(
    bucket: str,
    prefix: str = "",
    boto3_session=None
) -> pd.DataFrame:
    """Extract call center logs from S3"""
    try:
        logger.info("Extracting call center logs from S3...")
        
        # List all CSV objects
        objects = list_s3_objects(bucket, prefix=prefix, suffix='.csv', boto3_session=boto3_session)
        
        # Filter for call log files
        call_log_files = [obj for obj in objects if 'call' in obj.lower() or 'logs' in obj.lower()]
        
        if not call_log_files:
            raise FileNotFoundError("No call log CSV files found")
        
        logger.info(f"Found {len(call_log_files)} call log files")
        
        # Read all files
        df = read_multiple_csvs_from_s3(call_log_files, boto3_session=boto3_session)
        
        # Clean column names
        df.columns = df.columns.str.lower().str.strip().str.replace(' ', '_')
        
        # Add metadata
        s3_base_path = f"s3://{bucket}/{prefix}" if prefix else f"s3://{bucket}/"
        df = add_extraction_metadata(df, 's3_call_logs', s3_base_path)
        
        logger.info(f"Extracted {len(df)} call log records")
        return df
        
    except Exception as e:
        logger.error(f"Error extracting call logs: {str(e)}")
        raise


def extract_social_media_from_s3(
    bucket: str,
    prefix: str = "",
    boto3_session=None
) -> pd.DataFrame:
    """Extract social media complaints from S3"""
    try:
        logger.info("Extracting social media data from S3...")
        
        # List all JSON objects
        objects = list_s3_objects(bucket, prefix=prefix, suffix='.json', boto3_session=boto3_session)
        
        # Filter for social media files
        social_files = [obj for obj in objects if 'social' in obj.lower()]
        
        if not social_files:
            raise FileNotFoundError("No social media JSON files found")
        
        logger.info(f"Found {len(social_files)} social media files")
        
        # Read all JSON files
        df = read_multiple_jsons_from_s3(social_files, boto3_session=boto3_session)
        
        # Clean column names
        df.columns = df.columns.str.lower().str.strip().str.replace(' ', '_')
        
        # Add metadata
        s3_base_path = f"s3://{bucket}/{prefix}" if prefix else f"s3://{bucket}/"
        df = add_extraction_metadata(df, 's3_social_media', s3_base_path)
        
        logger.info(f"Extracted {len(df)} social media records")
        return df
        
    except Exception as e:
        logger.error(f"Error extracting social media data: {str(e)}")
        raise


def add_extraction_metadata(
    df: pd.DataFrame,
    source: str,
    location: str
) -> pd.DataFrame:
    """Add extraction metadata columns to DataFrame"""
    df = df.copy()
    df['_extraction_timestamp'] = datetime.now()
    df['_source_system'] = source
    df['_source_location'] = location
    df['_row_count'] = len(df)
    
    return df


def extract_csv_files(
    local_dir: str,
    name_contains: Optional[str] = None
) -> List[str]:
    """
    Generic CSV extractor from local filesystem.
    Returns list of CSV file paths.
    """
    base = Path(local_dir)

    if not base.exists():
        raise FileNotFoundError(f"{local_dir} does not exist")

    csv_files = list(base.glob("*.csv"))

    if name_contains:
        csv_files = [
            p for p in csv_files
            if name_contains.lower() in p.name.lower()
        ]

    if not csv_files:
        raise FileNotFoundError("No CSV files found")

    files = [str(p) for p in csv_files]

    logger.info(f"Found {len(files)} CSV files")
    return files


def convert_csv_to_parquet(csv_path: str) -> str:
    csv_path = Path(csv_path)
    parquet_path = csv_path.with_suffix(".parquet")

    df = pd.read_csv(csv_path)

    df.columns = (
        df.columns
        .str.lower()
        .str.strip()
        .str.replace(" ", "_")
    )

    df.to_parquet(
        parquet_path,
        engine="pyarrow",
        compression="snappy",
        index=False
    )

    return str(parquet_path)


def fetch_files(local_dir: str):
    """Fetch CSV or JSON files from local directory"""
    path = Path(local_dir)
    if not path.exists():
        raise FileNotFoundError(f"{local_dir} does not exist")

    files = list(path.glob("*"))
    files = [f for f in files if f.suffix.lower() in [".csv", ".json"]]
    if not files:
        raise FileNotFoundError(f"No CSV or JSON files found in {local_dir}")
    return [str(f) for f in files]

