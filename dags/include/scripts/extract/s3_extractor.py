import awswrangler as wr
import pandas as pd
from typing import List, Optional, Dict
import logging
from datetime import datetime

logger = logging.getLogger(__name__)


def read_csv_from_s3(
        s3_path: str,
        boto3_session=None, 
        **kwargs) -> pd.DataFrame:
    """
    Read CSV file from S3 using awswrangler
    
    Args:
        s3_path: Full S3 path (s3://bucket/key)
        **kwargs: Additional arguments for awswrangler read_csv
        
    Returns:
        DataFrame with CSV data
    """
    try:
        logger.info(f"Reading CSV from: {s3_path}")
        df = wr.s3.read_csv(
            s3_path, 
            boto3_session=boto3_session, 
            **kwargs
            )
        logger.info(f"Successfully read {len(df)} rows from {s3_path}")
        return df
        
    except Exception as e:
        logger.error(f"Error reading CSV from {s3_path}: {str(e)}")
        raise


def read_json_from_s3(
        s3_path: str, 
        boto3_session=None
        ) -> pd.DataFrame:
    """
    Read JSON file from S3 using awswrangler
    
    Args:
        s3_path: Full S3 path (s3://bucket/key)
        **kwargs: Additional arguments for awswrangler read_json
        
    Returns:
        DataFrame with JSON data
    """
    try:
        logger.info(f"Reading JSON from: {s3_path}")
        df = wr.s3.read_json(
            s3_path, 
            boto3_session=boto3_session,
            # lines=False
            )
        # records = list(raw_dict.values())
        # df = pd.json_normalize(records)
        logger.info(f"Successfully read {len(df)} rows from {s3_path}")
        return df
        
    except Exception as e:
        logger.error(f"Error reading JSON from {s3_path}: {str(e)}")
        raise

def read_multiple_jsons_from_s3(
        s3_paths: List[str], 
        boto3_session=None
        ) -> pd.DataFrame:
    """Read multiple JSON files (each is a dict-of-records) and normalize"""
    dfs = []
    for path in s3_paths:
        logger.info(f"Reading JSON: {path}")
        # Read JSON as dict
        obj = wr.s3.read_json(
            path, 
            boto3_session=boto3_session, 
            lines=False
            )
        records = list(obj.values())
        df = pd.json_normalize(records)
        dfs.append(df)
    
    if not dfs:
        raise ValueError("No data read from JSON files")
    return pd.concat(dfs, ignore_index=True)


def read_multiple_csvs_from_s3(
        s3_paths: List[str],
        boto3_session=None
        ) -> pd.DataFrame:
    """
    Read multiple CSV files and combine into single DataFrame
    
    Args:
        s3_paths: List of S3 paths
        
    Returns:
        Combined DataFrame
    """
    try:
        logger.info(f"Reading {len(s3_paths)} CSV files from S3")
        df = wr.s3.read_csv(
            s3_paths, 
            boto3_session=boto3_session)
        logger.info(f"Successfully combined {len(df)} total rows")
        return df
        
    except Exception as e:
        logger.error(f"Error reading multiple CSVs: {str(e)}")
        raise


def extract_customers_from_s3(
        bucket: str, 
        key: Optional[str] = None,
        boto3_session=None
        ) -> pd.DataFrame:
    """
    Extract customer data from S3
    
    Args:
        bucket: S3 bucket name
        key: Specific object key, or None to search
        
    Returns:
        DataFrame with customer data
    """
    try:
        logger.info("Extracting customer data from S3...")
        
        if key:
            s3_path = f"s3://{bucket}/{key}"
        else:
            # Search for customers file
            objects = wr.s3.list_objects(
                f"s3://{bucket}/", 
                boto3_session=boto3_session
                )
            customer_files = [obj for obj in objects if 'customers' in obj.lower() and obj.endswith('.csv')]
            
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
    """
    Extract call center logs from S3
    
    Args:
        bucket: S3 bucket name
        prefix: Prefix path in bucket
        start_date: Start date for filtering (YYYY-MM-DD)
        end_date: End date for filtering (YYYY-MM-DD)
        
    Returns:
        DataFrame with call log data
    """
    try:
        logger.info("Extracting call center logs from S3...")
        
        # List all objects
        s3_base_path = f"s3://{bucket}/{prefix}" if prefix else f"s3://{bucket}/"
        objects = wr.s3.list_objects(
            s3_base_path, 
            boto3_session=boto3_session, 
            suffix='.csv')
        
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
    """
    Extract social media complaints from S3
    
    Args:
        bucket: S3 bucket name
        prefix: Prefix path in bucket
        start_date: Start date for filtering (YYYY-MM-DD)
        end_date: End date for filtering (YYYY-MM-DD)
        
    Returns:
        DataFrame with social media data
    """
    try:
        logger.info("Extracting social media data from S3...")
        
        # List all objects
        s3_base_path = f"s3://{bucket}/{prefix}" if prefix else f"s3://{bucket}/"
        objects = wr.s3.list_objects(
            s3_base_path, 
            boto3_session=boto3_session, 
            suffix='.json')
        
        # Filter for social media files
        social_files = [obj for obj in objects if 'social' in obj.lower()]
        
        if not social_files:
            raise FileNotFoundError("No social media JSON files found")
        
        logger.info(f"Found {len(social_files)} social media files")
        
        # Read all JSON files
        # dataframes = []
        # for s3_path in social_files:
        #     df = read_json_from_s3(s3_path, boto3_session=boto3_session)  
        #     if not df.empty:
        #         dataframes.append(df)
        
        # if not dataframes:
        #     return pd.DataFrame()
        df = read_json_from_s3(social_files, boto3_session=boto3_session)
        
        # df = pd.concat(dataframes, ignore_index=True)
        
        # Clean column names
        df.columns = df.columns.str.lower().str.strip().str.replace(' ', '_')
        
        # Add metadata
        df = add_extraction_metadata(df, 's3_social_media', s3_base_path)
        
        logger.info(f"Extracted {len(df)} social media records")
        return df
        
    except Exception as e:
        logger.error(f"Error extracting social media data: {str(e)}")
        raise


def add_extraction_metadata(
        df: pd.DataFrame, 
        source: str, 
        location: str) -> pd.DataFrame:
    """
    Add extraction metadata columns to DataFrame
    
    Args:
        df: DataFrame to add metadata to
        source: Source system name
        location: Source location (S3 path, etc.)
        
    Returns:
        DataFrame with metadata columns
    """
    df = df.copy()
    df['_extraction_timestamp'] = datetime.now()
    df['_source_system'] = source
    df['_source_location'] = location
    df['_row_count'] = len(df)
    
    return df


