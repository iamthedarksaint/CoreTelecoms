import awswrangler as wr
import pandas as pd
from typing import Dict, Optional
import logging
from datetime import datetime

logger = logging.getLogger(__name__)


def write_parquet_to_s3(
    df: pd.DataFrame,
    bucket: str,
    path: str,
    dataset: bool = False,
    partition_cols: Optional[list] = None,
    mode: str = "overwrite_partitions",
    boto3_session=None
) -> Dict:
    """
    Write DataFrame to S3 as Parquet
    
    Args:
        df: DataFrame to write
        bucket: S3 bucket name
        path: S3 path (without bucket, e.g., 'raw/customers/')
        dataset: If True, write as partitioned dataset
        partition_cols: Columns to partition by
        mode: Write mode ('overwrite', 'append', 'overwrite_partitions')
        
    Returns:
        Dictionary with write results
    """
    try:
        s3_path = f"s3://{bucket}/{path}"
        logger.info(f"Writing {len(df)} rows to {s3_path}")

        result = wr.s3.to_parquet(
                df=df,
                path=s3_path,
                dataset=dataset,
                partition_cols=partition_cols,
                mode=mode,
                compression='snappy',
                boto3_session=boto3_session
        )
        
        logger.info(f"Successfully wrote data to {s3_path}")
        return {'status': 'success', 'path': s3_path, 'paths': result['paths']}
        
    except Exception as e:
        logger.error(f"Error writing to S3: {str(e)}")
        raise


def write_to_raw_layer(
    df: pd.DataFrame,
    bucket: str,
    source_name: str,
    execution_date: Optional[str] = None,
    boto3_session=None,
        ) -> Dict:
    """
    Write data to raw/bronze layer with date partitioning
    Path structure: raw/{source_name}/year=YYYY/month=MM/day=DD/
    
    Args:
        df: DataFrame to write
        bucket: S3 bucket name
        source_name: Source system name (e.g., 'customers', 'call_logs')
        execution_date: Date for partitioning (YYYY-MM-DD), defaults to today
        
    Returns:
        Dictionary with write results
    """
    try:
        logger.info("=" * 60)
        logger.info(f"WRITING TO RAW LAYER: {source_name}")
        logger.info("=" * 60)
        
        # Add execution date if not provided
        if execution_date is None:
            execution_date = datetime.now().strftime('%Y-%m-%d')
        
        # Add partition columns
        df_with_partitions = df.copy()
        execution_dt = pd.to_datetime(execution_date)
        df_with_partitions['year'] = execution_dt.year
        df_with_partitions['month'] = execution_dt.month
        df_with_partitions['day'] = execution_dt.day

        
        # Write to S3
        path = f"raw/{source_name}/"
        result = write_parquet_to_s3(
            df=df_with_partitions,
            bucket=bucket,
            path=path,
            dataset=True,
            partition_cols=['year', 'month', 'day'],
            mode='overwrite_partitions',
            boto3_session=boto3_session
        )
        
        logger.info(f"Raw layer write complete")
        logger.info(f"Location: s3://{bucket}/{path}")
        logger.info(f"Records written: {len(df)}")
        logger.info("=" * 60)
        
        return result
        
    except Exception as e:
        logger.error(f"Error writing to raw layer: {str(e)}")
        raise


def write_to_staging_layer(
    df: pd.DataFrame,
    bucket: str,
    table_name: str,
    execution_date: Optional[str] = None,
    boto3_session=None,
) -> Dict:
    """
    Write data to staging/silver layer
    Path structure: staging/{table_name}/year=YYYY/month=MM/day=DD/
    
    Args:
        df: DataFrame to write
        bucket: S3 bucket name
        table_name: Table name
        execution_date: Date for partitioning (YYYY-MM-DD)
        
    Returns:
        Dictionary with write results
    """
    try:
        logger.info(f"Writing to staging layer: {table_name}")
        
        # Add execution date if not provided
        if execution_date is None:
            execution_date = datetime.now().strftime('%Y-%m-%d')
        
        # Add partition columns
        df_with_partitions = df.copy()
        execution_dt = pd.to_datetime(execution_date)
        df_with_partitions['year'] = execution_dt.year
        df_with_partitions['month'] = execution_dt.month
        df_with_partitions['day'] = execution_dt.day
        
        # Write to S3
        path = f"staging/{table_name}/"
        result = write_parquet_to_s3(
            df=df_with_partitions,
            bucket=bucket,
            path=path,
            dataset=True,
            partition_cols=['year', 'month', 'day'],
            mode='overwrite_partitions',
            boto3_session=boto3_session
        )
        
        logger.info(f"Staging layer write complete for {table_name}")
        return result
        
    except Exception as e:
        logger.error(f"Error writing to staging layer: {str(e)}")
        raise


def write_to_processed_layer(
    df: pd.DataFrame,
    bucket: str,
    table_name: str,
    partition_cols: Optional[list] = None,
    boto3_session=None,
) -> Dict:
    """
    Write data to processed/gold layer
    Path structure: processed/{table_name}/
    
    Args:
        df: DataFrame to write
        bucket: S3 bucket name
        table_name: Table name
        partition_cols: Optional columns to partition by
        
    Returns:
        Dictionary with write results
    """
    try:
        logger.info(f"Writing to processed layer: {table_name}")
        
        # Write to S3
        path = f"processed/{table_name}/"
        result = write_parquet_to_s3(
            df=df,
            bucket=bucket,
            path=path,
            dataset=True if partition_cols else False,
            partition_cols=partition_cols,
            mode='overwrite',
            boto3_session=boto3_session,
        )
        
        logger.info(f"Processed layer write complete for {table_name}")
        return result
        
    except Exception as e:
        logger.error(f"Error writing to processed layer: {str(e)}")
        raise


def save_extraction_metadata(
    bucket: str,
    source_name: str,
    metadata: Dict,
    execution_date: Optional[str] = None,
    boto3_session=None,
) -> Dict:
    """
    Save extraction metadata as JSON
    
    Args:
        bucket: S3 bucket name
        source_name: Source system name
        metadata: Metadata dictionary
        execution_date: Execution date
        
    Returns:
        Dictionary with write results
    """
    try:
        if execution_date is None:
            execution_date = datetime.now().strftime('%Y-%m-%d')
        
        # Add timestamp
        metadata['extraction_timestamp'] = datetime.utcnow().isoformat()
        metadata['execution_date'] = execution_date
        
        # Convert to DataFrame
        df_metadata = pd.DataFrame([metadata])
        
        # Write to S3
        s3_path = f"s3://{bucket}/metadata/{source_name}/{execution_date}.json"
        
        wr.s3.to_json(
            df=df_metadata,
            path=s3_path,
            orient='records',
            lines=True,
            boto3_session=boto3_session
        )
        
        logger.info(f"Metadata saved to {s3_path}")
        return {'status': 'success', 'path': s3_path}
        
    except Exception as e:
        logger.error(f"Error saving metadata: {str(e)}")
        raise


