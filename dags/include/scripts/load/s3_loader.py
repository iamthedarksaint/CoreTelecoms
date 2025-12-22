import pandas as pd
from typing import Dict, Optional
import logging
from datetime import datetime
import io
import json
from airflow.hooks.base import BaseHook
import boto3
from pathlib import Path
import awswrangler as wr
import re

from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook

dest_hook = AwsBaseHook(aws_conn_id="aws_dest", client_type="s3")
dest_hook.get_session()



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
    Write DataFrame to S3 as Parquet using boto3
    """
    try:
        s3_path = f"s3://{bucket}/{path}"
        logger.info(f"Writing {len(df)} rows to {s3_path}")

        # Convert DataFrame to parquet in memory
        parquet_buffer = io.BytesIO()
        df.to_parquet(parquet_buffer, index=False, engine='pyarrow', compression='snappy')
        parquet_buffer.seek(0)
        
        # Get S3 client from session
        if boto3_session:
            s3_client = boto3_session.client('s3')
        else:
            conn = BaseHook.get_connection('aws_dest')
            s3_client = boto3.client(
                's3',
                aws_access_key_id=conn.login,
                aws_secret_access_key=conn.password,
                region_name='eu-north-1'
            )
        
        # Generate key based on partition columns
        if dataset and partition_cols:
            # Extract partition values from first row
            partition_path = "/".join([f"{col}={df[col].iloc[0]}" for col in partition_cols])
            key = f"{path}{partition_path}/data.parquet"
        else:
            key = f"{path}data.parquet"
        
        # Upload to S3
        s3_client.put_object(
            Bucket=bucket,
            Key=key,
            Body=parquet_buffer.getvalue()
        )
        
        logger.info(f"Successfully wrote data to s3://{bucket}/{key}")
        return {'status': 'success', 'path': s3_path, 'paths': [f"s3://{bucket}/{key}"]}
        
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
    """
    try:
        logger.info("=" * 60)
        logger.info(f"WRITING TO RAW LAYER: {source_name}")
        logger.info("=" * 60)
        
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


def save_extraction_metadata(
    bucket: str,
    source_name: str,
    metadata: Dict,
    execution_date: Optional[str] = None,
    boto3_session=None,
) -> Dict:
    """
    Save extraction metadata as JSON using boto3
    """
    try:
        if execution_date is None:
            execution_date = datetime.now().strftime('%Y-%m-%d')
        
        # Add timestamp
        metadata['extraction_timestamp'] = datetime.utcnow().isoformat()
        metadata['execution_date'] = execution_date
        
        # Convert to JSON string
        json_str = json.dumps(metadata, indent=2)
        
        # Get S3 client
        if boto3_session:
            s3_client = boto3_session.client('s3')
        else:
            from airflow.hooks.base import BaseHook
            import boto3
            conn = BaseHook.get_connection('aws_dest')
            s3_client = boto3.client(
                's3',
                aws_access_key_id=conn.login,
                aws_secret_access_key=conn.password,
                region_name='eu-north-1'
            )
        
        # Upload to S3
        key = f"metadata/{source_name}/{execution_date}.json"
        s3_client.put_object(
            Bucket=bucket,
            Key=key,
            Body=json_str.encode('utf-8'),
            ContentType='application/json'
        )
        
        s3_path = f"s3://{bucket}/{key}"
        logger.info(f"Metadata saved to {s3_path}")
        return {'status': 'success', 'path': s3_path}
        
    except Exception as e:
        logger.error(f"Error saving metadata: {str(e)}")
        raise

def write_to_static_layer(
    df: pd.DataFrame,
    bucket: str,
    table_name: str,
    execution_date: Optional[str] = None,
    boto3_session=None,
) -> Dict:
    """
    Write static/reference data to S3 without date partitioning
    Path: raw/{table_name}/data.parquet (always overwrites)
    """
    try:
        logger.info("=" * 60)
        logger.info(f"WRITING STATIC DATA: {table_name}")
        logger.info("=" * 60)
        
        # Write to S3 without partitions
        path = f"raw/{table_name}/"
        
        # Convert DataFrame to parquet in memory
        parquet_buffer = io.BytesIO()
        df.to_parquet(parquet_buffer, index=False, engine='pyarrow', compression='snappy')
        parquet_buffer.seek(0)
        
        # Get S3 client
        if boto3_session:
            s3_client = boto3_session.client('s3')
        else:
            conn = BaseHook.get_connection('aws_dest')
            s3_client = boto3.client(
                's3',
                aws_access_key_id=conn.login,
                aws_secret_access_key=conn.password,
                region_name='eu-north-1'
            )
        
        # Upload to S3 (overwrites existing)
        key = f"{path}agents.parquet"
        s3_client.put_object(
            Bucket=bucket,
            Key=key,
            Body=parquet_buffer.getvalue()
        )
        
        logger.info(f"Static data write complete")
        logger.info(f"Location: s3://{bucket}/{key}")
        logger.info(f"Records written: {len(df)}")
        logger.info("=" * 60)
        
        return {'status': 'success', 'path': f"s3://{bucket}/{key}"}
        
    except Exception as e:
        logger.error(f"Error writing static data: {str(e)}")
        raise


def ingest_to_s3(files: list, bucket: str, dataset_type: str,execution_date: Optional[str] = None, incremental: bool = False):
    """Convert files to Parquet and upload to S3 with multipart support for large files"""
    
    from botocore.config import Config
    import time
    
    conn = BaseHook.get_connection('aws_dest')
    
    # Configure boto3 with retries
    config = Config(
        retries={'max_attempts': 3, 'mode': 'standard'},
        connect_timeout=120,
        read_timeout=120,
        signature_version='s3v4'
    )
    
    session = boto3.Session(
        aws_access_key_id=conn.login,
        aws_secret_access_key=conn.password,
        region_name="eu-north-1"
    )
    
    s3_client = session.client('s3', config=config)
    uploaded_paths = []

    def upload_large_file(buffer, bucket, key):
        """Upload large file using multipart upload"""
        buffer.seek(0)
        file_size = buffer.getbuffer().nbytes
        
        # For files > 100MB, use multipart upload
        if file_size > 100 * 1024 * 1024:  # 100MB
            logger.info(f"Large file detected ({file_size / (1024*1024):.2f} MB). Using multipart upload...")
            
            # Initiate multipart upload
            mpu = s3_client.create_multipart_upload(Bucket=bucket, Key=key)
            upload_id = mpu['UploadId']
            
            try:
                parts = []
                part_size = 50 * 1024 * 1024  # 50MB chunks
                part_num = 1
                
                buffer.seek(0)
                while True:
                    data = buffer.read(part_size)
                    if not data:
                        break
                    
                    logger.info(f"Uploading part {part_num}...")
                    
                    # Upload part with retry
                    max_retries = 3
                    for attempt in range(max_retries):
                        try:
                            response = s3_client.upload_part(
                                Bucket=bucket,
                                Key=key,
                                PartNumber=part_num,
                                UploadId=upload_id,
                                Body=data
                            )
                            parts.append({
                                'ETag': response['ETag'],
                                'PartNumber': part_num
                            })
                            break
                        except Exception as e:
                            if attempt < max_retries - 1:
                                logger.warning(f"Part {part_num} upload attempt {attempt + 1} failed. Retrying...")
                                time.sleep(2 ** attempt)
                            else:
                                raise
                    
                    part_num += 1
                
                # Complete multipart upload
                s3_client.complete_multipart_upload(
                    Bucket=bucket,
                    Key=key,
                    UploadId=upload_id,
                    MultipartUpload={'Parts': parts}
                )
                logger.info(f"Multipart upload completed: {part_num - 1} parts")
                
            except Exception as e:
                # Abort multipart upload on failure
                logger.error(f"Multipart upload failed. Aborting...")
                s3_client.abort_multipart_upload(
                    Bucket=bucket,
                    Key=key,
                    UploadId=upload_id
                )
                raise
        else:
            # Small file - regular upload
            logger.info(f"File size: {file_size / (1024*1024):.2f} MB. Using regular upload...")
            s3_client.put_object(
                Bucket=bucket,
                Key=key,
                Body=buffer.getvalue()
            )

    if incremental:
        if execution_date:
            dt = pd.to_datetime(execution_date)
            date_info = {
                'year': str(dt.year),
                'month': str(dt.month).zfill(2),
                'day': str(dt.day).zfill(2)
            }
            files_by_date = {execution_date: {'files': files, **date_info}}
        else:
            files_by_date = {}
            
            for file in files:
                match = re.search(r"(\d{4})[-_](\d{2})[-_](\d{2})", Path(file).stem)
                if match:
                    date_key = match.group(0)  
                    year, month, day = match.groups()
                else:
                    today = datetime.today()
                    date_key = today.strftime("%Y_%m_%d")
                    year = str(today.year)
                    month = str(today.month).zfill(2)
                    day = str(today.day).zfill(2)
                
                if date_key not in files_by_date:
                    files_by_date[date_key] = {
                        'files': [],
                        'year': year,
                        'month': month,
                        'day': day
                    }
                files_by_date[date_key]['files'].append(file)
        
        for date_key, date_info in files_by_date.items():
            dfs = []
            
            for file in date_info['files']:
                ext = Path(file).suffix.lower()
                
                if ext == ".csv":
                    df = pd.read_csv(file)
                elif ext == ".json":
                    df = pd.read_json(file)
                else:
                    logger.warning(f"Skipping unsupported file: {file}")
                    continue
                
                df.columns = df.columns.str.lower().str.strip().str.replace(" ", "_")
                dfs.append(df)
            
            if not dfs:
                continue
            
            combined_df = pd.concat(dfs, ignore_index=True)
            
            # Write parquet to buffer
            parquet_buffer = io.BytesIO()
            combined_df.to_parquet(parquet_buffer, index=False, engine='pyarrow', compression='snappy')
            
            key = f"{dataset_type}/year={date_info['year']}/month={date_info['month']}/day={date_info['day']}/data.parquet"
            
            # Upload with multipart support
            upload_large_file(parquet_buffer, bucket, key)
            
            uploaded_path = f"s3://{bucket}/{key}"
            logger.info(f"Uploaded {len(date_info['files'])} files → {uploaded_path} ({len(combined_df)} rows)")
            uploaded_paths.append(uploaded_path)
    
    else:
        # Static data
        logger.info(f"Processing static dataset: {dataset_type}")
        logger.info(f"Files to process: {files}")
        
        dfs = []
        
        for file in files:
            ext = Path(file).suffix.lower()
            
            logger.info(f"Reading file: {file} (extension: {ext})")
            
            if ext == ".csv":
                # Read CSV in chunks for large files
                file_size = Path(file).stat().st_size
                if file_size > 100 * 1024 * 1024:  # > 100MB
                    logger.info(f"Large CSV detected ({file_size / (1024*1024):.2f} MB). Reading in chunks...")
                    chunks = []
                    for chunk in pd.read_csv(file, chunksize=100000):
                        chunks.append(chunk)
                    df = pd.concat(chunks, ignore_index=True)
                else:
                    df = pd.read_csv(file)
            elif ext == ".json":
                df = pd.read_json(file)
            else:
                logger.warning(f"Skipping unsupported file: {file}")
                continue
            
            logger.info(f"Read {len(df)} rows from {file}")
            df.columns = df.columns.str.lower().str.strip().str.replace(" ", "_")
            dfs.append(df)
        
        if dfs:
            combined_df = pd.concat(dfs, ignore_index=True)
            logger.info(f"Combined {len(dfs)} files into {len(combined_df)} total rows")
            
            # Write parquet to buffer
            parquet_buffer = io.BytesIO()
            combined_df.to_parquet(parquet_buffer, index=False, engine='pyarrow', compression='snappy')
            
            key = f"{dataset_type}/data.parquet"
            
            # Upload with multipart support
            upload_large_file(parquet_buffer, bucket, key)
            
            uploaded_path = f"s3://{bucket}/{key}"
            logger.info(f"Uploaded {len(files)} files → {uploaded_path} ({len(combined_df)} rows)")
            uploaded_paths.append(uploaded_path)
        else:
            logger.warning(f"No valid files found for {dataset_type}")

    return uploaded_paths
