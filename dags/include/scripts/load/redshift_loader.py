"""
Data Warehouse Loader Functions
Load data from S3 to Amazon Redshift using AWS Data Wrangler
Pure functional approach - no classes
"""
import awswrangler as wr
import pandas as pd
from typing import Dict, Optional
import logging
from sqlalchemy import create_engine

logger = logging.getLogger(__name__)


def get_redshift_connection(
    host: str,
    port: int,
    database: str,
    user: str,
    password: str,
    schema: str = "public"
):
    """
    Create Redshift connection using SQLAlchemy engine (required by awswrangler)
    
    Args:
        host: Redshift cluster endpoint
        port: Port (usually 5439)
        database: Database name
        user: Username
        password: Password
        schema: Default schema
        
    Returns:
        SQLAlchemy engine object
    """
    try:
        conn_str = f"postgresql://{user}:{password}@{host}:{port}/{database}"
        engine = create_engine(conn_str, connect_args={"options": f"-c search_path={schema}"})
        logger.info(f"Connected to Redshift: {database}.{schema}")
        return engine
        
    except Exception as e:
        logger.error(f"Error connecting to Redshift: {str(e)}")
        raise


def write_to_redshift(
    df: pd.DataFrame,
    connection,
    table: str,
    schema: str = "public",
    mode: str = "overwrite", 
    boto3_session=None
) -> Dict:
    """
    Write DataFrame to Redshift table via S3 (uses Redshift COPY internally)
    
    Args:
        df: DataFrame to write
        connection: SQLAlchemy engine
        table: Table name
        schema: Schema name
        mode: Write mode ('overwrite', 'append')
        boto3_session: Optional boto3 session for S3 staging
        
    Returns:
        Dictionary with write results
    """
    try:
        logger.info(f"Writing {len(df)} rows to Redshift: {schema}.{table}")
        
        wr.redshift.to_sql(
            df=df,
            con=connection,
            table=table,
            schema=schema,
            mode=mode,
            index=False,
            boto3_session=boto3_session,
            use_column_names=True
        )
        
        logger.info(f"Successfully wrote to Redshift table: {schema}.{table}")
        return {'status': 'success', 'table': f"{schema}.{table}", 'rows': len(df)}
        
    except Exception as e:
        logger.error(f"Error writing to Redshift: {str(e)}")
        raise


def load_from_s3_to_redshift(
    s3_path: str,
    connection,
    table: str,
    schema: str = "public",
    mode: str = "overwrite",
    boto3_session=None
) -> Dict:
    """
    Load data directly from S3 Parquet to Redshift (most efficient method)
    
    Args:
        s3_path: S3 path to Parquet files (e.g., s3://bucket/path/)
        connection: SQLAlchemy engine
        table: Target table name
        schema: Target schema
        mode: 'overwrite' or 'append'
        boto3_session: Optional boto3 session (for cross-account or custom creds)
        
    Returns:
        Dictionary with load results
    """
    try:
        logger.info(f"Loading from {s3_path} to Redshift: {schema}.{table}")
        
        wr.redshift.copy_from_files(
            path=s3_path,
            con=connection,
            table=table,
            schema=schema,
            mode=mode,
            boto3_session=boto3_session,
            sql_copy_extra_params="PARQUET"
        )
        
        # Estimate row count (optional: you can query count(*) if needed)
        row_count = "unknown (loaded via COPY)"
        logger.info(f"Successfully loaded data from S3 to Redshift")
        return {'status': 'success', 'table': f"{schema}.{table}", 'rows': row_count}
        
    except Exception as e:
        logger.error(f"Error loading from S3 to Redshift: {str(e)}")
        raise


def execute_sql(connection, sql: str) -> pd.DataFrame:
    """
    Execute SQL query in Redshift
    
    Args:
        connection: SQLAlchemy engine
        sql: SQL query
        
    Returns:
        DataFrame with results
    """
    try:
        logger.info("Executing SQL query in Redshift")
        df = wr.redshift.read_sql_query(sql=sql, con=connection)
        logger.info(f"Query returned {len(df)} rows")
        return df
        
    except Exception as e:
        logger.error(f"Error executing SQL: {str(e)}")
        raise


def create_table_from_dataframe(
    df: pd.DataFrame,
    connection,
    table: str,
    schema: str = "public"
) -> bool:
    """
    Create Redshift table from DataFrame schema
    
    Args:
        df: DataFrame with schema to replicate
        connection: SQLAlchemy engine
        table: Table name
        schema: Schema name
        
    Returns:
        True if successful
    """
    try:
        logger.info(f"Creating Redshift table: {schema}.{table}")
        
        wr.redshift.to_sql(
            df=df.head(0),  # Empty DataFrame with schema
            con=connection,
            table=table,
            schema=schema,
            mode="overwrite",
            index=False
        )
        
        logger.info(f"Table created successfully")
        return True
        
    except Exception as e:
        logger.error(f"Error creating table: {str(e)}")
        raise


def load_raw_layer_to_warehouse(
    bucket: str,
    source_name: str,
    connection,
    schema: str = "raw",
    execution_date: Optional[str] = None,
    boto3_session=None
) -> Dict:
    """
    Load data from S3 raw layer to Redshift
    
    Args:
        bucket: S3 bucket name
        source_name: Source name (e.g., 'customers')
        connection: SQLAlchemy engine
        schema: Redshift schema (e.g., 'raw')
        execution_date: Optional execution date for partitioned paths
        boto3_session: Optional boto3 session
        
    Returns:
        Dictionary with load results
    """
    try:
        logger.info("=" * 60)
        logger.info(f"LOADING RAW LAYER TO REDSHIFT: {source_name}")
        logger.info("=" * 60)
        
        # Construct S3 path
        if execution_date:
            from datetime import datetime
            exec_dt = pd.to_datetime(execution_date)
            s3_path = f"s3://{bucket}/raw/{source_name}/year={exec_dt.year}/month={exec_dt.month}/day={exec_dt.day}/"
        else:
            s3_path = f"s3://{bucket}/raw/{source_name}/"
        
        table = f"raw_{source_name}"
        
        # Load directly from S3 to Redshift (efficient)
        result = load_from_s3_to_redshift(
            s3_path=s3_path,
            connection=connection,
            table=table,
            schema=schema,
            mode="append",
            boto3_session=boto3_session
        )
        
        logger.info(f"Redshift load complete")
        logger.info(f"Table: {schema}.{table}")
        logger.info(f"Source: {s3_path}")
        logger.info("=" * 60)
        
        return result
        
    except Exception as e:
        logger.error(f"Error loading to Redshift: {str(e)}")
        raise