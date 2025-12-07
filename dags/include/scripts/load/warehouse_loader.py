import awswrangler as wr
import pandas as pd
from typing import Dict, Optional, List
import logging

logger = logging.getLogger(__name__)


def get_snowflake_connection(
    account: str,
    user: str,
    password: str,
    warehouse: str,
    database: str,
    schema: str,
    role: str = "ACCOUNTADMIN"
):
    """
    Create Snowflake connection using awswrangler
    
    Args:
        account: Snowflake account
        user: Username
        password: Password
        warehouse: Warehouse name
        database: Database name
        schema: Schema name
        role: Role name
        
    Returns:
        Snowflake connection object
    """
    try:
        conn = wr.snowflake.connect(
            account=account,
            user=user,
            password=password,
            warehouse=warehouse,
            database=database,
            schema=schema,
            role=role
        )
        logger.info(f"Connected to Snowflake: {database}.{schema}")
        return conn
        
    except Exception as e:
        logger.error(f"Error connecting to Snowflake: {str(e)}")
        raise


def write_to_snowflake(
    df: pd.DataFrame,
    connection,
    table: str,
    schema: str,
    database: str,
    mode: str = "overwrite"
) -> Dict:
    """
    Write DataFrame to Snowflake table
    
    Args:
        df: DataFrame to write
        connection: Snowflake connection
        table: Table name
        schema: Schema name
        database: Database name
        mode: Write mode ('overwrite', 'append')
        
    Returns:
        Dictionary with write results
    """
    try:
        logger.info(f"Writing {len(df)} rows to Snowflake: {database}.{schema}.{table}")
        
        wr.snowflake.to_sql(
            df=df,
            con=connection,
            table=table,
            schema=schema,
            database=database,
            mode=mode,
            use_column_names=True
        )
        
        logger.info(f"Successfully wrote to Snowflake table: {table}")
        return {'status': 'success', 'table': f"{database}.{schema}.{table}", 'rows': len(df)}
        
    except Exception as e:
        logger.error(f"Error writing to Snowflake: {str(e)}")
        raise


def load_from_s3_to_snowflake(
    s3_path: str,
    connection,
    table: str,
    schema: str,
    database: str,
    mode: str = "overwrite"
) -> Dict:
    """
    Load data from S3 Parquet to Snowflake
    
    Args:
        s3_path: S3 path to Parquet files
        connection: Snowflake connection
        table: Table name
        schema: Schema name
        database: Database name
        mode: Write mode
        
    Returns:
        Dictionary with load results
    """
    try:
        logger.info(f"Loading from {s3_path} to Snowflake: {database}.{schema}.{table}")
        
        # Read from S3
        df = wr.s3.read_parquet(path=s3_path)
        
        # Write to Snowflake
        result = write_to_snowflake(
            df=df,
            connection=connection,
            table=table,
            schema=schema,
            database=database,
            mode=mode
        )
        
        logger.info(f"Successfully loaded {len(df)} rows from S3 to Snowflake")
        return result
        
    except Exception as e:
        logger.error(f"Error loading from S3 to Snowflake: {str(e)}")
        raise


def execute_sql(connection, sql: str) -> pd.DataFrame:
    """
    Execute SQL query in data warehouse
    
    Args:
        connection: Database connection
        sql: SQL query
        
    Returns:
        DataFrame with results
    """
    try:
        logger.info("Executing SQL query")
        df = wr.snowflake.read_sql_query(sql=sql, con=connection)
        logger.info(f"Query returned {len(df)} rows")
        return df
        
    except Exception as e:
        logger.error(f"Error executing SQL: {str(e)}")
        raise


def create_table_from_dataframe(
    df: pd.DataFrame,
    connection,
    table: str,
    schema: str,
    database: str
) -> bool:
    """
    Create table in data warehouse from DataFrame schema
    
    Args:
        df: DataFrame with schema to replicate
        connection: Database connection
        table: Table name
        schema: Schema name
        database: Database name
        
    Returns:
        True if successful
    """
    try:
        logger.info(f"Creating table: {database}.{schema}.{table}")
        
        # Write empty table to establish schema
        wr.snowflake.to_sql(
            df=df.head(0),  # Empty DataFrame with schema
            con=connection,
            table=table,
            schema=schema,
            database=database,
            mode="overwrite"
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
    database: str,
    schema: str = "RAW",
    execution_date: Optional[str] = None
) -> Dict:
    """
    Load data from S3 raw layer to data warehouse
    
    Args:
        bucket: S3 bucket name
        source_name: Source name
        connection: Database connection
        database: Database name
        schema: Schema name
        execution_date: Execution date for partitions
        
    Returns:
        Dictionary with load results
    """
    try:
        logger.info("=" * 60)
        logger.info(f"LOADING RAW LAYER TO WAREHOUSE: {source_name}")
        logger.info("=" * 60)
        
        # Construct S3 path
        if execution_date:
            from datetime import datetime
            exec_dt = pd.to_datetime(execution_date)
            s3_path = f"s3://{bucket}/raw/{source_name}/year={exec_dt.year}/month={exec_dt.month}/day={exec_dt.day}/"
        else:
            s3_path = f"s3://{bucket}/raw/{source_name}/"
        
        # Table name
        table = f"raw_{source_name}"
        
        # Load to warehouse
        result = load_from_s3_to_snowflake(
            s3_path=s3_path,
            connection=connection,
            table=table,
            schema=schema,
            database=database,
            mode="append"
        )
        
        logger.info(f"Warehouse load complete")
        logger.info(f"Table: {database}.{schema}.{table}")
        logger.info(f"Records loaded: {result['rows']}")
        logger.info("=" * 60)
        
        return result
        
    except Exception as e:
        logger.error(f"Error loading to warehouse: {str(e)}")
        raise


