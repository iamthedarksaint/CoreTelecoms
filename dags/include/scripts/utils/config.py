import os
from typing import Dict, Optional
from dotenv import load_dotenv
import pandas as pd

load_dotenv()


def get_aws_config() -> Dict[str, str]:
    """
    Get AWS configuration from environment
    
    Returns:
        Dictionary with AWS configuration
    """
    return {
        'region': os.getenv('AWS_REGION', 'eu-north-1'),
        'access_key_id': os.getenv('AWS_ACCESS_KEY_ID'),
        'secret_access_key': os.getenv('AWS_SECRET_ACCESS_KEY'),
        'raw_bucket': os.getenv('RAW_BUCKET', 'coretelecoms-raw-data-dev'),
        'staging_bucket': os.getenv('STAGING_BUCKET', 'coretelecoms-staging-data-dev'),
        'processed_bucket': os.getenv('PROCESSED_BUCKET', 'coretelecoms-processed-data-dev'),
        'source_bucket': os.getenv('SOURCE_BUCKET', 'core-telecoms-data-lake')
    }


def get_database_config() -> Dict[str, str]:
    """
    Get database configuration from environment
    
    Returns:
        Dictionary with database configuration
    """
    return {
        'host': os.getenv('DB_HOST'),
        'port': int(os.getenv('DB_PORT', 6543)),
        'database': os.getenv('DB_NAME'),
        'user': os.getenv('DB_USER'),
        'password': os.getenv('DB_PASSWORD'),
        'schema': os.getenv('DB_SCHEMA', 'customer_complaints')
    }


def get_data_warehouse_config() -> Dict[str, str]:
    """
    Get data warehouse configuration from environment
    
    Returns:
        Dictionary with data warehouse configuration
    """
    return {
        'account': os.getenv('DW_ACCOUNT'),
        'user': os.getenv('DW_USER'),
        'password': os.getenv('DW_PASSWORD'),
        'warehouse': os.getenv('DW_WAREHOUSE', 'COMPUTE_WH'),
        'database': os.getenv('DW_DATABASE', 'CORETELECOMS'),
        'schema': os.getenv('DW_SCHEMA', 'RAW'),
        'role': os.getenv('DW_ROLE', 'ACCOUNTADMIN')
    }


def get_google_sheets_config() -> Dict[str, str]:
    """
    Get Google Sheets configuration from environment
    
    Returns:
        Dictionary with Google Sheets configuration
    """
    return {
        'spreadsheet_id': os.getenv('GOOGLE_SHEET_ID'),
        'worksheet_name': os.getenv('GOOGLE_WORKSHEET_NAME', 'agents')
    }


def get_app_config() -> Dict[str, str]:
    """
    Get application configuration from environment
    
    Returns:
        Dictionary with application configuration
    """
    return {
        'environment': os.getenv('ENVIRONMENT', 'development'),
        'log_level': os.getenv('LOG_LEVEL', 'INFO')
    }


def get_db_connection_string() -> str:
    """
    Get PostgreSQL connection string
    
    Returns:
        Database connection string
    """
    db_config = get_database_config()
    return f"postgresql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}"


def csv_to_parquet(csv_path: str, parquet_path: str):
    df = pd.read_csv(csv_path)
    df.columns = (
        df.columns.str.lower()
        .str.strip()
        .str.replace(' ', '_')
    )
    df.to_parquet(parquet_path, engine="pyarrow", index=False)
