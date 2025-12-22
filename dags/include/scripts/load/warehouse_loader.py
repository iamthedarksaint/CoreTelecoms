import logging

logger = logging.getLogger(__name__)

def get_agents_table_ddl() -> str:
    return """
    CREATE TABLE IF NOT EXISTS CORETELECOMS.RAW.agents (
        id STRING,
        name STRING,
        experience STRING,
        state STRING
    );
    """

def get_customers_table_ddl() -> str:
    return """
    CREATE TABLE IF NOT EXISTS CORETELECOMS.RAW.customers (
        customer_id STRING,
        name STRING,
        gender STRING,
        date_of_birth STRING,
        signup_date STRING,
        email STRING,
        address STRING
    );
    """

def get_call_logs_table_ddl() -> str:
    return """
    CREATE TABLE IF NOT EXISTS CORETELECOMS.RAW.call_logs (
        "unnamed:_0" STRING,
        call_id STRING,
        customer_id STRING,
        complaint_catego_ry STRING,
        agent_id STRING,
        call_start_time TIMESTAMP,
        call_end_time TIMESTAMP,
        resolutionstatus STRING,
        calllogsgenerationdate TIMESTAMP
    );
    """

def get_social_media_table_ddl() -> str:
    return """
    CREATE TABLE IF NOT EXISTS CORETELECOMS.RAW.social_media (
        complaint_id STRING,
        customer_id STRING,
        complaint_catego_ry STRING,
        agent_id STRING,
        resolutionstatus STRING,
        request_date STRING,
        resolution_date STRING,
        media_channel STRING,
        mediacomplaintgenerationdate STRING
    );
    """

def get_web_forms_table_ddl() -> str:
    return """
    CREATE TABLE IF NOT EXISTS CORETELECOMS.RAW.web_forms (
        form_id STRING,
        customer_id STRING,
        agent_id STRING,
        complaint_type STRING,
        resolution_status STRING,
        submission_date TIMESTAMP,
        year NUMBER,
        month NUMBER,
        day NUMBER,
        _extraction_timestamp TIMESTAMP,
        _source_system STRING,
        _source_location STRING,
        _row_count NUMBER
    );
    """

def get_database_setup_sql() -> str:
    return """
    USE ROLE ACCOUNTADMIN;
    CREATE DATABASE IF NOT EXISTS CORETELECOMS;
    USE DATABASE CORETELECOMS;
    CREATE SCHEMA IF NOT EXISTS RAW;
    USE SCHEMA RAW;
    CREATE OR REPLACE FILE FORMAT parquet_format
        TYPE = 'PARQUET'
        COMPRESSION = 'SNAPPY';
    """

def verify_s3_data():
    """Verify data exists in S3 before loading to Snowflake"""
    from airflow.hooks.base import BaseHook
    import boto3
    
    conn = BaseHook.get_connection('aws_dest')
    session = boto3.Session(
        aws_access_key_id=conn.login,
        aws_secret_access_key=conn.password,
        region_name="eu-north-1"
    )
    s3_client = session.client('s3')
    
    bucket = "coretelecoms-raw-data-dev"
    
    # Check agents
    try:
        response = s3_client.list_objects_v2(Bucket=bucket, Prefix='agents/')
        if 'Contents' in response:
            logger.info(f"✓ Found {len(response['Contents'])} objects in agents/")
            for obj in response['Contents']:
                logger.info(f"  - {obj['Key']} ({obj['Size']} bytes)")
        else:
            logger.warning("✗ No objects found in agents/")
    except Exception as e:
        logger.error(f"Error checking agents: {e}")
    
    # Check call_logs
    try:
        response = s3_client.list_objects_v2(Bucket=bucket, Prefix='call_logs/')
        if 'Contents' in response:
            logger.info(f"Found {len(response['Contents'])} objects in call_logs/")
            for obj in response['Contents'][:5]:  # Show first 5
                logger.info(f"  - {obj['Key']} ({obj['Size']} bytes)")
        else:
            logger.warning("No objects found in call_logs/")
    except Exception as e:
        logger.error(f"Error checking call_logs: {e}")