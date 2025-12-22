import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent))

from airflow.sdk import dag, task
from pendulum import datetime
from datetime import timedelta
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.hooks.base import BaseHook
from airflow.providers.standard.operators.empty import EmptyOperator
import boto3

from include.scripts.utils.logger import setup_logging
from include.scripts.utils.config import get_aws_config, get_google_sheets_config
from include.scripts.utils.aws_utils import get_parameters_by_path, parse_json_parameters
from include.scripts.utils.data_quality import run_quality_checks
from include.scripts.extract.s3_extractor import (
    extract_customers_from_s3,
    extract_call_logs_from_s3,
    extract_social_media_from_s3,
    fetch_files
)
from include.scripts.extract.google_sheets_extractor import extract_agents_from_google_sheets
from include.scripts.extract.postgres_extractor import (
    extract_web_forms_from_postgres,
    get_postgres_connection_string
)
from include.scripts.load.s3_loader import write_to_raw_layer, save_extraction_metadata, write_to_static_layer, ingest_to_s3
from include.scripts.load.warehouse_loader import get_database_setup_sql, get_agents_table_ddl, get_call_logs_table_ddl, get_customers_table_ddl, get_social_media_table_ddl, get_web_forms_table_ddl
from datetime import datetime
from typing import Dict, Optional, List
import logging
import pandas as pd
import json

logger = logging.getLogger(__name__)
logger = setup_logging("main_extract", log_level="INFO")

LOCAL_CUSTOMERS_DIR = "/opt/airflow/data/customers"
LOCAL_CALL_LOGS_DIR = "/opt/airflow/data/call_logs"
LOCAL_SOCIAL_DIR = "/opt/airflow/data/social_media"
LOCAL_AGENTS = "/opt/airflow/data/agents"
DEST_BUCKET = "coretelecoms-raw-data-dev"

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

@dag(
    dag_id="customer_complaints_pipeline",
    start_date=datetime(2025, 11, 20),
    end_date=datetime(2025, 12, 23),
    max_active_runs=1,
    catchup=True,
    schedule="@daily",
    default_args=default_args,
    tags=["coretelecoms"]
)

def run_pipeline():

    aws_dest = BaseHook.get_connection('aws_dest')

    setup_database = SQLExecuteQueryOperator(
        task_id='setup_database',
        conn_id='snowflake_default',
        sql=get_database_setup_sql()
    )

    create_agents_table = SQLExecuteQueryOperator(
        task_id='create_agents_table',
        conn_id='snowflake_default',
        sql=get_agents_table_ddl()
    )

    create_customers_table = SQLExecuteQueryOperator(
        task_id='create_customers_table',
        conn_id='snowflake_default',
        sql=get_customers_table_ddl()
    )

    create_call_logs_table = SQLExecuteQueryOperator(
        task_id='create_call_logs_table',
        conn_id='snowflake_default',
        sql=get_call_logs_table_ddl()
    )

    create_social_media_table = SQLExecuteQueryOperator(
        task_id='create_social_media_table',
        conn_id='snowflake_default',
        sql=get_social_media_table_ddl()
    )

    create_web_forms_table = SQLExecuteQueryOperator(
        task_id='create_web_forms_table',
        conn_id='snowflake_default',
        sql=get_web_forms_table_ddl()
    )

    @task
    def extract_agent() -> pd.DataFrame:
        credential_path = "/opt/airflow/dags/service_account_key.json"
        with open(credential_path, 'r') as f:
            credentials = json.load(f)
        sheets_config = get_google_sheets_config()
        df = extract_agents_from_google_sheets(
            credentials_dict=credentials,
            spreadsheet_id=sheets_config['spreadsheet_id'],
            worksheet_name='agents'
        )
        
        logger.info(f"Extracted {len(df)} agents")
        return df

    @task
    def load_agent(bucket: str, df: pd.DataFrame, ds: str = None) -> dict:
        dest_conn = BaseHook.get_connection('aws_dest')
        session = boto3.Session(
        aws_access_key_id=dest_conn.login,
        aws_secret_access_key=dest_conn.password,
        region_name="eu-north-1"
        )

        checks_config = {'min_rows': 1, 'required_columns': ['agent_id']}
        quality_results = run_quality_checks(df, checks_config)
        if not quality_results['all_passed']:
            logger.warning("Agent quality checks failed")

        write_to_static_layer(df, bucket, 'agents', ds, session)
        save_extraction_metadata(bucket, 'agents', {
            'source': 'agents',
            'row_count': len(df),
            'quality_checks': quality_results,
            'status': 'success'
        }, ds, session)

        return {'source': 'agents', 'rows': len(df), 'status': 'success'}

    @task
    def extract_customer() -> pd.DataFrame:
        files = fetch_files(LOCAL_CUSTOMERS_DIR)
        logger.info(f"Extracted {len(files)} customers")
        return files

    @task
    def load_customer(files: List):
        # checks = {'min_rows': 1, 'required_columns': ['customer_id']}
        # results = run_quality_checks(df, checks)
        # write_to_raw_layer(df, bucket, 'customers', ds, session)
        # save_extraction_metadata(bucket, 'customers', {
        #     'row_count': len(df), 'quality_checks': results, 'status': 'success'
        # }, ds, session)
        # return {'source': 'customers', 'rows': len(df), 'status': 'success'}
        uploaded_path = ingest_to_s3(files, DEST_BUCKET, "customers", incremental=False)
        return uploaded_path

    @task
    def extract_call_logs():
        files = fetch_files(LOCAL_CALL_LOGS_DIR)
        return files

    @task
    def load_call_logs(files: List, ds: str):
        # write_to_raw_layer(df, bucket, 'call_logs', ds, session)
        # save_extraction_metadata(bucket, 'call_logs', {
        #     'row_count': len(df), 'quality_checks': {'all_passed': True}, 'status': 'success'
        # }, ds, session)
        # return {'source': 'call_logs', 'rows': len(df), 'status': 'success'}
        uploaded_path = ingest_to_s3(files, DEST_BUCKET, "call_logs", execution_date=ds, incremental=True)
        return uploaded_path

    @task
    def extract_social_media() -> pd.DataFrame:
        files = fetch_files(LOCAL_SOCIAL_DIR)
        return files

    @task
    def load_social_media(files: List, ds: str):
        # write_to_raw_layer(df, bucket, 'social_media', ds, session)
        # save_extraction_metadata(bucket, 'social_media', {
        #     'row_count': len(df), 'quality_checks': {'all_passed': True}, 'status': 'success'
        # }, ds, session)
        # return {'source': 'social_media', 'rows': len(df), 'status': 'success'}
        uploaded_path = ingest_to_s3(files, DEST_BUCKET, "social_media", execution_date=ds, incremental=True)
        return uploaded_path

    @task
    def extract_web_forms() -> List[Dict]:
        conn = BaseHook.get_connection('aws_source')
        session = boto3.Session(
            aws_access_key_id=conn.login,
            aws_secret_access_key=conn.password,
            region_name="eu-north-1"
        )
        db_params = get_parameters_by_path("/coretelecoms/db/", boto3_session=session)
        conn_str = get_postgres_connection_string(
            host=db_params['host'],
            port=int(db_params['port']),
            database=db_params['name'],
            user=db_params['user'],
            password=db_params['password']
        )
        raw_dfs = extract_web_forms_from_postgres(conn_str, schema='customer_complaints')
        
        result = []
        for table_date, df in raw_dfs:
            if not df.empty:
                dt = pd.to_datetime(table_date)
                df = df.copy()
                df['year'] = dt.year
                df['month'] = dt.month
                df['day'] = dt.day
            result.append({"date": table_date, "df": df})
        return result

    @task
    def load_web_forms(bucket: str, extracted_data: List[Dict], ds: str = None) -> dict:
        conn = BaseHook.get_connection('aws_dest')
        session = boto3.Session(
            aws_access_key_id=conn.login,
            aws_secret_access_key=conn.password,
            region_name="eu-north-1"
        )
        total = 0
        for item in extracted_data:
            table_date = item["date"]
            df = item["df"]
            write_to_raw_layer(df, bucket, 'web_forms', table_date, session)
            total += len(df)
        save_extraction_metadata(bucket, 'web_forms', {
            'row_count': total, 'quality_checks': {'all_passed': True}, 'status': 'success'
        }, ds, session)
        return {'source': 'web_forms', 'rows': total}


    
    aws_dest = BaseHook.get_connection('aws_dest')

    load_agents_to_snowflake = SQLExecuteQueryOperator(
    task_id='load_agents_to_snowflake',
    conn_id='snowflake_default',
    sql=f"""
    USE DATABASE CORETELECOMS;
    USE SCHEMA RAW;

    COPY INTO agents
    FROM 's3://{DEST_BUCKET}/agents/agents.parquet'
    CREDENTIALS = (
        AWS_KEY_ID = '{aws_dest.login}'
        AWS_SECRET_KEY = '{aws_dest.password}'
    )
    FILE_FORMAT = (TYPE = 'PARQUET')
    MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
    PURGE = FALSE;
    """
    )

    load_customers_to_snowflake = SQLExecuteQueryOperator(
    task_id='load_customers_to_snowflake',
    conn_id='snowflake_default',
    sql=f"""
    USE DATABASE CORETELECOMS;
    USE SCHEMA RAW;
    
    COPY INTO customers
    FROM 's3://{DEST_BUCKET}/customers/data.parquet'
    CREDENTIALS = (
        AWS_KEY_ID = '{aws_dest.login}'
        AWS_SECRET_KEY = '{aws_dest.password}'
    )
    FILE_FORMAT = (TYPE = 'PARQUET')
    MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
    PURGE = FALSE;
    """
)

    load_call_logs_to_snowflake = SQLExecuteQueryOperator(
        task_id='load_call_logs_to_snowflake',
        conn_id='snowflake_default',
        sql=f"""
        USE DATABASE CORETELECOMS;
        USE SCHEMA RAW;
        
        COPY INTO call_logs
        FROM 's3://{DEST_BUCKET}/call_logs/year={{{{ logical_date.year }}}}/month={{{{ logical_date.strftime('%m') }}}}/day={{{{ logical_date.strftime('%d') }}}}/'
        CREDENTIALS = (
            AWS_KEY_ID = '{aws_dest.login}'
            AWS_SECRET_KEY = '{aws_dest.password}'
        )
        FILE_FORMAT = (TYPE = 'PARQUET')
        MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
        PURGE = FALSE;
        """
    )

    load_social_media_to_snowflake = SQLExecuteQueryOperator(
    task_id='load_social_media_to_snowflake',
    conn_id='snowflake_default',
    sql=f"""
    USE DATABASE CORETELECOMS;
    USE SCHEMA RAW;
    
    COPY INTO social_media
    FROM 's3://{DEST_BUCKET}/social_media/year={{{{ logical_date.year }}}}/month={{{{ logical_date.strftime('%m') }}}}/day={{{{ logical_date.strftime('%d') }}}}/'
    CREDENTIALS = (
        AWS_KEY_ID = '{aws_dest.login}'
        AWS_SECRET_KEY = '{aws_dest.password}'
    )
    FILE_FORMAT = (TYPE = 'PARQUET')
    MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
    PURGE = FALSE;
    """
)
    start = EmptyOperator(task_id='start')

    end = EmptyOperator(task_id='end')





    aws_config = get_aws_config()
    raw_bucket = aws_config['raw_bucket']
    # source_bucket = aws_config['source_bucket']


    agent_df = extract_agent()
    customer_files = extract_customer()
    call_log_files = extract_call_logs()
    social_media_files = extract_social_media()
    # cust_df = extract_customer(source_bucket)
    # call_df = extract_call_logs(source_bucket)
    # sm_df = extract_social_media(source_bucket)


    agent_loaded = load_agent(raw_bucket, agent_df)
    customer_loaded = load_customer(customer_files)
    call_log_loaded = load_call_logs(call_log_files)
    social_media_loaded = load_social_media(social_media_files)
    # cust_loaded = load_customer(raw_bucket, cust_df)
    # # call_loaded = load_call_logs(raw_bucket, call_df)
    # sm_loaded = load_social_media(raw_bucket, sm_df)
    # web_data = extract_web_forms()
    # web_loaded = load_web_forms(raw_bucket, web_data)

    # sf_task = load_to_snowflake()
    # [customer_loaded, call_log_loaded, social_media_loaded, web_loaded] >> sf_task

    start >> setup_database

    setup_database >> [
        create_agents_table,
        create_customers_table,
        create_call_logs_table,
        create_social_media_table
    ]

    tables_created = EmptyOperator(task_id='tables_created')

    [
        create_agents_table,
        create_customers_table,
        create_call_logs_table,
        create_social_media_table
    ] >> tables_created
    
    tables_created >> [agent_df, customer_files, call_log_files, social_media_files]

    agent_df >> agent_loaded
    customer_files >> customer_loaded
    call_log_files >> call_log_loaded
    social_media_files >> social_media_loaded

    agent_loaded >> load_agents_to_snowflake
    customer_loaded >> load_customers_to_snowflake
    call_log_loaded >> load_call_logs_to_snowflake
    social_media_loaded >> load_social_media_to_snowflake

    [
        load_agents_to_snowflake,
        load_customers_to_snowflake,
        load_call_logs_to_snowflake,
        load_social_media_to_snowflake
    ] >> end


 
dag_instance = run_pipeline()