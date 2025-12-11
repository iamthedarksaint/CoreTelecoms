import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent))

from airflow.sdk import dag, task
from pendulum import datetime
from datetime import timedelta
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
# from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.hooks.base import BaseHook
import boto3

from include.scripts.utils.logger import setup_logging
from include.scripts.utils.config import get_aws_config, get_google_sheets_config
from include.scripts.utils.aws_utils import get_parameters_by_path, parse_json_parameters
from include.scripts.utils.data_quality import run_quality_checks
from include.scripts.extract.s3_extractor import (
    extract_customers_from_s3,
    extract_call_logs_from_s3,
    extract_social_media_from_s3
)
from include.scripts.extract.google_sheets_extractor import extract_agents_from_google_sheets
from include.scripts.extract.postgres_extractor import (
    extract_web_forms_from_postgres,
    get_postgres_connection_string
)
from include.scripts.load.s3_loader import write_to_raw_layer, save_extraction_metadata
from datetime import datetime
from typing import Dict, Optional, List
import logging
import pandas as pd

logger = logging.getLogger(__name__)
logger = setup_logging("main_extract", log_level="INFO")

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

@dag(
    dag_id="customer_complaints_pipeline",
    start_date=datetime(2025, 11, 20),
    end_date=datetime(2025, 11, 23),
    max_active_runs=1,
    catchup=True,
    schedule="@daily",
    default_args=default_args,
    tags=["coretelecoms"]
)

def run_pipeline():

    
    # @task
    # def get_sessions():
    #     """Get boto3 sessions for source and destination AWS accounts."""
    #     source_conn = BaseHook.get_connection('aws_source')
    #     dest_conn = BaseHook.get_connection('aws_dest')

    #     source_session = boto3.Session(
    #     aws_access_key_id=source_conn.login,
    #     aws_secret_access_key=source_conn.password
    #     )

    #     dest_session = boto3.Session(
    #     aws_access_key_id=dest_conn.login,
    #     aws_secret_access_key=dest_conn.password
    #     )
    #     return {
    #         'source_session': source_session,
    #         'dest_session': dest_session
    #     }

    @task
    def extract_agent() -> pd.DataFrame:

        dest_conn = BaseHook.get_connection('aws_dest')
        session = boto3.Session(
            aws_access_key_id=dest_conn.login,
            aws_secret_access_key=dest_conn.password,
            region_name="eu-north-1"
        )
        credentials = get_parameters_by_path("/coretelecoms/hassan/", boto3_session=session)
        credentials = parse_json_parameters(credentials, ["google-sheets"])
        sheets_config = get_google_sheets_config()
        df = extract_agents_from_google_sheets(
            credentials_dict=credentials["google-sheets"],
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

        write_to_raw_layer(df, bucket, 'agents', ds, session)
        save_extraction_metadata(bucket, 'agents', {
            'source': 'agents',
            'row_count': len(df),
            'quality_checks': quality_results,
            'status': 'success'
        }, ds, session)

        return {'source': 'agents', 'rows': len(df), 'status': 'success'}

    @task
    def extract_customer(source_bucket: str) -> pd.DataFrame:
        conn = BaseHook.get_connection('aws_source')
        session = boto3.Session(
        aws_access_key_id=conn.login,
        aws_secret_access_key=conn.password,
        region_name="eu-north-1"
        )
        df = extract_customers_from_s3(bucket=source_bucket, boto3_session=session)
        logger.info(f"Extracted {len(df)} customers")
        return df

    @task
    def load_customer(bucket: str, df: pd.DataFrame, ds: str =  None) -> dict:
        checks = {'min_rows': 1, 'required_columns': ['customer_id']}
        results = run_quality_checks(df, checks)
        conn = BaseHook.get_connection('aws_dest')
        session = boto3.Session(
        aws_access_key_id=conn.login,
        aws_secret_access_key=conn.password,
        region_name="eu-north-1"
        )
        write_to_raw_layer(df, bucket, 'customers', ds, session)
        save_extraction_metadata(bucket, 'customers', {
            'row_count': len(df), 'quality_checks': results, 'status': 'success'
        }, ds, session)
        return {'source': 'customers', 'rows': len(df), 'status': 'success'}

    @task
    def extract_call_logs(source_bucket: str) -> pd.DataFrame:
        conn = BaseHook.get_connection('aws_source')
        session = boto3.Session(
        aws_access_key_id=conn.login,
        aws_secret_access_key=conn.password,
        region_name="eu-north-1"
        )
        return extract_call_logs_from_s3(bucket=source_bucket, boto3_session=session)

    @task
    def load_call_logs(bucket: str, df: pd.DataFrame, ds: str = None) -> dict:
        conn = BaseHook.get_connection('aws_dest')
        session = boto3.Session(
        aws_access_key_id=conn.login,
        aws_secret_access_key=conn.password,
        region_name="eu-north-1"
        )
        write_to_raw_layer(df, bucket, 'call_logs', ds, session)
        save_extraction_metadata(bucket, 'call_logs', {
            'row_count': len(df), 'quality_checks': {'all_passed': True}, 'status': 'success'
        }, ds, session)
        return {'source': 'call_logs', 'rows': len(df), 'status': 'success'}

    @task
    def extract_social_media(source_bucket: str) -> pd.DataFrame:
        conn = BaseHook.get_connection('aws_source')
        session = boto3.Session(
        aws_access_key_id=conn.login,
        aws_secret_access_key=conn.password,
        region_name="eu-north-1"
        )
        return extract_social_media_from_s3(bucket=source_bucket, boto3_session=session)

    @task
    def load_social_media(bucket: str, df: pd.DataFrame, ds: str = None) -> dict:
        conn = BaseHook.get_connection('aws_dest')
        session = boto3.Session(
        aws_access_key_id=conn.login,
        aws_secret_access_key=conn.password,
        region_name="eu-north-1"
        )
        write_to_raw_layer(df, bucket, 'social_media', ds, session)
        save_extraction_metadata(bucket, 'social_media', {
            'row_count': len(df), 'quality_checks': {'all_passed': True}, 'status': 'success'
        }, ds, session)
        return {'source': 'social_media', 'rows': len(df), 'status': 'success'}

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


    @task
    def load_to_snowflake(ds: str):
        hook = SnowflakeHook(snowflake_conn_id='snowflake_default')
        conn = hook.get_conn()
        cursor = conn.cursor()
        bucket = get_aws_config()['raw_bucket']
        s3_path = f"s3://{bucket}/raw/{ds}/"
        tables = ['agents', 'customers', 'call_logs', 'social_media', 'web_forms']
        for table in tables:
            cursor.execute(f"""
                COPY INTO CORETELECOMS.RAW.{table}
                FROM '{s3_path}{table}.parquet'
                FILE_FORMAT = (TYPE = 'PARQUET')
                MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;
            """)
        conn.commit()
        cursor.close()
        conn.close()
        logger.info("Loaded all tables to Snowflake")



    aws_config = get_aws_config()
    raw_bucket = aws_config['raw_bucket']
    source_bucket = aws_config['source_bucket']


    agent_df = extract_agent()
    agent_loaded = load_agent(raw_bucket, agent_df)

    cust_df = extract_customer(source_bucket)
    cust_loaded = load_customer(raw_bucket, cust_df)

    call_df = extract_call_logs(source_bucket)
    call_loaded = load_call_logs(raw_bucket, call_df)

    sm_df = extract_social_media(source_bucket)
    sm_loaded = load_social_media(raw_bucket, sm_df)

    web_data = extract_web_forms()
    web_loaded = load_web_forms(raw_bucket, web_data)

    sf_task = load_to_snowflake()

    [agent_loaded, cust_loaded, call_loaded, sm_loaded, web_loaded] >> sf_task

dag_instance = run_pipeline()