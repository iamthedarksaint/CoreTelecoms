import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent))

from airflow.sdk import DAG, Variable
from pendulum import datetime
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.smtp.operators.smtp import EmailOperator

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
    get_postgres_connection_string,
    list_tables_in_schema
)
from include.scripts.load.s3_loader import write_to_raw_layer, save_extraction_metadata
from datetime import datetime
from typing import Dict, Optional
import logging
import pandas as pd

logger = logging.getLogger(__name__)




def extract_and_load_customers(
        bucket: str, 
        source_bucket: str, 
        execution_date: str,
        source_session=None, 
        dest_session=None 
        ) -> Dict:
    """
    Extract customer data and load to raw layer
    
    Args:
        bucket: Target S3 bucket (raw data lake)
        source_bucket: Source S3 bucket
        execution_date: Execution date
        
    Returns:
        Extraction results dictionary
    """
    try:
        logger.info("\n" + "=" * 80)
        logger.info("EXTRACTING CUSTOMERS")
        logger.info("=" * 80)
        
        # Extract
        df = extract_customers_from_s3(
            bucket=source_bucket,
            boto3_session=source_session
            )
        
        # Quality checks
        checks_config = {
            'min_rows': 1,
            'required_columns': ['customer_id']
        }
        quality_results = run_quality_checks(df, checks_config)
        
        if not quality_results['all_passed']:
            logger.warning("Some quality checks failed for customers")
        
        # Load to raw layer
        write_to_raw_layer(
            df=df,
            bucket=bucket,
            source_name='customers',
            execution_date=execution_date,
            boto3_session=dest_session
        )
        
        # Save metadata
        metadata = {
            'source': 'customers',
            'row_count': len(df),
            'quality_checks': quality_results,
            'status': 'success'
        }
        save_extraction_metadata(
            bucket, 'customers', 
            metadata, 
            execution_date,
            boto3_session=dest_session
            )
        
        logger.info("CUSTOMERS EXTRACTION COMPLETE\n")
        
        return {
            'source': 'customers',
            'status': 'success',
            'rows': len(df),
            'quality': quality_results
        }
        
    except Exception as e:
        logger.error(f"CUSTOMERS EXTRACTION FAILED: {str(e)}\n")
        return {
            'source': 'customers',
            'status': 'failed',
            'error': str(e)
        }


def extract_and_load_agents(
        bucket: str, 
        execution_date: str,
        dest_session=None 
        ) -> Dict:
    """
    Extract agents data from Google Sheets and load to raw layer
    
    Args:
        bucket: Target S3 bucket
        execution_date: Execution date
        
    Returns:
        Extraction results dictionary
    """
    try:
        logger.info("\n" + "=" * 80)
        logger.info("EXTRACTING AGENTS")
        logger.info("=" * 80)
        
        # Get Google Sheets credentials from AWS Secrets Manager
        credentials = get_parameters_by_path("/coretelecoms/hassan/", profile_name='destination')
        print(f"Retrieved parameter keys: {list(credentials.keys())}")

        credentials = parse_json_parameters(credentials, json_keys=["google-sheets"])

        
        credentials = credentials["google-sheets"] 
        
        # Get spreadsheet ID from config
        sheets_config = get_google_sheets_config()
        spreadsheet_id = sheets_config['spreadsheet_id']
        
        # Extract
        df = extract_agents_from_google_sheets(
            credentials_dict=credentials,
            spreadsheet_id=spreadsheet_id,
            worksheet_name='agents'
        )
        
        # Quality checks
        checks_config = {
            'min_rows': 1,
            'required_columns': ['agent_id']
        }
        quality_results = run_quality_checks(df, checks_config)
        
        if not quality_results['all_passed']:
            logger.warning("Some quality checks failed for agents")
        
        # Load to raw layer
        write_to_raw_layer(
            df=df,
            bucket=bucket,
            source_name='agents',
            execution_date=execution_date,
            boto3_session=dest_session
        )
        
        # Save metadata
        metadata = {
            'source': 'agents',
            'row_count': len(df),
            'quality_checks': quality_results,
            'status': 'success'
        }
        save_extraction_metadata(
            bucket, 'agents', 
            metadata, execution_date,
            boto3_session=dest_session
            )
        
        logger.info("AGENTS EXTRACTION COMPLETE\n")
        
        return {
            'source': 'agents',
            'status': 'success',
            'rows': len(df),
            'quality': quality_results
        }
        
    except Exception as e:
        logger.error(f"AGENTS EXTRACTION FAILED: {str(e)}\n")
        return {
            'source': 'agents',
            'status': 'failed',
            'error': str(e)
        }


def extract_and_load_call_logs(
        bucket: str, 
        source_bucket: str, 
        execution_date: str,
        source_session=None, 
        dest_session=None 
        ) -> Dict:
    """
    Extract call center logs and load to raw layer
    
    Args:
        bucket: Target S3 bucket
        source_bucket: Source S3 bucket
        execution_date: Execution date
        
    Returns:
        Extraction results dictionary
    """
    try:
        logger.info("\n" + "=" * 80)
        logger.info("EXTRACTING CALL LOGS")
        logger.info("=" * 80)
        
        # Extract
        df = extract_call_logs_from_s3(
            bucket=source_bucket,
            boto3_session=source_session
        )

        print(f"DataFrame shape before write: {df.shape}")
        print(f"DataFrame columns: {list(df.columns)}")

        if len(df) == 0:
            logger.warning(f"No call log data found for date {execution_date}. Skipping load.")
            return {
                'source': 'call_logs',
                'status': 'success',
                'rows': 0,
                'quality': {'all_passed': True}
            }
        
        # Quality checks
        checks_config = {
            'min_rows': 0  
        }
        quality_results = run_quality_checks(df, checks_config)
        
        # Load to raw layer
        write_to_raw_layer(
            df=df,
            bucket=bucket,
            source_name='call_logs',
            execution_date=execution_date,
            boto3_session=dest_session
        )
        
        # Save metadata
        metadata = {
            'source': 'call_logs',
            'row_count': len(df),
            'quality_checks': quality_results,
            'status': 'success'
        }
        save_extraction_metadata(
            bucket, 'call_logs', 
            metadata, 
            execution_date,
            boto3_session=dest_session)
        
        logger.info("CALL LOGS EXTRACTION COMPLETE\n")
        
        return {
            'source': 'call_logs',
            'status': 'success',
            'rows': len(df),
            'quality': quality_results
        }
        
    except Exception as e:
        logger.error(f"CALL LOGS EXTRACTION FAILED: {str(e)}\n")
        return {
            'source': 'call_logs',
            'status': 'failed',
            'error': str(e)
        }


def extract_and_load_social_media(
        bucket: str, 
        source_bucket: str, 
        execution_date: str,
        source_session=None, 
        dest_session=None 
        ) -> Dict:
    """
    Extract social media data and load to raw layer
    
    Args:
        bucket: Target S3 bucket
        source_bucket: Source S3 bucket
        execution_date: Execution date
        
    Returns:
        Extraction results dictionary
    """
    try:
        logger.info("\n" + "=" * 80)
        logger.info("EXTRACTING SOCIAL MEDIA")
        logger.info("=" * 80)
        
        # Extract
        df = extract_social_media_from_s3(
            bucket=source_bucket,
            boto3_session=source_session
        )

        print(f"DataFrame shape before write: {df.shape}")
        print(f"DataFrame columns: {list(df.columns)}")
        
        # Quality checks
        checks_config = {
            'min_rows': 0
        }
        quality_results = run_quality_checks(df, checks_config)
        
        # Load to raw layer
        write_to_raw_layer(
            df=df,
            bucket=bucket,
            source_name='social_media',
            execution_date=execution_date,
            boto3_session=dest_session
        )
        
        # Save metadata
        metadata = {
            'source': 'social_media',
            'row_count': len(df),
            'quality_checks': quality_results,
            'status': 'success'
        }
        save_extraction_metadata(
            bucket, 'social_media', 
            metadata, 
            execution_date, 
            boto3_session=dest_session
            )
        
        logger.info("SOCIAL MEDIA EXTRACTION COMPLETE\n")
        
        return {
            'source': 'social_media',
            'status': 'success',
            'rows': len(df),
            'quality': quality_results
        }
        
    except Exception as e:
        logger.error(f"SOCIAL MEDIA EXTRACTION FAILED: {str(e)}\n")
        return {
            'source': 'social_media',
            'status': 'failed',
            'error': str(e)
        }


def extract_and_load_web_forms(
        bucket: str, 
        execution_date: str,
        source_session=None, 
        dest_session=None 
        ) -> Dict:
    """
    Extract web forms from PostgreSQL and load to raw layer
    
    Args:
        bucket: Target S3 bucket
        execution_date: Execution date
        
    Returns:
        Extraction results dictionary
    """
    try:
        logger.info("\n" + "=" * 80)
        logger.info("EXTRACTING WEB FORMS")
        logger.info("=" * 80)
        
        # Get database credentials from SSM
        db_params = get_parameters_by_path(
            "/coretelecomms/database/",profile_name='source')
        
        # Create connection string
        connection_string = get_postgres_connection_string(
            host=db_params['db_host'],
            port=int(db_params['db_port']),
            database=db_params['db_name'],
            user=db_params['db_username'],
            password=db_params['db_password']
        )
        
        # Extract
        dfs = extract_web_forms_from_postgres(
            connection_string,
            schema='customer_complaints'
        )
        print(dfs.head())

        if not dfs:
            logger.warning("No data extracted")
            return {'status': 'success', 'rows': 0, 'source': 'web_forms', 'quality': {'all_passed': True}}

        total_rows = 0
        successful_dates = []
        
        # Load each date individually
        for table_date, df in dfs:
            try:
                dt = pd.to_datetime(table_date)
                df_with_partitions = df.copy()
                df_with_partitions['year'] = dt.year
                df_with_partitions['month'] = dt.month
                df_with_partitions['day'] = dt.day

                if len(df_with_partitions) > 100000:
                    chunks = [df_with_partitions[i:i+100000] for i in range(0, len(df_with_partitions), 100000)]
                    for chunk in chunks:

                        write_to_raw_layer(
                                df=chunk,
                                bucket=bucket,
                                source_name='web_forms',
                                execution_date=table_date,
                                boto3_session=dest_session
                            )
                        
                else:
                    write_to_raw_layer(
                        df=df_with_partitions,
                        bucket=bucket,
                        source_name='web_forms',
                        execution_date=table_date,
                        boto3_session=dest_session
                    )
                

                total_rows += len(df)
                successful_dates.append(table_date)
                print(f"Loaded {len(df)} rows for {table_date}")

            except Exception as e:
                logger.error(f"Failed to load data for {table_date}: {str(e)}")
                continue


        # Quality checks
        checks_config = {
            'min_rows': 0
        }
        quality_results = run_quality_checks(df, checks_config)
        
        # Load to raw layer
        
        # Save metadata
        metadata = {
            'source': 'web_forms',
            'row_count': len(df),
            'quality_checks': quality_results,
            'status': 'success'
        }
        save_extraction_metadata(bucket, 'web_forms', metadata, execution_date, boto3_session=dest_session)
        
        logger.info("WEB FORMS EXTRACTION COMPLETE\n")
        
        return {
            'source': 'web_forms',
            'status': 'success',
            'rows': len(df),
            'quality': quality_results
        }
        
    except Exception as e:
        logger.error(f"WEB FORMS EXTRACTION FAILED: {str(e)}\n")
        return {
            'source': 'web_forms',
            'status': 'failed',
            'error': str(e)
        }


def run_full_extraction(execution_date: Optional[str] = None) -> Dict:
    """
    Run full extraction pipeline for all sources
    
    Args:
        execution_date: Execution date (YYYY-MM-DD), defaults to today
        
    Returns:
        Dictionary with all extraction results
    """
    # Set up logging
    logger = setup_logging("main_extract", log_level="INFO")
    
    try:
        # Get execution date
        if execution_date is None:
            execution_date = datetime.now().strftime('%Y-%m-%d')
        
        logger.info("\n" + "#" * 80)
        logger.info(f"#  CORETELECOMS DATA EXTRACTION PIPELINE")
        logger.info(f"#  Execution Date: {execution_date}")
        logger.info(f"#  Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info("#" * 80 + "\n")

        from scripts.utils.aws_utils import get_boto3_session
        source_session = get_boto3_session(profile_name="source")
        dest_session = get_boto3_session(profile_name="destination")
        
        # Get configuration
        aws_config = get_aws_config()
        target_bucket = aws_config['raw_bucket']
        source_bucket = aws_config['source_bucket']
        
        logger.info(f"Target Bucket: {target_bucket}")
        logger.info(f"Source Bucket: {source_bucket}\n")
        
        # Run extractions
        results = []
        
        # 1. Customers
        # results.append(extract_and_load_customers(
        #     target_bucket, 
        #     source_bucket, 
        #     execution_date,
        #     source_session=source_session,
        #     dest_session=dest_session
        #     ))
        
        # 2. Agents
        # results.append(extract_and_load_agents(
        #     target_bucket, 
        #     execution_date,
        #     dest_session=dest_session
        #     ))
        
        # 3. Call
        # results.append(extract_and_load_call_logs(
        #     target_bucket, 
        #     source_bucket, 
        #     execution_date,
        #     source_session=source_session,
        #     dest_session=dest_session
        #     ))

        # 4. Social Media
        # results.append(extract_and_load_social_media(
        #     target_bucket, 
        #     source_bucket, 
        #     execution_date,
        #     source_session=source_session,
        #     dest_session=dest_session
        #     ))
    
        # 5. Web Forms
        results.append(extract_and_load_web_forms(
            target_bucket, 
            execution_date,
            source_session=source_session,
            dest_session=dest_session
            ))
        
        # Summary
        logger.info("\n" + "#" * 80)
        logger.info("#  EXTRACTION PIPELINE SUMMARY")
        logger.info("#" * 80)
        
        successful = [r for r in results if r['status'] == 'success']
        failed = [r for r in results if r['status'] == 'failed']
        
        logger.info(f"\nTotal Sources: {len(results)}")
        logger.info(f"Successful: {len(successful)}")
        logger.info(f"Failed: {len(failed)}")
        
        if successful:
            logger.info("\nSUCCESSFUL EXTRACTIONS:")
            for r in successful:
                logger.info(f"  - {r['source']}: {r.get('rows', 0)} rows")
        
        if failed:
            logger.error("\nFAILED EXTRACTIONS:")
            for r in failed:
                logger.error(f"  - {r['source']}: {r.get('error', 'Unknown error')}")
        
        total_rows = sum(r.get('rows', 0) for r in successful)
        logger.info(f"\nTotal Rows Extracted: {total_rows}")
        logger.info(f"Completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info("#" * 80 + "\n")
        
        return {
            'execution_date': execution_date,
            'total_sources': len(results),
            'successful': len(successful),
            'failed': len(failed),
            'total_rows': total_rows,
            'results': results
        }
        
    except Exception as e:
        logger.error(f"\nEXTRACTION PIPELINE FAILED: {str(e)}\n")
        raise
if __name__ == "__main__":
    # Run extraction
    import sys

    execution_date = sys.argv[1] if len(sys.argv) > 1 else None
    try:
        result = run_full_extraction(execution_date)
        sys.exit(0)
    except Exception as e:
        print(f"\nExtraction pipeline failed: {str(e)}")
        sys.exit(1)