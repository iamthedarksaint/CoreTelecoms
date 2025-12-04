import awswrangler as wr
import pandas as pd
from typing import List, Optional, Dict
import logging
from datetime import datetime
import psycopg2
from sqlalchemy import create_engine
import re

logger = logging.getLogger(__name__)


def get_postgres_connection_string(
    host: str,
    port: int,
    database: str,
    user: str,
    password: str
) -> str:
    """
    Create PostgreSQL connection string
    
    Args:
        host: Database host
        port: Database port
        database: Database name
        user: Database user
        password: Database password
        
    Returns:
        Connection string
    """
    return f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}"


def execute_query(
        connection_string: str, 
        sql: str
        ) -> pd.DataFrame:
    """
    Execute SQL query and return DataFrame
    
    Args:
        connection_string: PostgreSQL connection string
        sql: SQL query
        
    Returns:
        DataFrame with query results
    """
    try:
        logger.info(f"Executing query...")
        engine = create_engine(connection_string)
        df = pd.read_sql_query(sql, engine)
        logger.info(f"Query returned {len(df)} rows")
        return df
        
    except Exception as e:
        logger.error(f"Error executing query: {str(e)}")
        raise


def list_tables_in_schema(
        connection_string: str,
        schema: str = "public",
        # boto3_session=None,
        ) -> List[str]:
    """
    List all tables in PostgreSQL schema
    
    Args:
        connection_string: PostgreSQL connection string
        schema: Schema name
        
    Returns:
        List of table names
    """
    try:
        sql = f"""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = '{schema}' 
            AND table_type = 'BASE TABLE'
            ORDER BY table_name
        """
        
        df = execute_query(
            connection_string,
            sql
            )
        tables = df['table_name'].tolist()
        
        logger.info(f"Found {len(tables)} tables in schema '{schema}'")
        return tables
        
    except Exception as e:
        logger.error(f"Error listing tables: {str(e)}")
        raise


def read_table(
    connection_string: str,
    table: str,
    schema: str = "public",
    columns: Optional[List[str]] = None,
    where: Optional[str] = None,
    limit: Optional[int] = None
) -> pd.DataFrame:
    """
    Read data from PostgreSQL table
    
    Args:
        connection_string: PostgreSQL connection string
        table: Table name
        schema: Schema name
        columns: List of columns to select (None = all)
        where: WHERE clause without 'WHERE' keyword
        limit: Maximum number of rows
        
    Returns:
        DataFrame with table data
    """
    try:
        # Build SQL query
        col_list = ", ".join(columns) if columns else "*"
        sql = f"SELECT {col_list} FROM {schema}.{table}"
        
        if where:
            sql += f" WHERE {where}"
        
        if limit:
            sql += f" LIMIT {limit}"
        
        logger.info(f"Reading from {schema}.{table}")
        df = execute_query(
            connection_string, 
            sql
            )
        
        return df
        
    except Exception as e:
        logger.error(f"Error reading table {schema}.{table}: {str(e)}")
        raise


def extract_web_forms_from_postgres(
    connection_string: str,
    schema: str = "customer_complaints",
    # start_date: Optional[str] = None,
    # end_date: Optional[str] = None
) -> pd.DataFrame:
    """
    Extract web form requests from PostgreSQL
    Tables are named: web_form_request_YYYY_MM_DD
    
    Args:
        connection_string: PostgreSQL connection string
        schema: Schema name
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)
        
    Returns:
        Combined DataFrame with all web form data
    """
    try:
        logger.info("=" * 60)
        logger.info("EXTRACTING WEB FORMS FROM POSTGRESQL")
        logger.info("=" * 60)
        logger.info(f"Schema: {schema}")
        
        # List all tables in schema
        tables = list_tables_in_schema(
            connection_string,
            schema
            )
        print(f"All tables in schema: {tables}")
        
        
        # Filter for web form tables
        web_form_tables = [t for t in tables if t.startswith('web_form_request_')]

        print(f"Web form tables found: {web_form_tables}")
        
        if not web_form_tables:
            return []
        
        logger.info(f"Found {len(web_form_tables)} web form tables")
        
        # Extract and combine tables
        dataframes = []
        
        for table in web_form_tables:
            try:
                # Extract date from table name: web_form_request_YYYY_MM_DD
                date_parts = table.split('_')[-3:]
                if len(date_parts)  != 3:
                    continue
                table_date = '-'.join(date_parts)

                    # Read table
                df = read_table_chunked(
                        connection_string,
                        table, 
                        schema
                    )
                    
                if df.empty:
                    continue

                df.columns = (
                    df.columns
                    .str.lower()
                    .str.strip()
                    .str.replace(' ', '_')
                    .str.replace('[^a-z0-9_]', '', regex=True)
                )

                df['form_date'] = table_date
                
        
                # Add metadata
                metadata = {
                    'system': 'postgres_web_forms',
                    'location': f"postgres:{schema}.{table}"
                }
                
                from scripts.utils.data_quality import add_metadata_columns
                df = add_metadata_columns(df, metadata)

                dataframes.append((table_date, df))
                logger.info(f"Extracted {len(df)} rows for {table_date}")
            
            except Exception as e:
                logger.warning(f"Error processing table {table}: {str(e)}")
                continue

        return dataframes
        
    except Exception as e:
        logger.error(f"Error extracting web forms: {str(e)}")
        raise


def extract_incremental(
    connection_string: str,
    table: str,
    date_column: str,
    last_extracted_date: Optional[str] = None,
    schema: str = "public"
) -> pd.DataFrame:
    """
    Extract data incrementally based on date column
    
    Args:
        connection_string: PostgreSQL connection string
        table: Table name
        date_column: Date column for filtering
        last_extracted_date: Last extraction date (YYYY-MM-DD)
        schema: Schema name
        
    Returns:
        DataFrame with new/updated records
    """
    try:
        where_clause = None
        
        if last_extracted_date:
            where_clause = f"{date_column} > '{last_extracted_date}'"
            logger.info(f"Incremental extraction from {last_extracted_date}")
        else:
            logger.info("Full extraction (no last extraction date)")
        df = read_table(
                connection_string,    
                table,
                schema=schema,
                where=where_clause
                )
    
        logger.info(f"Extracted {len(df)} records")
        return df
    
    except Exception as e:
        logger.error(f"Error in incremental extraction: {str(e)}")
        raise


def read_table_chunked(
    connection_string: str,
    table: str,
    schema: str = "customer_complaints",
    chunk_size: int = 50000
) -> pd.DataFrame:
    """
    Read a large table in chunks to avoid statement_timeout
    """
    logger.info(f"Reading {schema}.{table} in chunks of {chunk_size}")
    
    all_chunks = []
    last_ctid = None
    
    while True:
        if last_ctid is None:
            sql = f"""
                SELECT ctid, * FROM {schema}.{table}
                ORDER BY ctid
                LIMIT {chunk_size}
            """
        else:
            sql = f"""
                SELECT ctid, * FROM {schema}.{table}
                WHERE ctid > '{last_ctid}'
                ORDER BY ctid
                LIMIT {chunk_size}
            """
        
        chunk = pd.read_sql(sql, connection_string)
        if chunk.empty:
            break
            
        last_ctid = chunk['ctid'].iloc[-1]
        # Drop ctid before returning
        chunk = chunk.drop(columns=['ctid'])
        all_chunks.append(chunk)
        logger.info(f"Fetched chunk ending at ctid={last_ctid}")
    
    return pd.concat(all_chunks, ignore_index=True) if all_chunks else pd.DataFrame()