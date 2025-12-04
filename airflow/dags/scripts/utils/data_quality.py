import pandas as pd
from typing import Dict, List, Optional
import logging

logger = logging.getLogger(__name__)


def validate_row_count(df: pd.DataFrame, min_rows: int = 1) -> Dict:
    """
    Validate DataFrame has minimum number of rows
    
    Args:
        df: DataFrame to validate
        min_rows: Minimum expected rows
        
    Returns:
        Validation result dictionary
    """
    row_count = len(df)
    passed = row_count >= min_rows
    
    result = {
        'check': 'row_count',
        'passed': passed,
        'actual': row_count,
        'expected_min': min_rows,
        'message': f"Row count: {row_count} (min: {min_rows})"
    }
    
    if passed:
        logger.info(f"Row count validation passed: {row_count} rows")
    else:
        logger.warning(f"Row count validation failed: {row_count} rows (min: {min_rows})")
    
    return result


def validate_required_columns(df: pd.DataFrame, required_columns: List[str]) -> Dict:
    """
    Validate DataFrame has required columns
    
    Args:
        df: DataFrame to validate
        required_columns: List of required column names
        
    Returns:
        Validation result dictionary
    """
    missing_columns = [col for col in required_columns if col not in df.columns]
    passed = len(missing_columns) == 0
    
    result = {
        'check': 'required_columns',
        'passed': passed,
        'missing_columns': missing_columns,
        'message': f"Missing columns: {missing_columns}" if missing_columns else "All required columns present"
    }
    
    if passed:
        logger.info("Required columns validation passed")
    else:
        logger.warning(f"Missing columns: {missing_columns}")
    
    return result


def validate_no_nulls(df: pd.DataFrame, columns: List[str]) -> Dict:
    """
    Validate specified columns have no null values
    
    Args:
        df: DataFrame to validate
        columns: List of columns to check
        
    Returns:
        Validation result dictionary
    """
    null_counts = {}
    for col in columns:
        if col in df.columns:
            null_count = df[col].isnull().sum()
            if null_count > 0:
                null_counts[col] = int(null_count)
    
    passed = len(null_counts) == 0
    
    result = {
        'check': 'no_nulls',
        'passed': passed,
        'null_counts': null_counts,
        'message': f"Nulls found in: {null_counts}" if null_counts else "No nulls in critical columns"
    }
    
    if passed:
        logger.info("Null validation passed")
    else:
        logger.warning(f"Null validation failed: {null_counts}")
    
    return result


def validate_no_duplicates(df: pd.DataFrame, columns: List[str]) -> Dict:
    """
    Validate no duplicate rows based on specified columns
    
    Args:
        df: DataFrame to validate
        columns: Columns to check for duplicates
        
    Returns:
        Validation result dictionary
    """
    duplicate_count = df.duplicated(subset=columns).sum()
    passed = duplicate_count == 0
    
    result = {
        'check': 'no_duplicates',
        'passed': passed,
        'duplicate_count': int(duplicate_count),
        'message': f"Found {duplicate_count} duplicates" if duplicate_count > 0 else "No duplicates"
    }
    
    if passed:
        logger.info("Duplicate validation passed")
    else:
        logger.warning(f"Found {duplicate_count} duplicates")
    
    return result


def clean_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean DataFrame column names
    
    Args:
        df: DataFrame with columns to clean
        
    Returns:
        DataFrame with cleaned column names
    """
    df_clean = df.copy()
    df_clean.columns = (
        df_clean.columns
        .str.lower()
        .str.strip()
        .str.replace(' ', '_')
        .str.replace('[^a-z0-9_]', '', regex=True)
    )
    
    logger.info("Cleaned column names")
    return df_clean


def clean_email_column(df: pd.DataFrame, email_column: str) -> pd.DataFrame:
    """
    Clean email column (lowercase, trim)
    
    Args:
        df: DataFrame
        email_column: Name of email column
        
    Returns:
        DataFrame with cleaned emails
    """
    df_clean = df.copy()
    
    if email_column in df_clean.columns:
        df_clean[email_column] = (
            df_clean[email_column]
            .astype(str)
            .str.lower()
            .str.strip()
        )
        logger.info(f"Cleaned email column: {email_column}")
    
    return df_clean


def add_metadata_columns(df: pd.DataFrame, source_info: Dict) -> pd.DataFrame:
    """
    Add extraction metadata columns to DataFrame
    
    Args:
        df: DataFrame to add metadata to
        source_info: Dictionary with source information
        
    Returns:
        DataFrame with metadata columns
    """
    from datetime import datetime
    
    df_with_metadata = df.copy()
    df_with_metadata['_extraction_timestamp'] = datetime.utcnow()
    df_with_metadata['_source_system'] = source_info.get('system', 'unknown')
    df_with_metadata['_source_location'] = source_info.get('location', 'unknown')
    df_with_metadata['_row_count'] = len(df)
    
    logger.info("Added metadata columns")
    return df_with_metadata


def run_quality_checks(df: pd.DataFrame, checks_config: Dict) -> Dict:
    """
    Run multiple quality checks on DataFrame
    
    Args:
        df: DataFrame to validate
        checks_config: Configuration for checks
        
    Returns:
        Dictionary with all validation results
    """
    results = {
        'total_checks': 0,
        'passed_checks': 0,
        'failed_checks': 0,
        'checks': []
    }
    
    # Row count check
    if 'min_rows' in checks_config:
        result = validate_row_count(df, checks_config['min_rows'])
        results['checks'].append(result)
        results['total_checks'] += 1
        if result['passed']:
            results['passed_checks'] += 1
        else:
            results['failed_checks'] += 1
    
    # Required columns check
    if 'required_columns' in checks_config:
        result = validate_required_columns(df, checks_config['required_columns'])
        results['checks'].append(result)
        results['total_checks'] += 1
        if result['passed']:
            results['passed_checks'] += 1
        else:
            results['failed_checks'] += 1
    
    # No nulls check
    if 'no_null_columns' in checks_config:
        result = validate_no_nulls(df, checks_config['no_null_columns'])
        results['checks'].append(result)
        results['total_checks'] += 1
        if result['passed']:
            results['passed_checks'] += 1
        else:
            results['failed_checks'] += 1
    
    # No duplicates check
    if 'unique_columns' in checks_config:
        result = validate_no_duplicates(df, checks_config['unique_columns'])
        results['checks'].append(result)
        results['total_checks'] += 1
        if result['passed']:
            results['passed_checks'] += 1
        else:
            results['failed_checks'] += 1
    
    results['all_passed'] = results['failed_checks'] == 0
    
    logger.info(f"Quality checks: {results['passed_checks']}/{results['total_checks']} passed")
    
    return results