import gspread
from google.oauth2.service_account import Credentials
import pandas as pd
from typing import Optional, Dict, List
import logging
from datetime import datetime
from dotenv import load_dotenv
import os
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent.parent))

from scripts.utils.logger import setup_logging
from scripts.utils.aws_utils import get_parameters_by_path

logger = logging.getLogger(__name__)

load_dotenv()

# Google Sheets API scopes
SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets.readonly',
    'https://www.googleapis.com/auth/drive.readonly',
    'https://www.googleapis.com/auth/spreadsheets'
]


def create_google_sheets_client(credentials_dict: Dict):
    """
    Create authenticated Google Sheets client
    
    Args:
        credentials_dict: Service account credentials as dictionary
        
    Returns:
        Authenticated gspread client
    """
    try:
        credentials = Credentials.from_service_account_info(
            credentials_dict,
            scopes=SCOPES
        )
        client = gspread.authorize(credentials)
        logger.info("Google Sheets client authenticated successfully")
        return client
        
    except Exception as e:
        logger.error(f"Error creating Google Sheets client: {str(e)}")
        raise


def read_worksheet(
    client,
    spreadsheet_id: str,
    worksheet_name: Optional[str] = None,
    worksheet_index: int = 0
) -> pd.DataFrame:
    """
    Read data from Google Sheets worksheet
    
    Args:
        client: Authenticated gspread client
        spreadsheet_id: Google Sheets spreadsheet ID
        worksheet_name: Name of worksheet (optional)
        worksheet_index: Index of worksheet if name not provided
        
    Returns:
        DataFrame with worksheet data
    """
    try:
        # Open spreadsheet
        spreadsheet = client.open_by_key(spreadsheet_id)
        
        # Get worksheet
        if worksheet_name:
            worksheet = spreadsheet.worksheet(worksheet_name)
            logger.info(f"Opened worksheet: {worksheet_name}")
        else:
            worksheet = spreadsheet.get_worksheet(worksheet_index)
            logger.info(f"Opened worksheet at index: {worksheet_index}")
        
        # Get all records
        records = worksheet.get_all_records()
        
        # Convert to DataFrame
        df = pd.DataFrame(records)
        
        logger.info(f"Read {len(df)} rows from worksheet")
        return df
        
    except gspread.exceptions.SpreadsheetNotFound:
        logger.error(f"Spreadsheet not found: {spreadsheet_id}")
        logger.error("Make sure the spreadsheet is shared with your service account email")
        raise
    except gspread.exceptions.WorksheetNotFound:
        logger.error(f"Worksheet not found: {worksheet_name}")
        raise
    except Exception as e:
        logger.error(f"Error reading worksheet: {str(e)}")
        raise


def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean DataFrame from Google Sheets
    
    Args:
        df: Raw DataFrame
        
    Returns:
        Cleaned DataFrame
    """
    # Remove completely empty rows
    df = df.dropna(how='all')
    
    # Clean column names
    df.columns = df.columns.str.lower().str.strip().str.replace(' ', '_').str.replace('[^a-z0-9_]', '', regex=True)
    
    # Trim whitespace from string columns
    string_columns = df.select_dtypes(include=['object']).columns
    for col in string_columns:
        df[col] = df[col].astype(str).str.strip()
    
    return df


def extract_agents_from_google_sheets(
    credentials_dict: Dict,
    spreadsheet_id: str,
    worksheet_name: str = "agents"
) -> pd.DataFrame:
    """
    Extract agents data from Google Sheets
    
    Args:
        credentials_dict: Service account credentials dictionary
        spreadsheet_id: Google Sheets spreadsheet ID
        worksheet_name: Name of worksheet containing agents
        
    Returns:
        DataFrame with agents data
    """
    try:
        logger.info("=" * 60)
        logger.info("EXTRACTING AGENTS DATA FROM GOOGLE SHEETS")
        logger.info("=" * 60)
        logger.info(f"Spreadsheet ID: {spreadsheet_id}")
        logger.info(f"Worksheet: {worksheet_name}")
        
        # Create client
        client = create_google_sheets_client(credentials_dict)
        
        # Read worksheet
        df = read_worksheet(client, spreadsheet_id, worksheet_name)
        
        # Clean data
        df = clean_dataframe(df)
        
        # Add metadata
        metadata = {
            'system': 'google_sheets_agents',
            'location': f"spreadsheet:{spreadsheet_id}/worksheet:{worksheet_name}"
        }
        
        from scripts.utils.data_quality import add_metadata_columns
        df = add_metadata_columns(df, metadata)
        
        logger.info(f"Successfully extracted {len(df)} agent records")
        logger.info(f"Columns: {list(df.columns)}")
        logger.info("=" * 60)
        
        return df
        
    except Exception as e:
        logger.error(f"Error extracting agents data: {str(e)}")
        raise


def list_all_worksheets(client, spreadsheet_id: str) -> List[str]:
    """
    List all worksheets in a spreadsheet
    
    Args:
        client: Authenticated gspread client
        spreadsheet_id: Spreadsheet ID
        
    Returns:
        List of worksheet names
    """
    try:
        spreadsheet = client.open_by_key(spreadsheet_id)
        worksheets = spreadsheet.worksheets()
        worksheet_names = [ws.title for ws in worksheets]
        
        logger.info(f"Found {len(worksheet_names)} worksheets: {worksheet_names}")
        return worksheet_names
        
    except Exception as e:
        logger.error(f"Error listing worksheets: {str(e)}")
        raise


