import yaml
from pathlib import Path
from typing import Dict, Any
import logging

logger = logging.getLogger(__name__)


def load_pipeline_config() -> Dict[str, Any]:
    """
    Load pipeline configuration from YAML file
    
    Returns:
        Dictionary with pipeline configuration
    """
    config_path = Path(__file__).parent.parent / "config" / "pipeline_config.yaml"
    
    try:
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
        
        logger.info("Pipeline configuration loaded successfully")
        return config
    
    except Exception as e:
        logger.error(f"Error loading pipeline configuration: {str(e)}")
        raise


def get_default_args() -> Dict[str, Any]:
    """
    Get default arguments for DAGs
    
    Returns:
        Dictionary with default DAG arguments
    """
    config = load_pipeline_config()
    retry_config = config.get('retry', {})
    
    from datetime import timedelta
    
    return {
        'owner': 'data-engineering',
        'depends_on_past': False,
        'email_on_failure': config.get('alerts', {}).get('email', {}).get('enabled', True),
        'email_on_retry': False,
        'email': config.get('alerts', {}).get('email', {}).get('recipients', []),
        'retries': retry_config.get('retries', 3),
        'retry_delay': timedelta(minutes=retry_config.get('retry_delay_minutes', 5)),
        'retry_exponential_backoff': retry_config.get('retry_exponential_backoff', True),
        'max_retry_delay': timedelta(minutes=retry_config.get('max_retry_delay_minutes', 60)),
    }


def get_schedule_interval() -> str:
    """
    Get schedule interval from configuration
    
    Returns:
        Schedule interval string
    """
    config = load_pipeline_config()
    return config.get('pipeline', {}).get('schedule', '0 2 * * *')