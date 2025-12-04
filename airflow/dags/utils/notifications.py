import logging
from typing import Dict, Any, Optional
import requests

logger = logging.getLogger(__name__)


def send_success_notification(context: Dict[str, Any]) -> None:
    """
    Send success notification
    
    Args:
        context: Airflow task context
    """
    try:
        dag_id = context['dag'].dag_id
        task_id = context['task'].task_id
        execution_date = context['execution_date']
        
        message = f"""
            Task Success
            DAG: {dag_id}
            Task: {task_id}
            Execution Date: {execution_date}
        """
        
        logger.info(f"Success notification: {message}")
        
        # Send to Slack if configured
        send_slack_notification(message, success=True)
        
    except Exception as e:
        logger.error(f"Error sending success notification: {str(e)}")


def send_failure_notification(context: Dict[str, Any]) -> None:
    """
    Send failure notification
    
    Args:
        context: Airflow task context
    """
    try:
        dag_id = context['dag'].dag_id
        task_id = context['task'].task_id
        execution_date = context['execution_date']
        exception = context.get('exception', 'Unknown error')
        
        message = f"""
                    Task Failed
                    DAG: {dag_id}
                    Task: {task_id}
                    Execution Date: {execution_date}
                    Error: {exception}
                    """
        
        logger.error(f"Failure notification: {message}")
        
        # Send to Slack if configured
        send_slack_notification(message, success=False)
        
    except Exception as e:
        logger.error(f"Error sending failure notification: {str(e)}")


def send_slack_notification(message: str, success: bool = True) -> None:
    """
    Send notification to Slack
    
    Args:
        message: Message to send
        success: Whether this is a success or failure notification
    """
    try:
        from airflow.models import Variable
        
        # Get Slack webhook URL from Airflow variables
        slack_webhook = Variable.get("slack_webhook", default_var=None)
        
        if not slack_webhook:
            logger.info("Slack webhook not configured, skipping Slack notification")
            return
        
        color = "good" if success else "danger"
        
        payload = {
            "attachments": [
                {
                    "color": color,
                    "mrkdwn_in": ["text"]
                }
            ]
        }
        
        response = requests.post(slack_webhook, json=payload, timeout=10)
        
        if response.status_code == 200:
            logger.info("Slack notification sent successfully")
        else:
            logger.warning(f"Slack notification failed: {response.status_code}")
    
    except Exception as e:
        logger.error(f"Error sending Slack notification: {str(e)}")


def send_sla_miss_notification(dag, task_list, blocking_task_list, slas, blocking_tis):
    """
    Send notification when SLA is missed
    
    Args:
        dag: DAG object
        task_list: List of tasks that missed SLA
        blocking_task_list: List of tasks blocking the SLA
        slas: SLA details
        blocking_tis: Blocking task instances
    """
    message = f"""
            SLA Missed
            DAG: {dag.dag_id}
            Tasks: {', '.join([t.task_id for t in task_list])}
            """
    
    logger.warning(f"SLA missed: {message}")
    send_slack_notification(message, success=False)