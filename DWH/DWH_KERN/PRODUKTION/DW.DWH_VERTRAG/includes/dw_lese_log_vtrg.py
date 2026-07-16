"""
Migrated Utility from UC4 JOBI: DW.LESE_LOG_VTRG
Provides a unified logging format for tracking DAG and Task contexts.
"""

import logging
from airflow.models import TaskInstance
from airflow.exceptions import AirflowFailException


def execute_protocol_log(**context) -> None: 
    """
    Extracts runtime metadata context parameters and writes a standardized
    protocol log mimicking the legacy UC4 diagnostic tracking.
    """
    try:
        dag_id = context['dag'].dag_id if 'dag' in context else "UNKNOWN_DAG"
        
        ti: TaskInstance = context.get('ti')
        task_id = ti.task_id if ti else "UNKNOWN_TASK"
        
        # Original Print Literal Rule: Must not translate, localize, or rephrase the literal German output.
        logging.info(f"Protokolleintrag: {task_id} innerhalb {dag_id}")
        
    except Exception as e:
        # Diagnostic logging failures should fail the task to prevent silent gaps
        error_msg = f"Failed to execute diagnostic protocol write: {str(e)}"
        logging.error(error_msg)
        raise AirflowFailException(error_msg)