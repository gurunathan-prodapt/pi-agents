"""
Module: lese_log_knzb.py
Purpose: Auditing and logging helper mapped from DW.LESE_LOG_KNZB.
"""

import logging

logger = logging.getLogger("airflow.task")

def log_uc4_metadata(context: dict) -> None: 
    """
    Retrieves the DAG ID and Task ID from the execution context 
    and writes a structured protocol audit log.
    
    Args:
        context (dict): Airflow task execution context.
    """
    dag_id = context.get('dag').dag_id if context.get('dag') else "UNKNOWN_DAG"
    task_id = context.get('task_instance').task_id if context.get('task_instance') else "UNKNOWN_TASK"
    
    # Preserve the original literal German output format exactly, character-for-character
    logger.info(f"Protokolleintrag: {task_id} innerhalb {dag_id}")