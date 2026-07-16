"""
Utility logging module for structured audit logging.
Migrated from UC4 Include Script (JOBI) 'DW.LESE_LOG_KNZB'.
"""

import logging
from typing import Any, Dict

logger = logging.getLogger("airflow.task")


def log_uc4_metadata(context: Dict[str, Any]) -> None:
    """
    Extracts processing context (DAG name, Task name) from the Airflow task
    execution context and prints structured audit logs matching the original source.
    
    Preserves German audit syntax from legacy systems character-for-character:
    "Protokolleintrag: &ADMJOB innerhalb &ADMJP"
    """
    try:
        # Gracefully extract parameters from the context dictionary
        dag = context.get("dag")
        task_instance = context.get("task_instance")
        
        adm_jp = dag.dag_id if dag else "Unknown_DAG"
        adm_job = task_instance.task_id if task_instance else "Unknown_Task"

        # Output matches the original UC4 protocol log standard strictly
        logger.info("-" * 80)
        logger.info(f"Protokolleintrag: {adm_job} innerhalb {adm_jp}")
        logger.info("-" * 80)
        
    except Exception as e:
        # Ensure that logging failures never crash the parent data pipeline
        logger.warning(f"Non-blocking failure resolving UC4 logging metadata: {str(e)}")