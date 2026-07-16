"""
Utility module for standardizing logs and operational metadata tracking.
Replaces the legacy UC4 JOBI: DW.LESE_LOG_VTRG.
"""

import logging
from typing import Any, Dict, Optional


def log_uc4_metadata(
    context: Dict[str, Any], step_message: Optional[str] = None
) -> None:
    """Logs runtime execution parameters using the legacy UC4 formatting structure.

    Preserves character-for-character translation patterns:
    "Protokolleintrag: &ADMJOB innerhalb &ADMJP"

    Args: 
        context (Dict[str, Any]): Airflow execution context dictionary.
        step_message (Optional[str]): Optional custom operational logs to append.
    """
    dag_instance = context.get("dag")
    task_instance = context.get("task_instance")

    dag_id = dag_instance.dag_id if dag_instance else "UNKNOWN_DAG"
    task_id = task_instance.task_id if task_instance else "UNKNOWN_TASK"

    # OUTPUT/PRINT LITERAL RULE: Verbatim preservation of legacy German log syntax
    logging.info(f"Protokolleintrag: {task_id} innerhalb {dag_id}")

    if step_message:
        logging.info(step_message)