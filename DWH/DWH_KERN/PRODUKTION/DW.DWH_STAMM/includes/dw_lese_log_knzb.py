"""
Module: dags/includes/dw_lese_log_knzb.py
Purpose: Standard logging outputs preserving legacy execution context.
Source Reference: DWH/DWH_KERN/PRODUKTION/DW.DWH_STAMM/includes/DW.LESE_LOG_KNZB.xml
"""

import logging

# Instantiate module logger
logger = logging.getLogger(__name__)


def log_activity(dag_id: str, task_id: str) -> None:
    """Outputs standardized legacy tracking log messages to stdout and the logs.

    Args:
        dag_id (str): The identifier of the orchestrating Airflow DAG.
        task_id (str): The identifier of the running Airflow Task.
    """
    # OUTPUT/PRINT LITERAL RULE: Verbatim preservation of German log pattern
    message = f"Protokolleintrag: {task_id} innerhalb {dag_id}"
    print(message)
    logger.info(message)