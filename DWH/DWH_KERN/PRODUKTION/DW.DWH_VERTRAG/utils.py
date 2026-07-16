"""
Common utilities for DWH Vertrag workflows.
Handles environment configuration lookups and standardized UC4 logging.
"""

import logging
from typing import Dict, Any
from airflow.models import Variable


def resolve_environment_paths() -> Dict[str, str]:
    """
    Resolves the required system path variables.
    Equivalent to the merged UC4 Include: DW.HOLE_PFAD_VTRG
    
    Returns:
        Dict[str, str]: Dictionary containing resolved system paths.
    """
    paths = {
        "DWH_HOME": Variable.get("DWH_HOME", default_var="/opt/dwh"),
        "HOME": Variable.get("HOME", default_var="/home/airflow"),
        "PMS_HOME": Variable.get("PMS_HOME", default_var="/opt/pms"),
    }
    logging.debug(f"Resolved paths: {paths}")
    return paths


def write_standard_log(task_id: str, dag_id: str) -> None:
    """
    Formats and outputs standard log entries conforming to target migration layout.
    Equivalent to the merged UC4 Include: DW.LESE_LOG_VTRG
    
    Args:
        task_id (str): The active Airflow Task ID.
        dag_id (str): The active Airflow DAG ID.
    """
    # Preserves the print output format EXACTLY (OUTPUT/PRINT LITERAL RULE)
    logging.info(f"Protokolleintrag: {task_id} innerhalb {dag_id}")