"""
Module: uc4_helpers.py
Location: dags/DWH/DWH_KERN/PRODUKTION/DW.DWH_VERTRAG/includes/

This module replaces the UC4 Job Includes (JOBI):
  - DW.HOLE_PFAD_VTRG: Dynamic path retrieval from Airflow variables.
  - DW.LESE_LOG_VTRG: Standardized execution metadata logging.

It is designed to be imported and executed dynamically within target Airflow DAGs.
"""

import logging
from typing import Dict, Any, Optional
from airflow.models import Variable

# Setup professional, standardized logging
logger = logging.getLogger("airflow.task")


def hole_pfad_vtrg() -> Dict[str, Optional[str]]:
    """
    Equivalent to UC4 JOBI 'DW.HOLE_PFAD_VTRG'.
    
    Retrieves execution and environment directory paths from Airflow Variables.
    It checks first for a unified JSON configuration container named 'dw_variablen' 
    and falls back to individual variable lookups.

    Returns:
        dict: A dictionary containing absolute or configured system paths for:
              - DWH_HOME
              - HOME
              - PMS_HOME
    """
    # 1. Attempt to fetch from unified JSON Airflow variable to reduce DB queries
    try:
        dw_vars = Variable.get("dw_variablen", deserialize_json=True)
    except Exception as e:
        logger.debug(f"Could not retrieve 'dw_variablen' JSON block: {e}. Falling back to individual keys.")
        dw_vars = {}

    # 2. Extract specific variables with fallback to individual Airflow Variables
    dwh_home = dw_vars.get(
        "DWH_HOME", 
        Variable.get("dwh_home", default_var=None)
    )
    home = dw_vars.get(
        "HOME", 
        Variable.get("home", default_var=None)
    )
    pms_home = dw_vars.get(
        "PMS_HOME", 
        Variable.get("pms_home", default_var=None)
    )

    logger.info(
        f"Path configuration loaded successfully. "
        f"DWH_HOME: {dwh_home}, HOME: {home}, PMS_HOME: {pms_home}"
    )

    return {
        "DWH_HOME": dwh_home,
        "HOME": home,
        "PMS_HOME": pms_home
    }


def lese_log_vtrg(context: Dict[str, Any]) -> None:
    """
    Equivalent to UC4 JOBI 'DW.LESE_LOG_VTRG'.
    
    Extracts runtime metadata from the active Airflow task execution context
    and writes standardized tracking lines into the system log.
    
    Compliance Rule: Exact German text structure and terms are strictly preserved.

    Args:
        context (dict): The Airflow task execution context dictionary.
    """
    # Extract running context analogous to UC4's SYS_ACT_JPNAME and SYS_ACT_JOBNAME
    try:
        dag_name = context["dag"].dag_id
        task_name = context["task_instance"].task_id
    except KeyError as e:
        logger.error(f"Failed to extract execution context: Missing context key {e}")
        dag_name = "UNKNOWN_DAG"
        task_name = "UNKNOWN_TASK"

    # COMPLIANCE: Character-for-character translation matching original UC4 print behavior
    print(f"Protokolleintrag: {task_name} innerhalb {dag_name}")
    
    # Internal operational log
    logger.info(f"Execution context tracked for job step in DAG: {dag_name}")