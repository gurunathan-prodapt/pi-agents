"""
Module: dw_dwh_stamm_includes.py
Description: Migrated UC4 include (JOBI) components for the shared DWH_STAMM platform.
             Provides path resolution and standardized execution logging.
"""

import logging
from typing import Dict, Any
from airflow.models import Variable, TaskInstance

# Global Default Environment Configuration
# This represents the environment's primary Cloud Storage root.
# It is recommended to set this variable in Airflow: Variable.set("GCS_BUCKET", "gs://your-actual-bucket")
DEFAULT_GCS_BUCKET = Variable.get("GCS_BUCKET", default_var="gs://YOUR_BUCKET_NAME")


def hole_pfad_knzb() -> Dict[str, str]:
    """
    Standard-Include equivalent to read path variables from the Airflow Variable configuration store.
    
    UC4 Source: DW.HOLE_PFAD_KNZB
    
    Returns:
        Dict[str, str]: A dictionary containing resolved paths for:
            - DWH_HOME
            - HOME
            - ISTNS_HOME
            
    Raises:
        KeyError: If standard fallback defaults are not set and variables cannot be resolved.
    """
    logging.info("Resolving system path variables from Airflow variables store...")

    # Fetch variables, falling back to structured GCS paths if not explicitly defined
    dwh_home = Variable.get(
        "dw_variablen_dwh_home", 
        default_var=f"{DEFAULT_GCS_BUCKET}/dwh/home"
    )
    home = Variable.get(
        "dw_variablen_home", 
        default_var=f"{DEFAULT_GCS_BUCKET}/home"
    )
    istns_home = Variable.get(
        "dw_variablen_istns_home", 
        default_var=f"{DEFAULT_GCS_BUCKET}/istns_home"
    )

    logging.info(f"DWH_HOME resolved to: {dwh_home}")
    logging.info(f"HOME resolved to: {home}")
    logging.info(f"ISTNS_HOME resolved to: {istns_home}")

    return {
        "DWH_HOME": dwh_home,
        "HOME": home,
        "ISTNS_HOME": istns_home
    }


def log_parent_context(ti: TaskInstance, **context: Any) -> None:
    """
    Equivalent to UC4 JOBI: DW.LESE_LOG_KNZB.
    Extracts the active Airflow execution context and writes structured log entries.
    Designed to fail-safe so logging issues do not disrupt core pipeline runs.
    
    Args:
        ti (TaskInstance): The Airflow Task Instance object automatically provided by context.
        **context (Any): Dynamic dictionary of context variables passed from the execution frame.
    """
    try: 
        # Extract execution details (mapping perfectly to old UC4 global functions)
        dag_id = ti.dag_id if ti else context.get("dag").dag_id
        task_id = ti.task_id if ti else context.get("task").task_id
        
        # Format the standardized tracking entry (Protokolleintrag)
        # Note: Under OUTPUT/PRINT LITERAL RULE, the literal text inside output statements 
        # must be preserved character-for-character as written in the source xml:
        # "Protokolleintrag: &ADMJOB innerhalb &ADMJP"
        log_message = f"Protokolleintrag: {task_id} innerhalb {dag_id}"
        
        # Output structured boundary log
        logging.info("=" * 60)
        logging.info(log_message)
        logging.info("=" * 60)
        
    except Exception as e:
        # Fail-safe protection so logging errors never fail the business workflow
        logging.warning(f"Non-critical failure logging context in log_parent_context: {str(e)}")