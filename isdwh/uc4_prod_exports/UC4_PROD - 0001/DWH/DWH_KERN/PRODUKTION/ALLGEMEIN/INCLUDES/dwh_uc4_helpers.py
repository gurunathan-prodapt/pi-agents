"""
dwh_uc4_helpers.py

This module contains shared, reusable utility functions and Airflow callbacks 
translated from UC4 JOBI Include Scripts (DW.HOLE_PFAD and DW.LESE_LOG).
It provides centralized environment parameter resolution, date arithmetic, 
and automated pipeline execution logging.
"""

import logging
from datetime import datetime
from typing import Any, Dict

from dateutil.relativedelta import relativedelta
from airflow.models import Variable
from airflow.exceptions import AirflowException

# Setup logger for monitoring operations
logger = logging.getLogger("airflow.task")


# ==============================================================================
# 1. ENVIRONMENT CONFIGURATION & PARAMETER RESOLUTION (DW.HOLE_PFAD)
# ==============================================================================

def get_global_gcp_config() -> Dict[str, str]:
    """
    Dynamically fetches shared infrastructure settings from Airflow Variables.
    Fallbacks are provided to ensure fail-safe behavior in non-production or local testing.
    """
    return {
        "GCP_PROJECT_ID": Variable.get("GCP_PROJECT", default_var="your-gcp-project-dev"),
        "DATAPROC_REGION": Variable.get("GCP_REGION", default_var="europe-west3"),
        "DATAPROC_CLUSTER_NAME": Variable.get("DATAPROC_CLUSTER", default_var="dwh-dataproc-cluster"),
        "GCS_BUCKET_NAME": Variable.get("GCS_BUCKET", default_var="your-gcs-dwh-bucket"),
    }


def resolve_dwh_variables(logical_date_str: str) -> Dict[str, Any]:
    """
    Calculates operational and date-arithmetic variables equivalent to 
    the legacy UC4 DW.HOLE_PFAD include logic.

    Args:
        logical_date_str (str): The logical execution date of the run (ISO string format).

    Returns:
        Dict[str, Any]: A dictionary containing environment homes, status flags, and 
                        calculated operational target months.
    """
    # 1. Sourcing operational parameters from Airflow Metadata Store (with sensible fallbacks)
    variables = {
        "DWH_HOME": Variable.get("dwh_home", default_var="/opt/dwh"),
        "HOME": Variable.get("home", default_var="/home/airflow"),
        "KWS_HOME": Variable.get("kws_home", default_var="/opt/kws"),
        "PMS_HOME": Variable.get("pms_home", default_var="/opt/pms"),
        "ISTNS_HOME": Variable.get("istns_home", default_var="/opt/istns"),
        "AKTIV_CARMEN": Variable.get("aktiv_carmen", default_var="1"),
        "AKTIV_CRS": Variable.get("aktiv_crs", default_var="1"),
        "AKTIV_CTEL": Variable.get("aktiv_ctel", default_var="1"),
        "AKTIV_DPPS": Variable.get("aktiv_dpps", default_var="1"),
        "AKTIV_KDS": Variable.get("aktiv_kds", default_var="1"),
        "AKTIV_WUERFEL": Variable.get("aktiv_wuerfel", default_var="1"),
        "AKTIV_XTRA": Variable.get("aktiv_xtra", default_var="1"),
        "AKTUELL_CACHE": Variable.get("aktuell_cache", default_var="1")
    }

    # Parse logical execution date (context['logical_date'])
    # Format parsed securely to withstand microsecond and timezone-offset variations
    try:
        logical_date = datetime.fromisoformat(logical_date_str)
    except (ValueError, TypeError):
        # Fallback to current datetime if string conversion fails
        logical_date = datetime.utcnow()

    # 2. UC4 Date Arithmetic Logic Replication
    first_of_current_month = logical_date.replace(day=1)

    # PRELASTMONTH_YYYYMM: Subtracting 2 months from the 1st of the current month
    prelast_month_dt = first_of_current_month - relativedelta(months=2)
    variables["PRELASTMONTH_YYYYMM"] = prelast_month_dt.strftime("%Y%m")

    # LASTMONTH_YYYYMM: Subtracting 1 day from the 1st of the current month
    last_month_dt = first_of_current_month - relativedelta(days=1)
    variables["LASTMONTH_YYYYMM"] = last_month_dt.strftime("%Y%m")

    # NEXTMONTH_YYYYMM: Adding 1 month to the logical execution date
    next_month_dt = logical_date + relativedelta(months=1)
    variables["NEXTMONTH_YYYYMM"] = next_month_dt.strftime("%Y%m")

    return variables


def run_hole_pfad_task(**context) -> None:
    """
    Wrapper function designed to run within a PythonOperator. Resolves all variables 
    and pushes them to XCom for consumption by downstream operational tasks.
    """
    logical_date_val = context.get('logical_date')
    if logical_date_val is None:
        logical_date_str = datetime.utcnow().isoformat()
    elif isinstance(logical_date_val, datetime):
        logical_date_str = logical_date_val.isoformat()
    else:
        logical_date_str = str(logical_date_val)

    # 1. Execute DB Start Monitoring Logging (Replicating DW.DWH_ADM_JOB_MONITOR_START)
    task_instance = context.get("task_instance")
    task_id = task_instance.task_id if task_instance else "unknown_task"
    run_id = context.get("run_id", "manual_run")
    job_kennung = f"{task_id}_{run_id}"
    
    register_job_monitor_state( 
        job_kennung=job_kennung,
        status="RUNNING",
        detail="DW.HOLE_PFAD: Initializing environment paths and date ranges."
    )

    # 2. Resolve parameters
    resolved_vars = resolve_dwh_variables(logical_date_str)

    # 3. Stream resolved metadata directly into XCom
    ti = context['ti']
    for key, val in resolved_vars.items():
        ti.xcom_push(key=key, value=val)
        logger.info(f"DWH Variable registered to XCom -> {key} = {val}")


# ==============================================================================
# 2. MONITORING & DB METADATA TRACKING (DW.DWH_ADM_JOB_MONITOR_START/END)
# ==============================================================================

def register_job_monitor_state(job_kennung: str, status: str, detail: str = "") -> None:
    """
    Handles logging updates. Replaces the sub-includes:
    DW.DWH_ADM_JOB_MONITOR_START & DW.DWH_ADM_JOB_MONITOR_END.
    
    In a target Cloud Composer environment, this routes to Cloud Logging,
    which is then systematically exported to BigQuery tracking tables.
    """
    log_boundary = "=" * 80
    logger.info(log_boundary)
    logger.info(f"[JOB MONITOR UPDATE] | Job: {job_kennung} | Status: {status}")
    if detail:
        logger.info(f"[JOB MONITOR DETAILS] | {detail}")
    logger.info(log_boundary)


# ==============================================================================
# 3. SUCCESS / FAILURE EXECUTION EVALUATORS (DW.LESE_LOG)
# ==============================================================================

def dwh_on_failure_callback(context: Dict[str, Any]) -> None:
    """
    Acts as the error-trapping engine derived from DW.LESE_LOG.
    Triggered when an individual task fails.
    """
    ti = context.get("task_instance")
    task_id = ti.task_id if ti else "unknown_task"
    run_id = context.get("run_id", "manual_run")
    job_kennung = f"{task_id}_{run_id}"
    
    # 1. Replicate formatting markers from standard output of DW.LESE_LOG
    print("*" * 80)
    print(f"Rueckgabewert: '1' (Fehlerfall)***************************")
    print("*" * 80)
    
    # 2. SHOWLOG.KSH replacement: Print structured access links directly into Task Logs
    if ti:
        print(f"Access Real-Time Task Execution logs via Composer/Airflow GUI:")
        print(f"Log URL: {ti.log_url}")
    
    # Exception recovery/reason evaluation
    exception = context.get("exception")
    if exception:
         print(f"Execution Error Diagnostics: {exception}")

    print("*" * 80)
    
    # 3. Execute DW.DWH_ADM_JOB_MONITOR_END logic for Failure States
    register_job_monitor_state(
        job_kennung=job_kennung,
        status="FAILED",
        detail=f"Task terminated unexpectedly. Reference Exception: {str(exception)}"
    )


def dwh_on_success_callback(context: Dict[str, Any]) -> None:
    """
    Handles successful task terminations, executing the success 
    path of DW.LESE_LOG.
    """
    ti = context.get("task_instance")
    task_id = ti.task_id if ti else "unknown_task"
    run_id = context.get("run_id", "manual_run")
    job_kennung = f"{task_id}_{run_id}"
    
    print("*" * 80)
    print(f"Rueckgabewert: '0' ***************************************")
    print("*" * 80)
    
    # Execute DW.DWH_ADM_JOB_MONITOR_END logic for Success States
    register_job_monitor_state(
        job_kennung=job_kennung,
        status="SUCCESS",
        detail="Task completed successfully without processing errors."
    )