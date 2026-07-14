"""
Module: dwh_monitor_callback.py
Source Reference: DW.LESE_LOG.xml
Purpose: Simulates output, handles error code parsing, and logs states 
         dynamically inside Airflow Task execution contexts.
"""

import os
from typing import Any, Dict


def execute_showlog_stub(job_kennung: str) -> None:
    """
    Bypasses and logs warnings for the unresolved SHOWLOG.KSH script.
    Fallback prints redirect cleanly to Google Cloud Logging.
    """
    home_dir = os.environ.get("HOME", "/home/dwh")
    print(f"Executing: {home_dir}/tools/showlog -uc4 {job_kennung}")
    print("Warning: Legacy showlog execution requested, but no source was found. Defaulting to Cloud Logging.")


def trigger_job_monitor_end(context: Dict[str, Any], status: str) -> None:
    """
    Functional Hook mimicking legacy :INC DW.DWH_ADM_JOB_MONITOR_END.xml
    """
    dag_id = context['dag'].dag_id
    task_id = context['task_instance'].task_id
    print(f"[JOB MONITOR END] Terminated DAG: {dag_id} | Task: {task_id} | State: {status}")


def resolve_lese_log_behavior(context: Dict[str, Any]) -> None:
    """
    Core engine translating legacy UC4 return-code checks to Airflow logging streams.
    Retains literal, verbatim original German console outputs character-for-character.
    """
    ti = context.get('task_instance')
    dag_id = context.get('dag').dag_id
    task_id = ti.task_id if ti else "unknown_task"
    
    dwh_job_kennung = f"{dag_id}.{task_id}"
    
    # Check if this context is handling an execution failure
    exception = context.get('exception')
    
    if exception is not None:
        # Failure Flow (Simulates return-code '1')
        execute_showlog_stub(dwh_job_kennung)
        print("****************************************************************")
        print("Rueckgabewert: '1' (Fehlerfall)***************************")
        print("****************************************************************")
        trigger_job_monitor_end(context, status="FAILED")
    else: 
        # Success Flow (Simulates return-code '0')
        print("****************************************************************")
        print("Rueckgabewert: '0' ***************************************")
        print("****************************************************************")
        trigger_job_monitor_end(context, status="SUCCESS")


def dwh_success_callback(context: Dict[str, Any]) -> None:
    """Reusable Success Callback hook for downstream DAGs/Tasks."""
    resolve_lese_log_behavior(context)


def dwh_failure_callback(context: Dict[str, Any]) -> None:
    """Reusable Failure Callback hook for downstream DAGs/Tasks."""
    resolve_lese_log_behavior(context)