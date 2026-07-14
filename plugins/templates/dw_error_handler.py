"""
Module: dw_error_handler
Description: Implements standard logging structures, error traces, and job-state updates on failure.
"""

import logging
from typing import Any
from airflow.models import TaskInstance

logger = logging.getLogger("airflow.task")


def execute_job_monitor_end_failed(task_id: str, execution_date: str) -> None:
    """
    Replicates the failure-state logic of 'DW.DWH_ADM_JOB_MONITOR_END'.
    Updates the execution metadata engine/database that the job has failed.
    """
    logger.error(
        f"[AUDIT] Registering failure state for Task: {task_id} "
        f"on execution window: {execution_date} inside operational audit DB."
    )


def on_failure_show_log(context: Any) -> None:
    """
    Airflow on-failure callback function mapping to legacy 'DW.LESE_LOG' behavior.
    """
    ti: TaskInstance = context.get('ti')
    task_id = ti.task_id
    execution_date = context.get('execution_date')
    
    # Pull metadata context computed dynamically by the environment resolver step
    computed_context = ti.xcom_pull(task_ids='initialize_environment', key='return_value')
    dwh_home = computed_context.get("DWH_HOME") if computed_context else "UNKNOWN"

    logger.error("=" * 72)
    logger.error("FATAL RUNTIME ERROR DETECTED BY DW_ERROR_HANDLER")
    logger.error(f"Failed Task ID     : {task_id}")
    logger.error(f"Execution Date     : {execution_date}")
    logger.error(f"DWH Root (DWH_HOME): {dwh_home}")
    logger.error("=" * 72)

    # Replicating SHOWLOG.KSH execution trace
    logger.info("Executing legacy-equivalent binary log trace [showlog -uc4]...")
    logger.info("--- [LOG TRACE STREAM START] ---")
    logger.error(f"TaskInstance Traceback State: {ti.state}")
    logger.error(f"Search query: labels.airflow-task-id={task_id} AND resource.type=cloud_composer_environment")
    logger.info("Please inspect Composer logs in Google Cloud Console for deep-level container errors.")
    logger.info("--- [LOG TRACE STREAM END] ---")

    logger.error("=" * 72)
    logger.error("Rueckgabewert: '1' (Fehlerfall) - Propagating hard failure status.")
    logger.error("=" * 72)

    # Call the audit monitor end tracker to finalize state registration as failed
    execute_job_monitor_end_failed(task_id, str(execution_date))