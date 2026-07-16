"""
Module: dw_lese_log_vtrg
Purpose: Shared Airflow logging utility to log task execution context and resolve 
         UC4-specific workflow runtime parameters.
"""

from datetime import datetime
import logging
from airflow import DAG
from airflow.operators.python import PythonOperator


# ── Reusable Logic for Import inside Downstream DAGs ──────
def log_vtrg_context_executable(**context) -> str:
    """Implements the migrated core logic of DW.LESE_LOG_VTRG.

    Extracts execution runtime context and outputs formatted German log lines.

    Args:
        **context: Airflow task execution context dictionary.

    Returns:
        str: The generated log message.
    """
    # Extract Parent DAG ID (corresponds to &ADMJP / SYS_ACT_JPNAME)
    parent_dag_name = context["dag"].dag_id

    # Extract Current Task ID (corresponds to &ADMJOB / SYS_ACT_JOBNAME)
    current_task_name = context["task"].task_id

    # Render and output log message verbatim in German as designed
    log_message = (
        f"Protokolleintrag: {current_task_name} innerhalb {parent_dag_name}"
    )
    logging.info(log_message)

    return log_message


# ── Standalone Utility Helper DAG ─────────────────────────
default_args = {
    "owner": "data_engineering",
    "start_date": datetime(2026, 1, 1),
    "retries": 0,
}

dag = DAG(
    dag_id="dw_lese_log_vtrg_helper",
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    default_args=default_args,
    doc_md=__doc__,
)

# Standalone Task execution configuration
log_vtrg_context = PythonOperator(
    task_id="log_vtrg_context",
    python_callable=log_vtrg_context_executable,
    provide_context=True,
    dag=dag,
)