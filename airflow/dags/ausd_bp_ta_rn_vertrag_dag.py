# Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_rn_vertrag.ksh
# Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_rn_vertrag.ksh

from __future__ import annotations

import pendulum
from datetime import timedelta
from typing import Any, Dict, Optional

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.empty import EmptyOperator
from airflow.utils.context import Context
from airflow.utils.trigger_rule import TriggerRule

try:
    # Preferred operator if available in your Composer/Airflow version
    from airflow.providers.google.cloud.operators.bigquery import (
        BigQueryExecuteStoredProcedureOperator,
    )
except Exception as exc:  # pragma: no cover
    raise ImportError(
        "BigQueryExecuteStoredProcedureOperator is not available in this Airflow environment."
    ) from exc


# ------------------------------------------------------------------------------
# DAG CONFIG
# ------------------------------------------------------------------------------

DAG_ID = "ausd_bp_ta_rn_vertrag_dag"
PROJECT_ID = "gcp-project-id"
DATASET_ID = "bq_dataset_name"
STORED_PROCEDURE_NAME = "ausd_bp_ta_rn_vertrag_wrapper"

DEFAULT_ARGS: Dict[str, Any] = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
}

DAG_PARAMS = {
    "stichtag": Param(
        default=pendulum.today("Europe/Berlin").format("DDMMYYYY"),
        type="string",
        description="Reference date in DDMMYYYY format. Defaults to today's date.",
    ),
    "wiederanlaufwert": Param(
        default="0",
        type="string",
        description="Restart threshold. Defaults to '0'.",
    ),
}


# ------------------------------------------------------------------------------
# REUSABLE HELPERS
# ------------------------------------------------------------------------------

def build_stored_procedure_fully_qualified_name(
    project_id: str,
    dataset_id: str,
    procedure_name: str,
) -> str:
    """
    Build the fully qualified BigQuery stored procedure name.
    """
    return f"{project_id}.{dataset_id}.{procedure_name}"


def build_stored_procedure_parameters(
    stichtag: str,
    wiederanlaufwert: str,
) -> Dict[str, Any]:
    """
    Build the parameter payload passed to the stored procedure.
    """
    return {
        "stichtag": stichtag,
        "wiederanlaufwert": wiederanlaufwert,
    }


def validate_ddmmyyyy(date_str: str) -> bool:
    """
    Validate DDMMYYYY format.
    """
    try:
        pendulum.from_format(date_str, "DDMMYYYY", tz="Europe/Berlin")
        return True
    except Exception:
        return False


def resolve_runtime_params(context: Context) -> Dict[str, str]:
    """
    Resolve DAG params from runtime context and apply defaults/validation.
    """
    params = context.get("params", {}) or {}

    stichtag = params.get("stichtag") or pendulum.today("Europe/Berlin").format("DDMMYYYY")
    wiederanlaufwert = params.get("wiederanlaufwert") or "0"

    if not validate_ddmmyyyy(stichtag):
        raise ValueError(
            f"Invalid 'stichtag' value: {stichtag}. Expected format is DDMMYYYY."
        )

    return {
        "stichtag": stichtag,
        "wiederanlaufwert": str(wiederanlaufwert),
    }


def log_runtime_parameters(context: Context) -> None:
    """
    Log runtime parameters to Airflow logs (forwarded to Cloud Logging in Composer).
    """
    runtime_params = resolve_runtime_params(context)
    context["ti"].log.info("Resolved runtime parameters: %s", runtime_params)


# ------------------------------------------------------------------------------
# TASK CALLBACKS
# ------------------------------------------------------------------------------

def on_failure_callback(context: Context) -> None:
    """
    Failure callback for task-level logging.
    """
    task_instance = context.get("task_instance")
    exception = context.get("exception")
    if task_instance:
        task_instance.log.error(
            "Task failed: dag_id=%s, task_id=%s, run_id=%s, exception=%s",
            task_instance.dag_id,
            task_instance.task_id,
            task_instance.run_id,
            exception,
        )


# ------------------------------------------------------------------------------
# DAG DEFINITION
# ------------------------------------------------------------------------------

with DAG(
    dag_id=DAG_ID,
    description="Orchestrates execution of BigQuery stored procedure ausd_bp_ta_rn_vertrag_wrapper",
    default_args=DEFAULT_ARGS,
    schedule=None,
    start_date=pendulum.datetime(2024, 1, 1, tz="Europe/Berlin"),
    catchup=False,
    max_active_runs=1,
    tags=["bigquery", "stored-procedure", "composer"],
    params=DAG_PARAMS,
    render_template_as_native_obj=True,
    on_failure_callback=on_failure_callback,
) as dag:

    start = EmptyOperator(task_id="start")

    log_params = EmptyOperator(
        task_id="log_params",
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    execute_wrapper_sp = BigQueryExecuteStoredProcedureOperator(
        task_id="execute_ausd_bp_ta_rn_vertrag_wrapper",
        project_id=PROJECT_ID,
        dataset_id=DATASET_ID,
        procedure_id=STORED_PROCEDURE_NAME,
        parameters={
            "stichtag": "{{ params.stichtag }}",
            "wiederanlaufwert": "{{ params.wiederanlaufwert }}",
        },
        location="EU",  # adjust if needed
        gcp_conn_id="google_cloud_default",
        retries=2,
        retry_delay=timedelta(minutes=5),
        on_failure_callback=on_failure_callback,
    )

    end = EmptyOperator(task_id="end", trigger_rule=TriggerRule.ALL_DONE)

    # Optional pre-execution validation/logging via a lightweight Python task
    # If you prefer, replace this with a PythonOperator.
    def _precheck(**context: Any) -> None:
        runtime_params = resolve_runtime_params(context)
        context["ti"].log.info("Precheck passed with params: %s", runtime_params)

    from airflow.operators.python import PythonOperator

    precheck = PythonOperator(
        task_id="precheck_runtime_params",
        python_callable=_precheck,
        provide_context=True,
        on_failure_callback=on_failure_callback,
    )

    start >> precheck >> execute_wrapper_sp >> end