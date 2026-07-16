"""
Primary DAG Orchestrator: dw_dwh_vertrag_tarif_sync_jp

This pipeline executes weekly synchronization loops for contract and tariff
assignments between STAMMDATEN and DWH_KERN tables. It preserves legacy UC4
concurrency locks and state serialization variables.
"""

import logging
from datetime import datetime, timedelta
from typing import Any, Dict

from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator

# Folder-integrity compliant local helper imports
from dags.dwh.dwh_kern.produktion.dw_dwh_vertrag.includes.dw_hole_pfad_vtrg import (
    get_vtrg_paths,
)
from dags.dwh.dwh_kern.produktion.dw_dwh_vertrag.includes.dw_lese_log_vtrg import (
    log_uc4_metadata,
)

# ─── GLOBAL / ENVIRONMENT CONFIGURATION ───────────────────────────────────────
GCP_PROJECT = Variable.get("GCP_PROJECT", default_var=None)
GCS_BUCKET = Variable.get("GCS_BUCKET", default_var=None)

# ─── JOB-SPECIFIC PARAMETERS ──────────────────────────────────────────────────
VTRG_PATHS = get_vtrg_paths()

# ─── START WORKFLOW LOGIC (DW.DWH_VERTRAG_TARIF_SYNC_START_JS) ────────────────


def evaluate_sync_status(**context: Dict[str, Any]) -> str:
    """Evaluates the state lock variable to prevent parallel synchronization loops.

    Replaces:
        :SET &SYNC_STATUS = GET_VAR('DW.VARIABLEN_VTRG','SYNC_STATUS')
        :IF &SYNC_STATUS = "GESPERRT" -> STOP_JOB()

    Returns:
        str: Target task execution identifier based on condition evaluation.
    """
    log_uc4_metadata(context)

    sync_status = Variable.get(
        "dw_variablen_vtrg_sync_status", default_var="FREI"
    )
    logical_date = context["logical_date"]
    lauf_datum = logical_date.strftime("%Y%m%d")

    if sync_status == "GESPERRT":
        # OUTPUT/PRINT LITERAL RULE: Verbatim preservation of legacy German log
        logging.error(
            f"Vertrags-/Tarifabgleich fuer {lauf_datum} ist gesperrt - Abbruch"
        )
        return "abort_execution"

    return "update_sync_variables"


def abort_job(**context: Dict[str, Any]) -> None:
    """Terminates execution with failure when a 'GESPERRT' lock state is active."""
    log_uc4_metadata(context)
    raise AirflowFailException(
        "Vertrags-/Tarifabgleich execution blocked (GESPERRT). Aborting workflow."
    )


def set_running_state(**context: Dict[str, Any]) -> None:
    """Acquires lock and updates execution timestamps.

    Replaces:
        :PUT_VAR DW.VARIABLEN_VTRG, SYNC_STATUS, "LAEUFT"
        :PUT_VAR DW.VARIABLEN_VTRG, LETZTER_LAUF, &LAUF_DATUM
    """
    logical_date = context["logical_date"]
    lauf_datum = logical_date.strftime("%Y%m%d")

    Variable.set("dw_variablen_vtrg_sync_status", "LAEUFT")
    Variable.set("dw_variablen_vtrg_letzter_lauf", lauf_datum)

    log_uc4_metadata(context)


# ─── END WORKFLOW LOGIC (DW.DWH_VERTRAG_TARIF_SYNC_ENDE_JS) ───────────────────


def release_sync_lock(**context: Dict[str, Any]) -> None:
    """Cleans up pipeline state lock on success and records completion runs.

    Replaces:
        :SET &LAUF_DATUM = GET_VAR('DW.VARIABLEN_VTRG','LETZTER_LAUF')
        :PUT_VAR DW.VARIABLEN_VTRG, SYNC_STATUS, "FREI"
        :PRINT "Vertrags-/Tarifabgleich fuer Lauf &LAUF_DATUM erfolgreich beendet"
    """
    logical_date = context["logical_date"]
    default_lauf = logical_date.strftime("%Y%m%d")

    lauf_datum = Variable.get(
        "dw_variablen_vtrg_letzter_lauf", default_var=default_lauf
    )

    # Release Lock
    Variable.set("dw_variablen_vtrg_sync_status", "FREI")

    # OUTPUT/PRINT LITERAL RULE: Verbatim preservation of legacy German log
    complete_msg = (
        f"Vertrags-/Tarifabgleich fuer Lauf {lauf_datum} erfolgreich beendet"
    )

    log_uc4_metadata(context, step_message=complete_msg)


# ─── DAG DECLARATION & ARGUMENTS ─────────────────────────────────────────────

default_args = {
    "owner": "DWH_KERN",
    "depends_on_past": False,
    "start_date": datetime(2024, 12, 1),
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="dw_dwh_vertrag_tarif_sync_jp",
    default_args=default_args,
    description="Woechentlicher Abgleich Vertrags-/Tarifzuordnung "
    "zwischen STAMMDATEN und DWH_KERN",
    schedule_interval="0 3 * * 7",  # Weekly on Sundays at 03:00 AM
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    tags=["dwh_kern", "sync", "vertrag", "tarif"],
) as dag:

    # ─── PIPELINE TASKS ───────────────────────────────────────────────────────

    start = EmptyOperator(
        task_id="start",
    )

    check_sync_status = BranchPythonOperator(
        task_id="check_sync_status",
        python_callable=evaluate_sync_status,
        provide_context=True,
    )

    abort_execution = PythonOperator(
        task_id="abort_execution",
        python_callable=abort_job,
        provide_context=True,
    )

    update_sync_variables = PythonOperator(
        task_id="update_sync_variables",
        python_callable=set_running_state,
        provide_context=True,
    )

    # Core logic execution block placeholder (simulates table modifications)
    core_sync_execution = EmptyOperator(
        task_id="core_sync_execution",
    )

    release_sync_lock_task = PythonOperator(
        task_id="release_sync_lock",
        python_callable=release_sync_lock,
        provide_context=True,
    )

    end = EmptyOperator(
        task_id="end",
    )

    # ─── TASK EXECUTION FLOW / DEPENDENCY MAP ─────────────────────────────────

    start >> check_sync_status

    # Path A: Blocked
    check_sync_status >> abort_execution

    # Path B: Allowed execution
    (
        check_sync_status
        >> update_sync_variables
        >> core_sync_execution
        >> release_sync_lock_task
        >> end
    )