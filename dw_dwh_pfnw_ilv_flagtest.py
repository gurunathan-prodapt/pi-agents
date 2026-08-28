"""
DAG: dw_dwh_pfnw_ilv_flagtest
Source UC4 Object: DW.DWH_PFNW_ILV_FLAGTEST (JOBS_UNIX)

This DAG is a migration of the UC4 UNIX job DW.DWH_PFNW_ILV_FLAGTEST.
Its primary function is to execute a shell script that checks if a specific
"Fill-Flag" for Maximo has been set. This flag check is a mandatory
prerequisite for the downstream Inter-Company Activity (ILV) job plan to proceed.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator

# ── GLOBAL CONFIGURATION ─────────────────────────────────────────────────────
GCP_PROJECT = Variable.get("GCP_PROJECT")
GCP_REGION = Variable.get("GCP_REGION")

# ── DEFAULT ARGS ─────────────────────────────────────────────────────────────
DEFAULT_ARGS = {
    "owner": "dw",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# ── DAG DEFINITION ───────────────────────────────────────────────────────────
with DAG(
    dag_id="dw_dwh_pfnw_ilv_flagtest",
    default_args=DEFAULT_ARGS,
    description="Tests whether the Maximo Fill-Flag is set to allow the ILV jobplan to proceed",
    schedule=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    tags=["dwh", "maximo", "uc4_migration"],
    params={
        "DWH_JOB_KENNUNG": "PFNW_ILV_FLAGTEST ",
    },
) as dag:

    # ── TASK DEFINITIONS ───────────────────────────────────────────────────────

    # # REVIEW-STRUCT: launcher command not recognised during conversion — implement the correct operator once its behaviour is confirmed
    # Original UC4 Command:
    #   $HOME/aktuell/pruef/nw/bin/k_pfnw_ilv_flagtest.ksh -q $HOME/aktuell/pruef/nw/sql/d_pfnw_ilv_flagtest.sql -j PFNW_ILV_FLAGTEST
    # Environment Parameter:
    #   DWH_JOB_KENNUNG = 'PFNW_ILV_FLAGTEST '
    dw_dwh_pfnw_ilv_flagtest_task = EmptyOperator(
        task_id="dw_dwh_pfnw_ilv_flagtest_task",
        params={
            "DWH_JOB_KENNUNG": "PFNW_ILV_FLAGTEST ",
        },
    )

    # ── DEPENDENCIES ───────────────────────────────────────────────────────────
    # Single-task pipeline; no complex dependency routing required.
    dw_dwh_pfnw_ilv_flagtest_task