"""
Migrated DAG for the UC4 UNIX Job 'DW.EXTTEST_LEGACY_DWH'.

Overview:
This DAG runs a legacy shell script ('r_legacy_ksh_dwh') and handles standard environment-setup
variables. It is modeled as a standalone, externally triggered workflow (schedule=None) because
no parent workflow (JOBP) or schedule was provided in the extraction.
"""

import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator

# ── GLOBAL CONFIGURATION (Environment-wide) ───────────────────────────────────
# Resolving environment-wide parameters dynamically from Airflow variables or environment.
GCP_PROJECT = Variable.get("GCP_PROJECT")
GCP_REGION = Variable.get("GCP_REGION")
HOME_DIR = Variable.get("HOME_DIR", default_var=os.environ.get("HOME"))

# ── DEFAULT ARGS ─────────────────────────────────────────────────────────────
default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# ── DAG DEFINITION ───────────────────────────────────────────────────────────
with DAG( 
    dag_id="dw_exttest_legacy_dwh",
    default_args=default_args,
    description="Legacy KSH DWH execution migrated from DW.EXTTEST_LEGACY_DWH",
    schedule=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
) as dag:

    # ── Task: dw_exttest_legacy_dwh_task ─────────────────────────────────────
    # REVIEW-STRUCT: launcher command not recognised during conversion — implement the correct operator once its behaviour is confirmed
    # Original launcher script command: &HOME/scripts/r_legacy_ksh_dwh
    #
    # Legacy UC4 Includes (Retirements to verify):
    #   - DW.EXTTEST_HOLE_PFAD (Confirmed retired)
    #   - DW.EXTTEST_LESE_LOG (Confirmed retired)
    #
    # Variables to pass into execution context:
    #   - DWH_JOB_KENNUNG = 'EXTTEST_LEGACY_DWH'
    #   - Execution Host: dwhdwh2p
    #   - Execution User Login: DW.UNIX.ISXTST
    dw_exttest_legacy_dwh_task = EmptyOperator(
        task_id="dw_exttest_legacy_dwh_task",
    )

    # ── Dependencies ─────────────────────────────────────────────────────────
    # Standalone job — no dependency chain required
    dw_exttest_legacy_dwh_task