"""
DAG: dw_exttest_legacy_dwh
Description: Legacy KSH DWH execution - migrated from UC4.
             This represents the migration of the UC4 job DW.EXTTEST_LEGACY_DWH.
             The original job executed a legacy shell script (r_legacy_ksh_dwh).
"""

import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator

# ==============================================================================
# ── Global Configuration (Environment-wide) ───────────────────────────────────
# ==============================================================================
GCP_PROJECT = Variable.get("GCP_PROJECT")
GCS_BUCKET = Variable.get("GCS_BUCKET")
HOME = os.environ.get("AIRFLOW_HOME")

# ==============================================================================
# ── Job-Specific Configuration ────────────────────────────────────────────────
# ==============================================================================
JOB_CONFIG = {
    "dwh_job_kennung": "EXTTEST_LEGACY_DWH",
    "home_scripts_dir": f"{HOME}/scripts" if HOME else "/home/airflow/gcs/data/scripts"
}

# ==============================================================================
# ── Default Args ──────────────────────────────────────────────────────────────
# ==============================================================================
DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# ==============================================================================
# ── DAG Definition ────────────────────────────────────────────────────────────
# ==============================================================================
# REVIEW: Created standalone DAG because no parent JOBP was provided in the extraction.
with DAG( 
    dag_id="dw_exttest_legacy_dwh",
    default_args=DEFAULT_ARGS,
    description="Legacy KSH DWH execution - migrated from UC4",
    schedule=None,  # Externally triggered / Standalone utility
    start_date=datetime(2023, 1, 1),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,  # Active=1 in UC4 source
    tags=["migrated_uc4", "legacy_dwh"],
) as dag:

    # ==========================================================================
    # ── Task: dw_exttest_legacy_dwh_task ──────────────────────────────────────
    # ==========================================================================
    # REVIEW-STRUCT: launcher command not recognised during conversion — implement the correct operator once its behaviour is confirmed
    # Original UC4 Command: &HOME/scripts/r_legacy_ksh_dwh
    # Context variables set in source: DWH_JOB_KENNUNG='EXTTEST_LEGACY_DWH'
    dw_exttest_legacy_dwh_task = BashOperator(
        task_id="dw_exttest_legacy_dwh_task",
        bash_command=f"python {JOB_CONFIG['home_scripts_dir']}/r_legacy_ksh_dwh.py",
        env={"DWH_JOB_KENNUNG": JOB_CONFIG["dwh_job_kennung"]},
    )

    dw_exttest_legacy_dwh_task