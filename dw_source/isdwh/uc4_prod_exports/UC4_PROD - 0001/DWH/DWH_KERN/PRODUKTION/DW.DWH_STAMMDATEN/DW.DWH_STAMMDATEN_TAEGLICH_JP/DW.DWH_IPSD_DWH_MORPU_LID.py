"""
Import der Rechnungsleistungen für MORPU Berechnung.

This DAG is a migration of the standalone UC4 UNIX job DW.DWH_IPSD_DWH_MORPU_LID.
It is designed to run the import binary r_ipis for MORPU map LID processes.
As a standalone task with no scheduled trigger in the source extraction, 
this DAG has schedule=None and is intended to be triggered externally.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.models import Variable

# ─── GLOBAL CONFIGURATION ─────────────────────────────────────────────────────
GCP_PROJECT = Variable.get("GCP_PROJECT", default_var=None)
GCS_BUCKET = Variable.get("GCS_BUCKET", default_var=None)
SSH_CONN_ID = Variable.get("SSH_CONN_ID", default_var="ssh_default")

# ─── JOB-SPECIFIC CONFIGURATION ───────────────────────────────────────────────
JOB_CONFIG = {
    "dwh_job_kennung": "IPSD_DWH_MORPU_LID",
    "import_script_path": "$HOME/aktuell/import/is/bin/r_ipis",
    "import_script_args": "-s dwh -k morpu_map_lid",
}

# ─── DEFAULT ARGS ─────────────────────────────────────────────────────────────
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# ─── DAG DEFINITION ───────────────────────────────────────────────────────────
with DAG(
    dag_id='dw_dwh_ipsd_dwh_morpu_lid',
    default_args=default_args,
    description='Import der Rechnungsleistungen für MORPU Berechnung.',
    schedule=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    tags=['uc4_migration', 'dwh', 'morpu'],
) as dag:

    # ─── TASKS ────────────────────────────────────────────────────────────────

    # REVIEW-STRUCT: launcher command not recognised during conversion — implement the correct operator once its behaviour is confirmed
    # Original UC4 Command: $HOME/aktuell/import/is/bin/r_ipis -s dwh -k morpu_map_lid
    # Login User: DW.UNIX.ISTNS | Host: dwhdwh1p
    # Sourced environment dependencies: .dw_init, DW.HOLE_PFAD, DW.LESE_LOG
    # Job Kennung: IPSD_DWH_MORPU_LID
    dw_dwh_ipsd_dwh_morpu_lid_task = EmptyOperator(
        task_id='dw_dwh_ipsd_dwh_morpu_lid_task',
    )

    # ─── DEPENDENCIES ─────────────────────────────────────────────────────────
    # Single task execution flow; no dependencies required.
    dw_dwh_ipsd_dwh_morpu_lid_task