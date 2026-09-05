"""
DAG: dw_dwh_exis_cpdw_direct
Description: Exportiert Lookupdaten nach CPDW (Migrated from JOBS_UNIX).
This DAG represents a standalone Unix job designed to export lookup data to the CPDW system
using an SFTP direct transfer protocol. Since no parent JOBP workflow was supplied in the 
extraction, this task is wrapped in its own standalone DAG.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator

# ── GCP Configuration ────────────────────────────────────
# Global environment-wide variables sourced from Airflow Variables
GCP_PROJECT_ID = Variable.get("GCP_PROJECT")
DATAPROC_REGION = Variable.get("DATAPROC_REGION")
DATAPROC_CLUSTER_NAME = Variable.get("DATAPROC_CLUSTER")
GCS_BUCKET = Variable.get("GCS_BUCKET")

# ── Default Args ─────────────────────────────────────────
DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# ── DAG Definition ────────────────────────────────────────
with DAG(
    dag_id="dw_dwh_exis_cpdw_direct",
    default_args=DEFAULT_ARGS,
    description="Exportiert Lookupdaten nach CPDW (Migrated from JOBS_UNIX)",
    schedule=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    params={
        "DWH_JOB_KENNUNG": "EXIS_CPDW_DIRECT",
    },
    tags=["uc4_migration", "unrecognized_launcher"],
) as dag:

    # ── Task: dwh_exis_cpdw_direct ────────────────────────
    # Export lookup data to CPDW system using SFTP direct transfer protocol.
    # Original UC4 Script details:
    #   :inc DW.HOLE_PFAD
    #   :set &DWH_JOB_KENNUNG='EXIS_CPDW_DIRECT'
    #   . $HOME/.dw_init
    #   $HOME/aktuell/exporter/is/bin/r_exis -s cpdw -r direct -f sftp
    #   :inc DW.LESE_LOG
    #
    # REVIEW-STRUCT: launcher command not recognised during conversion — implement the correct operator once its behaviour is confirmed
    dwh_exis_cpdw_direct = EmptyOperator(
        task_id="dwh_exis_cpdw_direct",
    )

    # ── Dependencies ─────────────────────────────────────────
    # Single standalone task; no dependency chain required.
    dwh_exis_cpdw_direct