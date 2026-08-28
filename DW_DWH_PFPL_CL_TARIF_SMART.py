"""
DAG: DW_DWH_PFPL_CL_TARIF_SMART

This DAG is responsible for checking the currentness (freshness/integrity)
of the smart-tarif-mapping schema or table by executing a specific SQL script
(d_pfpl_classic_tarif_smart.sql). It was converted from the UC4 native UNIX job
DW.DWH_PFPL_CL_TARIF_SMART.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator

# ─── ENVIRONMENT CONFIGURATION ────────────────────────────────────────────────
GCP_PROJECT = Variable.get("GCP_PROJECT")
BQ_LOCATION = Variable.get("BQ_LOCATION")
BQ_DATASET = Variable.get("BQ_DATASET")
GCS_BUCKET = Variable.get("GCS_BUCKET")
R_SQLSCRIPT_PATH = Variable.get("R_SQLSCRIPT_PATH")

# ─── DEFAULT ARGS ─────────────────────────────────────────────────────────────
DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# ─── DAG DEFINITION ───────────────────────────────────────────────────────────
with DAG(
    dag_id="dw_dwh_pfpl_cl_tarif_smart",
    default_args=DEFAULT_ARGS,
    description="Check the currentness of smart-tarif-mapping",
    schedule=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    template_searchpath=["/home/airflow/gcs/dags/sql"],
    tags=["uc4_migration", "dwh", "validation"],
) as dag:

    # ─── TASKS ────────────────────────────────────────────────────────────────
    # Replaced BigQueryInsertJobOperator with BashOperator to execute the migrated wrapper script
    check_smart_tarif_mapping = BashOperator(
        task_id="check_smart_tarif_mapping",
        bash_command=f"python {R_SQLSCRIPT_PATH} -f d_pfpl_classic_tarif_smart.sql -j PFPL_CL_TARIF_SMART -m v2",
        env={
            "DWH_JOB_KENNUNG": "PFPL_CL_TARIF_SMART",
            "GCP_PROJECT": GCP_PROJECT,
            "BQ_DATASET": BQ_DATASET,
        },
        append_env=True,
    )

    # ─── DEPENDENCIES ─────────────────────────────────────────────────────────
    check_smart_tarif_mapping