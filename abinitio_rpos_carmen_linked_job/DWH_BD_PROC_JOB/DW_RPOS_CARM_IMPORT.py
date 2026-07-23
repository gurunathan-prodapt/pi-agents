"""
DAG Name: dw_rpos_carm_import
Schedule: None (Manual/External trigger - GAP in source UC4 schedule configuration)
Controller: Apache Airflow 2.x

Overview:
    This DAG coordinates the migration of the legacy UC4 job `DW.RPOS_CARM_IMPORT` 
    to Google Cloud Platform. It executes a migrated Ab Initio graph (`map_rpos_carmen_import.mp`) 
    as a Dataproc PySpark job. 
    The processing logic ingests billing raw files (`CARMEN_B_*_pos.fix`) and targets the 
    following Data Warehouse (DWH) tables:
      - DWH$TA_F_RPOS_CARM
      - DWH$TA_F_RPOS_FACT_CARM
      - DWH$TA_F_RPOS_RESELLING_CARM
      - DWH$TA_F_GPOS_FACT_CARM
      - DWH$TA_T_RPOS_CARM

Tasks:
    - start: Dummy task marking the beginning of the DAG execution.
    - rpos_carm_import_task: Submits the PySpark compilation of the legacy Ab Initio 
      graph "RPOS_CARM_IMPORT" to Dataproc. Passes execution arguments verbatim from 
      the legacy config.
    - end: Dummy task marking successful DAG execution.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator

# ── GCP CONFIGURATION CONSTANTS ─────────────────────────────────────────────
# Environment-wide variables sourced dynamically from Airflow Variables per policy
GCP_PROJECT_ID = Variable.get("GCP_PROJECT")
DATAPROC_REGION = Variable.get("GCP_REGION")
DATAPROC_CLUSTER_NAME = Variable.get("DATAPROC_CLUSTER")
GCS_BUCKET_NAME = Variable.get("GCS_BUCKET")

# Path to the compiled PySpark script translated from Ab Initio 'RPOS_CARM_IMPORT'
PYSPARK_SCRIPT_URI = f"gs://{GCS_BUCKET_NAME}/pyspark_scripts/rpos_carm_import.py"

# ── DEFAULT ARGS DEFINITION ──────────────────────────────────────────────────
# Sourced from legacy runtime configurations (no retries defined in legacy UC4)
DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2026, 4, 21),
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

# ── DAG DEFINITION ───────────────────────────────────────────────────────────
with DAG(
    dag_id='dw_rpos_carm_import',
    default_args=DEFAULT_ARGS,
    schedule=None,
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    tags=['migrated', 'uc4', 'abinitio']
) as dag:

    # ── TASK: START ──────────────────────────────────────────────────────────
    start = EmptyOperator(
        task_id='start'
    )

    # ── TASK: RPOS CARM IMPORT TASK ──────────────────────────────────────────
    pyspark_job_definition = {
        "reference": {
            "project_id": GCP_PROJECT_ID,
            "job_id": "{{ dag.dag_id }}_{{ run_id }}_rpos_carm_import"
        },
        "placement": {
            "cluster_name": DATAPROC_CLUSTER_NAME
        },
        "pyspark_job": {
            "main_python_file_uri": PYSPARK_SCRIPT_URI,
            "args": [
                "--job_kennung", "RPOS_CARM_IMPORT",
                "--cfg_file", "map_rpos_carmen_import.cfg"
            ]
        }
    }

    rpos_carm_import_task = DataprocSubmitJobOperator(
        task_id='rpos_carm_import_task',
        project_id=GCP_PROJECT_ID,
        region=DATAPROC_REGION,
        job=pyspark_job_definition
    )

    # ── TASK: END ────────────────────────────────────────────────────────────
    end = EmptyOperator(
        task_id='end'
    )

    # ── TASK DEPENDENCY MAP ──────────────────────────────────────────────────
    start >> rpos_carm_import_task >> end