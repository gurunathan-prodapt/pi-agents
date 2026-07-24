"""
This DAG orchestrates the execution of the converted PySpark job for DW.RPOS_CARM_IMPORT.
The original UC4 job's primary responsibility is to execute an Ab Initio graph (map_rpos_carmen_import)
via the r_ai_start launcher utility. This process imports Carmen retail/point-of-sale (RPOS) data
into the Data Warehouse.

Since this job was supplied as an isolated Unix job, it is defined as an externally triggered standalone DAG.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator

# ── GCP CONFIGURATION ────────────────────────────────────────────────────────
GCP_PROJECT = Variable.get("GCP_PROJECT")
GCP_REGION = Variable.get("GCP_REGION")
DATAPROC_CLUSTER = Variable.get("DATAPROC_CLUSTER")
GCS_BUCKET = Variable.get("GCS_BUCKET")

# ── DEFAULT ARGS ─────────────────────────────────────────────────────────────
DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# ── DAG DEFINITION ───────────────────────────────────────────────────────────
with DAG(
    dag_id='dw_rpos_carm_import',
    default_args=DEFAULT_ARGS,
    description='Standalone run of RPOS Carmen Import, migrated from UC4 JOBS_UNIX DW.RPOS_CARM_IMPORT',
    schedule=None,  # Externally triggered / No schedule in extraction
    start_date=datetime(2026, 4, 21),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    tags=['dwh', 'abinitio_migration', 'rpos'],
) as dag:

    # PySpark Job Configuration
    pyspark_job = {
        "reference": {
            "project_id": GCP_PROJECT,
            "job_id": "{{ dag.dag_id }}_{{ run_id }}_map_rpos_carmen_import"
        },
        "placement": {"cluster_name": DATAPROC_CLUSTER},
        "pyspark_job": {
            "main_python_file_uri": f"gs://{GCS_BUCKET}/pyspark_scripts/map_rpos_carmen_import.py",
            "args": [
                "--job_kennung", "RPOS_CARM_IMPORT"
            ]
        },
    }

    # REVIEW-STRUCT: Ab Initio graph map_rpos_carmen_import migrated to PySpark execution on Dataproc
    map_rpos_carmen_import = DataprocSubmitJobOperator(
        task_id='map_rpos_carmen_import',
        job=pyspark_job,
        region=GCP_REGION,
        project_id=GCP_PROJECT,
    )

    # Standalone task, no upstream or downstream dependencies defined within this extraction.
    map_rpos_carmen_import