from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.operators.bash import BashOperator

# === GCP CONFIGURATION ===
GCP_PROJECT = Variable.get("GCP_PROJECT")
GCP_REGION = Variable.get("GCP_REGION")
DATAPROC_CLUSTER = Variable.get("DATAPROC_CLUSTER")
GCS_BUCKET = Variable.get("GCS_BUCKET")
BQ_DATASET = Variable.get("BQ_DATASET")
ALL_DIR_ROOT = Variable.get("ALL_DIR_ROOT", default_var="/isall")

# Job-specific constants
DWH_JOB_KENNUNG = "ALL_TYPES_MASTER"

DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="dw_dwh_all_types_master",
    default_args=DEFAULT_ARGS,
    description="Showcase job combining Ab Initio, Oracle SQL, KSH and AWK components in a single chain",
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
) as dag:

    # Task 1: Ab Initio-graph (PySpark) task
    pyspark_job_config = {
        "reference": {"project_id": GCP_PROJECT},
        "placement": {"cluster_name": DATAPROC_CLUSTER},
        "pyspark_job": {
            "main_python_file_uri": f"gs://{GCS_BUCKET}/pyspark_scripts/all_types_graph.py",
            "args": ["--job_kennung", DWH_JOB_KENNUNG],
        },
    }

    submit_pyspark_graph = DataprocSubmitJobOperator(
        task_id="jobs_unix_dw_dwh_all_types_master",
        project_id=GCP_PROJECT,
        region=GCP_REGION,
        job=pyspark_job_config,
        job_id="{{ dag.dag_id }}_{{ run_id }}_all_types_graph",
    )

    # Task 2: Post-Processing Master Execution (migrated Python wrapper script)
    post_processing_master = BashOperator(
        task_id="post_processing_master",
        bash_command="python3 /isall/aufbereitung/bin/r_all_types_master.py",
        env={
            "DWH_JOB_KENNUNG": DWH_JOB_KENNUNG,
            "GCP_PROJECT": GCP_PROJECT,
            "GCP_REGION": GCP_REGION,
            "GCS_BUCKET": GCS_BUCKET,
            "BQ_DATASET": BQ_DATASET,
            "ALL_DIR_ROOT": ALL_DIR_ROOT,
        }
    )

    # Sequential dependency execution order
    submit_pyspark_graph >> post_processing_master