"""
Airflow DAG: dw_dwh_all_types_master

Description:
Showcase workflow combining Ab Initio graph components and an auxiliary Korn Shell (KSH) script.
The job sets environment metadata variable DWH_JOB_KENNUNG to 'ALL_TYPES_MASTER', launches
an Ab Initio graph named all_types_graph (migrated to PySpark on GCP Dataproc), and then executes
a downstream preparation script r_all_types_master.ksh (migrated separately).
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.operators.bash import BashOperator
from airflow.models import Variable

# ── GCP Configuration ──────────────────────────────────────────────────────────
GCP_PROJECT_ID = Variable.get("GCP_PROJECT")
DATAPROC_REGION = Variable.get("GCP_REGION")
DATAPROC_CLUSTER_NAME = Variable.get("GCP_CLUSTER_NAME")
GCS_BUCKET = Variable.get("GCS_BUCKET")

# ── Default Args ─────────────────────────────────────────────────────────────────
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# ── DAG Definition ───────────────────────────────────────────────────────────────
with DAG(
    dag_id="dw_dwh_all_types_master",
    default_args=default_args,
    description="Showcase job combining Ab Initio, Oracle SQL, KSH and AWK components in a single chain",
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
) as dag:

    # ── Task: all_types_graph ───────────────────────────────────────────────────
    # Maps to source Ab Initio Graph 'all_types_graph' executed via PySpark on Dataproc.
    pyspark_job = {
        "reference": {
            "project_id": GCP_PROJECT_ID,
            "job_id": "{{ dag.dag_id }}_{{ run_id }}_all_types_graph"
        },
        "placement": {
            "cluster_name": DATAPROC_CLUSTER_NAME
        },
        "pyspark_job": {
            "main_python_file_uri": f"gs://{GCS_BUCKET}/pyspark_scripts/all_types_graph.py",
            "args": [
                "--job_arg=ALL_TYPES_MASTER",
                "--job_type=all_types",
                "--key=all_types_graph",
                "--job_kennung=ALL_TYPES_MASTER"
            ]
        }
    }

    all_types_graph = DataprocSubmitJobOperator(
        task_id="all_types_graph",
        job=pyspark_job,
        region=DATAPROC_REGION,
        project_id=GCP_PROJECT_ID,
    )

    # ── Task: task_r_all_types_master ──────────────────────────────────────────
    # Downstream Master Prep Execution: Replaces the legacy r_all_types_master.ksh execution.
    # It is invoked via a BashOperator executing isall/aufbereitung/bin/r_all_types_master.py.
    task_r_all_types_master = BashOperator(
        task_id="task_r_all_types_master",
        bash_command="python /home/airflow/gcs/dags/isall/aufbereitung/bin/r_all_types_master.py",
        env={
            "DWH_JOB_KENNUNG": "ALL_TYPES_MASTER",
            "ALL_TYPES_Projektverzeichnis": "/Projects/TMD/processing/ALL_TYPES/",
            "ALL_TYPES_Graph": "all_types_graph",
            "ALL_TYPES_Version": "RLS_ALL_TYPES_current",
            "ALL_TYPES_Prozesstyp": "N",
            "ALL_TYPES_Datenobjekt": "-",
            "ALL_TYPES_AI_DAT_FILE_DIR": "$ALL_TYPES_DIR_EXP_UTL/cubes/at",
            "ALL_DIR_ROOT": "/home/airflow/gcs/dags/isall",
            "DW_ORAUSER": "dummy_user",
            "GCP_PROJECT": GCP_PROJECT_ID,
            "BQ_DATASET": Variable.get("BQ_DATASET", default_var="default_dataset"),
        }
    )

    # ── Dependencies ────────────────────────────────────────────────────────────
    all_types_graph >> task_r_all_types_master