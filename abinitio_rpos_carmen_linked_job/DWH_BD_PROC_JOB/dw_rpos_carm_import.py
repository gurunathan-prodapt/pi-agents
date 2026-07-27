from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator

# Resolve global environment variables per strict environment policy
GCP_PROJECT_ID = Variable.get("GCP_PROJECT")
GCP_REGION = Variable.get("GCP_REGION")
GCS_BUCKET = Variable.get("GCS_BUCKET")
DATAPROC_CLUSTER = Variable.get("DATAPROC_CLUSTER", default_var=None)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='dw_rpos_carm_import',
    default_args=default_args,
    description='Airflow DAG orchestrating map_rpos_carmen_import PySpark pipeline on Dataproc Serverless',
    schedule=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
) as dag:

    # Configuration mappings sourced from map_rpos_carmen_import.cfg
    pyspark_job_config = {
        "reference": {"project_id": GCP_PROJECT_ID},
        "placement": {"cluster_name": DATAPROC_CLUSTER} if DATAPROC_CLUSTER else {},
        "pyspark_job": {
            "main_python_file_uri": f"gs://{GCS_BUCKET}/pyspark_scripts/map_rpos_carmen_import.py",
            "args": [
                "--job_kennung", "RPOS_CARM_IMPORT",
                "--config_file", f"gs://{GCS_BUCKET}/cfg/bd_proc/map_rpos_carmen_import.cfg"
            ]
        }
    }

    dw_rpos_carm_import_task = DataprocSubmitJobOperator(
        task_id='dw_rpos_carm_import_task',
        job=pyspark_job_config,
        region=GCP_REGION,
        project_id=GCP_PROJECT_ID,
    )

    dw_rpos_carm_import_task