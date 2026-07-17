from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.operators.empty import EmptyOperator

# ==============================================================================
# GCP CONFIGURATION & CONSTANTS (Sourced dynamically from Airflow Variables)
# ==============================================================================
GCP_PROJECT_ID = Variable.get("GCP_PROJECT")
DATAPROC_REGION = Variable.get("GCP_REGION")
DATAPROC_CLUSTER_NAME = Variable.get("DATAPROC_CLUSTER_NAME")
GCS_BUCKET = Variable.get("GCS_BUCKET_NAME")

PYSPARK_SCRIPT_URI = f"gs://{GCS_BUCKET}/scripts/uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/dwh_dummy_absd_plato_tarife_job.py"

# ==============================================================================
# DEFAULT ARGUMENTS
# ==============================================================================
default_args = {
    'owner': 'DW.UNIX.ISTNS',
    'depends_on_past': False,
    'start_date': datetime(2026, 3, 30),
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

# ==============================================================================
# DAG DEFINITION
# ==============================================================================
with DAG(
    dag_id='dw_dwh_dummy_absd_plato_tarife',
    default_args=default_args,
    description='Migration of UC4 dummy UNIX job for Plato Tarif Mapping',
    schedule=None,
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
) as dag:

    start = EmptyOperator(
        task_id='start',
    )

    pyspark_job_config = {
        "reference": {"project_id": GCP_PROJECT_ID},
        "placement": {"cluster_name": DATAPROC_CLUSTER_NAME},
        "pyspark_job": {
            "main_python_file_uri": PYSPARK_SCRIPT_URI
        }
    }

    dwh_dummy_absd_plato_tarife = DataprocSubmitJobOperator(
        task_id='dwh_dummy_absd_plato_tarife',
        project_id=GCP_PROJECT_ID,
        region=DATAPROC_REGION,
        job=pyspark_job_config,
        job_id="dw_dwh_dummy_absd_plato_tarife_{{ ds_nodash }}_{{ hms_triggered }}",
    )

    end = EmptyOperator(
        task_id='end',
    )

    start >> dwh_dummy_absd_plato_tarife >> end