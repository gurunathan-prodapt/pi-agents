# ── IMPORTS ──────────────────────────────────────────────
from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from datetime import datetime, timedelta

# ── GLOBAL CONFIGURATION (ENVIRONMENT-WIDE CONSTANTS) ─────
GCP_PROJECT = Variable.get("GCP_PROJECT")
GCP_REGION = Variable.get("GCP_REGION")
DATAPROC_CLUSTER = Variable.get("DATAPROC_CLUSTER")
GCS_BUCKET = Variable.get("GCS_BUCKET")

# ── DEFAULT ARGS ─────────────────────────────────────────
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2026, 3, 30),
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

# ── DAG DEFINITION ───────────────────────────────────────
dag = DAG(
    dag_id='dw_dwh_dummy_absd_plato_tarife',
    default_args=default_args,
    description='Migration of UC4 dummy Unix job DW.DWH_DUMMY_ABSD_PLATO_TARIFE',
    schedule=None,  # Handled daily by parent workflow execution chain
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    tags=['uc4_migration', 'dw_dwh_plato_tarif_mapping_taeglich_jp']
)

# ── TASKS ────────────────────────────────────────────────
start = EmptyOperator(
    task_id='start',
    dag=dag
)

pyspark_job_config = {
    'reference': {'project_id': GCP_PROJECT},
    'placement': {'cluster_name': DATAPROC_CLUSTER},
    'pyspark_job': {
        'main_python_file_uri': f'gs://{GCS_BUCKET}/uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/dw_dwh_dummy_absd_plato_tarife_job.py',
        'args': ['--note', 'placeholder_for_dummy_execution']
    }
}

dw_dwh_dummy_absd_plato_tarife = DataprocSubmitJobOperator(
    task_id='dw_dwh_dummy_absd_plato_tarife',
    project_id=GCP_PROJECT,
    region=GCP_REGION,
    job=pyspark_job_config,
    job_id='dw_dwh_dummy_absd_plato_tarife_{{ ts_nodash }}',
    gcp_conn_id='google_cloud_default',
    asynchronous=False, 
    dag=dag
)

end = EmptyOperator(
    task_id='end',
    dag=dag
)

# ── DEPENDENCIES ─────────────────────────────────────────
start >> dw_dwh_dummy_absd_plato_tarife >> end