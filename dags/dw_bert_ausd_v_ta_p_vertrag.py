# This Airflow DAG replaces the UC4 job DW.BERT_AUSD_V_TA_P_VERTRAG.
# It orchestrates a PySpark job on Dataproc.

from datetime import timedelta
from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.utils.dates import days_ago

# GCP Configuration
GCP_PROJECT_ID = "YOUR_GCP_PROJECT_ID"
DATAPROC_REGION = "YOUR_DATAPROC_REGION"
DATAPROC_CLUSTER_NAME = "YOUR_DATAPROC_CLUSTER_NAME"
GCS_BUCKET_NAME = "YOUR_BUCKET_NAME"
PYSPARK_SCRIPT_URI = f"gs://{GCS_BUCKET_NAME}/pyspark_scripts/r_ausd_v_ta_p_vertrag.py"

default_args = {
    'owner': 'data_platform',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(seconds=0),
    'start_date': days_ago(1), # Placeholder, adjust as needed
}

with DAG(
    dag_id='dw_bert_ausd_v_ta_p_vertrag',
    default_args=default_args,
    description='Airflow DAG for DW.BERT_AUSD_V_TA_P_VERTRAG',
    schedule_interval=None, # No schedule derived from source UC4 XML
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    tags=['uc4', 'dataproc', 'pyspark', 'bigquery'],
) as dag:
    run_pyspark_job = DataprocSubmitJobOperator(
        task_id='run_bert_ausd_v_ta_p_vertrag',
        project_id=GCP_PROJECT_ID,
        region=DATAPROC_REGION,
        job={
            'placement': {'cluster_name': DATAPROC_CLUSTER_NAME},
            'pyspark_job': {
                'main_python_file_uri': PYSPARK_SCRIPT_URI,
                'args': [
                    '--job_kennung', 'AUSD_V_TA_P_VERTRAG'
                ],
            },
        },
    )