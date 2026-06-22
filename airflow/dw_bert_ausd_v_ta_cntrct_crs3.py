# Migrated from vobs/dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/BERT/PRODUKTION/DW.BERT_STAMMDATEN_JP/DW.BERT_P_VERTRAG_JP/DW.BERT_AUSD_V_TA_CNTRCT_CRS3.xml
# Job: DW.BERT_AUSD_V_TA_CNTRCT_CRS3

from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0, # As per design, no retries by default
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='dw_bert_ausd_v_ta_cntrct_crs3',
    default_args=default_args,
    description='Orchestrates contract data synchronization to BigQuery.',
    start_date=days_ago(1), # Fixed past date as per design
    schedule_interval=None, # Manual trigger as per design
    catchup=False,
    tags=['bert', 'contract', 'bigquery', 'dataproc'],
) as dag:
    
    # Placeholder for Dataproc cluster configuration.
    # These values should be configured for your GCP environment.
    PROJECT_ID = 'your-gcp-project-id'
    REGION = 'your-gcp-region' # e.g., 'us-central1'
    CLUSTER_NAME = 'your-dataproc-cluster-name'
    GCS_BUCKET_FOR_SCRIPTS = 'gs://your-gcs-bucket-for-dataproc-scripts' # GCS bucket where r_ausd_v_ta_cntrct_crs3.py is stored

    submit_dataproc_job = DataprocSubmitJobOperator(
        task_id='bert_ausd_v_ta_cntrct_crs3',
        project_id=PROJECT_ID,
        region=REGION,
        job={
            "placement": {
                "cluster_name": CLUSTER_NAME
            },
            "pyspark_job": { # Using pyspark_job for Python script execution on Dataproc
                "main_python_file_uri": f"{GCS_BUCKET_FOR_SCRIPTS}/r_ausd_v_ta_cntrct_crs3.py",
                "args": [
                    "--job_kennung",
                    "DW.BERT_AUSD_V_TA_CNTRCT_CRS3"
                    # Add --eintrags_nr if it becomes relevant, default is "0" in the script
                ],
            },
        },
    )