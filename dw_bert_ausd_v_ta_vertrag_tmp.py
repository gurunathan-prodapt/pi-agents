# Legacy Source: DW.BERT_AUSD_V_TA_VERTRAG_TMP (UC4 JOBS_UNIX)
# Job: DW.BERT_AUSD_V_TA_VERTRAG_TMP
# Purpose: Airflow DAG for orchestrating the contract-related data processing.

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from datetime import datetime, timedelta

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# --- GCP Configuration Placeholders ---
# IMPORTANT: Replace these with your actual GCP project ID, region, cluster name, and GCS bucket.
GCP_PROJECT_ID = 'your-gcp-project-id'
GCP_REGION = 'your-gcp-region'  # e.g., 'us-central1'
DATAPROC_CLUSTER_NAME = 'your-dataproc-cluster-name'
GCS_PYSPARK_BUCKET = 'your-gcs-bucket-for-pyspark-scripts' # e.g., 'gs://my-dataproc-scripts'
# -----------------------------------

with DAG(
    dag_id='dw_bert_ausd_v_ta_vertrag_tmp',
    default_args=default_args,
    description='Orchestrates contract-related data processing using Dataproc PySpark.',
    schedule_interval=None,  # Set your desired schedule, e.g., '0 0 * * *' for daily
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['dataproc', 'pyspark', 'contract'],
) as dag:
    start_task = DummyOperator(
        task_id='start',
    )

    # Task to submit the PySpark job to Dataproc
    submit_pyspark_job = DataprocSubmitJobOperator(
        task_id='dw_bert_ausd_v_ta_vertrag_tmp_pyspark_job',
        project_id=GCP_PROJECT_ID,
        region=GCP_REGION,
        job={
            "placement": {
                "cluster_name": DATAPROC_CLUSTER_NAME
            },
            "pyspark_job": {
                "main_python_file_uri": f"{GCS_PYSPARK_BUCKET}/pyspark_scripts/r_ausd_v_ta_vertrag_tmp.py",
                # Pass UC4 job variables as arguments to the PySpark script
                "args": [
                    "--job_kennung", "AUSD_V_TA_VERTRAG_TMP",
                    # Add any other arguments or configurations required by your PySpark script
                ],
                "properties": {
                    # Optional: Add any specific Dataproc job properties here, e.g.,
                    # "spark.executor.memory": "4g",
                    # "spark.driver.memory": "4g"
                }
            }
        },
        # api_version="v1" # Use this if not default
    )

    end_task = DummyOperator(
        task_id='end',
    )

    # Define task dependencies
    start_task >> submit_pyspark_job >> end_task