# This Airflow DAG orchestrates the EXIS_SD_APT_BESTANDS job.
# It runs a PySpark application on Dataproc to export stock data to GCS.
# This DAG replaces the legacy UC4 job DW.DWH_EXIS_SD_APT_BESTANDS.

from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitPySparkJobOperator

# --- CONFIGURATION ---
# TODO: Replace with your actual GCP project ID, region, cluster name, and GCS bucket.
GCP_PROJECT_ID = "YOUR_GCP_PROJECT_ID"
DATAPROC_REGION = "YOUR_DATAPROC_REGION"
DATAPROC_CLUSTER_NAME = "YOUR_DATAPROC_CLUSTER_NAME"
GCS_BUCKET_NAME = "YOUR_BUCKET_NAME"
BIGQUERY_DATASET = "YOUR_BIGQUERY_DATASET" # Dataset where SOF_TA_BPR_OPTIONEN, etc., reside

# Path to the PySpark script in GCS (e.g., gs://YOUR_BUCKET_NAME/dags/pyspark/r_exis_v2.py)
# Make sure to upload r_exis_v2.py to this location in GCS.
PYSPARK_JOB_FILE = f"gs://{GCS_BUCKET_NAME}/dags/pyspark/r_exis_v2.py"

# Default arguments for the DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
}

with DAG(
    dag_id="dw_dwh_exis_sd_apt_bestands",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule=None,  # TODO: Define your specific schedule interval, e.g., "@daily", "0 0 * * *", or None for manual runs.
    catchup=False,
    tags=["exis", "dataproc", "pyspark", "bigquery"],
    default_args=default_args,
    description="Orchestrates the EXIS_SD_APT_BESTANDS stock data export via PySpark on Dataproc.",
) as dag:
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    # Task to submit the PySpark job to Dataproc
    dwh_exis_sd_apt_bestands_task = DataprocSubmitPySparkJobOperator(
        task_id="dwh_exis_sd_apt_bestands_pyspark_export",
        project_id=GCP_PROJECT_ID,
        region=DATAPROC_REGION,
        cluster_name=DATAPROC_CLUSTER_NAME,
        main=PYSPARK_JOB_FILE,
        # Arguments to be passed to the PySpark script (r_exis_v2.py)
        arguments=[
            GCP_PROJECT_ID,
            BIGQUERY_DATASET,
            GCS_BUCKET_NAME
        ],
        # TODO: Add any additional Dataproc job configuration here if needed,
        # e.g., spark_driver_memory, spark_executor_cores, properties, etc.
        # Check DataprocSubmitPySparkJobOperator documentation for full options.
    )

    # Define task dependencies
    start >> dwh_exis_sd_apt_bestands_task >> end