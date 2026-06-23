# Legacy source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_geschaeftspartner.ksh
# Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_geschaeftspartner.ksh

from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitPySparkJobOperator

# Default arguments for the DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
}

with DAG(
    dag_id="r_ausd_geschaeftspartner_dag",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule=None,  # No legacy scheduling information, so manual trigger
    catchup=False,
    tags=["isbert", "business_partner", "dataproc", "bigquery"],
    default_args=default_args,
    description="Airflow DAG for orchestrating the initial provisioning of contract caches for the demand scoring system (FOS).",
) as dag:
    # Get the processing date (Stichtag) from Airflow's execution date or manual trigger config
    # Default to current date if not provided in DAG run configuration
    # The format required by PySpark script is YYYYMMDD
    # Use {{ ds_nodash }} for the execution date in YYYYMMDD format
    # Allow overriding with 'stichtag' from DAG run config
    stichtag_yyyymmdd = "{{ dag_run.conf.get('stichtag', ds_nodash) }}"

    # DataprocSubmitPySparkJobOperator to execute the PySpark script
    # Assumes a Dataproc cluster named 'your-dataproc-cluster' and a GCS bucket for the PySpark script.
    # The PySpark script k_ausd_geschaeftspartner.py and its corresponding SQL file
    # d_ausd_geschaeftspartner_bq.sql should be uploaded to a GCS bucket.
    # Example paths:
    #   main_python_file: gs://your-gcs-bucket/dataproc_jobs/pyspark/k_ausd_geschaeftspartner.py
    #   sql_file_path argument: gs://your-gcs-bucket/dataproc_jobs/sql/d_ausd_geschaeftspartner_bq.sql
    run_contract_cache_initial_load = DataprocSubmitPySparkJobOperator(
        task_id="run_contract_cache_initial_load",
        project_id="your-gcp-project-id", # Replace with your GCP Project ID
        region="your-gcp-region",         # Replace with your GCP Region (e.g., 'us-central1')
        cluster_name="your-dataproc-cluster", # Replace with your Dataproc cluster name
        main_python_file="gs://your-gcs-bucket/dataproc_jobs/pyspark/k_ausd_geschaeftspartner.py",
        arguments=[
            "--sql_file_path", "gs://your-gcs-bucket/dataproc_jobs/sql/d_ausd_geschaeftspartner_bq.sql",
            "--stichtag_yyyymmdd", stichtag_yyyymmdd,
            # If the original k_ausd_geschaeftspartner.ksh used $p_wiederanlaufWert,
            # and it needs to be passed to the PySpark script, uncomment and configure below:
            # "--wiederanlaufwert", "{{ dag_run.conf.get('wiederanlaufwert', '0') }}",
        ],
    )