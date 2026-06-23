# Airflow DAG to schedule and invoke the BigQuery stored procedure sp_temp_adressabzug_crs
# Replaces legacy UC4 scheduler for job vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_adressen.ksh
# Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_adressen.ksh

from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator

# Define project and dataset for BigQuery resources
# These should be configured appropriately for your GCP environment
BIGQUERY_PROJECT_ID = "project"  # Replace with your GCP project ID
BIGQUERY_DATASET_ID = "dataset"  # Replace with your BigQuery dataset ID

with DAG(
    dag_id="temp_adressabzug_crs",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule=None,  # Set your desired schedule, e.g., "@daily", "0 0 * * *"
    catchup=False,
    tags=["bigquery", "etl", "address"],
    params={
        "stichtag": None,  # Optional: 'DDMMYYYY' format, defaults to current date in SP
        "wiederanlaufwert": None,  # Optional: integer as string, defaults to '0' in SP
    }
) as dag:
    # Task to call the BigQuery stored procedure
    # The parameters are passed as strings and handled by the stored procedure's logic
    call_adressabzug_sp = BigQueryExecuteQueryOperator(
        task_id="call_sp_temp_adressabzug_crs",
        sql=f"""
        CALL `{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET_ID}.sp_temp_adressabzug_crs`(
            p_stichtag => '{{ params.stichtag if params.stichtag is not none else "" }}',
            p_wiederanlaufWert => '{{ params.wiederanlaufwert if params.wiederanlaufwert is not none else "" }}'
        );
        """,
        use_legacy_sql=False,
        gcp_conn_id="google_cloud_default",  # Ensure this connection is configured in Airflow
    )

    # You can add more tasks here, e.g., for monitoring, data quality checks, etc.
    # For example, checking the job_control table for the final status
    # This example only covers the direct invocation.