# Apache Airflow DAG for r_ausd_rechempf.ksh
# Replaces vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_rechempf.ksh

from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator

with DAG(
    dag_id="r_ausd_rechempf_dag",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule=None, # Define your schedule here, e.g., "0 3 * * *" for daily at 3 AM UTC
    catchup=False,
    tags=["isbert", "fos", "bigquery"],
    params={
        "stichtag": pendulum.now("UTC").format("YYYYMMDD"),  # Default Stichtag to today
        "wiederanlaufwert": None,  # Placeholder for Wiederanlaufwert parameter
    },
) as dag:
    execute_bigquery_transformation = BigQueryExecuteQueryOperator(
        task_id="execute_bigquery_transformation",
        sql="sql/d_ausd_rechempf_bq.sql",
        use_legacy_sql=False,
        gcp_conn_id="google_cloud_default",  # Ensure this connection ID is configured in Airflow
        # Optional: Specify a project and dataset if your BigQuery tables are not in the default project
        # and you need to override the project for specific operations.
        # project_id="your-gcp-project-id",
        # dataset_id="your-default-dataset-id",
    )