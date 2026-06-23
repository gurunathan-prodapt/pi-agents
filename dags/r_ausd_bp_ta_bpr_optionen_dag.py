# Migrated from vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_optionen.ksh
# Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_optionen.ksh

from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteStoredProcedureOperator

# Define your GCP Project and BigQuery Dataset here
# IMPORTANT: Replace these placeholders with your actual GCP project ID and BigQuery dataset ID
GCP_PROJECT_ID = "your_gcp_project"
BQ_DATASET_ID = "your_bq_dataset"

with DAG(
    dag_id="r_ausd_bp_ta_bpr_optionen",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule=None,  # Set your desired schedule, e.g., "@daily", "0 0 * * *"
    catchup=False,
    tags=["bigquery", "etl", "isbert"],
    params={
        "stichtag": None,  # Optional: Cutoff date in DDMMYYYY format, e.g., "01012023"
        "wiederanlaufWert": 0,  # Optional: Restart value, default to 0
    },
    default_args={
        "owner": "airflow",
        "depends_on_past": False,
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": pendulum.duration(minutes=5),
    },
) as dag:
    call_main_procedure = BigQueryExecuteStoredProcedureOperator(
        task_id="call_r_ausd_bp_ta_bpr_optionen_sp",
        project_id=GCP_PROJECT_ID,
        dataset_id=BQ_DATASET_ID,
        procedure_id="r_ausd_bp_ta_bpr_optionen",
        gcp_conn_id="google_cloud_default",  # Ensure this connection is configured in Airflow
        parameters=[
            {"name": "p_stichtag", "parameterType": {"type": "STRING"}, "value": "{{ params.stichtag }}"},
            {"name": "p_wiederanlaufWert", "parameterType": {"type": "INT64"}, "value": "{{ params.wiederanlaufWert }}"},
        ],
    )

    # You can add more tasks here, e.g., for data validation, notification, etc.