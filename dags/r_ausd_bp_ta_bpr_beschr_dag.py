# Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_beschr.ksh
# Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_beschr.ksh
# Description: Apache Airflow DAG to schedule and trigger the BigQuery Stored Procedure.

from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteStoredProcedureOperator

PROJECT_ID = "project"  # Replace with your GCP project ID
DATASET_ID = "dataset"  # Replace with your BigQuery dataset ID
STORED_PROCEDURE_NAME = "ausd_bp_ta_bpr_beschr"

with DAG(
    dag_id="r_ausd_bp_ta_bpr_beschr_dag",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule=None,  # Set your desired schedule, e.g., "@daily", "0 0 * * *"
    catchup=False,
    tags=["bigquery", "etl"],
    params={
        "stichtag_string": {
            "type": "string",
            "title": "Stichtag (DDMMYYYY)",
            "description": "Processing date in DDMMYYYY format. Defaults to current date if empty.",
            "default": "",
            "examples": ["01012023", ""],
        },
        "wiederanlauf_wert": {
            "type": "integer",
            "title": "Restart Value",
            "description": "Restart value for incremental processing. Defaults to 0 (full refresh) if empty.",
            "default": 0,
            "examples": [0, 1000],
        },
    },
) as dag:
    start_task = BashOperator(
        task_id="start",
        bash_command="echo 'Starting BigQuery Stored Procedure execution...'",
    )

    call_bigquery_sp = BigQueryExecuteStoredProcedureOperator(
        task_id="call_ausd_bp_ta_bpr_beschr_sp",
        project_id=PROJECT_ID,
        dataset_id=DATASET_ID,
        procedure_id=STORED_PROCEDURE_NAME,
        parameters=[
            {"name": "p_stichtag_string", "parameterType": {"type": "STRING"}, "parameterValue": "{{ params.stichtag_string }}"},
            {"name": "p_wiederanlaufWert", "parameterType": {"type": "INT64"}, "parameterValue": "{{ params.wiederanlauf_wert }}"},
        ],
        gcp_conn_id="google_cloud_default", # Ensure this connection is configured in Airflow
    )

    end_task = BashOperator(
        task_id="end",
        bash_command="echo 'BigQuery Stored Procedure execution completed.'",
    )

    start_task >> call_bigquery_sp >> end_task