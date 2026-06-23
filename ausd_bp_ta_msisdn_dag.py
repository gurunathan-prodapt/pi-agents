# Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_msisdn.ksh
# Description: Apache Airflow DAG for orchestrating the BigQuery Stored Procedure.
from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteStoredProcedureOperator
from airflow.operators.bash import BashOperator

# Replace with your GCP project and dataset IDs
GCP_PROJECT_ID = "project"
BQ_DATASET_ID = "dataset"

with DAG(
    dag_id="ausd_bp_ta_msisdn_orchestration",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule=None,  # Set your desired schedule, e.g., "@daily", "0 3 * * *"
    catchup=False,
    tags=["bigquery", "orchestration", "bert"],
    params={
        "p_stichtag_string": "",  # Optional: "DDMMYYYY" format, defaults to current date in SP if empty
        "p_wiederanlaufWert": 0,  # Optional: Integer, defaults to 0 in SP
    }
) as dag:
    start_task = BashOperator(
        task_id="start",
        bash_command="echo 'Starting BigQuery Stored Procedure orchestration for ausd_bp_ta_msisdn'",
    )

    execute_bp_ta_msisdn_wrapper = BigQueryExecuteStoredProcedureOperator(
        task_id="execute_ausd_bp_ta_msisdn_wrapper",
        project_id=GCP_PROJECT_ID,
        dataset_id=BQ_DATASET_ID,
        procedure_id="ausd_bp_ta_msisdn_wrapper",
        parameters=[
            {"name": "p_stichtag_string", "parameterType": {"type": "STRING"}, "parameterValue": "{{ params.p_stichtag_string }}"},
            {"name": "p_wiederanlaufWert", "parameterType": {"type": "INT64"}, "parameterValue": "{{ params.p_wiederanlaufWert }}"},
        ],
    )

    end_task = BashOperator(
        task_id="end",
        bash_command="echo 'BigQuery Stored Procedure orchestration for ausd_bp_ta_msisdn completed'",
    )

    start_task >> execute_bp_ta_msisdn_wrapper >> end_task