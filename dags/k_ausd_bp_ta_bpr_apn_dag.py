# Airflow DAG for k_ausd_bp_ta_bpr_apn.ksh migration
# Replaces vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bpr_apn.ksh
# Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bpr_apn.ksh

from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteStoredProcedureOperator

with DAG(
    dag_id="k_ausd_bp_ta_bpr_apn_dag",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule=None,  # Define your schedule here, e.g., "@daily", "0 0 * * *"
    catchup=False,
    tags=["bigquery", "etl"],
    params={
        "job_kennung": {
            "type": "string",
            "title": "Job Identifier",
            "description": "Unique identifier for the job.",
            "default": "DEFAULT_JOB",
            "minLength": 1,
        },
        "eintrags_nr": {
            "type": "string",
            "title": "Entry Number",
            "description": "Entry number for processing.",
            "default": "1",
            "minLength": 1,
        },
        "stichtag": {
            "type": "string",
            "title": "Reference Date (DDMMYYYY)",
            "description": "Reference date in DDMMYYYY format.",
            "default": pendulum.today("UTC").format("DDMMYYYY"),
            "pattern": r"^\d{8}$",
        },
        "wiederanlauf_wert": {
            "type": ["integer", "null"],
            "title": "Restart Value",
            "description": "Optional restart value (integer).",
            "default": 0,
            "minimum": 0,
        },
    }
) as dag:
    start_task = EmptyOperator(task_id="start")

    call_main_bigquery_sp = BigQueryExecuteStoredProcedureOperator(
        task_id="call_main_bigquery_stored_procedure",
        project_id="{{ var.value.get('gcp_project_id', 'your-gcp-project-id') }}", # Replace with your GCP Project ID or use Airflow Variable
        dataset_id="prod_dw_isrpt",
        procedure_id="k_ausd_bp_ta_bpr_apn_sp",
        parameters=[
            {"name": "p_JobKennung", "value": "{{ params.job_kennung }}"},
            {"name": "p_EintragsNr", "value": "{{ params.eintrags_nr }}"},
            {"name": "p_Stichtag", "value": "{{ params.stichtag }}"},
            {"name": "p_wiederanlaufWert", "value": "{{ params.wiederanlauf_wert }}"},
        ],
        gcp_conn_id="google_cloud_default", # Ensure this connection is configured in Airflow
    )

    end_task = EmptyOperator(task_id="end")

    start_task >> call_main_bigquery_sp >> end_task