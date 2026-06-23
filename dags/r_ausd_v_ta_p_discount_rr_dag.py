# Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_p_discount_rr.ksh
# Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_p_discount_rr.ksh

from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.operators.dummy import DummyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteStoredProcedureOperator

# Define your BigQuery project and dataset IDs
PROJECT_ID = "my_gcp_project"
DATASET_ID = "my_bq_dataset"
MAIN_PROCEDURE_NAME = "Vertragsdatenabgleich"

with DAG(
    dag_id="r_ausd_v_ta_p_discount_rr",
    schedule=None,  # Or set a schedule, e.g., "0 0 * * *" for daily at midnight
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    tags=["bigquery", "data_reconciliation"],
    params={
        "stichtag": {
            "type": "string",
            "title": "Reference Date (DDMMYYYY)",
            "description": "The date for which the data reconciliation should run (e.g., 01012023).",
            "minLength": 8,
            "maxLength": 8,
            "pattern": r"^\d{8}$",
        },
        "laufnummer": {
            "type": "string",
            "title": "Run Number",
            "description": "Unique run number for this execution.",
            "pattern": r"^\d+$",
        },
    },
) as dag:
    start_task = DummyOperator(task_id="start")

    # Call the main BigQuery Stored Procedure
    call_main_procedure = BigQueryExecuteStoredProcedureOperator(
        task_id="call_vertragsdatenabgleich_procedure",
        project_id=PROJECT_ID,
        dataset_id=DATASET_ID,
        procedure_id=MAIN_PROCEDURE_NAME,
        # Pass parameters from Airflow's DAG run config or default if not provided
        parameters=[
            {"name": "p_h", "parameterType": {"type": "BOOL"}, "value": "false"}, # -h is for help, always false for actual execution
            {"name": "p_s", "parameterType": {"type": "STRING"}, "value": "{{ params.stichtag }}"},
            {"name": "p_l", "parameterType": {"type": "STRING"}, "value": "{{ params.laufnummer }}"},
        ],
        # Recommended to use standard SQL for stored procedures
        use_legacy_sql=False,
    )

    end_task = DummyOperator(task_id="end")

    start_task >> call_main_procedure >> end_task