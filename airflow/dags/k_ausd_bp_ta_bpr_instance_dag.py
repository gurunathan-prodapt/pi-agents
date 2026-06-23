"""
Airflow DAG for k_ausd_bp_ta_bpr_instance.
Orchestrates the execution of BigQuery stored procedures for basis product instance data preparation.
Replaces the legacy KornShell script: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bpr_instance.ksh
"""

from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteStoredProcedureOperator

# Define BigQuery project and dataset IDs
PROJECT_ID = "my_gcp_project"
DATASET_ID = "my_bq_dataset"

with DAG(
    dag_id="k_ausd_bp_ta_bpr_instance_dag",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule=None,  # Or define a schedule like "@daily", "0 0 * * *"
    catchup=False,
    tags=["bigquery", "data_preparation"],
    params={
        "p_jobkennung": {
            "type": "string",
            "title": "Job Identifier",
            "description": "Identifier for the job run (e.g., BERT_BP_TA_BPR)",
            "default": "BERT_BP_TA_BPR",
            "minLength": 1
        },
        "p_eintragsnr": {
            "type": "string",
            "title": "Entry Number",
            "description": "Entry number for job tracking (e.g., '1')",
            "default": "1",
            "minLength": 1
        },
        "p_stichtag": {
            "type": "string",
            "title": "Reference Date (YYYYMMDD)",
            "description": "The reference date for data processing in YYYYMMDD format (e.g., '20230115'). Defaults to yesterday.",
            "default": "{{ yesterday_ds_nodash }}",
            "minLength": 8,
            "maxLength": 8,
            "pattern": "^[0-9]{8}$"
        },
        "p_wiederanlaufwert": {
            "type": "string",
            "title": "Restart Value",
            "description": "Value for restart logic (e.g., 'N' for no restart, 'Y' for restart).",
            "default": "N"
        },
    }
) as dag:
    start_task = EmptyOperator(task_id="start")

    # Task to execute the main orchestration BigQuery Stored Procedure
    execute_bpr_instance_procedure = BigQueryExecuteStoredProcedureOperator(
        task_id="execute_bpr_instance_procedure",
        project_id=PROJECT_ID,
        dataset_id=DATASET_ID,
        procedure_id="r_ausd_bp_ta_bpr_instance",
        parameters=[
            {"name": "p_JobKennung", "parameterType": {"type": "STRING"}, "value": "{{ params.p_jobkennung }}"},
            {"name": "p_EintragsNr", "parameterType": {"type": "STRING"}, "value": "{{ params.p_eintragsnr }}"},
            {"name": "p_Stichtag", "parameterType": {"type": "STRING"}, "value": "{{ params.p_stichtag }}"},
            {"name": "p_wiederanlaufWert", "parameterType": {"type": "STRING"}, "value": "{{ params.p_wiederanlaufwert }}"},
        ],
        # The stored procedure returns records_processed as an OUT parameter.
        # This value can be retrieved for logging or subsequent tasks if needed.
        # However, BigQueryExecuteStoredProcedureOperator does not directly capture OUT params in XComs.
        # A workaround would involve writing the OUT parameter to a log table or returning it from a SELECT query wrapper.
        # For this example, we assume the procedure handles its own logging.
    )

    end_task = EmptyOperator(task_id="end")

    start_task >> execute_bpr_instance_procedure >> end_task