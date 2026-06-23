"""
Airflow DAG for k_ausd_v_ta_notice.ksh migration.
Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_notice.ksh

This DAG orchestrates the execution of the BigQuery Stored Procedure
`your_project_id.isbert_reporting.r_ausd_v_ta_notice`, which handles the core
data processing logic migrated from the original KornShell script and its
associated SQL file.
"""

from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteStoredProcedureOperator

# Define your Google Cloud Project and BigQuery Dataset IDs
# IMPORTANT: Replace "your_project_id" and "isbert_reporting" with your actual IDs.
BIGQUERY_PROJECT_ID = "your_project_id"
BIGQUERY_DATASET_ID = "isbert_reporting"
BIGQUERY_SP_NAME = "r_ausd_v_ta_notice"

with DAG(
    dag_id="k_ausd_v_ta_notice_dag",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule=None,  # Set a schedule (e.g., "@daily", "0 0 * * *") or leave None for manual triggers.
    catchup=False,  # Do not run for past missed schedules
    tags=["isbert", "bigquery", "data-processing"],
    doc_md=__doc__,
    params={
        "job_kennung": {
            "type": "string",
            "title": "Job Identifier",
            "description": "Unique identifier for the job run (e.g., 'BERT_TA_NOTICE_DAILY', 'BERT_TA_NOTICE_ADHOC')",
            "default": "BERT_TA_NOTICE_ADHOC",
            "minLength": 1,
            "maxLength": 50,
            "pattern": r"^[a-zA-Z0-9_-]+$"
        },
        "eintrags_nr": {
            "type": "string",
            "title": "Entry Number (YYYYMMDD Date String)",
            "description": "Date string in YYYYMMDD format for data filtering (e.g., '20231026'). Defaults to yesterday.",
            "default": (pendulum.today("UTC") - pendulum.duration(days=1)).strftime("%Y%m%d"),
            "pattern": r"^\d{8}$"  # Ensures YYYYMMDD format
        }
    }
) as dag:
    call_bigquery_sp = BigQueryExecuteStoredProcedureOperator(
        task_id="call_r_ausd_v_ta_notice_sp",
        project_id=BIGQUERY_PROJECT_ID,
        dataset_id=BIGQUERY_DATASET_ID,
        procedure_id=BIGQUERY_SP_NAME,
        # Ensure 'google_cloud_default' connection is configured in your Airflow environment
        gcp_conn_id="google_cloud_default",
        # Pass parameters from the Airflow DAG run to the BigQuery Stored Procedure
        parameters={
            "p_JobKennung": "{{ params.job_kennung }}",
            "p_EintragsNr": "{{ params.eintrags_nr }}"
        }
    )