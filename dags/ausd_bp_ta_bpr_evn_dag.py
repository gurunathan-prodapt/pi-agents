-- Legacy source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_evn.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_evn.ksh

"""
Cloud Composer DAG to orchestrate the BigQuery stored procedure
your_gcp_project.your_bigquery_dataset.ausd_bp_ta_bpr_evn_wrapper.

Schedule: daily (@daily)
Task:
- call_wrapper_sp: calls the wrapper stored procedure with p_stichtag = {{ ds }}
  and p_wiederanlaufWert = 0 (configurable via params).
"""

from datetime import datetime

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

# GCP configuration constants
GCP_PROJECT_ID = "your_gcp_project"  # TODO: replace with your GCP project ID
BIGQUERY_DATASET = "your_bigquery_dataset"  # TODO: replace with your BigQuery dataset
BIGQUERY_LOCATION = "US"  # TODO: replace with your BigQuery location if needed

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
}

with DAG(
    dag_id="ausd_bp_ta_bpr_evn_dag",
    description="Orchestrates the BigQuery stored procedure ausd_bp_ta_bpr_evn_wrapper.",
    default_args=default_args,
    start_date=datetime(2023, 1, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["bert", "bigquery", "etl"],
) as dag:
    # Calls the wrapper stored procedure with the execution date and restart value.
    call_wrapper_sp = BigQueryInsertJobOperator(
        task_id="call_wrapper_sp",
        project_id=GCP_PROJECT_ID,
        location=BIGQUERY_LOCATION,
        configuration={
            "query": {
                "query": (
                    f"CALL `{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.ausd_bp_ta_bpr_evn_wrapper`("
                    f"'{ { ds } }', "  # Pass ds as a string literal
                    "{{ params.p_wiederanlaufWert }}"
                    ")"
                ),
                "useLegacySql": False,
            }
        },
        params={
            "p_wiederanlaufWert": 0,
        },
    )