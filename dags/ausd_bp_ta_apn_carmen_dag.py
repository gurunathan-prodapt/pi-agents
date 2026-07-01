"""
Airflow DAG for ausd_bp_ta_apn_carmen migration.

This DAG orchestrates the daily run of the BigQuery stored procedure
`sof.proc_d_ausd_bp_ta_apn_carmen`, replacing the legacy UC4 job
`DW.BERT_AUSD_BP_TA_APN_CARMEN` and its wrapper scripts.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

# Configuration from the Migration Design Document
GCP_PROJECT_ID = "gcp-project-id"
CONN_ID = "bigquery_default"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="ausd_bp_ta_apn_carmen_dag",
    description="Orchestrates the active GPRS/UMTS PDP context and APN association mapping.",
    schedule="0 6 * * *",  # Run daily at 06:00
    start_date=datetime(2023, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["migration", "bigquery", "sof"],
) as dag:

    run_stored_procedure = BigQueryInsertJobOperator(
        task_id="run_proc_d_ausd_bp_ta_apn_carmen",
        gcp_conn_id=CONN_ID,
        configuration={
            "query": {
                "query": f"CALL `{GCP_PROJECT_ID}.sof.proc_d_ausd_bp_ta_apn_carmen`();",
                "useLegacySql": False,
            }
        },
    )

    run_stored_procedure