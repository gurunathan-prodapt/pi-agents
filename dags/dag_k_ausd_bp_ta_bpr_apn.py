# Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bpr_apn.ksh
# Purpose: Orchestrates the BigQuery stored procedure sp_k_ausd_bp_ta_bpr_apn daily.

from __future__ import annotations

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

# GCP Configuration Constants (Replace with environment configuration values)
GCP_PROJECT_ID = "gcp-project-dw"
BIGQUERY_DATASET = "dataset_isbert"
GCP_LOCATION = "EU"

DAG_ID = "dw_k_ausd_bp_ta_bpr_apn"
SCHEDULE_CRON = "0 6 * * *"

default_args = {
    "owner": "data_engineering",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    description="Triggers BigQuery stored procedure sp_k_ausd_bp_ta_bpr_apn for PoolBasisprodukt processing.",
    schedule=SCHEDULE_CRON,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=True,
    tags=["bigquery", "composer", "migration", "poolbasisprodukt"],
) as dag:

    # Trigger stored procedure calling dynamically using Airflow macros to inject the variables.
    run_sp_k_ausd_bp_ta_bpr_apn = BigQueryInsertJobOperator(
        task_id="run_sp_k_ausd_bp_ta_bpr_apn",
        configuration={
            "query": {
                "query": f"""
                CALL `{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.sp_k_ausd_bp_ta_bpr_apn`(
                  '{{{{ dag_run.conf.get("p_JobKennung", "DEFAULT_JOB") }}}}',
                  '{{{{ dag_run.conf.get("p_EintragsNr", "DEFAULT_ENTRY") }}}}',
                  '{{{{ dag_run.conf.get("p_Stichtag", ds_format(ds, "%Y-%m-%d", "%d%m%Y")) }}}}',
                  '{{{{ dag_run.conf.get("p_wiederanlaufWert", "0") }}}}'
                )
                """,
                "useLegacySql": False,
            }
        },
        project_id=GCP_PROJECT_ID,
        location=GCP_LOCATION,
    )