# File: orchestration/dags/dag_k_ausd_bp_ta_bpr_apn.py

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

PROJECT_ID = "project_id"
DATASET_ID = "isbert_dataset"
PROCEDURE_NAME = f"{PROJECT_ID}.{DATASET_ID}.sp_k_ausd_bp_ta_bpr_apn"

default_args = {
    "owner": "data-platform",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="dag_k_ausd_bp_ta_bpr_apn",
    default_args=default_args,
    description="Orchestrates BigQuery stored procedure for PoolBasisprodukt",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["bigquery", "migration", "poolbasisprodukt"],
) as dag:

    run_sp = BigQueryInsertJobOperator(
        task_id="run_sp_k_ausd_bp_ta_bpr_apn",
        configuration={
            "query": {
                "query": f"""
                CALL `{PROCEDURE_NAME}`(
                  @p_JobKennung,
                  @p_EintragsNr,
                  @p_Stichtag,
                  @p_wiederanlaufWert
                );
                """,
                "useLegacySql": False,
                "parameterMode": "NAMED",
                "queryParameters": [
                    {
                        "name": "p_JobKennung",
                        "parameterType": {"type": "STRING"},
                        "parameterValue": {"value": "{{ dag_run.conf.get('p_JobKennung', 'default_job') }}"}
                    },
                    {
                        "name": "p_EintragsNr",
                        "parameterType": {"type": "STRING"},
                        "parameterValue": {"value": "{{ dag_run.conf.get('p_EintragsNr', 'default_entry') }}"}
                    },
                    {
                        "name": "p_Stichtag",
                        "parameterType": {"type": "STRING"},
                        "parameterValue": {"value": "{{ dag_run.conf.get('p_Stichtag', ds_nodash) }}"}
                    },
                    {
                        "name": "p_wiederanlaufWert",
                        "parameterType": {"type": "STRING"},
                        "parameterValue": {"value": "{{ dag_run.conf.get('p_wiederanlaufWert', '0') }}"}
                    },
                ],
            }
        },
    )