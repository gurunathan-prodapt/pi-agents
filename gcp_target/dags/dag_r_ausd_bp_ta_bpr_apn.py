from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

GCP_PROJECT_ID = "${GCP_PROJECT_ID}"
GCP_DATASET = "${GCP_DATASET}"

default_args = {
    "owner": "data-platform",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}

with DAG(
    dag_id="dag_r_ausd_bp_ta_bpr_apn",
    default_args=default_args,
    description="Orchestrates BigQuery procedure r_ausd_bp_ta_bpr_apn",
    schedule_interval="0 6 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["bigquery", "migration", "isbert"],
) as dag:

    run_wrapper_proc = BigQueryInsertJobOperator(
        task_id="run_r_ausd_bp_ta_bpr_apn",
        configuration={
            "query": {
                "query": f"""
                CALL `{GCP_PROJECT_ID}.{GCP_DATASET}.r_ausd_bp_ta_bpr_apn`(
                  @p_JobKennung,
                  @p_EintragsNr,
                  @p_Stichtag,
                  @p_wiederanlaufWert
                )
                """,
                "useLegacySql": False,
                "parameterMode": "NAMED",
                "queryParameters": [
                    {"name": "p_JobKennung", "parameterType": {"type": "STRING"}, "parameterValue": {"value": "{{ dag_run.conf.get('p_JobKennung') }}"}},
                    {"name": "p_EintragsNr", "parameterType": {"type": "STRING"}, "parameterValue": {"value": "{{ dag_run.conf.get('p_EintragsNr') }}"}},
                    {"name": "p_Stichtag", "parameterType": {"type": "STRING"}, "parameterValue": {"value": "{{ dag_run.conf.get('p_Stichtag') }}"}},
                    {"name": "p_wiederanlaufWert", "parameterType": {"type": "STRING"}, "parameterValue": {"value": "{{ dag_run.conf.get('p_wiederanlaufWert', '0') }}"}},
                ],
            }
        },
    )