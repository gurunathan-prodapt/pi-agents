import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.models.param import Param
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

# Environment variables with sensible defaults from the Migration Design Document
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID", "prod-isbert-data")
BQ_DATASET = os.getenv("BQ_DATASET", "isbert_aufbereitung")
GCP_CONN_ID = "google_cloud_default"

default_args = {
    "owner": "isbert_etl",
    "depends_on_past": False,
    "email_on_failure": True,
    "email": ["isbert_alerts@example.com"],
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="k_ausd_bp_ta_rn_einzeln_dag",
    default_args=default_args,
    description="Orchestrator DAG for k_ausd_bp_ta_rn_einzeln.ksh (PoolBasisprodukt data processing)",
    schedule_interval=None,  # Triggered on-demand or by parent orchestrator
    start_date=datetime(2023, 1, 1),
    catchup=False,
    params={
        "job_kennung": Param("", type="string", description="Jobkennung parameter (e.g., J12345)"),
        "eintrags_nr": Param("", type="string", description="EintragsNr parameter (e.g., E9876)"),
        "stichtag": Param("", type="string", description="Stichtag date format DDMMYYYY (e.g., 31122023)"),
        "wiederanlauf_wert": Param("0", type="string", description="Wiederanlaufwert (restart value, defaults to '0')"),
    },
) as dag:

    # Execute the BigQuery stored procedure representing the core legacy shell script wrapper
    execute_stored_procedure = BigQueryInsertJobOperator(
        task_id="execute_sp_k_ausd_bp_ta_rn_einzeln",
        gcp_conn_id=GCP_CONN_ID,
        configuration={
            "query": {
                "query": f"""
                    CALL `{GCP_PROJECT_ID}.{BQ_DATASET}.sp_k_ausd_bp_ta_rn_einzeln_safe`(
                        @job_kennung,
                        @eintrags_nr,
                        @stichtag,
                        @wiederanlauf_wert
                    )
                """,
                "useLegacySql": False,
                "parameterMode": "NAMED",
                "queryParameters": [
                    {
                        "name": "job_kennung",
                        "parameterType": {"type": "STRING"},
                        "parameterValue": {"value": "{{ params.job_kennung }}"},
                    },
                    {
                        "name": "eintrags_nr",
                        "parameterType": {"type": "STRING"},
                        "parameterValue": {"value": "{{ params.eintrags_nr }}"},
                    },
                    {
                        "name": "stichtag",
                        "parameterType": {"type": "STRING"},
                        "parameterValue": {"value": "{{ params.stichtag }}"},
                    },
                    {
                        "name": "wiederanlauf_wert",
                        "parameterType": {"type": "STRING"},
                        "parameterValue": {"value": "{{ params.wiederanlauf_wert }}"},
                    },
                ],
            }
        },
    )

    execute_stored_procedure