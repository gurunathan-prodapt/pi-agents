from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

# Header for generated file
# Replaces: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_apn_vertrag.ksh
# Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_apn_vertrag.ksh

with DAG(
    dag_id="k_ausd_bp_ta_apn_vertrag_dag",
    schedule=None, # Define your schedule, e.g., "@daily"
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    tags=["bigquery", "etl"],
    params={
        "job_kennung": "DEFAULT_JOB",
        "eintrags_nr": "001",
        "stichtag": pendulum.now().format("DDMMYYYY"), # Default to today's date
        "wiederanlauf_wert": "0",
    },
) as dag:
    call_bigquery_sp = BigQueryInsertJobOperator(
        task_id="call_k_ausd_bp_ta_apn_vertrag_sp",
        project_id="project", # Replace with your GCP project ID
        configuration={
            "query": {
                "query": """
                    CALL project.dataset.sp_k_ausd_bp_ta_apn_vertrag(
                        @job_kennung,
                        @eintrags_nr,
                        @stichtag,
                        @wiederanlauf_wert
                    );
                """,
                "useLegacySql": False,
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