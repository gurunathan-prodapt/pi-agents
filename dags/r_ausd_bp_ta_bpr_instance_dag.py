# Airflow DAG for orchestrating the BigQuery stored procedure
# Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_instance.ksh
# Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_instance.ksh

from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

with DAG(
    dag_id="r_ausd_bp_ta_bpr_instance_dag",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule=None, # Define your schedule here, e.g., "@daily", "0 0 * * *"
    catchup=False,
    tags=["bigquery", "etl", "isbert"],
    params={
        "stichtag_str": None,  # Processing date in DDMMYYYY format (optional)
        "wiederanlaufwert": 0, # Restart value (optional, defaults to 0)
    },
) as dag:
    call_main_procedure = BigQueryInsertJobOperator(
        task_id="call_ausd_bp_ta_bpr_instance_sp",
        project_id="project", # Replace with your GCP Project ID
        configuration={
            "query": {
                "query": """
                    CALL project.dataset.ausd_bp_ta_bpr_instance(
                        p_stichtag_str => @stichtag_str,
                        p_wiederanlaufwert => @wiederanlaufwert
                    );
                """,
                "useLegacySql": False,
                "queryParameters": [
                    {
                        "name": "stichtag_str",
                        "parameterType": {"type": "STRING"},
                        "parameterValue": {"value": "{{ params.stichtag_str if params.stichtag_str is not none else '' }}"},
                    },
                    {
                        "name": "wiederanlaufwert",
                        "parameterType": {"type": "INT64"},
                        "parameterValue": {"value": "{{ params.wiederanlaufwert }}"},
                    },
                ],
            }
        },
    )