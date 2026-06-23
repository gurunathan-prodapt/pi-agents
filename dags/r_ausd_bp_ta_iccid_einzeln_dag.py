# Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_iccid_einzeln.ksh
# Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_iccid_einzeln.ksh

from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteStoredProcedureOperator

# Define your GCP project and dataset
GCP_PROJECT_ID = "project"
BQ_DATASET_ID = "dataset"

with DAG(
    dag_id="r_ausd_bp_ta_iccid_einzeln",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule=None,  # Set your desired schedule, e.g., "@daily", "0 0 * * *"
    catchup=False,
    tags=["bigquery", "etl", "isbert"],
    params={
        "stichtag_raw": {
            "type": "string",
            "title": "Stichtag (DDMMYYYY)",
            "description": "Processing date in DDMMYYYY format. Defaults to current date if empty.",
            "default": "",
        },
        "wiederanlaufwert_raw": {
            "type": "string",
            "title": "Wiederanlaufwert",
            "description": "Restart value for DWH_VERTRAG_ID filter. Optional.",
            "default": "",
        },
    }
) as dag:
    call_wrapper_sp = BigQueryExecuteStoredProcedureOperator(
        task_id="call_ausd_bp_ta_iccid_einzeln_wrapper",
        project_id=GCP_PROJECT_ID,
        dataset_id=BQ_DATASET_ID,
        procedure_id="ausd_bp_ta_iccid_einzeln_wrapper",
        parameters=[
            {"name": "p_stichtag_raw", "parameterType": {"type": "STRING"}, "value": "{{ params.stichtag_raw }}"},
            {"name": "p_wiederanlaufWert_raw", "parameterType": {"type": "STRING"}, "value": "{{ params.wiederanlaufwert_raw }}"},
        ],
    )