# Apache Airflow DAG for orchestrating the BigQuery Stored Procedure
# Legacy Orchestration: UC4 job DW.BERT_P_BASISPRODUKT_JP/DW.BERT_AUSD_BP_TA_BPR_BESCHR.xml
# Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_beschr.ksh
# Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_beschr.ksh

from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteStoredProcedureOperator

with DAG(
    dag_id="bert_ausd_bp_ta_bpr_beschr_dag",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule=None,  # Define your schedule here, e.g., "@daily" or "0 5 * * *"
    catchup=False,
    tags=["bert", "bigquery", "etl"],
    params={
        "stichtag": None,  # Optional: Override Stichtag, expects DDMMYYYY format
        "wiederanlaufwert": 0, # Optional: Override Wiederanlaufwert
    }
) as dag:
    call_ausd_bp_ta_bpr_beschr_sp = BigQueryExecuteStoredProcedureOperator(
        task_id="call_ausd_bp_ta_bpr_beschr_sp",
        project_id="project",  # Replace with your Google Cloud Project ID
        dataset_id="dataset",  # Replace with your BigQuery Dataset ID
        procedure_id="ausd_bp_ta_bpr_beschr",
        parameters=[
            {"name": "p_stichtag", "parameterType": {"type": "STRING"}, "value": "{{ params.stichtag or ds_nodash }}"},
            {"name": "p_wiederanlaufWert", "parameterType": {"type": "INT64"}, "value": "{{ params.wiederanlaufwert }}"},
        ],
    )