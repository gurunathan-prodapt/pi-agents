# Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_iccid_vertrag.ksh
# Description: Airflow DAG to orchestrate the BigQuery Stored Procedure for
# `ausd_bp_ta_iccid_vertrag_wrapper`, passing necessary parameters.

import os
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta

# Define project and dataset (replace with your actual values)
PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "your-gcp-project-id")
DATASET_ID = "your_bigquery_dataset" # e.g., 'isbert_dwh_migrated'

with DAG(
    dag_id="ausd_bp_ta_iccid_vertrag_orchestrator",
    start_date=days_ago(1),
    schedule_interval=timedelta(days=1), # Example: Run daily
    catchup=False,
    tags=["isbert", "bigquery", "orchestrator"],
    description="Orchestrates the BigQuery Stored Procedure for ausd_bp_ta_iccid_vertrag_wrapper",
) as dag:
    # Get the execution date (ds_nodash: YYYYMMDD) and format it to DDMMYYYY for p_stichtag
    stichtag_formatted = "{{ ds_nodash[6:8] + ds_nodash[4:6] + ds_nodash[0:4] }}"

    # Define parameters for the BigQuery Stored Procedure
    # p_wiederanlaufWert is set to 0 as a common default, but can be made dynamic if needed.
    procedure_params = [
        {"name": "p_stichtag", "parameterType": {"type": "STRING"}, "parameterValue": {"value": stichtag_formatted}},
        {"name": "p_wiederanlaufWert", "parameterType": {"type": "INT64"}, "parameterValue": {"value": "0"}},
    ]

    call_wrapper_procedure = BigQueryInsertJobOperator(
        task_id="call_ausd_bp_ta_iccid_vertrag_wrapper",
        project_id=PROJECT_ID,
        configuration={
            "query": {
                "query": f"CALL `{PROJECT_ID}.{DATASET_ID}.ausd_bp_ta_iccid_vertrag_wrapper`(@p_stichtag, @p_wiederanlaufWert);",
                "useLegacySql": False,
                "parameterMode": "NAMED",
                "queryParameters": procedure_params,
            }
        },
        gcp_conn_id="google_cloud_default", # Ensure this connection is configured in Airflow
    )