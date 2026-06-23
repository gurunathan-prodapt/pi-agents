# Airflow DAG for orchestrating BigQuery stored procedure execution
# Legacy Source: r_ausd_v_ta_inv_def.ksh
# Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_inv_def.ksh

from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteStoredProcedureOperator

PROJECT_ID = "your-gcp-project-id"  # Replace with your GCP project ID
DATASET_ID = "your_dataset_id"     # Replace with your BigQuery dataset ID

with DAG(
    dag_id="r_ausd_v_ta_inv_def_dag",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    schedule=None,  # Set to a schedule string like "@daily" or "0 0 * * *" for regular execution
    tags=["bigquery", "etl", "ta_inv_def"],
    description="Orchestrates the execution of sp_r_ausd_v_ta_inv_def BigQuery stored procedure.",
) as dag:
    call_main_sp = BigQueryExecuteStoredProcedureOperator(
        task_id="call_sp_r_ausd_v_ta_inv_def",
        project_id=PROJECT_ID,
        dataset_id=DATASET_ID,
        procedure_id="sp_r_ausd_v_ta_inv_def",
        gcp_conn_id="google_cloud_default",  # Ensure this connection is configured in Airflow
        parameters=[
            {"name": "p_h", "parameterType": {"type": "BOOL"}, "value": "false"}, # Set to true to see help message
            {"name": "p_s", "parameterType": {"type": "STRING"}, "value": "default_s_value"}, # Replace with actual logic for -s
            {"name": "p_l", "parameterType": {"type": "STRING"}, "value": "default_l_value"}, # Replace with actual logic for -l
        ],
    )