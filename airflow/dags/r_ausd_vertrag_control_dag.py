# Apache Airflow DAG to orchestrate the BigQuery Stored Procedure.
# This DAG invokes the `r_ausd_vertrag_control` procedure.
# Replaces job vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_apn_ve.ksh orchestration.
from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteStoredProcedureOperator

with DAG(
    dag_id="r_ausd_vertrag_control_dag",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    schedule=None, # Define your desired schedule here, e.g., "@daily", "0 2 * * *"
    tags=["bigquery", "etl", "isrpt", "isbert"],
    description="Orchestrates the k_ausd_v_ta_apn_ve.ksh migration to BigQuery stored procedures.",
) as dag:
    call_main_procedure = BigQueryExecuteStoredProcedureOperator(
        task_id="call_r_ausd_vertrag_control",
        project_id="project", # Replace with your GCP project ID
        dataset_id="dataset", # Replace with your BigQuery dataset ID
        procedure_id="r_ausd_vertrag_control",
        parameters={
            "p_JobKennung": "EXAMPLE_JOB_KENNUNG", # Replace with dynamic value or Airflow variable
            "p_EintragsNr": "EXAMPLE_EINTRAGS_NR", # Replace with dynamic value or Airflow variable
        },
    )