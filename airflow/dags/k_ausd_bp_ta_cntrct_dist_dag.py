# Apache Airflow DAG for k_ausd_bp_ta_cntrct_dist.ksh migration
# Legacy source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_cntrct_dist.ksh
# Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_cntrct_dist.ksh

from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.operators.dummy import DummyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteStoredProcedureOperator

PROJECT_ID = "your_project_id"  # Replace with your GCP project ID
DATASET_ID = "your_dataset_id"  # Replace with your BigQuery dataset ID
BIGQUERY_CONN_ID = "google_cloud_default" # Or your specific BigQuery connection ID

with DAG(
    dag_id="k_ausd_bp_ta_cntrct_dist_dag",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule=None,  # Define your schedule here, e.g., "@daily"
    catchup=False,
    tags=["bigquery", "orchestration"],
    doc_md="""
    ### k_ausd_bp_ta_cntrct_dist_dag
    This DAG orchestrates the execution of the `r_ausd_bp_ta_cntrct_dist` BigQuery stored procedure,
    migrated from the legacy KornShell script `k_ausd_bp_ta_cntrct_dist.ksh`.
    It handles parameter passing, execution, and basic error handling.
    """,
) as dag:
    start_task = DummyOperator(task_id="start")

    # Define parameters for the BigQuery stored procedure
    # These could come from Airflow Variables, XComs, or default values
    # For demonstration, using static values. In a real scenario, you'd make them dynamic.
    job_kennung = "BP_TA_CNTRCT_DIST_JOB"
    entry_number = "001"
    # For 'Stichtag', use a dynamic date, e.g., execution_date formatted as DDMMYYYY
    stichtag_raw = "{{ ds_nodash }}" # This will format the execution date as YYYYMMDD, adjust if DDMMYYYY is strict
    # To strictly get DDMMYYYY:
    # stichtag_raw = "{{ execution_date.strftime('%d%m%Y') }}"
    wiederanlauf_wert_raw = "0"

    call_orchestration_sp = BigQueryExecuteStoredProcedureOperator(
        task_id="call_r_ausd_bp_ta_cntrct_dist",
        project_id=PROJECT_ID,
        dataset_id=DATASET_ID,
        procedure_id="r_ausd_bp_ta_cntrct_dist",
        gcp_conn_id=BIGQUERY_CONN_ID,
        parameters=[
            {"name": "p_JobKennung", "parameterType": {"type": "STRING"}, "value": job_kennung},
            {"name": "p_EintragsNr", "parameterType": {"type": "STRING"}, "value": entry_number},
            {"name": "p_Stichtag_raw", "parameterType": {"type": "STRING"}, "value": stichtag_raw},
            {"name": "p_wiederanlaufWert_raw", "parameterType": {"type": "STRING"}, "value": wiederanlauf_wert_raw},
        ],
    )

    end_task = DummyOperator(task_id="end")

    start_task >> call_orchestration_sp >> end_task