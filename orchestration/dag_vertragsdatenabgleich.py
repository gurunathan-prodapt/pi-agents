# Apache Airflow DAG for vertragsdatenabgleich
# Orchestrates the BigQuery Stored Procedure that replaces
# vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_action_assoc.ksh
# Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_action_assoc.ksh

from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryStoredProcedureOperator

# Define your project and dataset IDs
BIGQUERY_PROJECT_ID = 'your_project'
BIGQUERY_DATASET_ID = 'your_dataset'

with DAG(
    dag_id='vertragsdatenabgleich_workflow',
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=['bigquery', 'etl', 'vertragsdatenabgleich'],
    doc_md="""
    ### Vertragsdatenabgleich Workflow
    This DAG orchestrates the BigQuery Stored Procedure `vertragsdatenabgleich`
    which acts as a wrapper for the contract data reconciliation process.
    It replaces the legacy KornShell script `r_ausd_v_ta_action_assoc.ksh`.
    """
) as dag:
    call_vertragsdatenabgleich_sp = BigQueryStoredProcedureOperator(
        task_id="call_vertragsdatenabgleich_sp",
        project_id=BIGQUERY_PROJECT_ID,
        dataset_id=BIGQUERY_DATASET_ID,
        procedure_id="vertragsdatenabgleich",
        gcp_conn_id="google_cloud_default", # Ensure this connection exists in Airflow
        parameters={
            "p_jobkennung": "AIRFLOW_TRIGGERED_JOB__{{ ds_nodash }}",
            "p_run_date": "{{ ds }}", # Airflow's execution date as YYYY-MM-DD
            "p_enable_help": False
        },
    )

    # You can add more tasks here, e.g., for data validation, notification, etc.
    # call_vertragsdatenabgleich_sp >> some_next_task