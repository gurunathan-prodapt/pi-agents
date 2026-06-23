# Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bpr_beschr.ksh
# Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bpr_beschr.ksh
#
# Migrated Cloud Composer (Airflow) DAG to orchestrate the BigQuery Stored Procedure.

from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteStoredProcedureOperator
from airflow.operators.dummy import DummyOperator

# Define your GCP Project ID and BigQuery Dataset ID
# It is strongly recommended to store these in Airflow Variables or environment variables.
# For demonstration purposes, placeholders are used.
GCP_PROJECT_ID = 'your-gcp-project-id' # TODO: Replace with your GCP project ID
BQ_DATASET_ID = 'your_bigquery_dataset' # TODO: Replace with your BigQuery dataset ID

with DAG(
    dag_id='k_ausd_bp_ta_bpr_beschr_dag',
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"), # Adjust start date as needed
    schedule='0 0 * * *',  # Example: daily at midnight UTC. Adjust based on legacy schedule.
    catchup=False,         # Set to True if historical runs are required
    tags=['isbert', 'bigquery', 'etl'],
    doc_md="""
    ### Airflow DAG for k_ausd_bp_ta_bpr_beschr Migration
    This DAG orchestrates the execution of the BigQuery Stored Procedure `r_ausd_bp_ta_bpr_beschr`.
    It replaces the functionality of the legacy KornShell script `k_ausd_bp_ta_bpr_beschr.ksh`.

    The `stichtag_str` parameter is derived from Airflow's `data_interval_start`,
    formatted as 'DDMMYYYY', representing the logical date for which data is processed.
    """,
    params={
        # These can be overridden at runtime or configured as Airflow Variables
        'job_kennung': 'k_ausd_bp_ta_bpr_beschr',
        'eintrags_nr': '1', # Default value; adjust as per specific job requirements
        'wiederanlauf_wert': '0' # Default value; adjust as per specific job requirements
    },
) as dag:
    start_task = DummyOperator(task_id='start_etl_process')

    # The 'stichtag_str' parameter for the BigQuery Stored Procedure.
    # It typically represents the logical date of the data being processed.
    # Here, we use `data_interval_start` (the beginning of the DAG's logical data interval)
    # and format it to 'DDMMYYYY' as expected by the stored procedure.
    # If the 'Stichtag' needs to be, for example, the day before `data_interval_start`,
    # adjust the macro accordingly (e.g., `(data_interval_start - macros.timedelta(days=1))`).
    stichtag_param_macro = "{{ data_interval_start.strftime('%d%m%Y') }}"

    call_main_bigquery_procedure = BigQueryExecuteStoredProcedureOperator(
        task_id='execute_r_ausd_bp_ta_bpr_beschr',
        project_id=GCP_PROJECT_ID,
        dataset_id=BQ_DATASET_ID,
        procedure_id='r_ausd_bp_ta_bpr_beschr',
        parameters={
            'job_kennung': '{{ params.job_kennung }}',
            'eintrags_nr': '{{ params.eintrags_nr }}',
            'stichtag_str': stichtag_param_macro,
            'wiederanlauf_wert': '{{ params.wiederanlauf_wert }}'
        },
        gcp_conn_id='google_cloud_default', # Ensure this connection ID is configured in Airflow
    )

    end_task = DummyOperator(task_id='end_etl_process')

    start_task >> call_main_bigquery_procedure >> end_task