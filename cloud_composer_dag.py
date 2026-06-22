# Legacy source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_msisdn.ksh
# Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_msisdn.ksh

import os
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteStoredProcedureOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

# --- Configuration ---
# Get GCP project ID and BigQuery dataset ID from environment variables
# or set default values if not available.
PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "my-gcp-project")
DATASET_ID = os.environ.get("BIGQUERY_DATASET_ID", "isbert_dataset")
BQ_SP_NAME = "r_ausd_bp_ta_msisdn"

# --- Default Arguments for the DAG ---
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

# --- DAG Definition ---
with DAG(
    dag_id='r_ausd_bp_ta_msisdn_dag',
    default_args=default_args,
    description='Orchestrates the r_ausd_bp_ta_msisdn BigQuery Stored Procedure for FOS process.',
    start_date=days_ago(1), # This date refers to the start of the DAG's operational history.
                            # For backfilling, you might adjust it.
    schedule_interval='0 1 * * *', # Example: run daily at 01:00 AM UTC
    catchup=False, # Set to True if you want to run for past missed schedules.
    tags=['isbert', 'bigquery', 'dwh', 'etl'],
) as dag:
    # --- Task: Execute BigQuery Stored Procedure ---
    # This task calls the main BigQuery Stored Procedure.
    # Parameters are passed dynamically from Airflow, allowing for flexible execution.
    execute_bq_sp = BigQueryExecuteStoredProcedureOperator(
        task_id='execute_r_ausd_bp_ta_msisdn_sp',
        project_id=PROJECT_ID,
        dataset_id=DATASET_ID,
        procedure_id=BQ_SP_NAME,
        # Parameters for the Stored Procedure.
        # Airflow macros (e.g., {{ ds_nodash }}) can be used for dynamic values.
        # The SP handles defaulting for None or empty string values.
        parameters=[
            {
                "name": "p_stichtag_str",
                "parameterType": {"type": "STRING"},
                # Pass execution date as 'DDMMYYYY'. Use data_interval_end for scheduled runs.
                # For manual triggers, provide a specific value or leave blank for SP default (CURRENT_DATE()).
                "value": "{{ data_interval_end.strftime('%d%m%Y') }}"
            },
            {
                "name": "p_wiederanlaufwert_param",
                "parameterType": {"type": "INT64"},
                # Example: Pass a fixed value or make it dynamic. None lets SP default to 0.
                "value": "0"
            },
            {
                "name": "p_job_kennung",
                "parameterType": {"type": "STRING"},
                "value": "FOS_BP_TA_MSISDN" # Example job identifier for audit
            },
            {
                "name": "p_eintrags_nr",
                "parameterType": {"type": "STRING"},
                "value": None # This parameter is optional and can be left as None
            }
        ]
    )

    # Currently, there's only one task. If more tasks were added (e.g., data quality checks),
    # their dependencies would be defined here.
    # execute_bq_sp