# Airflow DAG for vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bpr_apn.ksh
# This DAG orchestrates the execution of the BigQuery Stored Procedure
# project.dataset.r_ausd_bp_ta_bpr_apn, which replaces the legacy KornShell script.

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.utils.dates import days_ago
import pendulum

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

# Define the DAG
with DAG(
    dag_id='r_ausd_bp_ta_bpr_apn_dag',
    default_args=default_args,
    description='Orchestrates BigQuery Stored Procedure for k_ausd_bp_ta_bpr_apn.ksh migration',
    schedule_interval=None,  # Set your desired schedule interval (e.g., '@daily', '0 0 * * *')
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"), # Adjust start date as appropriate
    catchup=False,
    tags=['bigquery', 'migration'],
) as dag:
    # Task to call the main BigQuery Stored Procedure
    call_main_stored_procedure = BigQueryInsertJobOperator(
        task_id='call_r_ausd_bp_ta_bpr_apn_sp',
        configuration={
            "query": {
                "query": """
                    CALL `project.dataset.r_ausd_bp_ta_bpr_apn`(
                        p_JobKennung => 'r_ausd_bp_ta_bpr_apn_dag', -- Example: DAG ID or a specific job identifier
                        p_EintragsNr => '12345',                     -- Placeholder: Replace with actual value or dynamic variable
                        p_Stichtag => '{{ ds_nodash }}',             -- Pass execution date as YYYYMMDD
                        p_wiederanlaufWert => '0'                    -- Default restart value
                    );
                """,
                "useLegacySql": False,  # Important for BigQuery Standard SQL and Stored Procedures
                "queryParameters": []   # Parameters are embedded directly in the query string via named args
            }
        },
        gcp_conn_id='google_cloud_default',  # Ensure this BigQuery connection is configured in Airflow
    )