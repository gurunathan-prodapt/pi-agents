# Airflow DAG for job vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bpr_apn.ksh
# This DAG orchestrates the execution of the BigQuery Stored Procedure.

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteStoredProcedureOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

# Define GCP project and dataset
GCP_PROJECT_ID = 'your_gcp_project'  # Replace with your actual GCP Project ID
BIGQUERY_DATASET = 'your_bigquery_dataset'  # Replace with your actual BigQuery Dataset

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='k_ausd_bp_ta_bpr_apn_workflow',
    default_args=default_args,
    description='Orchestrates BigQuery Stored Procedure for Basisprodukt processing',
    schedule_interval='@daily',  # Example schedule: daily. Adjust as needed.
    start_date=days_ago(1),
    catchup=False,
    tags=['bigquery', 'etl', 'basisprodukt'],
) as dag:
    # Define parameters for the BigQuery Stored Procedure
    # These can be dynamic (e.g., from Airflow variables, XComs, or macros)
    # For demonstration, we use static values and Airflow macros for date.
    job_kennung = 'BP_APN_DAILY'
    eintrags_nr = '101'
    # Stichtag in DDMMYYYY format. Use Airflow macro to get yesterday's date.
    stichtag = "{{ yesterday_ds_nodash }}" # e.g., '31122023' for Dec 31, 2023
    wiederanlauf_wert = None # Or set dynamically, e.g., 'Y' if restart logic applies

    execute_bigquery_sp = BigQueryExecuteStoredProcedureOperator(
        task_id='execute_basisprodukt_sp',
        project_id=GCP_PROJECT_ID,
        dataset_id=BIGQUERY_DATASET,
        procedure_id='r_ausd_bp_ta_bpr_apn',
        parameters={
            'p_jobkennung': job_kennung,
            'p_eintragsnr': eintrags_nr,
            'p_stichtag': stichtag,
            'p_wiederanlaufwert': wiederanlauf_wert
        },
        gcp_conn_id='google_cloud_default' # Ensure this connection is configured in Airflow
    )

    # You can add further tasks for monitoring, data quality checks,
    # downstream processes, etc. here.
    # For example:
    # data_quality_check = BigQueryOperator(task_id='data_quality_check', ...)
    # send_notification = EmailOperator(task_id='send_success_notification', ...)

    # Define task dependencies
    # execute_bigquery_sp >> data_quality_check >> send_notification