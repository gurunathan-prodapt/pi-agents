# Apache Airflow DAG for project.job_control.r_ausd_vertrag_control
# Replaces KornShell script: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_cntrct_crs.ksh
# Legacy Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_cntrct_crs.ksh

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteStoredProcedureOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

with DAG(
    dag_id='r_ausd_vertrag_control_dag',
    default_args=default_args,
    description='DAG to orchestrate BigQuery stored procedure for contract data processing.',
    schedule_interval=None, # Define your schedule here, e.g., '0 0 * * *' for daily
    catchup=False,
    tags=['bigquery', 'etl'],
) as dag:
    call_main_sp = BigQueryExecuteStoredProcedureOperator(
        task_id='call_r_ausd_vertrag_control_sp',
        project_id='project', # Replace with your GCP project ID
        dataset_id='job_control',
        procedure_id='r_ausd_vertrag_control',
        gcp_conn_id='google_cloud_default', # Ensure this connection is configured in Airflow
        # Pass parameters as a dictionary. Example values; adjust as needed.
        parameters={
            "p_JobKennung": "BERT_TA_CNTRCT_CRS_JOB",
            "p_EintragsNr": "1"
        }
    )