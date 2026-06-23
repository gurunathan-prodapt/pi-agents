"""
Airflow DAG to invoke the BigQuery Stored Procedure for k_ausd_v_ta_c_bfc.ksh migration.

Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_c_bfc.ksh
"""

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
    dag_id='k_ausd_v_ta_c_bfc_bigquery_migration',
    default_args=default_args,
    description='Invokes BigQuery Stored Procedure for k_ausd_v_ta_c_bfc.ksh migration',
    schedule_interval=None, # Define your schedule here, e.g., '0 0 * * *' for daily
    catchup=False,
    tags=['bigquery', 'migration'],
) as dag:
    call_bigquery_procedure = BigQueryExecuteStoredProcedureOperator(
        task_id='call_r_ausd_ta_c_bfc_procedure',
        project_id='your_project',  # Replace with your GCP Project ID
        dataset_id='your_dataset',  # Replace with your BigQuery Dataset ID
        procedure_id='r_ausd_ta_c_bfc',
        parameters=[
            {"name": "p_job_kennung", "argument_type": "STRING", "value": "DEFAULT_JOB_KENNUNG"}, # Replace with actual job kennung
            {"name": "p_eintrags_nr", "argument_type": "STRING", "value": "DEFAULT_EINTRAGS_NR"}  # Replace with actual entry number
        ],
        gcp_conn_id='google_cloud_default', # Ensure you have a BigQuery connection configured
    )