# Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_cntrct_crs3.ksh
# Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_cntrct_crs3.ksh
# Description: Airflow DAG to orchestrate the execution of the BigQuery Stored Procedure.

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
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
    dag_id='r_ausd_v_ta_cntrct_crs3_dag',
    default_args=default_args,
    description='Airflow DAG for r_ausd_v_ta_cntrct_crs3 BigQuery migration',
    schedule_interval=None, # Define your schedule here, e.g., '0 5 * * *' for daily at 5 AM
    catchup=False,
    tags=['bigquery', 'data_sync'],
) as dag:
    call_main_sp = BigQueryInsertJobOperator(
        task_id='call_sp_vertragsdatenabgleich',
        configuration={
            "query": {
                # Replace YOUR_PROJECT_ID and YOUR_DATASET_ID with actual values
                "query": "CALL `YOUR_PROJECT_ID.YOUR_DATASET_ID.sp_vertragsdatenabgleich`(NULL, NULL);",
                "useLegacySql": False,
            }
        },
        location='us-central1', # Specify your BigQuery dataset location (e.g., 'us-central1', 'eu')
    )