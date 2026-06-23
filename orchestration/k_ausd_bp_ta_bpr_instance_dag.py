"""
This Airflow DAG orchestrates the execution of the BigQuery stored procedure
`r_ausd_bp_ta_bpr_instance`, which replaces the legacy KornShell script
vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bpr_instance.ksh.
"""

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

# Define BigQuery project and dataset
BIGQUERY_PROJECT_ID = 'your_project_id'
BIGQUERY_DATASET_ID = 'your_dataset_id'
MAIN_PROCEDURE_NAME = 'r_ausd_bp_ta_bpr_instance'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='k_ausd_bp_ta_bpr_instance_migration_dag',
    default_args=default_args,
    description='Executes BigQuery stored procedure for Basisprodukt data preparation.',
    start_date=days_ago(1),
    schedule_interval=timedelta(days=1), # Example: Run daily
    catchup=False,
    tags=['bigquery', 'data_preparation'],
) as dag:
    # Example of how to call the BigQuery stored procedure
    # Parameters are passed dynamically using Airflow macros or static values.
    # p_Stichtag needs to be in 'DDMMYYYY' format.
    execute_main_procedure = BigQueryExecuteQueryOperator(
        task_id='call_r_ausd_bp_ta_bpr_instance',
        sql=f"""
        CALL `{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET_ID}.{MAIN_PROCEDURE_NAME}`(
            p_JobKennung => 'DAILY_RUN',
            p_EintragsNr => '{{{{ ds_nodash }}}}', -- Example: current date as 'YYYYMMDD'
            p_Stichtag => '{{{{ ds_nodash[:2] + ds_nodash[4:6] + ds_nodash[2:4] }}}}', -- Convert 'YYYYMMDD' (ds_nodash) to 'DDMMYYYY'
            p_wiederanlaufWert => NULL
        );
        """,
        use_legacy_sql=False,
        location='US', # Specify your BigQuery location
    )