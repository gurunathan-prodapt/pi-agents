# Migrated from legacy source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_p_basisprod.ksh
# Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_p_basisprod.ksh

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.utils.dates import days_ago
from datetime import datetime

# Define your BigQuery project and dataset
BIGQUERY_PROJECT = 'your-gcp-project-id'
BIGQUERY_DATASET = 'your_bigquery_dataset'

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

with DAG(
    dag_id='k_ausd_bp_ta_p_basisprod_job',
    default_args=default_args,
    description='Orchestrates BigQuery stored procedure for k_ausd_bp_ta_p_basisprod',
    schedule_interval=None, # Define your schedule, e.g., '@daily', timedelta(days=1)
    tags=['bigquery', 'etl'],
    catchup=False,
) as dag:
    run_bigquery_sp = BigQueryInsertJobOperator(
        task_id='run_bigquery_stored_procedure',
        project_id=BIGQUERY_PROJECT,
        configuration={
            "query": {
                "query": f"""
                    CALL `{BIGQUERY_PROJECT}.{BIGQUERY_DATASET}.sp_k_ausd_bp_ta_p_basisprod`(
                        p_JobKennung => 'DEFAULT_JOB_KENNUNG', -- Replace with dynamic value or Airflow variable
                        p_EintragsNr => 'DEFAULT_EINTRAGS_NR', -- Replace with dynamic value or Airflow variable
                        p_Stichtag => '{datetime.now().strftime('%d%m%Y')}', -- Example: current date as DDMMYYYY
                        p_wiederanlaufWert => 'DEFAULT_WIEDERANLAUF_WERT' -- Replace with dynamic value or Airflow variable
                    );
                """,
                "useLegacySql": False,
            }
        },
    )