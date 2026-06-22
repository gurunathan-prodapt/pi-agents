# Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_discount.ksh
# This Airflow DAG orchestrates the BigQuery stored procedure for discount data processing.

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

with DAG(
    dag_id='k_ausd_v_ta_discount_migration',
    default_args=default_args,
    description='Migrates k_ausd_v_ta_discount.ksh to BigQuery stored procedure.',
    start_date=days_ago(1),
    schedule_interval=None,
    tags=['isbert', 'bigquery', 'discount'],
    catchup=False,
) as dag:
    call_discount_sp = BigQueryInsertJobOperator(
        task_id='call_bigquery_stored_procedure',
        configuration={
            "query": {
                "query": "CALL `project.dataset.r_ausd_v_ta_discount`(@job_kennung, @eintrags_nr);",
                "useLegacySql": False,
                "queryParameters": [
                    {
                        "name": "job_kennung",
                        "parameterType": {"type": "STRING"},
                        "parameterValue": {"value": "BERT_DISCOUNT_JOB_ID"} # Placeholder
                    },
                    {
                        "name": "eintrags_nr",
                        "parameterType": {"type": "STRING"},
                        "parameterValue": {"value": "12345"} # Placeholder
                    }
                ]
            }
        },
        location='US', # Specify your BigQuery dataset location
    )