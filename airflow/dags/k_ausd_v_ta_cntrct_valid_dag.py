# Legacy source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_cntrct_valid.ksh
# Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_cntrct_valid.ksh

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

with DAG(
    dag_id='k_ausd_v_ta_cntrct_valid_dag',
    start_date=days_ago(1),
    schedule_interval=None, # Define your schedule, e.g., '0 0 * * *' for daily
    catchup=False,
    tags=['bigquery', 'etl'],
    default_args=default_args,
    description='Orchestrates the BigQuery stored procedure for contract validity processing.'
) as dag:
    call_bigquery_sp = BigQueryExecuteQueryOperator(
        task_id='call_bert_k_ausd_v_ta_cntrct_valid_sp',
        gcp_conn_id='google_cloud_default', # Ensure this connection exists in Airflow
        sql="""
        CALL `project.dataset.bert_k_ausd_v_ta_cntrct_valid`(
            p_job_kennung => 'CONTRACT_VALIDITY_JOB',
            p_eintrags_nr => '{{ ds_nodash }}' -- Using Airflow macro for a dynamic entry number
        );
        """,
        use_legacy_sql=False,
        location='US', # Specify your BigQuery dataset location
    )