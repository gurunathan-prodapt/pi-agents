# Airflow DAG for DW.BERT_AUSD_BP_TA_APN_VERTRAG
# Orchestrates BigQuery SQL transformation.
# Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_bp_ta_apn_vertrag.sql
# Job: DW.BERT_AUSD_BP_TA_APN_VERTRAG

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryOperator
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
    dag_id='dw_bert_ausd_bp_ta_apn_vertrag',
    default_args=default_args,
    description='BigQuery transformation for APN and contract data aggregation.',
    schedule_interval=None, # Define your schedule here, e.g., '0 3 * * *' for 3 AM daily
    tags=['bigquery', 'transformation', 'dw'],
    catchup=False,
) as dag:
    run_apn_vertrag_transformation = BigQueryOperator(
        task_id='run_apn_vertrag_transformation',
        sql='sql/d_ausd_bp_ta_apn_vertrag_bq.sql',
        use_legacy_sql=False,
    )