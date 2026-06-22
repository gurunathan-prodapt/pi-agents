# Header: Airflow DAG for BERT_V_TA_DISC_ZUSGF
# Legacy Source: DW.BERT_AUSD_V_TA_DISC_ZUSGF.xml (UC4 Job)
# Job: BERT_V_TA_DISC_ZUSGF

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
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
    dag_id='dw_bert_ausd_v_ta_disc_zusgf',
    default_args=default_args,
    description='Orchestrates the concatenation of discount descriptions in BigQuery',
    schedule_interval='@daily', # Example schedule, adjust as per UC4
    catchup=False,
    tags=['bert', 'dwh', 'bigquery'],
) as dag:
    start_job = BigQueryExecuteQueryOperator(
        task_id='run_bert_v_ta_disc_zusgf_sp',
        gcp_conn_id='google_cloud_default', # Ensure this connection is configured
        sql="""
        CALL `bert_dwh.r_ausd_v_ta_disc_zusgf`(
            job_kennung_param => 'BERT_V_TA_DISC_ZUSGF',
            eintrags_nr_param => NULL -- Let the SP generate a new entry number
        );
        """,
        use_legacy_sql=False,
        location='US', # Specify your BigQuery dataset location
    )