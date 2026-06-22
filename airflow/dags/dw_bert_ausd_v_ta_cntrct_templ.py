#
# Airflow DAG for DW.BERT_AUSD_V_TA_CNTRCT_TEMPL
# Legacy Source: DW.BERT_AUSD_V_TA_CNTRCT_TEMPL.xml (UC4), r_ausd_v_ta_cntrct_templ.ksh, k_ausd_v_ta_cntrct_templ.ksh (KornShell)
#

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
    # 'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='dw_bert_ausd_v_ta_cntrct_templ',
    default_args=default_args,
    description='Mirrors Carmen contract templates to BigQuery',
    schedule_interval=None,  # Or a specific schedule, e.g., '0 3 * * *' for daily at 3 AM UTC
    tags=['bigquery', 'etl', 'contract_templates'],
    catchup=False,
) as dag:
    execute_bigquery_sp = BigQueryExecuteQueryOperator(
        task_id='execute_contract_template_sp',
        sql="""CALL `your_project.your_dataset.r_ausd_v_ta_cntrct_templ`(
            p_JobKennung => 'BERT_AUSD_V_TA_CNTRCT_TEMPL',
            p_EintragsNr => 1 -- Example entry number, adjust as needed
        )""",
        use_legacy_sql=False,
        gcp_conn_id='google_cloud_default', # Ensure this connection ID is configured in Airflow
    )

    # Further tasks can be added here, e.g., data quality checks, notifications, etc.
    # For now, the DAG directly calls the BigQuery Stored Procedure which handles the core logic.