# Airflow DAG for DW.BERT_AUSD_V_TA_P_DISCOUNT_RR
# Replaces legacy UC4 job: vobs/dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/BERT/PRODUKTION/DW.BERT_STAMMDATEN_JP/DW.BERT_P_VERTRAG_JP/DW.BERT_AUSD_V_TA_P_DISCOUNT_RR.xml
# Replaces legacy KornShell scripts:
#   vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_p_discount_rr.ksh
#   vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_p_discount_rr.ksh
# Replaces legacy Oracle SQL*Plus script:
#   vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_v_ta_p_discount_rr.sql

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': ['your_email@example.com'],
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 1, 1), # Adjust as per actual deployment date
}

with DAG(
    dag_id='dw_bert_ausd_v_ta_p_discount_rr',
    default_args=default_args,
    description='Prepares and enriches discount data with contract details in BigQuery.',
    schedule_interval='@daily', # Adjust based on original UC4 schedule
    catchup=False,
    tags=['bert', 'discount', 'bigquery'],
) as dag:
    start_task = EmptyOperator(
        task_id='start',
    )

    -- Define the project and dataset for BigQuery operations.
    -- Replace 'your-gcp-project-id' and 'your_bigquery_dataset' with your actual GCP project ID and BigQuery dataset ID.
    PROJECT_ID = 'your-gcp-project-id'
    DATASET_ID = 'your_bigquery_dataset'

    main_data_processing = BigQueryExecuteQueryOperator(
        task_id='main_data_processing',
        sql='sql/bq_d_ausd_v_ta_p_discount_rr.sql',
        use_legacy_sql=False,
        destination_dataset_table=f'{PROJECT_ID}.{DATASET_ID}.sof_ta_p_discount_rr',
        write_disposition='WRITE_TRUNCATE', # This is handled by the SQL script's TRUNCATE, but kept here for explicit task definition
        gcp_conn_id='google_cloud_default', # Ensure this Airflow connection is configured
    )

    post_processing_log = EmptyOperator(
        task_id='post_processing_log',
        # In a real scenario, this task would include logic to write to a logging table
        # or Cloud Logging, replacing the original KornShell logging.
    )

    end_task = EmptyOperator(
        task_id='end',
    )

    start_task >> main_data_processing >> post_processing_log >> end_task