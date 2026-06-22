# Migrated from vobs/dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/BERT/PRODUKTION/DW.BERT_STAMMDATEN_JP/DW.BERT_P_BASISPRODUKT_JP/DW.BERT_AUSD_BP_TA_BCP_MSISDN.xml
# Job: DW.BERT_AUSD_BP_TA_BCP_MSISDN

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.utils.dates import days_ago

from datetime import timedelta

# Import utility functions from the local module
from bert_bcp_msisdn_utils import prepare_parameters_task, log_job_status_task

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the Airflow DAG
with DAG(
    dag_id='dw_bert_ausd_bp_ta_bcp_msisdn',
    default_args=default_args,
    description='Migrated job for BERT_AUSD_BP_TA_BCP_MSISDN - prepares instantiated basic products with MSISDN data.',
    start_date=days_ago(1),
    schedule_interval=timedelta(days=1), # Example schedule: daily. Adjust as per UC4.
    catchup=False,
    tags=['bert', 'msisdn', 'bigquery'],
) as dag:
    
    start_task = PythonOperator(
        task_id='start_job',
        python_callable=lambda: print("Starting DW.BERT_AUSD_BP_TA_BCP_MSISDN migration job..."),
    )

    prepare_parameters = PythonOperator(
        task_id='prepare_parameters',
        python_callable=prepare_parameters_task,
        provide_context=True, # Allows access to dag_run.conf and other Airflow context
    )

    execute_bq_transformation = BigQueryExecuteQueryOperator(
        task_id='execute_bq_transformation',
        sql='d_ausd_bp_ta_bcp_msisdn_bq.sql', # Refers to the SQL file in the DAGs folder or GCS
        use_legacy_sql=False,
        destination_dataset_table='sof.ta_bcp_msisdn', # Set to get metadata about the job
        write_disposition='WRITE_TRUNCATE', # As the SQL does a TRUNCATE and then INSERT
        create_disposition='CREATE_IF_NEEDED',
        gcp_conn_id='google_cloud_default', # Ensure this connection exists
        location='EU', # Specify your BigQuery dataset location
    )

    log_job_status = PythonOperator(
        task_id='log_job_status',
        python_callable=log_job_status_task,
        provide_context=True,
    )

    # Define task dependencies
    start_task >> prepare_parameters >> execute_bq_transformation >> log_job_status