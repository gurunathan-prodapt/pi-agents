# Migrated from vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_austausch.ksh
# Legacy Source: r_ausd_austausch.ksh, k_ausd_austausch.ksh, d_ausd_austausch.sql

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

# Define your project and dataset IDs
PROJECT_ID = "my_project"  # REPLACE WITH YOUR ACTUAL GOOGLE CLOUD PROJECT ID
DATASET_ID = "my_dataset"  # REPLACE WITH YOUR ACTUAL BIGQUERY DATASET ID

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='bert_austausch_daily_pipeline',
    default_args=default_args,
    description='ETL job for BERT report and Forderungsscoring contract cache snapshot.',
    schedule_interval=timedelta(days=1),  # Example: daily schedule. Adjust as per UC4 schedule.
    start_date=days_ago(1),
    catchup=False,
    tags=['bert', 'etl', 'bigquery'],
) as dag:
    # Task to call the main BigQuery Stored Procedure
    call_bert_austausch_sp = BigQueryOperator(
        task_id='call_bert_austausch_stored_procedure',
        sql=f"""
            CALL `{PROJECT_ID}.{DATASET_ID}.BERT_AUSTAUSCH_KSH`(
                p_stichtag_raw => "{{{{ ds_nodash }}}}", -- Pass execution date as Stichtag (DDMMYYYY)
                p_wiederanlauf_wert_raw => "0" -- Default restart value; can be configured as a DAG param if dynamic
            );
        """,
        use_legacy_sql=False,
        gcp_conn_id='google_cloud_default', # Ensure this connection exists in Airflow
    )

    # Note: Additional tasks for data ingestion (Step 7) would go here,
    # for example, using Dataflow, Data Transfer Service, or custom Python operators
    # to load data into the sof_ta_p_* source tables before this DAG runs.

    # The single task for this pipeline is calling the main stored procedure.
    # Any pre-processing or post-processing steps (e.g., source data ingestion,
    # notification, data quality checks) would be added as upstream/downstream tasks.