# Airflow DAG for DW.BERT_AUSD_BP_TA_BCP_MSISDN
# Replaces: vobs/dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/BERT/PRODUKTION/DW.BERT_STAMMDATEN_JP/DW.BERT_P_BASISPRODUKT_JP/DW.BERT_AUSD_BP_TA_BCP_MSISDN.xml
# Job: DW.BERT_AUSD_BP_TA_BCP_MSISDN
# Purpose: Orchestrates the BigQuery stored procedures to provision selected basic products for BERT.

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteStoredProcedureOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
import pendulum

# --- Airflow Variables Configuration ---
# These variables must be set in your Airflow environment for the DAG to run correctly.
# 1. BIGQUERY_PROJECT_ID: Your GCP project ID where BigQuery resources reside (e.g., 'your-gcp-project')
# 2. BIGQUERY_DATASET_ID: Your BigQuery dataset ID where the tables and stored procedures are located (e.g., 'your_dataset')

# Retrieve BigQuery project and dataset IDs from Airflow Variables
# Provide default values for local testing or if variables are not yet set
BIGQUERY_PROJECT_ID = Variable.get("BIGQUERY_PROJECT_ID", "your_gcp_project_id")
BIGQUERY_DATASET_ID = Variable.get("BIGQUERY_DATASET_ID", "your_bigquery_dataset_id")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    # 'retry_delay': timedelta(minutes=5), # Uncomment and set if specific retry delay is needed
}

with DAG(
    dag_id='dw_bert_ausd_bp_ta_bcp_msisdn',
    default_args=default_args,
    description='Migrated DW.BERT_AUSD_BP_TA_BCP_MSISDN job to BigQuery and Airflow',
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"), # Set a clear, non-dynamic start date
    # Schedule: The original UC4 job's schedule is not specified, defaulting to daily.
    # Adjust as per actual UC4 schedule.
    schedule_interval='@daily',
    catchup=False, # Set to True if you want to run for past missed schedules after deployment
    tags=['bigquery', 'etl', 'bert', 'msisdn'],
) as dag:
    # Task to call the main BigQuery Stored Procedure `sp_r_ausd_bp_ta_bcp_msisdn`.
    # This procedure is the entry point for the BigQuery side of the migration,
    # encapsulating the orchestration and logic previously handled by r_ausd_bp_ta_bcp_msisdn.ksh.
    #
    # Parameters for the stored procedure can be dynamically set:
    # - `p_stichtag_str_in`: The cutoff date.
    #   Defaults to `ds_nodash` (Airflow's execution date in YYYYMMDD format) if not provided
    #   via DAG run configuration. The stored procedure itself will use CURRENT_DATE()
    #   if `None` or an empty string is passed.
    # - `p_wiederanlaufWert_in`: The restart value.
    #   Defaults to 0 if not provided via DAG run configuration. The stored procedure
    #   will also default to 0 if `None` is passed.
    call_main_bert_sp = BigQueryExecuteStoredProcedureOperator(
        task_id='call_sp_r_ausd_bp_ta_bcp_msisdn',
        project_id=BIGQUERY_PROJECT_ID,
        dataset_id=BIGQUERY_DATASET_ID,
        procedure_id='sp_r_ausd_bp_ta_bcp_msisdn',
        parameters=[
            {
                "name": "p_stichtag_str_in",
                "parameterType": {"type": "STRING"},
                # Use value from DAG run config 'stichtag' if available, otherwise Airflow's execution date (YYYYMMDD)
                "value": "{{ dag_run.conf.get('stichtag') or ds_nodash }}"
            },
            {
                "name": "p_wiederanlaufWert_in",
                "parameterType": {"type": "INT64"},
                # Use value from DAG run config 'wiederanlaufWert' if available, otherwise default to 0
                "value": "{{ dag_run.conf.get('wiederanlaufWert') or 0 }}"
            }
        ],
        gcp_conn_id='google_cloud_default', # Ensure this BigQuery connection is configured in Airflow
    )