#
# Apache Airflow DAG to orchestrate the BigQuery Stored Procedure for
# vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_evn.ksh.
# This DAG triggers the main BigQuery orchestration stored procedure.
#
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteStoredProcedureOperator
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
    dag_id='r_ausd_bp_ta_bpr_evn_dag',
    default_args=default_args,
    description='Orchestrates r_ausd_bp_ta_bpr_evn BigQuery Stored Procedure',
    schedule_interval=None, # Define your desired schedule here, e.g., '0 0 * * *' for daily
    catchup=False,
    tags=['bigquery', 'etl', 'bert'],
) as dag:
    # Example of how to call the BigQuery Stored Procedure.
    # Parameters can be passed dynamically, e.g., from Airflow macros or XComs.
    call_bigquery_sp = BigQueryExecuteStoredProcedureOperator(
        task_id='call_sp_r_ausd_bp_ta_bpr_evn',
        project_id='project', # Replace with your GCP project ID
        dataset_id='dataset', # Replace with your BigQuery dataset ID
        procedure_id='sp_r_ausd_bp_ta_bpr_evn',
        parameters=[
            {"name": "p_stichtag_in", "parameterType": {"type": "STRING"}, "value": "{{ ds_nodash }}"}, # Example: Pass execution date as stichtag
            {"name": "p_wiederanlaufWert_in", "parameterType": {"type": "INT64"}, "value": "0"}
        ],
        gcp_conn_id='google_cloud_default', # Ensure you have a BigQuery connection configured
    )