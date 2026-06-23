#
# Cloud Composer DAG for r_ausd_bp_ta_apn_vertrag.ksh migration.
# Orchestrates the execution of the BigQuery Stored Procedure.
#
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryStartStoredProcedureOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

with DAG(
    dag_id='ausd_bp_ta_apn_vertrag_wrapper_dag',
    default_args=default_args,
    description='Triggers the BigQuery Stored Procedure for contract cache provisioning.',
    schedule_interval='@daily',  # Example: daily schedule; adjust as needed
    start_date=days_ago(1),
    catchup=False,
    tags=['bigquery', 'dwh', 'migration'],
) as dag:
    # Get the current date in 'DDMMYYYY' format for p_stichtag
    # Airflow macros are evaluated at runtime
    current_date_ddmmyyyy = "{{ ds_nodash }}" # ds_nodash gives YYYYMMDD, we need DDMMYYYY
    # For DDMMYYYY, better to use a Python callable or more explicit formatting
    # Using '{{ ds_nodash[6:8] + ds_nodash[4:6] + ds_nodash[0:4] }}' for DDMMYYYY
    stichtag_param = "{{ ds_nodash[6:8] + ds_nodash[4:6] + ds_nodash[0:4] }}"

    # Call the BigQuery Stored Procedure wrapper
    call_bq_sp = BigQueryStartStoredProcedureOperator(
        task_id='call_ausd_bp_ta_apn_vertrag_wrapper',
        project_id='your_gcp_project',
        dataset_id='your_bq_dataset',
        procedure_id='ausd_bp_ta_apn_vertrag_wrapper',
        gcp_conn_id='google_cloud_default', # Ensure this connection exists and is configured
        parameters={
            'p_stichtag': stichtag_param,
            'p_wiederanlaufWert': '0' # Default value; can be configured via Airflow variables or XComs
        },
    )

    # Define task dependencies if any (none for this single task DAG)
    # call_bq_sp