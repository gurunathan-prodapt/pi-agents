-- Example Airflow DAG to orchestrate the BigQuery parser_main stored procedure.
-- This DAG demonstrates how to trigger the BigQuery processing.
-- Migrates from vobs/dw_source/isdwh/allgemein/is/util/bin/h_alis_parser.ksh

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
    dag_id='h_alis_parser_bigquery_migration',
    default_args=default_args,
    description='Triggers BigQuery stored procedure for h_alis_parser.ksh migration',
    schedule_interval=None,
    catchup=False,
    tags=['bigquery', 'parsing', 'migration'],
) as dag:
    # Example task to execute the parser_main stored procedure
    execute_parser_sp = BigQueryExecuteStoredProcedureOperator(
        task_id='run_parser_main_sp',
        project_id='your-gcp-project-id', # Replace with your GCP project ID
        dataset_id='dataset',           # Replace with your BigQuery dataset ID
        procedure_id='parser_main',
        gcp_conn_id='google_cloud_default', # Your Airflow GCP connection ID
        parameters={
            "template_id_param": "my_template_123", # Example template ID
            "output_table_name": "project.dataset.parsed_output_table" # Output table
        }
    )

    # Further tasks could be added here, e.g., for exporting the output table
    # or triggering downstream processes.