"""
Example Airflow DAG demonstrating the usage of the BigQuery SQL executor utility.

This DAG replaces the legacy h_alis_sqlplus.ksh script's orchestration role
by showcasing how to execute BigQuery SQL queries using the
`execute_bigquery_script` function.
"""
# Replaces legacy source vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_sqlplus.ksh
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from dags.utils.bigquery_sql_executor import execute_bigquery_script

with DAG(
    dag_id='example_bigquery_sql_executor_dag',
    start_date=datetime(2023, 1, 1),
    schedule_interval=None, # This is an example, typically set to a cron expression or None for manual runs
    catchup=False,
    tags=['utility', 'example', 'bigquery'],
    description='An example DAG using the custom BigQuery SQL executor function.',
) as dag:
    # Example 1: Execute inline BigQuery SQL
    run_inline_sql_task = PythonOperator(
        task_id='run_inline_sql_example',
        python_callable=execute_bigquery_script,
        op_kwargs={
            'entry_number': 'INLINE_SQL_001',
            'script_ref': """
                SELECT 'Hello from Airflow' AS message,
                       CURRENT_DATETIME() AS current_time;
            """,
            'sql_parameters': {}, # No parameters for this simple inline query
        },
    )

    # Example 2: Execute BigQuery SQL from a GCS file
    # NOTE: For this to work, ensure 'gs://my-gcs-bucket/sql/daily_report.sql'
    #       exists and contains valid BigQuery SQL.
    #       Also, the service account running Airflow must have permissions to read from GCS.
    run_gcs_sql_task = PythonOperator(
        task_id='run_gcs_sql_example',
        python_callable=execute_bigquery_script,
        op_kwargs={
            'entry_number': 'GCS_SQL_002',
            # Replace with an actual GCS path to your BigQuery SQL file
            'script_ref': 'gs://my-gcs-bucket/sql/daily_report.sql',
            'sql_parameters': {
                'report_date': '{{ ds }}', # Airflow macro for execution date
                'region': 'US'
            },
        },
    )

    # Example 3: Execute another inline BigQuery SQL with parameters
    run_parameterized_sql_task = PythonOperator(
        task_id='run_parameterized_sql_example',
        python_callable=execute_bigquery_script,
        op_kwargs={
            'entry_number': 'PARAM_SQL_003',
            'script_ref': """
                SELECT 'Report for {{ params.report_date }} in {{ params.region }}' AS report_summary;
            """,
            'sql_parameters': {
                'report_date': '{{ ds_nodash }}',
                'region': 'EU'
            },
        },
    )

    # Define task dependencies (optional, but good practice for clarity)
    run_inline_sql_task >> run_gcs_sql_task >> run_parameterized_sql_task