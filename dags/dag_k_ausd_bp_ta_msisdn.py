# Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_msisdn.ksh
# Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_msisdn.ksh

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryOperator
from airflow.utils.dates import days_ago
import datetime
import os
import sys

# Add the dags directory to the path to import utils
# In a Composer environment, this might be handled by DAG deployment,
# but for local testing, ensure the path is correct.
# Assuming 'dags' is the root, and 'utils.py' is in 'dags/utils.py'
# sys.path.append(os.path.dirname(os.path.abspath(__file__)))
# from utils import validate_parameters_func, derive_dates_func, capture_record_count_func, custom_error_handler
from dags import utils # This assumes utils.py is directly under the dags folder or properly packaged

# Define DAG arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
    'start_date': days_ago(1), # Set a start date in the past for immediate scheduling
    'project_id': os.environ.get('GCP_PROJECT_ID', 'your-gcp-project-id'), # Replace with your GCP project ID
    # 'on_failure_callback': utils.custom_error_handler, # Example of custom error handling
}

# Define the DAG
with DAG(
    dag_id='dag_k_ausd_bp_ta_msisdn',
    default_args=default_args,
    description='Airflow DAG for k_ausd_bp_ta_msisdn.ksh migration',
    schedule_interval=None, # Set to a schedule like '0 0 * * *' for daily, or None for manual/external trigger
    catchup=False,
    tags=['bigquery', 'ksh_migration', 'isbert'],
) as dag:

    # Task 1: Validate Parameters
    # Replaces the ksh `getopts` and `pruefeParameterGesetzt` logic.
    # Parameters are expected via dag_run.conf (manual trigger) or Airflow variables/macros
    validate_parameters = PythonOperator(
        task_id='validate_parameters',
        python_callable=utils.validate_parameters_func,
        provide_context=True,
    )

    # Task 2: Derive Dates
    # Replaces `gestern.ksh` functionality.
    derive_dates = PythonOperator(
        task_id='derive_dates',
        python_callable=utils.derive_dates_func,
        provide_context=True,
    )

    # Task 3: Execute BigQuery SQL
    # Replaces the `starteSQLSkript` function executing `d_ausd_bp_ta_msisdn.sql`.
    # The SQL content is assumed to be in sql/d_ausd_bp_ta_msisdn_bq.sql
    # and converted to BigQuery Standard SQL.
    # Note: Ensure the SQL file path is correct relative to the DAGs folder
    # or accessible via GCS.
    bigquery_sql_path = 'sql/d_ausd_bp_ta_msisdn_bq.sql'
    # For a real scenario, you might read this from GCS:
    # with open(f'{os.environ["AIRFLOW_HOME"]}/dags/{bigquery_sql_path}', 'r') as f:
    #     bigquery_sql_query = f.read()

    # Placeholder SQL content (ideally loaded from file or GCS)
    # Using a simple SELECT for demonstration; actual SQL will be in the file
    # This example assumes the SQL is simple enough to be inlined or fetched
    # and can use Jinja templating for parameters.
    # For larger SQL, load from a file as shown commented above.
    with open(f'{os.path.dirname(os.path.abspath(__file__))}/../sql/d_ausd_bp_ta_msisdn_bq.sql', 'r') as f:
        bigquery_sql_query = f.read()

    execute_bigquery_sql = BigQueryOperator(
        task_id='execute_bigquery_sql',
        sql=bigquery_sql_query,
        use_legacy_sql=False,
        destination_dataset_table=None, # Set if the SQL writes to a specific table directly, otherwise the SQL handles INSERT/MERGE
        write_disposition='WRITE_APPEND', # or WRITE_TRUNCATE, if applicable, based on SQL logic
        create_disposition='CREATE_IF_NEEDED',
        params={
            'reference_date_str': "{{ ti.xcom_pull(task_ids='validate_parameters', key='reference_date_str') }}",
            'today_date': "{{ ti.xcom_pull(task_ids='derive_dates', key='today_date') }}",
            'yesterday_date': "{{ ti.xcom_pull(task_ids='derive_dates', key='yesterday_date') }}",
            # Add other parameters required by the SQL script
        },
        gcp_conn_id='google_cloud_default', # Ensure this BigQuery connection is configured
    )

    # Task 4: Capture Record Count
    # Replaces reading `$tmpFile` for record counts.
    capture_record_count = PythonOperator(
        task_id='capture_record_count',
        python_callable=utils.capture_record_count_func,
        provide_context=True,
    )

    # Define task dependencies
    validate_parameters >> derive_dates >> execute_bigquery_sql >> capture_record_count