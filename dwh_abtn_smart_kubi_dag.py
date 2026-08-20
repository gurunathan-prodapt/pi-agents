import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.models import Variable

# Retrieve global environment variables
GCP_PROJECT = Variable.get("GCP_PROJECT", default_var=os.environ.get("GCP_PROJECT"))
GCP_REGION = Variable.get("GCP_REGION", default_var=os.environ.get("GCP_REGION", "europe-west3"))

# Define default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    'dwh_abtn_smart_kubi_dag',
    default_args=default_args,
    description='Orchestration DAG for DW.DWH_ABTN_SMART_KUBI',
    schedule_interval='0 2 1 * *',  # Example schedule: monthly on the 1st at 02:00
    catchup=False,
    template_searchpath=['/home/airflow/gcs/dags/sql', '/opt/airflow/dags/sql', os.path.dirname(__file__)],
) as dag:

    # Task to execute the migrated BigQuery SQL script
    # The reviewer feedback requires:
    # 1. Pass query parameters '@param_monats_id' and '@param_eintrags_nr'
    # 2. Provide the actual SQL string to the BigQuery operator (not a gcs:// URI)
    # We use Airflow's template search path to load 'd_abtn_x_smart_kubi.sql' as a string.
    execute_sql = BigQueryInsertJobOperator(
        task_id='execute_d_abtn_x_smart_kubi',
        configuration={
            "query": {
                "query": "{% include 'd_abtn_x_smart_kubi.sql' %}",
                "useLegacySql": False,
                "parameterMode": "NAMED",
                "queryParameters": [
                    {
                        "name": "param_monats_id",
                        "parameterType": {"type": "STRING"},
                        "parameterValue": {
                            # Calculate MONATSID: previous month in YYYYMM format
                            "value": "{{ (execution_date - macros.timedelta(days=1)).strftime('%Y%m') }}"
                        }
                    },
                    {
                        "name": "param_eintrags_nr",
                        "parameterType": {"type": "INT64"},
                        "parameterValue": {
                            # Use Airflow's run_id or a hash of it as the entry number
                            "value": "{{ run_id | hash | string | truncate(9, True, '') | int if run_id else 1001 }}"
                        }
                    }
                ]
            }
        },
        gcp_conn_id='google_cloud_default',
    )

    execute_sql