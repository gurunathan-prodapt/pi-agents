"""
Airflow DAG executing the weekly customer reconciliation job (DW_DWH_KUNDE_ABGL_WOECHENTLICH_JS).
Conforms to folder integrity and scheduling requirements.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from dags.DWH.DWH_KERN.PRODUKTION.DW.DWH_KUNDE.bin.d_abgl_kunde_woech_bin import execute_reconciliation_logic

# Default configuration settings
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Instantiate the orchestration workflow
with DAG(
    dag_id='DW_DWH_KUNDE_ABGL_WOECHENTLICH_JS',
    default_args=default_args,
    description='Weekly customer data comparison and alignment in BigQuery',
    schedule_interval='0 6 * * 1',  # Matches "Every Monday at 06:00 AM"
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['dwh', 'kunde', 'bigquery'],
) as dag:

    reconciliation_task = PythonOperator(
        task_id='run_reconciliation',
        python_callable=execute_reconciliation_logic,
        provide_context=True,
    )