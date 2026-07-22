import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable

# Dynamic Global Variable Resolution
GCP_PROJECT = os.environ.get("GCP_PROJECT")
GCP_REGION = os.environ.get("GCP_REGION")
GCS_BUCKET = Variable.get("GCS_BUCKET")

DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2026, 3, 30),
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

def execute_dummy_action():
    # OUTPUT/PRINT LITERAL RULE: Verbatim message with original typo retained character-for-character
    print("Doing nothinig")

with DAG(
    dag_id='dw_dwh_dummy_absd_plato_tarife',
    default_args=DEFAULT_ARGS,
    schedule=None,
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    tags=['migrated_uc4', 'dwh_plato_tarif'],
) as dag:

    task_start = EmptyOperator(task_id='start')

    # Native Python execution prevents cluster initialization overheads for a simple print command
    task_dummy_plato_tarife = PythonOperator(
        task_id='dw_dwh_dummy_absd_plato_tarife',
        python_callable=execute_dummy_action,
    )

    task_end = EmptyOperator(task_id='end')

    task_start >> task_dummy_plato_tarife >> task_end