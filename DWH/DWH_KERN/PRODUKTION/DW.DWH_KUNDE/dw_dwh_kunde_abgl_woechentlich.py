import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from scripts.r_abgl_kunde_woech import run_address_alignment

# Environment variables (Global Configuration mapped to Airflow Variables / Env)
GCP_PROJECT = os.environ.get("GCP_PROJECT")
BQ_DATASET = os.environ.get("BQ_DATASET", "DWH_KUNDE")

default_args = {
    'owner': 'dw_operators',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='dw_dwh_kunde_abgl_woechentlich_js',
    default_args=default_args,
    description='Weekly customer address alignment run (Migrated from UC4)',
    schedule_interval='0 3 * * 7',  # Weekly on Sundays
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
) as dag:

    def execute_alignment(**kwargs):
        # Retrieve date parameter
        lauf_woche = kwargs['templates_dict']['lauf_woche']
        
        # Exact UC4 XML Print Literal:
        print(f"Kundenadressabgleich fuer Lauf {lauf_woche} angestossen")
        
        # Execute the migrated logic wrapper
        run_address_alignment(stichtag=lauf_woche)

    run_alignment_task = PythonOperator(
        task_id='dw_dwh_kunde_abgl_woechentlich_js',
        python_callable=execute_alignment,
        templates_dict={'lauf_woche': '{{ ds_nodash }}'},
        provide_context=True,
    )