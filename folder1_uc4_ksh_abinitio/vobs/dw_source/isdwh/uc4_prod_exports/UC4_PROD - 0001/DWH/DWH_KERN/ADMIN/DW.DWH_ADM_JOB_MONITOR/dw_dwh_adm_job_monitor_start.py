"""
DW.DWH_ADM_JOB_MONITOR_START Pipeline.
Pre-processing monitoring task to register runtime execution status.
"""

from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

from utils.job_monitor_utils import register_job_monitoring_start_logic

# --- DEFAULT ARGS ---
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 6, 11),
    'retries': 0,
}

def register_job_start_wrapper(**context):
    """Wrapper mapping Airflow context dictionary to functional logical core."""
    dag_id = context['dag'].dag_id
    task_id = context['task_instance'].task_id
    run_id = context['run_id']
    
    register_job_monitoring_start_logic(
        dag_id=dag_id,
        task_id=task_id,
        run_id=run_id
    )

# --- DAG DEFINITION ---
with DAG(
    dag_id='dw_dwh_adm_job_monitor_start',
    default_args=default_args,
    schedule_interval=None,  # Triggered contextually within parent flows
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    doc_md=__doc__
) as dag:

    # --- TASK: register_job_start ---
    register_job_start = PythonOperator(
        task_id='register_job_start',
        python_callable=register_job_start_wrapper,
        provide_context=True
    )

    register_job_start