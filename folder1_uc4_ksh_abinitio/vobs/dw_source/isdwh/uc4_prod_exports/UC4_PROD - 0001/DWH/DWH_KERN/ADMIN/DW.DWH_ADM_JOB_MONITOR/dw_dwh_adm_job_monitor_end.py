"""
DW.DWH_ADM_JOB_MONITOR_END Pipeline.
Post-processing monitoring task to update the state registry and write audit lines.
"""

from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

from utils.job_monitor_utils import register_job_monitoring_end_logic

# --- DEFAULT ARGS ---
default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 0,
}

def register_job_end_wrapper(**context):
    """Wrapper mapping Airflow execution context to functional logical core."""
    dag_id = context['dag'].dag_id
    task_id = context['task_instance'].task_id
    
    # Safely retrieve dag_run configuration dictionary
    dag_run = context.get('dag_run')
    dag_run_conf = dag_run.conf if dag_run else {}
    
    register_job_monitoring_end_logic(
        dag_id=dag_id,
        task_id=task_id,
        dag_run_conf=dag_run_conf
    )

# --- DAG DEFINITION ---
with DAG(
    dag_id='dw_dwh_adm_job_monitor_end',
    default_args=default_args,
    schedule_interval=None,  # Dynamic helper DAG
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    doc_md=__doc__
) as dag:

    # --- TASK: log_and_register_job_end ---
    log_and_register_job_end = PythonOperator(
        task_id='log_and_register_job_end',
        python_callable=register_job_end_wrapper,
        provide_context=True,
    )

    log_and_register_job_end