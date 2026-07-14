"""
DAG: dw_produktion_allgemein_includes
Description: Replicates the overall legacy UC4 scheduling flow, utilizing modular include components.
"""

from datetime import datetime
from typing import Any

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

# Import migrated modular include files from custom structures
from templates.dw_env_resolver import compute_environment_context
from templates.dw_error_handler import on_failure_show_log

# Define default configuration arguments
DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 0,  # Replicates the immediate fail-fast strategy of legacy systems
    'on_failure_callback': on_failure_show_log
}


def register_job_monitor_start(**context: Any) -> None:
    """
    Substitutes legacy 'DW.DWH_ADM_JOB_MONITOR_START'.
    Registers the start of the daily execution flow inside target audit layers.
    """
    ti = context['ti']
    # Extract the computed environment variable map from XCom
    computed_vars = ti.xcom_pull(task_ids='initialize_environment', key='return_value') or {}
    
    print("--- [MONITOR START] ---")
    print(f"Registering job run start timestamp: {datetime.utcnow().isoformat()}")
    print(f"Target Run Context Variables Extracted: {list(computed_vars.keys())}")
    print(f"Active Carmen Configuration Flag      : {computed_vars.get('AKTIV_CARMEN', '0')}")
    print("-----------------------")


def register_job_monitor_end(**context: Any) -> None:
    """
    Substitutes legacy 'DW.DWH_ADM_JOB_MONITOR_END' under successful pipeline executions.
    """
    print("--- [MONITOR END] ---")
    print(f"Registering successful job execution path at: {datetime.utcnow().isoformat()}")
    print("Pipeline run completed cleanly.")
    print("---------------------")


# Primary Airflow DAG orchestration mapping to the Daily midnight execution schedule
with DAG(
    dag_id='dw_produktion_allgemein_includes',
    default_args=DEFAULT_ARGS,
    schedule_interval='0 0 * * *',
    start_date=datetime(2023, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['dwh', 'production', 'uc4_only']
) as dag:

    # Step 1: Initialize Environment and Compute Date Calculus (DW.HOLE_PFAD equivalent)
    initialize_environment = PythonOperator(
        task_id='initialize_environment',
        python_callable=compute_environment_context,
        op_kwargs={'logical_date': '{{ ds }}'},
        provide_context=True
    )

    # Step 2: Initialize Audit Logs (DW.DWH_ADM_JOB_MONITOR_START equivalent)
    job_monitor_start = PythonOperator(
        task_id='job_monitor_start',
        python_callable=register_job_monitor_start,
        provide_context=True
    )

    # Step 3: Main Execution Step Placeholder (Target for specific business transformations)
    execute_production_work = EmptyOperator(
        task_id='execute_production_work',
    )

    # Step 4: Finalize Success Auditing (DW.DWH_ADM_JOB_MONITOR_END equivalent)
    job_monitor_end = PythonOperator(
        task_id='job_monitor_end',
        python_callable=register_job_monitor_end,
        provide_context=True,
        trigger_rule='all_success'  # Ensures step only triggers if upstream pipeline runs completely without error
    )

    # Step Execution Flow Setup
    initialize_environment >> job_monitor_start >> execute_production_work >> job_monitor_end