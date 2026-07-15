"""
gcp_environment_setup_dag.py

Example Airflow DAG showcasing how to consume the central GCPEnvironmentLoader
plugin to prepare global variables, check access credentials, and run step-by-step locks.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable

# Import the modern environment engine from local plugins
from gcp_environment_loader import GCPEnvironmentLoader

# Default configurations mapping back to global parameters section
DEFAULT_PROJECT_ID = Variable.get("gcp_project_id", default_var="gcp-prod-dwh")
DEFAULT_ENV_PHASE = Variable.get("environment_phase", default_var="dev")

default_args = {
    "owner": "migration_engine",
    "depends_on_past": False,
    "start_date": datetime(2023, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def verify_environment_variables(**context):
    """Initializes setup configurations and checks basic ecosystem paths."""
    loader = GCPEnvironmentLoader(project_id=DEFAULT_PROJECT_ID, environment_phase=DEFAULT_ENV_PHASE)
    
    # Assert initialization lock
    dag_id = context["dag"].dag_id
    loader.job_initialization_handshake(job_name=dag_id)
    
    try:
        # Emulate .dw_global resolving
        resolved_serial = loader.resolve_global_paths("AI_SERIAL")
        resolved_temp = loader.resolve_global_paths("AI_TEMP")
        
        # Emulate .dw_ai resolving
        parallel_config = loader.fetch_execution_parallelism()
        
        # Log gathered configuration states to standard runner output
        print(f"Validated Landing Path: {resolved_serial}")
        print(f"Validated Transient Path: {resolved_temp}")
        print(f"Worker Configuration Profiles Resolved: {parallel_config}")
        
        # Share variables with downstream tasks through XCom
        context["ti"].xcom_push(key="parallel_config", value=parallel_config)
    except Exception as e:
        # Ensure lock release on failure
        loader.job_finalization_release(job_name=dag_id, exit_status="FAILED")
        raise e


def cleanup_environment_lock(**context):
    """Closes execution windows and unlocks processes for execution singletons."""
    loader = GCPEnvironmentLoader(project_id=DEFAULT_PROJECT_ID, environment_phase=DEFAULT_ENV_PHASE)
    dag_id = context["dag"].dag_id
    loader.job_finalization_release(job_name=dag_id, exit_status="SUCCESS")


with DAG(
    "gcp_environment_setup_verification",
    default_args=default_args,
    description="Verification wrapper asserting global, secret, and parallelism states.",
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
) as dag:

    verify_env_task = PythonOperator(
        task_id="verify_environment_variables",
        python_callable=verify_environment_variables,
        provide_context=True,
    )

    cleanup_lock_task = PythonOperator(
        task_id="cleanup_environment_lock",
        python_callable=cleanup_environment_lock,
        provide_context=True,
        trigger_rule="all_done",  # Run even if predecessors fail to release resources
    )

    verify_env_task >> cleanup_lock_task