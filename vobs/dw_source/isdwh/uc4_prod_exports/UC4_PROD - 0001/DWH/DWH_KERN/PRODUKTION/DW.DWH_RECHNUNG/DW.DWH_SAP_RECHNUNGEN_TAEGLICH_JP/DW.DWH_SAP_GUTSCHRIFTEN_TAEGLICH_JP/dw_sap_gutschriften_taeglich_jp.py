from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

# Importing the reusable component from our custom plugins directory
from utils.dw_job_helper import calculate_dwh_variables, evaluate_job_status

# Default setup properties matching targeted system configuration
DEFAULT_ARGS = {
    "owner": "airflow",
    "start_date": datetime(2026, 3, 29),
    "retries": 1,
    "depends_on_past": False,
}


def execution_preparation_task(**context) -> None:
    """
    Executes variable calculation. Dynamically pushes properties 
    into XComs for consumption by downstream tasks.
    """
    # Fetch execution context date dynamically to maintain idempotence
    logical_date = context["ds"]
    calculated_vars = calculate_dwh_variables(logical_date)
    
    # Store dynamic keys in the task instance database context (XCom)
    for key, val in calculated_vars.items():
        context["ti"].xcom_push(key=key, value=val)


def run_workload_and_post_process(**context) -> None:
    """
    Simulates a workload execution step, automatically handling status assessment
    via the modular evaluate_job_status framework.
    """
    task_instance = context["ti"]
    
    # Dynamically extract parameters processed in the upstream preparation step
    dwh_home = task_instance.xcom_pull(task_ids="initialize_legacy_paths", key="DWH_HOME")
    last_month = task_instance.xcom_pull(task_ids="initialize_legacy_paths", key="LASTMONTH_YYYYMM")
    
    # Mocking standard operational tasks execution
    mock_run_script_path = f"{dwh_home}/scripts/sap_gutschriften.sh"
    logger_identifier = f"DW.DWH_SAP_GUTSCHRIFTEN_TAEGLICH_JP_RUN (Date Scope: {last_month})"
    
    print(f"Executing simulated bash payload pointing to: {mock_run_script_path}")
    
    # Simulated execution exit status (0 = success, other values = error states)
    simulated_return_code = 0 
    
    # Execute structural error post-processor
    evaluate_job_status(return_code=simulated_return_code, job_identifier=logger_identifier)


with DAG(
    dag_id="dw_sap_gutschriften_taeglich_jp",
    schedule_interval="@daily",
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
) as dag:

    # 1. Calculation step (Translating DW.HOLE_PFAD)
    initialize_legacy_paths = PythonOperator(
        task_id="initialize_legacy_paths",
        python_callable=execution_preparation_task,
        provide_context=True,
    )

    # 2. Main Processing and Validation step (Translating DW.LESE_LOG post-processor)
    execute_workload = PythonOperator(
        task_id="execute_workload",
        python_callable=run_workload_and_post_process,
        provide_context=True,
    )

    # Simple workflow dependency mapping
    initialize_legacy_paths >> execute_workload