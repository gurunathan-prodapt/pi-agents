from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.dataform import (
    DataformCreateCompilationResultOperator,
    DataformCreateWorkflowInvocationOperator,
)

# Retrieve Global Constants
GCP_PROJECT = Variable.get("GCP_PROJECT")
GCP_REGION = Variable.get("GCP_REGION")
GCS_BUCKET = Variable.get("GCS_BUCKET")
DATAFORM_REPOSITORY = Variable.get("DATAFORM_REPOSITORY")

default_args = {
    'owner': 'Data-Migration',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'email_on_failure': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'dw_cfg_load_params',
    default_args=default_args,
    schedule_interval='0 2 * * *',
    catchup=False,
    max_active_runs=1,
) as dag:

    # Task 1: Execute Python Parameter Loader
    def run_loader_script():
        import sys
        # Ensure we can find the config_env_linked_job module in the system path
        airflow_home = os.environ.get("AIRFLOW_HOME", "/home/airflow/gcs")
        dags_folder = os.path.join(airflow_home, "dags")
        if dags_folder not in sys.path:
            sys.path.append(dags_folder)
            
        from config_env_linked_job.iscfg.bin import r_load_params
        param_file = f"gs://{GCS_BUCKET}/config/param_load.properties"
        r_load_params.load_parameters(
            param_file_path=param_file,
            project_id=GCP_PROJECT,
            dataset_id="DWH_STG",
            table_id="PARAM_LOAD"
        )

    load_parameters_task = PythonOperator(
        task_id='load_parameters_to_staging',
        python_callable=run_loader_script
    )

    # Task 2: Create Dataform Compilation Result
    create_compilation_result = DataformCreateCompilationResultOperator(
        task_id="create_compilation_result",
        project_id=GCP_PROJECT,
        region=GCP_REGION,
        repository_id=DATAFORM_REPOSITORY,
        compilation_result={
            "git_commit_val": "main",
        },
    )

    # Task 3: Create Dataform Workflow Invocation for Merge logic
    run_dataform_merge = DataformCreateWorkflowInvocationOperator(
        task_id='run_dataform_merge',
        project_id=GCP_PROJECT,
        region=GCP_REGION,
        repository_id=DATAFORM_REPOSITORY,
        workflow_invocation={
            "compilation_result_id": "{{ task_instance.xcom_pull(task_ids='create_compilation_result')['name'].split('/')[-1] }}"
        },
    )

    load_parameters_task >> create_compilation_result >> run_dataform_merge