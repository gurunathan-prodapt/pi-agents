from datetime import datetime
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.dataform import (
    DataformCreateCompilationResultOperator,
    DataformCreateWorkflowInvocationOperator,
)

# Sourced following GLOBAL Variable policies
GCP_PROJECT = Variable.get("GCP_PROJECT")
GCP_REGION = Variable.get("GCP_REGION")

default_args = {
    'owner': 'composer',
    'start_date': datetime(2026, 1, 1),
}

with DAG(
    dag_id='dw_cfg_load_params_dag',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:

    # Task 1: Run the converted Python script to load staging
    def run_load_logic():
        from config_env_linked_job.iscfg.bin.r_load_params import main as load_main
        load_main()

    task_stage_params = PythonOperator(
        task_id='r_load_params',
        python_callable=run_load_logic
    )

    # Task 2a: Compile Dataform
    task_compile_dataform = DataformCreateCompilationResultOperator(
        task_id='compile_dataform',
        project_id=GCP_PROJECT,
        location=GCP_REGION,
        repository_id="dwh_dataform_repo",
        compilation_result={
            "git_commit_val": "main",
        },
    )

    # Task 2b: Run/Invoke Dataform for our action
    task_merge_params = DataformCreateWorkflowInvocationOperator(
        task_id='d_param_load',
        project_id=GCP_PROJECT,
        location=GCP_REGION,
        repository_id="dwh_dataform_repo",
        workflow_invocation={
            "compilation_result": "{{ task_instance.xcom_pull('compile_dataform')['name'] }}",
            "invocation_config": {
                "included_tags": ["params_load"],
            }
        },
    )

    task_stage_params >> task_compile_dataform >> task_merge_params