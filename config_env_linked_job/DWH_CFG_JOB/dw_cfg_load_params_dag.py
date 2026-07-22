from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.dataform import (
    DataformCreateCompilationResultOperator,
    DataformCreateWorkflowInvocationOperator
)
from airflow.models import Variable

# Retrieve environment-wide global settings
GCP_PROJECT = Variable.get("GCP_PROJECT", default_var=os.environ.get("GCP_PROJECT"))
GCP_REGION = Variable.get("GCP_REGION", default_var="us-central1")
GCS_BUCKET = Variable.get("GCS_BUCKET")
DATAFORM_REPOSITORY_ID = Variable.get("DATAFORM_REPOSITORY_ID")

# Import python mapping code
from config_env_linked_job.iscfg.bin.r_load_params import load_parameters

default_args = {
    'owner': 'ComposerAdmin',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'dw_cfg_load_params_dag',
    default_args=default_args,
    description='Orchestrates loading parameter configuration files into BigQuery',
    schedule_interval='@daily',
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['dwh', 'cfg', 'load_params'],
) as dag:

    # Task 1: Staging Load (KSH SQL*Loader conversion)
    load_staging_params = PythonOperator(
        task_id='load_staging_params',
        python_callable=load_parameters,
        op_kwargs={
            'gcs_bucket': GCS_BUCKET,
            'source_blob': 'config/d_param_load.properties',
            'target_project': GCP_PROJECT,
            'target_dataset': 'DWH_STG',
            'target_table': 'PARAM_LOAD'
        }
    )

    # Task 2: Compile Dataform
    create_compilation = DataformCreateCompilationResultOperator(
        task_id='create_compilation',
        project_id=GCP_PROJECT,
        region=GCP_REGION,
        repository_id=DATAFORM_REPOSITORY_ID,
        compilation_result={
            "git_commit_val": "main",
        }
    )

    # Task 3: Execute Target Merge via Dataform Tags
    execute_dataform_merge = DataformCreateWorkflowInvocationOperator(
        task_id='execute_dataform_merge',
        project_id=GCP_PROJECT,
        region=GCP_REGION,
        repository_id=DATAFORM_REPOSITORY_ID,
        workflow_invocation={
            "compilation_result_id": "{{ task_instance.xcom_pull(task_ids='create_compilation')['name'].split('/')[-1] }}",
            "invocation_config": {
                "included_tags": ["cfg_load_params"],
            }
        }
    )

    load_staging_params >> create_compilation >> execute_dataform_merge