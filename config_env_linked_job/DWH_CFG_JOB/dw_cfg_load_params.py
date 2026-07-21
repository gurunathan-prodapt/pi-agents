from datetime import datetime, timedelta
import importlib.util
import os
from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.dataform import (
    DataformCreateCompilationResultOperator,
    DataformCreateWorkflowInvocationOperator,
)

# ─── ENVIRONMENTAL VARIABLES ──────────────────────────────────────────────────
# Global infrastructure configurations
GCP_PROJECT = os.environ.get("GCP_PROJECT") or Variable.get("GCP_PROJECT", default_var=None)
GCP_REGION = os.environ.get("GCP_REGION") or Variable.get("GCP_REGION", default_var=None)
DATAFORM_REPOSITORY_ID = os.environ.get("DATAFORM_REPOSITORY_ID") or Variable.get("DATAFORM_REPOSITORY_ID", default_var="dwh_dataform_repo")

# Job-specific variables
DWH_JOB_KENNUNG = "AUSD_V_TA_PERIOD"

# ─── DEFAULT ARGS ─────────────────────────────────────────────────────────────
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2026, 4, 21),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# ─── TASK FUNCTIONS ───────────────────────────────────────────────────────────
def run_load_params(**kwargs):
    """
    Dynamically loads and runs the python script mirroring r_load_params.ksh
    """
    dags_folder = os.environ.get("DAGS_FOLDER", "/home/airflow/gcs/dags")
    script_path = os.path.join(
        dags_folder,
        "config_env_linked_job/iscfg/bin/r_load_params.py"
    )
    
    if not os.path.exists(script_path):
        raise FileNotFoundError(f"Target python script not found at {script_path}")
        
    spec = importlib.util.spec_from_file_location("r_load_params", script_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    
    # Run the main parameter loading logic
    module.main(job_kennung=DWH_JOB_KENNUNG)

# ─── DAG DEFINITION ───────────────────────────────────────────────────────────
with DAG(
    dag_id='dw_cfg_load_params',
    default_args=default_args,
    description='Load DWH parameter file into staging - converted from UC4 DW.CFG_LOAD_PARAMS',
    schedule_interval='0 3 * * *', # Daily at 03:00 UTC
    catchup=False,
    max_active_runs=1,
) as dag:

    start_boundary = EmptyOperator(task_id='start')

    load_params_task = PythonOperator(
        task_id='load_params',
        python_callable=run_load_params,
    )

    # Triggers compilation of the Dataform repository
    compile_dataform_repo = DataformCreateCompilationResultOperator(
        task_id='compile_dataform_repo',
        project_id=GCP_PROJECT,
        region=GCP_REGION,
        repository_id=DATAFORM_REPOSITORY_ID,
        compilation_result={
            "git_commit_val": "main",
        },
    )

    # Runs Dataform upsert process for d_param_load
    run_dataform_upsert = DataformCreateWorkflowInvocationOperator(
        task_id='run_dataform_upsert',
        project_id=GCP_PROJECT,
        region=GCP_REGION,
        repository_id=DATAFORM_REPOSITORY_ID,
        workflow_invocation={
            "compilation_result_id": "{{ task_instance.xcom_pull(task_ids='compile_dataform_repo')['name'].split('/')[-1] }}",
            "invocation_config": {
                "included_targets": [
                    {
                        "database": GCP_PROJECT,
                        "schema": "DWH_ADM",
                        "name": "d_param_load"
                    }
                ]
            }
        },
    )

    end_boundary = EmptyOperator(task_id='end')

    # Sequential workflow pipeline
    start_boundary >> load_params_task >> compile_dataform_repo >> run_dataform_upsert >> end_boundary