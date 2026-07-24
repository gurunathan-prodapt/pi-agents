import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.dataform import (
    DataformCreateCompilationResultOperator,
    DataformRunOperator,
)

# ==============================================================================
# RUNTIME CONFIGURATION RESOLUTION
# ==============================================================================
# Avoid hardcoded environment placeholders by resolving through Airflow Variables
GCP_PROJECT = Variable.get("GCP_PROJECT")
GCP_REGION = Variable.get("GCP_REGION", default_var="europe-west3")
DATAFORM_REPOSITORY = Variable.get("DATAFORM_REPOSITORY", default_var="dwh-dataform-repo")
GCS_BUCKET = Variable.get("GCS_BUCKET")

DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2026, 4, 21),
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="dw_cfg_load_params",
    default_args=DEFAULT_ARGS,
    description="Orchestrator for loading DWH parameter files - Migrated from UC4 DW.CFG_LOAD_PARAMS",
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
) as dag:

    # 1. Initial Boundary Marker
    start_boundary = EmptyOperator(task_id="start_boundary")

    # 2. Execute Python-translated script (r_load_params.py)
    run_load_params_script = BashOperator(
        task_id="run_load_params_script",
        bash_command="python3 /workspace/config_env_linked_job/iscfg/bin/r_load_params.py --job_kennung AUSD_V_TA_PERIOD",
        env={
            "GCP_PROJECT": GCP_PROJECT,
            "GCS_BUCKET": GCS_BUCKET,
            "BQ_DATASET_STG": "DWH_STG"
        }
    )

    # 3. Create a Dataform Compilation Result
    compile_dataform = DataformCreateCompilationResultOperator(
        task_id="compile_dataform",
        project_id=GCP_PROJECT,
        region=GCP_REGION,
        repository_id=DATAFORM_REPOSITORY,
        compilation_result={
            "git_commit_ish": "main"
        }
    )

    # 4. Trigger Dataform Merge Operation (d_param_load.sqlx)
    run_dataform_merge = DataformRunOperator(
        task_id="run_dataform_merge",
        project_id=GCP_PROJECT,
        region=GCP_REGION,
        repository_id=DATAFORM_REPOSITORY,
        invocation_config={
            "included_targets": [{"name": "d_param_load"}]
        }
    )

    # 5. Terminal Boundary Marker
    end_boundary = EmptyOperator(task_id="end_boundary")

    # ==============================================================================
    # TASK DEPENDENCY GRAPH
    # ==============================================================================
    start_boundary >> run_load_params_script >> compile_dataform >> run_dataform_merge >> end_boundary