"""
DAG: dw_cfg_load_params

This DAG loads DWH parameter files into the staging environment.
It was migrated from the legacy UC4 JOBS_UNIX object `DW.CFG_LOAD_PARAMS`.

The target platform uses Cloud Composer, Dataform, and BigQuery.
The execution sequence is preserved as follows:
1. Parent Airflow DAG coordinates execution of downstream tasks.
2. Execution of Load Script: Runs `r_load_params.py` (migrated from `r_load_params.ksh`).
3. Execution of SQL Transformation: Triggers Dataform transformation `d_param_load.sqlx`.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.dataform import (
    DataformCreateCompilationResultOperator,
    DataformCreateWorkflowInvocationOperator,
)

# ── GLOBAL CONFIGURATION (Sourced at Runtime) ─────────────────────────────────
GCP_PROJECT = Variable.get("GCP_PROJECT")
GCS_BUCKET = Variable.get("GCS_BUCKET")
BQ_DATASET = Variable.get("BQ_DATASET")
CONN_DW_UNIX_ISBERT = Variable.get("CONN_DW_UNIX_ISBERT", default_var="CONN_DW_UNIX_ISBERT")
GCP_REGION = Variable.get("GCP_REGION", default_var="us-central1")

# ── JOB-SPECIFIC PARAMETERS ───────────────────────────────────────────────────
DWH_JOB_KENNUNG = "AUSD_V_TA_PERIOD"

# ── DEFAULT ARGUMENTS ────────────────────────────────────────────────────────
DEFAULT_ARGS = { 
    'owner': 'dw',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# ── DAG DEFINITION ───────────────────────────────────────────────────────────
with DAG(
    dag_id='dw_cfg_load_params',
    default_args=DEFAULT_ARGS,
    description='Load DWH parameter file into staging - Migrated from UC4',
    schedule=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    max_active_runs=1,  # Prevent race conditions during parameter file staging operations
    is_paused_upon_creation=False,
    tags=['dwh', 'migration_uc4', 'parameter_load'],
) as dag:

    # ── TASK 1: Execution of Python Load Task ────────────────────────────────
    # Executes the translated Python script 'r_load_params.py'
    # Env variables map legacy variables and GCP config keys for runtime resolution
    run_load_params = BashOperator(
        task_id='run_load_params',
        bash_command='python3 ${AIRFLOW_HOME}/dags/config_env_linked_job/iscfg/bin/r_load_params.py',
        env={
            'DWH_JOB_KENNUNG': DWH_JOB_KENNUNG,
            'GCP_PROJECT': GCP_PROJECT,
            'GCS_BUCKET': GCS_BUCKET,
            'BQ_DATASET': BQ_DATASET,
            'CONN_DW_UNIX_ISBERT': CONN_DW_UNIX_ISBERT,
        },
    )

    # ── TASK 2: Dataform Compilation ─────────────────────────────────────────
    # Compiles the Dataform repository for the BigQuery transformation
    create_compilation = DataformCreateCompilationResultOperator(
        task_id='create_compilation',
        project_id=GCP_PROJECT,
        region=GCP_REGION,
        repository_id='dwh_dataform_repository',
        compilation_result={
            'git_commitish': 'main',
        },
    )

    # ── TASK 3: Execution of SQL Transformation ──────────────────────────────
    # Invokes the specific d_param_load model in Dataform after load script completion
    invoke_dataform = DataformCreateWorkflowInvocationOperator(
        task_id='invoke_dataform',
        project_id=GCP_PROJECT,
        region=GCP_REGION,
        repository_id='dwh_dataform_repository',
        workflow_invocation={
            'compilation_result': "{{ task_instance.xcom_pull(task_ids='create_compilation')['name'] }}",
            'invocation_config': {
                'included_targets': [
                    {
                        'database': GCP_PROJECT,
                        'schema': BQ_DATASET,
                        'name': 'd_param_load'
                    }
                ]
            }
        },
    )

    # ── TASK DEPENDENCY MAP ──────────────────────────────────────────────────
    run_load_params >> create_compilation >> invoke_dataform