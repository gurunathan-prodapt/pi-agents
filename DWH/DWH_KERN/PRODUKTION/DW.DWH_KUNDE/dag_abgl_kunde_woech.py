"""
Apache Airflow DAG: dag_abgl_kunde_woech
Mirrors the folder integrity rule from:
DWH/DWH_KERN/PRODUKTION/DW.DWH_KUNDE/dag_abgl_kunde_woech.py
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.google.cloud.operators.dataform import (
    DataformCreateCompilationResultOperator,
    DataformWriteApiOperator
)

# Import modules from mirrored task actions path
from bin.dag_abgl_kunde_woech_bin import (
    evaluate_run_discrepancies,
    log_warning_message,
    log_completion_message
)

# Default Composer settings
default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': True,
    'email': ['alerts-dwh@company.de'],
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

PROJECT_ID = Variable.get("GCP_PROJECT")
REGION = Variable.get("GCP_REGION")
DATAFORM_REPOSITORY = "kunden-master-reconciliations"

with DAG(
    dag_id='dag_abgl_kunde_woech',
    default_args=default_args,
    description='Orchestrates weekly address reconciliation matching',
    schedule_interval='@weekly',
    catchup=False,
    max_active_runs=1,
    tags=['dwh_kunde', 'production']
) as dag:

    # 1. Output initialization string
    log_start = BashOperator(
        task_id='log_start_message',
        bash_command=(
            'export l_Stichtag="{{ logical_date.strftime(\'%Y%m%d\') }}" && '
            'echo "Starte Adressabgleich Kundenstammdaten fuer Stichtag $l_Stichtag"'
        )
    )

    # 2. Compile Dataform dependencies
    create_compilation = DataformCreateCompilationResultOperator(
        task_id='create_dataform_compilation',
        project_id=PROJECT_ID,
        region=REGION,
        repository_id=DATAFORM_REPOSITORY,
        compilation_result={
            "git_commitish": "main",
            "code_compilation_config": {
                "vars": {
                    "stichtag": "{{ logical_date.strftime('%Y%m%d') }}"
                }
            }
        }
    )

    # 3. Trigger compilation write tasks
    execute_dataform = DataformWriteApiOperator(
        task_id='execute_dataform_models',
        project_id=PROJECT_ID,
        region=REGION,
        repository_id=DATAFORM_REPOSITORY,
        compilation_result="{{ task_instance.xcom_pull('create_dataform_compilation') }}",
        write_api_payload={
            "execution_action": {
                "included_tags": ["weekly_reconciliation"]
            }
        }
    )

    # 4. Run checking and branching logic ported from KSH
    evaluate_metrics = BranchPythonOperator(
        task_id='evaluate_metrics',
        python_callable=evaluate_run_discrepancies,
        provide_context=True
    )

    warning_notification_task = PythonOperator(
        task_id='warning_notification_task',
        python_callable=log_warning_message,
        provide_context=True
    )

    completion_notification_task = PythonOperator(
        task_id='completion_notification_task',
        python_callable=log_completion_message,
        provide_context=True
    )

    # Define exact execution task graph flow preserving historical sequence
    log_start >> create_compilation >> execute_dataform >> evaluate_metrics
    evaluate_metrics >> [warning_notification_task, completion_notification_task]