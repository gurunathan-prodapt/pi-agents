"""
Auto-generated Airflow DAG for ausd_bp_ta_bpr_basis_orchestration
"""

import datetime
from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.dataform import (
    DataformCreateCompilationResultOperator,
    DataformWorkflowInvocationOperator,
)

PROJECT_ID = "gcp-project-id"
REGION = "us-central1"
REPOSITORY_ID = "bert-data-transforms"
BRANCH = "main"


def validate_parameters(**kwargs):
    dag_run = kwargs.get("dag_run")
    conf = getattr(dag_run, "conf", None) or {}
    stichtag = conf.get("stichtag", datetime.datetime.now().strftime("%d%m%Y"))

    try:
        datetime.datetime.strptime(stichtag, "%d%m%Y")
    except ValueError as exc:
        raise AirflowException(
            f"Validation failed: Parameter Stichtag '{stichtag}' is not in DDMMYYYY format."
        ) from exc

    return stichtag


default_args = {
    "owner": "composer",
    "start_date": datetime.datetime(2023, 1, 1),
    "retries": 1,
}

with DAG(
    dag_id="ausd_bp_ta_bpr_basis_orchestration",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    tags=["migration", "dataform", "bigquery"],
) as dag:

    validate_params = PythonOperator(
        task_id="validate_params",
        python_callable=validate_parameters,
        provide_context=True,
    )

    compile_dataform = DataformCreateCompilationResultOperator(
        task_id="compile_dataform",
        project_id=PROJECT_ID,
        region=REGION,
        repository_id=REPOSITORY_ID,
        compilation_result={
            "git_commitish": BRANCH
        },
    )

    execute_dataform = DataformWorkflowInvocationOperator(
        task_id="execute_dataform",
        project_id=PROJECT_ID,
        region=REGION,
        repository_id=REPOSITORY_ID,
        workflow_invocation={
            "compilation_result": "{{ ti.xcom_pull(task_ids='compile_dataform') }}"
        },
    )

    validate_params >> compile_dataform >> execute_dataform