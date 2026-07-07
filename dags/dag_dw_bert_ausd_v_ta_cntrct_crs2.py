import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.dataform import (
    DataformCreateCompilationResultOperator,
    DataformCreateWorkflowInvocationOperator,
)

PROJECT_ID = "gcp-project-id"
REGION = "europe-west3"
REPOSITORY_ID = "dwh-bert-dataform"
COMPILATION_RESULT_ID = "dw_bert_ausd_v_ta_cntrct_crs2_compilation"

default_args = {
    "owner": "Data-Warehouse-Migration",
    "start_date": datetime.datetime(2026, 1, 1),
    "retries": 1,
    "retry_delay": datetime.timedelta(minutes=5),
}

with DAG(
    dag_id="dw_bert_ausd_v_ta_cntrct_crs2",
    default_args=default_args,
    schedule="0 2 * * *",
    catchup=False,
    tags=["DWH", "BERT", "Contracts"],
) as dag:
    start = EmptyOperator(task_id="start")

    compile_dataform = DataformCreateCompilationResultOperator(
        task_id="compile_dataform",
        project_id=PROJECT_ID,
        region=REGION,
        repository_id=REPOSITORY_ID,
        compilation_result={
            "git_commitish": "main",
        },
    )

    run_ta_cntrct_crs2 = DataformCreateWorkflowInvocationOperator(
        task_id="execute_ta_cntrct_crs2_transformation",
        project_id=PROJECT_ID,
        region=REGION,
        repository_id=REPOSITORY_ID,
        workflow_invocation={
            "compilation_result": "{{ task_instance.xcom_pull(task_ids='compile_dataform')['name'] }}",
            "invocation_config": {
                "included_targets": [
                    {
                        "database": PROJECT_ID,
                        "schema": "isbert_schema",
                        "name": "sof_ta_cntrct_crs2",
                    }
                ]
            },
        },
    )

    end = EmptyOperator(task_id="end")

    start >> compile_dataform >> run_ta_cntrct_crs2 >> end