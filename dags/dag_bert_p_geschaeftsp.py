from datetime import datetime
from airflow import DAG
from airflow.providers.google.cloud.operators.dataform import DataformCreateCompilationResultOperator, DataformGetCompilationResultOperator
from airflow.models import Variable

# Project configuration from Airflow Variables with design document fallbacks
GCP_PROJECT_ID = Variable.get("gcp_project_id", default_var="{{var.value.gcp_project_id}}")
DATAFORM_REPOSITORY = Variable.get("dataform_repository", default_var="bert_dataform_repo")
GCP_LOCATION = Variable.get("gcp_location", default_var="europe-west3")

define_v_datum = "{{ ds_nodash }}"

# Note regarding legacy unresolved components:
# - DW.BERT_LESE_LOG: SOURCE: NOT FOUND. Purpose was logging management, now handled natively by Airflow execution logging and Stackdriver Logging.
# - DW.HOLE_PFAD: SOURCE: NOT FOUND. Purpose was path initialization, now natively handled by Airflow variable management and standard environment variables.

with DAG(
    dag_id="dag_bert_p_geschaeftsp",
    schedule_interval="0 2 * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["bert", "dwh", "master_data"],
) as dag:

    # Trigger Dataform compilation and execution substituting execution date for v_datum
    compile_dataform = DataformCreateCompilationResultOperator(
        task_id="compile_dataform",
        project_id=GCP_PROJECT_ID,
        location=GCP_LOCATION,
        repository_id=DATAFORM_REPOSITORY,
        compilation_result={
            "code_compilation_config": {
                "vars": {
                    "v_datum": define_v_datum
                }
            }
        }
    )

    compile_dataform