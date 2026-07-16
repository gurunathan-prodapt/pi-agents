from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

# Import Modular Business Components
from dags.modules import logger as custom_logger
from dags.modules.date_utils import resolve_stichtag
from dags.modules.dataform_operations import DataformExecutionHelper
from dags.modules.bq_operations import BigQueryEgressHelper

# -------------------------------------------------------------
# ENV VARIABLE CLASSIFICATION & INGESTION
# -------------------------------------------------------------
# GLOBAL (Environment-Wide Configurations)
PROJECT_ID = Variable.get("GCP_PROJECT")
LOCATION = Variable.get("GCP_LOCATION", default_var="europe-west3")
REPOSITORY_ID = Variable.get("DATAFORM_REPOSITORY_ID")
GCS_BUCKET = Variable.get("GCS_BUCKET")

# JOB-SPECIFIC (Pipeline-Specific Configurations)
DATASET_ID = "dw_staging"
TABLE_ID = "tmp_export_rechnung_taeglich"

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "start_date": datetime(2023, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def run_resolve_stichtag(**kwargs) -> str:
    """Wrapper function to dynamically calculate date bounds parameter."""
    dag_run_conf = kwargs.get("dag_run").conf if kwargs.get("dag_run") else None
    return resolve_stichtag(dag_run_conf)


def run_log_initialization(**kwargs) -> None:
    """Diagnostic initializer stage logging."""
    ti = kwargs["ti"]
    stichtag = ti.xcom_pull(task_ids="resolve_stichtag_task")
    custom_logger.log_initialization(stichtag)


def run_log_start_export(**kwargs) -> None:
    """Informative startup step logging."""
    ti = kwargs["ti"]
    stichtag = ti.xcom_pull(task_ids="resolve_stichtag_task")
    custom_logger.log_start_export(stichtag)


def run_dataform_pipeline(**kwargs) -> str:
    """Compiles and executes the corresponding target Dataform components."""
    ti = kwargs["ti"]
    stichtag = ti.xcom_pull(task_ids="resolve_stichtag_task")

    helper = DataformExecutionHelper(
        project_id=PROJECT_ID, location=LOCATION, repository_id=REPOSITORY_ID
    )

    # 1. Compile Dataform with dynamic variable bindings
    compilation_name = helper.trigger_model_compilation(
        git_commitish="main", vars_dict={"stichtag": stichtag}
    )

    # 2. Invoke Model Processing Execution
    invocation_name = helper.execute_target_model(
        compilation_result_name=compilation_name,
        dataset_id=DATASET_ID,
        table_id=TABLE_ID,
    )

    # 3. Block till successful completion
    helper.await_execution(invocation_name)
    return invocation_name


def run_validation_and_egress(**kwargs) -> None:
    """Verifies staging record states and exports result set."""
    ti = kwargs["ti"]
    stichtag = ti.xcom_pull(task_ids="resolve_stichtag_task")

    egress_helper = BigQueryEgressHelper(project_id=PROJECT_ID)

    # Perform Count Query Verification
    row_count = egress_helper.check_records_count(
        dataset_id=DATASET_ID, table_id=TABLE_ID, stichtag=stichtag
    )

    # Zero-Row Conditional Verification Logic
    if row_count == 0:
        custom_logger.log_warning_no_data(stichtag)
        return

    # Record target system execution logs
    custom_logger.log_row_count(row_count)

    # Perform table export to Google Cloud Storage
    stichtag_clean = stichtag.replace("-", "")
    filename_prefix = f"rechnung_export_{stichtag_clean}.dat"

    egress_helper.extract_table_to_gcs(
        dataset_id=DATASET_ID,
        table_id=TABLE_ID,
        gcs_bucket=GCS_BUCKET,
        filename_prefix=filename_prefix,
    )

    # Log successful execution completion state
    custom_logger.log_clean_completion()


# -------------------------------------------------------------
# DAG ORCHESTRATION PIPELINE DEFINITION
# -------------------------------------------------------------
with DAG(
    "dw_dwh_rechnung_export_taeglich_js",
    default_args=DEFAULT_ARGS,
    schedule_interval="0 6 * * *",  # Daily 06:00 UTC
    catchup=False,
) as dag:

    resolve_stichtag_task = PythonOperator(
        task_id="resolve_stichtag_task",
        python_callable=run_resolve_stichtag,
        provide_context=True,
    )

    log_init = PythonOperator(
        task_id="log_initialization",
        python_callable=run_log_initialization,
        provide_context=True,
    )

    log_start = PythonOperator(
        task_id="log_start_export",
        python_callable=run_log_start_export,
        provide_context=True,
    )

    dataform_execution_task = PythonOperator(
        task_id="dataform_execution_task",
        python_callable=run_dataform_pipeline,
        provide_context=True,
    )

    verify_and_extract = PythonOperator(
        task_id="verify_and_extract",
        python_callable=run_validation_and_egress,
        provide_context=True,
    )

    # Pipeline task ordering
    resolve_stichtag_task >> log_init >> log_start >> dataform_execution_task >> verify_and_extract