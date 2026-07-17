from datetime import datetime, timedelta
import logging
from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateBatchOperator

# ==============================================================================
# ENVIRONMENT CONFIGURATION (Airflow Variables)
# ==============================================================================
try:
    GCP_PROJECT = Variable.get("GCP_PROJECT")
    GCP_REGION = Variable.get("GCP_REGION")
    GCS_BUCKET = Variable.get("GCS_BUCKET")
except KeyError as err:
    logging.error("Missing required Airflow Variable: %s", str(err))
    raise

# ==============================================================================
# DEFAULT ARGUMENTS
# ==============================================================================
# No retries were defined in the UC4 source structure; defaults to 0.
DEFAULT_ARGS = { 
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2026, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

# ==============================================================================
# HELPER FUNCTIONS
# ==============================================================================
def build_batch_config(bucket: str, company: str = "ALL") -> dict:
    """
    Builds the Dataproc Serverless Batch Configuration payload.
    
    Args:
        bucket (str): The target Cloud Storage bucket containing the code assets.
        company (str): Target consolidation scope. Defaults to 'ALL'.
        
    Returns:
        dict: Configured payload for DataprocCreateBatchOperator.
    """
    script_uri = (
        f"gs://{bucket}/pyspark/dw_source/isdwh/import/umsatz/"
        "umsatz_konsolidierung.py"
    )
    
    return {
        "pyspark_batch": {
            "main_python_file_uri": script_uri,
            "args": [
                "-m", "{{ logical_date.strftime('%Y%m') }}",
                "-k", company
            ]
        },
        "environment_config": {
            "execution_config": {}
        }
    }

# ==============================================================================
# DAG DEFINITION
# ==============================================================================
with DAG( 
    dag_id="dw_dwh_umsatz_konsolidierung_monatlich_jp",
    default_args=DEFAULT_ARGS,
    description="Monatliche Konsolidierung der Umsatzdaten (UMSATZ) ueber alle Konzerngesellschaften",
    schedule="0 3 1 * *",  # Executes monthly on the 1st at 03:00 AM
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    tags=["dwh", "umsatz", "konsolidierung", "monthly"],
) as dag:

    start = EmptyOperator(task_id="start")

    # Construct the execution payload using our modular helper function
    dataproc_batch_config = build_batch_config(bucket=GCS_BUCKET, company="ALL")

    dw_dwh_umsatz_konsolidierung_monatlich_js = DataprocCreateBatchOperator( 
        task_id="dw_dwh_umsatz_konsolidierung_monatlich_js",
        project_id=GCP_PROJECT,
        region=GCP_REGION,
        batch=dataproc_batch_config,
        batch_id="dw-dwh-ums-kons-{{ logical_date.strftime('%Y%m') }}-js"
    )

    end = EmptyOperator(task_id="end")

    # Workflow Execution Order
    start >> dw_dwh_umsatz_konsolidierung_monatlich_js >> end