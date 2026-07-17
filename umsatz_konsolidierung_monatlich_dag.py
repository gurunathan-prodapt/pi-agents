"""
Target File: dags/umsatz_konsolidierung_monatlich_dag.py
Description: Composer Airflow DAG executing monthly revenue processing jobs.
             Replaces UC4 plans JP/JS and triggers the Dataproc Serverless batch task.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateBatchOperator

# Import helper arguments directly from config module
from bin.umsatz_konsolidierung_monatlich_dag import ENV_CONFIG, get_job_arguments

# Fetch target corporate entity from Airflow global configurations, defaulting if missing
TARGET_KONZERNGESELLSCHAFT = Variable.get("KONZERNGESELLSCHAFT", default_var="HQ_CORP")

# Default values applied to tasks inside this workflow
default_args = {
    "owner": "composer-dwh-team",
    "depends_on_past": False,
    "email_on_failure": True,
    "email": ["dwh-alerts@yourcompany.com"],
    "retries": 1,
    "retry_delay": timedelta(minutes=15),
}

with DAG(
    "dw_dwh_umsatz_konsolidierung_monatlich_dag",
    default_args=default_args,
    description="Monthly corporate revenue consolidation processing workflow.",
    schedule_interval="0 2 1 * *",  # Executes monthly on the 1st day of the month at 02:00 AM UTC
    start_date=datetime(2023, 1, 1),
    catchup=False,
    max_active_runs=1,
) as dag:

    # Dynamic templates to map parameters at task launch-time
    gcp_project = Variable.get("GCP_PROJECT", default_var=ENV_CONFIG["GCP_PROJECT"])
    gcs_bucket = Variable.get("GCS_BUCKET", default_var=ENV_CONFIG["GCS_BUCKET"])
    
    # Derives active billing cycle month dynamically (Format: YYYYMM) based on the scheduled execution date
    run_date_val = "{{ execution_date.strftime('%Y%m') }}"
    
    # Configuration structure detailing Dataproc Serverless resource scaling parameters
    batch_configuration = {
        "pyspark_batch": {
            "main_python_file_uri": f"gs://{gcs_bucket}/abinitio/umsatz_konsolidierung.py",
            "args": get_job_arguments(run_date_val, TARGET_KONZERNGESELLSCHAFT),
            "jar_file_uris": [
                f"gs://{gcs_bucket}/lib/ojdbc8.jar"  # Path to Oracle JDBC driver inside environment GCS
            ],
        },
        "environment_config": {
            "execution_config": {
                # Executed on custom network/subnets configured with Private Google Access
                "subnetwork_uri": "default"
            }
        },
        "runtime_config": {
            "version": "2.1"  # Spark 3.4 Runtime Environment
        }
    }

    # Dataproc Serverless Submission Operator Task definition
    submit_dataproc_job = DataprocCreateBatchOperator(
        task_id="execute_umsatz_konsolidierung_pyspark",
        project_id=gcp_project,
        region="europe-west3",  # Assigned GCP execution datacenter
        batch_id=f"umsatz-konsolidierung-{run_date_val}-batch",
        batch=batch_configuration,
        asynchronous=False  # Block execution thread until processing finishes or fails
    )

    submit_dataproc_job