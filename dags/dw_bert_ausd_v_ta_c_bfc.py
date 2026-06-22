# This DAG replaces the legacy UC4 job DW.BERT_AUSD_V_TA_C_BFC.
# Its purpose is to orchestrate the PySpark job for updating contract extension period caching.

from datetime import timedelta
import pendulum

from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator

# --- GCP Configuration (Placeholders to be replaced) ---
GCP_PROJECT_ID = "YOUR_GCP_PROJECT_ID"
DATAPROC_REGION = "YOUR_DATAPROC_REGION"
DATAPROC_CLUSTER_NAME = "YOUR_DATAPROC_CLUSTER_NAME" # Consider ephemeral clusters for cost efficiency
GCS_BUCKET_NAME = "YOUR_BUCKET_NAME"
PYSPARK_SCRIPT_URI = f"gs://{GCS_BUCKET_NAME}/pyspark_scripts/r_ausd_v_ta_c_bfc.py"

# --- Default Arguments for the DAG ---
default_args = {
    'owner': 'uc4_migration',
    'depends_on_past': False,
    'retries': 0, # Default, can be overridden per task or globally
    'retry_delay': timedelta(minutes=0),
    'start_date': pendulum.datetime(2023, 1, 1, tz="UTC"), # Initial start date; adjust as needed
}

# --- DAG Definition ---
with DAG(
    dag_id="dw_bert_ausd_v_ta_c_bfc",
    schedule=None,  # No schedule found in UC4, set to None for manual triggering initially
    catchup=False,  # Set to False to prevent backfills for past missed schedules
    max_active_runs=1, # Maps to UC4 sync object "Else=Wait" behavior
    is_paused_upon_creation=False, # UC4 job was active
    default_args=default_args,
    tags=['uc4', 'dataproc', 'pyspark', 'data_cache'],
    description='Airflow DAG for updating contract extension period caching, migrated from UC4.',
) as dag:
    # --- Task: Submit PySpark Job to Dataproc ---
    run_dataproc_job = DataprocSubmitJobOperator(
        task_id="run_dw_bert_ausd_v_ta_c_bfc",
        project_id=GCP_PROJECT_ID,
        region=DATAPROC_REGION,
        cluster_name=DATAPROC_CLUSTER_NAME, # Or use a cluster selector for ephemeral clusters
        job={
            "placement": {"cluster_name": DATAPROC_CLUSTER_NAME},
            "pyspark_job": {
                "main_python_file_uri": PYSPARK_SCRIPT_URI,
                "args": ["--job-identifier", "AUSD_V_TA_C_BFC"], # Example: Pass DWH_JOB_KENNUNG
                # Add other PySpark job properties like 'jar_file_uris', 'python_file_uris', 'file_uris' if needed
            },
        },
        # Optional: Add dataproc_job_id for custom job naming
        # Optional: Add execution_timeout for this task
    )

    # --- Task Dependencies ---
    # As there is only one core task, the dependency is implicit from start to finish.
    # Additional tasks (e.g., data quality checks, notification tasks) could be added here.