"""
dwh_parent_workflow.py

An execution wrapper implementing environment configurations and logging 
callbacks using importable helpers in dwh_uc4_helpers.py.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator

# Import migrated UC4 environment logic and handlers
from utils.dwh_uc4_helpers import (
    get_global_gcp_config,
    run_hole_pfad_task,
    dwh_on_success_callback,
    dwh_on_failure_callback
)

# Fetch dynamic variables globally
gcp_env = get_global_gcp_config()

DEFAULT_ARGS = {
    "owner": "airflow-dwh",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2023, 1, 1),
    # Wire the exception evaluators directly onto execution defaults
    "on_failure_callback": dwh_on_failure_callback,
    "on_success_callback": dwh_on_success_callback,
}

with DAG(
    dag_id="dwh_parent_workflow_execution",
    schedule_interval=None,  # Handled on-demand or by parent orchestrator
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    doc_md="""
    ### DWH Parent Integrated Workflow
    Leverages imported utilities from `dwh_uc4_helpers` to calculate 
    environment parameters (`DW.HOLE_PFAD`) and handle logs/state updates (`DW.LESE_LOG`).
    """
) as dag:

    # 1. Execute variable computations and establish context (DW.HOLE_PFAD equivalent)
    initialize_dwh_context = PythonOperator(
        task_id="initialize_dwh_context",
        python_callable=run_hole_pfad_task,
        provide_context=True,
    )

    # 2. Example PySpark Task Configuration
    # Uses environment configurations fetched from dynamic global setup methods
    pyspark_job_config = {
        "reference": {"project_id": gcp_env["GCP_PROJECT_ID"]},
        "placement": {"cluster_name": gcp_env["DATAPROC_CLUSTER_NAME"]},
        "pyspark_job": {
            "main_python_file_uri": f"gs://{gcp_env['GCS_BUCKET_NAME']}/pyspark_scripts/dwh_stamm_processor.py"
        }
    }
    
    execute_pyspark_payload = DataprocSubmitJobOperator(
        task_id="execute_pyspark_payload",
        job=pyspark_job_config,
        region=gcp_env["DATAPROC_REGION"],
        project_id=gcp_env["GCP_PROJECT_ID"]
    )

    # Wire Execution Sequence
    initialize_dwh_context >> execute_pyspark_payload