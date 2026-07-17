"""
Airflow DAG representing the UC4 Job Plan 'DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP'.
Orchestrates the daily setups and mappings for the Plato tariff system.
"""

import os
from datetime import datetime, timedelta
from typing import Dict, Any

from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.utils.trigger_rule import TriggerRule

# ==========================================
# ENVIRONMENT & CONFIGURATION RESOLUTION
# ==========================================
# Fallback logic resolves from Airflow Variables first, then OS Environment, then default values
GCP_PROJECT_ID = Variable.get("GCP_PROJECT", default_var=os.environ.get("GCP_PROJECT", "YOUR_GCP_PROJECT_ID"))
GCP_REGION = Variable.get("GCP_REGION", default_var=os.environ.get("GCP_REGION", "YOUR_DATAPROC_REGION"))
DATAPROC_CLUSTER_NAME = Variable.get("DATAPROC_CLUSTER_NAME", default_var=os.environ.get("DATAPROC_CLUSTER_NAME", "YOUR_DATAPROC_CLUSTER_NAME"))
GCS_BUCKET_NAME = Variable.get("GCS_BUCKET_NAME", default_var=os.environ.get("GCS_BUCKET_NAME", "YOUR_BUCKET_NAME"))

# PySpark Target Script Path
PYSPARK_SCRIPT_URI = f"gs://{GCS_BUCKET_NAME}/pyspark_scripts/uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/dw_dwh_dummy_absd_plato_tarife.py"

# ==========================================
# POSTCONDITION ALARM & ERROR HANDLING
# ==========================================
def on_failure_alarm(context: Dict[str, Any]) -> None:
    """
    Callback representing UC4 error handler 'DW.CALL_STANDARD ##911011'.
    Triggered upon upstream task exceptions or failure states.
    """
    task_instance = context.get('task_instance')
    execution_date = context.get('execution_date')
    
    task_id = task_instance.task_id if task_instance else "unknown_task"
    
    error_message = (
        f"ALARM [DW.CALL_STANDARD ##911011]: "
        f"Task [{task_id}] failed during run execution for date [{execution_date}]. "
        f"Triggering core warning and alignment mechanisms."
    )
    # Log to Airflow task stdout for traceback collection
    print(error_message)


# ==========================================
# DAG DEFINITION
# ==========================================
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2026, 3, 30),
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='dw_dwh_plato_tarif_mapping_taeglich_jp',
    default_args=default_args,
    description='Taeglicher Aufbau der Plato Mapping Tabelle zur Verbindung der Plato und der DWH Basistarife',
    schedule_interval=None,      # Triggered manually or by external orchestrators (no EVNT_TIME found)
    catchup=False,
    max_active_runs=1,           # Simulates UC4 Sync Object 'Else=Wait' concurrency gate
    is_paused_upon_creation=False,
    tags=['uc4_migration', 'plato', 'dwh']
) as dag:

    # --- Start and End Boundary Nodes ---
    start_node = EmptyOperator(
        task_id='start',
    )

    end_node = EmptyOperator(
        task_id='end',
        trigger_rule=TriggerRule.ALL_SUCCESS  # Ensures downstream tasks fail if workload fails
    )

    # --- Dataproc Job Parameter Payload Generator ---
    def get_pyspark_job_config() -> Dict[str, Any]:
        """
        Generates PySpark job submission payload dynamically.
        """
        return {
            "reference": {"project_id": GCP_PROJECT_ID},
            "placement": {"cluster_name": DATAPROC_CLUSTER_NAME},
            "pyspark_job": {
                "main_python_file_uri": PYSPARK_SCRIPT_URI
            }
        }

    # --- Task: dw_dwh_dummy_absd_plato_tarife ---
    # Maps directly to JOBS_UNIX object: 'DW.DWH_DUMMY_ABSD_PLATO_TARIFE'
    dw_dwh_dummy_absd_plato_tarife = DataprocSubmitJobOperator(
        task_id='dw_dwh_dummy_absd_plato_tarife',
        project_id=GCP_PROJECT_ID,
        region=GCP_REGION,
        job=get_pyspark_job_config(),
        # Use Jinja template to guarantee unique job IDs across backfills or executions
        job_id="dw_dwh_dummy_absd_plato_tarife_{{ run_id | replace(':', '_') | replace('+', '_') | replace('.', '_') }}",
        on_failure_callback=on_failure_alarm,
        trigger_rule=TriggerRule.ALL_SUCCESS
    )

    # --- Dependency Pipeline Layout ---
    start_node >> dw_dwh_dummy_absd_plato_tarife >> end_node