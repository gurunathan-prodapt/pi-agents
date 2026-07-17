"""
DAG Name: dw_dwh_dummy_absd_plato_tarife
UC4 Source Job: DW.DWH_DUMMY_ABSD_PLATO_TARIFE
Source File Path: uc4_airflow_linked_job/DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP/DW.DWH_DUMMY_ABSD_PLATO_TARIFE.xml

Overview:
This DAG represents a migrated UC4 workflow module that defines a single Unix job task.
It acts as an operational utility step (a placeholder or "dummy" task) within a larger
daily Plato tariff mapping sequence. The underlying script runs a non-operational dummy
shell command execution ("Doing nothing"), returning a success state immediately.
It is configured to run on Google Cloud Dataproc as a PySpark job submission.

Schedule:
This DAG has no independent schedule (schedule=None) because it is designed to be
triggered externally within a parent workflow sequence (DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP)
using a TriggerDagRunOperator.

Concurrency & Behavior:
- max_active_runs is set to 1 to mirror legacy UC4 sync object behavior.
- catchup is set to False.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.operators.empty import EmptyOperator

# ==============================================================================
# GCP CONFIGURATION & ENVIRONMENT VARIABLES (Sourced dynamically per policy)
# ==============================================================================
GCP_PROJECT_ID = Variable.get("GCP_PROJECT")
DATAPROC_REGION = Variable.get("GCP_REGION")
DATAPROC_CLUSTER_NAME = Variable.get("DATAPROC_CLUSTER")
GCS_BUCKET = Variable.get("GCS_BUCKET")

# Path to the PySpark wrapper script executing the dummy operations
PYSPARK_SCRIPT_URI = f"gs://{GCS_BUCKET}/pyspark_scripts/dw_dwh_dummy_absd_plato_tarife.py"

# ==============================================================================
# DEFAULT ARGS DEFINITION
# ==============================================================================
# Mapped legacy login/package context to the owner parameter.
# The source UC4 XML defined no retry conditions (retries=0).
default_args = {
    'owner': 'DW.UNIX.ISTNS',
    'depends_on_past': False, 
    'start_date': datetime(2026, 3, 30),
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

# ==============================================================================
# DAG DEFINITION
# ==============================================================================
with DAG(
    dag_id='dw_dwh_dummy_absd_plato_tarife',
    default_args=default_args,
    description='Airflow migration of UC4 job DW.DWH_DUMMY_ABSD_PLATO_TARIFE',
    schedule=None,  # Triggered externally by the parent pipeline
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,  # Source active flag was 1 (True)
) as dag:

    # Pipeline entry boundary step
    start = EmptyOperator(
        task_id='start',
    )

    # Operational utility/dummy Dataproc PySpark Job.
    # Executes the dummy shell command mapped to a PySpark placeholder script.
    # job_id utilizes a dynamic run_id macro to avoid duplicate execution ID rejections.
    dwh_dummy_absd_plato_tarife = DataprocSubmitJobOperator(
        task_id='dwh_dummy_absd_plato_tarife',
        project_id=GCP_PROJECT_ID,
        region=DATAPROC_REGION,
        job={
            'reference': {
                'project_id': GCP_PROJECT_ID,
                'job_id': 'dw_dwh_dummy_absd_plato_tarife_{{ run_id | ts_nodash_lower }}'
            },
            'placement': {
                'cluster_name': DATAPROC_CLUSTER_NAME
            },
            'pyspark_job': {
                'main_python_file_uri': PYSPARK_SCRIPT_URI,
                'args': []  # Script body outputs a print statement/utility placeholder action
            }
        },
    )

    # Pipeline exit boundary step
    end = EmptyOperator(
        task_id='end',
    )

    # ==============================================================================
    # TASK DEPENDENCIES
    # ==============================================================================
    start >> dwh_dummy_absd_plato_tarife >> end