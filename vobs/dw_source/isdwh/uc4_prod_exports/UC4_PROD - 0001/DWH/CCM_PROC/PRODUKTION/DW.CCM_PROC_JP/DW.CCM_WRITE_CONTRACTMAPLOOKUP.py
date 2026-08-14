"""
DAG to execute CCM_PROC: Write Contract Map Lookup (Ab Initio graph).
This DAG orchestrates the execution of the migrated PySpark job that replaces
the legacy Ab Initio graph BHB_CCM_PROC_WriteContractMapLookup.mp, which was 
previously executed via wrapper script BHB_CCM_PROC_WriteContractMapLookup.ksh.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.models import Variable

# ── GCP CONFIGURATION CONSTANTS ──────────────────────────────────────────────
GCP_PROJECT = Variable.get("GCP_PROJECT")
GCP_REGION = Variable.get("GCP_REGION")
DATAPROC_REGION = Variable.get("DATAPROC_REGION")
DATAPROC_CLUSTER = Variable.get("DATAPROC_CLUSTER")
GCS_BUCKET = Variable.get("GCS_BUCKET")

# Default arguments as per the design document properties
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='dw_ccm_write_contractmaplookup',
    default_args=default_args,
    description='CCM_PROC: Write Contract Map Lookup (Ab Initio graph)',
    schedule=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    tags=['ccm_proc', 'abinitio'],
) as dag:

    # ── TASK DEFINITIONS ──────────────────────────────────────────────────────
    # Executes the PySpark job converted from the BHB_CCM_PROC_WriteContractMapLookup Ab Initio graph.
    # Outputs the Contract Map Lookup attributes to the specified GCS location.
    ccm_write_contractmaplookup_task = DataprocSubmitJobOperator(
        task_id='ccm_write_contractmaplookup_task',
        project_id=GCP_PROJECT,
        region=DATAPROC_REGION,
        job_id="{{ dag.dag_id }}_{{ run_id }}_ccm_write_contractmaplookup",
        pyspark_job={
            "main_python_file_uri": f"gs://{GCS_BUCKET}/pyspark_scripts/bhb_ccm_proc_writecontractmaplookup.py",
            "args": [
                "--first_day", "{{ ds_nodash }}",
                "--last_day_plus_1", "{{ next_ds_nodash }}"
            ]
        },
        cluster_name=DATAPROC_CLUSTER,
    )

    # Dependency Map: Single task execution pipeline
    ccm_write_contractmaplookup_task