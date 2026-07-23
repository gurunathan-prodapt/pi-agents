/*
DAG: dw_rpos_carm_import
Source UC4 Job: DW.RPOS_CARM_IMPORT
Schedule: None (Manual / Triggered by parent orchestrator)

Overview:
    This DAG represents the migrated UC4 UNIX Job `DW.RPOS_CARM_IMPORT`.
    In the legacy environment, this job executed an Ab Initio graph named 
    `RPOS_CARM_IMPORT` using the configuration file `map_rpos_carmen_import.cfg` 
    under the UNIX login `DW.UNIX.ISTNS`.
    
    In the target Google Cloud Platform (GCP) architecture, the Ab Initio graph's 
    ingestion and processing logic is migrated to a PySpark script executed on 
    a Dataproc cluster. 

Execution Flow:
    1. start: Empty execution anchor.
    2. rpos_carm_import: Submits the PySpark/wrapper script (`map_rpos_carmen_import_wrapper.py`)
       to the specified Cloud Dataproc cluster. This wrapper validates configs, environment
       variables, and triggers the PySpark pipeline.
    3. end: Empty execution anchor denoting successful run completion.

Target Tables Updated:
    - DWH$TA_F_RPOS_CARM
    - DWH$TA_F_RPOS_FACT_CARM
    - DWH$TA_F_RPOS_RESELLING_CARM
    - DWH$TA_F_GPOS_FACT_CARM
    - DWH$TA_T_RPOS_CARM
*/

import datetime
from datetime import timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator

# ==============================================================================
# GCP CONFIGURATION CONSTANTS
# ==============================================================================
# Retrieve target environments via Airflow Variables as mandated by dynamic environment variable policies
GCP_PROJECT_ID = Variable.get("GCP_PROJECT")
DATAPROC_REGION = Variable.get("GCP_REGION")
DATAPROC_CLUSTER_NAME = Variable.get("DATAPROC_CLUSTER")
GCS_BUCKET = Variable.get("GCS_BUCKET")

# PySpark Script and Config Paths (Mirrors Legacy Folder Structure)
PYSPARK_SCRIPT_URI = f"gs://{GCS_BUCKET}/abinitio_rpos_carmen_linked_job/TMD_processing/BHB/BD_PROC/run/map_rpos_carmen_import_wrapper.py"
CONFIG_PATH = "abinitio_rpos_carmen_linked_job/isdwh/abinitio/cfg/bd_proc/map_rpos_carmen_import_config.py"

# ==============================================================================
# DEFAULT DAG ARGUMENTS
# ==============================================================================
DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime.datetime(2026, 4, 21),
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

# ==============================================================================
# DAG DEFINITION
# ==============================================================================
with DAG(
    dag_id="dw_rpos_carm_import",
    default_args=DEFAULT_ARGS,
    schedule=None,                     # Inherited / dynamic schedule
    catchup=False,                     # Prevents backfilling historic runs
    max_active_runs=1,                 # Standard lock pattern
    is_paused_upon_creation=False,     # Active status mapped from UC4 <Active>1</Active>
    tags=["migration", "uc4", "bd_proc"],
) as dag:

    # 1. Start Anchor
    start = EmptyOperator(
        task_id="start",
    )

    # PySpark Dataproc Job Configuration
    pyspark_job_config = {
        "reference": {
            "project_id": GCP_PROJECT_ID
        },
        "placement": {
            "cluster_name": DATAPROC_CLUSTER_NAME
        },
        "pyspark_job": {
            "main_python_file_uri": PYSPARK_SCRIPT_URI,
            "args": [
                "--config_path", CONFIG_PATH
            ],
        },
    }

    # 2. Dataproc PySpark Job Submission Task
    rpos_carm_import = DataprocSubmitJobOperator(
        task_id="rpos_carm_import",
        project_id=GCP_PROJECT_ID,
        region=DATAPROC_REGION,
        job=pyspark_job_config,
        job_id="{{ dag.dag_id }}_{{ run_id | ts_nodash }}_rpos_carm_import",
        wait_for_completion=True,
    )

    # 3. End Anchor
    end = EmptyOperator(
        task_id="end",
    )

    # ==============================================================================
    # TASK DEPENDENCY MAP
    # ==============================================================================
    start >> rpos_carm_import >> end