#!/usr/bin/env python3
"""
DAG: dw_dwh_abpz_kkm_ail_agent
Description: Daily extraction pipeline building the AgentADSLookup dataset.
             Converted from UC4 JOBS_UNIX 'DW.DWH_ABPZ_KKM_AIL_AGENT'.
"""

from datetime import datetime, timedelta
import logging
from airflow import DAG
from airflow.models import Variable
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator

# =========================================================================
# 1. ENVIRONMENT CONFIGURATION & RESOLUTION
# =========================================================================
# Read global environment configurations with safe fallbacks
GCP_PROJECT_ID = Variable.get("GCP_PROJECT", default_var="YOUR_GCP_PROJECT_ID")
DATAPROC_REGION = Variable.get("GCP_REGION", default_var="europe-west3")
DATAPROC_CLUSTER = Variable.get("DATAPROC_CLUSTER_NAME", default_var="YOUR_DATAPROC_CLUSTER_NAME")
GCS_BUCKET = Variable.get("GCS_BUCKET", default_var="YOUR_BUCKET_NAME")

# Job-specific paths and metadata parameters
PYSPARK_SCRIPT_URI = f"gs://{GCS_BUCKET}/pyspark_scripts/agent_ads_lookup.py"
JOB_KENNUNG = "ABPZ_KKM_AIL_AGENT"
OUTPUT_FILE_NAME = "AgentADSLookup.txt"
CONFIG_FILE_NAME = "BHB_CCM_PROC_WriteAgentADSLookup.cfg"


# =========================================================================
# 2. REUSABLE UTILITIES & ALARM HANDLERS
# =========================================================================
def on_failure_alarm(context):
    """
    Standard failure notifier. Triggered when the PySpark execution fails,
    emulating the legacy postcondition 'DW.LESE_LOG' routing.
    """
    task_instance = context.get('task_instance')
    dag_id = task_instance.dag_id
    task_id = task_instance.task_id
    execution_date = context.get('execution_date')
    
    error_message = (
        f"• [ALARM] Airflow Task Failure Notification!\n"
        f"DAG: {dag_id}\n"
        f"Task: {task_id}\n"
        f"Execution Time: {execution_date}\n"
        f"Log URL: {task_instance.log_url}"
    )
    
    # Structural output logged directly to Stackdriver / Cloud Logging
    logging.error(error_message)


# =========================================================================
# 3. DAG DEFINITION
# =========================================================================
default_args = {
    'owner': 'dwh_operations',
    'depends_on_past': False,
    'start_date': datetime(2023, 6, 11),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
    'email_on_retry': False,
}

dag = DAG(
    dag_id='dw_dwh_abpz_kkm_ail_agent',
    default_args=default_args,
    description='Reconstructs flat-file lookup dataset for view DWH$VI_S_SDM_AGENT_ADS',
    schedule_interval='0 3 * * *',  # Daily Run at 03:00 AM
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    tags=['dwh', 'kkm', 'lookup'],
)


# =========================================================================
# 4. TASK DEFINITION
# =========================================================================
# Fetch equivalent lookback date value for: GET_VAR('DW.VARIABLEN_DWK_KKM','KKM_Rueckblick_Ladedatum')
kkm_rueckblick_ladedatum = "{{ var.json.get('dw_variablen_dwk_kkm', {}).get('kkm_rueckblick_ladedatum', 'DEFAULT_DATE') }}"

pyspark_job_config = {
    "reference": {
        "project_id": GCP_PROJECT_ID
    },
    "placement": {
        "cluster_name": DATAPROC_CLUSTER
    },
    "pyspark_job": {
        "main_python_file_uri": PYSPARK_SCRIPT_URI,
        "args": [
            "--job_kennung", JOB_KENNUNG,
            "--rueckblick_ladedatum", kkm_rueckblick_ladedatum,
            "--output_file", OUTPUT_FILE_NAME,
            "--config", CONFIG_FILE_NAME
        ]
    }
}

submit_pyspark_job = DataprocSubmitJobOperator(
    task_id='dw_dwh_abpz_kkm_ail_agent',
    project_id=GCP_PROJECT_ID,
    region=DATAPROC_REGION,
    job=pyspark_job_config,
    # Dynamic Job ID ensures unique runs and historical auditing trail in Dataproc console
    job_id="dw_dwh_abpz_kkm_ail_agent_{{ ts_nodash }}",
    on_failure_callback=on_failure_alarm,
    dag=dag,
)

# Execution Flow
submit_pyspark_job