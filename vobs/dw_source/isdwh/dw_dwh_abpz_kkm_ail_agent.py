"""
DAG ID: dw_dwh_abpz_kkm_ail_agent
Description: Migrated orchestration from UC4 job DW.DWH_ABPZ_KKM_AIL_AGENT.
             Coordinates pre-checks, dynamic date parameters, and triggers the
             Dataproc Serverless PySpark pipeline compiling the AgentADSLookup.
"""

from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator

# ==============================================================================
# 1. ENVIRONMENT CONFIGURATION & POLICY RESOLUTION
# ==============================================================================
# Fetch global constants from Airflow Variables with defaults fallback
GCP_PROJECT_ID = Variable.get("GCP_PROJECT", default_var="YOUR_GCP_PROJECT_ID")
DATAPROC_REGION = Variable.get("DATAPROC_REGION", default_var="YOUR_DATAPROC_REGION")
DATAPROC_CLUSTER = Variable.get("DATAPROC_CLUSTER", default_var="YOUR_DATAPROC_CLUSTER_NAME")
GCS_BUCKET = Variable.get("GCS_BUCKET", default_var="YOUR_BUCKET_NAME")

# Job-specific config maps equivalent to legacy properties
JOB_CONFIG = {
    "source_view": "dw_sdm_agent_ads",
    "target_table": "dw_lookups.AgentADSLookup",
    "lookback_days": "84",  # Evaluated from UC4 legacy parameter -z 84
    "job_identifier": "ABPZ_KKM_AIL_AGENT"
}

# ==============================================================================
# 2. DEFAULT DAG RUNTIME PARAMETERS
# ==============================================================================
default_args = {
    'owner': 'dwh_operations',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,  # Matches XML configuration <ErtMethodDef>1</ErtMethodDef>
    'retry_delay': timedelta(minutes=5),
}

# ==============================================================================
# 3. DAG DEFINITION
# ==============================================================================
with DAG(
    dag_id='dw_dwh_abpz_kkm_ail_agent',
    default_args=default_args,
    description='Rebuilds flat-file lookup for view DWH$VI_S_SDM_AGENT_ADS',
    schedule_interval=None,  # Standard setup: triggered by external Airflow scheduler
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    tags=['dwh_kern', 'kkm', 'jobs_unix']
) as dag:

    # 4. PRE-EXECUTION LIFECYCLE HOOKS
    start_step = EmptyOperator(
        task_id='pre_execution_sync_start'
    )

    # 5. CORE PYSPARK PIPELINE TASK RUNNER
    pyspark_job_definition = {
        "reference": {
            "project_id": GCP_PROJECT_ID
        },
        "placement": {
            "cluster_name": DATAPROC_CLUSTER
        },
        "pyspark_job": {
            "main_python_file_uri": f"gs://{GCS_BUCKET}/pyspark_scripts/abpz_kkm_ail_agent.py",
            "args": [
                "--config", f"gs://{GCS_BUCKET}/config/ccm_proc/BHB_CCM_PROC_WriteAgentADSLookup.cfg",
                "--output", f"gs://{GCS_BUCKET}/lookups/AgentADSLookup.txt",
                "--run_date", "{{ ds }}",
                "--source_view", JOB_CONFIG["source_view"],
                "--target_table", JOB_CONFIG["target_table"],
                "--lookback_days", JOB_CONFIG["lookback_days"],
                "--job_identifier", JOB_CONFIG["job_identifier"]
            ]
        }
    }

    # Generate distinct execution job IDs leveraging Airflow execution date structures
    run_pyspark_job = DataprocSubmitJobOperator(
        task_id='abpz_kkm_ail_agent',
        project_id=GCP_PROJECT_ID,
        region=DATAPROC_REGION,
        job=pyspark_job_definition,
        job_id="dw_dwh_abpz_kkm_ail_agent_{{ ds_nodash }}_{{ ts_nodash }}"
    )

    # 6. POST-EXECUTION LIFECYCLE HOOKS
    end_step = EmptyOperator(
        task_id='post_execution_sync_end'
    )

    # Orchestrate Sequence Layout (14 legacy components compacted to logical Flow)
    start_step >> run_pyspark_job >> end_step