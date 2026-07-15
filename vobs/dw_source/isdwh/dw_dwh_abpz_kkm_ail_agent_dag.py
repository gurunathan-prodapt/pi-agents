"""
DAG ID: dw_dwh_abpz_kkm_ail_agent_dag
Legacy Job ID: DW.DWH_ABPZ_KKM_AIL_AGENT
Description: Migrated UC4 workflow representing the Agent ADS daily lookup execution flow.
             Submits a PySpark job to Dataproc Serverless.
"""

from datetime import datetime, timedelta
import logging
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.models import Variable
from airflow.utils.trigger_rule import TriggerRule

# ==========================================
# CONSTANTS & ENVIRONMENT RESOLUTIONS
# ==========================================
# Environment variables resolved from Airflow Variables or fallback defaults
GCP_PROJECT_ID = Variable.get("GCP_PROJECT")
DATAPROC_REGION = Variable.get("DATAPROC_REGION")
DATAPROC_CLUSTER_NAME = Variable.get("DATAPROC_CLUSTER")
GCS_BUCKET = Variable.get("GCS_BUCKET")

# Legacy initialization configurations
DWH_HOME = Variable.get("dwh_home", default_var="/opt/dwh")
KKM_RUECKBLICK_LADEDATUM = Variable.get("kkm_rueckblick_ladedatum", default_var="1900-01-01")

# ==========================================
# FAILURE CALLBACKS (Replaces showlog.ksh)
# ==========================================
def on_task_failure_callback(context):
    """
    Simulates showlog.ksh output patterns and dumps structural telemetry 
    to Cloud Logging upon Task failures.
    """
    task_instance = context.get("task_instance")
    task_id = task_instance.task_id
    execution_date = context.get("execution_date")
    exception = context.get("exception")
    
    logging.error("=" * 80)
    logging.error("!!! EXECUTION FAILURE ALERT !!!")
    logging.error(f"Jobkennung ABPZ_KKM_AIL_AGENT eingetragen für {task_id}")
    logging.error(f"Execution Window Date: {execution_date}")
    logging.error(f"Exception Log Detail: {exception}")
    logging.error("=" * 80)


def on_dag_success_callback(context):
    """
    Logs standard legacy success message string inside Airflow.
    """
    logging.info("=" * 80)
    logging.info("Die Abarbeitung des Rahmenskriptes wurde ohne erkennbare Fehler mit Rückgabewert 0 beendet.")
    logging.info("=" * 80)


# ==========================================
# DEFAULT ARGUMENTS
# ==========================================
default_args = {
    "owner": "DWH",
    "depends_on_past": False,
    "start_date": datetime(2023, 6, 11),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": on_task_failure_callback,
}

# ==========================================
# DAG DEFINITION
# ==========================================
with DAG(
    dag_id="dw_dwh_abpz_kkm_ail_agent_dag",
    default_args=default_args,
    description="Refactored UC4 DW.DWH_ABPZ_KKM_AIL_AGENT Daily Lookup Build Flow",
    schedule="0 3 * * *",  # Run daily at 03:00 AM UTC
    catchup=False,
    max_active_runs=1,
    on_success_callback=on_dag_success_callback,
    tags=["dwh", "kkm", "ab_initio_migration"],
) as dag:

    # 1. Start Orchestration Node
    start_step = EmptyOperator(
        task_id="start_pipeline",
    )

    # 2. Dataproc PySpark Execution Step (Replaces r_alis_objekt)
    pyspark_job_definition = {
        "reference": {
            "project_id": GCP_PROJECT_ID,
            "job_id": "dw_kkm_ail_agent_{{ run_id | ts_nodash | lower }}",
        },
        "placement": {
            "cluster_name": DATAPROC_CLUSTER_NAME
        },
        "pyspark_job": {
            "main_python_file_uri": f"gs://{GCS_BUCKET}/pyspark_scripts/agent_ads_lookup.py",
            "args": [
                "--job_kennung", "ABPZ_KKM_AIL_AGENT",
                "--output_file", "AgentADSLookup.txt",
                "--config_file", f"gs://{GCS_BUCKET}/abinitio/cfg/ccm_proc/BHB_CCM_PROC_WriteAgentADSLookup.cfg",
                "--rueckblick_ladedatum", KKM_RUECKBLICK_LADEDATUM,
                "--exec_date", "{{ ds }}",
                "--target_bucket", GCS_BUCKET,
                "--dwh_home", DWH_HOME
            ],
        },
    }

    execute_pyspark_job = DataprocSubmitJobOperator( 
        task_id="dw_dwh_abpz_kkm_ail_agent",
        project_id=GCP_PROJECT_ID,
        region=DATAPROC_REGION,
        job=pyspark_job_definition,
    )

    # 3. End Orchestration Node
    end_step = EmptyOperator(
        task_id="end_pipeline",
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    # Execution Call Graph Sequencing
    start_step >> execute_pyspark_job >> end_step