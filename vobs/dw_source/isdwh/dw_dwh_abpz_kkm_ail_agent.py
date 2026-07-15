"""
DAG ID: dw_dwh_abpz_kkm_ail_agent
Description: Orchestrates migration extraction pipeline from BigQuery views to GCS Agent Lookup files.
"""

from datetime import datetime, timedelta
import logging

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateBatchOperator
from airflow.utils.trigger_rule import TriggerRule

# ── ENVIRONMENT VARIABLES (Resolution & Validation) ─────────────────────────
try:
    GCP_PROJECT = Variable.get("GCP_PROJECT")
    GCP_REGION = Variable.get("GCP_REGION")
    GCS_BUCKET = Variable.get("GCS_BUCKET")
except KeyError as e:
    logging.error(f"Missing required Global Environment Airflow Variable: {str(e)}")
    raise

# ── JOB-SPECIFIC PARAMETERIZATION ──────────────────────────────────────────
LOOKUP_NAME = "AgentADSLookup.txt"
BACKLOOK_DAYS = 84
PROJECT_PREFIX = "BHB_CCM_PROC"
PYSPARK_FILE_PATH = f"gs://{GCS_BUCKET}/pyspark/agent_ads_lookup.py"

# ── REUSABLE HELPER FUNCTIONS / BUSINESS LOGIC ─────────────────────────────
def mock_monitoring_call(status: str, execution_date: str, **context):
    """
    Simulates the legacy job monitor call (previously start_monitoring / end_monitoring).
    """
    logging.info(f"JOB MONITORING: Operational state '{status}' registered for execution {execution_date}.")
    logging.info(f"DAG Run Context metadata: {context.get('dag_run')}")


def on_failure_callback(context):
    """
    Global failure callback hook designed to post diagnostic reports and preserve verbatim legacy failure logs.
    """
    task_instance = context.get('task_instance')
    # Verbatim original German failure status logs carried from source (Lese_Log execution failure)
    print("****************************************************************")
    print("Rueckgabewert: '1' (Fehlerfall)***************************")
    print("****************************************************************")
    
    err_msg = (
        f"🚨 TASK FAILURE DETECTED!\n"
        f"DAG: {task_instance.dag_id}\n"
        f"Task: {task_instance.task_id}\n"
        f"Execution: {context.get('ds')}\n"
        f"Log Link: {task_instance.log_url}"
    )
    logging.error(err_msg)


# ── DEFAULT ARGS DEFINITION ────────────────────────────────────────────────
default_args = {
    'owner': 'DWH_Team',
    'depends_on_past': False,
    'start_date': datetime(2023, 6, 11),
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': on_failure_callback
}

# ── DAG SPECIFICATION ──────────────────────────────────────────────────────
with DAG(
    dag_id='dw_dwh_abpz_kkm_ail_agent',
    default_args=default_args,
    description='Baut den Flat-File Lookup fuer den View DWH$VI_S_SDM_AGENT_ADS auf.',
    schedule_interval='0 5 * * *',  # Executed daily at 05:00 UTC
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False
) as dag:

    # ── Task: Monitor Start ──
    start_monitoring = PythonOperator(
        task_id="start_monitoring",
        python_callable=mock_monitoring_call,
        op_kwargs={
            "status": "STARTED",
            "execution_date": "{{ ds }}"
        }
    )

    # ── Task: Write Agent ADS Lookup (Dataproc Serverless) ──
    write_agent_ads_lookup = DataprocCreateBatchOperator(
        task_id="write_agent_ads_lookup",
        project_id=GCP_PROJECT,
        region=GCP_REGION,
        batch_id="batch-abpz-kkm-ail-agent-{{ ds_nodash | lower }}",
        batch={
            "pyspark_batch": {
                "main_python_file_uri": PYSPARK_FILE_PATH,
                "args": [
                    "--output_bucket", GCS_BUCKET,
                    "--output_file", LOOKUP_NAME,
                    "--backlook_days", str(BACKLOOK_DAYS),
                    "--project_prefix", PROJECT_PREFIX,
                    "--first_day", f"{{{{ macros.ds_add(ds, -{BACKLOOK_DAYS}) }}}}",
                    "--last_day_plus_1", "{{ tomorrow_ds }}",
                    "--gcp_project", GCP_PROJECT
                ]
            },
            "environment_config": {
                "execution_config": {
                    "subnetwork_uri": "default"
                }
            }
        }
    )

    # ── Task: Monitor End ──
    end_monitoring = PythonOperator(
        task_id="end_monitoring",
        python_callable=mock_monitoring_call,
        op_kwargs={
            "status": "COMPLETED",
            "execution_date": "{{ ds }}"
        },
        trigger_rule=TriggerRule.ALL_SUCCESS
    )

    # ── Task Dependency Graph Definition ──
    start_monitoring >> write_agent_ads_lookup >> end_monitoring