"""
DAG: dw_dwh_run_iar_bgf_gutschrift_import_jp_evt
Description: File Event Sensor DAG for BGF Gutschrift Import.
             Monitors for the arrival of credit memo chk files via GCS,
             verifies downstream pipeline execution state, and triggers it.
"""

import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectsWithPrefixPatternSensor
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.exceptions import AirflowSkipException
from airflow.models import DagRun, Variable
from airflow.utils.state import State

# ── GLOBAL CONFIGURATION (Sourced at runtime) ─────────────────────────────────
GCP_PROJECT = os.environ.get("GCP_PROJECT")
GCS_BUCKET = Variable.get("GCS_BUCKET")
SFTP_CONN_ID = Variable.get("SFTP_CONN_ID", default_var="sftp_dwh_conn")

# ── ON FAILURE CALLBACKS ──────────────────────────────────────────────────────
def on_failure_alarm(context):
    """
    Standard alerting stub mapping to the legacy CallOP behavior.
    """
    # Original UC4 behaviour called: DW.CALL_STANDARD
    pass

# ── HELPER FUNCTIONS ──────────────────────────────────────────────────────────
def check_target_active(**context):
    """
    Checks if the downstream processing DAG is currently running.
    Ensures single execution safety and satisfies UC4 log literal output rules.
    """
    target_dag_id = "dw_dwh_iar_bgf_gutschrift_import_jp"
    active_runs = DagRun.find(dag_id=target_dag_id, state=State.RUNNING)
    
    if active_runs:
        # OUTPUT/PRINT LITERAL RULE
        print("Jobplan DW.DWH_IAR_BGF_GUTSCHRIFT_IMPORT_JP is active!")
        raise AirflowSkipException("Target DAG is already running — skipping trigger execution.")
    else:
        # OUTPUT/PRINT LITERAL RULE
        print("Starting Jobplan DW.DWH_IAR_BGF_GUTSCHRIFT_IMPORT_JP ...")

def log_trigger_success(**context):
    """
    Logs trigger success utilizing the legacy print syntax resolved dynamically.
    """
    # date: SYS_DATE('JJJJMMTT') dynamically mapped using Jinja-like python execution context
    date_str = context['logical_date'].strftime('%Y%m%d')
    # OUTPUT/PRINT LITERAL RULE
    print(f"JP started at {date_str} ...")


# ── DEFAULT ARGS ─────────────────────────────────────────────────────────────
DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": on_failure_alarm,
}

# ── DAG DEFINITION ────────────────────────────────────────────────────────────
with DAG(
    dag_id="dw_dwh_run_iar_bgf_gutschrift_import_jp_evt",
    default_args=DEFAULT_ARGS,
    description="File Event Sensor DAG for BGF Gutschrift Import",
    start_date=datetime(2023, 1, 1),
    schedule="*/30 * * * *",  # Polling interval of 30 minutes in original UC4
    catchup=False,
    max_active_runs=1,
    tags=["uc4_migration", "file_event", "bgf"],
) as dag:

    # Task 1: Sense import file in GCS
    # Matches wildcard pattern: DWHK_DWHM_IAR_GUTSCHR_*.chk
    sense_import_file = GCSObjectsWithPrefixPatternSensor(
        task_id="sense_import_file",
        bucket=GCS_BUCKET,
        prefix="landing/",
        pattern="DWHK_DWHM_IAR_GUTSCHR_*.chk",
        poke_interval=1800,  # Polls every 30 minutes (equivalent to TimePeriodTT 0030)
        timeout=86400,       # Fails after 24 hours of waiting
        mode="poke",
    )

    # Task 2: Check active state of target downstream workflow
    check_target_active_task = PythonOperator(
        task_id="check_target_active",
        python_callable=check_target_active,
    )

    # Task 3: Trigger downstream workflow
    trigger_downstream_workflow = TriggerDagRunOperator(
        task_id="trigger_downstream_workflow",
        trigger_dag_id="dw_dwh_iar_bgf_gutschrift_import_jp",
        # wait_for_completion=False is set intentionally as the downstream workflow is designed to be fire-and-forget
        wait_for_completion=False,
        reset_dag_run=True,
        check_existence=True,
    )

    # Task 4: Log successful execution invocation
    log_success_task = PythonOperator(
        task_id="log_trigger_success",
        python_callable=log_trigger_success,
    )

    # ── DEPENDENCIES ──────────────────────────────────────────────────────────
    sense_import_file >> check_target_active_task >> trigger_downstream_workflow >> log_success_task