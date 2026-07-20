"""
Jobplan fuer den taeglichen Stammdatenabgleich Kundennummer/Basiszugang (KNZB) zwischen Quellsystem ISTNS und DWH-Kernschicht. Rein UC4-nativ, keine externen Shell-Aufrufe.

DAG Name: dw_dwh_stamm_knzb_abgl_jp
Schedule: 0 3 * * * (Daily at 03:00 UTC)

This orchestration DAG manages the execution flow of the master data reconciliation process.
It triggers downstream child DAGs to execute the start boundaries and final reconciliation steps,
ensuring consistent sync sequences while maintaining modular job architectures.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

# ─── ENVIRONMENT VALUES (CLASSIFIED BY ROLE) ──────────────────────────────────
# GLOBAL (Environment-Wide Infrastructure Configuration)
# Fetched dynamically from Airflow Variables as per GCP environment standards
GCP_PROJECT = Variable.get("GCP_PROJECT")
GCP_REGION = Variable.get("GCP_REGION")

# JOB-SPECIFIC (Workflow Configuration Options)
DAG_ID = "dw_dwh_stamm_knzb_abgl_jp"
DAG_TITLE = "Taeglicher Abgleich der Kundennummer-/Basiszugangs-Stammdaten (KNZB) in der DWH-Kernschicht"

# ─── ON FAILURE CALLBACK ──────────────────────────────────────────────────────
def on_failure_alarm(context):
    """
    Enterprise alerting execution stub for UC4-style notification parity.
    Triggered when workflow tasks encounter failures.
    """
    task_id = context['task_instance'].task_id
    execution_date = context['execution_date']
    # NOTE: Output/Print Literal Rule applied — preserving exact legacy messaging context
    print(f"Workflow failure on task: {task_id} at {execution_date}")

# ─── DEFAULT ARGS ─────────────────────────────────────────────────────────────
default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# ─── DAG DEFINITION ───────────────────────────────────────────────────────────
with DAG(
    dag_id=DAG_ID,
    description=DAG_TITLE,
    schedule='0 3 * * *',           # Daily execution cadence
    catchup=False,
    max_active_runs=1,             # Prevents concurrent execution of master data reconciliations
    is_paused_upon_creation=False, # Matches source active state = 1
    default_args=default_args,
    tags=['dwh', 'dwh_kern', 'knzb_reconciliation']
) as dag:

    # ─── TASK REPRESENTATIONS ─────────────────────────────────────────────────────

    # Start boundary marker
    start_boundary = EmptyOperator(
        task_id='start'
    )

    # Step 1: Trigger the child START process DAG
    # Corresponds to legacy UC4 object: DW.DWH_STAMM_KNZB_ABGL_START_JS
    trigger_knzb_start = TriggerDagRunOperator(
        task_id='dw_dwh_stamm_knzb_abgl_start_js',
        trigger_dag_id='dw_dwh_stamm_knzb_abgl_start_js',  # Sanitised Airflow DAG ID
        wait_for_completion=True,                          # Explicit synchronous wait
        poke_interval=60,                                  # Production polling interval
        reset_dag_run=True,                                # Ensures re-run safety
        on_failure_callback=on_failure_alarm
    )

    # Step 2: Trigger the child END process DAG
    # Corresponds to legacy UC4 object: DW.DWH_STAMM_KNZB_ABGL_ENDE_JS
    trigger_knzb_ende = TriggerDagRunOperator(
        task_id='dw_dwh_stamm_knzb_abgl_ende_js',
        trigger_dag_id='dw_dwh_stamm_knzb_abgl_ende_js',    # Sanitised Airflow DAG ID
        wait_for_completion=True,                          # Explicit synchronous wait
        poke_interval=60,                                  # Production polling interval
        reset_dag_run=True,                                # Ensures re-run safety
        on_failure_callback=on_failure_alarm
    )

    # End boundary marker
    end_boundary = EmptyOperator(
        task_id='end'
    )

    # ─── DEPENDENCY CHAIN ─────────────────────────────────────────────────────────
    # Sequential, single-lane execution chain mapping legacy UC4 execution grid
    start_boundary >> trigger_knzb_start >> trigger_knzb_ende >> end_boundary