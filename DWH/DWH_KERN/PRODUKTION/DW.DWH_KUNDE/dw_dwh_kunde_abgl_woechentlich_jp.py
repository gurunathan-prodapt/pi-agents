"""
DAG: dw_dwh_kunde_abgl_woechentlich_jp

Overview:
This DAG orchestrates the weekly customer master data address comparison against 
a reference system (originally UC4 Job Plan 'DW.DWH_KUNDE_ABGL_WOECHENTLICH_JP').
Following a pure orchestration migration pattern, it uses ExternalTaskSensors to verify 
upstream dependencies and then triggers the downstream reconciliation execution job 
using a TriggerDagRunOperator.

Schedule:
- Weekly on Mondays at 03:00 AM UTC (0 3 * * 1)

Tasks:
- wait_for_dw_dwh_abrechnung_reformat_js: Sensor waiting for the billing reformat stage.
- wait_for_dw_dwh_rechnung_export_taeglich_js: Sensor waiting for the daily billing export.
- wait_for_dw_dwh_tarifhist_scd_monatlich_js: Sensor waiting for the monthly tariff history updates.
- wait_for_dw_dwh_umsatz_konsolidierung_monatlich_js: Sensor waiting for revenue consolidation.
- start: Start boundary empty marker.
- dw_dwh_kunde_abgl_woechentlich_js: Trigger Dag Run task executing the actual PySpark reconciliation.
- end: End boundary empty marker.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.external_task import ExternalTaskSensor

# ==============================================================================
# ── GCP CONFIGURATION CONSTANTS ───────────────────────────────────────────────
# ==============================================================================
# All configuration values are loaded dynamically from Airflow Variables.
GCP_PROJECT_ID = Variable.get("GCP_PROJECT")
DATAPROC_REGION = Variable.get("DATAPROC_REGION", default_var="europe-west3")
DATAPROC_CLUSTER_NAME = Variable.get("DATAPROC_CLUSTER", default_var="dwh-dataproc-cluster")
GCS_BUCKET = Variable.get("GCS_BUCKET")

# ==============================================================================
# ── ON FAILURE CALLBACKS ──────────────────────────────────────────────────────
# ==============================================================================
def on_failure_alarm(context):
    """
    Callback function that triggers on task failure.
    Sends alerting/monitoring notifications (e.g. Email, Slack, PagerDuty).
    """
    task_id = context.get('task_instance').task_id
    execution_date = context.get('execution_date')
    log_url = context.get('task_instance').log_url
    
    print(f"ALERT: Task {task_id} failed for execution date {execution_date}. Logs: {log_url}")

# ==============================================================================
# ── DEFAULT ARGS ──────────────────────────────────────────────────────────────
# ==============================================================================
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=15),
}

# ==============================================================================
# ── DAG DEFINITION ────────────────────────────────────────────────────────────
# ==============================================================================
with DAG(
    dag_id='dw_dwh_kunde_abgl_woechentlich_jp',
    default_args=default_args,
    description='Woechentlicher Adressabgleich der Kundenstammdaten (KUNDE) gegen das Referenzsystem',
    schedule='0 3 * * 1',  # Weekly on Mondays at 03:00 AM
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,  # Retains source active state (Active=1)
) as dag:

    # ==============================================================================
    # ── SENSORS (UPSTREAM DEPENDENCIES) ───────────────────────────────────────────
    # ==============================================================================

    # Sensor monitoring the Billing Reformat stage
    wait_for_abrechnung_reformat = ExternalTaskSensor(
        task_id='wait_for_dw_dwh_abrechnung_reformat_js',
        external_dag_id='dw_dwh_abrechnung_reformat_js',
        external_task_id='end',
        allowed_states=['success'],
        mode='reschedule',
        poke_interval=300,
        timeout=7200,
    )

    # Sensor monitoring the Daily Billing Export stage
    wait_for_rechnung_export_taeglich = ExternalTaskSensor(
        task_id='wait_for_dw_dwh_rechnung_export_taeglich_js',
        external_dag_id='dw_dwh_rechnung_export_taeglich_js',
        external_task_id='end',
        allowed_states=['success'],
        mode='reschedule',
        poke_interval=300,
        timeout=7200,
    )

    # Sensor monitoring the Monthly Tariff History SCD updates
    wait_for_tarifhist_scd_monatlich = ExternalTaskSensor(
        task_id='wait_for_dw_dwh_tarifhist_scd_monatlich_js',
        external_dag_id='dw_dwh_tarifhist_scd_monatlich_js',
        external_task_id='end',
        allowed_states=['success'],
        mode='reschedule',
        poke_interval=600,
        timeout=14400,
    )

    # Sensor monitoring the Consolidated Revenue matching stage
    wait_for_umsatz_konsolidierung = ExternalTaskSensor(
        task_id='wait_for_dw_dwh_umsatz_konsolidierung_monatlich_js',
        external_dag_id='dw_dwh_umsatz_konsolidierung_monatlich_js',
        external_task_id='end',
        allowed_states=['success'],
        mode='reschedule',
        poke_interval=600,
        timeout=14400,
    )

    # ==============================================================================
    # ── BOUNDARY MARKERS & EXECUTION TASKS ────────────────────────────────────────
    # ==============================================================================

    # Start boundary empty operator
    start_task = EmptyOperator(
        task_id='start',
    )

    # Trigger Dag Run for the primary execution logic
    # Sanitised Airflow DAG ID is used, wait_for_completion=True, and poke_interval=60 set.
    trigger_kunde_abgleich = TriggerDagRunOperator(
        task_id='dw_dwh_kunde_abgl_woechentlich_js',
        trigger_dag_id='dw_dwh_kunde_abgl_woechentlich_js',
        wait_for_completion=True,
        poke_interval=60,
        reset_dag_run=True,
        on_failure_callback=on_failure_alarm,
    )

    # End boundary empty operator
    end_task = EmptyOperator(
        task_id='end',
    )

    # ==============================================================================
    # ── DEPENDENCY GRAPH ──────────────────────────────────────────────────────────
    # ==============================================================================
    [
        wait_for_abrechnung_reformat,
        wait_for_rechnung_export_taeglich,
        wait_for_tarifhist_scd_monatlich,
        wait_for_umsatz_konsolidierung
    ] >> start_task >> trigger_kunde_abgleich >> end_task