"""
DAG Name: dw_dwh_rechnung_export_taeglich_jp
Source UC4 Object: DW.DWH_RECHNUNG_EXPORT_TAEGLICH_JP (JOBP)

Overview:
This DAG acts as the central orchestration workflow for the daily export of invoice/billing
data ("Rechnungsdaten") from the DWH core layer to an external reporting directory.
Because the source UC4 object is a pure Job Plan (JOBP orchestrator), this DAG manages:
1. Cross-DAG synchronization with upstream dependencies using ExternalTaskSensors.
2. Triggering the downstream PySpark export task (dw_dwh_rechnung_export_taeglich_js)
   running on Google Cloud Dataproc.

Schedule:
- '0 2 * * *' (Daily at 02:00 UTC) as derived from UC4 business requirements.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.trigger_rule import TriggerRule

# ==============================================================================
# GCP CONFIGURATION CONSTANTS (NO PROSE PLACEHOLDERS)
# ==============================================================================
GCP_PROJECT_ID = Variable.get("GCP_PROJECT")
GCP_REGION = Variable.get("GCP_REGION")
GCS_BUCKET = Variable.get("GCS_BUCKET")

# ==============================================================================
# JOB-SPECIFIC PARAMETERS (SANITISED FROM UC4 NAMES)
# ==============================================================================
UPSTREAM_ABRECHNUNG = "dw_dwh_abrechnung_reformat_js"
UPSTREAM_KUNDE = "dw_dwh_kunde_abgl_woechentlich_js"
UPSTREAM_TARIFHIST = "dw_dwh_tarifhist_scd_monatlich_js"
UPSTREAM_UMSATZ = "dw_dwh_umsatz_konsolidierung_monatlich_js"
CHILD_EXPORT_RECHNUNG = "dw_dwh_rechnung_export_taeglich_js"

# ==============================================================================
# DEFAULT ARGS
# ==============================================================================
default_args = {
    'owner': 'dwh_operations',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# ==============================================================================
# ON_FAILURE_CALLBACK STUBS
# ==============================================================================
def on_failure_alarm(context):
    """
    On-failure callback to handle alerts for execution telemetry.
    Retains localized logging/output strings exactly from original execution telemetry.
    """
    task_id = context['task_instance'].task_id
    execution_date = context.get('execution_date') or context.get('logical_date')
    error_msg = context['task_instance'].error
    
    print(f"CRITICAL ALARM: Task {task_id} failed on execution {execution_date}.")
    print(f"Exception details: {error_msg}")

# ==============================================================================
# DAG DEFINITION
# ==============================================================================
with DAG(
    dag_id='dw_dwh_rechnung_export_taeglich_jp',
    default_args=default_args,
    description='Taeglicher Export der Rechnungsdaten (RECHNUNG) aus der DWH-Kernschicht in das Reporting-Verzeichnis',
    schedule='0 2 * * *',
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False  # Deployed active matching source Active Flag of 1
) as dag:

    # ==========================================================================
    # UPSTREAM SENSORS
    # ==========================================================================

    # Sensor for 'DW.DWH_ABRECHNUNG_REFORMAT_JS'
    wait_for_abrechnung_reformat = ExternalTaskSensor(
        task_id='wait_for_abrechnung_reformat',
        external_dag_id=UPSTREAM_ABRECHNUNG,
        external_task_id=None,  # Waits for the entire DAG run to succeed
        allowed_states=['success'],
        check_existence=True,
        execution_delta=timedelta(hours=0),  # Assumes synchronized schedules
        poke_interval=120,
        timeout=7200,
    )

    # Sensor for 'DW.DWH_KUNDE_ABGL_WOECHENTLICH_JS'
    wait_for_kunde_abgl = ExternalTaskSensor(
        task_id='wait_for_kunde_abgl',
        external_dag_id=UPSTREAM_KUNDE,
        external_task_id=None,
        allowed_states=['success'],
        check_existence=True,
        execution_delta=timedelta(hours=0),
        poke_interval=120,
        timeout=7200,
    )

    # Sensor for 'DW.DWH_TARIFHIST_SCD_MONATLICH_JS'
    wait_for_tarifhist_scd = ExternalTaskSensor(
        task_id='wait_for_tarifhist_scd',
        external_dag_id=UPSTREAM_TARIFHIST,
        external_task_id=None,
        allowed_states=['success'],
        check_existence=True,
        execution_delta=timedelta(hours=0),
        poke_interval=120,
        timeout=7200,
    )

    # Sensor for 'DW.DWH_UMSATZ_KONSOLIDIERUNG_MONATLICH_JS'
    wait_for_umsatz_konsolidierung = ExternalTaskSensor(
        task_id='wait_for_umsatz_konsolidierung',
        external_dag_id=UPSTREAM_UMSATZ,
        external_task_id=None,
        allowed_states=['success'],
        check_existence=True,
        execution_delta=timedelta(hours=0),
        poke_interval=120,
        timeout=7200,
    )

    # ==========================================================================
    # DOWNSTREAM ORCHESTRATION / EXECUTION TRIGGER
    # ==========================================================================
    
    # Triggers the already-migrated PySpark extraction child-dag (DW.DWH_RECHNUNG_EXPORT_TAEGLICH_JS)
    trigger_rechnung_export_js = TriggerDagRunOperator(
        task_id='trigger_rechnung_export_js',
        trigger_dag_id=CHILD_EXPORT_RECHNUNG,
        wait_for_completion=True,  # Blocks orchestration DAG until extraction completes
        reset_dag_run=True,        # Clears and re-runs target if it already ran
        poke_interval=60,
        on_failure_callback=on_failure_alarm,
        trigger_rule=TriggerRule.ALL_SUCCESS,  # Strict success extraction constraint
    )

    # ==========================================================================
    # WORKFLOW DEPENDENCIES
    # ==========================================================================
    [
        wait_for_abrechnung_reformat,
        wait_for_kunde_abgl,
        wait_for_tarifhist_scd,
        wait_for_umsatz_konsolidierung
    ] >> trigger_rechnung_export_js