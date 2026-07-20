"""
DAG ID: dw_dwh_abrechnung_reformat_jp

Overview:
    This DAG is a daily translation of the billing and settlement data (Abrechnungsdaten)
    reformatting job plan (DW.DWH_ABRECHNUNG_REFORMAT_JP). It acts as an orchestrator 
    wrapper that monitors multiple upstream dependencies via ExternalTaskSensors, 
    and upon successful completion of those upstreams, triggers the child workflow 
    (dw_dwh_abrechnung_reformat_js) which executes the core billing data reformatting logic.

Schedule:
    - Cron: '0 2 * * *' (Daily at 02:00 AM)
    - Catchup: False
    - Max Active Runs: 1 (to replicate UC4 sync object behavior and prevent overlaps)

Tasks:
    - Upstream Sensors:
        1. sensor_kunde_abgl_woechentlich: Monitors weekly customer reconciliation.
        2. sensor_rechnung_export_taeglich: Monitors daily invoice export.
        3. sensor_tarifhist_scd_monatlich: Monitors monthly tariff history SCD.
        4. sensor_umsatz_konsolidierung_monatlich: Monitors monthly sales consolidation.
    - Start Anchor: EmptyOperator marking the logical initiation.
    - Trigger Task (trigger_dw_dwh_abrechnung_reformat_js): Triggers the child DAG
      containing the actual legacy Perl/PySpark billing reformat execution.
    - End Anchor: EmptyOperator marking workflow finalization.
"""

from datetime import datetime, timedelta
import os

from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.external_task import ExternalTaskSensor

# ==============================================================================
# RUNTIME ENVIRONMENT CONFIGURATION (GLOBAL & JOB-SPECIFIC)
# ==============================================================================
# Sourced dynamically from Airflow Variables with failover checks
GCP_PROJECT_ID = Variable.get("GCP_PROJECT", default_var=os.environ.get("GCP_PROJECT"))
GCP_REGION = Variable.get("GCP_REGION", default_var=os.environ.get("GCP_REGION", "europe-west3"))
GCS_BUCKET_NAME = Variable.get("GCS_BUCKET", default_var=os.environ.get("GCS_BUCKET"))

# Target child DAG ID to trigger
CHILD_DAG_ID = "dw_dwh_abrechnung_reformat_js"

# ==============================================================================
# DEFAULT ARGUMENTS
# ==============================================================================
DEFAULT_ARGS = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# ==============================================================================
# DAG DEFINITION
# ==============================================================================
with DAG(
    dag_id='dw_dwh_abrechnung_reformat_jp',
    default_args=DEFAULT_ARGS,
    description='Jobplan zum taeglichen Reformatieren der Abrechnungsdaten fuer den Downstream-Feed. '
                'Ruft ein Legacy-Perl-Script auf, das noch aus der Erstmigration stammt.',
    schedule='0 2 * * *',
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False
) as dag:

    # ==========================================================================
    # SENSORS (UPSTREAM DEPENDENCY MONITORING)
    # ==========================================================================

    # Sensor monitoring the weekly customer reconciliation pipeline
    sensor_kunde_abgl = ExternalTaskSensor(
        task_id='sensor_kunde_abgl_woechentlich',
        external_dag_id='dw_dwh_kunde_abgl_woechentlich_js',
        external_task_id='end',
        allowed_states=['success'],
        execution_delta=timedelta(hours=0),
        mode='reschedule',
        poke_interval=300,
        timeout=3600,
    )

    # Sensor monitoring the daily invoice export pipeline
    sensor_rechnung_export = ExternalTaskSensor(
        task_id='sensor_rechnung_export_taeglich',
        external_dag_id='dw_dwh_rechnung_export_taeglich_js',
        external_task_id='end',
        allowed_states=['success'],
        execution_delta=timedelta(hours=0),
        mode='reschedule',
        poke_interval=300,
        timeout=3600,
    )

    # Sensor monitoring the monthly SCD tariff history pipeline
    sensor_tarifhist_scd = ExternalTaskSensor(
        task_id='sensor_tarifhist_scd_monatlich',
        external_dag_id='dw_dwh_tarifhist_scd_monatlich_js',
        external_task_id='end',
        allowed_states=['success'],
        execution_delta=timedelta(hours=0),
        mode='reschedule',
        poke_interval=300,
        timeout=3600,
    )

    # Sensor monitoring the monthly sales consolidation pipeline
    sensor_umsatz_konsol = ExternalTaskSensor(
        task_id='sensor_umsatz_konsolidierung_monatlich',
        external_dag_id='dw_dwh_umsatz_konsolidierung_monatlich_js',
        external_task_id='end',
        allowed_states=['success'],
        execution_delta=timedelta(hours=0),
        mode='reschedule',
        poke_interval=300,
        timeout=3600,
    )

    # ==========================================================================
    # TASKS DEFINITION
    # ==========================================================================

    # Workflow Start Anchor (UC4 <START> object)
    start = EmptyOperator(
        task_id='start'
    )

    # Trigger Task to execute the child workflow containing the actual Perl reformatting logic
    trigger_reformat_js = TriggerDagRunOperator(
        task_id='trigger_dw_dwh_abrechnung_reformat_js',
        trigger_dag_id=CHILD_DAG_ID,
        wait_for_completion=True,
        poke_interval=60,
        reset_dag_run=True,
    )

    # Workflow End Anchor (UC4 <END> object)
    end = EmptyOperator(
        task_id='end'
    )

    # ==========================================================================
    # DEPENDENCIES
    # ==========================================================================
    # Upstream cross-DAG dependencies must complete before start is processed,
    # followed by the trigger task, and finally closing at the end anchor.
    [
        sensor_kunde_abgl,
        sensor_rechnung_export,
        sensor_tarifhist_scd,
        sensor_umsatz_konsol
    ] >> start >> trigger_reformat_js >> end