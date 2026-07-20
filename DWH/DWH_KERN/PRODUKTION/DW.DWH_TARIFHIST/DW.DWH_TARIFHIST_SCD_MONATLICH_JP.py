"""
DAG: dw_dwh_tarifhist_scd_monatlich_jp
Schedule: 0 3 1 * * (Monthly execution on the 1st of the month at 03:00 AM)

Overview:
This is an orchestration-only Airflow DAG representing the monthly UC4 Job Plan (JOBP)
for the monthly history tracking of tariff assignments (SCD Type 2 merge) within the
'DWH_ZIEL' target schema. 

Instead of running the PySpark job directly within this parent plan, it coordinates
external execution dependencies using ExternalTaskSensors and triggers the child execution
DAG 'dw_dwh_tarifhist_scd_monatlich_js' via TriggerDagRunOperator once all upstream
conditions are satisfied.

Tasks:
- start: Technical workflow boundary start.
- wait_for_abrechnung_reformat: Sensor waiting for the billing reformatting DAG.
- wait_for_kunde_abgl: Sensor waiting for the weekly client reconciliation DAG.
- wait_for_rechnung_export: Sensor waiting for the daily invoice export DAG.
- wait_for_umsatz_konsolidierung: Sensor waiting for the monthly revenue consolidation DAG.
- dw_dwh_tarifhist_scd_monatlich_js: Triggers the migrated child execution DAG.
- end: Technical workflow boundary end.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.external_task import ExternalTaskSensor

# --- GLOBAL CONFIGURATION (RESOLVED AT RUNTIME) ---
GCP_PROJECT_ID = Variable.get("GCP_PROJECT")
DATAPROC_REGION = Variable.get("GCP_REGION")
GCS_BUCKET = Variable.get("GCS_BUCKET")

# --- DEFAULT ARGUMENTS ---
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# --- ALARM/FAILURE CALLBACKS ---
def on_failure_alarm(context):
    """
    Standard failure callback function to trigger alerting.
    """
    dag_id = context['task_instance'].dag_id
    task_id = context['task_instance'].task_id
    execution_date = context['execution_date']
    error = context.get('exception')
    
    print(f"ALERT: Task {task_id} in DAG {dag_id} failed on {execution_date}. Error: {error}")

# --- DAG DEFINITION ---
dag = DAG(
    dag_id='dw_dwh_tarifhist_scd_monatlich_jp',
    default_args=default_args,
    description='Monatliche Historisierung der Tarifhistorie via SCD-Typ-2-Merge (Orchestration DAG)',
    schedule='0 3 1 * *',          # Every 1st of the month at 03:00 AM
    catchup=False,
    max_active_runs=1,             # Aligns with UC4 sync object behavior
    is_paused_upon_creation=False, # Replicates Active=1 from UC4 definition
)

# --- TASKS ---

# Technical start boundary
start = EmptyOperator(
    task_id='start',
    dag=dag,
)

# External Upstream Sensor: DW.DWH_ABRECHNUNG_REFORMAT_JS
sensor_abrechnung_reformat = ExternalTaskSensor(
    task_id='wait_for_abrechnung_reformat',
    external_dag_id='dw_dwh_abrechnung_reformat_js',
    external_task_id='end',
    allowed_states=['success'],
    execution_delta=timedelta(0),
    poke_interval=300,
    timeout=86400,
    on_failure_callback=on_failure_alarm,
    dag=dag,
)

# External Upstream Sensor: DW.DWH_KUNDE_ABGL_WOECHENTLICH_JS
sensor_kunde_abgl = ExternalTaskSensor(
    task_id='wait_for_kunde_abgl',
    external_dag_id='dw_dwh_kunde_abgl_woechentlich_js',
    external_task_id='end',
    allowed_states=['success'],
    execution_delta=timedelta(0),
    poke_interval=300,
    timeout=86400,
    on_failure_callback=on_failure_alarm,
    dag=dag,
)

# External Upstream Sensor: DW.DWH_RECHNUNG_EXPORT_TAEGLICH_JS
sensor_rechnung_export = ExternalTaskSensor(
    task_id='wait_for_rechnung_export',
    external_dag_id='dw_dwh_rechnung_export_taeglich_js',
    external_task_id='end', 
    allowed_states=['success'],
    execution_delta=timedelta(0),
    poke_interval=300,
    timeout=86400,
    on_failure_callback=on_failure_alarm,
    dag=dag,
)

# External Upstream Sensor: DW.DWH_UMSATZ_KONSOLIDIERUNG_MONATLICH_JS
sensor_umsatz_konsolidierung = ExternalTaskSensor(
    task_id='wait_for_umsatz_konsolidierung',
    external_dag_id='dw_dwh_umsatz_konsolidierung_monatlich_js',
    external_task_id='end',
    allowed_states=['success'],
    execution_delta=timedelta(0),
    poke_interval=300,
    timeout=86400,
    on_failure_callback=on_failure_alarm,
    dag=dag,
)

# Trigger Task for Separately Migrated Child Execution DAG (DW.DWH_TARIFHIST_SCD_MONATLICH_JS)
trigger_tarifhist_scd_js = TriggerDagRunOperator(
    task_id='dw_dwh_tarifhist_scd_monatlich_js',
    trigger_dag_id='dw_dwh_tarifhist_scd_monatlich_js',
    wait_for_completion=True,
    poke_interval=60,
    on_failure_callback=on_failure_alarm,
    dag=dag,
)

# Technical end boundary
end = EmptyOperator(
    task_id='end',
    dag=dag,
)

# --- DEPENDENCY GRAPH ---
start >> [
    sensor_abrechnung_reformat, 
    sensor_kunde_abgl, 
    sensor_rechnung_export, 
    sensor_umsatz_konsolidierung
] >> trigger_tarifhist_scd_js >> end