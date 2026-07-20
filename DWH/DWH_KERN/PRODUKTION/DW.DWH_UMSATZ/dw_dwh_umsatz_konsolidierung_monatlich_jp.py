"""
DAG: dw_dwh_umsatz_konsolidierung_monatlich_jp

Overview:
This DAG orchestrates the monthly consolidation of revenue data (UMSATZ) across all 
corporate entities. It operates as a master job plan wrapped around a legacy execution 
script (originally migrated from an Ab Initio graph structure).

Schedule:
- '0 0 1 * *': Runs monthly at midnight on the 1st of every month.

German Title / Legacy Documentation:
- "Monatliche Konsolidierung der Umsatzdaten (UMSATZ) ueber alle Konzerngesellschaften"
- "Jobplan zur monatlichen Konsolidierung der Umsatzdaten ueber alle Konzerngesellschaften. 
   Ruft ein Legacy-Ab-Initio-Graph auf, das noch aus der Erstmigration stammt."

Tasks:
1. start: Dummy execution start node.
2. Upstream Sensors:
   - sensor_dw_dwh_abrechnung_reformat_js
   - sensor_dw_dwh_kunde_abgl_woechentlich_js
   - sensor_dw_dwh_rechnung_export_taeglich_js
   - sensor_dw_dwh_tarifhist_scd_monatlich_js
   These sensors monitor the completion of upstream jobs required before revenue consolidation.
3. dw_dwh_umsatz_konsolidierung_monatlich_js: Submits the PySpark job to Dataproc that 
   runs the consolidation process.
4. end: Dummy execution end node.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator

try:
    from airflow.operators.empty import EmptyOperator
except ImportError:
    from airflow.operators.dummy import DummyOperator as EmptyOperator

# ─── GCP CONFIGURATION ────────────────────────────────────────────────────────
# Fetch global runtime variables from Airflow (Hard Rule 5: No fallback prose placeholders)
GCP_PROJECT_ID = Variable.get("GCP_PROJECT")
DATAPROC_REGION = Variable.get("GCP_REGION")
DATAPROC_CLUSTER = Variable.get("DATAPROC_CLUSTER")
GCS_BUCKET = Variable.get("GCS_BUCKET")

# ─── ON_FAILURE_CALLBACK STUBS ────────────────────────────────────────────────
def on_failure_alarm(context):
    """
    Alerting logic on task execution failure.
    Retrieves execution details from context and logs/sends notifications.
    """
    ti = context.get('task_instance')
    task_id = ti.task_id
    run_id = context.get('run_id')
    print(f"Task {task_id} inside run {run_id} failed. Sending alarm notification.")

# ─── DEFAULT ARGS ─────────────────────────────────────────────────────────────
DEFAULT_ARGS = {
    'owner': 'airflow',
    'start_date': datetime(2026, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': on_failure_alarm,
}

# ─── DAG DEFINITION ───────────────────────────────────────────────────────────
with DAG(
    dag_id="dw_dwh_umsatz_konsolidierung_monatlich_jp",
    default_args=DEFAULT_ARGS,
    schedule="0 0 1 * *",  # Runs monthly at midnight on the 1st
    catchup=False,
    max_active_runs=1,     # Replicates UC4 sync object behavior
    is_paused_upon_creation=False,
    doc_md=__doc__,
) as dag:

    # ─── START & END NODES ────────────────────────────────────────────────────
    start = EmptyOperator(
        task_id="start",
    )

    end = EmptyOperator(
        task_id="end",
    )

    # ─── UPSTREAM CROSS-DAG SENSORS ───────────────────────────────────────────
    sensor_dw_dwh_abrechnung_reformat_js = ExternalTaskSensor(
        task_id="sensor_dw_dwh_abrechnung_reformat_js",
        external_dag_id="dw_dwh_abrechnung_reformat_js",
        external_task_id=None,  # Senses the overall DAG success status
        allowed_states=["success"],
        mode="poke",
        poke_interval=60,
        timeout=3600,
    )

    sensor_dw_dwh_kunde_abgl_woechentlich_js = ExternalTaskSensor(
        task_id="sensor_dw_dwh_kunde_abgl_woechentlich_js",
        external_dag_id="dw_dwh_kunde_abgl_woechentlich_js",
        external_task_id=None,
        allowed_states=["success"],
        mode="poke",
        poke_interval=60,
        timeout=3600,
    )

    sensor_dw_dwh_rechnung_export_taeglich_js = ExternalTaskSensor(
        task_id="sensor_dw_dwh_rechnung_export_taeglich_js",
        external_dag_id="dw_dwh_rechnung_export_taeglich_js",
        external_task_id=None,
        allowed_states=["success"],
        mode="poke",
        poke_interval=60,
        timeout=3600,
    )

    sensor_dw_dwh_tarifhist_scd_monatlich_js = ExternalTaskSensor(
        task_id="sensor_dw_dwh_tarifhist_scd_monatlich_js",
        external_dag_id="dw_dwh_tarifhist_scd_monatlich_js",
        external_task_id=None,
        allowed_states=["success"],
        mode="poke",
        poke_interval=60,
        timeout=3600,
    )

    # ─── CORE PROCESSING TASKS ────────────────────────────────────────────────
    dw_dwh_umsatz_konsolidierung_monatlich_js = DataprocSubmitJobOperator(
        task_id="dw_dwh_umsatz_konsolidierung_monatlich_js",
        project_id=GCP_PROJECT_ID,
        region=DATAPROC_REGION,
        job={
            "reference": {
                "project_id": GCP_PROJECT_ID,
                "job_id": "{{ dag.dag_id }}_{{ run_id | ts_nodash | replace('+', '_') | replace(':', '_') | replace('.', '_') }}_dw_dwh_ums_kons_mon_js"
            },
            "placement": {
                "cluster_name": DATAPROC_CLUSTER
            },
            "pyspark_job": {
                "main_python_file_uri": f"gs://{GCS_BUCKET}/pyspark_scripts/dw_dwh_umsatz_konsolidierung_monatlich_js.py",
                "args": [
                    "--job_name", "dw_dwh_umsatz_konsolidierung_monatlich_js",
                    "--execution_date", "{{ ds }}"
                ]
            }
        },
        wait_for_completion=True,
    )

    # ─── DEPENDENCIES ─────────────────────────────────────────────────────────
    start >> [
        sensor_dw_dwh_abrechnung_reformat_js,
        sensor_dw_dwh_kunde_abgl_woechentlich_js,
        sensor_dw_dwh_rechnung_export_taeglich_js,
        sensor_dw_dwh_tarifhist_scd_monatlich_js
    ]

    [
        sensor_dw_dwh_abrechnung_reformat_js,
        sensor_dw_dwh_kunde_abgl_woechentlich_js,
        sensor_dw_dwh_rechnung_export_taeglich_js,
        sensor_dw_dwh_tarifhist_scd_monatlich_js
    ] >> dw_dwh_umsatz_konsolidierung_monatlich_js

    dw_dwh_umsatz_konsolidierung_monatlich_js >> end