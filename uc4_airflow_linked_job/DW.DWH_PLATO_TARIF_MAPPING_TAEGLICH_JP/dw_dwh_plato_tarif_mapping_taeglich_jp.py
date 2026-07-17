from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.operators.empty import EmptyOperator

# ── GCP CONFIGURATION (GLOBAL Env Policy Classification) ──
GCP_PROJECT = Variable.get("GCP_PROJECT")
DATAPROC_REGION = Variable.get("DATAPROC_REGION")
DATAPROC_CLUSTER = Variable.get("DATAPROC_CLUSTER")
GCS_BUCKET = Variable.get("GCS_BUCKET")

# ── DEFAULT ARGS ─────────────────────────────────────────────────────────────
default_args = {
    "owner": "DW",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

# ── ON_FAILURE_CALLBACK STUBS ────────────────────────────────────────────────
def on_failure_alarm(context):
    """
    Fires on task failure to execute notification.
    Corresponds to UC4 call of DW.CALL_STANDARD with parameter ##911011.
    """
    task_id = context['task_instance'].task_id
    execution_date = context.get('execution_date')
    print(f"ALARM: Task {task_id} failed on {execution_date}. Triggering alarm payload: ##911011")

# ── DAG DEFINITION ───────────────────────────────────────────────────────────
with DAG(
    dag_id="dw_dwh_plato_tarif_mapping_taeglich_jp",
    default_args=default_args,
    description="Täglicher Aufbau der Plato Mapping Tabelle zur Verbindung der Plato und der DWH Basistarife",
    schedule="0 3 * * *",
    start_date=datetime(2026, 3, 30),
    catchup=False,
    max_active_runs=1,  # Corresponds to UC4 Sync Object "Wait" state mapping logic
    is_paused_upon_creation=False,
) as dag:

    # ── Task: Start ──
    start = EmptyOperator(
        task_id="start",
    )

    # ── Task: dw_dwh_dummy_absd_plato_tarife ──
    pyspark_job_dummy = {
        "reference": {"project_id": GCP_PROJECT},
        "placement": {"cluster_name": DATAPROC_CLUSTER},
        "pyspark_job": {
            "main_python_file_uri": f"gs://{GCS_BUCKET}/pyspark_scripts/dw_dwh_dummy_absd_plato_tarife.py",
            "args": []
        }
    }

    dw_dwh_dummy_absd_plato_tarife = DataprocSubmitJobOperator(
        task_id="dw_dwh_dummy_absd_plato_tarife",
        job=pyspark_job_dummy,
        region=DATAPROC_REGION,
        project_id=GCP_PROJECT,
        on_failure_callback=on_failure_alarm,
    )

    # ── Task: End ──
    end = EmptyOperator(
        task_id="end",
    )

    # ── DEPENDENCY PIPELINE ──────────────────────────────────────────────────
    start >> dw_dwh_dummy_absd_plato_tarife >> end