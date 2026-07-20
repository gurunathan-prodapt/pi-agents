"""
DAG Name: dw_dwh_plato_tarif_mapping_taeglich_jp
Description:
    This DAG orchestrates the migrated UC4/Automic pipeline for the Plato tariff mapping daily workflow 
    (originally DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP). It contains a placeholder/dummy execution 
    step mapped from DW.DWH_DUMMY_ABSD_PLATO_TARIFE to preserve the structural orchestration and execution 
    tracing of the legacy DWH system.
Schedule:
    Daily at 05:00 UTC ('0 5 * * *')
Tasks:
    - start: Entry boundary node marking pipeline trigger.
    - dw_dwh_dummy_absd_plato_tarife: Submits a PySpark script to Dataproc that executes the legacy 
      print literal step ("Doing nothinig") and exits cleanly.
    - end: Exit boundary node marking pipeline completion.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator

# ── GCP CONFIGURATION CONSTANTS ──────────────────────────────────────────────
# Runtime variables resolved dynamically via Airflow Variables without prose placeholders
GCP_PROJECT_ID = Variable.get("GCP_PROJECT")
DATAPROC_REGION = Variable.get("GCP_REGION")
DATAPROC_CLUSTER_NAME = Variable.get("DATAPROC_CLUSTER")
GCS_BUCKET = Variable.get("GCS_BUCKET")

# ── DEFAULT ARGS DEFINITION ──────────────────────────────────────────────────
default_args = {
    'owner': 'dw',
    'depends_on_past': False,
    'start_date': datetime(2026, 3, 30),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,                                 # No retries configured in legacy source metadata
    'retry_delay': timedelta(minutes=5),
}

# ── DAG DEFINITION ───────────────────────────────────────────────────────────
with DAG(
    dag_id='dw_dwh_plato_tarif_mapping_taeglich_jp',
    default_args=default_args,
    schedule='0 5 * * *',                         # Daily morning schedule mapped from parent JP metadata
    catchup=False,                                # Standard best practice for data warehousing workflows
    max_active_runs=1,                            # Protects against concurrent run overlap
    is_paused_upon_creation=False,                # Matches Active status ("1") from source XML
) as dag:

    # ── BOUNDARY START TASK ──────────────────────────────────────────────────
    # Marker indicating the start of the daily mapping process
    start = EmptyOperator(
        task_id='start',
    )

    # ── PYSPARK JOB CONFIGURATION ────────────────────────────────────────────
    # Structural configuration for Dataproc submit job execution
    pyspark_job_config = {
        'reference': {
            'project_id': GCP_PROJECT_ID
        },
        'placement': {
            'cluster_name': DATAPROC_CLUSTER_NAME
        },
        'pyspark_job': {
            'main_python_file_uri': f"gs://{GCS_BUCKET}/pyspark_scripts/dw_dwh_dummy_absd_plato_tarife.py"
        }
    }

    # ── DUMMY PLATO TARIF MAPPING STEP ───────────────────────────────────────
    # Submits the placeholder execution command to Cloud Dataproc
    dw_dwh_dummy_absd_plato_tarife = DataprocSubmitJobOperator(
        task_id='dw_dwh_dummy_absd_plato_tarife',
        job=pyspark_job_config,
        region=DATAPROC_REGION,
        project_id=GCP_PROJECT_ID,
        # Dynamic job_id generation to avoid duplicate ID rejections on execution retries
        job_id="{{ dag.dag_id }}_{{ run_id | ts_nodash | lower }}_dummy_absd_plato_tarife"
    )

    # ── BOUNDARY END TASK ────────────────────────────────────────────────────
    # Marker indicating the end of the daily mapping process
    end = EmptyOperator(
        task_id='end',
    )

    # ── TASK DEPENDENCY PIPELINE ─────────────────────────────────────────────
    start >> dw_dwh_dummy_absd_plato_tarife >> end