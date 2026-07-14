"""
DAG ID: dw_dwh_exis_ikdb_stamm_r
Description: Migrated from UC4 JOBS_UNIX 'DW.DWH_EXIS_IKDB_STAMM_R' and wrapper 'r_exp_ikdb.ksh'.
             Orchestrates the export of contract master data from IKDB.
             Enforces single execution via max_active_runs=1 and dynamic database execution checks.
"""

from datetime import datetime, timedelta
import logging

from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.providers.google.cloud.hooks.postgres import PostgresHook

# ── 1. ENVIRONMENT CONFIGURATION & VARIABLES ─────────────────────────────────
# Retrieved dynamically at runtime via Airflow variables to prevent hardcoding.
GCP_PROJECT_ID = Variable.get("GCP_PROJECT", default_var="gcp-project-placeholder")
DATAPROC_REGION = Variable.get("GCP_REGION", default_var="europe-west3")
DATAPROC_CLUSTER_NAME = Variable.get("DATAPROC_CLUSTER", default_var="dataproc-cluster-placeholder")
GCS_BUCKET = Variable.get("GCS_BUCKET", default_var="gcs-bucket-placeholder")

# Job-specific constants mapping to legacy parameters
JOB_KEY = "EXIS_IKDB_STAMM_R"
FILE_TYPE = "STAMM_OUT_TMD"
NUMERIC_PARAM = "7"
SQL_SCRIPT = "d_ikdb_exp_stamm.sql"


# ── 2. DEFAULT ARGS & ERROR HANDLING ───────────────────────────────────────
def on_failure_alarm(context):
    """
    On-failure alarm: Triggered on task failure.
    Logs comprehensive traceback information conforming to lineage and auditing rules.
    """
    task_instance = context.get("task_instance")
    task_id = task_instance.task_id
    run_id = context.get("run_id")
    exception = context.get("exception")
    
    logging.error(
        f"[ALERT] Task '{task_id}' failed in DAG run '{run_id}'. "
        f"Exception: {exception}. Initiate support protocol."
    )


def_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'start_date': datetime(2026, 4, 21),
    'retries': 0,  # Retries set to 0 to align with source UC4 XML definition
    'retry_delay': timedelta(minutes=5),
}


# ── 3. REUSABLE METADATA CHECK LOGIC ────────────────────────────────────────
def check_prior_run_status(**context):
    """
    Queries DWTK_MELDUNGEN (via the metadata database hook) to check if an execution 
    for this business date (t-1 logical day) already succeeded with STATUS_NR = '2'.
    """
    # Calculate target logical date (t-1 business day as per legacy code)
    execution_date = context['execution_date']
    target_date_str = (execution_date - timedelta(days=1)).strftime('%Y%m%d')
    
    logging.info(f"Checking prior run status for Job: '{JOB_KEY}' and Target Date: '{target_date_str}'")

    try:
        # Establish connection with metadata audit database (PostgresHook/OracleHook wrapper)
        db_hook = PostgresHook(postgres_conn_id='metadata_db')
        
        sql = """
            SELECT COUNT(1) 
            FROM DWTK_MELDUNGEN 
            WHERE JOB_KENNUNG = %s 
              AND STATUS_NR = '2' 
              AND STICHTAG = TO_DATE(%s, 'YYYYMMDD');
        """
        result = db_hook.get_first(sql, parameters=(JOB_KEY, target_date_str))
        
        if result and result[0] > 0:
            logging.info(f"Exportjob lief bereits am Datum: {target_date_str}. (Already completed).")
            return "skipped_already_run"
        else:
            logging.info(f"Export required for date: {target_date_str}. (Zuweisung erfolgt).")
            return "run_export_ikdb_task"
            
    except Exception as e:
        logging.error(f"Error checking prior metadata runs: {str(e)}. Defaulting to run pipeline.")
        return "run_export_ikdb_task"


# ── 4. DAG WORKFLOW DEFINITION ──────────────────────────────────────────────
# max_active_runs=1 enforces the original Else="Wait" UC4 Sync configuration
with DAG(
    dag_id='dw_dwh_exis_ikdb_stamm_r',
    default_args=def_args, 
    schedule=None,  # No static schedule, triggered dynamically via upstream DAGs
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    tags=['uc4_migration', 'dwh', 'exis', 'ikdb'],
) as dag:

    start_pipeline = EmptyOperator(task_id='start')

    # Branch Operator substituting the legacy bash state checking loop
    check_already_executed = BranchPythonOperator(
        task_id='check_already_executed', 
        python_callable=check_prior_run_status,
        provide_context=True,
    )

    # PySpark Dataproc configuration for core data export
    pyspark_job_args = [
        "--query", SQL_SCRIPT,
        "--job_key", JOB_KEY,
        "--file_type", FILE_TYPE,
        "--numeric_param", NUMERIC_PARAM,
        "--target_date", "{{ (execution_date - macros.timedelta(days=1)).strftime('%Y%m%d') }}"
    ]

    pyspark_export_config = {
        "reference": {"project_id": GCP_PROJECT_ID},
        "placement": {"cluster_name": DATAPROC_CLUSTER_NAME},
        "pyspark_job": {
            "main_python_file_uri": f"gs://{GCS_BUCKET}/pyspark_scripts/exis_ikdb_stamm_r.py",
            "args": pyspark_job_args,
        },
    }

    # Task triggers Dataproc processing job
    run_export_ikdb_task = DataprocSubmitJobOperator(
        task_id='run_export_ikdb_task',
        project_id=GCP_PROJECT_ID,
        region=DATAPROC_REGION,
        job=pyspark_export_config,
        job_id="dw_dwh_exis_ikdb_stamm_r_{{ ds_nodash }}_{{ task_instance.try_number }}",
        on_failure_callback=on_failure_alarm,
    )

    skipped_already_run = EmptyOperator(task_id='skipped_already_run')
    end_pipeline = EmptyOperator(task_id='end')

    # ── 5. PIPELINE DEPENDENCIES ────────────────────────────────────────────
    start_pipeline >> check_already_executed
    
    # Path A: Export has not run yet. Execute processing, then complete.
    check_already_executed >> run_export_ikdb_task >> end_pipeline
    
    # Path B: Export has already been run successfully. Skip execution.
    check_already_executed >> skipped_already_run >> end_pipeline