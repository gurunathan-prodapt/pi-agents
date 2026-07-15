"""
Cloud Composer DAG for DW.DWH_ABPZ_KKM_AIL_AGENT
Orchestrates the verification, processing window calculation, and Dataproc execution.
"""

from datetime import datetime, timedelta
import logging
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator

# Configure local task logging standard
logger = logging.getLogger("airflow.task")

# 1. Environment Variable Retrieval via Airflow Config Store
GCP_PROJECT = Variable.get("GCP_PROJECT")
GCS_BUCKET = Variable.get("GCS_BUCKET")
DATAPROC_REGION = Variable.get("DATAPROC_REGION")
DATAPROC_CLUSTER = Variable.get("DATAPROC_CLUSTER")

# 2. Retain original job variables for tracing
DWH_JOB_KENNUNG = "ABPZ_KKM_AIL_AGENT"
LOOKBACK_DAYS = 84

default_args = {
    'owner': 'dwh_operator',
    'start_date': datetime(2026, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


# --- Python Callback Functions (Replaces KornShell utility scripts) ---

def handle_job_monitor_start():
    """Replaces legacy 'DW.DWH_ADM_JOB_MONITOR_START' and Start Hook."""
    print(f"Jobkennung {DWH_JOB_KENNUNG} eingetragen")
    print("Die Ab Initio Verarbeitung ist gestartet.")


def calculate_processing_window(ds, **kwargs):
    """
    Replaces legacy 'h_alis_date.ksh' logic.
    Calculates operational lookback range matching '-z 84' configuration.
    """
    execution_date = datetime.strptime(ds, "%Y-%m-%d")
    
    # Calculate FirstDay and LastDayPlus1
    first_day = (execution_date - timedelta(days=LOOKBACK_DAYS)).strftime("%Y%m%d")
    last_day_plus_1 = execution_date.strftime("%Y%m%d")
    
    logger.info(f"Calculated lookback window: FirstDay={first_day} | LastDayPlus1={last_day_plus_1}")
    
    # Push parameters to XCom for Dataproc Task ingestion
    ti = kwargs['ti']
    ti.xcom_push(key='first_day', value=first_day)
    ti.xcom_push(key='last_day_plus_1', value=last_day_plus_1)


def handle_job_monitor_end():
    """Replaces legacy 'DW.DWH_ADM_JOB_MONITOR_END' and End Hook status checks."""
    print("Die Ab Initio Verarbeitung ist fertig.")
    print("Die Abarbeitung des Rahmenskriptes wurde ohne erkennbare Fehler mit Rueckgabewert 0 beendet.")


# --- DAG Declaration ---

with DAG(
    dag_id='dw_dwh_abpz_kkm_ail_agent',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    max_active_runs=1,
    tags=['dwh', 'kkm', 'agent'],
) as dag:

    # Task 1: Initialization monitoring log
    start_monitor = PythonOperator(
        task_id='dw_dwh_adm_job_monitor_start',
        python_callable=handle_job_monitor_start,
    )

    # Task 2: Parameter preparation
    date_calculation = PythonOperator(
        task_id='calculate_processing_window',
        python_callable=calculate_processing_window,
        provide_context=True,
    )

    # PySpark Job Definition Config
    pyspark_job_config = {
        "reference": {"project_id": GCP_PROJECT},
        "placement": {"cluster_name": DATAPROC_CLUSTER},
        "pyspark_job": {
            "main_python_file_uri": f"gs://{GCS_BUCKET}/dags/scripts/dataproc/write_agent_ads_lookup.py",
            "environment_variables": {
                "GCP_PROJECT": GCP_PROJECT,
                "GCS_BUCKET": GCS_BUCKET,
                "BHB_CCM_PROC_FirstDay": (
                    "{{ task_instance.xcom_pull(task_ids='calculate_processing_window', key='first_day') }}"
                ),
                "BHB_CCM_PROC_LastDayPlus1": (
                    "{{ task_instance.xcom_pull(task_ids='calculate_processing_window', key='last_day_plus_1') }}"
                ),
            }
        },
    }

    # Task 3: Spark Job Execution on Dataproc
    submit_pyspark = DataprocSubmitJobOperator(
        task_id='run_write_agent_ads_lookup',
        job=pyspark_job_config,
        region=DATAPROC_REGION,
        project_id=GCP_PROJECT,
    )

    # Task 4: Finalization monitoring log
    end_monitor = PythonOperator(
        task_id='dw_dwh_adm_job_monitor_end',
        python_callable=handle_job_monitor_end,
    )

    # Execution Sequence Flow
    start_monitor >> date_calculation >> submit_pyspark >> end_monitor