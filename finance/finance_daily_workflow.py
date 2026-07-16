from datetime import datetime, timedelta
import logging
import os

from airflow import DAG
from airflow.models import Variable, DagRun
from airflow.utils.state import State
from airflow.exceptions import AirflowSkipException
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

# ==============================================================================
# ENVIRONMENT & INFRASTRUCTURE CONFIGURATION (GLOBAL CONFIG)
# ==============================================================================
GCP_PROJECT_ID = os.environ.get("GCP_PROJECT", "YOUR_GCP_PROJECT_ID")
DATAPROC_REGION = os.environ.get("DATAPROC_REGION", "YOUR_DATAPROC_REGION")
DATAPROC_CLUSTER = os.environ.get("DATAPROC_CLUSTER", "YOUR_DATAPROC_CLUSTER_NAME")
GCS_BUCKET = Variable.get("gcs_bucket_name", default_var="YOUR_BUCKET_NAME")

SPARK_SCRIPTS_PATH = f"gs://{GCS_BUCKET}/pyspark_scripts"

# ==============================================================================
# JOB-SPECIFIC PARAMETER MAPPING & VARIABLES
# ==============================================================================
FINANCE_NOTIFY_EMAIL = Variable.get("finance_notify_email", default_var="finance-etl@company.com")
FINANCE_RETRY_MAX = int(Variable.get("finance_retry_max", default_var="3"))
ALLOW_EMPTY = Variable.get("finance_allow_empty", default_var="N")

# ==============================================================================
# ERROR HANDLING & ALARM CALLBACK
# ==============================================================================
def on_failure_alarm(context):
    """
    Unified failure callback to handle critical alerts.
    Fires immediate warning signals to operations.
    """
    task_instance = context.get('task_instance')
    dag_id = context.get('dag').dag_id
    logical_date = context.get('ds')
    
    # Literal print of the system log matching original source instructions
    print(f"ALERT: Task {task_instance.task_id} inside {dag_id} failed on {logical_date}. Notification dispatched.")

# ==============================================================================
# DEFAULT TEMPLATED ARGUMENTS
# ==============================================================================
DEFAULT_ARGS = { 
    'owner': 'finance_etl',
    'start_date': datetime(2024, 1, 1),
    'email': [FINANCE_NOTIFY_EMAIL, 'dw-alerts@company.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(seconds=60)
}

# ==============================================================================
# DAG DECLARATION
# ==============================================================================
with DAG(
    dag_id='finance_daily_workflow',
    schedule_interval='0 1 * * 1-5',  # Monday to Friday at 01:00 AM Europe/London
    catchup=False,
    max_active_runs=1,                # Enforce single execution path
    is_paused_upon_creation=False,    # <Active>1</Active>
    default_args=DEFAULT_ARGS,
    tags=['finance', 'daily', 'gl']
) as dag:

    # 1. Guard Task: Prevents concurrent workflow instances
    def run_guard_logic(**context):
        current_run_id = context['run_id']
        active_runs = DagRun.find(dag_id=dag.dag_id, state=State.RUNNING)
        other_active_runs = [r for r in active_runs if r.run_id != current_run_id]
        
        if other_active_runs:
            raise AirflowSkipException(
                f"Another active instance is running (ID: {other_active_runs[0].run_id}). "
                "Skipping this execution run to prevent database collisions."
            )
        logging.info("Concurrency guard check passed. No other active executions found.")

    concurrency_guard = PythonOperator(
        task_id='concurrency_guard',
        python_callable=run_guard_logic,
        provide_context=True
    )

    # 2. Pre Check Task: Database Connection Validation
    pre_check_job = {
        "reference": {"project_id": GCP_PROJECT_ID},
        "placement": {"cluster_name": DATAPROC_CLUSTER},
        "pyspark_job": {
            "main_python_file_uri": f"{SPARK_SCRIPTS_PATH}/finance_daily_pre_check.py"
        }
    }

    pre_check = DataprocSubmitJobOperator(
        task_id='finance_daily_pre_check',
        job=pre_check_job,
        region=DATAPROC_REGION,
        project_id=GCP_PROJECT_ID,
        retries=2,
        retry_delay=timedelta(seconds=60),
        on_failure_callback=on_failure_alarm
    )

    # 3. Account Load Task: Refresh Account Master Dimension
    acct_load_job = {
        "reference": {"project_id": GCP_PROJECT_ID},
        "placement": {"cluster_name": DATAPROC_CLUSTER},
        "pyspark_job": {
            "main_python_file_uri": f"{SPARK_SCRIPTS_PATH}/finance_daily_acct_load.py",
            "args": ["{{ ds }}", "ALL", "ACCOUNT_ONLY"]
        }
    }

    acct_load = DataprocSubmitJobOperator(
        task_id='finance_daily_acct_load',
        job=acct_load_job,
        region=DATAPROC_REGION,
        project_id=GCP_PROJECT_ID,
        retries=FINANCE_RETRY_MAX,
        retry_delay=timedelta(seconds=60),
        on_failure_callback=on_failure_alarm
    )

    # 4. Rate Extract Task: Daily exchange rate population
    rate_extract_job = {
        "reference": {"project_id": GCP_PROJECT_ID},
        "placement": {"cluster_name": DATAPROC_CLUSTER},
        "pyspark_job": {
            "main_python_file_uri": f"{SPARK_SCRIPTS_PATH}/finance_daily_rate_extract.py",
            "args": [
                "{{ ds }}",
                "{{ dag_run.logical_date.strftime('%Y') }}",
                "{{ dag_run.logical_date.strftime('%m') }}"
            ]
        }
    }

    rate_extract = DataprocSubmitJobOperator(
        task_id='finance_daily_rate_extract',
        job=rate_extract_job,
        region=DATAPROC_REGION,
        project_id=GCP_PROJECT_ID,
        on_failure_callback=on_failure_alarm
    )

    # 5. GL Extract Task: Multi-Entity parallelized operational loops
    gl_extract_job = {
        "reference": {"project_id": GCP_PROJECT_ID},
        "placement": {"cluster_name": DATAPROC_CLUSTER},
        "pyspark_job": {
            "main_python_file_uri": f"{SPARK_SCRIPTS_PATH}/finance_daily_gl_extract.py",
            "args": ["{{ ds }}"]
        }
    }

    gl_extract = DataprocSubmitJobOperator(
        task_id='finance_daily_gl_extract',
        job=gl_extract_job,
        region=DATAPROC_REGION,
        project_id=GCP_PROJECT_ID,
        retries=FINANCE_RETRY_MAX,
        retry_delay=timedelta(seconds=120),
        on_failure_callback=on_failure_alarm
    )

    # 6. GL Close Task: Publish signal audit verification
    gl_close_job = {
        "reference": {"project_id": GCP_PROJECT_ID},
        "placement": {"cluster_name": DATAPROC_CLUSTER},
        "pyspark_job": {
            "main_python_file_uri": f"{SPARK_SCRIPTS_PATH}/finance_daily_gl_close.py",
            "args": ["{{ ds }}", ALLOW_EMPTY]
        }
    }

    gl_close = DataprocSubmitJobOperator(
        task_id='finance_daily_gl_close',
        job=gl_close_job,
        region=DATAPROC_REGION,
        project_id=GCP_PROJECT_ID,
        on_failure_callback=on_failure_alarm
    )

    # 7. Downstream Fire-and-Forget Target Triggers
    trigger_retail = TriggerDagRunOperator(
        task_id='trigger_retail_daily_workflow',
        trigger_dag_id='retail_daily_workflow',
        conf={"period_date": "{{ ds }}"},
        wait_for_completion=False
    )

    trigger_crm = TriggerDagRunOperator(
        task_id='trigger_crm_weekly_workflow',
        trigger_dag_id='crm_weekly_workflow',
        conf={"period_date": "{{ ds }}"},
        wait_for_completion=False
    )

    # ==============================================================================
    # TASK DEPENDENCY MAP
    # ==============================================================================
    concurrency_guard >> pre_check
    pre_check >> [acct_load, rate_extract]
    [acct_load, rate_extract] >> gl_extract
    gl_extract >> gl_close
    gl_close >> [trigger_retail, trigger_crm]