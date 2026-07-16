"""
DAG ID: finance_daily_workflow
Author: Finance ETL Team
Description: Migrated pipeline from UC4 (FINANCE_DAILY_WORKFLOW).
             Orchestrates GL transaction extraction, master data alignment, 
             and staging table loads targeting Google Dataproc and Pub/Sub.
"""

from datetime import datetime, timedelta
import logging

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import ShortCircuitOperator, PythonOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.providers.google.cloud.operators.pubsub import PubSubPublishMessageOperator

# ── ENVIRONMENT VARIABLES POLICY (SECTION 3) ──────────────────────────────────
GCP_PROJECT_ID = Variable.get("GCP_PROJECT")
DATAPROC_REGION = Variable.get("GCP_REGION")
DATAPROC_CLUSTER_NAME = Variable.get("DATAPROC_CLUSTER_NAME")
GCS_BUCKET = Variable.get("GCS_BUCKET_NAME")
NOTIFY_EMAIL = Variable.get("finance_notify_email", default_var="finance-etl@company.com")

SPARK_SCRIPT_PATH = f"gs://{GCS_BUCKET}/pyspark_scripts"

# ── JOB-SPECIFIC PARAMETERS (SECTION 3) ───────────────────────────────────────
JOB_CONFIG = {
    "source_system": "Oracle_ERP_Finance",
    "entities_scope": "UK_ENTITY,DE_ENTITY,FR_ENTITY",
    "allow_empty": "N"
}

# ── NOTIFICATIONS & ALERTING CALLBACKS (SECTION 1.4 & 1.7) ────────────────────
def on_failure_alarm(context):
    """
    Simulates NOTIFY_AND_ABORT.
    Triggered by critical pipeline failures.
    """
    task_instance = context.get('task_instance')
    ds = context.get('ds')
    subject = f"[CRITICAL] FINANCE_DAILY_WORKFLOW FAILED for {ds}"
    
    logging.error(f"SENDING CRITICAL ALERT to {NOTIFY_EMAIL} and dw-alerts@company.com")
    logging.error(f"Subject: {subject}")
    logging.error(f"Exception context: {context.get('exception')}")


def on_failure_alarm_continue(context):
    """
    Simulates NOTIFY then CONTINUE action.
    Triggered by non-blocking extraction task failures to permit downstream review.
    """
    task_instance = context.get('task_instance')
    ds = context.get('ds')
    subject = f"[WARNING] FINANCE_DAILY_WORKFLOW non-blocking task failed on {ds}"
    
    logging.warning(f"SENDING WARNING ALERT to {NOTIFY_EMAIL}")
    logging.warning(f"Subject: {subject}. Execution path proceeding with downstream evaluation pipelines.")


# ── CUSTOM HOLIDAY CALENDAR FILTER (SECTION 1.8 & 2.5) ────────────────────────
def check_uk_holiday_calendar(**context):
    """
    Evaluates whether the execution run falls on a registered UK Public Holiday.
    Returns False to skip the downstream tasks if today is a holiday.
    """
    logical_date = context['dag_run'].logical_date
    formatted_date = logical_date.strftime('%Y-%m-%d')
    
    # Placeholder representation of "PUBLIC_HOLIDAYS_UK" calendar
    uk_holidays = ["2024-01-01", "2024-03-29", "2024-04-01", "2024-12-25", "2024-12-26"]
    
    if formatted_date in uk_holidays:
        logging.info(f"Execution date {formatted_date} matched UK Holiday Calendar. Skipping pipeline execution.")
        return False
    
    logging.info(f"Execution date {formatted_date} is a regular business day. Proceeding.")
    return True


def audit_log_gl_close(**context):
    """
    Appends GL close metrics to logs, satisfying the absolute requirement to preserve
    the legacy output log literal exactly as written in the source.
    """
    period_date = context['ds']
    # Preserving the exact literal string logic: "[FINANCE_DAILY_GL_CLOSE] Period=" + PERIOD_DATE + " complete"
    literal_log = "[FINANCE_DAILY_GL_CLOSE] Period=" + period_date + " complete"
    print(literal_log)
    logging.info(literal_log)


# ── DAG DEFINITION ────────────────────────────────────────────────────────────
default_args = {
    'owner': 'finance_etl',
    'start_date': datetime(2024, 1, 1),
    'email': [NOTIFY_EMAIL],
    'email_on_failure': True,
    'retries': 0,
}

with DAG( 
    dag_id="finance_daily_workflow",
    schedule="0 1 * * 1-5",  # Monday to Friday at 01:00 (Europe/London)
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    default_args=default_args,
) as dag:

    # Task 0: Holiday Sensor Check
    holiday_calendar_sensor = ShortCircuitOperator(
        task_id="check_holiday_calendar",
        python_callable=check_uk_holiday_calendar,
    )

    # Task 1: Pre-Check (Verify Oracle database availability)
    pre_check_job = {
        "reference": {"project_id": GCP_PROJECT_ID},
        "placement": {"cluster_name": DATAPROC_CLUSTER_NAME},
        "pyspark_job": {
            "main_python_file_uri": f"{SPARK_SCRIPT_PATH}/finance_daily_pre_check.py",
            "args": ["--verify-db-connection", "ORACLE_FIN_LOGIN"]
        },
    }

    task_pre_check = DataprocSubmitJobOperator(
        task_id="finance_daily_pre_check",
        job=pre_check_job,
        region=DATAPROC_REGION,
        project_id=GCP_PROJECT_ID,
        retries=2,
        retry_delay=timedelta(seconds=60),
        on_failure_callback=on_failure_alarm,
    )

    # Task 2: Account Master Load (Parallel Branch) - UNRESOLVED COMPONENT STUB
    acct_load_job = {
        "reference": {"project_id": GCP_PROJECT_ID},
        "placement": {"cluster_name": DATAPROC_CLUSTER_NAME},
        "pyspark_job": {
            "main_python_file_uri": f"{SPARK_SCRIPT_PATH}/run_account_load.py",
            "args": [
                "--period-date", "{{ ds }}",
                "--load-scope", "ALL",
                "--execution-mode", "ACCOUNT_ONLY"
            ]
        },
    }

    task_acct_load = DataprocSubmitJobOperator(
        task_id="finance_daily_acct_load",
        job=acct_load_job,
        region=DATAPROC_REGION,
        project_id=GCP_PROJECT_ID,
        retries=3,
        retry_delay=timedelta(seconds=60),
        execution_timeout=timedelta(minutes=30),
        on_failure_callback=on_failure_alarm_continue,
    )

    # Task 3: Exchange Rate Extract (Parallel Branch) - UNRESOLVED COMPONENT STUB
    rate_extract_job = {
        "reference": {"project_id": GCP_PROJECT_ID},
        "placement": {"cluster_name": DATAPROC_CLUSTER_NAME},
        "pyspark_job": {
            "main_python_file_uri": f"{SPARK_SCRIPT_PATH}/rate_extract.py",
            "args": [
                "--period-date", "{{ ds }}",
                "--period-year", "{{ dag_run.logical_date.strftime('%Y') }}",
                "--period-month", "{{ dag_run.logical_date.strftime('%m') }}"
            ]
        },
    }

    task_rate_extract = DataprocSubmitJobOperator(
        task_id="finance_daily_rate_extract",
        job=rate_extract_job,
        region=DATAPROC_REGION,
        project_id=GCP_PROJECT_ID,
        execution_timeout=timedelta(minutes=20),
        on_failure_callback=on_failure_alarm_continue,
    )

    # Task 4: Daily GL Extract (Multi-Entity Parallel Spark Runner) - UNRESOLVED COMPONENT STUB
    gl_extract_job = {
        "reference": {"project_id": GCP_PROJECT_ID},
        "placement": {"cluster_name": DATAPROC_CLUSTER_NAME},
        "pyspark_job": {
            "main_python_file_uri": f"{SPARK_SCRIPT_PATH}/run_gl_close.py",
            "args": [
                "--period-date", "{{ ds }}",
                "--entities", JOB_CONFIG["entities_scope"],
                "--allow-empty", JOB_CONFIG["allow_empty"]
            ]
        },
    }

    task_gl_extract = DataprocSubmitJobOperator(
        task_id="finance_daily_gl_extract",
        job=gl_extract_job,
        region=DATAPROC_REGION,
        project_id=GCP_PROJECT_ID,
        retries=3,
        retry_delay=timedelta(seconds=120),
        execution_timeout=timedelta(minutes=90),
        on_failure_callback=on_failure_alarm,
    )

    # Task 5: Daily GL Close (Audit Logging & Event Broker)
    task_gl_close_log = PythonOperator(
        task_id="finance_daily_gl_close_log",
        python_callable=audit_log_gl_close,
        provide_context=True,
    )

    task_gl_close_publish = PubSubPublishMessageOperator(
        task_id="finance_daily_gl_close_publish",
        project_id=GCP_PROJECT_ID,
        topic="finance_gl_close_complete",
        messages=[{
            "data": b"FINANCE_GL_CLOSE_COMPLETE",
            "attributes": {
                "period_date": "{{ ds }}",
                "audit_timestamp": datetime.utcnow().isoformat()
            }
        }],
        on_failure_callback=on_failure_alarm,
    )

    # ── DEPENDENCY TREE DEFINITION (SECTION 1.5) ──────────────────────────────
    holiday_calendar_sensor >> task_pre_check
    task_pre_check >> [task_acct_load, task_rate_extract]
    [task_acct_load, task_rate_extract] >> task_gl_extract >> task_gl_close_log >> task_gl_close_publish