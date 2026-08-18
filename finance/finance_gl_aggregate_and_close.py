"""
DAG: finance_gl_aggregate_and_close

This DAG is migrated from the UC4 JOBS_UNIX object FINANCE.GL_AGGREGATE_AND_CLOSE.
It represents a financial process responsible for executing a Spark-based financial 
ledger aggregation for analytical outputs, writing a closing audit record, and 
dispatching a status notification.

This workflow is configured to be externally triggered (schedule is None).
"""

from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.models import Variable

# ==============================================================================
# GLOBAL CONFIGURATION (Environment-Wide Infrastructure)
# ==============================================================================
GCP_PROJECT = Variable.get("GCP_PROJECT", default_var=None) or os.environ.get("GCP_PROJECT")
GCP_REGION = Variable.get("GCP_REGION", default_var=None) or os.environ.get("GCP_REGION")
DATAPROC_REGION = Variable.get("DATAPROC_REGION", default_var=None) or os.environ.get("DATAPROC_REGION")
DATAPROC_CLUSTER = Variable.get("DATAPROC_CLUSTER", default_var=None) or os.environ.get("DATAPROC_CLUSTER")
GCS_BUCKET = Variable.get("GCS_BUCKET", default_var=None) or os.environ.get("GCS_BUCKET")
BQ_DATASET = Variable.get("ANALYTICS_SCHEMA", default_var=None)
HOME_DIR = os.environ.get("AIRFLOW_HOME", "/opt/airflow")

# ==============================================================================
# JOB-SPECIFIC CONFIGURATION
# ==============================================================================
NOTIFY_EMAIL = Variable.get("finance_gl_aggregate_and_close_notify_email", default_var="finance-etl@example.com")

# ==============================================================================
# DEFAULT ARGUMENTS
# ==============================================================================
DEFAULT_ARGS = {
    "owner": "UNIX_ETL_SVC",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# ==============================================================================
# DAG DEFINITION
# ==============================================================================
with DAG(
    dag_id="finance_gl_aggregate_and_close",
    default_args=DEFAULT_ARGS,
    description="Spark aggregation for analytical outputs, write close audit record, and notify.",
    schedule=None,  # Externally triggered
    start_date=datetime(2023, 1, 1),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    tags=["finance", "migration_uc4"],
) as dag:

    # ==========================================================================
    # Task: gl_aggregate_and_close
    # ==========================================================================
    # Original behavior executed KSH script: . &HOME/finance/r_gl_aggregate_and_close.ksh
    # Sibling files (r_gl_aggregate_and_close.ksh, d_gl_close_audit.sql) are outside
    # this conversion pass scope. This task is mapped to a BashOperator that executes
    # the KornShell script with the required environment variables set.
    gl_aggregate_and_close = BashOperator(
        task_id="gl_aggregate_and_close",
        bash_command="""
            export PERIOD_NAME="{{ (dag_run.logical_date - macros.dateutil.relativedelta.relativedelta(months=1)).strftime('%b-%Y').upper() }}"
            export FISCAL_YEAR="{{ var.value.get('CURRENT_FISCAL_YEAR') }}"
            export NOTIFY_EMAIL="{{ var.value.get('finance_gl_aggregate_and_close_notify_email', 'finance-etl@example.com') }}"
            python {{ params.home_dir }}/finance/r_gl_aggregate_and_close.py
        """,
        params={
            "home_dir": HOME_DIR
        }
    )

    # Single-task pipeline execution flow
    gl_aggregate_and_close