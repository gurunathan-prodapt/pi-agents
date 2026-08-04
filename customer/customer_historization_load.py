"""
DAG for SCD2 historization of the weekly customer segment/score into the segment dimension.
This is migrated from the legacy UC4 JOBS_UNIX object: CUSTOMER.HISTORIZATION_LOAD.

The workflow executes on-demand (schedule=None) and can be triggered manually,
by an upstream DAG via TriggerDagRunOperator, or by an external Airflow dataset/event trigger.
"""

import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator

# ── GCP CONFIGURATION ────────────────────────────────────────────────────────
# Global environment variables sourced at runtime following GCP best practices.
GCP_PROJECT = Variable.get("GCP_PROJECT", default_var=os.environ.get("GCP_PROJECT"))
GCP_REGION = Variable.get("GCP_REGION", default_var=os.environ.get("GCP_REGION"))

# ── JOB-SPECIFIC CONFIGURATION ───────────────────────────────────────────────
JOB_CONFIG = {
    "MAX_EXPECTED_CHANGE_PCT": 25
}

# ── DEFAULT ARGUMENTS ────────────────────────────────────────────────────────
DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# ── DAG DEFINITION ───────────────────────────────────────────────────────────
with DAG(
    dag_id='customer_historization_load',
    default_args=DEFAULT_ARGS,
    description='SCD2 historization of the weekly customer segment/score into the segment dimension',
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
) as dag:

    # ── TASK: customer_historization_load ────────────────────────────────────
    # Legacy execution context details:
    #   Host: |ETLHOST2|HOST -> Re-architected to Google Cloud execution environment
    #   Login: UNIX.ETL_SVC
    #   Command context:
    #     :SET &RUN_DATE='&$TODAY'
    #     . &HOME/customer/r_historization_load.ksh
    #
    # Run date in Airflow should utilize: {{ dag_run.conf.get('RUN_DATE', ds) }}
    # to support manual runs, backfills, and runtime schedule compatibility.
    customer_historization_load = EmptyOperator(
        task_id='customer_historization_load',
    )

    # ── DEPENDENCY MAP ───────────────────────────────────────────────────────
    # Single task DAG; no dependency chain required.
    customer_historization_load