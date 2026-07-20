"""
DAG: dw_dwh_dummy_absd_plato_tarife

Overview:
This DAG represents the migrated UC4 object 'DW.DWH_DUMMY_ABSD_PLATO_TARIFE'.
In the source system, this was an isolated Unix Job (JOBS_UNIX) that performed
no operational system or data processing steps, acting as a structural milestone,
synchronisation point, or placeholder within a larger parent Job Plan (JOBP).

Because the original script simply printed a status message (":print Doing nothinig"),
this is migrated as a single PythonOperator task that logs the equivalent message.

Schedule:
- None (This workflow must be triggered manually or run via an upstream calling process).
"""

import os
import logging
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# ==============================================================================
# GCP CONFIGURATION
# Sourced from environment variables. No prose placeholders permitted.
# ==============================================================================
GCP_PROJECT_ID = os.environ.get("GCP_PROJECT")
DATAPROC_REGION = os.environ.get("DATAPROC_REGION")
DATAPROC_CLUSTER_NAME = os.environ.get("DATAPROC_CLUSTER")
GCS_BUCKET = os.environ.get("GCS_BUCKET")

# ==============================================================================
# DEFAULT ARGS
# ==============================================================================
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2026, 3, 30),
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

# ==============================================================================
# DAG DEFINITION
# ==============================================================================
with DAG(
    dag_id='dw_dwh_dummy_absd_plato_tarife',
    default_args=default_args,
    description='Migrated UC4 dummy job acting as a milestone/synchronisation point',
    schedule=None,  # No schedule defined in the source UC4 object
    catchup=False,
    max_active_runs=1,  # Concurrency protection (replicates UC4 sync object behavior)
    is_paused_upon_creation=False,  # Active flag in UC4 was 1 (True)
    tags=['migrated_uc4', 'dummy_job'],
) as dag:

    # ==========================================================================
    # TASKS
    # ==========================================================================
    
    # Original print statement: ":print Doing nothinig" (verbatim text preserved)
    def log_dummy_action():
        """Logs the verbatim legacy dummy statement."""
        logging.info("Doing nothinig")

    # Task representing the legacy dummy print script.
    dwh_dummy_absd_plato_tarife = PythonOperator(
        task_id='dwh_dummy_absd_plato_tarife',
        python_callable=log_dummy_action,
    )

    # ==========================================================================
    # DEPENDENCY MAP
    # Simple single-node workflow structure
    # ==========================================================================
    dwh_dummy_absd_plato_tarife