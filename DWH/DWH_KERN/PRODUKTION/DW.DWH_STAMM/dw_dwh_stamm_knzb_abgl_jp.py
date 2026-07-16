"""
Main Workflow DAG: dw_dwh_stamm_knzb_abgl_jp
Orchestrates the sequence flow mapped from the UC4 Job Plan (JOBP):
DW.DWH_STAMM_KNZB_ABGL_JP.xml
"""

import os
from datetime import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

# Resolve Global Target Configurations
GCP_PROJECT = os.environ.get("GCP_PROJECT")
BQ_DATASET = os.environ.get("BQ_DATASET", "DW_DWH_STAMM")

# Import modular execution tasks
from tasks.dw_dwh_stamm_knzb_abgl_start_js import run_start_js
from tasks.dw_dwh_stamm_knzb_abgl_ende_js import run_ende_js

# Default execution settings
default_args = {
    "owner": "DWH_KERN",
    "depends_on_past": False,
    "start_date": datetime(2026, 7, 16),
    "retries": 0,
}

with DAG(
    dag_id="dw_dwh_stamm_knzb_abgl_jp",
    default_args=default_args,
    description="Taeglicher Abgleich der Kundennummer-/Basiszugangs-Stammdaten (KNZB) in der DWH-Kernschicht",
    schedule_interval=None,  # Manually triggered or externally orchestrated
    catchup=False,
    max_active_runs=1,
) as dag:

    # 1. Start Workflow Boundary
    start = EmptyOperator(
        task_id="start"
    )

    # 2. Run Start alignment, verify states and flag as RUNNING
    execute_start_js = PythonOperator(
        task_id="dw_dwh_stamm_knzb_abgl_start_js",
        python_callable=run_start_js,
        provide_context=True,
    )

    # 3. Complete processing validation, release flags to FREE
    execute_ende_js = PythonOperator(
        task_id="dw_dwh_stamm_knzb_abgl_ende_js",
        python_callable=run_ende_js,
        provide_context=True,
    )

    # 4. End Workflow Boundary
    end = EmptyOperator(
        task_id="end"
    )

    # Re-engineered dependency tree matching original JOBP structure
    start >> execute_start_js >> execute_ende_js >> end