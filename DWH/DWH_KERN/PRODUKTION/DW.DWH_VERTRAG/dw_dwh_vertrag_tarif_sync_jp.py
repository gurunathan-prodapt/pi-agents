"""
DAG File: dw_dwh_vertrag_tarif_sync_jp.py
Purpose: Orchestrates the weekly synchronization workflow using an Airflow DAG.
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from dags.dwh_vertrag.tasks.dw_dwh_vertrag_tarif_sync_ende import (
    execute_ende_task,
)
from dags.dwh_vertrag.tasks.dw_dwh_vertrag_tarif_sync_start import (
    execute_start_task,
)

default_args = {
    "owner": "data_engineering",
    "depends_on_past": False,
    "start_date": datetime(2026, 7, 16),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="dw_dwh_vertrag_tarif_sync_jp",
    default_args=default_args,
    description="Woechentlicher Abgleich Vertrags-/Tarifzuordnung zwischen STAMMDATEN und DWH_KERN",
    schedule_interval="0 3 * * 7",  # Weekly on Sunday at 03:00 AM
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
) as dag:

    # Start Task Initialization
    dw_dwh_vertrag_tarif_sync_start_js = PythonOperator(
        task_id="dw_dwh_vertrag_tarif_sync_start_js",
        python_callable=execute_start_task,
        provide_context=True,
    )

    # Ende Task Finalization
    dw_dwh_vertrag_tarif_sync_ende_js = PythonOperator(
        task_id="dw_dwh_vertrag_tarif_sync_ende_js",
        python_callable=execute_ende_task,
        provide_context=True,
    )

    # Workflow Dependency Topology
    dw_dwh_vertrag_tarif_sync_start_js >> dw_dwh_vertrag_tarif_sync_ende_js