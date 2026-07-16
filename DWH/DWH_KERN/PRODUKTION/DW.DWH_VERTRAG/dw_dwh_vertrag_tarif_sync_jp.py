"""
DAG Orchestrator: dw_dwh_vertrag_tarif_sync_jp
Weekly contract/tariff alignment sync orchestration.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

# Import target python task scripts modularly
from tasks.dw_dwh_vertrag_tarif_sync_start import execute_start_task
from tasks.dw_dwh_vertrag_tarif_sync_ende import execute_end_task

# ── Default Args Configuration ──────────────────────────
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 12, 1),
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

# ── DAG Definition ───────────────────────────────────────
with DAG(
    dag_id='dw_dwh_vertrag_tarif_sync_jp',
    default_args=default_args,
    description='Woechentlicher Abgleich Vertrags-/Tarifzuordnung zwischen STAMMDATEN und DWH_KERN',
    schedule_interval='0 0 * * 0',  # Runs weekly on Sunday at midnight
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    tags=['dwh', 'vertrag', 'sync', 'uc4_migration'],
) as dag:

    # Start boundary marker
    start = EmptyOperator(
        task_id='start'
    )

    # Task 1: Check lock status and register execution run
    start_js = PythonOperator(
        task_id='dw_dwh_vertrag_tarif_sync_start_js',
        python_callable=execute_start_task,
        provide_context=True
    )

    # Task 2: Sync completion, reset status lock, and log final stats
    ende_js = PythonOperator(
        task_id='dw_dwh_vertrag_tarif_sync_ende_js',
        python_callable=execute_end_task,
        provide_context=True
    )

    # End boundary marker
    end = EmptyOperator(
        task_id='end'
    )

    # ── Sequential Execution Dependency Chain ─────────────────
    start >> start_js >> ende_js >> end