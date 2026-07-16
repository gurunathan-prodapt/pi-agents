"""
DAG: dw_dwh_stamm_knzb_abgl_jp
Purpose: Master pipeline defining the execution order of the KNZB matching sequence.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

# Import helper functions from plugins
from helpers.lese_log_knzb import log_uc4_metadata

DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 11, 4),
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'on_execute_callback': log_uc4_metadata,
}

with DAG(
    dag_id='dw_dwh_stamm_knzb_abgl_jp',
    default_args=DEFAULT_ARGS,
    description='Daily orchestration pipeline for master data reconciliation (KNZB)',
    schedule_interval=None,  # Triggered manually or upstream
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
) as dag:

    start = EmptyOperator(
        task_id='start',
    )

    # Trigger start wrapper component
    dw_dwh_stamm_knzb_abgl_start_js = TriggerDagRunOperator(
        task_id='dw_dwh_stamm_knzb_abgl_start_js',
        trigger_dag_id='dw_dwh_stamm_knzb_abgl_start_js',
        wait_for_completion=True,
        poke_interval=30,
        reset_dag_run=True,
        check_fully_qualified_dag_status=True,
    )

    # Trigger end wrapper component
    dw_dwh_stamm_knzb_abgl_ende_js = TriggerDagRunOperator(
        task_id='dw_dwh_stamm_knzb_abgl_ende_js',
        trigger_dag_id='dw_dwh_stamm_knzb_abgl_ende_js',
        wait_for_completion=True,
        poke_interval=30,
        reset_dag_run=True,
        check_fully_qualified_dag_status=True,
    )

    end = EmptyOperator(
        task_id='end',
    )

    # Master Execution Chain strictly adhering to UC4 flow
    start >> dw_dwh_stamm_knzb_abgl_start_js >> dw_dwh_stamm_knzb_abgl_ende_js >> end