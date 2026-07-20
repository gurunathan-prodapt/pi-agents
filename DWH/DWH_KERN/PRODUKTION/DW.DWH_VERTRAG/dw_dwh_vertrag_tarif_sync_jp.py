"""
DAG: dw_dwh_vertrag_tarif_sync_jp
Source File: DWH/DWH_KERN/PRODUKTION/DW.DWH_VERTRAG/DW.DWH_VERTRAG_TARIF_SYNC_JP.xml

Overview:
This DAG performs a weekly reconciliation (Abgleich) of contract and tariff 
assignments (Vertrags-/Tarifzuordnung) between the source system (Stammdaten / Master Data) 
and the Core Data Warehouse layer (DWH_KERN). 

To ensure single sources of truth, this orchestration DAG triggers downstream 
child components ("dw_dwh_vertrag_tarif_sync_start_js" and "dw_dwh_vertrag_tarif_sync_ende_js") 
sequentially using the TriggerDagRunOperator.

Schedule:
- Weekly on Sundays at 03:00 AM UTC ("0 3 * * 0").
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

# ── GLOBAL CONFIGURATION ────────────────────────────────────────────────────────
GCP_PROJECT = Variable.get("GCP_PROJECT", default_var=None)
GCS_BUCKET = Variable.get("GCS_BUCKET", default_var=None)


# ── DEFAULT ARGUMENTS ────────────────────────────────────────────────────────
default_args = {
    'owner': 'dwh_ops',
    'depends_on_past': False,
    'start_date': datetime(2024, 12, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


# ── ON FAILURE CALLBACK ──────────────────────────────────────────────────────
def on_failure_alarm(context):
    """
    Unified operational failure notification.
    Extracts failure context details and routes alerts to downstream channels.
    """
    task_instance = context.get('task_instance')
    dag_id = context.get('dag').dag_id
    execution_date = context.get('execution_date')
    
    print(f"CRITICAL: Task {task_instance.task_id} in DAG {dag_id} failed on run {execution_date}.")


# ── DAG DEFINITION ───────────────────────────────────────────────────────────
with DAG(
    dag_id='dw_dwh_vertrag_tarif_sync_jp',
    default_args=default_args,
    description='Woechentlicher Abgleich Vertrags-/Tarifzuordnung zwischen STAMMDATEN und DWH_KERN',
    schedule='0 3 * * 0',  # Weekly on Sunday at 03:00 AM
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False
) as dag:

    # Logical Start Node
    start = EmptyOperator(
        task_id='start'
    )

    # Trigger Task 1: START Sync Component
    # Runs the initial weekly contract sync pipeline and waits for completion
    trigger_start_js = TriggerDagRunOperator(
        task_id='trigger_dw_dwh_vertrag_tarif_sync_start_js',
        trigger_dag_id='dw_dwh_vertrag_tarif_sync_start_js',
        wait_for_completion=True,
        poke_interval=60,
        reset_dag_run=True,
        on_failure_callback=on_failure_alarm
    )

    # Trigger Task 2: END Sync Component
    # Finalizes weekly sync pipelines and commits updates, waiting for completion
    trigger_ende_js = TriggerDagRunOperator(
        task_id='trigger_dw_dwh_vertrag_tarif_sync_ende_js',
        trigger_dag_id='dw_dwh_vertrag_tarif_sync_ende_js',
        wait_for_completion=True,
        poke_interval=60,
        reset_dag_run=True,
        on_failure_callback=on_failure_alarm
    )

    # Logical End Node
    end = EmptyOperator(
        task_id='end'
    )

    # Orchestration Chain / Dependencies
    start >> trigger_start_js >> trigger_ende_js >> end