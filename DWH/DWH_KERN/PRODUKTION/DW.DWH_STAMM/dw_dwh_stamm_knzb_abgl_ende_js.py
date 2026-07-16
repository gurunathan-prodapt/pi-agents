"""
DAG: dw_dwh_stamm_knzb_abgl_ende_js
Purpose: Lock release and completion status reporter (Ende-Baustein).
"""

from datetime import datetime, timedelta
import logging

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable

# Import helper functions from plugins
from helpers.hole_pfad_knzb import resolve_knzb_paths
from helpers.lese_log_knzb import log_uc4_metadata

logger = logging.getLogger("airflow.task")

VAR_CONTAINER_NAME = "dw_variablen_knzb"

DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'on_execute_callback': log_uc4_metadata,
}

def release_knzb_lock_callable(**context):
    """
    Releases the concurrency lock inside the Airflow Variable container.
    """
    # 1. Resolve paths
    paths = resolve_knzb_paths()
    
    # 2. Get and update variables
    state_container = Variable.get(VAR_CONTAINER_NAME, deserialize_json=True, default_var={})
    lauf_datum = state_container.get("letzter_lauf", datetime.today().strftime('%Y%m%d'))
    
    logger.info(f"Retrieved last run date: {lauf_datum}")
    logger.info(f"Working directory resolved to DWH_HOME: {paths['DWH_HOME']}")
    
    # Reset lock variable to FREE
    state_container["abgleich_status"] = "FREI"
    Variable.set(VAR_CONTAINER_NAME, state_container, serialize_json=True)
    logger.info(f"Reset '{VAR_CONTAINER_NAME}' state 'abgleich_status' back to 'FREI'")
    
    # Print exactly as in original source log (German language character-for-character)
    print(f"KNZB-Stammdatenabgleich fuer Lauf {lauf_datum} erfolgreich beendet")

with DAG(
    dag_id='dw_dwh_stamm_knzb_abgl_ende_js',
    default_args=DEFAULT_ARGS,
    description='Ende-Baustein: Gibt die Laufkennung im Variablencontainer wieder frei',
    schedule=None,
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False
) as dag:

    start = EmptyOperator(task_id='start')

    release_lock = PythonOperator(
        task_id='release_lock',
        python_callable=release_knzb_lock_callable,
        provide_context=True,
    )

    end = EmptyOperator(task_id='end')

    start >> release_lock >> end