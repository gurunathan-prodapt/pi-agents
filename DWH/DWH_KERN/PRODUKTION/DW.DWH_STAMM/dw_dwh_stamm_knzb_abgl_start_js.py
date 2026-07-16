"""
DAG: dw_dwh_stamm_knzb_abgl_start_js
Purpose: State-control and pre-execution initialization (Start-Baustein).
"""

from datetime import datetime, timedelta
import logging

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.exceptions import AirflowSkipException

# Import helper functions from plugins
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

def check_and_update_knzb_status(**context):
    """
    Checks the status variable. Rejects execution if 'GESPERRT'.
    Otherwise, marks status as 'LAEUFT' and updates the runtime execution date.
    """
    execution_date_str = context['ds_nodash']
    
    state_container = Variable.get(VAR_CONTAINER_NAME, deserialize_json=True, default_var={})
    current_status = state_container.get("abgleich_status", "FREI")
    
    logger.info(f"Current KNZB Reconciliation Status: {current_status}")
    logger.info(f"Current Execution Date: {execution_date_str}")
    
    if current_status == "GESPERRT":
        # Preserve the original German logging format exactly
        message = f"KNZB-Abgleich fuer {execution_date_str} ist gesperrt - Abbruch der Verarbeitung"
        logger.warning(message)
        raise AirflowSkipException(message)
        
    state_container["abgleich_status"] = "LAEUFT"
    state_container["letzter_lauf"] = execution_date_str
    
    Variable.set(VAR_CONTAINER_NAME, state_container, serialize_json=True)
    logger.info("Status updated successfully to 'LAEUFT' in variable container.")

with DAG(
    dag_id='dw_dwh_stamm_knzb_abgl_start_js',
    default_args=DEFAULT_ARGS,
    description='Start-Baustein: Laufkennung und Sperr-Check setzen fuer KNZB-Stammdatenabgleich',
    schedule=None,
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False
) as dag:

    start = EmptyOperator(task_id='start')

    check_and_update_status = PythonOperator(
        task_id='check_and_update_status',
        python_callable=check_and_update_knzb_status,
        provide_context=True,
    )

    end = EmptyOperator(task_id='end')

    start >> check_and_update_status >> end