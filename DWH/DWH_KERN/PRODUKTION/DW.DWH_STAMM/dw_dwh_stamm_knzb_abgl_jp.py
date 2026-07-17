from datetime import datetime, timedelta
import os
import logging
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowSkipException, AirflowFailException

# Import modularized includes to preserve folder structure and code reuse
from dags.dwh.dwh_kern.produktion.dw_dwh_stamm.includes.dw_hole_pfad_knzb import hole_pfad_knzb
from dags.dwh.dwh_kern.produktion.dw_dwh_stamm.includes.dw_lese_log_knzb import lese_log_knzb

# ── GCP Configuration ────────────────────────────────────
GCP_PROJECT = os.environ.get("GCP_PROJECT")
GCP_REGION = os.environ.get("GCP_REGION")

# ── Default Args ─────────────────────────────────────────
default_args = {
    'owner': 'DWH_TEAM',
    'depends_on_past': False,
    'start_date': datetime(2024, 11, 4),
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

# ── on_failure_callback ──────────────────────────────────
def on_workflow_failure(context):
    """
    Safeguards against deadlocks. If a task fails while holding
    the lock, we switch the status to ERROR_STATE.
    """
    logging.warning("Workflow execution failed. Releasing state lock...")
    try:
        knzb_vars = Variable.get("dw_variablen_knzb", deserialize_json=True)
        if knzb_vars.get("ABGLEICH_STATUS") == "LAEUFT":
            knzb_vars["ABGLEICH_STATUS"] = "ERROR_STATE"
            Variable.set("dw_variablen_knzb", knzb_vars, serialize_json=True)
            logging.info("State set to ERROR_STATE. Manual verification required.")
    except Exception as e:
        logging.error(f"Failed to reset state variables: {str(e)}")

# ── DAG Definition ───────────────────────────────────────
dag = DAG(
    dag_id='dw_dwh_stamm_knzb_abgl_jp',
    default_args=default_args,
    description='Taeglicher Abgleich der Kundennummer-/Basiszugangs-Stammdaten (KNZB) in der DWH-Kernschicht',
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    on_failure_callback=on_workflow_failure
)

# ── Guard Task ───────────────────────────────────────────
def check_active_runs(**context):
    from airflow.models import DagRun
    from airflow.utils.state import State

    active_runs = DagRun.find(dag_id=context['dag'].dag_id, state=State.RUNNING)
    other_active_runs = [r for r in active_runs if r.run_id != context['run_id']]
    if other_active_runs:
        raise AirflowSkipException("Another instance of this DAG is currently running. Skipping execution.")

start_guard = PythonOperator(
    task_id='start_guard',
    python_callable=check_active_runs,
    provide_context=True,
    dag=dag
)

# ── Task: knzb_abgl_start ────────────────────────────────
def process_abgl_start(**context):
    """
    Simulates DW.DWH_STAMM_KNZB_ABGL_START_JS:
    - Imports paths
    - Checks for lock status "GESPERRT"
    - Changes status to "LAEUFT" and writes logs verbatim
    """
    # Load paths from Include: DW.HOLE_PFAD_KNZB
    hole_pfad_knzb()

    lauf_datum = context['ds_nodash']

    try:
        knzb_vars = Variable.get("dw_variablen_knzb", deserialize_json=True)
    except KeyError:
        knzb_vars = {"ABGLEICH_STATUS": "FREI", "LETZTER_LAUF": ""}
        logging.warning("dw_variablen_knzb variable not found. Initializing with default values.")

    abgleich_status = knzb_vars.get("ABGLEICH_STATUS", "FREI")

    if abgleich_status == "GESPERRT":
        logging.error(f"KNZB-Abgleich fuer {lauf_datum} ist gesperrt - Abbruch der Verarbeitung")
        raise AirflowFailException("Processing aborted because status is set to GESPERRT.")

    knzb_vars["ABGLEICH_STATUS"] = "LAEUFT"
    knzb_vars["LETZTER_LAUF"] = lauf_datum
    Variable.set("dw_variablen_knzb", knzb_vars, serialize_json=True)

    # Log output using Include: DW.LESE_LOG_KNZB
    lese_log_knzb("DW.DWH_STAMM_KNZB_ABGL_START_JS")

knzb_abgl_start = PythonOperator(
    task_id='knzb_abgl_start',
    python_callable=process_abgl_start,
    provide_context=True,
    dag=dag
)

# ── Task: knzb_abgl_ende ─────────────────────────────────
def process_abgl_ende(**context):
    """
    Simulates DW.DWH_STAMM_KNZB_ABGL_ENDE_JS:
    - Frees the state lock to "FREI"
    - Logs completion verbatim
    """
    hole_pfad_knzb()

    knzb_vars = Variable.get("dw_variablen_knzb", deserialize_json=True)
    letzter_lauf = knzb_vars.get("LETZTER_LAUF", context['ds_nodash'])

    knzb_vars["ABGLEICH_STATUS"] = "FREI"
    Variable.set("dw_variablen_knzb", knzb_vars, serialize_json=True)

    # German console outputs maintained character-for-character as required
    logging.info(f"KNZB-Stammdatenabgleich fuer Lauf {letzter_lauf} erfolgreich beendet")

    lese_log_knzb("DW.DWH_STAMM_KNZB_ABGL_ENDE_JS")

knzb_abgl_ende = PythonOperator(
    task_id='knzb_abgl_ende',
    python_callable=process_abgl_ende,
    provide_context=True,
    dag=dag
)

# ── Dependencies ─────────────────────────────────────────
start_guard >> knzb_abgl_start >> knzb_abgl_ende