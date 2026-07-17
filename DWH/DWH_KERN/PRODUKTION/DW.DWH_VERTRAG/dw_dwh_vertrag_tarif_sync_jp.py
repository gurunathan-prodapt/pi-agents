from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.models import Variable
import os

# Import partitioned modular helper functions to maintain strict folder integrity
from dags.dwh_dwh_vertrag.includes.dw_hole_pfad_vtrg import load_env_paths
from dags.dwh_dwh_vertrag.includes.dw_lese_log_vtrg import log_execution_status

# --------------------------------------------------
# ENVIRONMENT CONFIGURATION (GLOBAL ENVIRONMENT VARIABLES)
# --------------------------------------------------
GCP_PROJECT = os.environ.get("GCP_PROJECT")
GCP_REGION = os.environ.get("GCP_REGION")
GCS_BUCKET = os.environ.get("GCS_BUCKET")

# Sourced dynamically from the modular helper representing legacy DW.HOLE_PFAD_VTRG
env_paths = load_env_paths()
DWH_HOME = env_paths["DWH_HOME"]
HOME = env_paths["HOME"]
PMS_HOME = env_paths["PMS_HOME"]

# --------------------------------------------------
# DEFAULT DAG ARGUMENTS
# --------------------------------------------------
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 12, 1),
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

# --------------------------------------------------
# DAG DEFINITION
# --------------------------------------------------
dag = DAG(
    dag_id='dw_dwh_vertrag_tarif_sync_jp',
    default_args=default_args,
    description='Woechentlicher Abgleich Vertrags-/Tarifzuordnung zwischen STAMMDATEN und DWH_KERN',
    schedule_interval=None,  # No schedule defined in legacy xml files
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False
)

# --------------------------------------------------
# TASK PYTHON CALLABLES
# --------------------------------------------------
def check_and_lock_sync_status(**context):
    """
    Emulates:
      - DW.DWH_VERTRAG_TARIF_SYNC_START_JS
    """
    # Emulate Include: DW.LESE_LOG_VTRG via imported helper
    log_execution_status(context['dag'].dag_id, context['task'].task_id)
    
    # Retrieve legacy variable container values from Airflow Variables
    sync_status = Variable.get("dw_variablen_vtrg_sync_status", default_var="FREI").upper()
    lauf_datum = context['ds_nodash'] # 'YYYYMMDD' equivalent of SYS_DATE("YYYYMMDD")
    
    if sync_status == "GESPERRT":
        # OUTPUT/PRINT LITERAL RULE: VERBATIM ORIGINAL GERMAN ABORT MSG
        print(f"Vertrags-/Tarifabgleich fuer {lauf_datum} ist gesperrt - Abbruch")
        return "skip_execution"
        
    # Set run variable indicators atomically
    Variable.set("dw_variablen_vtrg_sync_status", "LAEUFT")
    Variable.set("dw_variablen_vtrg_letzter_lauf", lauf_datum)
    
    return "execute_sync_dummy"


def release_sync_lock_status(**context):
    """
    Emulates:
      - DW.DWH_VERTRAG_TARIF_SYNC_ENDE_JS
    """
    lauf_datum = Variable.get("dw_variablen_vtrg_letzter_lauf", default_var=context['ds_nodash'])
    
    # Free the execution status
    Variable.set("dw_variablen_vtrg_sync_status", "FREI")
    
    # OUTPUT/PRINT LITERAL RULE: VERBATIM ORIGINAL SUCCESS MSG
    print(f"Vertrags-/Tarifabgleich fuer Lauf {lauf_datum} erfolgreich beendet")
    
    # Emulate Include: DW.LESE_LOG_VTRG via imported helper
    log_execution_status(context['dag'].dag_id, context['task'].task_id)

# --------------------------------------------------
# OPERATORS / TASKS
# --------------------------------------------------
check_and_lock_sync = BranchPythonOperator(
    task_id='check_and_lock_sync',
    python_callable=check_and_lock_sync_status,
    provide_context=True,
    dag=dag,
)

skip_execution = EmptyOperator(
    task_id='skip_execution',
    dag=dag,
)

execute_sync_dummy = EmptyOperator(
    task_id='execute_sync_dummy',
    dag=dag,
)

release_sync_lock = PythonOperator(
    task_id='release_sync_lock',
    python_callable=release_sync_lock_status,
    provide_context=True,
    dag=dag,
)

# --------------------------------------------------
# WORKFLOW DEPENDENCIES (PRESERVING LEGACY GRAPH)
# --------------------------------------------------
check_and_lock_sync >> execute_sync_dummy >> release_sync_lock
check_and_lock_sync >> skip_execution