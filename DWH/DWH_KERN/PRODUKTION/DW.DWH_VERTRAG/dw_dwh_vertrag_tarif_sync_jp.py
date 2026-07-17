import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.exceptions import AirflowFailException

# Import our refactored folder-integrity compliant includes using correct paths matching actual locations
from DWH_KERN.PRODUKTION.DW_DWH_VERTRAG.includes.dw_hole_pfad_vtrg import get_vtrg_paths
from DWH_KERN.PRODUKTION.DW_DWH_VERTRAG.includes.dw_lese_log_vtrg import write_execution_log

# JOB-SPECIFIC PARAMETERS
DAG_ID = "DW_DWH_VERTRAG_TARIF_SYNC_JP"
JOB_NAME_START = "DW.DWH_VERTRAG_TARIF_SYNC_START_JS"
JOB_NAME_ENDE = "DW.DWH_VERTRAG_TARIF_SYNC_ENDE_JS"
DWH_JOB_KENNUNG = "VERTRAG_TARIF_SYNC"

def run_start_js(**context):
    """
    Logic from DW.DWH_VERTRAG_TARIF_SYNC_START_JS.xml
    Validates if sync is locked, and updates sync state status and execution date.
    """
    # 1. Include Path logic
    paths = get_vtrg_paths()
    
    # 2. Variable resolution
    execution_date = context['ds_nodash']  # YYYYMMDD format
    
    # Access state-tracking variable container (using Airflow Variable)
    # Default to "FREI" if not defined yet
    sync_status = Variable.get("DW_VARIABLEN_VTRG__SYNC_STATUS", default_var="FREI")
    
    # 3. Status Gate check
    if sync_status == "GESPERRT":
        # OUTPUT/PRINT LITERAL RULE: Literal print text must match the legacy UC4 print statement
        msg = f"Vertrags-/Tarifabgleich fuer {execution_date} ist gesperrt - Abbruch"
        print(msg)
        raise AirflowFailException(msg)
        
    # 4. State Updates
    Variable.set("DW_VARIABLEN_VTRG__SYNC_STATUS", "LAEUFT")
    Variable.set("DW_VARIABLEN_VTRG__LETZTER_LAUF", execution_date)
    
    # 5. Include Log logic
    write_execution_log(DAG_ID, JOB_NAME_START)

def run_ende_js(**context):
    """
    Logic from DW.DWH_VERTRAG_TARIF_SYNC_ENDE_JS.xml
    Resets the sync status variable back to 'FREI'.
    """
    # 1. Include Path logic
    paths = get_vtrg_paths()
    
    # 2. Get last run execution date for print statements
    lauf_datum = Variable.get("DW_VARIABLEN_VTRG__LETZTER_LAUF", default_var=context['ds_nodash'])
    
    # 3. Put variable state change
    Variable.set("DW_VARIABLEN_VTRG__SYNC_STATUS", "FREI")
    
    # OUTPUT/PRINT LITERAL RULE: Literal print text must match the legacy UC4 print statement
    print(f"Vertrags-/Tarifabgleich fuer Lauf {lauf_datum} erfolgreich beendet")
    
    # 4. Include Log logic
    write_execution_log(DAG_ID, JOB_NAME_ENDE)


# Define standard weekly schedule logic as declared in legacy JP metadata
default_args = {
    'owner': 'DWH_VERTRAG_OWNER',
    'depends_on_past': False,
    'start_date': datetime.datetime(2024, 12, 1),
    'retries': 0
}

with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    description="Woechentlicher Abgleich Vertrags-/Tarifzuordnung zwischen STAMMDATEN und DWH_KERN",
    schedule_interval="0 6 * * 0",  # Sunday morning weekly schedule
    catchup=False,
    max_active_runs=1
) as dag:

    start_task = PythonOperator(
        task_id="start_task",
        python_callable=run_start_js,
        provide_context=True
    )

    ende_task = PythonOperator(
        task_id="ende_task",
        python_callable=run_ende_js,
        provide_context=True
    )

    # Execution order wiring (task sequence)
    start_task >> ende_task