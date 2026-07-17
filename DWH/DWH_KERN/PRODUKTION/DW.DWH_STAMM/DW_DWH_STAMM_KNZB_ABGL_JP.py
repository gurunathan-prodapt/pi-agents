import os
import sys
import logging
from datetime import datetime
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowFailException

# Resolve path relative to DAGs root folder to ensure Python can resolve includes cleanly
sys.path.append(os.path.join(os.path.dirname(__file__), 'includes'))

# Import the mapped dependencies from their matching folders
from DW_HOLE_PFAD_KNZB import include_hole_pfad_knzb
from DW_LESE_LOG_KNZB import include_lese_log_knzb

# Initialize logging
logger = logging.getLogger("airflow.task")

# ==========================================
# ENV VARIABLE POLICY: Global Configurations
# ==========================================
GCP_PROJECT = os.environ.get("GCP_PROJECT")

def execute_start_js(**context):
    """
    Translates legacy JOBS 'DW.DWH_STAMM_KNZB_ABGL_START_JS'
    """
    task_id = context['task'].task_id
    dag_id = context['dag'].dag_id
    
    # 1. Execute INCLUDE DW.HOLE_PFAD_KNZB
    paths = include_hole_pfad_knzb()
    
    # 2. Local variables
    dwh_job_kennung = 'STAMM_KNZB_ABGL'
    lauf_datum = datetime.now().strftime("%Y%m%d")
    
    # Get current status from DW.VARIABLEN_KNZB variable store
    try:
        knzb_vars = Variable.get("DW_VARIABLEN_KNZB", deserialize_json=True)
    except KeyError:
        knzb_vars = {"ABGLEICH_STATUS": "FREI", "LETZTER_LAUF": ""}

    abgleich_status = knzb_vars.get("ABGLEICH_STATUS", "FREI")
    
    # 3. Check condition: :IF &ABGLEICH_STATUS = "GESPERRT"
    if abgleich_status == "GESPERRT":
        # Original: :PRINT "KNZB-Abgleich fuer &LAUF_DATUM ist gesperrt - Abbruch der Verarbeitung"
        logger.error(f"KNZB-Abgleich fuer {lauf_datum} ist gesperrt - Abbruch der Verarbeitung")
        raise AirflowFailException(f"Abrupt stop triggered by legacy logic. Status: {abgleich_status}")
    
    # 4. Set status variables: :PUT_VAR
    knzb_vars["ABGLEICH_STATUS"] = "LAEUFT"
    knzb_vars["LETZTER_LAUF"] = lauf_datum
    Variable.set("DW_VARIABLEN_KNZB", knzb_vars, serialize_json=True)
    
    # 5. Execute INCLUDE DW.LESE_LOG_KNZB
    include_lese_log_knzb(task_name=task_id, dag_name=dag_id)

def execute_ende_js(**context):
    """
    Translates legacy JOBS 'DW.DWH_STAMM_KNZB_ABGL_ENDE_JS'
    """
    task_id = context['task'].task_id
    dag_id = context['dag'].dag_id
    
    # 1. Execute INCLUDE DW.HOLE_PFAD_KNZB
    paths = include_hole_pfad_knzb()
    
    # 2. Retrieve last run date
    try:
        knzb_vars = Variable.get("DW_VARIABLEN_KNZB", deserialize_json=True)
    except KeyError:
        knzb_vars = {"ABGLEICH_STATUS": "LAEUFT", "LETZTER_LAUF": datetime.now().strftime("%Y%m%d")}
        
    lauf_datum = knzb_vars.get("LETZTER_LAUF", datetime.now().strftime("%Y%m%d"))
    
    # 3. Reset status: :PUT_VAR DW.VARIABLEN_KNZB, ABGLEICH_STATUS, "FREI"
    knzb_vars["ABGLEICH_STATUS"] = "FREI"
    Variable.set("DW_VARIABLEN_KNZB", knzb_vars, serialize_json=True)
    
    # 4. Print completion statement in German (verbatim)
    # Original: :PRINT "KNZB-Stammdatenabgleich fuer Lauf &LAUF_DATUM erfolgreich beendet"
    logger.info(f"KNZB-Stammdatenabgleich fuer Lauf {lauf_datum} erfolgreich beendet")
    
    # 5. Execute INCLUDE DW.LESE_LOG_KNZB
    include_lese_log_knzb(task_name=task_id, dag_name=dag_id)


# ==========================================
# Airflow DAG Definition
# ==========================================
default_args = {
    'owner': 'DWH_STAMM_TEAM',
    'start_date': datetime(2024, 11, 4),
    'retries': 0,
}

with DAG(
    dag_id='DW_DWH_STAMM_KNZB_ABGL_JP',
    default_args=default_args,
    description='Taeglicher Abgleich der Kundennummer-/Basiszugangs-Stammdaten (KNZB) in der DWH-Kernschicht',
    schedule_interval='0 4 * * *',  # Runs daily at 04:00 UTC
    catchup=False,
    tags=['dwh', 'stamm_knzb', 'uc4_migration']
) as dag:

    # Start Task (sets run lock)
    start_task = PythonOperator(
        task_id='DW_DWH_STAMM_KNZB_ABGL_START_JS',
        python_callable=execute_start_js,
        provide_context=True,
    )

    # End Task (releases lock)
    ende_task = PythonOperator(
        task_id='DW_DWH_STAMM_KNZB_ABGL_ENDE_JS',
        python_callable=execute_ende_js,
        provide_context=True,
    )

    # Establish sequence matching UC4 design sequence exactly
    start_task >> ende_task