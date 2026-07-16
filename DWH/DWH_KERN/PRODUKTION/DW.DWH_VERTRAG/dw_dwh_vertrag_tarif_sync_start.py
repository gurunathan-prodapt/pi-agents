"""
Task logic for executing the start-control block.
"""

import logging
from datetime import datetime
from airflow.exceptions import AirflowFailException
from airflow.models import Variable

from tasks.utils import resolve_environment_paths, write_standard_log


def execute_start_task(**context) -> None:
    """
    Checks lock state, sets synchronization status, and updates run logs.
    """
    dag_id = context['dag'].dag_id
    task_id = context['task'].task_id

    # 1. Path Lookup logic (Merged Include: DW.HOLE_PFAD_VTRG)
    _ = resolve_environment_paths()
    
    # 2. Start-JS Script Execution Logic
    lauf_datum = datetime.now().strftime("%Y%m%d")
    
    # Retrieve variable 'SYNC_STATUS' (Mapped to Airflow Variable 'vtrg_sync_status')
    sync_status = Variable.get("vtrg_sync_status", default_var="FREI")
    
    if sync_status == "GESPERRT":
        # Rule output exact match: "Vertrags-/Tarifabgleich fuer &LAUF_DATUM ist gesperrt - Abbruch"
        logging.error(f"Vertrags-/Tarifabgleich fuer {lauf_datum} ist gesperrt - Abbruch")
        raise AirflowFailException(f"Job aborted because sync is locked: {sync_status}")
        
    # Update active state variables
    Variable.set("vtrg_sync_status", "LAEUFT")
    Variable.set("vtrg_letzter_lauf", lauf_datum)
    logging.info(f"Set vtrg_sync_status to 'LAEUFT' and vtrg_letzter_lauf to '{lauf_datum}'")
    
    # 3. Write Log logic (Merged Include: DW.LESE_LOG_VTRG)
    write_standard_log(task_id, dag_id)