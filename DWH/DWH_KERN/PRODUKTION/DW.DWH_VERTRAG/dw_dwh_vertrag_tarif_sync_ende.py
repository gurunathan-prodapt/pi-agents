"""
Task logic for executing the end-control block.
"""

import logging
from airflow.models import Variable

from tasks.utils import resolve_environment_paths, write_standard_log


def execute_end_task(**context) -> None:
    """
    Resets the synchronization lock state and writes success audit entries.
    """
    dag_id = context['dag'].dag_id
    task_id = context['task'].task_id

    # 1. Path Lookup logic (Merged Include: DW.HOLE_PFAD_VTRG)
    _ = resolve_environment_paths()

    # 2. Ende-JS Script Execution Logic
    lauf_datum = Variable.get("vtrg_letzter_lauf", default_var="unknown")
    
    # Reset lock status variables to free up pipeline for subsequent runs
    Variable.set("vtrg_sync_status", "FREI")
    logging.info("Set vtrg_sync_status back to 'FREI'")
    
    # Rule output exact match: "Vertrags-/Tarifabgleich fuer Lauf &LAUF_DATUM erfolgreich beendet"
    logging.info(f"Vertrags-/Tarifabgleich fuer Lauf {lauf_datum} erfolgreich beendet")
    
    # 3. Write Log logic (Merged Include: DW.LESE_LOG_VTRG)
    write_standard_log(task_id, dag_id)