"""
Utility module for Ab Initio integration.
Contains reusable functions for polling status and updating execution states.
"""

import time
import logging
from datetime import datetime
from airflow.models import Variable
from airflow.exceptions import AirflowException

# Shared Variable Configuration
VAR_NAME = "dw_adm_ab_initio_var"
APPLICATION_KEY = "status_dwh"
DEFAULT_WAIT_INTERVAL = 10  # Seconds between polls


def get_current_timestamps():
    """Generates standardized time and date formats."""
    now = datetime.utcnow()
    return now.strftime('%H:%M:%S'), now.strftime('%d.%m.%Y')


def update_variable_state(key: str, value: str) -> None:
    """
    Safely retrieves, updates, and writes back the JSON-backed Airflow Variable.
    Prevents wiping out other active keys in the same variable.
    """
    try:
        state_dict = Variable.get(VAR_NAME, deserialize_json=True, default_var={})
    except Exception as e:
        logging.warning(f"Could not deserialize variable {VAR_NAME}. Resetting to empty dict. Error: {e}")
        state_dict = {}

    state_dict[key] = value
    Variable.set(VAR_NAME, state_dict, serialize_json=True)
    logging.info(f"Updated variable '{VAR_NAME}' key '{key}' to: '{value}'")


def poll_ab_initio_status_fn(**context) -> None:
    """
    Implements the polling loop from the UC4 JOBI Start script.
    Checks the application state inside `dw_adm_ab_initio_var`.
    - If status is 'go': marks task state as ACTIVE and returns successfully.
    - If status is 'exit1': marks state as aborted and raises AirflowException.
    - Otherwise, sleeps for the defined interval and polls again.
    """
    jobplan_name = context['dag'].dag_id
    job_name = context['task'].task_id
    betr_job = f"{jobplan_name} -> {job_name}"
    
    time_str, date_str = get_current_timestamps()
    
    # Initialize state inside the tracker variable
    initial_wait_msg = f"WAIT for Ab Initio ({time_str} {date_str})"
    update_variable_state(betr_job, initial_wait_msg)
    
    logging.info(f"Der Pruefung laeuft fuer {job_name} im Jobplan {jobplan_name}")
    
    while True:
        # Retrieve the updated dict on each loop iteration
        try:
            state_dict = Variable.get(VAR_NAME, deserialize_json=True, default_var={})
        except Exception as e:
            logging.error(f"Failed to fetch variable '{VAR_NAME}' during poll loop: {e}")
            time.sleep(DEFAULT_WAIT_INTERVAL)
            continue
            
        status_appl = state_dict.get(APPLICATION_KEY, "wait")
        time_str, _ = get_current_timestamps()
        
        logging.info(f"Der Status fuer die Applikation DWH ist: {status_appl} ({time_str})")
        
        if status_appl == "go":
            _, date_str = get_current_timestamps()
            success_msg = f"ACTIVE in Ab Initio {time_str} {date_str}"
            logging.info(f"Pruefung erfolgreich, starte Ab Initio Job(s) ({time_str})")
            update_variable_state(betr_job, success_msg)
            break
            
        elif status_appl == "exit1":
            fail_msg = f"Pruefjob wurde abgebrochen {time_str}"
            logging.error(f"Der Status fuer den Pruefjob wurde auf exit1 gesetzt, beende Pruefjob ({time_str})")
            update_variable_state(betr_job, fail_msg)
            raise AirflowException("Ab Initio check returned failure state (exit1). Execution halted.")
            
        logging.info(f"PRUEFE ... ({time_str})")
        time.sleep(DEFAULT_WAIT_INTERVAL)


def update_ab_initio_status_fn(**context) -> None:
    """
    Implements the UC4 End script execution tracker logic.
    Updates the execution state variable to complete ('fertig').
    """
    applikation = "DWH"
    jobplan_name = context['dag'].dag_id
    job_name = context['task'].task_id
    betr_job = f"{jobplan_name} -> {job_name}"
    
    time_str, date_str = get_current_timestamps()
    
    # Fetch status for verification logging
    state_dict = Variable.get(VAR_NAME, deserialize_json=True, default_var={})
    status_appl = state_dict.get(APPLICATION_KEY, "UNKNOWN")
    
    status_fertig = f"fertig ({time_str} {date_str})"
    
    logging.info(f"Der Prüfjob {job_name} läuft im Jobplan {jobplan_name}")
    logging.info(f"Der Status für die Applikation {applikation} ist: {status_appl} ({time_str} {date_str})")
    logging.info(f"Die Ab Initio Verarbeitung ist fertig. Der Status wird auf {status_fertig} umgesetzt.")
    
    # Save finished status back to state tracking variable
    update_variable_state(betr_job, status_fertig)