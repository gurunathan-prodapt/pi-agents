"""
Task execution logic representing: DW.DWH_STAMM_KNZB_ABGL_START_JS
Verifies that processing is not locked, sets state flags, and logs the initialization phase.
"""

import logging
from datetime import datetime
from airflow.models import Variable
from airflow.exceptions import AirflowFailException
from utils.knzb_helpers import include_hole_pfad_knzb, include_lese_log_knzb

logger = logging.getLogger("airflow.task")

def run_start_js(**context) -> None:
    """
    Executes pre-run verification steps, checks alignment locks, 
    and sets environmental status indicators.
    """
    # Load shared system path configuration
    dwh_home, home, istns_home = include_hole_pfad_knzb()
    
    # Task metadata initialization
    dwh_job_kennung = "STAMM_KNZB_ABGL"
    lauf_datum = datetime.now().strftime("%Y%m%d")
    
    # Retrieve dynamic lock status variable
    abgleich_status = Variable.get("dw_variablen_knzb_abgleich_status", default_var="FREI")
    
    # Process Execution Constraints Checks
    if abgleich_status == "GESPERRT":
        # OUTPUT/PRINT LITERAL RULE: Must maintain German log string character for character
        logger.error(f"KNZB-Abgleich fuer {lauf_datum} ist gesperrt - Abbruch der Verarbeitung")
        raise AirflowFailException("Processing locked downstream. Stopping execution.")
        
    # Atomically write state transitions and indicators back to the Metastore
    Variable.set("dw_variablen_knzb_abgleich_status", "LAEUFT")
    Variable.set("dw_variablen_knzb_letzter_lauf", lauf_datum)
    
    # Write execution record back to logs
    include_lese_log_knzb(
        adm_job="DW.DWH_STAMM_KNZB_ABGL_START_JS", 
        adm_jp="DW.DWH_STAMM_KNZB_ABGL_JP"
    )