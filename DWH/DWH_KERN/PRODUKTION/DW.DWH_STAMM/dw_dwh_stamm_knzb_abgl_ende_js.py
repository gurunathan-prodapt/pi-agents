"""
Task execution logic representing: DW.DWH_STAMM_KNZB_ABGL_ENDE_JS
Releases execution states, logs statistics, and marks workflow completion.
"""

import logging
from airflow.models import Variable
from utils.knzb_helpers import include_hole_pfad_knzb, include_lese_log_knzb

logger = logging.getLogger("airflow.task")

def run_ende_js(**context) -> None:
    """
    Executes post-run verification, resets execution locks, 
    and records completion signatures.
    """
    # Load shared system path configuration
    dwh_home, home, istns_home = include_hole_pfad_knzb()
    
    # Retrieve current running date representation
    lauf_datum = Variable.get("dw_variablen_knzb_letzter_lauf", default_var="UNKNOWN")
    
    # Release Lock Status Variable for subsequent daily runs
    Variable.set("dw_variablen_knzb_abgleich_status", "FREI")
    
    # OUTPUT/PRINT LITERAL RULE: Verbatim print translation matching original execution logs
    logger.info(f"KNZB-Stammdatenabgleich fuer Lauf {lauf_datum} erfolgreich beendet")
    
    # Write final execution block summary log
    include_lese_log_knzb(
        adm_job="DW.DWH_STAMM_KNZB_ABGL_ENDE_JS", 
        adm_jp="DW.DWH_STAMM_KNZB_ABGL_JP"
    )