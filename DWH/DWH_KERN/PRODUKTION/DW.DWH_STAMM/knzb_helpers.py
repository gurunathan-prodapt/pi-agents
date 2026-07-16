"""
Reusable helper utilities for the KNZB (Kundennummer-/Basiszugangs-Stammdaten) pipeline.
Translates shared UC4 includes (DW.HOLE_PFAD_KNZB, DW.LESE_LOG_KNZB) into Python helper functions.
"""

import logging
from typing import Tuple
from airflow.models import Variable

logger = logging.getLogger("airflow.task")

def include_hole_pfad_knzb() -> Tuple[str, str, str]:
    """
    Simulates the legacy include: :inc DW.HOLE_PFAD_KNZB
    Extracts global operational and system path structures.
    
    Returns:
        Tuple[str, str, str]: (dwh_home, home, istns_home)
    """
    dwh_home = Variable.get("DWH_HOME", default_var="/home/gurunathan_t/clean_migration_dataset")
    home = Variable.get("HOME", default_var="/home/gurunathan_t")
    istns_home = Variable.get("ISTNS_HOME", default_var="/home/gurunathan_t/istns")
    
    return dwh_home, home, istns_home


def include_lese_log_knzb(adm_job: str, adm_jp: str) -> None:
    """
    Simulates the legacy include: :inc DW.LESE_LOG_KNZB
    Writes log entries conforming to the exact German output translation schema.
    
    Args:
        adm_job (str): Name of the executing job task.
        adm_jp (str): Name of the parent orchestration process/Job Plan.
    """
    # OUTPUT/PRINT LITERAL RULE: Must output exact German text string from legacy log
    logger.info(f"Protokolleintrag: {adm_job} innerhalb {adm_jp}")