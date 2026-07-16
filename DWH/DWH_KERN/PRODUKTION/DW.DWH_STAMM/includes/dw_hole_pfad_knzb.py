"""
Module: dags/includes/dw_hole_pfad_knzb.py
Purpose: Resolves legacy environment path variables using Airflow Variables.
Source Reference: DWH/DWH_KERN/PRODUKTION/DW.DWH_STAMM/includes/DW.HOLE_PFAD_KNZB.xml
"""

from typing import Dict
from airflow.models import Variable


def resolve_paths() -> Dict[str, str]:
    """Resolves and returns legacy environment path variables.

    Retrieves system paths from Airflow Variable configurations, providing
    sane defaults matching the system specifications.

    Returns:
        Dict[str, str]: A dictionary containing environmental configuration keys:
                        - DWH_HOME: Path to the DWH installation.
                        - HOME: Home path of the operational system user.
                        - ISTNS_HOME: Home path for the source system.
                        - DWH_JOB_KENNUNG: Static task descriptor.
    """
    return {
        "DWH_HOME": Variable.get("DWH_HOME", default_var="/opt/dwh"),
        "HOME": Variable.get("HOME", default_var="/home/dwh_user"),
        "ISTNS_HOME": Variable.get("ISTNS_HOME", default_var="/opt/istns"),
        "DWH_JOB_KENNUNG": "STAMM_KNZB_ABGL",
    }