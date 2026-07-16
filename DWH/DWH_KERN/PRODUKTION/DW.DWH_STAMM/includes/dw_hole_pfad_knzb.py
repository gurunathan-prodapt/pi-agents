# -*- coding: utf-8 -*-
"""
Migrated from JOBI DW.HOLE_PFAD_KNZB
Provides centralized environment path variable resolution using Airflow Variables.
"""

from typing import Dict
from airflow.models import Variable


def get_knzb_paths(use_fallback: bool = False) -> Dict[str, str]:
    """
    Retrieves execution paths from Airflow's global Variable store.
    Matches variables mapped from UC4 container 'DW.VARIABLEN'.

    Args:
        use_fallback (bool): If True, returns predefined default values 
                             instead of raising an exception if a key is missing.

    Returns:
        Dict[str, str]: Dictionary containing paths for DWH_HOME, HOME, and ISTNS_HOME.

    Raises:
        KeyError: If any of the variables are missing in Airflow and use_fallback is False.
    """
    try:
        if use_fallback:
            return {
                "DWH_HOME": Variable.get("dw_variablen_dwh_home", default_var="/opt/dwh_home"),
                "HOME": Variable.get("dw_variablen_home", default_var="/home/dwh_user"),
                "ISTNS_HOME": Variable.get("dw_variablen_istns_home", default_var="/opt/istns_home")
            }
        
        # Standard strict resolution to prevent execution with incomplete path configurations
        return {
            "DWH_HOME": Variable.get("dw_variablen_dwh_home"),
            "HOME": Variable.get("dw_variablen_home"),
            "ISTNS_HOME": Variable.get("dw_variablen_istns_home")
        }
    except KeyError as err:
        raise KeyError(
            f"Required Airflow Variable missing: {str(err)}. "
            "Please configure the following keys in your Cloud Composer environment: "
            "'dw_variablen_dwh_home', 'dw_variablen_home', 'dw_variablen_istns_home'."
        ) from err