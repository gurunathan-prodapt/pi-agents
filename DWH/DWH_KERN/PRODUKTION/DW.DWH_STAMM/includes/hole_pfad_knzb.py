"""
Module: hole_pfad_knzb.py
Purpose: Reusable environment path resolution utility mapped from DW.HOLE_PFAD_KNZB.
"""

import logging
from airflow.models import Variable

logger = logging.getLogger("airflow.task")

def resolve_knzb_paths() -> dict:
    """
    Queries environment path variables from Airflow and returns them as a dictionary.
    
    Returns:
        dict: Resolved paths for DWH_HOME, HOME, and ISTNS_HOME.
    """
    try: 
        dw_vars = Variable.get("dw_variablen", deserialize_json=True, default_var={})
        
        # Use runtime retrieval using environment concepts
        dwh_home = dw_vars.get("DWH_HOME", "gs://your-dwh-home-bucket/dwh")
        home = dw_vars.get("HOME", "gs://your-home-bucket/home")
        istns_home = dw_vars.get("ISTNS_HOME", "gs://your-istns-home-bucket/istns")
        
        logger.info("Successfully resolved environment configurations from 'dw_variablen'.")
        logger.info(f"DWH_HOME: {dwh_home}")
        logger.info(f"HOME: {home}")
        logger.info(f"ISTNS_HOME: {istns_home}")
        
        return {
            "DWH_HOME": dwh_home,
            "HOME": home,
            "ISTNS_HOME": istns_home
        }
    except Exception as e:
        error_msg = f"Failed to resolve environment path configurations: {str(e)}"
        logger.error(error_msg)
        raise RuntimeError(error_msg)