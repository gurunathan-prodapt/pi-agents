import logging
from airflow.models import Variable

logger = logging.getLogger("airflow.task")

def include_hole_pfad_knzb():
    """
    Translates legacy JOBI 'DW.HOLE_PFAD_KNZB'
    Retrieves variables from 'DW.VARIABLEN' Airflow variables.
    """
    try:
        dw_variablen = Variable.get("DW_VARIABLEN", deserialize_json=True)
    except KeyError:
        # Fallbacks for variables if not defined in the Airflow environment
        dw_variablen = {
            "DWH_HOME": "/opt/dwh",
            "HOME": "/home/airflow",
            "ISTNS_HOME": "/opt/istns"
        }
    
    dwh_home = dw_variablen.get("DWH_HOME")
    home = dw_variablen.get("HOME")
    istns_home = dw_variablen.get("ISTNS_HOME")
    
    logger.info(f"Loaded paths - DWH_HOME: {dwh_home}, HOME: {home}, ISTNS_HOME: {istns_home}")
    return {
        "DWH_HOME": dwh_home,
        "HOME": home,
        "ISTNS_HOME": istns_home
    }