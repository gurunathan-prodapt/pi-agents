import logging
from airflow.models import Variable

def hole_pfad_knzb():
    """
    Simulates the Include: DW.HOLE_PFAD_KNZB
    Retrieves path variables from the central variable container or airflow variables.
    """
    try:
        global_vars = Variable.get("dw_variablen", deserialize_json=True)
        dwh_home = global_vars.get("DWH_HOME")
        home = global_vars.get("HOME")
        istns_home = global_vars.get("ISTNS_HOME")
        logging.info(f"Loaded paths: DWH_HOME={dwh_home}, HOME={home}, ISTNS_HOME={istns_home}")
        return dwh_home, home, istns_home
    except Exception as e:
        logging.warning(f"Could not load standard path variables: {str(e)}")
        return None, None, None