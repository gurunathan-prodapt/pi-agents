"""
Module: DW.HOLE_PFAD_KNZB
Description: Utility module to fetch core path configuration values from the Airflow 
             Variable Metadata DB. Emulates the legacy UC4 GET_VAR lookup against 
             the 'DW.VARIABLEN' container.
"""

import logging
from typing import Dict
from airflow.models import Variable
from airflow.exceptions import AirflowException


def get_path_variables(**context) -> Dict[str, str]:
    """
    Standard-Include zum Auslesen der Pfad-Variablen aus dem Variablencontainer DW.VARIABLEN.
    Loads configurations from Airflow Variables and pushes them to XCom.

    Args:
        **context: Airflow task execution context dictionary.

    Returns:
        Dict[str, str]: Dictionary containing paths for 'DWH_HOME', 'HOME', and 'ISTNS_HOME'.
    """
    logging.info("Resolving environment paths (DW.HOLE_PFAD_KNZB equivalent)")
    try: 
        # Retrieve target variables from Airflow variable store
        dwh_home = Variable.get("dw_variablen_dwh_home")
        home = Variable.get("dw_variablen_home")
        istns_home = Variable.get("dw_variablen_istns_home")

        logging.info(f"Loaded DWH_HOME: {dwh_home}")
        logging.info(f"Loaded HOME: {home}")
        logging.info(f"Loaded ISTNS_HOME: {istns_home}")

        # Check if the execution context is present before pushing to XCom
        if context and 'ti' in context:
            ti = context['ti']
            ti.xcom_push(key='DWH_HOME', value=dwh_home)
            ti.xcom_push(key='HOME', value=home)
            ti.xcom_push(key='ISTNS_HOME', value=istns_home)

        return {
            "DWH_HOME": dwh_home,
            "HOME": home,
            "ISTNS_HOME": istns_home
        }
    except Exception as e:
        raise AirflowException(
            f"Error fetching path variables from Airflow metadata store: {str(e)}"
        )