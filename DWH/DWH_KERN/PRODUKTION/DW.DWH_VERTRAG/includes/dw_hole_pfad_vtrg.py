"""
Module: dw_hole_pfad_vtrg
Purpose: Shared Airflow Python variable loader module. Reads global configuration 
         parameters from the Airflow Variable 'dw_variablen' in JSON format.
"""

from typing import Dict
from airflow.models import Variable
from airflow.exceptions import AirflowException


def load_dw_variables() -> Dict[str, str]:
    """Retrieves UC4 path constants from the Airflow Variable metastore.

    Required keys inside Airflow JSON Variable 'dw_variablen':
        - dwh_home: Path or GCS URI for DWH
        - home: Path for Airflow home directory
        - pms_home: Path or GCS URI for PMS

    Returns:
        Dict[str, str]: A dictionary containing environment-wide configurations:
            - "DWH_HOME"
            - "HOME"
            - "PMS_HOME"

    Raises:
        AirflowException: If the variable is missing or keys are incomplete.
    """
    try: 
        # Retrieve and deserialize the unified variable container
        dw_vars = Variable.get("dw_variablen", deserialize_json=True)
    except KeyError:
        raise AirflowException(
            "The Airflow Variable 'dw_variablen' does not exist in the environment."
        )
    except Exception as e: 
        raise AirflowException(
            f"Failed to retrieve or deserialize 'dw_variablen': {str(e)}"
        )

    # Extract required keys
    dwh_home = dw_vars.get("dwh_home")
    home = dw_vars.get("home")
    pms_home = dw_vars.get("pms_home")

    # Validate presence of values
    missing_keys = [
        key
        for key, val in [
            ("dwh_home", dwh_home),
            ("home", home),
            ("pms_home", pms_home),
        ]
        if not val
    ]

    if missing_keys:
        raise AirflowException(
            f"Missing required key(s) in 'dw_variablen' Variable: {', '.join(missing_keys)}"
        )

    return {"DWH_HOME": dwh_home, "HOME": home, "PMS_HOME": pms_home}