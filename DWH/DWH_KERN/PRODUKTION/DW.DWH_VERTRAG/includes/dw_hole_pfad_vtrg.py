"""
Module: dw_hole_pfad_vtrg.py
Purpose: Shared helper to fetch global environment path variables.
"""
from airflow.models import Variable


def get_path_variables() -> dict:
    """Reads path environment variables from the shared Airflow Variables.

    Falls back to default paths if variables are not explicitly set.
    """
    dwh_home = Variable.get("GCP_DWH_HOME", default_var="/opt/dwh")
    home = Variable.get("GCP_HOME", default_var="/home/dwh_user")
    pms_home = Variable.get("GCP_PMS_HOME", default_var="/opt/pms")

    return {"DWH_HOME": dwh_home, "HOME": home, "PMS_HOME": pms_home}