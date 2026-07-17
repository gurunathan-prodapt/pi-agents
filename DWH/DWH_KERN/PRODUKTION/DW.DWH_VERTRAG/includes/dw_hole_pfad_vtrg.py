import os
from airflow.models import Variable

def get_vtrg_paths():
    """
    Standard-Include zum Auslesen der Pfad-Variablen aus dem Variablencontainer DW.VARIABLEN.
    Retrieves global paths using Airflow Variables, falling back to OS environment.
    """
    # Try fetching from Airflow Variables (Global Config Store), default to os.environ
    dwh_home = Variable.get("DWH_HOME", default_var=os.environ.get("DWH_HOME"))
    home = Variable.get("HOME", default_var=os.environ.get("HOME"))
    pms_home = Variable.get("PMS_HOME", default_var=os.environ.get("PMS_HOME"))
    
    return {
        "DWH_HOME": dwh_home,
        "HOME": home,
        "PMS_HOME": pms_home
    }