from airflow.models import Variable

def load_env_paths():
    """
    Emulates the DW.HOLE_PFAD_VTRG include logic.
    Provides standard environment paths extracted from the Airflow Variable Store.
    """
    return {
        "DWH_HOME": Variable.get("dw_variablen_dwh_home", default_var="/opt/dwh"),
        "HOME": Variable.get("dw_variablen_home", default_var="/home/dwh"),
        "PMS_HOME": Variable.get("dw_variablen_pms_home", default_var="/opt/pms")
    }