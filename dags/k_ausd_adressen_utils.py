# Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_adressen.ksh
# Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_adressen.ksh

import datetime
from airflow.exceptions import AirflowFailException

def validate_date(date_str: str, date_format: str = '%Y%m%d') -> bool:
    """
    Validates if a date string matches the expected format.
    Replicates the functionality of DWDate_Datum_Check.
    """
    try:
        datetime.datetime.strptime(date_str, date_format)
        return True
    except ValueError:
        return False

def calculate_yesterday_today(stichtag: str, date_format: str = '%Y%m%d') -> tuple[str, str]:
    """
    Calculates yesterday's and today's dates based on a given stichtag.
    Replicates the functionality of gestern.ksh.
    """
    try:
        current_date = datetime.datetime.strptime(stichtag, date_format)
        yesterday = current_date - datetime.timedelta(days=1)
        return current_date.strftime(date_format), yesterday.strftime(date_format)
    except ValueError as e:
        raise AirflowFailException(f"Error calculating dates with stichtag '{stichtag}': {e}")

def pruefe_parameter_gesetzt(param_name: str, param_value: str):
    """
    Checks if a parameter is set (not None or empty string).
    Replicates the functionality of pruefeParameterGesetzt.
    """
    if not param_value:
        raise AirflowFailException(f"Parameter '{param_name}' is not set.")

def log_error(err_nr: int, err_arg: str, message: str):
    """
    A placeholder for custom error logging. In Airflow, this would typically
    integrate with Airflow's logging system.
    Replicates the functionality of DWMSG_MeldeFehler.
    """
    print(f"ERROR: {err_nr} {err_arg} - {message}")
    # In a real scenario, this might trigger an alert or a specific Airflow XCom.
    raise AirflowFailException(f"Script failed with error {err_nr}: {message}")

---