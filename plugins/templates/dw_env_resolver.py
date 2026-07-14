"""
Module: dw_env_resolver
Description: Replicates the legacy UC4 'DW.HOLE_PFAD' configuration and date calculations.
"""

from datetime import datetime, date
from typing import Dict, Any, Union
from dateutil.relativedelta import relativedelta
from airflow.models import Variable


def get_airflow_variables() -> Dict[str, str]:
    """
    Retrieves global configurations and activation flags from the Airflow Variable store.
    Provides standard fallback defaults if the keys are not present.
    """
    return {
        # Directory Paths
        "DWH_HOME": Variable.get("dwh_home", default_var="gs://your-production-bucket/dwh"),
        "HOME": Variable.get("home", default_var="gs://your-production-bucket/home"),
        "KWS_HOME": Variable.get("kws_home", default_var="gs://your-production-bucket/kws"),
        "PMS_HOME": Variable.get("pms_home", default_var="gs://your-production-bucket/pms"),
        "ISTNS_HOME": Variable.get("istns_home", default_var="gs://your-production-bucket/istns"),
        
        # System Activation / Deactivation Indicators (AKTIV_*)
        "AKTIV_CARMEN": Variable.get("aktiv_carmen", default_var="0"),
        "AKTIV_CRS": Variable.get("aktiv_crs", default_var="0"),
        "AKTIV_CTEL": Variable.get("aktiv_ctel", default_var="0"),
        "AKTIV_DPPS": Variable.get("aktiv_dpps", default_var="0"),
        "AKTIV_KDS": Variable.get("aktiv_kds", default_var="0"),
        "AKTIV_WUERFEL": Variable.get("aktiv_wuerfel", default_var="0"),
        "AKTIV_XTRA": Variable.get("aktiv_xtra", default_var="0"),
        "AKTUELL_CACHE": Variable.get("aktuell_cache_dwk_kkm", default_var="0")
    }


def calculate_date_windows(base_date: Union[str, datetime, date]) -> Dict[str, str]:
    """
    Performs calendar calculations mirroring the legacy UC4 date boundaries.
    
    Calculations:
    - PRELASTMONTH_YYYYMM: The month 2 months prior to the execution date's month.
    - LASTMONTH_YYYYMM: The month immediately prior to the execution date's month.
    - NEXTMONTH_YYYYMM: The month immediately following the execution date's month.
    """
    if isinstance(base_date, str):
        parsed_date = datetime.strptime(base_date, "%Y-%m-%d")
    elif isinstance(base_date, (datetime, date)):
        parsed_date = base_date
    else: 
        raise TypeError("base_date must be a YYYY-MM-DD string, datetime, or date object.")

    # Always anchor calculations from the 1st of the execution month to prevent overflow issues
    first_of_month = datetime(parsed_date.year, parsed_date.month, 1)

    pre_last_month = first_of_month - relativedelta(months=2)
    last_month = first_of_month - relativedelta(months=1)
    next_month = first_of_month + relativedelta(months=1)

    return {
        "PRELASTMONTH_YYYYMM": pre_last_month.strftime("%Y%m"),
        "LASTMONTH_YYYYMM": last_month.strftime("%Y%m"),
        "NEXTMONTH_YYYYMM": next_month.strftime("%Y%m")
    }


def compute_environment_context(logical_date: str, **context: Any) -> Dict[str, str]:
    """
    Main entry point mimicking 'DW.HOLE_PFAD'. Computes date strings, 
    merges them with variables retrieved from Airflow, and returns the context map.
    """
    print(f"Resolving environment paths and parameters for logical date: {logical_date}")
    
    env_vars = get_airflow_variables()
    date_vars = calculate_date_windows(logical_date)
    
    # Merge environments and dates
    resolved_context = {**env_vars, **date_vars}
    
    print(f"Resolved Environment Context: {resolved_context}")
    return resolved_context