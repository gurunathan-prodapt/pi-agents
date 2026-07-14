"""
Module: dwh_env_resolver.py
Source Reference: DW.HOLE_PFAD.xml
Purpose: Reusable module to dynamically derive relative dates and extract 
         global DWH environment variables from Cloud Composer/Airflow.
"""

import os
from datetime import datetime, timedelta
from typing import Dict, Any
from dateutil.relativedelta import relativedelta
from airflow.models import Variable


# Global GCP Environment Configuration (Extracted from Composer Runtime/Environment)
GCP_PROJECT: str = os.environ.get("GCP_PROJECT")
GCP_REGION: str = os.environ.get("GCP_REGION")
GCS_BUCKET: str = os.environ.get("GCS_BUCKET")


def get_dwh_variables() -> Dict[str, str]:
    """
    Returns a mapped dictionary of configurations from Airflow Variables.
    Uses default fallbacks matching the legacy UC4 variables.
    """
    return {
        "DWH_HOME": Variable.get("DWH_HOME", default_value="/opt/dwh"),
        "HOME": Variable.get("HOME", default_value="/home/dwh"),
        "KWS_HOME": Variable.get("KWS_HOME", default_value=""),
        "PMS_HOME": Variable.get("PMS_HOME", default_value=""),
        "ISTNS_HOME": Variable.get("ISTNS_HOME", default_value=""),
        "AKTIV_CARMEN": Variable.get("AKTIV_CARMEN", default_value="0"),
        "AKTIV_CRS": Variable.get("AKTIV_CRS", default_value="0"),
        "AKTIV_CTEL": Variable.get("AKTIV_CTEL", default_value="0"),
        "AKTIV_DPPS": Variable.get("AKTIV_DPPS", default_value="0"),
        "AKTIV_KDS": Variable.get("AKTIV_KDS", default_value="0"),
        "AKTIV_WUERFEL": Variable.get("AKTIV_WUERFEL", default_value="0"),
        "AKTIV_XTRA": Variable.get("AKTIV_XTRA", default_value="0"),
        "AKTUELL_CACHE": Variable.get("AKTUELL_CACHE", default_value=""),
    }


def calculate_relative_dates(reference_date: datetime) -> Dict[str, str]:
    """
    Performs calendar math relative to the DAG execution logical date.
    
    Args:
        reference_date (datetime): Typically context['logical_date']
        
    Returns:
        Dict[str, str]: Derived dates in YYYYMM format.
    """
    # 1. Calculate LASTMONTH_YYYYMM (Last day of preceding month)
    first_of_current_month = reference_date.replace(day=1)
    last_day_of_last_month = first_of_current_month - timedelta(days=1)
    last_month_yyyymm = last_day_of_last_month.strftime("%Y%m")
    
    # 2. Calculate PRELASTMONTH_YYYYMM (Subtract 2 months from current first)
    pre_last_month_date = first_of_current_month - relativedelta(months=2)
    pre_last_month_yyyymm = pre_last_month_date.strftime("%Y%m")
    
    # 3. Calculate NEXTMONTH_YYYYMM (Add 1 month to reference date)
    next_month_date = reference_date + relativedelta(months=1)
    next_month_yyyymm = next_month_date.strftime("%Y%m")

    return {
        "LASTMONTH_YYYYMM": last_month_yyyymm,
        "PRELASTMONTH_YYYYMM": pre_last_month_yyyymm,
        "NEXTMONTH_YYYYMM": next_month_yyyymm,
    }


def trigger_job_monitor_start(context: Dict[str, Any]) -> None:
    """
    Functional Hook mimicking legacy :INC DW.DWH_ADM_JOB_MONITOR_START.xml
    """
    dag_id = context['dag'].dag_id
    logical_date = context['logical_date'].isoformat()
    print(f"[JOB MONITOR START] Execution registered for DAG: {dag_id} | Logical Date: {logical_date}")


def resolve_hole_pfad_context(**context) -> None:
    """
    Airflow task executable entrypoint. Resolves variables, performs date calculations,
    pushes outputs to XComs, and registers execution start.
    """
    ti = context['ti']
    logical_date = context['logical_date']

    # Resolve variables and relative times
    dwh_variables = get_dwh_variables()
    derived_dates = calculate_relative_dates(logical_date)

    # Push to XCom for downstream task consumption
    ti.xcom_push(key='dwh_paths', value=dwh_variables)
    ti.xcom_push(key='dwh_dates', value=derived_dates)

    # Trigger job monitoring start hook
    trigger_job_monitor_start(context)