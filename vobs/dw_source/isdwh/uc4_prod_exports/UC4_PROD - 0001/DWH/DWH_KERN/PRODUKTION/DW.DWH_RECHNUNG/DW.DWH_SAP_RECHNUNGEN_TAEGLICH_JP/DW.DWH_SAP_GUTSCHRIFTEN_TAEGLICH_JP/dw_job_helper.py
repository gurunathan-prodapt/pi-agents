"""
Utility module translating the legacy UC4 Include ('JOBI') logics:
- `DW.HOLE_PFAD`: Dynamic parameter calculation, paths, system activation states.
- `DW.LESE_LOG`: Standardized execution logging, return-code routing, and alerting.
"""

import logging
import sys
from datetime import datetime
from typing import Any, Dict, Optional
from dateutil.relativedelta import relativedelta
from airflow.models import Variable

# Initialize logger to align with Airflow task instance execution logs
logger = logging.getLogger("airflow.task")


def calculate_dwh_variables(logical_date_str: str) -> Dict[str, Optional[str]]:
    """
    Translates the legacy dynamic variable setting logic from DW.HOLE_PFAD.
    
    This calculation uses the DAG's logical execution date to guarantee 
    idempotence during retrospective historical backfills.

    Args:
        logical_date_str (str): Airflow execution/logical date in 'YYYY-MM-DD' format.

    Returns:
        Dict[str, Optional[str]]: Calculated date variables and lookup configurations.
    """
    try:
        exec_date = datetime.strptime(logical_date_str, "%Y-%m-%d")
    except ValueError as err:
        logger.error(f"Invalid logical date format provided: {logical_date_str}. Expected 'YYYY-MM-DD'.")
        raise err

    # -- Date Calculation Logic (Replicating UC4 Date Arithmetic) --
    # Reference legacy: :set &first = '01'
    current_month_first = exec_date.replace(day=1)
    
    # Legacy: :set &PRELASTMONTH_YYYYMM = SUB_PERIOD("...","MM:2","YYYYMMDD")
    pre_last_month = current_month_first - relativedelta(months=2)
    pre_last_month_yyyymm = pre_last_month.strftime("%Y%m")
    
    # Legacy: :set &LASTMONTH_YYYYMM = SUB_DAYS(&LASTMONTH_YYYYMM, 1) (Subtract 1 day from 1st of current month)
    last_month = current_month_first - relativedelta(days=1)
    last_month_yyyymm = last_month.strftime("%Y%m")
    
    # Legacy: :set &NEXTMONTH_YYYYMM = ADD_PERIOD("...","MM:1","YYYYMMDD")
    next_month = exec_date + relativedelta(months=1)
    next_month_yyyymm = next_month.strftime("%Y%m")

    # -- Global Variable Map (Replicating GET_VAR calls with fallback defaults) --
    dwh_variables = {
        # System Paths
        "DWH_HOME": Variable.get("dwh_home", default_var="/home/dwh"),
        "HOME": Variable.get("home", default_var="/home"),
        "KWS_HOME": Variable.get("kws_home", default_var=None),
        "PMS_HOME": Variable.get("pms_home", default_var=None),
        "ISTNS_HOME": Variable.get("istns_home", default_var=None),
        
        # System Triggers/Activation Flags (0/1)
        "AKTIV_CARMEN": Variable.get("aktiv_carmen", default_var="0"),
        "AKTIV_CRS": Variable.get("aktiv_crs", default_var="0"),
        "AKTIV_CTEL": Variable.get("aktiv_ctel", default_var="0"),
        "AKTIV_DPPS": Variable.get("aktiv_dpps", default_var="0"),
        "AKTIV_KDS": Variable.get("aktiv_kds", default_var="0"),
        "AKTIV_WUERFEL": Variable.get("aktiv_wuerfel", default_var="0"),
        "AKTIV_XTRA": Variable.get("aktiv_xtra", default_var="0"),
        "AKTUELL_CACHE": Variable.get("aktuell_cache", default_var=None),
        
        # Derived Dynamic Date boundaries
        "LASTMONTH_YYYYMM": last_month_yyyymm,
        "PRELASTMONTH_YYYYMM": pre_last_month_yyyymm,
        "NEXTMONTH_YYYYMM": next_month_yyyymm,
    }

    logger.info("Successfully derived legacy UC4 environment variables:")
    for key, value in dwh_variables.items():
        logger.info(f"Variable Setup: {key} = {value}")

    return dwh_variables


def evaluate_job_status(return_code: int, job_identifier: str) -> None:
    """
    Translates legacy DW.LESE_LOG post-execution monitoring.
    
    Ensures structural fidelity for success/failure reporting in the Airflow 
    task execution log, replacing the need for 'SHOWLOG.KSH'.

    Args:
        return_code (int): The exit status code of the preceding execution step.
        job_identifier (str): Unique legacy DWH job identifier context ('&DWH_JOB_KENNUNG').
    
    Raises:
        SystemExit: Propagates the original return code to notify the Airflow execution pool.
    """
    separator = "****************************************************************"
    
    if return_code != 0:
        logger.error(f"Airflow Task Execution Failure detected for Job Scope: {job_identifier}")
        logger.error(separator)
        logger.error(f"Rueckgabewert: '{return_code}' (Fehlerfall)***************************")
        logger.error(separator)
        sys.exit(return_code)
    else:
        logger.info(separator)
        logger.info(f"Rueckgabewert: '{return_code}' ***************************************")
        logger.info(separator)
        sys.exit(0)