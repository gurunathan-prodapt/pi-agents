"""
Utility module to resolve and validate environment path variables.
Migrated from UC4 Include Script (JOBI) 'DW.HOLE_PFAD_KNZB'.
"""

import os
import logging
from typing import Dict
from airflow.models import Variable
from airflow.exceptions import AirflowException

logger = logging.getLogger("airflow.task")


def get_path_variables() -> Dict[str, str]:
    """
    Retrieves and resolves path variables from Airflow Variables or falls back 
    to standard environment variables.

    Returns:
        Dict[str, str]: A dictionary containing resolved paths for 'DWH_HOME',
                        'HOME', and 'ISTNS_HOME'.
    
    Raises:
        AirflowException: If critical variables are missing or resolve to empty values.
    """
    logger.info("Resolving path variables equivalent to UC4 'DW.HOLE_PFAD_KNZB'...")

    # Fetch variables from Airflow configuration with runtime fallbacks
    gcp_bucket = os.environ.get("GCS_BUCKET")
    dwh_home_fallback = f"gs://{gcp_bucket}/dwh/" if gcp_bucket else None
    home_fallback = f"gs://{gcp_bucket}/home/" if gcp_bucket else None
    istns_home_fallback = f"gs://{gcp_bucket}/istns/" if gcp_bucket else None

    dwh_home = Variable.get("dw_variablen_dwh_home", default_var=dwh_home_fallback)
    home = Variable.get("dw_variablen_home", default_var=home_fallback)
    istns_home = Variable.get("dw_variablen_istns_home", default_var=istns_home_fallback)

    # Validate configuration values are populated
    if not dwh_home or not home or not istns_home:
        error_msg = (
            f"Missing required path variable configurations! "
            f"DWH_HOME: {dwh_home}, HOME: {home}, ISTNS_HOME: {istns_home}"
        )
        logger.error(error_msg)
        raise AirflowException(error_msg)

    logger.info(f"Successfully loaded DWH_HOME: {dwh_home}")
    logger.info(f"Successfully loaded HOME: {home}")
    logger.info(f"Successfully loaded ISTNS_HOME: {istns_home}")

    return {
        "DWH_HOME": dwh_home,
        "HOME": home,
        "ISTNS_HOME": istns_home
    }


def verify_and_load_paths_callable(**context) -> None:
    """
    PythonOperator-compatible wrapper that pushes path variables to Airflow XCom.
    """
    resolved_paths = get_path_variables()
    
    ti = context["ti"]
    for key, value in resolved_paths.items():
        ti.xcom_push(key=key, value=value)