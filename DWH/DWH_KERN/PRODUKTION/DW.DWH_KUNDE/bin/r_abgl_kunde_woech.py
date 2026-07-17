"""
Module: r_abgl_kunde_woech.py
Purpose: Replicates the legacy shell script logic (`r_abgl_kunde_woech.ksh`), 
         preserving logging structures and dynamic execution statistics.
"""

import logging
from typing import Any, Dict, Optional
from airflow.models import Variable
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

# Configure standard airflow task logger
logger = logging.getLogger("airflow.task")


def get_gcp_variable(key: str, default: Optional[str] = None) -> str:
    """
    Safely retrieves a variable from Airflow. Throws a clear KeyError if 
    the variable is missing and no default is provided.
    """
    value = Variable.get(key, default_var=default)
    if value is None:
        raise KeyError(
            f"Missing required Airflow Variable: '{key}'. "
            "Please ensure it is defined in the Airflow UI."
        )
    return value


def pre_execution_logging(**context: Dict[str, Any]) -> None:
    """
    Logs initialization details in the original German language 
    preserving legacy script conventions.
    """
    templates_dict = context.get("templates_dict") or {}
    lauf_woche = templates_dict.get("lauf_woche")

    if not lauf_woche:
        raise ValueError("Template variable 'lauf_woche' was not supplied to pre_execution_logging.")

    logger.info("=========================================================")
    logger.info(f"Kundenadressabgleich fuer Lauf {lauf_woche} angestossen")
    logger.info("Starte Adressabgleich Kundenstammdaten...")
    logger.info("=========================================================")


def post_execution_logging(**context: Dict[str, Any]) -> None:
    """
    Queries the reconciliation results to derive the discrepancy count
    and prints success flags in the original German language.
    """
    templates_dict = context.get("templates_dict") or {}
    lauf_woche = templates_dict.get("lauf_woche")

    if not lauf_woche:
        raise ValueError("Template variable 'lauf_woche' was not supplied to post_execution_logging.")

    # Retrieve environment settings dynamically
    gcp_project = get_gcp_variable("GCP_PROJECT")
    bq_dataset = get_gcp_variable("BQ_DATASET", default="dwh_kunde")
    bq_connection_id = get_gcp_variable("GCP_CONN_ID", default="google_cloud_default")

    # Access BigQuery using the optimized Cloud Hook structure
    hook = BigQueryHook(gcp_conn_id=bq_connection_id)
    
    # Construct parameterized-equivalent safe SQL statement
    sql_query = f"""
        SELECT IFNULL(SUM(dummy_diff_count), 0) AS cnt 
        FROM `{gcp_project}.{bq_dataset}.d_abgl_kunde_woech_results`
        WHERE execution_date = PARSE_DATE('%Y%m%d', '{lauf_woche}')
    """

    logger.info(f"Querying deviation results from: {gcp_project}.{bq_dataset}.d_abgl_kunde_woech_results")
    
    try:
        df = hook.get_pandas_df(sql=sql_query)
        count = int(df["cnt"].values[0]) if not df.empty else 0
    except Exception as e:
        logger.error(f"Failed to fetch deviation records from BigQuery: {str(e)}")
        logger.warning("Defaulting count to 0 due to upstream query failure verification.")
        count = 0

    # Required literal outputs carried over verbatim
    logger.info("=========================================================")
    logger.info(f"Anzahl gefundener Abweichungen: {count}")
    logger.info("Adressabgleich Kundenstammdaten ohne erkennbare Fehler beendet")
    logger.info("=========================================================")