"""
Task execution logic translating the original wrapper script (r_abgl_kunde_woech.ksh) 
to run BigQuery stored procedures within a Cloud Composer/Airflow context.
"""

import logging
from datetime import datetime, timedelta
from typing import Any, Dict, Optional
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

logger = logging.getLogger(__name__)


def calculate_stichtag(ds_nodash: str, manual_conf: Optional[Dict[str, Any]] = None) -> str:
    """
    Calculates the reference key date (stichtag) in YYYYMMDD format.
    Defaults to 7 days prior to the execution date if not passed manually via config.
    
    Args:
        ds_nodash: Airflow logical execution date string in YYYYMMDD format.
        manual_conf: Optional dictionary representing context.get('dag_run').conf.
        
    Returns:
        The calculated stichtag as a string.
    """
    manual_conf = manual_conf or {}
    stichtag = manual_conf.get("stichtag")
    
    if stichtag:
        return str(stichtag)
    
    # Calculate fallback: 7 days prior to logical execution date
    execution_dt = datetime.strptime(ds_nodash, "%Y%m%d")
    fallback_dt = execution_dt - timedelta(days=7)
    return fallback_dt.strftime("%Y%m%d")


def execute_and_log_reconciliation(gcp_project: str, bq_location: str, **context: Any) -> int:
    """
    Executes the BigQuery customer address reconciliation wrapper stored procedure, 
    parses deviations, and outputs standard German logging statements.
    
    Args:
        gcp_project: Google Cloud Target Project ID.
        bq_location: BigQuery region location (e.g., 'EU').
        **context: Airflow context dictionary passed at runtime.
        
    Returns:
        The integer count of deviations detected.
    """
    # Retrieve configuration and logical execution dates safely
    dag_run = context.get("dag_run")
    dag_run_conf = dag_run.conf if dag_run else {}
    ds_nodash = context["ds_nodash"]
    
    stichtag = calculate_stichtag(ds_nodash, dag_run_conf)

    # LITERAL LOGGING RULE: German start statement preserved exactly
    logger.info(f"Starte Adressabgleich Kundenstammdaten fuer Stichtag {stichtag}")

    # Initialize BigQuery hook
    hook = BigQueryHook(gcp_conn_id="google_cloud_default", use_legacy_sql=False)
    
    # Run the parent stored procedure and capture the output count of deviations
    sql_query = f"""
        DECLARE v_abweichungen INT64;
        CALL `{gcp_project}.dw_kern.r_abgl_kunde_woech`('{stichtag}', v_abweichungen);
        SELECT v_abweichungen as abweichungen;
    """
    
    logger.info(f"Executing query on BQ Project '{gcp_project}' in region '{bq_location}'")
    records = hook.get_records(sql=sql_query, location=bq_location)
    deviations = int(records[0][0]) if records and records[0] else 0

    # LITERAL LOGGING RULE: German deviation count statement preserved exactly
    logger.info(f"Anzahl gefundener Abweichungen: {deviations}")

    if deviations > 0:
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        # LITERAL LOGGING RULE: German warning log containing the timestamp and target table details
        warning_msg = (
            f"[W] {timestamp} {deviations} Abweichungen im Kundenadressabgleich "
            f"gefunden, siehe dw_stage.tmp_abgl_kunde_results"
        )
        logger.warning(warning_msg)
    else:
        # LITERAL LOGGING RULE: German success log preserved exactly
        logger.info("Adressabgleich Kundenstammdaten ohne erkennbare Fehler beendet")
        
    return deviations