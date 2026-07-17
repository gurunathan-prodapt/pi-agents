"""
Module: dw_dwh_kunde_abgl_woechentlich_bin.py
Path: dags/dw_dwh_kunde/bin/dw_dwh_kunde_abgl_woechentlich_bin.py

Description:
    Contains logger helpers and shell wrapper utilities converted from 
    legacy ksh scripts. Preserves literal logging outputs for alerting patterns.
"""

import logging
from typing import Any, Dict

logger = logging.getLogger("airflow.task")


def log_start_message(**context: Any) -> None:
    """Emits the exact start signature legacy logs parsed by operations dashboards."""
    logger.info("Starte Adressabgleich Kundenstammdaten...")


def log_end_message(**context: Any) -> None:
    """Emits the standard legacy successful workflow end signature."""
    logger.info("Adressabgleich Kundenstammdaten ohne erkennbare Fehler beendet")


def log_discrepancy_count(task_id_for_count: str, **context: Dict[str, Any]) -> None:
    """
    Extracts the counted discrepancies returned from upstream BigQuery execution
    and logs them in legacy format.

    Args:
        task_id_for_count (str): Task ID of the BQ selection task.
        context (dict): Airflow context dictionary containing the task instance.
    """
    ti = context.get("task_instance")
    if not ti:
        logger.warning("Airflow TaskInstance context unavailable. Logging default count 0.")
        logger.info("Anzahl gefundener Abweichungen: 0")
        return

    # Extract xcom data returned from the BigQuery Operator
    query_results = ti.xcom_pull(task_ids=task_id_for_count)

    # BigQuery Operator structures returned data list format: [[value]]
    try:
        if query_results and isinstance(query_results, list):
            count = query_results[0][0]
        else:
            count = 0
    except (IndexError, TypeError) as err:
        logger.error(f"Failed to extract count values from context: {str(err)}")
        count = 0

    logger.info(f"Anzahl gefundener Abweichungen: {count}")