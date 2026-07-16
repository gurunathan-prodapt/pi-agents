"""
Module: dw_lese_log_vtrg.py
Purpose: Shared helper to output execution status to standard task logs.
"""
import logging

logger = logging.getLogger("airflow.task")


def write_execution_log(admjob: str, admjp: str) -> None:
    """Writes standard execution status tracking metadata to logs.

    Conforms to OUTPUT/PRINT LITERAL RULE.
    """
    logger.info(f"Protokolleintrag: {admjob} innerhalb {admjp}")