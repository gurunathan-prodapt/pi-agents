"""
Module: DW.LESE_LOG_KNZB
Description: Standalone Python logging helper that preserves legacy German log 
             formatting and outputs structural operational execution metadata.
"""

import logging


def write_execution_log(**context) -> None:
    """
    Schreibt einen einfachen Protokolleintrag in das Cloud Composer / Airflow Laufprotokoll.
    Emulates the legacy JOBI logger using context-aware task instance details.

    Args:
        **context: Airflow task execution context dictionary containing DAG and task identifiers.
    """
    logging.info("Starting logging capture equivalent to JOBI: DW.LESE_LOG_KNZB")

    # Extract structural task metadata from context falling back to placeholders if called outside of tasks
    parent_job_plan = "UNKNOWN_DAG"
    active_job = "UNKNOWN_TASK"

    if context:
        if 'dag' in context and context['dag']:
            parent_job_plan = context['dag'].dag_id
        if 'task_instance' in context and context['task_instance']:
            active_job = context['task_instance'].task_id

    # OUTPUT/PRINT LITERAL RULE: Verbatim German output is strictly preserved
    logging.info(f"Protokolleintrag: {active_job} innerhalb {parent_job_plan}")