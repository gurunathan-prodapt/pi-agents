# -*- coding: utf-8 -*-
"""
Migrated from JOBI DW.LESE_LOG_KNZB
Emulates UC4 metadata context logging inside an Apache Airflow execution environment.
"""

import logging
from typing import Any, Dict

# Route logs directly to the active Airflow task execution log handler
logger = logging.getLogger("airflow.task")


def log_uc4_context_helper(**context: Any) -> None:
    """
    Extracts runtime execution metadata and outputs a structured log statement.
    Designed for use within a PythonOperator or via Airflow's execution context.

    Equivalent UC4 Logic:
        :SET &ADMJP  = SYS_ACT_JPNAME()
        :SET &ADMJOB = SYS_ACT_JOBNAME()
        :PRINT "Protokolleintrag: &ADMJOB innerhalb &ADMJP"
        
    Args:
        **context: Airflow task instance execution context dictionary.
    """
    try:
        # Extract metadata from execution context
        parent_plan_name = "Unknown_DAG"
        active_job_name = "Unknown_Task"

        if context:
            # Check DAG context
            if "dag" in context and context["dag"] is not None:
                parent_plan_name = context["dag"].dag_id
            elif "dag_run" in context and context["dag_run"] is not None:
                parent_plan_name = context["dag_run"].dag_id

            # Check Task context
            if "task" in context and context["task"] is not None:
                active_job_name = context["task"].task_id
            elif "task_instance" in context and context["task_instance"] is not None:
                active_job_name = context["task_instance"].task_id

        # Character-for-character reproduction of the original German print statement template
        logger.info(f"Protokolleintrag: {active_job_name} innerhalb {parent_plan_name}")

    except Exception as err:
        # Graceful non-blocking exception handling to ensure logging errors 
        # do not halt core data pipelines.
        logger.warning(f"Failed to log runtime context dynamically: {str(err)}")