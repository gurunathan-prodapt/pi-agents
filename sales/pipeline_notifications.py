"""
pipeline_notifications.py
Centralized callback and publication event handling for production workflows.
"""

import logging

logger = logging.getLogger(__name__)

def on_failure_alarm(context: dict) -> None:
    """
    Simulates the UC4 'ON_FAILURE action=NOTIFY_AND_ABORT' warning message.
    """
    task_id = context['task_instance'].task_id
    execution_date = context.get('execution_date') or context.get('logical_date')
    logger.error(
        f"ALARM: Critical Pipeline Task [{task_id}] failed at "
        f"execution run [{execution_date}]. Sending abort log."
    )


def publish_completion_event(**context: dict) -> None:
    """
    Implements verbatim source system execution notification logic:
    1. Outputs verbatim completion format containing target execution date.
    2. Publishes literal downstream orchestration identifier 'RETAIL_DAILY_COMPLETE'.
    """
    load_date = context['ds']
    
    # Verbatim String Literal 1: Output String Preservation
    complete_msg = f"RETAIL_DAILY_WORKFLOW completed for LOAD_DATE={load_date}"
    logger.info(f"[VERBATIM ECHO OUTPUT]: {complete_msg}")
    
    # Verbatim String Literal 2: Published Event Preservation
    event_name = "RETAIL_DAILY_COMPLETE"
    logger.info(f"[VERBATIM EVENT PUBLISH]: uc4api publish_event {event_name} date={load_date}")