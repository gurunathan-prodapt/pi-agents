# Legacy Source: vobs/dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/BERT/PRODUKTION/DW.BERT_STAMMDATEN_JP/DW.BERT_P_VERTRAG_JP/DW.BERT_P_VERTRAG_JP.xml
# Job Plan: DW.BERT_P_VERTRAG_JP

import logging

def on_terminal_failure(context):
    """
    Failure callback triggered when a task exhausts all configured retries.
    Simulates the legacy UC4 BLOCK state and handles notification logic
    (equivalent to legacy DW.CALL_STANDARD action).
    """
    ti = context.get("task_instance")
    task_id = ti.task_id if ti else "Unknown Task"
    try_number = ti.try_number if ti else "Unknown"
    max_tries = ti.max_tries if ti else "Unknown"
    execution_date = context.get("execution_date")
    
    msg = (
        f"CRITICAL ALERT: Task '{task_id}' in DAG '{context.get('dag').dag_id}' "
        f"failed terminally on execution date {execution_date} "
        f"after {try_number} of {max_tries} total attempts."
    )
    
    logging.error(msg)
    # Stubs for downstream enterprise monitoring systems integration (e.g., Slack, Pub/Sub, PagerDuty)
    # print(f"Sending alert to enterprise monitoring: {msg}")