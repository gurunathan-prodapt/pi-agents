import logging

def log_execution_details_callable(**context):
    """
    Writes execution metadata (parent DAG ID and executing task ID) to the standard execution log.
    Equivalent to UC4 JOBI: DW.LESE_LOG_VTRG
    
    Original German Log Message literal is strictly preserved:
    "Protokolleintrag: &ADMJOB innerhalb &ADMJP"
    """
    parent_dag_id = context['dag'].dag_id
    task_id = context['task_instance'].task_id
    
    log_message = f"Protokolleintrag: {task_id} innerhalb {parent_dag_id}"
    logging.info(log_message)