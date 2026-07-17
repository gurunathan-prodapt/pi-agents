import logging

logger = logging.getLogger("airflow.task")

def include_lese_log_knzb(task_name, dag_name):
    """
    Translates legacy JOBI 'DW.LESE_LOG_KNZB'
    OUTPUT/PRINT LITERAL RULE: Must output exact German logs unchanged.
    """
    # Original: :PRINT "Protokolleintrag: &ADMJOB innerhalb &ADMJP"
    logger.info(f"Protokolleintrag: {task_name} innerhalb {dag_name}")