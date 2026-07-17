import logging

def write_execution_log(dag_id, task_id):
    """
    Schreibt einen einfachen Protokolleintrag in das Airflow-Task-Laufprotokoll.
    Preserves literal original-language outputs character-for-character.
    """
    # OUTPUT/PRINT LITERAL RULE: Exact original German logging format preserved
    log_message = f"Protokolleintrag: {task_id} innerhalb {dag_id}"
    logging.info(log_message)
    print(log_message)