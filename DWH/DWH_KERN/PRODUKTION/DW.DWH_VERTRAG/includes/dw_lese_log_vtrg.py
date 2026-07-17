def log_execution_status(dag_id, task_id):
    """
    Emulates the DW.LESE_LOG_VTRG include logic.
    Maintains the verbatim original German log formats.
    """
    print(f"Protokolleintrag: {task_id} innerhalb {dag_id}")