# Migrated utility: f_alis_msgerr.ksh (Error Handling)
# Legacy job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bcp_msisdn.ksh
# This file provides basic error handling utilities.
# For Airflow, native error handling, logging, and alerting mechanisms are generally preferred.

import logging

def setup_logger(name, level=logging.INFO):
    """Sets up a basic logger."""
    logger = logging.getLogger(name)
    logger.setLevel(level)
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    return logger

def log_error(logger, error_code, error_message, extra_info=None):
    """Logs an error message."""
    full_message = f"ERROR {error_code}: {error_message}"
    if extra_info:
        full_message += f" ({extra_info})"
    logger.error(full_message)

def DWMSG_MeldeFehler(logger, exit_code, error_level, error_nr, error_arg):
    """
    Simulates the DWMSG_MeldeFehler function from f_alis_msgerr.ksh.
    In a BigQuery/Airflow context, this mostly translates to logging and
    potentially raising an exception to fail the task/DAG.
    """
    level_map = {
        'E': 'ERROR',
        'W': 'WARNING',
        'I': 'INFO'
    }
    log_level = level_map.get(error_level.upper(), 'INFO')

    message = f"Legacy Error {error_nr} ('{error_arg}'): Please check the logs for details."
    if log_level == 'ERROR':
        logger.error(message)
        # In Airflow, raising an exception will typically mark the task as failed.
        # raise ValueError(f"Job failed with error {error_nr}: {error_arg}")
    elif log_level == 'WARNING':
        logger.warning(message)
    else:
        logger.info(message)

# Example usage (for testing or direct script use)
if __name__ == "__main__":
    app_logger = setup_logger("AppLogger")
    log_error(app_logger, 100, "File not found", "data.csv")
    DWMSG_MeldeFehler(app_logger, 1, 'E', 193, "Necessary argument missing")
    DWMSG_MeldeFehler(app_logger, 0, 'I', 0, "Info message")