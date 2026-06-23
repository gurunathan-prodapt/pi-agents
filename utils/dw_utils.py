#
# Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_discount_rr.ksh (and its sourced utilities)
# Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_discount_rr.ksh
#
# This file contains Python utility functions re-implementing logic from
# legacy KornShell utility scripts such as f_alis_msgerr.ksh, h_alis_parameter.ksh,
# and h_alis_date.ksh. These are adapted for use within an Airflow environment.
#

import logging
from datetime import datetime

# Configure logging for the utility module. In a Cloud Composer environment,
# Airflow's logging is automatically integrated with Cloud Logging.
logger = logging.getLogger(__name__)
# The effective level will be determined by the Airflow environment's logging configuration.
# For local testing, you might want to set a specific level and handler.
# Example for local testing:
# logger.setLevel(logging.INFO)
# handler = logging.StreamHandler()
# formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
# handler.setFormatter(formatter)
# logger.addHandler(handler)


class DWError(Exception):
    """Custom exception for errors originating from the legacy DW framework."""
    pass


def dwmsg_meldefehler(error_code: int, error_arg: str, level: str = "ERROR"):
    """
    Simulates the behavior of the legacy DWMSG_MeldeFehler function.
    Logs an error message and raises a DWError to halt execution, similar to
    how the original script would exit on critical errors.

    Args:
        error_code: The numeric error code.
        error_arg: Additional context or arguments for the error.
        level: The logging level ('ERROR', 'WARNING', 'INFO').
    """
    message = f"DWMSG_ERROR - Code: {error_code}, Arg: '{error_arg}'"
    if level == "ERROR":
        logger.error(message)
    elif level == "WARNING":
        logger.warning(message)
    else:
        logger.info(message)
    raise DWError(message)


def pruefe_parameter_gesetzt(param_name: str, param_value: any):
    """
    Checks if a required parameter has been provided and is not empty.
    Mimics the 'pruefeParameterGesetzt' function from h_alis_parameter.ksh.

    Args:
        param_name: The name of the parameter being checked.
        param_value: The value of the parameter.

    Raises:
        DWError: If the parameter is missing or empty.
    """
    if param_value is None or (isinstance(param_value, str) and not param_value.strip()):
        dwmsg_meldefehler(193, f"Required parameter '{param_name}' is missing or empty.")
    logger.info(f"Parameter '{param_name}' successfully validated with value: '{param_value}'")


def get_current_dw_date_str(date_format: str = '%Y%m%d') -> str:
    """
    Returns the current system date formatted as a string.
    Similar to how v_sysdate might be derived in the legacy scripts.

    Args:
        date_format: The desired format for the date string (e.g., '%Y%m%d').

    Returns:
        A string representing the current date in the specified format.
    """
    return datetime.now().strftime(date_format)


def get_dw_eintrags_nr() -> str:
    """
    Generates a unique entry number, possibly for logging or tracking.
    In Airflow, `ti.run_id` or a combination of `dag_id` and `execution_date`
    often serve this purpose. This is a simple timestamp-based alternative.

    Returns:
        A string unique identifier based on the current timestamp (YYYYMMDDHHMMSSmmm).
    """
    return datetime.now().strftime('%Y%m%d%H%M%S%f')[:-3] # YYYYMMDDHHMMSS + milliseconds