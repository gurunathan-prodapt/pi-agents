# Legacy source: vobs/dw_source/isrpt/isbert/allgemein/is/util/bin/f_alis_msgerr.ksh
# Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_inv_acc.ksh

import logging

# Configure basic logging for Airflow context
log = logging.getLogger(__name__)

def log_error(entry_number: int, msg_type: str, error_number: int, *args):
    """
    Simulates the DWMSG_MeldeFehler function from the legacy ksh script.
    Logs an error message with severity.

    Args:
        entry_number: The entry number (not directly used in this basic implementation).
        msg_type: Type of message ('F' for Fatal, 'E' for Error, 'W' for Warning).
        error_number: The specific error code.
        *args: Additional arguments for the message (Zusatz1, Zusatz2).
    """
    full_message_parts = [f"Error {error_number}:"]
    if args:
        full_message_parts.extend(map(str, args))
    full_message = " ".join(full_message_parts)

    if msg_type == 'F':
        log.critical(full_message)
    elif msg_type == 'E':
        log.error(full_message)
    elif msg_type == 'W':
        log.warning(full_message)
    else:
        log.info(full_message) # Default to info for unknown types