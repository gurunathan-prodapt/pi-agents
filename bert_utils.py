# Legacy Sources:
# - vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/f_alis_msgerr.ksh
# - vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/h_alis_parameter.ksh
# - vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/h_alis_date.ksh
# Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bcp_msisdn.ksh

import logging
import datetime
import uuid

# Configure logging for the utility module
LOG_FORMAT = '%(asctime)s - %(levelname)s - %(message)s'
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
logger = logging.getLogger(__name__)

# --- Re-implementation of f_alis_msgerr.ksh functionality ---
def log_message(message: str, level=logging.INFO):
    """Logs a message with the specified level."""
    logger.log(level, message)

def log_error(error_code: int, message: str):
    """Logs an error message."""
    logger.error(f"ERROR {error_code}: {message}")
    # In an Airflow context, we generally don't sys.exit from a Python callable
    # unless it's a critical, unrecoverable error. Airflow task failures
    # should be handled by raising exceptions.

# --- Re-implementation of h_alis_date.ksh functionality (simplified based on doc) ---
def get_current_date_yyyymmdd() -> str:
    """Returns the current system date in YYYYMMDD format."""
    return datetime.date.today().strftime('%Y%m%d')

def parse_date_ddmmyyyy_to_yyyymmdd(date_str: str) -> str:
    """Parses a DDMMYYYY date string and returns it in YYYYMMDD format."""
    try:
        dt_obj = datetime.datetime.strptime(date_str, '%d%m%Y').date()
        return dt_obj.strftime('%Y%m%d')
    except ValueError:
        log_error(194, f"Invalid date format for Stichtag: '{date_str}'. Expected DDMMYYYY.")
        raise ValueError(f"Invalid date format: {date_str}")

# --- Re-implementation of h_alis_parameter.ksh functionality ---
def parse_and_validate_parameters(stichtag_raw: str = None, wiederanlaufwert_raw: str = None) -> tuple[str, int]:
    """
    Parses and validates Stichtag and Wiederanlaufwert.
    Stichtag_raw is expected in DDMMYYYY format if provided.
    Wiederanlaufwert_raw is expected to be an integer-convertible string or None.
    Returns (stichtag_yyyymmdd, wiederanlaufwert_int).
    Raises ValueError if parsing fails.
    """
    stichtag_yyyymmdd = None
    if stichtag_raw:
        try:
            stichtag_yyyymmdd = parse_date_ddmmyyyy_to_yyyymmdd(stichtag_raw)
            log_message(f"Stichtag provided: {stichtag_raw} -> {stichtag_yyyymmdd}")
        except ValueError:
            raise # Re-raise the ValueError from parse_date_ddmmyyyy_to_yyyymmdd

    wiederanlaufwert_int = 0 # Default value as per design
    if wiederanlaufwert_raw is not None:
        try:
            wiederanlaufwert_int = int(wiederanlaufwert_raw)
            log_message(f"Wiederanlaufwert provided: {wiederanlaufwert_raw}")
        except ValueError:
            log_error(195, f"Invalid format for Wiederanlaufwert: '{wiederanlaufwert_raw}'. Expected an integer.")
            raise # Re-raise to stop processing

    # Default Stichtag to system date if not provided
    if not stichtag_yyyymmdd:
        stichtag_yyyymmdd = get_current_date_yyyymmdd()
        log_message(f"Stichtag not provided, defaulting to system date: {stichtag_yyyymmdd}")

    return stichtag_yyyymmdd, wiederanlaufwert_int

def generate_job_entry_number() -> str:
    """Generates a unique job entry number, mimicking DWMSG_ErmittleNr."""
    # Using timestamp and a short UUID part for uniqueness.
    timestamp_part = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
    uuid_part = str(uuid.uuid4())[:8] # First 8 characters for brevity
    return f"{timestamp_part}_{uuid_part}"