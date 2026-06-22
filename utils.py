# Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_aurd_rechstan.ksh
# Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_aurd_rechstan.ksh

import datetime
import logging
import sys

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)

class DWError(Exception):
    """Custom exception for legacy DW error handling."""
    def __init__(self, err_nr, err_arg=None, message=""):
        self.err_nr = err_nr
        self.err_arg = err_arg
        self.message = message
        super().__init__(self.message)

def DWMSG_MeldeFehler(entry_nr: int, error_type: str, err_nr: int, err_arg: str, log_file: str = None):
    """
    Python equivalent of DWMSG_MeldeFehler.
    Logs an error message.
    """
    msg = f"ERROR: [{error_type}] Entry_Nr: {entry_nr}, ErrNr: {err_nr}"
    if err_arg:
        msg += f", ErrArg: {err_arg}"
    logger.error(msg)
    if log_file:
        with open(log_file, 'a') as f:
            f.write(msg + '\n')

def DWMSG_ErmittleNr():
    """
    Python equivalent of DWMSG_ErmittleNr.
    In a real system, this would generate a unique job entry number.
    For now, it's a simple timestamp-based ID.
    """
    return int(datetime.datetime.now().strftime("%Y%m%d%H%M%S"))

def DWMSG_Logdateiname(job_kennung: str, entry_nr: int):
    """
    Python equivalent of DWMSG_Logdateiname.
    Generates a log file name.
    """
    return f"{job_kennung}_{entry_nr}.log"

def DWMSG_ErzeugeEintrag(entry_nr: int, job_kennung: str, script_name: str, log_file: str):
    """
    Python equivalent of DWMSG_ErzeugeEintrag.
    Logs the job entry creation.
    """
    msg = f"Job Entry Created: Entry_Nr={entry_nr}, JobKennung={job_kennung}, Script={script_name}, LogFile={log_file}"
    logger.info(msg)
    if log_file:
        with open(log_file, 'a') as f:
            f.write(msg + '\n')

def DWMSG_SetzeStichtagInfo(entry_nr: int, stichtag: str, date_format: str = "%d%m%Y", log_file: str = None):
    """
    Python equivalent of DWMSG_SetzeStichtagInfo.
    Logs the Stichtag information.
    """
    msg = f"Stichtag Info: Entry_Nr={entry_nr}, Stichtag={stichtag} (Format: {date_format})"
    logger.info(msg)
    if log_file:
        with open(log_file, 'a') as f:
            f.write(msg + '\n')

def DWMSG_Fehlerbehandlung(entry_nr: int, log_file: str = None):
    """
    Python equivalent of DWMSG_Fehlerbehandlung (simplified for now).
    Handles errors during execution.
    """
    msg = f"Job encountered an error for Entry_Nr: {entry_nr}. Aborting."
    logger.error(msg)
    if log_file:
        with open(log_file, 'a') as f:
            f.write(msg + '\n')

def DWMSG_SetzeStatusOK(entry_nr: int, log_file: str = None):
    """
    Python equivalent of DWMSG_SetzeStatusOK.
    Sets the job status to OK.
    """
    msg = f"Job finished successfully for Entry_Nr: {entry_nr}."
    logger.info(msg)
    if log_file:
        with open(log_file, 'a') as f:
            f.write(msg + '\n')

# Equivalent of DWDate_Gib_Zeitraum
def get_date_formatted(date_offset: int = 0, unit: str = 'D', output_format: str = 'DDMMYYYY'):
    """
    Returns a date string formatted as specified.
    - date_offset: offset from today (e.g., 0 for today, -1 for yesterday)
    - unit: 'D' for days (only days supported currently)
    - output_format: desired output date format (e.g., 'DDMMYYYY')
    """
    if unit != 'D':
        logger.warning(f"Only 'D' (days) unit is supported for date offset. Ignoring unit '{unit}'.")

    target_date = datetime.date.today() + datetime.timedelta(days=date_offset)

    if output_format == 'DDMMYYYY':
        return target_date.strftime("%d%m%Y")
    elif output_format == 'YYYYMMDD':
        return target_date.strftime("%Y%m%d")
    else:
        logger.warning(f"Unsupported date format: {output_format}. Defaulting to DDMMYYYY.")
        return target_date.strftime("%d%m%Y")

# Equivalent of pruefeParameterGesetzt
def pruefeParameterGesetzt(param_name: str, param_value: any):
    """
    Checks if a parameter is set (not None or empty string).
    Raises DWError if the parameter is not set.
    """
    if param_value is None or (isinstance(param_value, str) and not param_value.strip()):
        raise DWError(err_nr=193, err_arg=param_name, message=f"Parameter '{param_name}' is not set.")