# Legacy Source: f_alis_msgerr.ksh for vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_cntrct_crs3.ksh
# Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_cntrct_crs3.ksh

"""
This module provides logging and error handling utilities,
mimicking the functionality of f_alis_msgerr.ksh.

It uses Python's standard logging module and can be configured to integrate
with Google Cloud Logging for centralized log management.
The original script interacted with an Oracle 'BERT_MELDUNG' table via sqlplus.
This migration replaces that with standard logging practices suitable for GCP.
"""

import logging
import os
from datetime import datetime

# Configure a logger for this module
# In an Airflow environment, this would typically be handled by Airflow's
# default logger setup, which often integrates with Cloud Logging.
# For standalone testing, a basic console handler is set up.
logger = logging.getLogger(__name__)
if not logger.handlers:
    # Set up basic console handler for local testing if no handlers exist
    # In production, Airflow/Cloud Logging would provide more sophisticated handlers
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)

# Define constants for error types, mirroring legacy F/E/W (Fatal, Error, Warning)
FATAL = 'F'
ERROR = 'E'
WARNING = 'W'
INFO = 'I' # Added for general informational messages

class DWMSGError(Exception):
    """Custom exception for DWMSG-related errors."""
    def __init__(self, message, error_code=None, error_args=None):
        super().__init__(message)
        self.error_code = error_code
        self.error_args = error_args

def _log_message(level: str, message: str, entry_nr: str = None, job_id: str = None,
                 program_name: str = None, log_file: str = None,
                 error_type: str = None, error_code: int = None,
                 additional_info_1: str = None, additional_info_2: str = None,
                 stichtag: str = None, stichtag_fmt: str = None,
                 info_text: str = None, date_format: str = None):
    """
    Internal helper to log messages with structured data.
    This will be transformed into Cloud Logging structured logs.
    """
    log_data = {
        "job_entry_number": entry_nr,
        "job_id": job_id,
        "program_name": program_name,
        "log_file": log_file,
        "error_type": error_type,
        "error_code": error_code,
        "additional_info_1": additional_info_1,
        "additional_info_2": additional_info_2,
        "stichtag": stichtag,
        "stichtag_format": stichtag_fmt,
        "info_text": info_text,
        "date_format": date_format
    }
    # Filter out None values for cleaner logs
    log_data = {k: v for k, v in log_data.items() if v is not None}

    full_message = f"{message} | {', '.join(f'{k}={v}' for k, v in log_data.items() if k not in ['job_entry_number', 'job_id', 'program_name', 'log_file'])}"

    if level == FATAL:
        logger.critical(full_message, extra=log_data)
    elif level == ERROR:
        logger.error(full_message, extra=log_data)
    elif level == WARNING:
        logger.warning(full_message, extra=log_data)
    else: # Default to INFO
        logger.info(full_message, extra=log_data)

def dwmsg_fehlerbehandlung(entry_nr: str, error_code: int = 10, additional_info: str = ""):
    """
    Mimics DWMSG_Fehlerbehandlung. Called on error.
    Logs a fatal error and indicates job abortion.
    """
    message = f"Error handling initiated. Job will be aborted due to error."
    _log_message(FATAL, message, entry_nr=entry_nr, error_code=error_code,
                 additional_info_1=f"ErrorCode is: {error_code}", additional_info_2=additional_info)
    # In Airflow, this would typically lead to task failure.
    # For now, we'll raise an exception to stop execution.
    raise DWMSGError(f"Fatal error during job execution (EntryNr: {entry_nr})", error_code=error_code)

def dwmsg_setze_status_ok(entry_nr: str):
    """
    Mimics DWMSG_SetzeStatusOK. Marks the job entry as successful.
    """
    message = f"Job completed successfully (EntryNr: {entry_nr})."
    _log_message(INFO, message, entry_nr=entry_nr, error_type=INFO)

def dwmsg_setze_status_abbruch(entry_nr: str):
    """
    Mimics DWMSG_SetzeStatusAbbruch. Marks the job entry as aborted.
    """
    message = f"Job aborted (EntryNr: {entry_nr})."
    _log_message(ERROR, message, entry_nr=entry_nr, error_type=ERROR)

def dwmsg_ermittle_nr() -> str:
    """
    Mimics DWMSG_ErmittleNr. Generates a unique entry number.
    In the legacy system, this involved PL/SQL. Here, we generate a UUID
    or timestamp-based string for uniqueness.
    """
    # For simplicity, using a timestamp. In a real-world Airflow DAG,
    # task instance details or a UUID could be used.
    unique_id = datetime.now().strftime("%Y%m%d%H%M%S%f")
    logger.debug(f"Generated new entry number: {unique_id}")
    return unique_id

def dwmsg_erzeuge_eintrag(entry_nr: str, job_id: str, program_name: str, log_file: str):
    """
    Mimics DWMSG_ErzeugeEintrag. Creates a new log entry.
    """
    message = f"New job entry created: Program '{program_name}' with JobID '{job_id}' and LogFile '{log_file}'."
    _log_message(INFO, message, entry_nr=entry_nr, job_id=job_id,
                 program_name=program_name, log_file=log_file)

def dwmsg_melde_fehler(entry_nr: str, error_level: str, error_code: int,
                       additional_info_1: str = None, additional_info_2: str = None):
    """
    Mimics DWMSG_MeldeFehler. Reports a specific error.
    Error_level can be FATAL (F), ERROR (E), WARNING (W).
    """
    message = f"Reporting error {error_code} (Level: {error_level})"
    if additional_info_1:
        message += f", Info1: '{additional_info_1}'"
    if additional_info_2:
        message += f", Info2: '{additional_info_2}'"

    _log_message(error_level, message, entry_nr=entry_nr, error_type=error_level,
                 error_code=error_code, additional_info_1=additional_info_1,
                 additional_info_2=additional_info_2)
    
    if error_level == FATAL:
        raise DWMSGError(f"Fatal error reported (Code: {error_code}, EntryNr: {entry_nr})", error_code, [additional_info_1, additional_info_2])

def dwmsg_logdateiname(job_id: str, entry_nr: str) -> str:
    """
    Mimics DWMSG_Logdateiname. Constructs a log file name.
    In a cloud environment, this might refer to a path in GCS or just a logical name.
    """
    from utils.env_config import EnvConfig
    log_dir = EnvConfig.DW_DIR_PROT
    timestamp = datetime.now().strftime("%Y%m%d_%H%M")
    log_file_name = f"{log_dir}/{job_id}_{timestamp}_{entry_nr}.log"
    logger.debug(f"Generated log filename: {log_file_name}")
    return log_file_name

def dwmsg_setze_stichtag_info(entry_nr: str, stichtag: str, stichtag_fmt: str):
    """
    Mimics DWMSG_SetzeStichtagInfo. Sets specific date info.
    """
    if not entry_nr or not stichtag or not stichtag_fmt:
        logger.error("Missing parameters for dwmsg_setze_stichtag_info.")
        raise DWMSGError("Missing parameters for setting Stichtag info.")

    message = f"Setting Stichtag info for EntryNr {entry_nr}: Date='{stichtag}', Format='{stichtag_fmt}'"
    _log_message(INFO, message, entry_nr=entry_nr, stichtag=stichtag, stichtag_fmt=stichtag_fmt)

def dwmsg_append_timing_infos(entry_nr: str, info_text: str, date_format: str):
    """
    Mimics DWMSG_AppendTimingInfos. Appends timing information.
    """
    if not entry_nr or not info_text or not date_format:
        logger.error("Missing parameters for dwmsg_append_timing_infos.")
        raise DWMSGError("Missing parameters for appending timing infos.")
    
    current_time = datetime.now().strftime(date_format)
    message = f"Appending timing info for EntryNr {entry_nr}: '{info_text} {current_time}'"
    _log_message(INFO, message, entry_nr=entry_nr, info_text=info_text, date_format=date_format)

if __name__ == "__main__":
    # Example usage
    logging.basicConfig(level=logging.DEBUG) # Set root logger level for example

    entry = dwmsg_ermittle_nr()
    job = "TEST_JOB_001"
    program = "my_script.py"
    log_f = dwmsg_logdateiname(job, entry)

    dwmsg_erzeuge_eintrag(entry, job, program, log_f)
    dwmsg_setze_status_ok(entry)
    dwmsg_melde_fehler(entry, WARNING, 100, "Something minor happened")
    
    try:
        dwmsg_melde_fehler(entry, FATAL, 200, "Critical issue", "Restart required")
    except DWMSGError as e:
        print(f"Caught expected fatal error: {e}")

    dwmsg_setze_stichtag_info(entry, "20230115", "%Y%m%d")
    dwmsg_append_timing_infos(entry, "Processing started", "%Y-%m-%d %H:%M:%S")

    dwmsg_setze_status_abbruch(entry)