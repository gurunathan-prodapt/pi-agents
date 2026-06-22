# Migrated from DW.BERT_AUSD_BP_TA_ICCID_VERTRAG (legacy: common KornShell utilities)

import datetime
import logging

# Configure basic logging for demonstration purposes
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def parse_stichtag_and_wiederanlaufwert(args: list[str]) -> tuple[str, str]:
    """
    Parses command-line arguments for 'Stichtag' (-s) and 'Wiederanlaufwert' (-l).
    This function mimics the 'getopts' logic from KornShell scripts.
    In Airflow, parameters are usually passed differently (e.g., via Airflow variables,
    DAG run configuration, or custom operators). This is a conceptual re-implementation.

    Args:
        args: A list of arguments, typically from sys.argv[1:].

    Returns:
        A tuple containing (stichtag, wiederanlaufwert).
    """
    stichtag = None
    wiederanlaufwert = "0"  # Default value as per ksh script

    i = 0
    while i < len(args):
        arg = args[i]
        if arg == "-s":
            if i + 1 < len(args):
                stichtag = args[i+1]
                i += 1
            else:
                logging.error("Option -s requires an argument.")
                raise ValueError("Missing argument for -s (Stichtag)")
        elif arg == "-l":
            if i + 1 < len(args):
                wiederanlaufwert = args[i+1]
                i += 1
            else:
                logging.error("Option -l requires an argument.")
                raise ValueError("Missing argument for -l (Wiederanlaufwert)")
        i += 1
            
    if stichtag is None:
        # Default to yesterday if not provided, mimicking common DW practices
        stichtag = (datetime.date.today() - datetime.timedelta(days=1)).strftime("%Y%m%d")
        logging.info(f"Stichtag not provided, defaulting to: {stichtag}")

    return stichtag, wiederanlaufwert

def validate_date(date_str: str, date_format: str = "%Y%m%d") -> bool:
    """
    Validates if a given string is a valid date in the specified format.
    Mimics DWDate_Datum_Check from h_alis_date.ksh.

    Args:
        date_str: The date string to validate.
        date_format: The expected format of the date string (default: YYYYMMDD).

    Returns:
        True if the date is valid, False otherwise.
    """
    try:
        datetime.datetime.strptime(date_str, date_format)
        return True
    except ValueError:
        return False

def get_date_range(stichtag_str: str, date_format: str = "%Y%m%d") -> dict:
    """
    Calculates various date-related values based on a 'Stichtag'.
    Mimics DWDate_Gib_Zeitraum from h_alis_date.ksh.
    This is a simplified version; real implementation would be more comprehensive.

    Args:
        stichtag_str: The key date string.
        date_format: The format of the stichtag_str.

    Returns:
        A dictionary containing various date representations.
    """
    try:
        stichtag_date = datetime.datetime.strptime(stichtag_str, date_format).date()
    except ValueError:
        logging.error(f"Invalid stichtag_str provided to get_date_range: {stichtag_str}")
        raise

    return {
        "stichtag": stichtag_date.strftime(date_format),
        "stichtag_minus_1_day": (stichtag_date - datetime.timedelta(days=1)).strftime(date_format),
        "stichtag_month_start": stichtag_date.replace(day=1).strftime(date_format),
        "stichtag_year": stichtag_date.strftime("%Y"),
        # Add more date calculations as needed based on h_alis_date.ksh
    }

def log_message(level: str, message: str):
    """
    Centralized logging function. Mimics DWMSG_... functions from f_alis_msgerr.ksh.
    In a real Airflow environment, this would integrate with Airflow's native logging
    and potentially Cloud Logging.

    Args:
        level: The logging level (e.g., "INFO", "WARNING", "ERROR").
        message: The message to log.
    """
    if level.upper() == "INFO":
        logging.info(message)
    elif level.upper() == "WARNING":
        logging.warning(message)
    elif level.upper() == "ERROR":
        logging.error(message)
    else:
        logging.debug(f"[{level.upper()}] {message}")

# Placeholder for environment initialization logic if any global variables or setup were needed
# from .dw_init
def initialize_environment():
    """
    Placeholder for environment initialization logic, similar to .dw_init.
    In Airflow, environment variables or Airflow Variables are preferred.
    """
    logging.info("Initializing BERT utilities environment (conceptual).")
    # Example: Set up constants or check for required environment variables
    # For instance, if PROJECT_ID and DATASET were configured here.
    pass

if __name__ == "__main__":
    # Example usage for local testing
    print("--- Testing bert_utilities.py ---")
    
    # Test parse_stichtag_and_wiederanlaufwert
    print("\nTesting argument parsing:")
    try:
        s, l = parse_stichtag_and_wiederanlaufwert(["-s", "20231026", "-l", "1"])
        print(f"Parsed: Stichtag={s}, Wiederanlaufwert={l}")
        s, l = parse_stichtag_and_wiederanlaufwert([])
        print(f"Parsed (default): Stichtag={s}, Wiederanlaufwert={l}")
    except ValueError as e:
        print(f"Error parsing args: {e}")

    # Test validate_date
    print("\nTesting date validation:")
    print(f"20231026 is valid: {validate_date('20231026')}")
    print(f"20231301 is valid: {validate_date('20231301')}") # Invalid month
    print(f"InvalidDate is valid: {validate_date('InvalidDate')}")

    # Test get_date_range
    print("\nTesting date range calculation:")
    try:
        date_info = get_date_range("20231026")
        print(f"Date info for 20231026: {date_info}")
    except ValueError as e:
        print(f"Error getting date range: {e}")

    # Test log_message
    print("\nTesting logging:")
    log_message("INFO", "This is an informational message.")
    log_message("WARNING", "This is a warning message.")
    log_message("ERROR", "This is an error message.")
    log_message("DEBUG", "This is a debug message (should be logged if level is DEBUG).")