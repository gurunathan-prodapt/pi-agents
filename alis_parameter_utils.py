# Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_parameter.ksh
# Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_parameter.ksh

"""
This module provides utility functions for parsing, validating, and converting
various parameters, re-implementing the functionality of the legacy KornShell
script h_alis_parameter.ksh for a BigQuery-centric data platform.
"""

from datetime import datetime, timedelta
import calendar

# --- Custom Exceptions ---
class ParameterError(Exception):
    """Base exception for parameter related errors."""
    pass

class ParameterNotSetError(ParameterError):
    """Raised when a required parameter is not set or empty."""
    pass

class InvalidParameterValueError(ParameterError):
    """Raised when a parameter has an invalid value."""
    pass

class InvalidDateError(ParameterError):
    """Raised when a date is invalid or in the wrong format."""
    pass

class InvalidDateRangeError(ParameterError):
    """Raised when a date range is invalid (e.g., start > end)."""
    pass

class InvalidCombinationError(ParameterError):
    """Raised when a combination of parameters is invalid."""
    pass

# --- Constants ---
DATE_FORMAT = "%Y%m%d"

# --- Placeholder Mappings (MUST BE POPULATED FROM SOURCE KSH) ---
# These dictionaries need to be populated with the actual mappings found in
# the original h_alis_parameter.ksh script's case statements and logic.

KENNZAHL_MAP = {
    # Example: "DESCRIPTIVE_KENNZAHL_NAME": "SHORT_CODE",
    "BUCHUNGEN_TAEGLICH": "BUCHT",
    "UMS_MONATLICH": "BUCHM",
    "VERKAUF_TAEGLICH": "VKFT",
    "BESTAND_MONATLICH": "BSTM",
    # Add all actual Kennzahl mappings here from the KSH script.
}

SYSTEM_MAP = {
    # Example: "DESCRIPTIVE_SYSTEM_NAME": "SHORT_CODE",
    "SAP_ERP": "ERP",
    "CRM_SYSTEM": "CRM",
    "LEGACY_WHS": "LWH",
    # Add all actual System mappings here from the KSH script.
}

SD_NAME_MAP = {
    # Example: "DESCRIPTIVE_SD_NAME": "SHORT_CODE",
    "STAMMDATEN_KUNDEN": "SDK",
    "STAMMDATEN_PRODUKTE": "SDP",
    # Add all actual Stammdaten-Liefersystem (SD Name) mappings here from the KSH script.
}

AUFB_STUFE_XTRA_MAP = {
    # Example: "DESCRIPTIVE_AUFBEREITUNGSSTUFE": "SHORT_CODE",
    "MERGED": "mrg",
    "FILLED": "fill",
    "MERGE_AND_FILL": "mfg",
    # Add all actual Aufbereitungsstufe mappings here from the KSH script.
}

ALLOWED_SYSTEM_KENNZAHL_COMBINATIONS = {
    # Example: ("SYSTEM_CODE", "KENNZAHL_CODE"),
    ("ERP", "BUCHT"),
    ("ERP", "BUCHM"),
    ("CRM", "VKFT"),
    ("LWH", "BSTM"),
    ("ERP", "BSTM"), # Example: ERP can also handle inventory
    # Add all actual allowed (system, kennzahl) combinations here from the KSH script's if/elif logic.
}

KENNZAHL_TO_BEREICH = {
    # Example: "KENNZAHL_CODE": "BEREICH_CODE",
    "BUCHT": "FI",
    "BUCHM": "FI",
    "VKFT": "SD",
    "BSTM": "MM",
    # Add all actual Kennzahl to Bereich mappings here from the KSH script's logic.
}

KENNZAHL_TO_INTERVALL = {
    # Example: "KENNZAHL_CODE": "t" (daily) or "m" (monthly),
    "BUCHT": "t",
    "VKFT": "t",
    "BUCHM": "m",
    "BSTM": "m",
    # Add all actual Kennzahl to Intervall mappings here from the KSH script's logic.
}


# --- Helper Functions ---

def _parse_date(date_str: str, parameter_name: str) -> datetime.date:
    """
    Helper to parse a date string into a datetime.date object.

    Args:
        date_str: The date string in YYYYMMDD format.
        parameter_name: The name of the parameter for error reporting.

    Returns:
        A datetime.date object.

    Raises:
        ParameterNotSetError: If the date string is None or empty.
        InvalidDateError: If the date string is not in YYYYMMDD format.
    """
    if date_str is None or not isinstance(date_str, str) or not date_str.strip():
        raise ParameterNotSetError(f"Date parameter '{parameter_name}' is not set or empty.")
    try:
        return datetime.strptime(date_str, DATE_FORMAT).date()
    except ValueError:
        raise InvalidDateError(
            f"Date parameter '{parameter_name}' has invalid format '{date_str}'. Expected YYYYMMDD."
        )

def _add_months(source_date: datetime.date, months: int) -> datetime.date:
    """
    Adds or subtracts months from a date, handling month-end correctly.
    If the day of the source_date is greater than the number of days in the
    target month, the day is adjusted to the last day of the target month.

    Args:
        source_date: The starting date.
        months: The number of months to add (can be negative for subtraction).

    Returns:
        A new datetime.date object with the months added/subtracted.
    """
    month = source_date.month - 1 + months
    year = source_date.year + month // 12
    month = month % 12 + 1
    day = min(source_date.day, calendar.monthrange(year, month)[1])
    return datetime(year, month, day).date()

# --- Main Utility Functions ---

def pruefeParameterGesetzt(param_value: str, param_name: str):
    """
    Checks if a parameter's value is set and not empty.
    Translates KornShell's checks for environment variables being set.

    Args:
        param_value: The actual value of the parameter.
        param_name: The name of the parameter for error reporting.

    Raises:
        ParameterNotSetError: If the parameter value is None or an empty string
                              after stripping whitespace.
    """
    if param_value is None or not isinstance(param_value, str) or not param_value.strip():
        raise ParameterNotSetError(f"Required parameter '{param_name}' is not set or empty.")

def pruefeZahlPositiv(value: str, parameter_name: str) -> float:
    """
    Checks if a given string value represents a positive number (>= 0).
    Translates KornShell numeric validation.

    Args:
        value: The string value to check.
        parameter_name: The name of the parameter for error reporting.

    Returns:
        The validated number (int or float).

    Raises:
        ParameterNotSetError: If the value is None or empty.
        InvalidParameterValueError: If the value is not a number or is negative.
    """
    pruefeParameterGesetzt(value, parameter_name) # First check if it's set
    try:
        # Try converting to int first, then float for broader compatibility
        number = int(value)
        if number < 0:
            raise InvalidParameterValueError(
                f"Parameter '{parameter_name}' must be a positive number, but got '{value}'."
            )
        return number
    except ValueError:
        try:
            number = float(value)
            if number < 0:
                raise InvalidParameterValueError(
                    f"Parameter '{parameter_name}' must be a positive number, but got '{value}'."
                )
            return number
        except ValueError:
            raise InvalidParameterValueError(
                f"Parameter '{parameter_name}' must be a valid number, but got '{value}'."
            )

def pruefeZeitraum(anfang: str, ende: str):
    """
    Validates a date range, ensuring both dates are valid YYYYMMDD and anfang <= ende.
    Replaces DWDate_Datum_Check and DWDate_Datum_LE.

    Args:
        anfang: The start date string in YYYYMMDD format.
        ende: The end date string in YYYYMMDD format.

    Raises:
        ParameterNotSetError: If `anfang` or `ende` is not set.
        InvalidDateError: If any date is malformed.
        InvalidDateRangeError: If anfang > ende.
    """
    # _parse_date already handles ParameterNotSetError and InvalidDateError
    anfang_dt = _parse_date(anfang, "Anfang (start date)")
    ende_dt = _parse_date(ende, "Ende (end date)")

    if not (anfang_dt <= ende_dt):
        raise InvalidDateRangeError(f"Start date '{anfang}' cannot be after end date '{ende}'.")

def pruefeZeitParameter(anfang: str, ende: str, zeitoffset: str):
    """
    Validates time parameters, ensuring either a valid date range (anfang, ende)
    or a valid positive time offset is provided, but not both.

    Args:
        anfang: Start date string (YYYYMMDD), optional if zeitoffset is provided.
        ende: End date string (YYYYMMDD), optional if zeitoffset is provided.
        zeitoffset: Time offset string (positive number), optional if anfang/ende are provided.

    Raises:
        ParameterNotSetError: If neither date range nor offset is provided.
        InvalidParameterValueError: If both date range and offset are provided.
                                    Or if `zeitoffset` is not a positive number.
        InvalidDateError: If date format is invalid.
        InvalidDateRangeError: If start date is after end date.
    """
    anfang_set = anfang is not None and anfang.strip() != ""
    ende_set = ende is not None and ende.strip() != ""
    zeitoffset_set = zeitoffset is not None and zeitoffset.strip() != ""

    if (anfang_set or ende_set) and zeitoffset_set:
        raise InvalidParameterValueError(
            "Cannot provide both a date range (Anfang/Ende) and a time offset (Zeitoffset)."
        )

    if not (anfang_set or ende_set or zeitoffset_set):
        raise ParameterNotSetError(
            "Either a date range (Anfang/Ende) or a time offset (Zeitoffset) must be provided."
        )

    if anfang_set or ende_set:
        if not (anfang_set and ende_set):
            raise ParameterNotSetError(
                "If providing a date range, both 'Anfang' and 'Ende' must be set."
            )
        pruefeZeitraum(anfang, ende) # This handles format and order
    elif zeitoffset_set:
        pruefeZahlPositiv(zeitoffset, "Zeitoffset") # This handles positive number check

def konvertiereKennzahl(kennzahl_desc: str) -> str:
    """
    Converts a descriptive Kennzahl name to its short code.
    Translates KornShell's case statement for Kennzahl conversion.

    Args:
        kennzahl_desc: The descriptive Kennzahl name.

    Returns:
        The short code for the Kennzahl.

    Raises:
        ParameterNotSetError: If the Kennzahl description is not set.
        InvalidParameterValueError: If the Kennzahl description is unknown.
    """
    pruefeParameterGesetzt(kennzahl_desc, "Kennzahl description")
    try:
        return KENNZAHL_MAP[kennzahl_desc.upper()] # Assuming case-insensitive lookup for input
    except KeyError:
        raise InvalidParameterValueError(f"Unknown Kennzahl description: '{kennzahl_desc}'. "
                                         "Please update KENNZAHL_MAP.")

def konvertiereSystem(system_desc: str) -> str:
    """
    Converts a descriptive System name to its normalized short code.
    Translates KornShell's case statement for System conversion.

    Args:
        system_desc: The descriptive System name.

    Returns:
        The normalized short code for the System.

    Raises:
        ParameterNotSetError: If the System description is not set.
        InvalidParameterValueError: If the System description is unknown.
    """
    pruefeParameterGesetzt(system_desc, "System description")
    try:
        return SYSTEM_MAP[system_desc.upper()]
    except KeyError:
        raise InvalidParameterValueError(f"Unknown System description: '{system_desc}'. "
                                         "Please update SYSTEM_MAP.")

def konvertiereSDName(sd_name_desc: str) -> str:
    """
    Converts a descriptive Stammdaten-Liefersystem (SD Name) to its short code.
    Translates KornShell's case statement for SD Name conversion.

    Args:
        sd_name_desc: The descriptive SD Name.

    Returns:
        The short code for the SD Name.

    Raises:
        ParameterNotSetError: If the SD Name description is not set.
        InvalidParameterValueError: If the SD Name description is unknown.
    """
    pruefeParameterGesetzt(sd_name_desc, "SD Name description")
    try:
        return SD_NAME_MAP[sd_name_desc.upper()]
    except KeyError:
        raise InvalidParameterValueError(f"Unknown SD Name description: '{sd_name_desc}'. "
                                         "Please update SD_NAME_MAP.")

def konvertiereAufbStufeXtra(aufb_stufe_desc: str) -> str:
    """
    Converts an Aufbereitungsstufe name to its short code.
    Translates KornShell's case statement for Aufbereitungsstufe conversion.

    Args:
        aufb_stufe_desc: The descriptive Aufbereitungsstufe name.

    Returns:
        The short code.

    Raises:
        ParameterNotSetError: If the Aufbereitungsstufe description is not set.
        InvalidParameterValueError: If the Aufbereitungsstufe description is unknown.
    """
    pruefeParameterGesetzt(aufb_stufe_desc, "Aufbereitungsstufe description")
    try:
        return AUFB_STUFE_XTRA_MAP[aufb_stufe_desc.upper()]
    except KeyError:
        raise InvalidParameterValueError(f"Unknown Aufbereitungsstufe description: '{aufb_stufe_desc}'. "
                                         "Please update AUFB_STUFE_XTRA_MAP.")

def pruefeSystemKennzahl(system: str, kennzahl: str):
    """
    Validates if a combination of system and kennzahl is allowed based on hardcoded logic.
    Translates KornShell's if/elif logic for system-kennzahl validation.

    Args:
        system: The normalized system code.
        kennzahl: The short kennzahl code.

    Raises:
        ParameterNotSetError: If system or kennzahl is not set.
        InvalidCombinationError: If the system-kennzahl combination is not allowed.
    """
    pruefeParameterGesetzt(system, "System")
    pruefeParameterGesetzt(kennzahl, "Kennzahl")

    if (system, kennzahl) not in ALLOWED_SYSTEM_KENNZAHL_COMBINATIONS:
        raise InvalidCombinationError(
            f"Invalid combination: System '{system}' with Kennzahl '{kennzahl}' is not allowed. "
            "Please update ALLOWED_SYSTEM_KENNZAHL_COMBINATIONS."
        )

def gibBereich(kennzahl: str) -> str:
    """
    Determines the "Bereich" (area) for a given kennzahl by searching predefined lists.
    Translates KornShell's logic for Bereich determination.

    Args:
        kennzahl: The short kennzahl code.

    Returns:
        The area code.

    Raises:
        ParameterNotSetError: If kennzahl is not set.
        InvalidParameterValueError: If the Kennzahl is unknown for Bereich mapping.
    """
    pruefeParameterGesetzt(kennzahl, "Kennzahl")
    try:
        return KENNZAHL_TO_BEREICH[kennzahl]
    except KeyError:
        raise InvalidParameterValueError(f"Unknown Kennzahl '{kennzahl}' for Bereich determination. "
                                         "Please update KENNZAHL_TO_BEREICH.")

def gibIntervall(kennzahl: str) -> str:
    """
    Determines the "Intervall" ('t' for daily, 'm' for monthly) for a given kennzahl.
    Translates KornShell's logic for Intervall determination.

    Args:
        kennzahl: The short kennzahl code.

    Returns:
        The interval code ('t' or 'm').

    Raises:
        ParameterNotSetError: If kennzahl is not set.
        InvalidParameterValueError: If the Kennzahl is unknown for Intervall mapping.
    """
    pruefeParameterGesetzt(kennzahl, "Kennzahl")
    try:
        return KENNZAHL_TO_INTERVALL[kennzahl]
    except KeyError:
        raise InvalidParameterValueError(f"Unknown Kennzahl '{kennzahl}' for Intervall determination. "
                                         "Please update KENNZAHL_TO_INTERVALL.")

def konvertiereZeitspanne(spanne_str: str, kennzahl: str) -> tuple[str, str]:
    """
    Calculates a start and end date based on a given span and kennzahl.
    The period typically ends "today" or at the time of execution.
    Replaces DWDate_Gib_Zeitraum logic.

    Args:
        spanne_str: The span (e.g., "01" for 1 unit).
        kennzahl: The short kennzahl code, which determines the unit ('t' for day, 'm' for month).

    Returns:
        A tuple (start_date_str, end_date_str) in YYYYMMDD format.

    Raises:
        InvalidParameterValueError: If spanne is not a positive number, kennzahl is unknown,
                                    or an unsupported interval unit is derived.
    """
    spanne = int(pruefeZahlPositiv(spanne_str, "Zeitspanne")) # Ensure integer span
    if spanne == 0:
        raise InvalidParameterValueError("Zeitspanne must be greater than 0.")

    today = datetime.now().date()
    end_date = today # Assuming the calculated period ends today by default

    interval_unit = gibIntervall(kennzahl) # 't' for daily, 'm' for monthly

    start_date = None

    if interval_unit == 't': # Daily interval
        start_date = today - timedelta(days=spanne)
    elif interval_unit == 'm': # Monthly interval
        start_date = _add_months(today, -spanne)
    else:
        raise InvalidParameterValueError(f"Unsupported interval unit '{interval_unit}' derived from Kennzahl '{kennzahl}'.")

    return start_date.strftime(DATE_FORMAT), end_date.strftime(DATE_FORMAT)
---