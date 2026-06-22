# Legacy source: vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_parameter.ksh
# Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_parameter.ksh

import datetime
from dateutil.relativedelta import relativedelta # Requires 'python-dateutil' library to be installed

def is_valid_date_format(date_str: str, date_format: str = '%Y%m%d') -> bool:
    """
    Replicates DWDate_Datum_Check. Checks if a date string matches the expected format.
    Args:
        date_str: The date string to check (e.g., '20231026').
        date_format: The expected format (e.g., '%Y%m%d').
    Returns:
        True if valid, False otherwise.
    """
    try:
        datetime.datetime.strptime(date_str, date_format)
        return True
    except ValueError:
        return False

def is_date1_le_date2(date_str1: str, date_str2: str, date_format: str = '%Y%m%d') -> bool:
    """
    Replicates DWDate_Datum_LE. Checks if date_str1 is less than or equal to date_str2.
    Args:
        date_str1: The first date string.
        date_str2: The second date string.
        date_format: The expected format.
    Returns:
        True if date1 <= date2, False otherwise. Returns False if dates are invalid.
    """
    if not is_valid_date_format(date_str1, date_format) or not is_valid_date_format(date_str2, date_format):
        return False
    
    date1 = datetime.datetime.strptime(date_str1, date_format).date()
    date2 = datetime.datetime.strptime(date_str2, date_format).date()
    return date1 <= date2

def get_date_range_from_span(span_value: int, unit: str, reference_date_str: str = None, date_format: str = '%Y%m%d') -> tuple[str, str]:
    """
    Replicates DWDate_Gib_Zeitraum behavior for calculating date ranges.
    Calculates a start and end date based on a numeric span and unit, relative to a reference date.
    Args:
        span_value: The numeric span (e.g., 7 for 7 days/months).
        unit: 'D' for days, 'M' for months.
        reference_date_str: Optional reference date string (YYYYMMDD). If None, uses today.
        date_format: The expected date format.
    Returns:
        A tuple (start_date_str, end_date_str) in the specified format.
    Raises:
        ValueError: If unit is invalid or span_value is not positive, or reference date is invalid.
    """
    if reference_date_str:
        if not is_valid_date_format(reference_date_str, date_format):
            raise ValueError(f"Invalid reference date format: {reference_date_str}")
        ref_date = datetime.datetime.strptime(reference_date_str, date_format).date()
    else:
        ref_date = datetime.date.today()

    if span_value <= 0:
        raise ValueError("Span value must be positive.")

    if unit == 'D':
        start_date = ref_date - datetime.timedelta(days=span_value - 1)
        end_date = ref_date
    elif unit == 'M':
        first_day_of_ref_month = ref_date.replace(day=1)
        start_date = first_day_of_ref_month - relativedelta(months=span_value - 1)
        
        # Calculate the last day of the reference month
        last_day_of_ref_month = (first_day_of_ref_month + relativedelta(months=1)) - datetime.timedelta(days=1)
        end_date = last_day_of_ref_month
    else:
        raise ValueError(f"Invalid unit for span: {unit}. Must be 'D' or 'M'.")

    return start_date.strftime(date_format), end_date.strftime(date_format)

def pruefeZahlPositiv(value: str, parameter_name: str) -> bool:
    """
    Checks if a given value is numeric and non-negative.
    Args:
        value: The string value to check.
        parameter_name: The name of the parameter for error reporting (unused in this simplified version).
    Returns:
        True if numeric and non-negative, False otherwise.
    """
    try:
        num_value = float(value)
        return num_value >= 0
    except ValueError:
        return False

def pruefeZeitraum(start_date_str: str, end_date_str: str, date_format: str = '%Y%m%d') -> bool:
    """
    Validates if two YYYYMMDD formatted dates form a valid period (start <= end).
    Args:
        start_date_str: The start date string (YYYYMMDD).
        end_date_str: The end date string (YYYYMMDD).
        date_format: The expected date format.
    Returns:
        True if the period is valid, False otherwise.
    """
    if not is_valid_date_format(start_date_str, date_format):
        return False
    if not is_valid_date_format(end_date_str, date_format):
        return False

    return is_date1_le_date2(start_date_str, end_date_str, date_format)

def pruefeZeitParameter(p_anfangsdatum: str, p_endedatum: str, p_zeitoffset: str, date_format: str = '%Y%m%d') -> tuple[bool, str]:
    """
    Validates combinations of start date, end date, and time span.
    Returns a tuple (is_valid, error_message).
    """
    # Case 1: Both start and end dates are provided
    if p_anfangsdatum and p_endedatum:
        if p_zeitoffset:
            return False, "Error: Cannot provide both dates and time offset."
        if not pruefeZeitraum(p_anfangsdatum, p_endedatum, date_format):
            return False, "Error: Invalid date range (start date after end date or invalid format)."
        return True, ""
    # Case 2: Only time offset is provided
    elif p_zeitoffset:
        if p_anfangsdatum or p_endedatum:
            return False, "Error: Cannot provide time offset with start or end date."
        if not pruefeZahlPositiv(p_zeitoffset, "zeitoffset"):
            return False, "Error: Time offset must be a positive number."
        return True, ""
    # Case 3: No parameters provided or invalid combination (e.g., only start or only end)
    else:
        return False, "Error: Either start/end dates or time offset must be provided."

def konvertiereZeitspanne(p_spanne: str, p_kennzahl: str, date_format: str = '%Y%m%d') -> tuple[str, str] | tuple[None, None]:
    """
    Calculates start and end dates based on a numeric span and key figure.
    Returns (start_date_str, end_date_str) or (None, None) if invalid.
    """
    if not pruefeZahlPositiv(p_spanne, "p_spanne"):
        return None, None

    span_value = int(float(p_spanne)) # Ensure it's an integer after checking positive
    offset_unit = 'D' # Default to Day

    # Determine offset unit based on p_kennzahl from original KornShell logic
    if p_kennzahl.lower() == "bst":
        offset_unit = 'M'

    try:
        start_date, end_date = get_date_range_from_span(span_value, offset_unit, date_format=date_format)
        return start_date, end_date
    except ValueError:
        return None, None