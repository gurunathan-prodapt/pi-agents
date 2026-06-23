# Legacy Source: h_alis_date.ksh for vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_cntrct_crs3.ksh
# Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_cntrct_crs3.ksh

"""
This module provides date calculation and validation utilities,
mimicking the functionality of h_alis_date.ksh.

It uses Python's standard `datetime` module for date operations.
The original script used `sqlplus` calls to an Oracle database for many functions,
which are replaced here with pure Python logic.
"""

from datetime import datetime, timedelta
from calendar import monthrange
import logging

logger = logging.getLogger(__name__)

class DWDateError(Exception):
    """Custom exception for DWDate-related errors."""
    pass

def dwdate_vormonat(date_format: str = "%Y%m%d") -> str:
    """
    Mimics DWDate_Vormonat. Calculates the previous month's date.
    The original script fetched this from Oracle's SYSDATE.
    Here, it returns the first day of the previous month relative to today.
    """
    today = datetime.now()
    first_day_of_current_month = today.replace(day=1)
    last_day_of_previous_month = first_day_of_current_month - timedelta(days=1)
    first_day_of_previous_month = last_day_of_previous_month.replace(day=1)
    logger.debug(f"Previous month's first day: {first_day_of_previous_month.strftime(date_format)}")
    return first_day_of_previous_month.strftime(date_format)

def dwdate_datum_check(date_value: str, date_format_str: str) -> bool:
    """
    Mimics DWDate_Datum_Check. Checks if a date string matches a given format.
    Returns True if valid, False otherwise.
    """
    try:
        datetime.strptime(date_value, date_format_str)
        logger.debug(f"Date '{date_value}' matches format '{date_format_str}'.")
        return True
    except ValueError:
        logger.warning(f"Date '{date_value}' does NOT match format '{date_format_str}'.")
        return False

def dwdate_datum_le(date1_str: str, date2_str: str, date_format_str: str = "%Y%m%d") -> bool:
    """
    Mimics DWDate_Datum_LE. Checks if date1 is less than or equal to date2.
    Dates are expected in YYYYMMDD format by default.
    Returns True if date1 <= date2, False otherwise.
    Raises DWDateError if dates are invalid or format mismatch.
    """
    try:
        date1 = datetime.strptime(date1_str, date_format_str)
        date2 = datetime.strptime(date2_str, date_format_str)
        is_le = date1 <= date2
        logger.debug(f"'{date1_str}' <= '{date2_str}' is {is_le}")
        return is_le
    except ValueError as e:
        logger.error(f"Invalid date format or value for comparison: {e}")
        raise DWDateError(f"Invalid date format or value for DWDate_Datum_LE: {e}")

def dwdate_gib_zeitraum(offset: int, unit: str, result_format: str = "%Y%m%d") -> tuple[str, str]:
    """
    Mimics DWDate_Gib_Zeitraum. Calculates a date range based on an offset and unit.
    Unit can be 'D' (Day), 'M' (Month), 'Y' (Year).
    Returns (start_date, end_date) as strings in result_format.
    The original script used Oracle's SYSDATE as a reference. Here, it uses today.
    """
    today = datetime.now()
    start_date = today
    end_date = today

    if unit == 'D':
        end_date = today + timedelta(days=offset)
        start_date = today # Startpunkt ist Sysdate (heute)
    elif unit == 'M':
        # Calculate first day of current month
        start_date_month_calc = today.replace(day=1)
        # Add/subtract months
        target_month = start_date_month_calc.month + offset
        target_year = start_date_month_calc.year + (target_month - 1) // 12
        target_month = (target_month - 1) % 12 + 1
        
        # This handles negative offsets and wraps years correctly
        # The original script for months/years always takes the 1st/ultimo
        # For simplicity, if offset is N months, the range is from the
        # 1st of current month to the last day of (current month + offset).
        # This implementation aligns more with "add offset to current date logic"
        # rather than "N months from current month's start/end"
        
        # Oracle's ADD_MONTHS often behaves differently for end-of-month dates.
        # Simple Python approach for month/year offsets:
        # Get target year and month after applying offset
        temp_date = today
        for _ in range(abs(offset)):
            if offset > 0:
                # Add one month, handling year rollover
                next_month = temp_date.month % 12 + 1
                next_year = temp_date.year + (1 if next_month == 1 else 0)
                temp_date = temp_date.replace(year=next_year, month=next_month, day=1)
            else:
                # Subtract one month, handling year rollover
                prev_month = (temp_date.month - 2 + 12) % 12 + 1
                prev_year = temp_date.year - (1 if prev_month == 12 else 0)
                temp_date = temp_date.replace(year=prev_year, month=prev_month, day=1)

        if offset > 0:
            start_date = today.replace(day=1)
            end_date = temp_date.replace(day=monthrange(temp_date.year, temp_date.month)[1])
        else: # Offset is negative or zero
            start_date = temp_date.replace(day=1)
            end_date = today.replace(day=monthrange(today.year, today.month)[1])
            
    elif unit == 'Y':
        if offset > 0:
            start_date = today.replace(month=1, day=1)
            end_date = today.replace(year=today.year + offset, month=12, day=31)
        else: # Offset is negative or zero
            start_date = today.replace(year=today.year + offset, month=1, day=1)
            end_date = today.replace(month=12, day=31)
    else:
        raise DWDateError(f"Unsupported unit for DWDate_Gib_Zeitraum: {unit}. Must be 'D', 'M', or 'Y'.")

    return start_date.strftime(result_format), end_date.strftime(result_format)

def letzter_tag_des_monats(date_str: str, date_format_str: str = "%Y%m%d") -> bool:
    """
    Mimics LetzterTagDesMonats. Checks if the given date is the last day of its month.
    """
    try:
        date_obj = datetime.strptime(date_str, date_format_str)
        _, last_day = monthrange(date_obj.year, date_obj.month)
        is_last_day = date_obj.day == last_day
        logger.debug(f"'{date_str}' is last day of month: {is_last_day}")
        return is_last_day
    except ValueError as e:
        logger.error(f"Invalid date format or value for letzter_tag_des_monats: {e}")
        raise DWDateError(f"Invalid date format or value: {e}")

def tage_im_monat(year: int, month: int) -> int:
    """
    Mimics TageimMonat. Returns the number of days in a given month and year.
    """
    if not (1 <= month <= 12):
        raise DWDateError(f"Invalid month: {month}")
    days = monthrange(year, month)[1]
    logger.debug(f"Days in {month}/{year}: {days}")
    return days

def addiere_datum(date_str: str, days_to_add: int, date_format_str: str = "%Y%m%d") -> str:
    """
    Mimics AddiereDatum. Adds a number of days to a given date.
    Returns the new date string in the specified format.
    """
    try:
        date_obj = datetime.strptime(date_str, date_format_str)
        new_date = date_obj + timedelta(days=days_to_add)
        logger.debug(f"'{date_str}' + {days_to_add} days = '{new_date.strftime(date_format_str)}'")
        return new_date.strftime(date_format_str)
    except ValueError as e:
        logger.error(f"Invalid date format or value for addiere_datum: {e}")
        raise DWDateError(f"Invalid date format or value: {e}")

if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)

    print(f"Vormonat: {dwdate_vormonat()}")
    print(f"Datum Check '20231026', '%Y%m%d': {dwdate_datum_check('20231026', '%Y%m%d')}")
    print(f"Datum Check '2023-10-26', '%Y%m%d': {dwdate_datum_check('2023-10-26', '%Y%m%d')}")
    
    try:
        print(f"Datum LE '20231026', '20231027': {dwdate_datum_le('20231026', '20231027')}")
        print(f"Datum LE '20231026', '20231026': {dwdate_datum_le('20231026', '20231026')}")
        print(f"Datum LE '20231028', '20231027': {dwdate_datum_le('20231028', '20231027')}")
    except DWDateError as e:
        print(f"Error in DWDate_Datum_LE example: {e}")

    try:
        start_d, end_d = dwdate_gib_zeitraum(5, 'D')
        print(f"Gib Zeitraum (5D): Start={start_d}, End={end_d}")
        start_m, end_m = dwdate_gib_zeitraum(-2, 'M')
        print(f"Gib Zeitraum (-2M): Start={start_m}, End={end_m}")
        start_y, end_y = dwdate_gib_zeitraum(1, 'Y')
        print(f"Gib Zeitraum (1Y): Start={start_y}, End={end_y}")
    except DWDateError as e:
        print(f"Error in DWDate_Gib_Zeitraum example: {e}")

    print(f"Letzter Tag des Monats '20231031': {letzter_tag_des_monats('20231031')}")
    print(f"Letzter Tag des Monats '20231026': {letzter_tag_des_monats('20231026')}")
    print(f"Tage im Monat (2024, 2): {tage_im_monat(2024, 2)}")
    print(f"Tage im Monat (2023, 2): {tage_im_monat(2023, 2)}")
    print(f"Addiere Datum '20231026', 7 Tage: {addiere_datum('20231026', 7)}")
    print(f"Addiere Datum '20231026', -7 Tage: {addiere_datum('20231026', -7)}")