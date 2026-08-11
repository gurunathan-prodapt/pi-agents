#!/usr/bin/env python3
import os
import sys
import argparse
import calendar
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

# REVIEW-STRUCT: environment file [.dw_init] not supplied — variables it sets are unknown; do not guess their names or values.

def dw_date_vormonat(var_name: str, format_str: str) -> str:
    """
    Calculates previous month date formatted according to format_str.
    Returns the formatted date string and prints the shell assignment.
    """
    # Step 1: Calculate previous month based on current time
    today = datetime.now()
    first_of_this_month = today.replace(day=1)
    previous_month_date = first_of_this_month - timedelta(days=1)
    
    # Step 2: Map Oracle format string to Python strftime format
    py_format = format_str.replace('YYYY', '%Y').replace('MM', '%m').replace('DD', '%d')
    result = previous_month_date.strftime(py_format)
    
    print(f"{var_name}={result}")
    return result


def dw_date_datum_check(wert: str, format_str: str) -> bool:
    """
    Returns True if 'wert' is a valid date matching 'format_str', else False.
    """
    # Step 1: Map format string
    py_format = format_str.replace('YYYY', '%Y').replace('MM', '%m').replace('DD', '%d')
    
    # Step 2: Try parsing date using Python native library
    try:
        datetime.strptime(wert, py_format)
        return True
    except ValueError:
        return False


def dw_date_datum_le(datum1_str: str, datum2_str: str) -> bool:
    """
    Asserts if datum1 <= datum2 (both in YYYYMMDD format).
    Raises ValueError if datum1 > datum2.
    """
    # Step 1: Parse input strings
    try:
        d1 = datetime.strptime(datum1_str, "%Y%m%d")
        d2 = datetime.strptime(datum2_str, "%Y%m%d")
    except ValueError as e:
        raise ValueError(f"Invalid date format (expected YYYYMMDD): {e}")

    # Step 2: Perform comparative logic
    if d1 > d2:
        raise ValueError(f"Datum {datum1_str} ist groesser als {datum2_str}")
    
    return True


def dw_date_gib_zeitraum(offset: int, stufe: str, format_str: str) -> tuple:
    """
    Computes a start (now) and end date range based on offset and unit step.
    Returns (start_date_str, end_date_str).
    """
    # Step 1: Establish current system date
    start_dt = datetime.now()
    py_format = format_str.replace('YYYY', '%Y').replace('MM', '%m').replace('DD', '%d')

    # Step 2: Compute end date based on Step 'stufe' (Y, M, D)
    stufe = stufe.upper()
    if stufe == 'D':
        end_dt = start_dt + timedelta(days=offset)
    elif stufe == 'M':
        # Align behavior with original logic (Month is based on Ultimo and firsts)
        # End date is offset by 'offset' months, adjusted to end of that month
        target_dt = start_dt + relativedelta(months=offset)
        # Standardize: Start of range is 1st of current month, End is last day of target month
        start_dt = start_dt.replace(day=1)
        _, last_day = calendar.monthrange(target_dt.year, target_dt.month)
        end_dt = target_dt.replace(day=last_day)
    elif stufe == 'Y':
        # Standardize: Start of range is New Year of current year, End is New Year's Eve of target year
        target_dt = start_dt + relativedelta(years=offset)
        start_dt = start_dt.replace(month=1, day=1)
        end_dt = target_dt.replace(month=12, day=31)
    else: 
        raise ValueError(f"Unsupported stufe: {stufe}. Must be 'Y', 'M', or 'D'.")

    return start_dt.strftime(py_format), end_dt.strftime(py_format)


def dw_date_gib_zeitraum_cli(offset: int, stufe: str, format_str: str, var_start: str, var_ende: str):
    """
    Wrapper for DWDate_Gib_Zeitraum CLI call to print shell variables.
    Preserves original output literals on error.
    """
    try:
        start_val, end_val = dw_date_gib_zeitraum(offset, stufe, format_str)
        print(f"{var_start}={start_val}")
        print(f"{var_ende}={end_val}")
    except Exception as e:
        # Output/Print Literal Preservation Character-for-Character
        print("!! Interner Fehler bei der Rueckgabe von Datumswerten", file=sys.stderr)
        print("   Funktion: DWDate_Gib_Zeitraum", file=sys.stderr)
        print("   1 Zeile erwartet, 0 Zeile(n) bekommen", file=sys.stderr)
        sys.exit(1)


def letzter_tag_des_monats(date_str: str) -> bool:
    """
    Returns True if date_str (YYYYMMDD) is the last day of its month.
    """
    try:
        jahr = int(date_str[0:4])
        monat = int(date_str[4:6])
        tag = int(date_str[6:8])
    except (ValueError, IndexError):
        return False
        
    if (jahr % 4 == 0 and jahr % 100 != 0) or (jahr % 400 == 0):
        letzter_feb = 29
    else:
        letzter_feb = 28
        
    letzter_tag = [0, 31, letzter_feb, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    
    if 1 <= monat <= 12:
        return letzter_tag[monat] == tag
    return False


def tage_im_monat(year: int, month: int) -> int:
    """
    Returns total days in the specified year and month.
    """
    if (year % 4 == 0 and year % 100 != 0) or (year % 400 == 0):
        letzter_feb = 29
    else:
        letzter_feb = 28
        
    letzter_tag = [0, 31, letzter_feb, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    return letzter_tag[month]


def addiere_datum(date_str: str, days_to_add: int) -> str:
    """
    Adds integer days to date string YYYYMMDD and returns resulting YYYYMMDD string.
    """
    base_dt = datetime.strptime(date_str, "%Y%m%d")
    result_dt = base_dt + timedelta(days=days_to_add)
    return result_dt.strftime("%Y%m%d")


def main():
    parser = argparse.ArgumentParser(description="Python port of h_alis_date.ksh functions")
    subparsers = parser.add_subparsers(dest="command", help="Function to execute")

    # DWDate_Vormonat
    p_vormonat = subparsers.add_parser("DWDate_Vormonat")
    p_vormonat.add_argument("var_name")
    p_vormonat.add_argument("format")

    # DWDate_Datum_Check
    p_check = subparsers.add_parser("DWDate_Datum_Check")
    p_check.add_argument("wert")
    p_check.add_argument("format")

    # DWDate_Datum_LE
    p_le = subparsers.add_parser("DWDate_Datum_LE")
    p_le.add_argument("datum1")
    p_le.add_argument("datum2")

    # DWDate_Gib_Zeitraum
    p_zeitraum = subparsers.add_parser("DWDate_Gib_Zeitraum")
    p_zeitraum.add_argument("offset", type=int)
    p_zeitraum.add_argument("stufe")
    p_zeitraum.add_argument("format")
    p_zeitraum.add_argument("var_start")
    p_zeitraum.add_argument("var_ende")

    # LetzterTagDesMonats
    p_last = subparsers.add_parser("LetzterTagDesMonats")
    p_last.add_argument("date_str")

    # TageimMonat
    p_days = subparsers.add_parser("TageimMonat")
    p_days.add_argument("year", type=int)
    p_days.add_argument("month", type=int)

    # AddiereDatum
    p_add = subparsers.add_parser("AddiereDatum")
    p_add.add_argument("date_str")
    p_add.add_argument("days", type=int)

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        return 1

    if args.command == "DWDate_Vormonat":
        dw_date_vormonat(args.var_name, args.format)
        return 0

    elif args.command == "DWDate_Datum_Check":
        is_valid = dw_date_datum_check(args.wert, args.format)
        if is_valid:
            return 0
        else:
            return 1

    elif args.command == "DWDate_Datum_LE":
        try:
            dw_date_datum_le(args.datum1, args.datum2)
            return 0
        except ValueError as e:
            print(f"Error: {e}", file=sys.stderr)
            return 1

    elif args.command == "DWDate_Gib_Zeitraum":
        dw_date_gib_zeitraum_cli(args.offset, args.stufe, args.format, args.var_start, args.var_ende)
        return 0

    elif args.command == "LetzterTagDesMonats":
        is_last = letzter_tag_des_monats(args.date_str)
        if is_last:
            return 0
        else:
            return 1

    elif args.command == "TageimMonat":
        days = tage_im_monat(args.year, args.month)
        print(days)
        return 0

    elif args.command == "AddiereDatum":
        res = addiere_datum(args.date_str, args.days)
        print(res)
        return 0

    return 0

if __name__ == "__main__":
    sys.exit(main())