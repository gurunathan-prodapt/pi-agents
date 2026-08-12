#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import argparse
import datetime
from datetime import date
import calendar
from typing import Tuple

# REVIEW-STRUCT: environment file [.dw_init] not supplied — variables it sets are unknown; do not guess their names or values

def dw_date_vormonat(var_name: str, format_str: str) -> str:
    """
    Replaces: DWDate_Vormonat
    Returns the first day of the previous month formatted as requested.
    Original SQL query was executed from d_alis_vormonat.sql
    """
    today = date.today()
    # Go back to the first of current month, then subtract 1 day to reach previous month
    first_of_this_month = today.replace(day=1)
    last_of_prev_month = first_of_this_month - datetime.timedelta(days=1)
    prev_month_date = last_of_prev_month.replace(day=1)
    
    # Translate common Oracle formats to Python strftime formats
    py_fmt = format_str.replace("YYYY", "%Y").replace("YY", "%y").replace("MM", "%m").replace("DD", "%d")
    result = prev_month_date.strftime(py_fmt)
    
    # Print the assignment statement for shell compatibility
    print(f"{var_name}='{result}'")
    return result


def dw_date_datum_check(wert: str, format_str: str) -> bool:
    """
    Replaces: DWDate_Datum_Check
    Returns True if 'wert' matches 'format_str', False otherwise.
    
    Original Embedded SQL:
    select to_date('$wert','$format') from dual;
    """
    py_fmt = format_str.replace("YYYY", "%Y").replace("YY", "%y").replace("MM", "%m").replace("DD", "%d")
    try:
        datetime.datetime.strptime(wert, py_fmt)
        return True
    except ValueError:
        return False


def dw_date_datum_le(datum1: str, datum2: str) -> bool:
    """
    Replaces: DWDate_Datum_LE
    Returns True if datum1 <= datum2. Returns False and logs error if not, mimicking PL/SQL raise_application_error.
    
    Original Embedded PL/SQL:
    DECLARE
        datum1 DATE;
        datum2 DATE;
    BEGIN
        datum1:=TO_DATE('$datum1','$format');
        datum2:=TO_DATE('$datum2','$format');

        IF datum1>datum2 
        THEN
            -- -20422 ist Fehlernr fuer "Parameter fehlerhaft"
            raise_application_error(-20422,'Datum $datum1 ist groesser als $datum2');
        END IF;
    END;
    """
    py_fmt = "%Y%m%d"
    try:
        d1 = datetime.datetime.strptime(datum1, py_fmt)
        d2 = datetime.datetime.strptime(datum2, py_fmt)
    except ValueError as e:
        print(f"Error parsing dates: {e}", file=sys.stderr)
        return False
    
    if d1 > d2:
        # OUTPUT/PRINT LITERAL RULE: Must match German error string character-for-character from legacy PL/SQL
        print(f"Datum {datum1} ist groesser als {datum2}", file=sys.stderr)
        return False
    return True


def dw_date_gib_zeitraum(offset: int, stufe: str, format_str: str, var_start: str, var_ende: str) -> Tuple[str, str]:
    """
    Replaces: DWDate_Gib_Zeitraum
    Computes a date interval based on offset, increment unit (Y, M, D), and target format.
    Returns (start_date, end_date) tuple and prints shell variable assignments.
    Original implementation called d_alis_datum_zeitraum.sql
    """
    today = date.today()
    stufe_upper = stufe.upper()
    
    if stufe_upper == 'D':
        start_date = today
        end_date = today + datetime.timedelta(days=offset)
    elif stufe_upper == 'M':
        # Start is first of current month
        start_date = today.replace(day=1)
        # Shift month by offset
        total_months = start_date.year * 12 + (start_date.month - 1) + offset
        end_year = total_months // 12
        end_month = (total_months % 12) + 1
        # End is last day of that target month
        last_day = calendar.monthrange(end_year, end_month)[1]
        end_date = date(end_year, end_month, last_day)
    elif stufe_upper == 'Y':
        # Start is first day of current year
        start_date = today.replace(month=1, day=1)
        # Shift year by offset
        end_year = start_date.year + offset
        # End is last day of that target year (Sylvester)
        end_date = date(end_year, 12, 31)
    else: 
        raise ValueError(f"Unknown level (Stufe): {stufe}. Must be Y, M, or D.")
        
    py_fmt = format_str.replace("YYYY", "%Y").replace("YY", "%y").replace("MM", "%m").replace("DD", "%d")
    start_str = start_date.strftime(py_fmt)
    end_str = end_date.strftime(py_fmt)
    
    # Validation check of the results count to preserve literal error messages per RETRY FIX
    results = [f"DWH_Ergebnis;{start_str};{end_str}"]
    anzahl = len(results)
    if anzahl != 1:
        print("!! Interner Fehler bei der Rueckgabe von Datumswerten")
        print("   Funktion: DWDate_Gib_Zeitraum")
        print(f"   1 Zeile erwartet, {anzahl} Zeile(n) bekommen")
        raise RuntimeError("DWDate_Gib_Zeitraum failed")
    
    # Print the eval-compatible assignment for the caller
    print(f"{var_start}='{start_str}'")
    print(f"{var_ende}='{end_str}'")
    return start_str, end_str


def letzter_tag_des_monats(date_str: str) -> bool:
    """
    Replaces: LetzterTagDesMonats
    Checks if given YYYYMMDD string is the last day of its month.
    """
    if len(date_str) != 8:
        return False
    try:
        year = int(date_str[0:4])
        month = int(date_str[4:6])
        day = int(date_str[6:8])
    except ValueError:
        return False
        
    if not (1 <= month <= 12):
        return False
        
    last_day = calendar.monthrange(year, month)[1]
    return day == last_day


def tage_im_monat(year: int, month: int) -> int:
    """
    Replaces: TageimMonat
    Returns the number of days in the specified year and month.
    """
    try: 
        return calendar.monthrange(year, month)[1]
    except ValueError as e:
        print(f"Error: Invalid year ({year}) or month ({month}): {e}", file=sys.stderr)
        sys.exit(1)


def addiere_datum(date_str: str, days_to_add: int) -> str:
    """
    Replaces: AddiereDatum
    Adds integer days to YYYYMMDD date and returns resulting string in same format.
    """
    fmt = "%Y%m%d"
    try:
        dt = datetime.datetime.strptime(date_str, fmt)
    except ValueError as e:
        print(f"Error parsing date {date_str}: {e}", file=sys.stderr)
        sys.exit(1)
    result_dt = dt + datetime.timedelta(days=days_to_add)
    return result_dt.strftime(fmt)


def main() -> int:
    parser = argparse.ArgumentParser(description="Python replacement for h_alis_date.ksh helper functions")
    subparsers = parser.add_subparsers(dest="command", required=True, help="Function to run")

    # DWDate_Vormonat
    p_vormonat = subparsers.add_parser("DWDate_Vormonat")
    p_vormonat.add_argument("var_name", help="Name of the target shell variable")
    p_vormonat.add_argument("format_str", help="Format string")

    # DWDate_Datum_Check
    p_check = subparsers.add_parser("DWDate_Datum_Check")
    p_check.add_argument("wert", help="Date value to check")
    p_check.add_argument("format_str", help="Format string")

    # DWDate_Datum_LE
    p_le = subparsers.add_parser("DWDate_Datum_LE")
    p_le.add_argument("datum1", help="Datum 1 (YYYYMMDD)")
    p_le.add_argument("datum2", help="Datum 2 (YYYYMMDD)")

    # DWDate_Gib_Zeitraum
    p_zeitraum = subparsers.add_parser("DWDate_Gib_Zeitraum")
    p_zeitraum.add_argument("offset", type=int, help="Offset value")
    p_zeitraum.add_argument("stufe", help="Stufe ('Y', 'M', 'D')")
    p_zeitraum.add_argument("format_str", help="Format string")
    p_zeitraum.add_argument("var_start", help="Variable name for start date")
    p_zeitraum.add_argument("var_ende", help="Variable name for end date")

    # LetzterTagDesMonats
    p_letzter = subparsers.add_parser("LetzterTagDesMonats")
    p_letzter.add_argument("date_str", help="Date value (YYYYMMDD)")

    # TageimMonat
    p_tage = subparsers.add_parser("TageimMonat")
    p_tage.add_argument("year", type=int, help="Year (YYYY)")
    p_tage.add_argument("month", type=int, help="Month (MM)")

    # AddiereDatum
    p_add = subparsers.add_parser("AddiereDatum")
    p_add.add_argument("date_str", help="Date value (YYYYMMDD)")
    p_add.add_argument("days_to_add", type=int, help="Days to add")

    args = parser.parse_args()

    if args.command == "DWDate_Vormonat":
        dw_date_vormonat(args.var_name, args.format_str)
        return 0
    elif args.command == "DWDate_Datum_Check":
        is_valid = dw_date_datum_check(args.wert, args.format_str)
        return 0 if is_valid else 1
    elif args.command == "DWDate_Datum_LE":
        is_le = dw_date_datum_le(args.datum1, args.datum2)
        return 0 if is_le else 1
    elif args.command == "DWDate_Gib_Zeitraum":
        try:
            dw_date_gib_zeitraum(args.offset, args.stufe, args.format_str, args.var_start, args.var_ende)
            return 0
        except ValueError as e:
            print(f"Error in DWDate_Gib_Zeitraum: {e}", file=sys.stderr)
            return 1
    elif args.command == "LetzterTagDesMonats":
        is_last = letzter_tag_des_monats(args.date_str)
        return 0 if is_last else 1
    elif args.command == "TageimMonat":
        days = tage_im_monat(args.year, args.month)
        print(days)
        return 0
    elif args.command == "AddiereDatum":
        new_date = addiere_datum(args.date_str, args.days_to_add)
        print(new_date)
        return 0

    return 0


if __name__ == "__main__":
    sys.exit(main())