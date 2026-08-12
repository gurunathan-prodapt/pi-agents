#! /usr/bin/env python3
# 
# Zweck:
#   Hilfsroutinen fuer das Rechnen mit Datumswerten
#   Konvertiert von KornShell (h_alis_date.ksh) zu nativem Python.
#   Ersetzt Oracle-Datenbankaufrufe durch in-memory Python datetime-Operationen.
#
# Vorbedingung:
#   Keine Datenbankverbindung erforderlich. Verwaltet Kalenderlogik,
#   Schaltjahre, Monatslaengen und Datumsberechnungen lokal in Python.
#
# Erzeugt von : Ralf Biermanns / Thorsten Juergens / Ingo Schwitters (Original KSH)
# Migriert von : Senior Data Engineer (Target: BigQuery/Python)

import sys
import os
import datetime
import calendar

def dw_date_vormonat(format_fmt: str) -> str:
    """
    Calculates the previous month relative to today's date.
    Equivalent to legacy d_alis_vormonat.sql execution.
    """
    today = datetime.date.today()
    first_of_this_month = today.replace(day=1)
    previous_month_dt = first_of_this_month - datetime.timedelta(days=1)
    
    # Map Oracle format symbols to Python strftime format symbols
    fmt_map = {
        "YYYYMMDD": "%Y%m%d",
        "YYYY-MM-DD": "%Y-%m-%d",
        "DD.MM.YYYY": "%d.%m.%Y",
        "YYYYMM": "%Y%m",
    }
    py_fmt = fmt_map.get(format_fmt.upper(), "%Y%m")
    return previous_month_dt.strftime(py_fmt)

def dw_date_datum_check(wert: str, format_str: str) -> bool:
    """
    Validates if a date string is a valid date under the specified format.
    Replaces Oracle select to_date(...) check.
    """
    if not wert or not format_str:
        raise ValueError("DWDate_Datum_Check requires exactly 2 parameters")
        
    # Map Oracle format symbols to Python strftime format symbols
    fmt_map = {
        "YYYYMMDD": "%Y%m%d",
        "YYYY-MM-DD": "%Y-%m-%d",
        "DD.MM.YYYY": "%d.%m.%Y",
        "YYYYMM": "%Y%m",
    }
    py_fmt = fmt_map.get(format_str.upper(), format_str)
    
    try:
        datetime.datetime.strptime(wert, py_fmt)
        return True
    except ValueError:
        return False

def dw_date_datum_le(datum1: str, datum2: str) -> bool:
    """
    Verifies if datum1 <= datum2 where dates are in YYYYMMDD format.
    Replaces PL/SQL block assertion with native exception matching original error text.
    """
    if not datum1 or not datum2:
        raise ValueError("DWDate_Datum_LE requires exactly 2 parameters")
        
    try:
        d1 = datetime.datetime.strptime(datum1, "%Y%m%d")
        d2 = datetime.datetime.strptime(datum2, "%Y%m%d")
    except ValueError as e:
        print(f"Fehler beim Parsen der Daten: {e}", file=sys.stderr)
        return False

    if d1 > d2:
        # Original error message from PL/SQL: 'Datum $datum1 ist groesser als $datum2'
        err_msg = f"Datum {datum1} ist groesser als {datum2}"
        print(err_msg, file=sys.stderr)
        raise ValueError(err_msg)
        
    return True

def _calculate_zeitraum(offset: int, stufe: str, format_str: str) -> tuple:
    """
    Internal helper to generate start and end dates based on offset and step unit.
    """
    today = datetime.date.today()
    
    if stufe == 'D':
        start_dt = today
        end_dt = today + datetime.timedelta(days=offset)
    elif stufe == 'M':
        # Start is always the first day of the current month
        start_dt = today.replace(day=1)
        
        # Calculate end month/year using math to handle overflow of offsets safely
        end_month_val = today.month + offset
        year_offset = (end_month_val - 1) // 12
        end_month = (end_month_val - 1) % 12 + 1
        end_year = today.year + year_offset
        
        _, last_day = calendar.monthrange(end_year, end_month)
        end_dt = datetime.date(end_year, end_month, last_day)
    elif stufe == 'Y':
        # Start is New Year (Jan 1 of current year)
        start_dt = today.replace(month=1, day=1)
        # End is New Year's Eve (Dec 31 of target year)
        end_year = today.year + offset
        end_dt = datetime.date(end_year, 12, 31)
    else: 
        raise ValueError(f"Invalid Stufe: {stufe}")
        
    fmt_map = {
        "YYYYMMDD": "%Y%m%d",
        "YYYY-MM-DD": "%Y-%m-%d",
        "DD.MM.YYYY": "%d.%m.%Y",
        "YYYYMM": "%Y%m",
    }
    py_fmt = fmt_map.get(format_str.upper(), "%Y%m%d")
    
    return start_dt.strftime(py_fmt), end_dt.strftime(py_fmt)

def dw_date_gib_zeitraum(offset: int, stufe: str, format_str: str) -> tuple:
    """
    Generates start and end dates based on offset and steps.
    Replaces legacy d_alis_datum_zeitraum.sql execution.
    """
    try:
        offset_val = int(offset)
    except (ValueError, TypeError):
        print("!! Interner Fehler bei der Rueckgabe von Datumswerten", file=sys.stderr)
        print("   Funktion: DWDate_Gib_Zeitraum", file=sys.stderr)
        print("   1 Zeile erwartet, 0 Zeile(n) bekommen", file=sys.stderr)
        return None
        
    if stufe not in ('Y', 'M', 'D') or not format_str:
        print("!! Interner Fehler bei der Rueckgabe von Datumswerten", file=sys.stderr)
        print("   Funktion: DWDate_Gib_Zeitraum", file=sys.stderr)
        print("   1 Zeile erwartet, 0 Zeile(n) bekommen", file=sys.stderr)
        return None
        
    try:
        return _calculate_zeitraum(offset_val, stufe, format_str)
    except Exception:
        print("!! Interner Fehler bei der Rueckgabe von Datumswerten", file=sys.stderr)
        print("   Funktion: DWDate_Gib_Zeitraum", file=sys.stderr)
        print("   1 Zeile erwartet, 0 Zeile(n) bekommen", file=sys.stderr)
        raise

def letzter_tag_des_monats(date_str: str) -> bool:
    """
    Returns True (equivalent to exit code 0) if date_str (YYYYMMDD) is the last day of the month.
    """
    if len(date_str) < 8:
        raise ValueError("Input date must be in YYYYMMDD format")
        
    jahr = int(date_str[0:4])
    monat = int(date_str[4:6])
    tag = int(date_str[6:8])
    
    if (jahr % 4 == 0 and jahr % 100 != 0) or (jahr % 400 == 0):
        letzter_feb = 29
    else:
        letzter_feb = 28
        
    letzter_tag = [0, 31, letzter_feb, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    
    if 1 <= monat <= 12:
        return letzter_tag[monat] == tag
    return False

def tage_im_monat(jahr: int, monat: int) -> int:
    """
    Returns the number of days in the specified month and year.
    """
    if (jahr % 4 == 0 and jahr % 100 != 0) or (jahr % 400 == 0):
        letzter_feb = 29
    else:
        letzter_feb = 28
        
    letzter_tag = [0, 31, letzter_feb, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    if 1 <= monat <= 12:
        return letzter_tag[monat]
    raise ValueError(f"Invalid month: {monat}")

def addiere_datum(date_str: str, days_to_add: int) -> str:
    """
    Adds offset days to a given YYYYMMDD date string.
    """
    dt = datetime.datetime.strptime(date_str, "%Y%m%d")
    new_dt = dt + datetime.timedelta(days=days_to_add)
    return new_dt.strftime("%Y%m%d")

def main():
    if len(sys.argv) < 2:
        print("Usage: h_alis_date.py <function_name> [args...]", file=sys.stderr)
        sys.exit(1)
        
    func_name = sys.argv[1]
    
    if func_name == "DWDate_Vormonat":
        if len(sys.argv) != 4:
            print("Usage: h_alis_date.py DWDate_Vormonat <var_name> <format>", file=sys.stderr)
            sys.exit(1)
        var_name = sys.argv[2]
        format_fmt = sys.argv[3]
        result = dw_date_vormonat(format_fmt)
        # Output shell variable assignment statement so parent shells can eval this output
        print(f"{var_name}='{result}'")
        sys.exit(0)
        
    elif func_name == "DWDate_Datum_Check":
        if len(sys.argv) != 4:
            print("Usage: h_alis_date.py DWDate_Datum_Check <wert> <format>", file=sys.stderr)
            sys.exit(1)
        wert = sys.argv[2]
        format_str = sys.argv[3]
        valid = dw_date_datum_check(wert, format_str)
        sys.exit(0 if valid else 1)
        
    elif func_name == "DWDate_Datum_LE":
        if len(sys.argv) != 4:
            print("Usage: h_alis_date.py DWDate_Datum_LE <datum1> <datum2>", file=sys.stderr)
            sys.exit(1)
        datum1 = sys.argv[2]
        datum2 = sys.argv[3]
        try:
            valid = dw_date_datum_le(datum1, datum2)
            sys.exit(0 if valid else 1)
        except ValueError:
            sys.exit(1)
            
    elif func_name == "DWDate_Gib_Zeitraum":
        if len(sys.argv) != 7:
            print("Usage: h_alis_date.py DWDate_Gib_Zeitraum <offset> <stufe> <format> <var_start> <var_ende>", file=sys.stderr)
            sys.exit(1)
        offset = int(sys.argv[2])
        stufe = sys.argv[3]
        format_str = sys.argv[4]
        var_start = sys.argv[5]
        var_ende = sys.argv[6]
        res = dw_date_gib_zeitraum(offset, stufe, format_str)
        if res:
            start, ende = res
            # Output shell variable assignment statements so parent shells can eval this output
            print(f"{var_start}='{start}'")
            print(f"{var_ende}='{ende}'")
            sys.exit(0)
        else: 
            sys.exit(1)
            
    elif func_name == "LetzterTagDesMonats":
        if len(sys.argv) != 3:
            print("Usage: h_alis_date.py LetzterTagDesMonats <date_str>", file=sys.stderr)
            sys.exit(1)
        date_str = sys.argv[2]
        is_last = letzter_tag_des_monats(date_str)
        sys.exit(0 if is_last else 1)
        
    elif func_name == "TageimMonat":
        if len(sys.argv) != 4:
            print("Usage: h_alis_date.py TageimMonat <jahr> <monat>", file=sys.stderr)
            sys.exit(1)
        jahr = int(sys.argv[2])
        monat = int(sys.argv[3])
        try:
            days = tage_im_monat(jahr, monat)
            print(days)
            sys.exit(0)
        except ValueError as e:
            print(str(e), file=sys.stderr)
            sys.exit(1)
            
    elif func_name == "AddiereDatum":
        if len(sys.argv) != 4:
            print("Usage: h_alis_date.py AddiereDatum <date_str> <days_to_add>", file=sys.stderr)
            sys.exit(1)
        date_str = sys.argv[2]
        days_to_add = int(sys.argv[3])
        try:
            result = addiere_datum(date_str, days_to_add)
            print(result)
            sys.exit(0)
        except ValueError as e:
            print(str(e), file=sys.stderr)
            sys.exit(1)
            
    else:
        print(f"Unknown function: {func_name}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()