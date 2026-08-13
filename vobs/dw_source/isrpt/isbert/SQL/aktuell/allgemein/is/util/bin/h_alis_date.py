#!/usr/bin/env python3
# 
# Zweck:
#   Hilfsroutinen fuer das Rechnen mit Datumswerten
#   Momentan basiert es noch auf SQLPLUS/SQL (Portiert auf Python Standard-Bibliotheken)
# Vorbedingung:
#   Keine Datenbankabhaengigkeiten mehr; reine In-Memory Datumsberechnungen.
#
# Erzeugt von : Ralf Biermanns
# Erzeugt am  : 03.09.1998
# Aenderungshistorie:
#    0.1.0; 03.09.1998; rb
#      - Initialversion, nur Funktion zur Berechnung des Vormonats
#    2.5.0; 27.09.1999; Thorsten Juergens
#      - Funktion DW_Date_Datum_Check und DW_Date_Datum_LE erstellt 
#    2.5.1; 28.09.1999; Thorsten Juergens
#      - Funktion DWDate_Gib_Zeitraum erstellt 
#    2.5.2; 29.09.1999; Thorsten Juergens
#      - Suche nach Pattern bei DWDate_Gib_Zeitraum zum Umgang mit 
#        Tracing-Ausgaben der SQLPLUS-Session
#    2.5.3; 04.11.1999; Thorsten Juergens
#      - Verhalten von DW_Gib_Zeitraum hat sich geaendert.
#        Monate und Jahre werden nicht mehr Tagesbasis bestimmt,
#        sondern aufgrund von Ultimo und Ersten.
#    3.0.0; 31.01.2000; Ingo Schwitters
#      - Funktion LetzterTagDesMonats hinzugefuegt
#    3.0.1; 15.5.2000; Ingo Schwitters
#      - Funktion AddiereDatum und TageimMonat hinzugefuegt
#    Python-Portierung: 2026
#      - Vollstaendige Portierung auf Python 3 unter Verwendung nativer Datumsberechnungen.

import sys
import os
import calendar
from datetime import datetime, timedelta

def _is_leap_year(year: int) -> bool:
    """Hilfsfunktion: Prueft ob das angegebene Jahr ein Schaltjahr ist."""
    return (year % 4 == 0 and year % 100 != 0) or (year % 400 == 0)

def _map_oracle_format_to_python(oracle_fmt: str) -> str:
    """Hilfsfunktion: Konvertiert Oracle TO_CHAR Datumsformate in Python strftime Patterns."""
    mapping = [
        ("YYYY", "%Y"),
        ("YY", "%y"),
        ("MM", "%m"),
        ("DD", "%d"),
        ("HH24", "%H"),
        ("HH", "%I"),
        ("MI", "%M"),
        ("SS", "%S"),
    ]
    py_fmt = oracle_fmt
    for ora, py in mapping:
        py_fmt = py_fmt.replace(ora, py)
    return py_fmt

def dw_date_vormonat(format_str: str) -> str:
    """
    Berechnet das Datum des vorherigen Monats relativ zum aktuellen Systemdatum
    und gibt es im angegebenen Format zurueck.
    """
    today = datetime.now()
    first_of_this_month = today.replace(day=1)
    last_day_of_prev_month = first_of_this_month - timedelta(days=1)
    py_fmt = _map_oracle_format_to_python(format_str)
    return last_day_of_prev_month.strftime(py_fmt)

def dw_date_datum_check(wert: str, format_str: str) -> bool:
    """
    Prueft, ob der angegebene Wert ein gueltiges Datum gemaess dem Format_str darstellt.
    Rueckgabe: True falls gueltig, sonst False.
    """
    py_fmt = _map_oracle_format_to_python(format_str)
    try:
        datetime.strptime(wert, py_fmt)
        return True
    except ValueError:
        return False

def dw_date_datum_le(datum1_str: str, datum2_str: str) -> bool:
    """
    Prueft, ob Datum1 <= Datum2 (Format YYYYMMDD).
    Wirft ein ValueError, falls Datum1 > Datum2.
    """
    fmt = "%Y%m%d"
    try:
        d1 = datetime.strptime(datum1_str, fmt)
        d2 = datetime.strptime(datum2_str, fmt)
    except ValueError as e:
        raise ValueError(f"Invalid date format: {e}")

    if d1 > d2:
        raise ValueError(f"Datum {datum1_str} ist groesser als {datum2_str}")
    return True

def dw_date_gib_zeitraum(offset: int, stufe: str, format_str: str, var_start: str, var_ende: str) -> tuple:
    """
    Berechnet Start- und Endzeitpunkt auf Basis eines Offsets und einer Stufe ('Y', 'M', 'D').
    Gibt ein Tuple (Startdatum, Enddatum) im gewuenschten Format zurueck.
    """
    try:
        start_dt = datetime.now()
        py_fmt = _map_oracle_format_to_python(format_str)
        
        stufe_upper = stufe.upper()
        if stufe_upper == 'D':
            end_dt = start_dt + timedelta(days=offset)
        elif stufe_upper == 'M':
            target_month_index = start_dt.month - 1 + offset
            target_year = start_dt.year + target_month_index // 12
            target_month = target_month_index % 12 + 1
            
            start_dt = datetime(target_year, target_month, 1)
            _, last_day = calendar.monthrange(target_year, target_month)
            end_dt = datetime(target_year, target_month, last_day)
        elif stufe_upper == 'Y':
            target_year = start_dt.year + offset
            start_dt = datetime(target_year, 1, 1)
            end_dt = datetime(target_year, 12, 31)
        else:
            raise ValueError(f"Unknown period unit Stufe: {stufe}")
            
        return start_dt.strftime(py_fmt), end_dt.strftime(py_fmt)
    except Exception as e:
        print("!! Interner Fehler bei der Rueckgabe von Datumswerten", file=sys.stderr)
        print("   Funktion: DWDate_Gib_Zeitraum", file=sys.stderr)
        print(f"   1 Zeile erwartet, 0 Zeile(n) bekommen (Details: {e})", file=sys.stderr)
        sys.exit(1)

def letzter_tag_des_monats(date_str: str) -> bool:
    """
    Prueft, ob der angegebene Datumswert (Format YYYYMMDD) der letzte Tag des Monats ist.
    """
    try:
        jahr = int(date_str[0:4])
        monat = int(date_str[4:6])
        tag = int(date_str[6:8])
    except (ValueError, IndexError):
        return False
    
    letzter_feb = 29 if _is_leap_year(jahr) else 28
    letzter_tag_arr = [0, 31, letzter_feb, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    
    if monat < 1 or monat > 12:
        return False
    return letzter_tag_arr[monat] == tag

def tage_im_monat(jahr: int, monat: int) -> int:
    """
    Gibt die Anzahl der Tage des Monats im angegebenen Jahr zurueck.
    """
    letzter_feb = 29 if _is_leap_year(jahr) else 28
    letzter_tag_arr = [0, 31, letzter_feb, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    if monat < 1 or monat > 12:
        raise ValueError(f"Invalid month: {monat}")
    return letzter_tag_arr[monat]

def addiere_datum(date_str: str, days_to_add: int) -> str:
    """
    Addiert die Anzahl der Tage auf das angegebene Datum (Format YYYYMMDD)
    und gibt das Ergebnis im gleichen Format zurueck.
    """
    fmt = "%Y%m%d"
    dt = datetime.strptime(date_str, fmt)
    res_dt = dt + timedelta(days=days_to_add)
    return res_dt.strftime(fmt)

def main():
    if len(sys.argv) < 2:
        print("Usage: h_alis_date.py <function_name> [arguments...]", file=sys.stderr)
        return 1

    func_name = sys.argv[1]
    func_args = sys.argv[2:]

    if func_name == "DWDate_Vormonat":
        if len(func_args) != 2:
            print("Usage: DWDate_Vormonat <var_name> <format_str>", file=sys.stderr)
            return 1
        var_name, format_str = func_args
        result = dw_date_vormonat(format_str)
        print(f"{var_name}='{result}'")
        return 0

    elif func_name == "DWDate_Datum_Check":
        if len(func_args) != 2:
            print("Usage: DWDate_Datum_Check <wert> <format_str>", file=sys.stderr)
            return 1
        wert, format_str = func_args
        result = dw_date_datum_check(wert, format_str)
        if result:
            return 0
        else:
            return 1

    elif func_name == "DWDate_Datum_LE":
        if len(func_args) != 2:
            print("Usage: DWDate_Datum_LE <datum1> <datum2>", file=sys.stderr)
            return 1
        datum1, datum2 = func_args
        try:
            result = dw_date_datum_le(datum1, datum2)
            if result:
                return 0
            else:
                return 1
        except ValueError as e:
            print(f"ERROR: {e}", file=sys.stderr)
            return 1

    elif func_name == "DWDate_Gib_Zeitraum":
        if len(func_args) != 5:
            print("Usage: DWDate_Gib_Zeitraum <offset> <stufe> <format> <var_start> <var_ende>", file=sys.stderr)
            return 1
        try:
            offset = int(func_args[0])
        except ValueError:
            print("Error: offset must be an integer", file=sys.stderr)
            return 1
        stufe, format_str, var_start, var_ende = func_args[1:]
        start_val, end_val = dw_date_gib_zeitraum(offset, stufe, format_str, var_start, var_ende)
        print(f"{var_start}='{start_val}'")
        print(f"{var_ende}='{end_val}'")
        return 0

    elif func_name == "LetzterTagDesMonats":
        if len(func_args) != 1:
            print("Usage: LetzterTagDesMonats <date_str>", file=sys.stderr)
            return 1
        result = letzter_tag_des_monats(func_args[0])
        if result:
            return 0
        else:
            return 1

    elif func_name == "TageimMonat":
        if len(func_args) != 2:
            print("Usage: TageimMonat <jahr> <monat>", file=sys.stderr)
            return 1
        try:
            jahr = int(func_args[0])
            monat = int(func_args[1])
        except ValueError:
            print("Error: jahr and monat must be integers", file=sys.stderr)
            return 1
        result = tage_im_monat(jahr, monat)
        print(result)
        return 0

    elif func_name == "AddiereDatum":
        if len(func_args) != 2:
            print("Usage: AddiereDatum <date_str> <days_to_add>", file=sys.stderr)
            return 1
        date_str = func_args[0]
        try:
            days_to_add = int(func_args[1])
        except ValueError:
            print("Error: days_to_add must be an integer", file=sys.stderr)
            return 1
        result = addiere_datum(date_str, days_to_add)
        print(result)
        return 0

    else:
        print(f"Unknown function: {func_name}", file=sys.stderr)
        return 1

if __name__ == "__main__":
    sys.exit(main())