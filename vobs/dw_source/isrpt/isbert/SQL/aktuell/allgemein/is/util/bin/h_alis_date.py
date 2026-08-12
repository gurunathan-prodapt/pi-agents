#! /usr/bin/env python3
# 
# Zweck:
#   Hilfsroutinen fuer das Rechnen mit Datumswerten
#   Konvertiert von KornShell (h_alis_date.ksh) zu nativem Python.
#   Mithilfe von Pythons standardmaessigen 'datetime' und 'calendar' Bibliotheken
#   werden die alten SQLPLUS-Datenbankabfragen fuer hervorragende Performance
#   und Plattformunabhaengigkeit komplett vermieden.
#
# Vorbedingung:
#   Laeuft autonom und plattformunabhaengig.
#
# Erzeugt von : Ralf Biermanns
# Erzeugt am  : 03.09.1998
# Aenderungshistorie (Legacy):
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

import os
import sys
import datetime
import calendar
import argparse

def translate_format(oracle_fmt: str) -> str:
    """
    Translates common Oracle date format masks to Python's strftime/strptime equivalents.
    """
    fmt_map = {
        "YYYYMMDD": "%Y%m%d",
        "YYYYMM": "%Y%m",
        "YYYY-MM-DD": "%Y-%m-%d",
        "DD.MM.YYYY": "%d.%m.%Y",
        "YYYY": "%Y",
        "MM": "%m",
        "DD": "%d"
    }
    cleaned = oracle_fmt.strip("'\"").upper()
    return fmt_map.get(cleaned, "%Y%m%d")

def DWDate_Vormonat(fmt: str) -> str:
    """
    Calculates the previous month of the current system time and formats it.
    """
    now = datetime.datetime.now()
    # Go to the first day of the current month, then subtract 1 day to reach the previous month
    first_of_this_month = now.replace(day=1)
    last_of_prev_month = first_of_this_month - datetime.timedelta(days=1)
    py_fmt = translate_format(fmt)
    return last_of_prev_month.strftime(py_fmt)

def DWDate_Datum_Check(wert: str, format_str: str) -> int:
    """
    Funktion:
      DWDate_Datum_Check
    Parameter:
      P1: zu pruefender Datumswert
      P2: Datumsformat von P1
    Rueckgabe:
      =0, falls Wert P1 ein gueltiges Datum des Formats P1 ist, sonst 1.
    """
    if not wert or not format_str:
        return 1
    
    py_fmt = translate_format(format_str)
    try:
        datetime.datetime.strptime(wert, py_fmt)
        return 0
    except ValueError:
        return 1

def DWDate_Datum_LE(datum1: str, datum2: str) -> int:
    """
    Funktion:
      DWDate_Datum_LE
    Parameter:
      P1: Datum1 im Format YYYYMMDD
      P2: Datum2 im Format YYYYMMDD
    Rueckgabe:
      =0, falls P1<=P2 ist, sonst 1.
    """
    if not datum1 or not datum2:
        return 1

    try:
        d1 = datetime.datetime.strptime(datum1, "%Y%m%d")
        d2 = datetime.datetime.strptime(datum2, "%Y%m%d")
    except ValueError:
        return 1

    if d1 > d2:
        # OUTPUT/PRINT LITERAL RULE: Exact legacy German error message preserved
        print(f"Datum {datum1} ist groesser als {datum2}", file=sys.stderr)
        return 1
    return 0

def DWDate_Gib_Zeitraum(offset: int, stufe: str, format_str: str) -> tuple:
    """
    Funktion:
      DWDate_Gib_Zeitraum
    Parameter:
      I-P1: Offset (ganze Zahl)
      I-P2: Stufe ('Y','M','D')
      I-P3: Ergebnisformat der Datumswerte
    Rueckgabe:
      Tuple of (Start_Date, End_Date) on success, or None on failure
    """
    start_dt = datetime.datetime.now()
    
    try:
        offset_val = int(offset)
    except ValueError:
        anzahl = 0
        print("!! Interner Fehler bei der Rueckgabe von Datumswerten", file=sys.stderr)
        print("   Funktion: DWDate_Gib_Zeitraum", file=sys.stderr)
        print(f"   1 Zeile erwartet, {anzahl} Zeile(n) bekommen", file=sys.stderr)
        return None

    if stufe == 'D':
        end_dt = start_dt + datetime.timedelta(days=offset_val)
    elif stufe == 'M':
        start_dt = start_dt.replace(day=1)
        # Calculate month shifting
        total_months = (start_dt.month - 1) + offset_val
        year_offset = total_months // 12
        new_month = (total_months % 12) + 1
        new_year = start_dt.year + year_offset
        last_day = calendar.monthrange(new_year, new_month)[1]
        end_dt = start_dt.replace(year=new_year, month=new_month, day=last_day)
    elif stufe == 'Y':
        start_dt = start_dt.replace(month=1, day=1)
        end_dt = start_dt.replace(year=start_dt.year + offset_val, month=12, day=31)
    else: 
        anzahl = 0
        print("!! Interner Fehler bei der Rueckgabe von Datumswerten", file=sys.stderr)
        print("   Funktion: DWDate_Gib_Zeitraum", file=sys.stderr)
        print(f"   1 Zeile erwartet, {anzahl} Zeile(n) bekommen", file=sys.stderr)
        return None

    py_fmt = translate_format(format_str)
    return start_dt.strftime(py_fmt), end_dt.strftime(py_fmt)

def LetzterTagDesMonats(datum: str) -> int:
    """
    Funktion:
      LetzterTagDesMonats
    Parameter:
      P1: zu pruefender Datumswert (Format YYYYMMDD)
    Rueckgabe:
      =0, falls Wert P1 der Letzte Tag des Monats ist, sonst 1.
    """
    if len(datum) != 8:
        return 1
    
    try:
        jahr = int(datum[0:4])
        monat = int(datum[4:6])
        tag = int(datum[6:8])
    except ValueError:
        return 1

    if ((jahr % 4 == 0) and (jahr % 100 != 0)) or (jahr % 400 == 0):
        letzter_feb = 29
    else:
        letzter_feb = 28

    letzter_tag = [0, 31, letzter_feb, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    
    try:
        if letzter_tag[monat] == tag:
            return 0
        else:
            return 1
    except IndexError:
        return 1

def TageimMonat(jahr_str: str, monat_str: str) -> int:
    """
    Funktion:
      TageimMonat
    Parameter:
      P1: Jahr (YYYY)
      P2: Monat (MM)
    Rueckgabe:
      gibt die Anzahl der Tage des Monats P2 im Jahr P1 zurueck
    """
    try:
        jahr = int(jahr_str)
        monat = int(monat_str)
    except ValueError:
        return 0

    if ((jahr % 4 == 0) and (jahr % 100 != 0)) or (jahr % 400 == 0):
        letzter_feb = 29
    else:
        letzter_feb = 28

    letzter_tag = [0, 31, letzter_feb, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    
    try:
        return letzter_tag[monat]
    except IndexError:
        return 0

def AddiereDatum(datum: str, tage_str: str) -> str:
    """
    Funktion:
      AddiereDatum
    Parameter:
      P1: Datumswert (Format YYYYMMDD)
      P2: Anzahl der Tage die addiert wird
    Rueckgabe:
      Datum: P1+(n Tage) im Format YYYYMMDD
    """
    try:
        dt = datetime.datetime.strptime(datum, "%Y%m%d")
        tage = int(tage_str)
        result_dt = dt + datetime.timedelta(days=tage)
        return result_dt.strftime("%Y%m%d")
    except ValueError:
        return ""

def main() -> int:
    parser = argparse.ArgumentParser(description="Helper routines for date calculations.")
    subparsers = parser.add_subparsers(dest="command", help="Sub-command to run")
    
    # Sub-command: DWDate_Vormonat
    p_vormonat = subparsers.add_parser("DWDate_Vormonat")
    p_vormonat.add_argument("var_name", help="Name of variables to assign the result (legacy compatibility)")
    p_vormonat.add_argument("format", help="Date format (e.g. YYYYMM)")
    
    # Sub-command: DWDate_Datum_Check
    p_check = subparsers.add_parser("DWDate_Datum_Check")
    p_check.add_argument("wert", help="Date value to check")
    p_check.add_argument("format", help="Date format mask")
    
    # Sub-command: DWDate_Datum_LE
    p_le = subparsers.add_parser("DWDate_Datum_LE")
    p_le.add_argument("datum1", help="Date 1 (YYYYMMDD)")
    p_le.add_argument("datum2", help="Date 2 (YYYYMMDD)")
    
    # Sub-command: DWDate_Gib_Zeitraum
    p_zeitraum = subparsers.add_parser("DWDate_Gib_Zeitraum")
    p_zeitraum.add_argument("offset", help="Offset value")
    p_zeitraum.add_argument("stufe", choices=["Y", "M", "D"], help="Stufe ('Y', 'M', 'D')")
    p_zeitraum.add_argument("format", help="Date format mask")
    p_zeitraum.add_argument("var_start", help="Legacy variable name for start")
    p_zeitraum.add_argument("var_ende", help="Legacy variable name for end")
    
    # Sub-command: LetzterTagDesMonats
    p_letzter = subparsers.add_parser("LetzterTagDesMonats")
    p_letzter.add_argument("datum", help="Date string (YYYYMMDD)")
    
    # Sub-command: TageimMonat
    p_tage = subparsers.add_parser("TageimMonat")
    p_tage.add_argument("jahr", help="Year (YYYY)")
    p_tage.add_argument("monat", help="Month (MM)")
    
    # Sub-command: AddiereDatum
    p_add = subparsers.add_parser("AddiereDatum")
    p_add.add_argument("datum", help="Date string (YYYYMMDD)")
    p_add.add_argument("days", help="Days to add")
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return 1
        
    if args.command == "DWDate_Vormonat":
        result = DWDate_Vormonat(args.format)
        # Print in a format evaluable by legacy shell scripts: value
        print(result)
        return 0
        
    elif args.command == "DWDate_Datum_Check":
        rc = DWDate_Datum_Check(args.wert, args.format)
        return rc
        
    elif args.command == "DWDate_Datum_LE":
        rc = DWDate_Datum_LE(args.datum1, args.datum2)
        return rc
        
    elif args.command == "DWDate_Gib_Zeitraum":
        res = DWDate_Gib_Zeitraum(args.offset, args.stufe, args.format)
        if res:
            # Print in standard environment evaluation layout:
            print(f"{args.var_start}={res[0]}")
            print(f"{args.var_ende}={res[1]}")
            return 0
        return 1
        
    elif args.command == "LetzterTagDesMonats":
        rc = LetzterTagDesMonats(args.datum)
        return rc
        
    elif args.command == "TageimMonat":
        days = TageimMonat(args.jahr, args.monat)
        print(days)
        return 0
        
    elif args.command == "AddiereDatum":
        new_date = AddiereDatum(args.datum, args.days)
        print(new_date)
        return 0
        
    return 0

if __name__ == "__main__":
    sys.exit(main())