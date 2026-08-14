#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# 
# Zweck:
#   Hilfsroutinen fuer das Rechnen mit Datumswerten
#   Momentan basiert es auf reinem Python (keine SQLPLUS/SQL-Abhaengigkeiten mehr)
# Vorbedingung:
#   Keine, da native Python-Datumsarithmetik verwendet wird.
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
#    4.0.0; 2026; Migriert nach Python für GCP/Composer-Umgebung

import os
import sys
import datetime
import re
import argparse

def translate_oracle_format(fmt: str) -> str:
    """
    Translates basic Oracle TO_CHAR/TO_DATE formats to Python's strftime formats.
    """
    replacements = [
        ("YYYY", "%Y"),
        ("YY", "%y"),
        ("MM", "%m"),
        ("DD", "%d"),
        ("HH24", "%H"),
        ("HH", "%I"),
        ("MI", "%M"),
        ("SS", "%S"),
    ]
    res = fmt
    for or_pat, py_pat in replacements:
        res = re.sub(re.escape(or_pat), py_pat, res, flags=re.IGNORECASE)
    return res

def DWDate_Vormonat(fmt: str) -> str:
    """
    Berechnung des Vormonats relativ zum aktuellen Datum.
    Formatangabe fuer Oracle to_char/to_date wird in Python-Format uebersetzt.
    """
    today = datetime.date.today()
    first_of_this_month = today.replace(day=1)
    prev_month = first_of_this_month - datetime.timedelta(days=1)
    py_fmt = translate_oracle_format(fmt)
    return prev_month.strftime(py_fmt)

def DWDate_Datum_Check(wert: str, format_val: str) -> bool:
    """
    Funktion: DWDate_Datum_Check
    Parameter:
      P1: zu pruefender Datumswert
      P2: Datumsformat von P1
    Rueckgabe:
      True, falls Wert P1 ein gueltiges Datum des Formats P2 ist, sonst False
    """
    if not wert or not format_val:
        return False
    py_fmt = translate_oracle_format(format_val)
    try:
        datetime.datetime.strptime(wert, py_fmt)
        return True
    except ValueError:
        return False

def DWDate_Datum_LE(datum1: str, datum2: str) -> bool:
    """
    Funktion: DWDate_Datum_LE
    Parameter:
      P1: Datum1 im Format YYYYMMDD
      P2: Datum2 im Format YYYYMMDD
    Rueckgabe:
      True, falls P1<=P2 ist
    Annahmen:
      P1,P2 sind gueltige Datumswerte
    """
    if not datum1 or not datum2:
        raise ValueError("Exactly two parameters required: datum1, datum2")
    
    try:
        d1 = datetime.datetime.strptime(datum1, "%Y%m%d")
        d2 = datetime.datetime.strptime(datum2, "%Y%m%d")
    except ValueError as e:
        raise ValueError(f"Invalid date format. Expected YYYYMMDD: {e}")
        
    if d1 > d2:
        # German error message preserved exactly as per instructions
        raise ValueError(f"Datum {datum1} ist groesser als {datum2}")
    return True

def DWDate_Gib_Zeitraum(offset: int, stufe: str, format_val: str) -> tuple:
    """
    Funktion: DWDate_Gib_Zeitraum
    Parameter:
      I-P1: Offset (ganze Zahl)
      I-P2: Stufe ('Y','M','D')
      I-P3: Ergebnisformat der Datumswerte
    Rueckgabe:
      (start, ende) als formatiertes Datum
    Beschreibung:
      Als Startpunkt wird Sysdate genutzt. Je nach Stufe werden
      eine bestimmte Anzahl (Offset) von Tagen, Monaten oder Jahren
      dem Systemdatum hinzugezaehlt.
      Bei Monaten ist der Anfang immer Monatserste und das Ende immer 
      der Ultimo des entsprechenden Monats
      Bei Jahren ist der Anfang immer Neujahr und das Ende immer Sylvester
      des entsprechenden Jahres
    """
    start_date = datetime.date.today()
    stufe = stufe.upper()
    
    if stufe == 'D':
        end_date = start_date + datetime.timedelta(days=offset)
    elif stufe == 'M':
        start_date = start_date.replace(day=1)
        year = start_date.year
        month = start_date.month + offset
        
        while month > 12:
            month -= 12
            year += 1
        while month < 1:
            month += 12
            year -= 1
            
        import calendar
        last_day = calendar.monthrange(year, month)[1]
        end_date = datetime.date(year, month, last_day)
    elif stufe == 'Y':
        start_date = start_date.replace(month=1, day=1)
        end_date = datetime.date(start_date.year + offset, 12, 31)
    else:
        raise ValueError(f"Invalid offset unit: {stufe}. Must be 'Y', 'M', or 'D'.")
        
    py_fmt = translate_oracle_format(format_val)
    return start_date.strftime(py_fmt), end_date.strftime(py_fmt)

def LetzterTagDesMonats(datum_str: str) -> int:
    """
    Funktion: LetzterTagDesMonat
    Parameter:
      P1: zu pruefender Datumswert (Format YYYYMMDD)
    Rueckgabe:
      =0, falls Wert P1 der Letzte Tag des Monats ist, sonst 1
    """
    try:
        jahr = int(datum_str[0:4])
        monat = int(datum_str[4:6])
        tag = int(datum_str[6:8])
    except (ValueError, IndexError):
        return 1
        
    if ((jahr % 4 == 0) and (jahr % 100 != 0)) or (jahr % 400 == 0):
        letzter_feb = 29
    else:
        letzter_feb = 28
        
    letzter_tag_array = [0, 31, letzter_feb, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    
    try:
        if letzter_tag_array[monat] == tag:
            return 0
        else:
            return 1
    except IndexError:
        return 1

def TageimMonat(jahr_str: str, monat_str: str) -> int:
    """
    Funktion TageimMonat
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
        
    letzter_tag_array = [0, 31, letzter_feb, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    try:
        return letzter_tag_array[monat]
    except IndexError:
        return 0

def AddiereDatum(datum_str: str, tage_to_add: int) -> str:
    """
    Funktion: AddiereDatum
    Parameter:
      P1: Datumswert (Format YYYYMMDD)
      P2: Anzahl der Tage die addiert wird
    Rueckgabe:
      Datum: P1+(n Tage) im Format YYYYMMDD
    """
    try:
        jahr = int(datum_str[0:4])
        monat = int(datum_str[4:6])
        tag = int(datum_str[6:8])
    except (ValueError, IndexError):
        raise ValueError(f"Invalid date format: {datum_str}")
        
    tag += tage_to_add
    
    while tag > TageimMonat(str(jahr), str(monat)):
        tag -= TageimMonat(str(jahr), str(monat))
        monat += 1
        
        while monat > 12:
            monat -= 12
            jahr += 1
            
    tag_padded = f"{tag:02d}"
    monat_padded = f"{monat:02d}"
    jahr_padded = f"{jahr:04d}"
    
    return f"{jahr_padded}{monat_padded}{tag_padded}"

def main():
    parser = argparse.ArgumentParser(description="Python-Portierung der h_alis_date.ksh Hilfsfunktionen")
    subparsers = parser.add_subparsers(dest="command", help="Funktionsbefehl")
    
    # DWDate_Vormonat Subparser
    parser_vormonat = subparsers.add_parser("DWDate_Vormonat")
    parser_vormonat.add_argument("var_name", nargs="?", default=None, help="Name der Variable (fuer Abwaertskompatibilitaet)")
    parser_vormonat.add_argument("fmt", help="Datumsformat")
    
    # DWDate_Datum_Check Subparser
    parser_check = subparsers.add_parser("DWDate_Datum_Check")
    parser_check.add_argument("wert", help="Zu pruefendes Datum")
    parser_check.add_argument("format", help="Datumsformat")
    
    # DWDate_Datum_LE Subparser
    parser_le = subparsers.add_parser("DWDate_Datum_LE")
    parser_le.add_argument("datum1", help="Datum 1 (YYYYMMDD)")
    parser_le.add_argument("datum2", help="Datum 2 (YYYYMMDD)")
    
    # DWDate_Gib_Zeitraum Subparser
    parser_zeitraum = subparsers.add_parser("DWDate_Gib_Zeitraum")
    parser_zeitraum.add_argument("offset", type=int, help="Offset")
    parser_zeitraum.add_argument("stufe", help="Stufe ('Y','M','D')")
    parser_zeitraum.add_argument("format", help="Datumsformat")
    parser_zeitraum.add_argument("var_start", nargs="?", default=None, help="Startvariable")
    parser_zeitraum.add_argument("var_ende", nargs="?", default=None, help="Endevariable")
    
    # LetzterTagDesMonats Subparser
    parser_letzter = subparsers.add_parser("LetzterTagDesMonats")
    parser_letzter.add_argument("datum", help="Datum im Format YYYYMMDD")
    
    # TageimMonat Subparser
    parser_tage = subparsers.add_parser("TageimMonat")
    parser_tage.add_argument("jahr", help="Jahr")
    parser_tage.add_argument("monat", help="Monat")
    
    # AddiereDatum Subparser
    parser_add = subparsers.add_parser("AddiereDatum")
    parser_add.add_argument("datum", help="Datum im Format YYYYMMDD")
    parser_add.add_argument("tage", type=int, help="Tage zum Addieren")
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return 1
        
    if args.command == "DWDate_Vormonat":
        res = DWDate_Vormonat(args.fmt)
        print(res)
        return 0
        
    elif args.command == "DWDate_Datum_Check":
        valid = DWDate_Datum_Check(args.wert, args.format)
        if valid:
            return 0
        else:
            return 1
            
    elif args.command == "DWDate_Datum_LE":
        try:
            valid = DWDate_Datum_LE(args.datum1, args.datum2)
            if valid:
                return 0
            else:
                return 1
        except ValueError as e:
            print(str(e), file=sys.stderr)
            return 1
            
    elif args.command == "DWDate_Gib_Zeitraum":
        try:
            start, ende = DWDate_Gib_Zeitraum(args.offset, args.stufe, args.format)
            print(f"DWH_Ergebnis;{start};{ende};")
            return 0
        except Exception as e:
            print("!! Interner Fehler bei der Rueckgabe von Datumswerten", file=sys.stderr)
            print("   Funktion: DWDate_Gib_Zeitraum", file=sys.stderr)
            # RESTORED LITERAL ERROR MESSAGE EXACTLY
            print("   1 Zeile erwartet, 0 Zeile(n) bekommen", file=sys.stderr)
            return 1
            
    elif args.command == "LetzterTagDesMonats":
        ret = LetzterTagDesMonats(args.datum)
        print(ret)
        return ret
        
    elif args.command == "TageimMonat":
        days = TageimMonat(args.jahr, args.monat)
        print(days)
        return 0
        
    elif args.command == "AddiereDatum":
        try:
            res = AddiereDatum(args.datum, args.tage)
            print(res)
            return 0
        except ValueError as e:
            print(str(e), file=sys.stderr)
            return 1
            
    return 0

if __name__ == "__main__":
    sys.exit(main())