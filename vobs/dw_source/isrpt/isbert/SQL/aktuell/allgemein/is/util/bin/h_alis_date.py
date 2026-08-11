#!/usr/bin/env python3
import os
import sys
import argparse
import datetime
import subprocess

# Global dictionary to simulate shell dynamic variable assignment (eval)
globals_dict = {}

try:
    from google.cloud import bigquery
except ImportError:
    bigquery = None

def query_bigquery(query_str, params=None):
    if bigquery is None:
        raise RuntimeError("Google Cloud BigQuery library is not installed.")
    project = os.environ.get("GCP_PROJECT")
    client = bigquery.Client(project=project)
    job_config = bigquery.QueryJobConfig(query_parameters=params)
    query_job = client.query(query_str, job_config=job_config)
    return query_job.result()

def oracle_to_python_format(fmt):
    """
    Converts Oracle DATE format models to Python's strftime/strptime format characters.
    """
    mapping = {
        "YYYY": "%Y",
        "YY": "%y",
        "MM": "%m",
        "DD": "%d",
        "HH24": "%H",
        "HH": "%I",
        "MI": "%M",
        "SS": "%S"
    }
    sorted_keys = sorted(mapping.keys(), key=len, reverse=True)
    for key in sorted_keys:
        fmt = fmt.replace(key, mapping[key])
    return fmt

def oracle_to_bq_format(fmt):
    """
    Converts Oracle DATE format models to BigQuery's parsing format characters.
    """
    return oracle_to_python_format(fmt)

def DWDate_Vormonat(VarName, DWDate_FMT):
    """
    P1 : Namen der Variablen, der das Ergebnis zugewiesen werden soll
    P2 : Formatangabe fuer Oracle to_char/to_date
    """
    # Calculate the previous month relative to current local date
    today = datetime.date.today()
    first_of_this_month = today.replace(day=1)
    prev_month_date = first_of_this_month - datetime.timedelta(days=1)
    
    # Format according to DWDate_FMT (converted from Oracle to Python format)
    py_fmt = oracle_to_python_format(DWDate_FMT)
    result = prev_month_date.strftime(py_fmt)
    
    # Assign to simulate shell global scope/environment variables
    globals_dict[VarName] = result
    os.environ[VarName] = result
    return result

def DWDate_Datum_Check(wert, format_str):
    """
    Funktion: DWDate_Datum_Check
    Parameter:
      P1: zu pruefender Datumswert
      P2: Datumsformat von P1
    Rueckgabe:
      =0, falls Wert P1 ein gueltiges Datum des Formats P1 ist
    """
    if not wert or not format_str:
        return 1

    # First try native parsing in Python
    py_fmt = oracle_to_python_format(format_str)
    try:
        datetime.datetime.strptime(wert, py_fmt)
        return 0
    except ValueError:
        # Fallback to BigQuery evaluation if native strptime is insufficient
        if bigquery is not None:
            bq_fmt = oracle_to_bq_format(format_str)
            query = "SELECT SAFE.PARSE_DATE(@bq_fmt, @wert) as check_date"
            params = [ 
                bigquery.ScalarQueryParameter("bq_fmt", "STRING", bq_fmt),
                bigquery.ScalarQueryParameter("wert", "STRING", wert)
            ]
            try:
                results = query_bigquery(query, params)
                for row in results:
                    if row.check_date is not None:
                        return 0
                return 1
            except Exception as e:
                print(f"ERROR: BigQuery format check failed: {e}", file=sys.stderr)
                return 1
        else:
            return 1

def DWDate_Datum_LE(datum1, datum2):
    """
    Funktion: DWDate_Datum_LE
    Parameter:
      P1: Datum1 im Format YYYYMMDD
      P2: Datum2 im Format YYYYMMDD
    Rueckgabe:
      =0, falls P1<=P2 ist
    """
    if not datum1 or not datum2:
        return 1
        
    try:
        d1 = datetime.datetime.strptime(datum1, "%Y%m%d")
        d2 = datetime.datetime.strptime(datum2, "%Y%m%d")
        
        if d1 > d2:
            # -20422 ist Fehlernr fuer "Parameter fehlerhaft"
            # Output exactly matching the legacy PL/SQL error string in German
            print(f"ERROR: Datum {datum1} ist groesser als {datum2}", file=sys.stderr)
            return 1
        return 0
    except ValueError as e:
        print(f"ERROR: DWDate_Datum_LE invalid parameters: {e}", file=sys.stderr)
        return 1

def DWDate_Gib_Zeitraum(Offset, Stufe, Format, Var_Start, Var_Ende):
    """
    Funktion: DWDate_Gib_Zeitraum
    Parameter:
      I-P1: Offset (ganze Zahl)
      I-P2: Stufe ('Y','M','D')
      I-P3: Ergebnisformat der Datumswerte
      O-P4: Variablenname fuer Startpunktes (=Sysdate)
      O-P5: Variablenname fuer Endepunkt (Start+Offset)
    """
    try:
        offset = int(Offset)
    except ValueError:
        return 1

    stufe = Stufe.upper()
    today = datetime.date.today()

    if stufe == 'D':
        # Days basis
        start_date = today
        end_date = start_date + datetime.timedelta(days=offset)
    elif stufe == 'M':
        # Months basis: start is always first of current month
        start_date = today.replace(day=1)
        
        # End is Ultimo of target month
        target_year = today.year
        target_month = today.month + offset
        
        while target_month > 12:
            target_month -= 12
            target_year += 1
        while target_month < 1:
            target_month += 12
            target_year -= 1
            
        last_day = TageimMonat(target_year, target_month)
        end_date = datetime.date(target_year, target_month, last_day)
    elif stufe == 'Y':
        # Years basis: start is always New Year of current year
        start_date = today.replace(month=1, day=1)
        
        # End is Sylvester of target year
        target_year = today.year + offset
        end_date = datetime.date(target_year, 12, 31)
    else: 
        return 1

    py_fmt = oracle_to_python_format(Format)
    start_str = start_date.strftime(py_fmt)
    end_str = end_date.strftime(py_fmt)

    globals_dict[Var_Start] = start_str
    globals_dict[Var_Ende] = end_str
    os.environ[Var_Start] = start_str
    os.environ[Var_Ende] = end_str
    return 0

def LetzterTagDesMonats(datum):
    """
    Funktion: LetzterTagDesMonat
    Parameter:
      P1: zu pruefender Datumswert (Format YYYYMMDD)
    Rueckgabe:
      =0, falls Wert P1 der Letzte Tag des Monats ist
    """
    try:
        jahr = int(datum[0:4])
        monat = int(datum[4:6])
        tag = int(datum[6:8])
        if TageimMonat(jahr, monat) == tag:
            return 0
        else:
            return 1
    except Exception:
        return 1

def TageimMonat(jahr, monat):
    """
    Funktion: TageimMonat
    Parameter:
      P1: Jahr (YYYY)
      P2: Monat (MM)
    Rueckgabe:
      gibt die Anzahl der Tage des Monats P2 im Jahr P1 zurueck
    """
    try:
        y = int(jahr)
        m = int(monat)
        if (y % 4 == 0 and y % 100 != 0) or (y % 400 == 0):
            letzter_feb = 29
        else:
            letzter_feb = 28
        letzter_tag = [0, 31, letzter_feb, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
        return letzter_tag[m]
    except Exception:
        return 0

def AddiereDatum(datum, tage):
    """
    Funktion: AddiereDatum
    Parameter:
      P1: Datumswert (Format YYYYMMDD)
      P2: Anzahl der Tage die addiert wird
    Rueckgabe:
      Datum: P1+(n Tage)
    """
    try:
        dt = datetime.datetime.strptime(datum, "%Y%m%d")
        res_dt = dt + datetime.timedelta(days=int(tage))
        res = res_dt.strftime("%Y%m%d")
        print(res)
        return res
    except Exception as e:
        print(f"ERROR: AddiereDatum failed: {e}", file=sys.stderr)
        return ""

def main():
    parser = argparse.ArgumentParser(description="Python conversion of h_alis_date.ksh")
    subparsers = parser.add_subparsers(dest="command")
    
    # Subparser for DWDate_Vormonat
    parser_vormonat = subparsers.add_parser("DWDate_Vormonat")
    parser_vormonat.add_argument("var_name")
    parser_vormonat.add_argument("format")
    
    # Subparser for DWDate_Datum_Check
    parser_check = subparsers.add_parser("DWDate_Datum_Check")
    parser_check.add_argument("wert")
    parser_check.add_argument("format")
    
    # Subparser for DWDate_Datum_LE
    parser_le = subparsers.add_parser("DWDate_Datum_LE")
    parser_le.add_argument("datum1")
    parser_le.add_argument("datum2")
    
    # Subparser for DWDate_Gib_Zeitraum
    parser_zeitraum = subparsers.add_parser("DWDate_Gib_Zeitraum")
    parser_zeitraum.add_argument("offset", type=int)
    parser_zeitraum.add_argument("stufe")
    parser_zeitraum.add_argument("format")
    parser_zeitraum.add_argument("var_start")
    parser_zeitraum.add_argument("var_ende")
    
    # Subparser for LetzterTagDesMonats
    parser_ultimo = subparsers.add_parser("LetzterTagDesMonats")
    parser_ultimo.add_argument("datum")
    
    # Subparser for TageimMonat
    parser_tage = subparsers.add_parser("TageimMonat")
    parser_tage.add_argument("jahr")
    parser_tage.add_argument("monat")
    
    # Subparser for AddiereDatum
    parser_add = subparsers.add_parser("AddiereDatum")
    parser_add.add_argument("datum")
    parser_add.add_argument("tage", type=int)
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return 1
    
    if args.command == "DWDate_Vormonat":
        res = DWDate_Vormonat(args.var_name, args.format)
        print(res)
        return 0
    elif args.command == "DWDate_Datum_Check":
        return DWDate_Datum_Check(args.wert, args.format)
    elif args.command == "DWDate_Datum_LE":
        return DWDate_Datum_LE(args.datum1, args.datum2)
    elif args.command == "DWDate_Gib_Zeitraum":
        ret = DWDate_Gib_Zeitraum(args.offset, args.stufe, args.format, args.var_start, args.var_ende)
        if ret == 0:
            print(f"{args.var_start}={globals_dict.get(args.var_start)}")
            print(f"{args.var_ende}={globals_dict.get(args.var_ende)}")
        return ret
    elif args.command == "LetzterTagDesMonats":
        ret = LetzterTagDesMonats(args.datum)
        print(ret)
        return ret
    elif args.command == "TageimMonat":
        res = TageimMonat(args.jahr, args.monat)
        print(res)
        return 0
    elif args.command == "AddiereDatum":
        res = AddiereDatum(args.datum, args.tage)
        return 0
    
    return 0

if __name__ == "__main__":
    sys.exit(main())