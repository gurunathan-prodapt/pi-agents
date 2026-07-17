#!/usr/bin/env python3
"""
Script: r_abgl_kunde_woech.py
Zweck: Woechentlicher Adressabgleich der Kundenstammdaten (KUNDE)
       gegen das Referenzsystem STAMMDATEN
"""

import sys
import os
import argparse
from datetime import datetime, timedelta

try:
    from DWH.DWH_KERN.PRODUKTION.DW.DWH_KUNDE.bin.utils import f_alis_msgerr, execute_bigquery_query
except ImportError:
    from utils import f_alis_msgerr, execute_bigquery_query

ProgName = f"Ausfuehrung Script {sys.argv[0]}"
ProgVersion = "1.1.0"


def usage() -> None:
    """Prints legacy usage documentation."""
    print(f"""   Programm: {ProgName}
   Zweck: Woechentlicher Adressabgleich der Kundenstammdaten (KUNDE)
          gegen das Referenzsystem STAMMDATEN
   Parameter:
       -s     Stichtag (Format: 'YYYYMMDD')""")


def perform_reconciliation(stichtag: str, sql_file_path: str, log_file_path: str) -> int:
    """
    Reads the BigQuery SQL script, runs reconciliation, and returns the deviation count.
    """
    if not os.path.exists(sql_file_path):
        raise FileNotFoundError(f"SQL file not found at path: {sql_file_path}")

    with open(sql_file_path, "r", encoding="utf-8") as sf:
        sql_text = sf.read()

    parameters = {
        "stichtag": {"type": "STRING", "value": stichtag}
    }

    results = execute_bigquery_query(sql_text, parameters)
    
    deviations_count = 0
    with open(log_file_path, "a", encoding="utf-8") as log_file:
        for row in results:
            status_str = str(row.reconciliation_status)
            log_file.write(f"{status_str} for ID {row.kunden_id}\n")
            if status_str.startswith("ABWEICHUNG"):
                deviations_count += 1
                
    return deviations_count


def main() -> None:
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument('-s', dest='stichtag', default=None)
    parser.add_argument('-h', '--help', action='store_true')
    
    args, _ = parser.parse_known_args()
    
    if args.help:
        usage()
        sys.exit(0)

    l_Stichtag = args.stichtag
    if not l_Stichtag:
        l_Stichtag = (datetime.now() - timedelta(days=7)).strftime('%Y%m%d')

    dw_dir_log = os.environ.get("DW_DIR_LOG", "/tmp/aktuell/log")
    os.makedirs(f"{dw_dir_log}/kunde", exist_ok=True)
    
    pid = os.getpid()
    protokoll_datei = f"{dw_dir_log}/kunde/abgl_kunde_woech_{pid}.log"

    start_msg = f"Starte Adressabgleich Kundenstammdaten fuer Stichtag {l_Stichtag}"
    print(start_msg)
    with open(protokoll_datei, "w", encoding="utf-8") as log_file:
        log_file.write(start_msg + "\n")

    sql_path = os.environ.get(
        "SQL_FILE_PATH", 
        "DWH/DWH_KERN/PRODUKTION/DW.DWH_KUNDE/sql/d_abgl_kunde_woech.sql"
    )

    try:
        l_Abweichungen = perform_reconciliation(l_Stichtag, sql_path, protokoll_datei)
    except Exception as e:
        sys.stderr.write(f"Database execution error: {str(e)}\n")
        sys.exit(1)

    count_msg = f"Anzahl gefundener Abweichungen: {l_Abweichungen}"
    print(count_msg)
    with open(protokoll_datei, "a", encoding="utf-8") as log_file:
        log_file.write(count_msg + "\n")

    if l_Abweichungen > 0:
        f_alis_msgerr("W", f"{l_Abweichungen} Abweichungen im Kundenadressabgleich gefunden, siehe {protokoll_datei}")

    print("Adressabgleich Kundenstammdaten ohne erkennbare Fehler beendet")


if __name__ == "__main__":
    main()