#!/usr/bin/env python3
#-----------------------------------------------------------------------------
#- Ersterstellung am:                           2020-03-09T09:00:00+02:00
#- Ersterstellung durch:                        mschaefer
#- Aenderung:                                   2022-11-20  khoffmann - Zusaetzliches Logging
#-----------------------------------------------------------------------------
import argparse
import os
import sys
from datetime import date, timedelta, datetime
from pathlib import Path
from google.cloud import bigquery

ProgName = None
ProgVersion = "1.1.0"


def usage():
    print(
        f"""   Programm: {ProgName}
   Zweck: Woechentlicher Adressabgleich der Kundenstammdaten (KUNDE)
          gegen das Referenzsystem STAMMDATEN
   Parameter:
       -s     Stichtag (Format: 'YYYYMMDD')"""
    )


def f_alis_msgerr(l_Level, l_Text):
    print(f"[{l_Level}] {datetime.now():%Y-%m-%d %H:%M:%S} {l_Text}", file=sys.stderr)


def run_bigquery_sql(sql_file_path, stichtag, log_file_path):
    project_id = os.environ.get("GCP_PROJECT")
    dataset_id = os.environ.get("BQ_DATASET")
    location = os.environ.get("BQ_LOCATION")

    if not project_id:
        raise ValueError("Environment variable GCP_PROJECT is not set.")

    client = bigquery.Client(project=project_id, location=location)
    
    with open(sql_file_path, "r", encoding="utf-8") as f:
        sql_content = f.read()
    
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("p_Stichtag", "STRING", stichtag)
        ]
    )
    
    query_job = client.query(sql_content, job_config=job_config)
    results = query_job.result()
    
    with open(log_file_path, "a", encoding="utf-8") as log_file:
        for row in results:
            row_str = " ".join(str(val) for val in row.values())
            log_file.write(row_str + "\n")


def main():
    global ProgName
    ProgName = f"Ausfuehrung Script {sys.argv[0]}"

    DW_DIR_ROOT = os.environ.get(
        "DW_DIR_ROOT",
        os.path.join(os.environ.get("HOME", "/tmp"), "aktuell", "dw_source", "isdwh")
    )
    DW_DIR_LOG = os.environ.get(
        "DW_DIR_LOG",
        os.path.join(os.environ.get("HOME", "/tmp"), "aktuell", "log")
    )

    l_Stichtag = None
    Protokoll_Datei = os.path.join(DW_DIR_LOG, "kunde", f"abgl_kunde_woech_{os.getpid()}.log")

    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("-s", dest="stichtag")
    parser.add_argument("-h", action="store_true")
    args = parser.parse_args()

    if args.h:
        usage()
        return 0

    l_Stichtag = args.stichtag

    if not l_Stichtag:
        l_Stichtag = (date.today() - timedelta(days=7)).strftime("%Y%m%d")

    sql_dir = os.path.join(DW_DIR_ROOT, "exporter", "kunde", "sql")
    if os.path.exists(sql_dir):
        os.chdir(sql_dir)
    else:
        print(f"Warning: Directory {sql_dir} does not exist. Using current working directory.")

    Path(os.path.dirname(Protokoll_Datei)).mkdir(parents=True, exist_ok=True)

    start_msg = f"Starte Adressabgleich Kundenstammdaten fuer Stichtag {l_Stichtag}"
    print(start_msg)
    with open(Protokoll_Datei, "w", encoding="utf-8") as log_file:
        log_file.write(start_msg + "\n")

    sql_file_name = "d_abgl_kunde_woech.sql"
    sql_file_path = sql_file_name if os.path.exists(sql_file_name) else os.path.join(sql_dir, sql_file_name)

    try:
        if not os.path.exists(sql_file_path):
            raise FileNotFoundError(f"SQL file {sql_file_path} not found.")
        
        run_bigquery_sql(sql_file_path, l_Stichtag, Protokoll_Datei)
    except Exception as e:
        print(f"ERROR: SQL execution failed: {str(e)}", file=sys.stderr)
        return 1

    with open(Protokoll_Datei, encoding="utf-8") as log_file:
        l_Abweichungen = sum(1 for line in log_file if line.startswith("ABWEICHUNG"))

    with open(Protokoll_Datei, "a", encoding="utf-8") as log_file:
        log_file.write(f"Anzahl gefundener Abweichungen: {l_Abweichungen}\n")

    if l_Abweichungen > 0:
        f_alis_msgerr(
            "W",
            f"{l_Abweichungen} Abweichungen im Kundenadressabgleich gefunden, siehe {Protokoll_Datei}",
        )

    print("Adressabgleich Kundenstammdaten ohne erkennbare Fehler beendet")
    return 0


if __name__ == "__main__":
    sys.exit(main())