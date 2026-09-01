#!/usr/bin/env python3
import sys
import os
import argparse
import logging
from google.cloud import bigquery

# Sourced environment variables
GCP_PROJECT = os.environ.get("GCP_PROJECT")
DW_DIR_ROOT = os.environ.get("DW_DIR_ROOT")
HOME = os.environ.get("HOME")

def usage():
    print("""
   Programm: Ausführung Script {}
   Version: 5.0.0
   Aufruf: {} Parameter

   Das als Parameter -f  übergebene SQL-Script wird ausgeführt.
   Es muß die Zeile "whenever sqlerror exit failure" enthalten,
   damit das Rahmenscript bei Fehlern abbricht.
   Der mit dem Parameter -i übergebene String wird an das SQL-Script
   weitergereicht
   Wenn das SQL-Script keinen Pfad hat, wird es  erst in  ../sql
   parallel zum Ablageverzeichnis dieses Rahmenscripts vermutet,
   dan in ../mig,
   dann direkt im Ablageverzeichnis dieses Rahmenscripts.
   Dies Rahmenscript muß deswegen immer mit Komplettpfad aufgerufen werden
   oder direkt aus  dem  Verzeichnis, in dem es gespeichert ist.


   Parameter:
       -f     hier wird der Name des SQL-Scripts angegeben
       -i     mögliche Parameter für das SQL-Script 

       -j     Jobkennung (default DWH_KORR)

       -h     zeigt diese Seite an

       -v     verbose (zeigt bei Fehler sofort die Logdatei an)
""".format(sys.argv[0], sys.argv[0]))

def execute_bigquery_sql(sql_file_path, dw_eintrags_nr, p_sqlpar):
    """
    Executes the SQL statements from the resolved SQL file on BigQuery.
    Replaces positional parameters (e.g., &1, &2, etc.) with the passed arguments.
    """
    if not os.path.isfile(sql_file_path):
        raise FileNotFoundError(f"SQL file not found: {sql_file_path}")

    with open(sql_file_path, 'r', encoding='utf-8') as f:
        query_content = f.read()

    # Prepare parameters for substitution
    # Parameter 1: DW_EintragsNr
    # Parameter 2..N: p_sqlpar (split by space if multiple)
    # Last Parameter: DW_EintragsNr
    params = [str(dw_eintrags_nr)]
    if p_sqlpar:
        params.extend(p_sqlpar.split())
    params.append(str(dw_eintrags_nr))

    # Perform simple positional parameter substitution (e.g., &1, &2, &3...)
    # Also handle Oracle-style &variable or &1.
    for idx, val in enumerate(params, start=1):
        query_content = query_content.replace(f"&{idx}.", val)
        query_content = query_content.replace(f"&{idx}", val)

    # Initialize BigQuery client
    client = bigquery.Client(project=GCP_PROJECT)
    
    # Split queries by semicolon if there are multiple statements
    # (Simple split, ignoring semicolons inside strings/comments for basic compatibility)
    queries = [q.strip() for q in query_content.split(';') if q.strip()]
    
    for query in queries:
        if not query:
            continue
        logging.info(f"Executing query:\n{query}")
        query_job = client.query(query)
        query_job.result()  # Wait for query to complete

def main():
    err_nr = 0
    err_arg = ""
    p_sqlscript = None
    p_sqlpar = None
    p_Job = None
    p_Verbose = False

    # Standard command-line argument parsing mirroring getopts ":hv$ParamList" (ParamList="f:j:i:")
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("-f", dest="p_sqlscript")
    parser.add_argument("-i", dest="p_sqlpar")
    parser.add_argument("-j", dest="p_Job")
    parser.add_argument("-v", action="store_true", dest="p_Verbose")
    parser.add_argument("-h", action="store_true", dest="p_Help")

    try:
        args, unknown = parser.parse_known_args()
        if unknown:
            err_nr = 192  # Parameter unbekannt
            err_arg = unknown[0]
        else:
            if args.p_Help:
                usage()
                sys.exit(0)
            
            p_sqlscript = args.p_sqlscript
            p_sqlpar = args.p_sqlpar
            p_Job = args.p_Job
            p_Verbose = args.p_Verbose
    except Exception as e:
        err_nr = 192
        err_arg = str(e)

    # Falls Fehler aufgetreten, abbrechen
    if err_nr != 0:
        usage()
        sys.exit(err_nr)

    if not p_sqlscript:
        err_nr = 193  # Notwendiges Argument fehlt
        err_arg = "-f"
        usage()
        sys.exit(err_nr)

    # typeset -l p_sqlscript
    p_sqlscript = p_sqlscript.lower()

    # cd `dirname $0`
    try:
        script_dir = os.path.dirname(os.path.abspath(sys.argv[0]))
        os.chdir(script_dir)
    except Exception:
        pass

    # Dynamic path resolution for l_DBskript
    p_sqlscript_dir = os.path.dirname(p_sqlscript)
    if p_sqlscript_dir in ("", "."):
        l_DBskript = os.path.join("..", "sql", p_sqlscript)
        if not os.path.isfile(l_DBskript):
            l_DBskript = os.path.join("..", "mig", p_sqlscript)
        if not os.path.isfile(l_DBskript):
            l_DBskript = p_sqlscript
    else:
        l_DBskript = p_sqlscript

    # Corrected legacy file check logic (error if file does NOT exist)
    if not os.path.isfile(l_DBskript):
        err_nr = 198  # Parameterwert unbekannt / Datei nicht gefunden
        err_arg = l_DBskript
        logging.error(f"Error {err_nr}: SQL script file not found: {err_arg}")
        sys.exit(err_nr)

    # typeset -u JobKennung
    if not p_Job:
        job_kennung = "DWH_KORR"
    else:
        job_kennung = p_Job.upper()

    print("----------------- Parameter -----------------")
    print(f"Jobkennung     : {job_kennung}")
    print(f"DB-Skript      : {l_DBskript}")
    print("---------------------------------------------")

    # Generate tracking identifier and log file destination
    dw_eintrags_nr = os.environ.get("DW_EintragsNr", "0")
    log_datei = f"{job_kennung}_{dw_eintrags_nr}.log"

    # Configure logging
    logging.basicConfig(
        filename=log_datei,
        filemode='a',
        format='%(asctime)s - %(levelname)s - %(message)s',
        level=logging.INFO
    )
    logging.info(f"ErzeugeEintrag: {dw_eintrags_nr}, {job_kennung}, {sys.argv[0]}_{l_DBskript}, {log_datei}")

    try:
        print("----------------- Job -----------------------")
        print(f"Job-Nr    : '{dw_eintrags_nr}'")
        print(f"Logdatei  : '{log_datei}'")
        print("---------------------------------------------")

        # Execute the SQL script on BigQuery
        execute_bigquery_sql(l_DBskript, dw_eintrags_nr, p_sqlpar)

        # DWMSG_SetzeStatusOK
        logging.info(f"SetzeStatusOK: {dw_eintrags_nr}")
        print("Die Abarbeitung des Rahmenskriptes ist ohne erkennbare Fehler beendet")

    except KeyboardInterrupt:
        error_msg = "!OSFEHLER gemeldet!"
        print(error_msg, file=sys.stderr)
        logging.error(error_msg)
        logging.error(f"Fehlerbehandlung for: {dw_eintrags_nr}")
        
        if p_Verbose:
            try:
                with open(log_datei, 'r') as lf:
                    print(lf.read(), file=sys.stderr)
            except Exception as log_err:
                print(f"Failed to read log file: {log_err}", file=sys.stderr)
                
        sys.exit(1)

    except Exception as e:
        error_msg = "!FEHLER gemeldet!"
        print(error_msg, file=sys.stderr)
        logging.error(f"Unexpected execution failure: {str(e)}")
        logging.error(f"Fehlerbehandlung for: {dw_eintrags_nr}")
        
        if p_Verbose:
            try:
                with open(log_datei, 'r') as lf:
                    print(lf.read(), file=sys.stderr)
            except Exception as log_err:
                print(f"Failed to read log file: {log_err}", file=sys.stderr)
                
        sys.exit(1)

if __name__ == "__main__":
    main()