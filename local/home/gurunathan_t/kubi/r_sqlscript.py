#!/usr/bin/env python3
import sys
import os
import argparse
import logging
from pathlib import Path
from google.cloud import bigquery
from google.api_core.exceptions import GoogleAPIError

# REVIEW-STRUCT: environment file .dw_init not supplied — variables it sets are unknown; do not guess their names or values
# REVIEW-STRUCT: environment file f_alis_msgerr.ksh not supplied — variables/functions it sets are unknown; do not guess their names or values
# REVIEW-STRUCT: environment file h_alis_sqlplus.ksh not supplied — defines starteSQLSkript; behaviour unknown

# Ensure mandatory environment parameters are set
gcp_project = os.environ.get("GCP_PROJECT")
dw_dir_root = os.environ.get("DW_DIR_ROOT")
home = os.environ.get("HOME")

# Configure native logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)

# REVIEW-STRUCT: legacy Oracle status-logging package DWMSG replaced with native logging — confirm target logging destination (Cloud Logging / BigQuery table) before deploying
def DWMSG_MeldeFehler(eintrags_nr, severity, err_nr, err_arg):
    logging.error(f"Error registered: Severity={severity}, ErrNr={err_nr}, Arg={err_arg}")

def DW_MSG_MeldeFehler(eintrags_nr, severity, err_nr, err_arg):
    DWMSG_MeldeFehler(eintrags_nr, severity, err_nr, err_arg)

def DWMSG_ErmittleNr():
    import time
    return int(time.time() * 1000) % 1000000

def DWMSG_Logdateiname(job_kennung, eintrags_nr):
    return f"/tmp/log_{job_kennung}_{eintrags_nr}.log"

def DWMSG_ErzeugeEintrag(eintrags_nr, job_kennung, script_run, log_file):
    logging.info(f"Log entry created: {job_kennung} - {script_run} -> {log_file}")

def DWMSG_Fehlerbehandlung(eintrags_nr, log_file, verbose):
    logging.error(f"Running error recovery for ID {eintrags_nr}")
    if verbose == 1:
        if os.path.exists(log_file):
            try:
                with open(log_file, 'r', encoding='utf-8') as f:
                    logging.error(f"Log content from {log_file}:\n{f.read()}")
            except Exception as e:
                logging.error(f"Could not read log file: {e}")

def DW_MSG_Fehlerbehandlung(eintrags_nr, log_file, verbose):
    DWMSG_Fehlerbehandlung(eintrags_nr, log_file, verbose)

def DWMSG_SetzeStatusOK(eintrags_nr):
    logging.info(f"Status set to OK for run {eintrags_nr}")

def usage():
    print("""   Programm: Ausführung Script r_sqlscript
   Version: 5.0.0
   Aufruf: r_sqlscript Parameter

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

       -v     verbose (zeigt bei Fehler sofort die Logdatei an)""")

def main():
    # Step 2: Parse command-line parameters
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("-f", dest="p_sqlscript")
    parser.add_argument("-i", dest="p_sqlpar", default="")
    parser.add_argument("-j", dest="p_Job", default="DWH_KORR")
    parser.add_argument("-v", dest="p_Verbose", action="store_true")
    parser.add_argument("-h", action="store_true")

    args, unknown = parser.parse_known_args()

    if args.h:
        usage()
        return 0

    if unknown:
        DW_MSG_MeldeFehler(0, "E", 192, str(unknown))
        usage()
        return 192

    if not args.p_sqlscript:
        DW_MSG_MeldeFehler(0, "E", 193, "-f")
        usage()
        return 193

    p_sqlscript = args.p_sqlscript.lower()
    p_sqlpar = args.p_sqlpar
    p_Job = args.p_Job
    p_Verbose = 1 if args.p_Verbose else 0

    # Step 3: Change directory to script's location and resolve SQL script path
    script_dir = Path(__file__).resolve().parent
    try:
        os.chdir(script_dir)
    except Exception as e:
        logging.error(f"Could not change directory to {script_dir}: {e}")
        return 1

    sql_path = Path(p_sqlscript)
    l_DBskript = None

    if sql_path.parent == Path('.'):
        # Search priority: ../sql, then ../mig, then current directory
        opt1 = script_dir.parent / "sql" / p_sqlscript
        opt2 = script_dir.parent / "mig" / p_sqlscript
        opt3 = script_dir / p_sqlscript

        if opt1.is_file():
            l_DBskript = opt1
        elif opt2.is_file():
            l_DBskript = opt2
        else:
            l_DBskript = opt3
    else:
        l_DBskript = sql_path

    # Step 4: Replicate legacy file existence checks and edge behavior
    # REVIEW: legacy script logic sets ErrNr=198 when the SQL script file EXISTS, and references undefined variable p_Kuerzel. Verify if this check is inverted or obsolete.
    if l_DBskript.is_file():
        err_nr = 198
        p_Kuerzel = ""  # Undefined in legacy script, initialized here to prevent runtime crash
        DW_MSG_MeldeFehler(0, "E", err_nr, p_Kuerzel)

    # Step 5: Format Job Kennung
    JobKennung = p_Job.upper()

    print("----------------- Parameter -----------------")
    print(f"Jobkennung     : {JobKennung}")
    print(f"DB-Skript      : {l_DBskript}")
    print("---------------------------------------------")

    # Step 6: Initialize Logging Framework Parameters
    DW_EintragsNr = DWMSG_ErmittleNr()
    LogDatei = DWMSG_Logdateiname(JobKennung, DW_EintragsNr)
    DWMSG_ErzeugeEintrag(DW_EintragsNr, JobKennung, f"r_sqlscript_{l_DBskript}", LogDatei)

    # Configure file logging to match legacy redirection behavior
    try:
        file_handler = logging.FileHandler(LogDatei, encoding='utf-8')
        file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        logging.getLogger().addHandler(file_handler)
    except Exception as e:
        logging.error(f"Could not create file handler for {LogDatei}: {e}")

    # Step 7: Core Job Block with exception handling (Python equivalent to trap INT ERR)
    try:
        print("----------------- Job -----------------------")
        print(f"Job-Nr    : '{DW_EintragsNr}'")
        print(f"Logdatei  : '{LogDatei}'")
        print("---------------------------------------------")

        # Step 8: Execute SQL Script (Representing the legacy starteSQLSkript resolved to BigQuery)
        # REVIEW-STRUCT: launcher starteSQLSkript invoked — internal behaviour not available in this extraction; confirm logging, error propagation, and credential handling before finalizing the conversion
        # REVIEW-STRUCT: connection parameters inferred from a cross-referenced .ksh file's declared environment parameters — confirm these exact env var names are set in this job's actual runtime environment before deploying
        if not l_DBskript.exists():
            raise FileNotFoundError(f"SQL file not found: {l_DBskript}")
        
        with open(l_DBskript, 'r', encoding='utf-8') as sql_file:
            query_text = sql_file.read()

        # Instantiate BigQuery Client using explicit project context
        client = bigquery.Client(project=gcp_project)
        
        logging.info(f"Executing Query: {l_DBskript} on BigQuery")
        if p_sqlpar:
            logging.info(f"Note: Parameters provided: {p_sqlpar}. POSITIONAL parameters (&1, &2) in BigQuery require explicit mapping.")

        query_job = client.query(query_text)
        query_job.result()  # Wait for job completion. Will raise GoogleAPIError on SQL failure.

    except GoogleAPIError as e:
        logging.error(f"BigQuery execution failed: {str(e)}")
        print("!FEHLER gemeldet!", file=sys.stderr)
        DW_MSG_Fehlerbehandlung(DW_EintragsNr, LogDatei, p_Verbose)
        return 1
    except Exception as e:
        logging.error(f"Execution failed: {str(e)}")
        print("!OSFEHLER gemeldet!", file=sys.stderr)
        DW_MSG_Fehlerbehandlung(DW_EintragsNr, LogDatei, p_Verbose)
        return 1

    # Step 9: Post-execution success procedures
    DWMSG_SetzeStatusOK(DW_EintragsNr)
    print("Die Abarbeitung des Rahmenskriptes ist ohne erkennbare Fehler beendet")
    return 0

if __name__ == "__main__":
    sys.exit(main())