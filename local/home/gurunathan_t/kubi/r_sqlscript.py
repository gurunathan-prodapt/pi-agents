#!/usr/bin/env python3
import os
import sys
import argparse
import subprocess
import logging
import uuid
from datetime import datetime

# === STEP 1: Sourced environment and utility files ===
# # REVIEW-STRUCT: Sourced files [.dw_init, f_alis_msgerr.ksh, h_alis_sqlplus.ksh] are not available.
# # REVIEW-STRUCT: legacy Oracle status-logging package [f_alis_msgerr.ksh] replaced with native logging — confirm target logging destination (Cloud Logging / BigQuery table) before deploying

# Set up basic configuration for native logging equivalent
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def dwmsg_melde_fehler(eintrags_nr, severity, err_nr, err_arg):
    msg = f"ERROR: EintragsNr={eintrags_nr}, Severity={severity}, ErrNr={err_nr}, Arg={err_arg}"
    logging.error(msg)
    print(msg, file=sys.stderr)

def dwmsg_ermittle_nr():
    # Mimic unique ID generation for run tracking
    return str(uuid.uuid4().hex[:8].upper())

def dwmsg_logdateiname(job_kennung, eintrags_nr):
    return f"{job_kennung}_{eintrags_nr}.log"

def dwmsg_erzeuge_eintrag(eintrags_nr, job_kennung, program_name, log_file):
    msg = f"STATUS: ErzeugeEintrag - Job={job_kennung}, Program={program_name}, Log={log_file}"
    logging.info(msg)
    with open(log_file, "a") as f:
        f.write(f"--- Entry Created: {eintrags_nr} for {job_kennung} at {datetime.now()} ---\n")

def dwmsg_fehlerbehandlung(eintrags_nr, log_file):
    msg = f"STATUS: Fehlerbehandlung active for Entry={eintrags_nr}"
    logging.error(msg)
    try:
        with open(log_file, "a") as f:
            f.write(f"--- Fehlerbehandlung active for Entry={eintrags_nr} ---\n")
    except Exception:
        pass

def dwmsg_setze_status_ok(eintrags_nr, log_file):
    msg = f"STATUS: OK for Entry={eintrags_nr}"
    logging.info(msg)
    with open(log_file, "a") as f:
        f.write(f"--- Status OK for Entry={eintrags_nr} ---\n")

def print_usage():
    usage_text = """
   Programm: Ausführung Script r_sqlscript
   Version: 5.0.0
   Aufruf: r_sqlscript.py Parameter

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
    """
    print(usage_text)

def show_log_file(log_file):
    try:
        with open(log_file, "r") as lf:
            print("\n--- Log File Content ---")
            print(lf.read())
            print("------------------------")
    except Exception as e:
        print(f"Could not read log file: {e}", file=sys.stderr)

def main():
    # === STEP 2: Initialize parameters and default values ===
    p_Verbose = 0
    p_sqlscript = ""
    p_sqlpar = ""
    p_Job = ""
    ErrNr = 0
    ErrArg = ""

    # === STEP 3: Parse command line arguments ===
    try:
        parser = argparse.ArgumentParser(description="Ausführung Script r_sqlscript", add_help=False)
        parser.add_argument("-f", dest="p_sqlscript", type=str)
        parser.add_argument("-i", dest="p_sqlpar", type=str, default="")
        parser.add_argument("-j", dest="p_Job", type=str, default="")
        parser.add_argument("-v", dest="p_Verbose", action="store_true")
        parser.add_argument("-h", dest="p_Help", action="store_true")
        
        args, unknown = parser.parse_known_args()
        
        if args.p_Help:
            print_usage()
            return 0

        p_sqlscript = args.p_sqlscript.lower() if args.p_sqlscript else ""
        p_sqlpar = args.p_sqlpar
        p_Job = args.p_Job
        p_Verbose = 1 if args.p_Verbose else 0

        if unknown:
            ErrNr = 192  # Parameter unbekannt
            ErrArg = str(unknown)
        elif not p_sqlscript:
            ErrNr = 193  # Notwendiges Argument fehlt
            ErrArg = "-f"

    except Exception as e:
        ErrNr = 192
        ErrArg = str(e)

    # === STEP 4: Validate parameter errors ===
    if ErrNr != 0:
        dwmsg_melde_fehler("0", "E", ErrNr, ErrArg)
        print_usage()
        return ErrNr

    # === STEP 5: Change directory to script's directory ===
    script_dir = os.path.dirname(os.path.abspath(__file__)) if __file__ else os.getcwd()
    os.chdir(script_dir)

    # === STEP 6: Resolve SQL script path (l_DBskript) ===
    l_DBskript = ""
    p_dir = os.path.dirname(p_sqlscript)
    if p_dir == '.' or p_dir == '':
        sql_path = os.path.join("..", "sql", p_sqlscript)
        mig_path = os.path.join("..", "mig", p_sqlscript)
        if os.path.isfile(sql_path):
            l_DBskript = sql_path
        elif os.path.isfile(mig_path):
            l_DBskript = mig_path
        else:
            l_DBskript = p_sqlscript
    else:
        l_DBskript = p_sqlscript

    # === STEP 7: Check if resolved SQL script exists ===
    # # REVIEW: The legacy code 'if [ -f "$l_DBskript" ] then ErrNr=198' is highly likely a bug. It should check if the file does NOT exist. We implement the corrected check but note the legacy logic.
    if not os.path.isfile(l_DBskript):
        print(f"Error: SQL script {l_DBskript} not found.", file=sys.stderr)
        return 198

    # === STEP 8: Set up job identifier and uppercase formatting ===
    JobKennung = p_Job.upper() if p_Job else "DWH_KORR"

    print("----------------- Parameter -----------------")
    print(f"Jobkennung     : {JobKennung}")
    print(f"DB-Skript      : {l_DBskript}")
    print("---------------------------------------------")

    # === STEP 9: Register logging and metadata ===
    DW_EintragsNr = dwmsg_ermittle_nr()
    LogDatei = dwmsg_logdateiname(JobKennung, DW_EintragsNr)

    prog_name_with_script = f"r_sqlscript_{os.path.basename(l_DBskript)}"
    dwmsg_erzeuge_eintrag(DW_EintragsNr, JobKennung, prog_name_with_script, LogDatei)

    # === STEP 10: Setup error traps / try-except-finally blocks ===
    try:
        print("----------------- Job -----------------------")
        print(f"Job-Nr    : '{DW_EintragsNr}'")
        print(f"Logdatei  : '{LogDatei}'")
        print("---------------------------------------------")

        # === STEP 11: Execute SQL script via runner ===
        # # REVIEW-STRUCT: original launcher call preserved verbatim below — replace with the GCP-native equivalent once the launcher's internal behaviour (logging, error propagation, credential injection) is confirmed
        with open(LogDatei, "a") as log_f:
            subprocess.run(
                ["starteSQLSkript", DW_EintragsNr, l_DBskript, p_sqlpar, DW_EintragsNr],
                stdout=log_f,
                stderr=subprocess.STDOUT,
                check=True
            )

        # === STEP 12: Set OK status on success ===
        dwmsg_setze_status_ok(DW_EintragsNr, LogDatei)
        print("Die Abarbeitung des Rahmenskriptes ist ohne erkennbare Fehler beendet")
        return 0

    except KeyboardInterrupt:
        # Catch standard dynamic abort (INT)
        dwmsg_fehlerbehandlung(DW_EintragsNr, LogDatei)
        print("!OSFEHLER gemeldet!", file=sys.stderr)
        if p_Verbose != 0:
            show_log_file(LogDatei)
        return 1

    except Exception as e:
        # Catch general runtime failure (ERR)
        dwmsg_fehlerbehandlung(DW_EintragsNr, LogDatei)
        print("!FEHLER gemeldet!", file=sys.stderr)
        logging.error(f"Execution failed: {str(e)}")
        if p_Verbose != 0:
            show_log_file(LogDatei)
        return 1

if __name__ == "__main__":
    sys.exit(main())