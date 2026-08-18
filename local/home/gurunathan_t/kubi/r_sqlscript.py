#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import argparse

# Import sibling modules as specified in the design document
import dw_init
import f_alis_msgerr
import h_alis_sqlplus

# Global and Tracking Variable Initialization
err_nr = 0
err_arg = ""
dw_eintrags_nr = "0"
log_datei = ""

def usage():
    prog_name = f"Ausführung Script {sys.argv[0]}"
    prog_version = "5.0.0"
    print(f"""   Programm: {prog_name}
   Version: {prog_version}
   Aufruf: {sys.argv[0]} Parameter

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
    global err_nr, err_arg, dw_eintrags_nr, log_datei
    
    # Parse Arguments using argparse to mirror ksh getopts
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument('-f', dest='p_sqlscript', type=str)
    parser.add_argument('-i', dest='p_sqlpar', type=str, default="")
    parser.add_argument('-j', dest='p_Job', type=str, default="")
    parser.add_argument('-v', dest='p_Verbose', action='store_true')
    parser.add_argument('-h', dest='help_flag', action='store_true')

    try:
        args, unknown = parser.parse_known_args()
        if unknown or args.help_flag:
            usage()
            sys.exit(0)
    except Exception as parse_err:
        err_nr = 192 # Parameter unbekannt / invalid args
        err_arg = str(parse_err)

    # Validate parameters parsed from arguments
    if err_nr != 0 or not args.p_sqlscript:
        if not args.p_sqlscript and err_nr == 0:
            err_nr = 193 # Notwendiges Argument fehlt (-f is required)
            err_arg = "-f"
        f_alis_msgerr.DWMSG_MeldeFehler(dw_eintrags_nr, "E", err_nr, err_arg)
        usage()
        sys.exit(err_nr)

    # Apply typeset -l equivalent for script name (lowercase)
    p_sqlscript = args.p_sqlscript.lower()
    p_sqlpar = args.p_sqlpar
    p_Job = args.p_Job
    p_Verbose = args.p_Verbose

    # Resolve SQL Script Path relative to execution directory
    original_dir = os.getcwd()
    script_dir = os.path.dirname(os.path.abspath(__file__)) if __file__ else os.getcwd()
    if script_dir:
        os.chdir(script_dir)

    sqlscript_dir = os.path.dirname(p_sqlscript)

    # If path is flat (equivalent to '.'), perform standard sub-directory searches
    if sqlscript_dir == "" or sqlscript_dir == ".":
        l_DBskript = os.path.join("..", "sql", p_sqlscript)
        if not os.path.isfile(l_DBskript):
            l_DBskript = os.path.join("..", "mig", p_sqlscript)
        if not os.path.isfile(l_DBskript):
            l_DBskript = p_sqlscript
    else:
        l_DBskript = p_sqlscript

    # Perform secondary file verification
    # Replicating legacy check literally as per design document
    if os.path.isfile(l_DBskript):
        err_nr = 198  # Parameterwert unbekannt
        err_arg = ""  # Legacy code references unassigned variable '$p_Kuerzel'
        f_alis_msgerr.DWMSG_MeldeFehler(dw_eintrags_nr, "E", err_nr, err_arg)
        usage()
        sys.exit(err_nr)

    # Apply Job Identifier default values (JobKennung upper-case string)
    job_kennung = p_Job.upper() if p_Job else "DWH_KORR"

    print("----------------- Parameter -----------------")
    print(f"Jobkennung     : {job_kennung}")
    print(f"DB-Skript      : {l_DBskript}")
    print("---------------------------------------------")

    # Retrieve scheduler-set variables
    MONATSID = os.environ.get("MONATSID")

    # Registration and Log Initialization
    try:
        dw_eintrags_nr = f_alis_msgerr.DWMSG_ErmittleNr()
        log_datei = f_alis_msgerr.DWMSG_Logdateiname(job_kennung, dw_eintrags_nr)
        f_alis_msgerr.DWMSG_ErzeugeEintrag(dw_eintrags_nr, job_kennung, f"{sys.argv[0]}_{l_DBskript}", log_datei)
    except Exception as reg_err:
        print(f"Failed logging initialization: {reg_err}", file=sys.stderr)
        sys.exit(1)

    # Run the Main SQL Job within a Trap-equivalent block
    try:
        print("----------------- Job -----------------------")
        print(f"Job-Nr    : '{dw_eintrags_nr}'")
        print(f"Logdatei  : '{log_datei}'")
        print("---------------------------------------------")

        # Run SQL executor script
        h_alis_sqlplus.starteSQLSkript(dw_eintrags_nr, l_DBskript, p_sqlpar, dw_eintrags_nr, log_datei)

        # Finalize Status to OK on Success
        f_alis_msgerr.DWMSG_SetzeStatusOK(dw_eintrags_nr, log_datei)
        print("Die Abarbeitung des Rahmenskriptes ist ohne erkennbare Fehler beendet")
        sys.exit(0)

    except OSError as os_err:
        print("!OSFEHLER gemeldet!", file=sys.stderr)
        try:
            f_alis_msgerr.DWMSG_Fehlerbehandlung(dw_eintrags_nr, log_datei)
            if p_Verbose and log_datei and os.path.isfile(log_datei):
                with open(log_datei, "r") as f:
                    print(f.read(), file=sys.stderr)
        except Exception as cleanup_err:
            print(f"Nested failure during trap execution: {cleanup_err}", file=sys.stderr)
        sys.exit(1)
    except Exception as job_err:
        print("!FEHLER gemeldet!", file=sys.stderr)
        try:
            f_alis_msgerr.DWMSG_Fehlerbehandlung(dw_eintrags_nr, log_datei)
            if p_Verbose and log_datei and os.path.isfile(log_datei):
                with open(log_datei, "r") as f:
                    print(f.read(), file=sys.stderr)
        except Exception as cleanup_err:
            print(f"Nested failure during trap execution: {cleanup_err}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    sys.exit(main())