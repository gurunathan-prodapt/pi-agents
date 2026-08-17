#!/usr/bin/env python3
import sys
import os
import argparse
import subprocess
from contextlib import redirect_stdout, redirect_stderr

# Add dynamic paths for sourcing libraries
dw_dir_root = os.environ.get("DW_DIR_ROOT")
if dw_dir_root:
    sys.path.append(os.path.join(dw_dir_root, "allgemein", "is", "util", "bin"))

home_dir = os.environ.get("HOME")
if home_dir:
    sys.path.append(os.path.join(home_dir, "aktuell"))

# Try importing the migrated modules. If they are not available in PYTHONPATH,
# we fall back to minimal stub implementations so the script remains runnable.

try:
    import dw_init
except ImportError:
    # REVIEW-STRUCT: environment file [.dw_init] not supplied — variables it sets are unknown; do not guess their names or values
    dw_init = None

try:
    from f_alis_msgerr import (
        DWMSG_MeldeFehler,
        DWMSG_ErmittleNr,
        DWMSG_Logdateiname,
        DWMSG_ErzeugeEintrag,
        DWMSG_Fehlerbehandlung,
        DWMSG_SetzeStatusOK
    )
except ImportError:
    # REVIEW-STRUCT: environment file [f_alis_msgerr.ksh] not supplied — falling back to stubs
    def DWMSG_MeldeFehler(eintrags_nr, severity, err_nr, err_arg):
        print(f"ERROR: {severity} {err_nr} {err_arg}", file=sys.stderr)

    def DWMSG_ErmittleNr():
        return 12345

    def DWMSG_Logdateiname(job_kennung, eintrags_nr):
        return f"log_{job_kennung}_{eintrags_nr}.log"

    def DWMSG_ErzeugeEintrag(eintrags_nr, job_kennung, program, log_file):
        print(f"Eintrag erzeugt: {eintrags_nr}, {job_kennung}, {program}, {log_file}")

    def DWMSG_Fehlerbehandlung(eintrags_nr):
        print(f"Fehlerbehandlung fuer {eintrags_nr}")

    def DWMSG_SetzeStatusOK(eintrags_nr):
        print(f"Status OK fuer {eintrags_nr}")

try:
    from h_alis_sqlplus import starteSQLSkript
except ImportError:
    # REVIEW-STRUCT: environment file [h_alis_sqlplus.ksh] not supplied — falling back to subprocess launcher stub
    # REVIEW-STRUCT: original launcher call preserved verbatim below — replace with the GCP-native equivalent once the launcher's internal behaviour (logging, error propagation, credential injection) is confirmed
    def starteSQLSkript(eintrags_nr, db_skript, sql_par, tracking_nr):
        cmd = ["starteSQLSkript", str(eintrags_nr), db_skript, sql_par, str(tracking_nr)]
        subprocess.run(cmd, check=True)


PROG_NAME = f"Ausführung Script {sys.argv[0]}"
PROG_VERSION = "5.0.0"


def usage():
    print(f"""   Programm: {PROG_NAME}
   Version: {PROG_VERSION}
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


def run_redirected(func, log_path, *args, **kwargs):
    """Utility to redirect stdout and stderr of a function to a log file."""
    with open(log_path, "a") as f:
        with redirect_stdout(f), redirect_stderr(f):
            func(*args, **kwargs)


def log_append(log_path, text):
    """Utility to append a raw string line to a log file."""
    with open(log_path, "a") as f:
        f.write(text + "\n")


def main():
    # Step 3: Argument Parsing (Simulating getopts)
    args = sys.argv[1:]
    p_sqlscript = None
    p_sqlpar = ""
    p_Job = ""
    p_Verbose = 0
    show_help = False

    err_nr = 0
    err_arg = ""

    i = 0
    while i < len(args):
        arg = args[i]
        if arg == "-h":
            show_help = True
            i += 1
        elif arg == "-v":
            p_Verbose = 1
            i += 1
        elif arg == "-f":
            if i + 1 < len(args):
                p_sqlscript = args[i+1].lower()  # typeset -l equivalent
                i += 2
            else:
                err_nr = 193
                err_arg = "f"
                break
        elif arg == "-i":
            if i + 1 < len(args):
                p_sqlpar = args[i+1]
                i += 2
            else:
                err_nr = 193
                err_arg = "i"
                break
        elif arg == "-j":
            if i + 1 < len(args):
                p_Job = args[i+1]
                i += 2
            else:
                err_nr = 193
                err_arg = "j"
                break
        elif arg.startswith("-"):
            err_nr = 192
            err_arg = arg
            break
        else:
            err_nr = 192
            err_arg = arg
            break

    # Step 4: Parameter Validation Handling
    if err_nr == 0 and not show_help and p_sqlscript is None:
        err_nr = 193
        err_arg = "-f"

    if show_help:
        usage()
        sys.exit(0)

    if err_nr != 0:
        DWMSG_MeldeFehler(0, "E", err_nr, err_arg)
        usage()
        sys.exit(err_nr)

    # Step 5: Dynamic Path Resolution
    # Change current working directory to script location
    script_dir = os.path.dirname(os.path.abspath(sys.argv[0]))
    os.chdir(script_dir)

    p_sqlscript_dir = os.path.dirname(p_sqlscript)
    if p_sqlscript_dir in ("", "."):
        l_DBskript = os.path.join("..", "sql", p_sqlscript)
        if not os.path.exists(l_DBskript):
            l_DBskript = os.path.join("..", "mig", p_sqlscript)
        if not os.path.exists(l_DBskript):
            l_DBskript = p_sqlscript
    else:
        l_DBskript = p_sqlscript

    # Step 6: Validate File Existence
    # REVIEW: Legacy code had 'if [ -f "$l_DBskript" ] then ErrNr=198' which raised an error if the file DID exist.
    # Corrected here to check if the file does NOT exist, as indicated in the design document risks section.
    if not os.path.exists(l_DBskript):
        err_nr_file = 198
        # NOTE: p_Kuerzel is referenced but undefined in the original script.
        DWMSG_MeldeFehler(0, "E", err_nr_file, "p_Kuerzel")
        sys.exit(err_nr_file)

    # Step 7: Logging and Environment Configuration Setup
    if not p_Job:
        job_kennung = "DWH_KORR"
    else:
        job_kennung = p_Job.upper()  # typeset -u equivalent

    print("----------------- Parameter -----------------")
    print(f"Jobkennung     : {job_kennung}")
    print(f"DB-Skript      : {l_DBskript}")
    print("---------------------------------------------")

    dw_eintrags_nr = DWMSG_ErmittleNr()
    log_datei = DWMSG_Logdateiname(job_kennung, dw_eintrags_nr)

    run_redirected(DWMSG_ErzeugeEintrag, log_datei, dw_eintrags_nr, job_kennung, f"{sys.argv[0]}_{l_DBskript}", log_datei)

    # Step 8: Safe Trapped Execution Block
    try:
        print("----------------- Job -----------------------")
        print(f"Job-Nr    : '{dw_eintrags_nr}'")
        print(f"Logdatei  : '{log_datei}'")
        print("---------------------------------------------")

        # Step 9: Core Execution
        run_redirected(starteSQLSkript, log_datei, dw_eintrags_nr, l_DBskript, p_sqlpar, dw_eintrags_nr)

        # Step 10: Completion and Cleanup
        run_redirected(DWMSG_SetzeStatusOK, log_datei, dw_eintrags_nr)

        print("Die Abarbeitung des Rahmenskriptes ist ohne erkennbare Fehler beendet")
        sys.exit(0)

    except KeyboardInterrupt:
        # Trapping INT signal
        # Run custom cleanup and print '!OSFEHLER gemeldet!'
        run_redirected(DWMSG_Fehlerbehandlung, log_datei, dw_eintrags_nr)
        log_append(log_datei, "!OSFEHLER gemeldet!")

        if p_Verbose != 0:
            if os.path.exists(log_datei):
                with open(log_datei, "r") as f:
                    print(f.read(), file=sys.stderr)
        print("!OSFEHLER gemeldet!", file=sys.stderr)
        sys.exit(1)

    except Exception as e:
        # Trapping execution ERR signal
        # Run custom cleanup and print '!FEHLER gemeldet!'
        run_redirected(DWMSG_Fehlerbehandlung, log_datei, dw_eintrags_nr)
        log_append(log_datei, "!FEHLER gemeldet!")

        if p_Verbose != 0:
            if os.path.exists(log_datei):
                with open(log_datei, "r") as f:
                    print(f.read(), file=sys.stderr)
        print("!FEHLER gemeldet!", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    sys.exit(main())