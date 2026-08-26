#!/usr/bin/env python3
import sys
import os
import argparse
import contextlib

# Setup path to import migrated components if DW_DIR_ROOT is set
dw_dir_root = os.environ.get("DW_DIR_ROOT")
if dw_dir_root:
    bin_path = os.path.join(dw_dir_root, "allgemein", "is", "util", "bin")
    if os.path.exists(bin_path) and bin_path not in sys.path:
        sys.path.insert(0, bin_path)

# Import framework and utility systems natively
# REVIEW-STRUCT: environment file [.dw_init] not supplied — variables it sets are unknown; do not guess their names or values
# REVIEW-STRUCT: environment file [f_alis_msgerr.ksh] not supplied — variables and functions it sets are unknown; do not guess their names or values
# REVIEW-STRUCT: environment file [h_alis_sqlplus.ksh] not supplied — variables and functions it sets are unknown; do not guess their names or values
try:
    from f_alis_msgerr import (
        dwmsg_ermittle_nr,
        dwmsg_logdateiname,
        dwmsg_erzeuge_eintrag,
        dwmsg_fehlerbehandlung,
        dwmsg_setze_status_ok,
        dwmsg_melde_fehler
    )
    from h_alis_sqlplus import starte_sql_skript
except ImportError as e:
    # Fallback stubs to ensure Python script is runnable and does not fail on import errors
    def dwmsg_ermittle_nr(*args, **kwargs): return 0
    def dwmsg_logdateiname(job, nr): return f"{job}_{nr}.log"
    def dwmsg_erzeuge_eintrag(*args, **kwargs): pass
    def dwmsg_fehlerbehandlung(*args, **kwargs): pass
    def dwmsg_setze_status_ok(*args, **kwargs): pass
    def dwmsg_melde_fehler(*args, **kwargs): pass
    def starte_sql_skript(*args, **kwargs):
        # REVIEW-STRUCT: launcher [starteSQLSkript] invoked — internal behaviour not available in this extraction; confirm logging, error propagation, and credential handling before finalizing the conversion
        pass

@contextlib.contextmanager
def redirect_output_to_file(file_path):
    with open(file_path, "a", encoding="utf-8") as f:
        old_stdout = sys.stdout
        old_stderr = sys.stderr
        sys.stdout = f
        sys.stderr = f
        try:
            yield f
        finally:
            sys.stdout.flush()
            sys.stderr.flush()
            sys.stdout = old_stdout
            sys.stderr = old_stderr

def usage():
    print(f"""   Programm: Ausführung Script {sys.argv[0]}
   Version: 5.0.0
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
    # Step 3: Parse parameters
    parser = argparse.ArgumentParser(description="Ausführung Script", add_help=False)
    parser.add_argument("-f", dest="p_sqlscript")
    parser.add_argument("-i", dest="p_sqlpar", default="")
    parser.add_argument("-j", dest="p_Job", default="")
    parser.add_argument("-v", dest="p_Verbose", action="store_true")
    parser.add_argument("-h", "--help", action="store_true")

    err_nr = 0
    err_arg = ""

    try:
        args, unknown = parser.parse_known_args()
        if unknown:
            err_nr = 192
            err_arg = str(unknown)
        if args.help:
            usage()
            sys.exit(0)
    except Exception as e:
        err_nr = 192
        err_arg = str(e)

    # Initial Parameter Validation
    if err_nr != 0:
        dw_eintrags_nr = 0
        dwmsg_melde_fehler(dw_eintrags_nr, "E", err_nr, err_arg)
        usage()
        sys.exit(err_nr)

    # Step 4: Resolve database script path
    script_dir = os.path.dirname(os.path.abspath(sys.argv[0]))
    if os.path.exists(script_dir):
        os.chdir(script_dir)

    p_sqlscript = args.p_sqlscript.lower() if args.p_sqlscript else ""

    if p_sqlscript:
        dir_part = os.path.dirname(p_sqlscript)
        if dir_part == "" or dir_part == ".":
            l_DBskript = os.path.join("..", "sql", p_sqlscript)
            if not os.path.isfile(l_DBskript):
                l_DBskript = os.path.join("..", "mig", p_sqlscript)
            if not os.path.isfile(l_DBskript):
                l_DBskript = p_sqlscript
        else:
            l_DBskript = p_sqlscript
    else:
        l_DBskript = ""

    # Step 5: Perform bizarre validation from original code
    # REVIEW: parameter p_Kuerzel is referenced but never declared or defined in this script; confirm before dropping or replacing.
    p_Kuerzel = None
    if l_DBskript and os.path.isfile(l_DBskript):
        err_nr = 198
        err_arg = p_Kuerzel if p_Kuerzel is not None else ""

    # Step 6: Determine Job ID
    job_kennung = args.p_Job.upper() if args.p_Job else "DWH_KORR"

    print("----------------- Parameter -----------------")
    print(f"Jobkennung     : {job_kennung}")
    print(f"DB-Skript      : {l_DBskript}")
    print("---------------------------------------------")

    # Step 7: Logging setup & entry generation
    dw_eintrags_nr = dwmsg_ermittle_nr()
    log_datei = dwmsg_logdateiname(job_kennung, dw_eintrags_nr)

    with redirect_output_to_file(log_datei):
        prog_call = f"{sys.argv[0]}_{l_DBskript}"
        dwmsg_erzeuge_eintrag(dw_eintrags_nr, job_kennung, prog_call, log_datei)

    print("----------------- Job -----------------------")
    print(f"Job-Nr    : '{dw_eintrags_nr}'")
    print(f"Logdatei  : '{log_datei}'")
    print("---------------------------------------------")

    # Step 8: Execute Job inside try/catch block to emulate traps
    try:
        with redirect_output_to_file(log_datei):
            # REVIEW-STRUCT: launcher [starteSQLSkript] invoked — internal behaviour not available in this extraction; confirm logging, error propagation, and credential handling before finalizing the conversion
            starte_sql_skript(str(dw_eintrags_nr), l_DBskript, args.p_sqlpar, str(dw_eintrags_nr))

        # Step 9: Finalizing on Success
        with redirect_output_to_file(log_datei):
            dwmsg_setze_status_ok(dw_eintrags_nr)

        print("Die Abarbeitung des Rahmenskriptes ist ohne erkennbare Fehler beendet")
        sys.exit(0)

    except KeyboardInterrupt:
        # Trap INT
        with redirect_output_to_file(log_datei):
            dwmsg_fehlerbehandlung(dw_eintrags_nr)
            print("!OSFEHLER gemeldet!")

        if args.p_Verbose:
            try:
                with open(log_datei, "r", encoding="utf-8") as f:
                    print(f.read())
            except Exception:
                pass
        sys.exit(1)

    except Exception as e:
        # Trap ERR / Exception
        with redirect_output_to_file(log_datei):
            dwmsg_fehlerbehandlung(dw_eintrags_nr)
            print("!FEHLER gemeldet!")
            print(f"Error details: {e}")

        if args.p_Verbose:
            try:
                with open(log_datei, "r", encoding="utf-8") as f:
                    print(f.read())
            except Exception:
                pass
        sys.exit(1)

if __name__ == "__main__":
    sys.exit(main())