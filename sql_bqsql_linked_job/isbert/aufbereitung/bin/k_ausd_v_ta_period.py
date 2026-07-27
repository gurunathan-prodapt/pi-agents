#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Kontrollscript zu r_ausd_vertrag.ksh
Autor:    Fabian Debus
Erstellt: 20.12.2007

Zweck:  Kontrollscript zu r_ausd_vertrag.ksh
        a) aktive Jobs werden ignoriert
        b) Aufruf SQL-Skript und Eintrag in die
           Job-Tabelle
        c) alte aktive Jobs werden einfach dekativiert
"""

import os
import sys
import argparse

# Try to import starteSQLSkript from the migrated shared utility module
try:
    from h_alis_sqlplus import starteSQLSkript
except ImportError:
    # Add potential shared utility paths to sys.path
    # The utilities are at sql_bqsql_linked_job/isbert/allgemein/is/util/bin/h_alis_sqlplus.py
    # While this script is at sql_bqsql_linked_job/isbert/aufbereitung/bin/k_ausd_v_ta_period.py
    current_dir = os.path.dirname(os.path.abspath(__file__))
    potential_path = os.path.abspath(os.path.join(current_dir, "..", "..", "allgemein", "is", "util", "bin"))
    if potential_path not in sys.path:
        sys.path.append(potential_path)
    try:
        from h_alis_sqlplus import starteSQLSkript
    except ImportError:
        # Fallback wrapper if import completely fails at runtime
        def starteSQLSkript(eintrags_nr, sql_script, eintrags_nr_2, job_kennung):
            import subprocess
            print("WARNING: h_alis_sqlplus import failed. Falling back to subprocess execution of starteSQLSkript.", file=sys.stderr)
            subprocess.run(
                ["starteSQLSkript", str(eintrags_nr), str(sql_script), str(eintrags_nr_2), str(job_kennung)],
                check=True
            )


def main():
    # Step 1: Environment Initialization
    bert_dir_root = os.environ.get("BERT_DIR_ROOT")
    if not bert_dir_root:
        print("ERROR: Environment variable BERT_DIR_ROOT is not set.", file=sys.stderr)
        sys.exit(1)

    dw_dir_utl = os.environ.get("DW_DIR_UTL")
    if not dw_dir_utl:
        print("ERROR: Environment variable DW_DIR_UTL is not set.", file=sys.stderr)
        sys.exit(1)

    # Step 2: Argument Parsing (mimicking getopts)
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("-j", dest="job_kennung", default=None)
    parser.add_argument("-f", dest="eintrags_nr", default=None)
    parser.add_argument("-h", action="store_true", dest="help_flag")

    try:
        args, unknown = parser.parse_known_args()
    except Exception:
        print("Bitte ueber Rahmenscript aufrufen")
        sys.exit(192)

    if unknown:
        print("Bitte ueber Rahmenscript aufrufen")
        sys.exit(192)

    if args.help_flag:
        print("Bitte ueber Rahmenscript aufrufen")
        sys.exit(0)

    # Step 3: Set Table Name
    v_TabName = "ta_period"

    # Step 4: Parameter Validation
    err_nr = 0
    err_arg = ""

    if not args.job_kennung:
        err_nr = 193
        err_arg = "Jobkennung"
    elif not args.eintrags_nr:
        err_nr = 193
        err_arg = "EintragsNr"

    # If error occurred, report and exit
    if err_nr != 0:
        # Imitating DWMSG_MeldeFehler via output
        # Print format: FEHLER: 0 E $ErrNr $ErrArg
        print(f"FEHLER: 0 E {err_nr} {err_arg}")
        print("Bitte ueber Rahmenscript aufrufen")
        sys.exit(err_nr)

    # Step 5: SQL File and Temp File Setup
    name_sql_skript = os.path.join(bert_dir_root, "aufbereitung", "sql", "d_ausd_v_ta_period.sql")
    pid = os.getpid()
    tmp_file = os.path.join(dw_dir_utl, f"bert_k_ausd_v_ta_period_{pid}.tmp")

    # Step 6: Execute DB Script
    try:
        starteSQLSkript(args.eintrags_nr, name_sql_skript, args.eintrags_nr, args.job_kennung)
    except Exception as e:
        print(f"ERROR: Database script execution failed: {e}", file=sys.stderr)
        sys.exit(1)

    print(" ---------- ENDE Datenverarbeitung ----------")

    # Step 7: Retrieve Number of Provided Records
    v_records = ""
    try:
        if os.path.exists(tmp_file):
            with open(tmp_file, "r") as f:
                v_records = f.read().strip()
            # Clean up the temporary metrics file
            try:
                os.remove(tmp_file)
            except OSError:
                pass
        else:
            print(f"WARNING: Metrics temporary file {tmp_file} does not exist.", file=sys.stderr)
    except Exception as e:
        print(f"WARNING: Unable to read metrics temporary file: {e}", file=sys.stderr)

    # Output records for potential downstream logging/tracking
    if v_records:
        print(f"INFO: Processed records count: {v_records}")

    return 0


if __name__ == "__main__":
    sys.exit(main())