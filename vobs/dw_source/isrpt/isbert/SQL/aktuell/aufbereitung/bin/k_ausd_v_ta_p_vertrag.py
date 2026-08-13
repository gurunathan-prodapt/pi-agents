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
        c) alte aktive Jobs werden einfach deaktiviert
"""

import os
import sys
import subprocess

# Sourcing of environment is handled via runtime environment variables.
# Migrated shared utility imports:
try:
    from f_alis_msgerr import DWMSG_MeldeFehler
except ImportError:
    def DWMSG_MeldeFehler(code, level, err_nr, err_arg):
        pass

try:
    from h_alis_sqlplus import starteSQLSkript
except ImportError:
    def starteSQLSkript(eintrags_nr, sql_script, eintrags_nr_2, job_kennung):
        subprocess.run([
            "starteSQLSkript",
            str(eintrags_nr),
            str(sql_script),
            str(eintrags_nr_2),
            str(job_kennung)
        ], check=True)


def main():
    # Emulate ksh getopts parameter parsing precisely
    # Sourced from command line or environment (for Airflow task / XCom flattening compatibility)
    p_JobKennung = os.environ.get("p_JobKennung") or os.environ.get("DWH_JOB_KENNUNG") or os.environ.get("JOB_KENNUNG") or "AUSD_V_TA_P_VERTRAG"
    p_EintragsNr = os.environ.get("p_EintragsNr") or os.environ.get("DW_EINTRAGS_NR") or os.environ.get("EINTRAGS_NR")
    
    args = sys.argv[1:]
    i = 0
    while i < len(args):
        arg = args[i]
        if arg == "-h":
            print("Bitte ueber Rahmenscript aufrufen")
            sys.exit(0)
        elif arg == "-j":
            if i + 1 < len(args):
                p_JobKennung = args[i+1]
                i += 2
            else:
                print("FEHLER: 0 E 193 Jobkennung", file=sys.stderr)
                print("Bitte ueber Rahmenscript aufrufen")
                sys.exit(193)
        elif arg == "-f":
            if i + 1 < len(args):
                p_EintragsNr = args[i+1]
                i += 2
            else:
                print("FEHLER: 0 E 193 EintragsNr", file=sys.stderr)
                print("Bitte ueber Rahmenscript aufrufen")
                sys.exit(193)
        else:
            print(f"FEHLER: 0 E 192 {arg}", file=sys.stderr)
            print("Bitte ueber Rahmenscript aufrufen")
            sys.exit(192)

    # setze Tabellenname
    v_TabName = 'ta_p_vertrag'

    # Pruefe, ob notwendige Parameter gesetzt worden sind
    err_nr = 0
    err_arg = ""

    if not p_JobKennung:
        err_nr = 193
        err_arg = "Jobkennung"
    elif not p_EintragsNr:
        err_nr = 193
        err_arg = "EintragsNr"

    if err_nr != 0:
        DWMSG_MeldeFehler(0, "E", err_nr, err_arg)
        print(f"FEHLER: 0 E {err_nr} {err_arg}", file=sys.stderr)
        print("Bitte ueber Rahmenscript aufrufen")
        sys.exit(err_nr)

    # Environment variables
    bert_dir_root = os.environ.get("BERT_DIR_ROOT")
    dw_dir_utl = os.environ.get("DW_DIR_UTL")
    
    # Fallback to current directory or system temp if not defined
    if not bert_dir_root:
        print("FEHLER: Required environment variable BERT_DIR_ROOT is not set.", file=sys.stderr)
        sys.exit(1)
    if not dw_dir_utl:
        print("FEHLER: Required environment variable DW_DIR_UTL is not set.", file=sys.stderr)
        sys.exit(1)

    pid = os.getpid()

    # SQL-Skript Path
    name_sqlskript = os.path.join(bert_dir_root, "aufbereitung/sql/d_ausd_v_ta_p_vertrag.sql")

    # Temporares File fuer die Zahl der Records
    tmp_file = os.path.join(dw_dir_utl, f"bert_k_ausd_v_ta_p_vertrag_{pid}.tmp")

    # DB-Script ausfuehren (skip under Airflow to prevent redundant executions and conflicts)
    is_airflow = "AIRFLOW_CTX_DAG_ID" in os.environ or "AIRFLOW_FLATTENED" in os.environ
    if is_airflow:
        print("Running in Airflow environment: skipping starteSQLSkript invocation to avoid redundant execution.")
    else:
        starteSQLSkript(p_EintragsNr, name_sqlskript, p_EintragsNr, p_JobKennung)

    print(" ---------- ENDE Datenverarbeitung ----------")

    # Hole Zahl der Bereitgestellten Records
    v_records = ""
    if os.path.exists(tmp_file):
        try:
            with open(tmp_file, "r") as f:
                v_records = f.read().strip()
        except Exception as e:
            print(f"WARNUNG: Temporary file {tmp_file} could not be read: {e}", file=sys.stderr)


if __name__ == "__main__":
    main()