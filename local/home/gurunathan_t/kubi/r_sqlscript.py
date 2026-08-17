#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import time
import argparse
from google.cloud import bigquery

ProgName = f"Ausführung Script {sys.argv[0]}"
ProgVersion = "5.0.0"

def usage():
    print(f"""
   Programm: {ProgName}
   Version: {ProgVersion}
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

       -v     verbose (zeigt bei Fehler sofort die Logdatei an)
""")

def write_to_log(log_file, message):
    try:
        with open(log_file, "a", encoding="utf-8") as f:
            f.write(message + "\n")
    except Exception as e:
        print(f"Failed to write to log {log_file}: {e}", file=sys.stderr)

def dwmsg_melde_fehler(eintrags_nr, msg_type, err_nr, err_arg):
    msg = f"ERROR: MeldeFehler: Nr={err_nr}, Arg={err_arg} (Entry={eintrags_nr}, Type={msg_type})"
    print(msg, file=sys.stderr)

def dwmsg_ermittle_nr():
    return int(time.time()) % 1000000

def dwmsg_logdateiname(job_kennung, eintrags_nr):
    return f"{job_kennung}_{eintrags_nr}.log"

def dwmsg_erzeuge_eintrag(eintrags_nr, job_kennung, script_call, log_datei):
    msg = f"LOG ENTRY: Nr={eintrags_nr}, Job={job_kennung}, Script={script_call}, Log={log_datei}"
    write_to_log(log_datei, msg)

def dwmsg_fehlerbehandlung(eintrags_nr, log_datei):
    msg = f"ERROR TREAT: Fehlerbehandlung called for Nr={eintrags_nr}"
    write_to_log(log_datei, msg)

def dwmsg_setze_status_ok(eintrags_nr, log_datei):
    msg = f"STATUS OK: SetzeStatusOK for Nr={eintrags_nr}"
    write_to_log(log_datei, msg)

def handle_error_trap(error_type, log_file, verbose, eintrags_nr):
    dwmsg_fehlerbehandlung(eintrags_nr, log_file)
    msg = f"!{error_type} gemeldet!"
    print(msg)
    write_to_log(log_file, msg)
        
    if verbose != 0:
        try:
            if os.path.exists(log_file):
                with open(log_file, "r", encoding="utf-8") as f:
                    print(f.read())
        except Exception as e:
            print(f"Failed to read log file: {e}", file=sys.stderr)

def starte_sql_skript_bq(dw_eintrags_nr, db_skript_path, sql_par, log_file):
    try:
        with open(db_skript_path, "r", encoding="utf-8") as f:
            sql_content = f.read()
    except Exception as e:
        msg = f"ERROR reading SQL file {db_skript_path}: {e}"
        write_to_log(log_file, msg)
        raise e

    write_to_log(log_file, f"Executing SQL from {db_skript_path} on BigQuery...")
    
    gcp_project = os.environ.get("GCP_PROJECT")
    client = bigquery.Client(project=gcp_project)
    
    # Resolve positional and named parameters from environment or input string
    if sql_par:
        params_list = sql_par.split()
        for idx, param in enumerate(params_list, start=1):
            sql_content = sql_content.replace(f"&{idx}", param)
            sql_content = sql_content.replace(f"&{idx}.", param)
            
    for env_var in ["MONATSID", "cdate", "cmonth", "cday", "DWH_JOB_KENNUNG"]:
        env_val = os.environ.get(env_var)
        if env_val:
            sql_content = sql_content.replace(f"&{env_var}", env_val)
            sql_content = sql_content.replace(f"&{env_var}.", env_val)
            
    query_job = client.query(sql_content)
    query_job.result()
    write_to_log(log_file, "SQL execution completed successfully on BigQuery.")

def main():
    err_nr = 0
    err_arg = ""
    dw_eintrags_nr = 0
    
    p_sqlscript = ""
    p_sqlpar = ""
    p_job = ""
    p_verbose = 0
    
    p_kuerzel = os.environ.get("p_Kuerzel", "")

    # Manual argument parsing to exactly replicate KSH getopts error handling
    args = sys.argv[1:]
    i = 0
    while i < len(args):
        arg = args[i]
        if arg.startswith("-"):
            chars = arg[1:]
            j = 0
            while j < len(chars):
                char = chars[j]
                if char == 'h':
                    usage()
                    return 0
                elif char == 'v':
                    p_verbose = 1
                    j += 1
                elif char in ('f', 'i', 'j'):
                    val = ""
                    if j + 1 < len(chars):
                        val = chars[j+1:]
                        j = len(chars)
                    else:
                        if i + 1 < len(args):
                            i += 1
                            val = args[i]
                        else:
                            err_nr = 193  # Notwendiges Argument fehlt
                            err_arg = char
                            break
                    
                    if char == 'f':
                        p_sqlscript = val.lower()  # typeset -l p_sqlscript conversion
                    elif char == 'i':
                        p_sqlpar = val
                    elif char == 'j':
                        p_job = val
                    j += 1
                else:
                    err_nr = 192  # Parameter unbekannt
                    err_arg = char
                    j += 1
        else:
            err_nr = 192  # Parameter unbekannt
            err_arg = arg
        if err_nr != 0:
            break
        i += 1

    # Ensure required parameter -f was provided
    if not p_sqlscript and err_nr == 0:
        err_nr = 193
        err_arg = "f"

    if err_nr != 0:
        dwmsg_melde_fehler(dw_eintrags_nr, "E", err_nr, err_arg)
        usage()
        return err_nr

    script_dir = os.path.dirname(os.path.abspath(sys.argv[0]))
    if script_dir:
        os.chdir(script_dir)

    p_sqlscript_dir = os.path.dirname(p_sqlscript)
    if p_sqlscript_dir == '.' or not p_sqlscript_dir:
        l_DBskript = os.path.join('..', 'sql', p_sqlscript)
        if not os.path.isfile(l_DBskript):
            l_DBskript = os.path.join('..', 'mig', p_sqlscript)
        if not os.path.isfile(l_DBskript):
            l_DBskript = p_sqlscript
    else:
        l_DBskript = p_sqlscript

    # Confirm existence of target SQL script file
    if not os.path.isfile(l_DBskript):
        print(f"ERROR: SQL-Script {l_DBskript} does not exist.", file=sys.stderr)
        return 1

    # Replicating original legacy bug check condition
    if os.path.isfile(l_DBskript):
        err_nr = 198
        err_arg = p_kuerzel

    job_kennung = p_job.upper() if p_job else "DWH_KORR"

    print("----------------- Parameter -----------------")
    print(f"Jobkennung     : {job_kennung}")
    print(f"DB-Skript      : {l_DBskript}")
    print("---------------------------------------------")

    dw_eintrags_nr = dwmsg_ermittle_nr()
    log_datei = dwmsg_logdateiname(job_kennung, dw_eintrags_nr)
    dwmsg_erzeuge_eintrag(dw_eintrags_nr, job_kennung, f"{sys.argv[0]}_{l_DBskript}", log_datei)

    print("----------------- Job -----------------------")
    print(f"Job-Nr    : '{dw_eintrags_nr}'")
    print(f"Logdatei  : '{log_datei}'")
    print("---------------------------------------------")

    try:
        starte_sql_skript_bq(dw_eintrags_nr, l_DBskript, p_sqlpar, log_datei)
    except Exception as e:
        handle_error_trap("FEHLER", log_datei, p_verbose, dw_eintrags_nr)
        return 1
    except KeyboardInterrupt:
        handle_error_trap("OSFEHLER", log_datei, p_verbose, dw_eintrags_nr)
        return 1

    dwmsg_setze_status_ok(dw_eintrags_nr, log_datei)
    print("Die Abarbeitung des Rahmenskriptes ist ohne erkennbare Fehler beendet")
    return 0

if __name__ == "__main__":
    sys.exit(main())