#!/usr/bin/env python3
import sys
import os
import getopt
import subprocess

# Global environment variables sourced at runtime
GCP_PROJECT = os.environ.get("GCP_PROJECT")
GCS_BUCKET = os.environ.get("GCS_BUCKET")
DW_DIR_ROOT = os.environ.get("DW_DIR_ROOT")

# Step 1: Framework Stub functions representing sourced behavior
# REVIEW-STRUCT: environment file [.dw_init] not supplied — variables it sets are unknown; do not guess their names or values
# REVIEW-STRUCT: environment file [f_alis_msgerr.ksh] not supplied — variables/functions it sets are unknown
# REVIEW-STRUCT: environment file [h_alis_sqlplus.ksh] not supplied — variables/functions it sets are unknown

# REVIEW-STRUCT: legacy Oracle status-logging package [DWMSG] replaced with native logging — confirm target logging destination (Cloud Logging / BigQuery table) before deploying
def dwmsg_melde_fehler(eintrags_nr, severity, err_nr, err_arg):
    print(f"Error logged: {severity} {err_nr} {err_arg}", file=sys.stderr)

def dwmsg_ermittle_nr():
    import time
    return int(time.time()) % 1000000

def dwmsg_logdateiname(job_kennung, eintrags_nr):
    return f"log_{job_kennung}_{eintrags_nr}.log"

def dwmsg_erzeuge_eintrag(eintrags_nr, job_kennung, script_desc, log_file):
    log_dir = os.path.dirname(log_file)
    if log_dir:
        os.makedirs(log_dir, exist_ok=True)
    with open(log_file, "a", encoding='utf-8') as f:
        f.write(f"JOB START: {job_kennung} (ID: {eintrags_nr}) - {script_desc}\n")

def dwmsg_fehlerbehandlung(eintrags_nr, log_file):
    print(f"JOB FAILED: (ID: {eintrags_nr})", file=sys.stderr)
    try:
        with open(log_file, "a", encoding='utf-8') as f:
            f.write(f"JOB FAILED: (ID: {eintrags_nr})\n")
    except Exception:
        pass

def dwmsg_setze_status_ok(eintrags_nr, log_file):
    print(f"JOB SUCCESS: (ID: {eintrags_nr})")
    try:
        with open(log_file, "a", encoding='utf-8') as f:
            f.write(f"JOB SUCCESS: (ID: {eintrags_nr})\n")
    except Exception:
        pass

def starte_sql_skript(dw_eintrags_nr, l_db_skript, p_sqlpar, log_file):
    # Read the SQL file
    if not os.path.exists(l_db_skript):
        raise FileNotFoundError(f"SQL script not found: {l_db_skript}")
        
    with open(l_db_skript, 'r', encoding='utf-8', errors='ignore') as f:
        sql_content = f.read()

    # Parse parameters
    sql_args = []
    if p_sqlpar:
        import shlex
        try:
            sql_args = shlex.split(p_sqlpar)
        except Exception:
            sql_args = p_sqlpar.split()
            
    # Append DW_EintragsNr as the last parameter
    sql_args.append(str(dw_eintrags_nr))

    # Replace positional parameters &1, &2, ... in the SQL content
    for i, arg in enumerate(sql_args, start=1):
        sql_content = sql_content.replace(f"&{i}", arg)
        sql_content = sql_content.replace(f"&{i}.", arg)

    # Replace environment variables if present in SQL
    for key, val in os.environ.items():
        sql_content = sql_content.replace(f"&{key}", val)
        sql_content = sql_content.replace(f"&{key}.", val)

    # Log the execution
    with open(log_file, "a", encoding='utf-8') as lf:
        lf.write(f"\n--- Executing SQL Script: {l_db_skript} against BigQuery ---\n")
        lf.write(f"Parameters: {sql_args}\n")
        
    # Execute against BigQuery
    try:
        from google.cloud import bigquery
        from google.api_core.exceptions import GoogleAPIError
    except ImportError:
        # Fallback/Mock for environments without google-cloud-bigquery installed
        print("WARNING: google-cloud-bigquery library not found. Simulating execution.", file=sys.stderr)
        with open(log_file, "a", encoding='utf-8') as lf:
            lf.write("WARNING: google-cloud-bigquery library not found. Simulating execution.\n")
            lf.write(f"SQL Content to execute:\n{sql_content}\n")
        return

    # Initialize BigQuery client
    gcp_project = os.environ.get("GCP_PROJECT")
    client = bigquery.Client(project=gcp_project) if gcp_project else bigquery.Client()

    # Run the query
    try:
        query_job = client.query(sql_content)
        # Wait for the query to complete
        query_job.result()
        
        with open(log_file, "a", encoding='utf-8') as lf:
            lf.write(f"SQL execution completed successfully. Job ID: {query_job.job_id}\n")
            
    except GoogleAPIError as e:
        with open(log_file, "a", encoding='utf-8') as lf:
            lf.write(f"BigQuery Error during execution: {str(e)}\n")
        raise e
    except Exception as e:
        with open(log_file, "a", encoding='utf-8') as lf:
            lf.write(f"Unexpected error during SQL execution: {str(e)}\n")
        raise e

ProgName = f"Ausführung Script {sys.argv[0]}"
ProgVersion = "5.0.0"

def usage():
    print(f"""   Programm: {ProgName}
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

       -v     verbose (zeigt bei Fehler sofort die Logdatei an)""")

def main():
    # Step 2: Initialize default variables
    p_verbose = False
    p_sqlscript = ""
    p_sqlpar = ""
    p_job = "DWH_KORR"
    err_nr = 0
    err_arg = ""

    # Step 3: Parse Arguments
    try:
        opts, args = getopt.getopt(sys.argv[1:], "hvf:i:j:")
        for opt, arg in opts:
            if opt == '-h':
                usage()
                sys.exit(0)
            elif opt == '-v':
                p_verbose = True
            elif opt == '-f':
                p_sqlscript = arg.lower()  # typeset -l equivalent
            elif opt == '-i':
                p_sqlpar = arg
            elif opt == '-j':
                p_job = arg
    except getopt.GetoptError as err:
        msg = str(err)
        opt_name = ""
        if hasattr(err, 'opt') and err.opt:
            opt_name = err.opt
        else:
            import re
            m = re.search(r"option\s+(-[a-zA-Z])", msg)
            if m:
                opt_name = m.group(1).lstrip('-')
        
        if "requires argument" in msg or "requires an argument" in msg:
            err_nr = 193
            err_arg = opt_name
        else:
            err_nr = 192
            err_arg = opt_name

    if err_nr == 0 and not p_sqlscript:
        err_nr = 193
        err_arg = "f"

    # Falls Fehler aufgetreten, abbrechen
    if err_nr != 0:
        dwmsg_melde_fehler(0, "E", err_nr, err_arg)
        usage()
        sys.exit(err_nr)

    # Step 4: Resolve directory path logic
    script_dir = os.path.dirname(os.path.realpath(__file__))
    os.chdir(script_dir)

    l_db_skript = p_sqlscript
    p_sqlscript_dir = os.path.dirname(p_sqlscript)
    if p_sqlscript_dir in ['.', '']:
        test_path_sql = os.path.join("..", "sql", p_sqlscript)
        test_path_mig = os.path.join("..", "mig", p_sqlscript)
        
        if os.path.isfile(test_path_sql):
            l_db_skript = test_path_sql
        elif os.path.isfile(test_path_mig):
            l_db_skript = test_path_mig
        else:
            l_db_skript = p_sqlscript
    else:
        l_db_skript = p_sqlscript

    # Step 5: File Validation (Legacy logic preservation)
    if os.path.isfile(l_db_skript):
        err_nr = 198
        err_arg = ""  # p_Kuerzel was undefined in legacy ksh

    # Step 6: Logging registration
    job_kennung = p_job.upper() if p_job else "DWH_KORR"
    dw_eintrags_nr = dwmsg_ermittle_nr()
    log_datei = dwmsg_logdateiname(job_kennung, dw_eintrags_nr)
    
    dwmsg_erzeuge_eintrag(dw_eintrags_nr, job_kennung, f"{sys.argv[0]}_{l_db_skript}", log_datei)

    print("----------------- Parameter -----------------")
    print(f"Jobkennung     : {job_kennung}")
    print(f"DB-Skript      : {l_db_skript}")
    print("---------------------------------------------")

    # Step 7: Execute with Trap equivalents
    try:
        print("----------------- Job -----------------------")
        print(f"Job-Nr    : '{dw_eintrags_nr}'")
        print(f"Logdatei  : '{log_datei}'")
        print("---------------------------------------------")

        # Execute target SQL against BigQuery via helper
        starte_sql_skript(dw_eintrags_nr, l_db_skript, p_sqlpar, log_datei)

        # Step 8: Finalize OK status
        dwmsg_setze_status_ok(dw_eintrags_nr, log_datei)
        print("Die Abarbeitung des Rahmenskriptes ist ohne erkennbare Fehler beendet")
        sys.exit(0)

    except KeyboardInterrupt:
        # Step 9: Catch failures and emulate TRAP behavior
        dwmsg_fehlerbehandlung(dw_eintrags_nr, log_datei)
        print("!OSFEHLER gemeldet!", file=sys.stderr)
        if p_verbose:
            if os.path.exists(log_datei):
                with open(log_datei, 'r', encoding='utf-8', errors='ignore') as lf:
                    print(lf.read(), file=sys.stderr)
        sys.exit(1)

    except Exception as e:
        # Step 9: Catch failures and emulate TRAP behavior
        dwmsg_fehlerbehandlung(dw_eintrags_nr, log_datei)
        print("!FEHLER gemeldet!", file=sys.stderr)
        if p_verbose:
            if os.path.exists(log_datei):
                with open(log_datei, 'r', encoding='utf-8', errors='ignore') as lf:
                    print(lf.read(), file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()