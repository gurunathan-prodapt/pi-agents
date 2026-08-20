#!/usr/bin/env python3
import os
import sys
import argparse

# Step 1: Import core dependencies and initialize setup
# REVIEW-STRUCT: environment file [.dw_init] not supplied — variables it sets are unknown
# REVIEW-STRUCT: environment file [f_alis_msgerr.ksh] not supplied — functions it defines are unknown
# REVIEW-STRUCT: environment file [h_alis_sqlplus.ksh] not supplied — defines starteSQLSkript

ProgName = f"Ausführung Script {sys.argv[0]}"
ProgVersion = "5.0.0"

# Verify required global environment variables are set
dw_dir_root = os.environ.get("DW_DIR_ROOT")
if not dw_dir_root:
    raise SystemExit("DW_DIR_ROOT must be set by the calling environment")

gcp_project = os.environ.get("GCP_PROJECT")
if not gcp_project:
    raise SystemExit("GCP_PROJECT must be set by the calling environment")

# Mock/Stubs for sourced functions whose definitions are not supplied:
def DWMSG_MeldeFehler(eintrags_nr, level, err_nr, err_arg):
    # Stub for the legacy DWMSG_MeldeFehler shell function
    print(f"ERROR: {level} {err_nr} {err_arg} (Entry: {eintrags_nr})", file=sys.stderr)

def DWMSG_ErmittleNr():
    # Stub for sequence generator DWMSG_ErmittleNr
    import random
    return os.environ.get("DW_EINTRAEGS_NR") or str(random.randint(100000, 999999))

def DWMSG_Logdateiname(job_kennung, eintrags_nr):
    # Stub for DWMSG_Logdateiname
    log_dir = os.environ.get("DW_LOG_DIR", "/tmp")
    return os.path.join(log_dir, f"{job_kennung}_{eintrags_nr}.log")

def DWMSG_ErzeugeEintrag(eintrags_nr, job_kennung, program, log_file):
    # Stub for DWMSG_ErzeugeEintrag
    with open(log_file, "a", encoding="utf-8") as f:
        f.write(f"ENTRY: {eintrags_nr} | JOB: {job_kennung} | PROGRAM: {program}\n")

def DWMSG_Fehlerbehandlung(eintrags_nr):
    # Stub for DWMSG_Fehlerbehandlung
    print(f"Error handling invoked for entry: {eintrags_nr}", file=sys.stderr)

def DWMSG_SetzeStatusOK(eintrags_nr):
    # Stub for DWMSG_SetzeStatusOK
    print(f"Status set to OK for entry: {eintrags_nr}")

def starteSQLSkript(eintrags_nr, db_script, sql_par, run_id, log_file_path):
    # REVIEW-STRUCT: connection parameters inferred from a cross-referenced .ksh file's declared environment parameters — confirm these exact env var names are set in this job's actual runtime environment before deploying
    
    with open(db_script, 'r', encoding='utf-8') as f:
        sql_text = f.read()
        
    # Standard GCP BigQuery client
    from google.cloud import bigquery
    client = bigquery.Client()
    
    # Configure query parameters based on known requirements
    query_params = []
    
    # Pass eintrags_nr as param_eintrags_nr
    try:
        val_int = int(eintrags_nr)
        query_params.append(bigquery.ScalarQueryParameter("param_eintrags_nr", "INT64", val_int))
    except ValueError:
        query_params.append(bigquery.ScalarQueryParameter("param_eintrags_nr", "STRING", str(eintrags_nr)))
        
    # Pass sql_par as param_monats_id (as expected by migrated BQ scripts)
    if sql_par:
        query_params.append(bigquery.ScalarQueryParameter("param_monats_id", "STRING", str(sql_par)))
        
    job_config = bigquery.QueryJobConfig(query_parameters=query_params)
    
    # Write execution log
    with open(log_file_path, "a", encoding='utf-8') as log_file:
        log_file.write(f"Executing SQL from {db_script} on BigQuery...\n")
        try:
            query_job = client.query(sql_text, job_config=job_config)
            query_job.result() # Wait for job to complete
            log_file.write("SQL execution completed successfully.\n")
        except Exception as e:
            log_file.write(f"SQL execution failed: {str(e)}\n")
            raise e

# Custom Argument Parser to emulate legacy getopts error codes
class CustomArgumentParser(argparse.ArgumentParser):
    def error(self, message):
        raise argparse.ArgumentError(None, message)

def main():
    # Step 2 & 3: Define and parse command-line arguments (getopts replacement)
    parser = CustomArgumentParser(description=ProgName, add_help=False)
    parser.add_argument('-f', required=True, help="SQL-Script")
    parser.add_argument('-i', default='', help="SQL parameters")
    parser.add_argument('-j', default='DWH_KORR', help="Job identifier")
    parser.add_argument('-v', action='store_true', help="Verbose mode")
    parser.add_argument('-h', action='help', help="Show help")

    err_nr = 0
    err_arg = ""
    p_sqlscript = ""
    p_sqlpar = ""
    p_Job = "DWH_KORR"
    p_Verbose = 0

    try:
        args = parser.parse_args()
        p_sqlscript = args.f.lower()  # typeset -l p_sqlscript
        p_sqlpar = args.i
        p_Job = args.j
        p_Verbose = 1 if args.v else 0
    except argparse.ArgumentError as e:
        if "required" in str(e) or "empty" in str(e) or "missing" in str(e):
            err_nr = 193  # Notwendiges Argument fehlt
        else:
            err_nr = 192  # Parameter unbekannt
        err_arg = str(e)
    except Exception as e:
        err_nr = 192
        err_arg = str(e)

    # Step 4: Validate Arguments
    if err_nr != 0:
        DWMSG_MeldeFehler(0, "E", err_nr, err_arg)
        parser.print_help()
        sys.exit(err_nr)

    # Step 5: Path Resolution
    # Change directory to parent directory of this wrapper script (dirname $0)
    original_dir = os.getcwd()
    script_dir = os.path.dirname(os.path.abspath(sys.argv[0]))
    os.chdir(script_dir)

    p_sqlscript_dir = os.path.dirname(p_sqlscript)
    if p_sqlscript_dir == '.' or p_sqlscript_dir == '':
        l_DBskript = os.path.join('..', 'sql', p_sqlscript)
        if not os.path.isfile(l_DBskript):
            l_DBskript = os.path.join('..', 'mig', p_sqlscript)
        if not os.path.isfile(l_DBskript):
            l_DBskript = p_sqlscript
    else:
        l_DBskript = p_sqlscript

    # Step 6: Validate File Presence (preserving legacy ksh behavior)
    # REVIEW: Legacy code contains: if [ -f "$l_DBskript" ] then ErrNr=198 ...
    # This logic erroneously triggers when the file is present. We preserve the assignment behavior.
    # p_Kuerzel is referenced but was never initialized in the source script.
    p_Kuerzel = os.environ.get("p_Kuerzel")
    if os.path.isfile(l_DBskript):
        # REVIEW: Legacy bug - setting ErrNr when file exists, but it was not checked/acted upon afterwards.
        err_nr = 198
        err_arg = p_Kuerzel if p_Kuerzel else ""

    # Step 7: Establish Job ID
    JobKennung = (p_Job or "DWH_KORR").upper()

    print("----------------- Parameter -----------------")
    print(f"Jobkennung     : {JobKennung}")
    print(f"DB-Skript      : {l_DBskript}")
    print("---------------------------------------------")

    # Step 8: Register Execution Context
    DW_EintragsNr = DWMSG_ErmittleNr()
    LogDatei = DWMSG_Logdateiname(JobKennung, DW_EintragsNr)

    # Ensure logging registry entry
    DWMSG_ErzeugeEintrag(DW_EintragsNr, JobKennung, f"{sys.argv[0]}_{l_DBskript}", LogDatei)

    # Step 9: Trap Setups (Implemented via try-except-finally block in main execution)
    try:
        print("----------------- Job -----------------------")
        print(f"Job-Nr    : '{DW_EintragsNr}'")
        print(f"Logdatei  : '{LogDatei}'")
        print("---------------------------------------------")
        
        # Step 10: SQL Script Invocation
        starteSQLSkript(DW_EintragsNr, l_DBskript, p_sqlpar, DW_EintragsNr, LogDatei)
        
        # Step 11: Post-execution Success Logging
        DWMSG_SetzeStatusOK(DW_EintragsNr)
        print("Die Abarbeitung des Rahmenskriptes ist ohne erkennbare Fehler beendet")
        sys.exit(0)

    except KeyboardInterrupt:
        # INT trap behavior
        with open(LogDatei, "a", encoding="utf-8") as lf:
            lf.write("!OSFEHLER gemeldet!\n")
        DWMSG_Fehlerbehandlung(DW_EintragsNr)
        if p_Verbose != 0:
            try:
                with open(LogDatei, "r", encoding="utf-8") as lf:
                    print(lf.read())
            except Exception:
                pass
        sys.exit(1)
    except Exception as e:
        # ERR trap behavior
        with open(LogDatei, "a", encoding="utf-8") as lf:
            lf.write(f"!FEHLER gemeldet!: {str(e)}\n")
        DWMSG_Fehlerbehandlung(DW_EintragsNr)
        if p_Verbose != 0:
            try:
                with open(LogDatei, "r", encoding="utf-8") as lf:
                    print(lf.read())
            except Exception:
                pass
        sys.exit(1)

if __name__ == "__main__":
    sys.exit(main())