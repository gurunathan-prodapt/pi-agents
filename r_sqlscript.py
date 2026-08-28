#!/usr/bin/env python3
import sys
import os
import argparse
import subprocess
import shutil
import datetime
import re
from google.cloud import bigquery
from google.api_core.exceptions import GoogleAPIError

# REVIEW-STRUCT: Sourced files [.dw_init, f_alis_msgerr.ksh, h_alis_sqlplus.ksh] are not supplied — variables and functions they define are unknown.
# REVIEW-STRUCT: legacy Oracle status-logging package [DWMSG_MeldeFehler, DWMSG_ErmittleNr, DWMSG_Logdateiname, DWMSG_ErzeugeEintrag, DWMSG_SetzeStatusOK, DWMSG_Fehlerbehandlung] replaced with native logging — confirm target logging destination (Cloud Logging / BigQuery table) before deploying

# Helper Functions representing replaced status-logging logic (DWMSG_*)
def get_eintrags_nr():
    """Simulates DWMSG_ErmittleNr by reading from env or generating a timestamp-based ID."""
    if "DW_EintragsNr" in os.environ and os.environ["DW_EintragsNr"]:
        return os.environ["DW_EintragsNr"]
    return datetime.datetime.now().strftime("%Y%m%d%H%M%S")

def get_log_dateiname(job_kennung, eintrags_nr):
    """Simulates DWMSG_Logdateiname by constructing a standardized local log path."""
    log_dir = os.environ.get("DW_LOG_DIR", ".")
    return os.path.join(log_dir, f"{job_kennung}_{eintrags_nr}.log")

def erzeuge_eintrag(log_file, eintrags_nr, job_kennung, program, args_str=None):
    """Simulates DWMSG_ErzeugeEintrag by appending metadata to the log file."""
    timestamp = datetime.datetime.now().isoformat()
    with open(log_file, "a", encoding="utf-8") as f:
        f.write(f"[{timestamp}] ENTRY CREATED: Job={job_kennung}, Entry={eintrags_nr}, Program={program}\n")
        if args_str:
            f.write(f"[{timestamp}] Parameters: {args_str}\n")

def setze_status_ok(log_file, eintrags_nr):
    """Simulates DWMSG_SetzeStatusOK by appending success metadata to the log file."""
    timestamp = datetime.datetime.now().isoformat()
    with open(log_file, "a", encoding="utf-8") as f:
        f.write(f"[{timestamp}] STATUS OK: Entry={eintrags_nr}\n")

def fehlerbehandlung(log_file, eintrags_nr):
    """Simulates DWMSG_Fehlerbehandlung by appending failure metadata to the log file."""
    timestamp = datetime.datetime.now().isoformat()
    with open(log_file, "a", encoding="utf-8") as f:
        f.write(f"[{timestamp}] ERROR OCCURRED: Entry={eintrags_nr}\n")

def melde_fehler(eintrags_nr, severity, err_nr, err_arg):
    """Simulates DWMSG_MeldeFehler by printing standardized error diagnostics to stderr."""
    print(f"ERROR: Entry={eintrags_nr}, Severity={severity}, Error Number={err_nr}, Argument={err_arg}", file=sys.stderr)


def run_sql_on_bigquery(sql_file_path, sql_args, log_file):
    """
    Executes the content of the target SQL script directly on BigQuery.
    Applies environment-specific variable substitutions (GCP_PROJECT and BQ_DATASET) 
    as well as positional arguments inside the SQL body before execution.
    """
    gcp_project = os.environ.get("GCP_PROJECT", "")
    bq_dataset = os.environ.get("BQ_DATASET", "")

    if not gcp_project:
        raise SystemExit("GCP_PROJECT must be set by the calling Airflow task")
    if not bq_dataset:
        raise SystemExit("BQ_DATASET must be set by the calling Airflow task")

    if not os.path.isfile(sql_file_path):
        raise FileNotFoundError(f"SQL file not found at path: {sql_file_path}")

    with open(sql_file_path, "r", encoding="utf-8") as f:
        sql_content = f.read()

    # Apply required placeholder replacements as specified in the RETRY FIX instructions
    sql_content = sql_content.replace("GCP_PROJECT", gcp_project)
    sql_content = sql_content.replace("BQ_DATASET", bq_dataset)

    # Replace positional placeholders (e.g., &1, &2, etc.) to mimic legacy script parameter ingestion
    for i, arg in enumerate(sql_args, start=1):
        val = str(arg) if arg is not None else ""
        sql_content = re.sub(rf"&{i}\b\.?", val, sql_content)
        sql_content = re.sub(rf"&{{{i}}}\.?", val, sql_content)

    with open(log_file, "a", encoding="utf-8") as lf:
        lf.write(f"--- Executing SQL Script: {sql_file_path} on BigQuery ---\n")
        lf.write(f"Parameters applied: {sql_args}\n")

    client = bigquery.Client(project=gcp_project)
    try:
        # REVIEW: PL/SQL block / Oracle package call requires manual rewrite as BigQuery-compatible SQL (e.g. explicit TRUNCATE TABLE `project.dataset.table` statements, one per call) — do not attempt an automatic translation
        query_job = client.query(sql_content)
        query_job.result()  # Wait for query to complete
        with open(log_file, "a", encoding="utf-8") as lf:
            lf.write("SQL Execution completed successfully on BigQuery.\n")
    except GoogleAPIError as e:
        with open(log_file, "a", encoding="utf-8") as lf:
            lf.write(f"BigQuery Execution failed: {str(e)}\n")
        raise e


def main():
    # Step 1: Environment Initialization
    # Sourced environment bootstrap logic (handled via Airflow/Composer context in target environment).

    # Step 2: Option Parsing
    parser = argparse.ArgumentParser(description="Ausführung Script r_sqlscript", add_help=False)
    parser.add_argument("-f", dest="p_sqlscript", required=False, default="")
    parser.add_argument("-i", dest="p_sqlpar", required=False, default="")
    parser.add_argument("-k", dest="p_sqlpar2", required=False, default="")
    parser.add_argument("-v", dest="p_Verbose", action="store_true", default=False)
    parser.add_argument("-j", dest="p_Job", required=False, default="")
    parser.add_argument("-m", dest="p_Modus", required=False, default="")
    parser.add_argument("-h", action="help", help="Zeigt diese Hilfeseite an")

    err_nr = 0
    err_arg = ""

    try:
        args = parser.parse_args()
    except Exception as e:
        # Step 3: Input Validation & Error handling for parsing failure
        err_nr = 192  # Parameter unbekannt / invalid args
        err_arg = str(e)
        melde_fehler("0", "E", err_nr, err_arg)
        sys.exit(err_nr)

    # typeset -l p_sqlscript -> lowercase
    p_sqlscript = args.p_sqlscript.lower() if args.p_sqlscript else ""
    p_sqlpar = args.p_sqlpar
    p_sqlpar2 = args.p_sqlpar2
    p_Verbose = args.p_Verbose
    p_Job = args.p_Job
    p_Modus = args.p_Modus

    # Step 4: SQL Path Resolution
    script_dir = os.path.dirname(os.path.realpath(__file__))
    if script_dir:
        os.chdir(script_dir)

    l_DBskript = ""
    p_sqlscript_dir = os.path.dirname(p_sqlscript)
    if p_sqlscript_dir in ["", "."]:
        l_DBskript = os.path.join("../sql", p_sqlscript)
        if not os.path.isfile(l_DBskript):
            l_DBskript = p_sqlscript
    else:
        l_DBskript = p_sqlscript

    # NOTE: parameter accepted for interface compatibility; unused in original script logic
    p_Kuerzel = "" 
    if os.path.isfile(l_DBskript):
        err_nr = 198  # Parameterwert unbekannt
        err_arg = p_Kuerzel

    # Step 5: Job Identification & Normalization (typeset -u JobKennung)
    JobKennung = p_Job.upper() if p_Job else "DWH_KORR"

    print("----------------- Parameter -----------------")
    print(f"Jobkennung     : {JobKennung}")
    print(f"DB-Skript      : {l_DBskript}")
    print("---------------------------------------------")

    # Step 6: Logging & Job Entry Allocation
    DW_EintragsNr = get_eintrags_nr()
    os.environ["DW_EintragsNr"] = DW_EintragsNr

    LogDatei = get_log_dateiname(JobKennung, DW_EintragsNr)

    # Create initial log entry
    if p_Modus == "v2":
        all_args = " ".join(sys.argv[1:])
        erzeuge_eintrag(LogDatei, DW_EintragsNr, JobKennung, sys.argv[0], all_args)
    else:
        program_identifier = f"{sys.argv[0]}_{l_DBskript}"
        erzeuge_eintrag(LogDatei, DW_EintragsNr, JobKennung, program_identifier)

    # Step 7: Trap Registration (via try-except blocks)
    def run_cleanup_and_exit(failed=False, error_msg=""):
        fehlerbehandlung(LogDatei, DW_EintragsNr)
        with open(LogDatei, "a", encoding="utf-8") as lf:
            if failed:
                lf.write("!FEHLER gemeldet!\n")
            else:
                lf.write("!OSFEHLER gemeldet!\n")
            if error_msg:
                lf.write(f"Details: {error_msg}\n")

        if p_Verbose:
            print("--- Verbose Log File Dump ---", file=sys.stderr)
            if os.path.isfile(LogDatei):
                with open(LogDatei, "r", encoding="utf-8") as lf:
                    print(lf.read(), file=sys.stderr)
        sys.exit(1)

    try:
        print("----------------- Job -----------------------")
        print(f"Job-Nr    : '{DW_EintragsNr}'")
        print(f"Logdatei  : '{LogDatei}'")
        print("---------------------------------------------")

        # Step 8: SQL Execution (starteSQLSkript)
        if p_Modus == "v2":
            # starteSQLSkript $DW_EintragsNr $l_DBskript $DW_EintragsNr $p_sqlpar $p_sqlpar2
            sql_args = [DW_EintragsNr, p_sqlpar, p_sqlpar2]
        else:
            # starteSQLSkript $DW_EintragsNr $l_DBskript $p_sqlpar $DW_EintragsNr
            sql_args = [p_sqlpar, DW_EintragsNr]

        # REVIEW-STRUCT: connection parameters inferred from a cross-referenced .ksh file's declared environment parameters — confirm these exact env var names are set in this job's actual runtime environment before deploying
        run_sql_on_bigquery(l_DBskript, sql_args, LogDatei)

        # Step 9: Framework Status Update (OK)
        setze_status_ok(LogDatei, DW_EintragsNr)

    except KeyboardInterrupt:
        run_cleanup_and_exit(failed=False, error_msg="Job execution interrupted by SIGINT")
    except Exception as e:
        run_cleanup_and_exit(failed=True, error_msg=str(e))

    # Step 10: Cleanup & normal completion message
    print("Die Abarbeitung des Rahmenskriptes ist ohne erkennbare Fehler beendet")
    return 0


if __name__ == "__main__":
    sys.exit(main())