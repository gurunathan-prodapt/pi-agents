#!/usr/bin/env python3
import os
import sys
import subprocess
import argparse

# Module metadata
MODUL_NAME = "alis_sqlplus"
MODUL_VERSION = "V1.1.3"

# REVIEW-STRUCT: external function DWMSG_MeldeFehler not supplied in extraction.
# This placeholder represents its expected interface and logs to stderr.
def dwmsg_melde_fehler(eintragsnr, severity, error_code, message):
    print(f"ERROR LOG: Entry={eintragsnr}, Severity={severity}, Code={error_code}, Msg={message}", file=sys.stderr)

def starte_sql_skript(p_eintragsnr, p_skript, *args):
    """
    Starts an SQL*Plus script after performing validation checks.
    """
    dw_orauser = os.environ.get("DW_ORAUSER")
    if not dw_orauser:
        raise SystemExit("DW_ORAUSER must be set by the calling Airflow task")

    # Step 2: Validate input parameters
    # REVIEW: Parameter validation "Modul_Name Modul_Version starteSQLSkript" corrected from original KSH typo Modul_Name.
    if not p_eintragsnr or not p_skript:
        dwmsg_melde_fehler(
            p_eintragsnr,
            "E",
            196,
            f"{MODUL_NAME} {MODUL_VERSION} starteSQLSkript"
        )
        return 196

    # Step 3: Validate file readability
    if not os.path.isfile(p_skript) or not os.access(p_skript, os.R_OK):
        dwmsg_melde_fehler(p_eintragsnr, "E", 201, p_skript)
        return 201

    # Step 4: Log invocation settings
    print("Rufe SQL*PLUS auf mit folgenden Einstellungen")
    print(f"Sql*Plus-Skript : {p_skript}")
    print(f"Skript-Parameter: {' '.join(str(x) for x in args)}")

    # Step 5: Prepare connection string and command
    # Formulate command arguments (equivalent to sqlplus ${DW_ORAUSER} @$p_Skript $*)
    cmd = ["sqlplus", dw_orauser, f"@{p_skript}"] + list(args)

    # Step 6: Execute SQL*Plus and capture exit status code safely (replicates set +e)
    try:
        # stdin=subprocess.DEVNULL mimics </dev/null
        result = subprocess.run(
            cmd,
            stdin=subprocess.DEVNULL,
            capture_output=False, # Output goes straight to stdout/stderr
            check=False          # Replicates set +e / return $errcode behavior
        )
        errcode = result.returncode
    except Exception as e:
        # In case the executable 'sqlplus' itself cannot be found or run
        print(f"Execution failed: {str(e)}", file=sys.stderr)
        errcode = 1  # Standard failure fallback

    # Step 7: Return execution exit code
    return errcode

def main(argv=None):
    if argv is None:
        argv = sys.argv[1:]
    
    parser = argparse.ArgumentParser(description="Run SQL*Plus script wrapper")
    parser.add_argument("eintragsnr", help="Fehlereintragsnummer")
    parser.add_argument("skript", help="Name of the script to start")
    parser.add_argument("script_args", nargs="*", help="Optional parameters for the SQL script")
    
    args = parser.parse_args(argv)
    
    try:
        return starte_sql_skript(args.eintragsnr, args.skript, *args.script_args)
    except SystemExit as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1

if __name__ == "__main__":
    sys.exit(main())