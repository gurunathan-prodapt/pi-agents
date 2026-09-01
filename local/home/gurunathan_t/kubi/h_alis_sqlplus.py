#!/usr/bin/env python3
import os
import sys
import subprocess
from pathlib import Path
import argparse

# Module metadata constants
MODUL_NAME = "alis_sqlplus"
MODUL_VERSION = "V1.1.3"

# # REVIEW-STRUCT: launcher [DWMSG_MeldeFehler] invoked — internal behaviour not available in this extraction;
# confirm logging, error propagation, and credential handling before finalizing the conversion
def dwmsg_melde_fehler(eintragsnr: str, severity: str, error_code: int, message: str) -> None:
    try:
        cmd = ["DWMSG_MeldeFehler", str(eintragsnr), severity, str(error_code), message]
        subprocess.run(cmd, check=True)
    except subprocess.CalledProcessError as e:
        print(f"Error calling DWMSG_MeldeFehler: {e}", file=sys.stderr)

def starte_sql_skript(p_eintragsnr: str, p_skript: str, *args: str) -> int:
    """
    Python equivalent of the starteSQLSkript shell function.
    Validates script existence and readability, then executes it.
    """
    # Step 1: Parameter Validation Guard
    if not p_eintragsnr or not p_skript:
        error_msg = f"{MODUL_NAME} {MODUL_VERSION} starteSQLSkript"
        dwmsg_melde_fehler(p_eintragsnr, "E", 196, error_msg)
        return 196

    # Step 2: File Accessibility Validation Guard
    skript_path = Path(p_skript)
    if not skript_path.is_file() or not os.access(skript_path, os.R_OK):
        dwmsg_melde_fehler(p_eintragsnr, "E", 201, str(p_skript))
        return 201

    # Step 3: Informational Logging
    print("Rufe SQL*PLUS auf mit folgenden Einstellungen")
    print(f"Sql*Plus-Skript : {p_skript}")
    print(f"Skript-Parameter: {' '.join(args)}")

    # Step 4: Environment Credentials Fetching
    # # REVIEW: target database platform is confirmed as BIGQUERY. 
    # Standard SQLPlus is Oracle-specific. If migrating fully to BigQuery, 
    # this helper should utilize google.cloud.bigquery.Client to run SQL contents.
    # We preserve the legacy subprocess call structure below for backwards-compatibility.
    dw_orauser = os.environ.get("DW_ORAUSER", "")

    # Step 5: Process Invocation with error-handling isolation (set +e equivalent)
    try:
        # # REVIEW-STRUCT: original launcher call preserved verbatim below — replace with the GCP-native equivalent once the launcher's internal behaviour (logging, error propagation, credential injection) is confirmed
        cmd = ["sqlplus", dw_orauser, f"@{p_skript}"] + list(args)
        
        # stdin=subprocess.DEVNULL matches </dev/null redirection
        result = subprocess.run(cmd, stdin=subprocess.DEVNULL, capture_output=False)
        errcode = result.returncode
    except Exception as e:
        print(f"Exception encountered during sqlplus execution: {e}", file=sys.stderr)
        errcode = 1

    # Step 6: Return the exit status code (mimicking return $errcode)
    return errcode

def main() -> int:
    parser = argparse.ArgumentParser(description="Run SQL*Plus script with helper validations")
    parser.add_argument("p_eintragsnr", help="Error entry number")
    parser.add_argument("p_skript", help="Path to the SQL script to be executed")
    parser.add_argument("script_args", nargs="*", help="Optional arguments passed to the SQL script")
    
    args = parser.parse_args()
    
    rc = starte_sql_skript(args.p_eintragsnr, args.p_skript, *args.script_args)
    return rc

if __name__ == "__main__":
    sys.exit(main())