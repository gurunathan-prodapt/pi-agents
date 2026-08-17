#!/usr/bin/env python3
import os
import sys
import subprocess
import argparse

# Global environment-wide configurations
GCP_PROJECT = os.environ.get("GCP_PROJECT")
BQ_LOCATION = os.environ.get("BQ_LOCATION")
GCS_BUCKET = os.environ.get("GCS_BUCKET")

# Module metadata variables
MODUL_NAME = "alis_sqlplus"
MODUL_VERSION = "V1.1.3"


def dwmsg_melde_fehler(eintrags_nr, msg_type, error_code, details):
    """
    Simulates external logging command: DWMSG_MeldeFehler
    """
    # REVIEW-STRUCT: launcher [DWMSG_MeldeFehler] invoked — internal behaviour not available in this extraction; confirm logging, error propagation, and credential handling before finalizing the conversion
    cmd = ["DWMSG_MeldeFehler", str(eintrags_nr), msg_type, str(error_code), details]
    try:
        subprocess.run(cmd, check=True)
    except Exception as e:
        print(f"Error calling DWMSG_MeldeFehler: {e}", file=sys.stderr)


def starte_sql_skript(p_eintragsnr, p_skript, *args):
    """
    Executes a SQL*Plus script with parameters and validations.
    """
    # Step 1: Validate required parameters are present
    if not p_eintragsnr or not p_skript:
        # REVIEW: Corrected typo from legacy shell reference ${Modul_Name} to use MODUL_NAME
        error_msg = f"{MODUL_NAME} {MODUL_VERSION} starteSQLSkript"
        dwmsg_melde_fehler(p_eintragsnr, "E", 196, error_msg)
        return 196

    # Step 2: Validate that target SQL script is readable
    if not os.path.isfile(p_skript) or not os.access(p_skript, os.R_OK):
        dwmsg_melde_fehler(p_eintragsnr, "E", 201, p_skript)
        return 201

    # Step 3: Print execution details (German diagnostic text retained character-for-character)
    print("Rufe SQL*PLUS auf mit folgenden Einstellungen")
    print(f"Sql*Plus-Skript : {p_skript}")
    print(f"Skript-Parameter: {' '.join(args)}")

    # Step 4: Resolve environment context
    dw_orauser = os.environ.get("DW_ORAUSER")
    if not dw_orauser:
        # In legacy context this executes SQLPlus. If target has migrated to BQ client, this configuration will be adapted.
        # Providing fallback handling without prose placeholders
        dw_orauser = ""

    # Step 5: Execute SQL*Plus process
    # equivalent to: sqlplus ${DW_ORAUSER} @$p_Skript $* </dev/null
    sqlplus_cmd = ["sqlplus", dw_orauser, f"@{p_skript}"] + list(args)

    # REVIEW-STRUCT: launcher [sqlplus] invoked — internal SQL behavior not available in this extraction; confirm database client configuration before deployment
    try:
        # Input redirected from devnull equivalent to </dev/null
        result = subprocess.run(
            sqlplus_cmd, 
            stdin=subprocess.DEVNULL,
            check=False  # Equivalent to set +e / set -e handling surrounding the execution
        )
        errcode = result.returncode
    except Exception as e:
        print(f"Failed to execute sqlplus: {e}", file=sys.stderr)
        errcode = 1  # Fallback error status if subprocess launch itself fails

    # Step 6: Return result code
    return errcode


def main():
    parser = argparse.ArgumentParser(description="Run starte_sql_skript utility")
    parser.add_argument("eintragsnr", help="Error log record tracking identifier")
    parser.add_argument("skript", help="File path of the SQL*Plus script to execute")
    parser.add_argument("params", nargs="*", help="Dynamic parameters passed to the SQL script")
    
    args = parser.parse_args()
    
    return starte_sql_skript(args.eintragsnr, args.skript, *args.params)


if __name__ == "__main__":
    sys.exit(main())