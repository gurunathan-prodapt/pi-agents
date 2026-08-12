#!/usr/bin/env python3
import os
import sys
import subprocess
import argparse

# Global module variables
MODUL_NAME = "alis_sqlplus"
MODUL_VERSION = "V1.1.3"

# Step 1: Define error logging helper
def dwmsg_melde_fehler(eintragsnr: str, msg_type: str, code: int, details: str):
    # REVIEW-STRUCT: original launcher call preserved verbatim below — replace with the GCP-native equivalent once the launcher's internal behaviour (logging, error propagation, credential injection) is confirmed
    try:
        subprocess.run(
            ["DWMSG_MeldeFehler", str(eintragsnr), msg_type, str(code), details],
            check=True
        )
    except subprocess.CalledProcessError as e:
        print(f"ERROR: DWMSG_MeldeFehler failed with exit code {e.returncode}", file=sys.stderr)
    except FileNotFoundError:
        print("ERROR: DWMSG_MeldeFehler command not found on PATH", file=sys.stderr)

# Step 2: Define helper function to start SQL*Plus scripts
def starte_sql_skript(p_eintragsnr: str, p_skript: str, *args) -> int:
    """
    Startet ein SQL-Pluskript.
    Checks for parameters, verifies readability of the script file, and executes sqlplus.
    """
    # REVIEW: original script referenced 'Modul_Name' in error string which was undeclared (only 'ModulName' was defined). Using MODUL_NAME instead.
    modul_name_resolved = MODUL_NAME

    # Step 3: Validate input parameters
    if not p_eintragsnr or not p_skript:
        dwmsg_melde_fehler(
            p_eintragsnr,
            "E",
            196,
            f"{modul_name_resolved} {MODUL_VERSION} starteSQLSkript"
        )
        return 196

    # Step 4: Validate script file readability
    if not os.path.exists(p_skript) or not os.access(p_skript, os.R_OK):
        dwmsg_melde_fehler(p_eintragsnr, "E", 201, p_skript)
        return 201

    # Step 5: Log execution details
    print("Rufe SQL*PLUS auf mit folgenden Einstellungen")
    print(f"Sql*Plus-Skript : {p_skript}")
    print(f"Skript-Parameter: {' '.join(args)}")

    # Step 6: Identify Oracle Connection String
    dw_orauser = os.environ.get("DW_ORAUSER")
    if not dw_orauser:
        raise SystemExit("DW_ORAUSER must be set by the calling Airflow task")

    # Step 7: Prepare and run SQL*Plus CLI command
    # Equivalent to sqlplus ${DW_ORAUSER} @$p_Skript $* </dev/null
    cmd = ["sqlplus", dw_orauser, f"@{p_skript}"] + list(args)

    # Step 8: Execute subprocess with error trapping (set +e / set -e equivalent)
    try:
        result = subprocess.run(
            cmd,
            stdin=subprocess.DEVNULL,  # </dev/null redirection
            check=False  # Do not raise exception on non-zero exit code to match KSH behavior
        )
        errcode = result.returncode
    except Exception as e:
        print(f"Fehler bei der Ausfuehrung von SQL*Plus: {e}", file=sys.stderr)
        errcode = 1  # Return non-zero failure code

    # Step 9: Return exit status code
    return errcode

def main():
    parser = argparse.ArgumentParser(
        description="Helper script to run SQL*Plus scripts with parameter and readability checks."
    )
    parser.add_argument("entry_nr", help="Fehlereintragsnummer")
    parser.add_argument("script", help="Path to SQL script to run")
    parser.add_argument("script_args", nargs="*", help="Optional arguments for the SQL script")

    args = parser.parse_args()

    # Run script logic and return exit code
    return starte_sql_skript(args.entry_nr, args.script, *args.script_args)

if __name__ == "__main__":
    sys.exit(main())