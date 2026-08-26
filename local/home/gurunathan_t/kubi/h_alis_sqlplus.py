#!/usr/bin/env python3
import os
import sys
import subprocess

MODUL_NAME = "alis_sqlplus"
MODUL_VERSION = "V1.1.3"

# Try to import the logging function from the sibling module as per design document.
try:
    from f_alis_msgerr import dwmsg_melde_fehler
except ImportError:
    # Fallback to external subprocess DWMSG_MeldeFehler if sibling file is not available.
    def dwmsg_melde_fehler(eintragsnr, severity, error_id, message):
        cmd = ["DWMSG_MeldeFehler", str(eintragsnr), severity, str(error_id), message]
        try:
            subprocess.run(cmd, check=True)
        except FileNotFoundError:
            print(f"Error: {cmd[0]} not found in PATH.", file=sys.stderr)
        except subprocess.CalledProcessError as e:
            print(f"Error logging failed with status {e.returncode}", file=sys.stderr)


def starte_sql_skript(p_eintragsnr, p_skript, *params):
    # Step 3: Validate input parameters
    if not p_eintragsnr or not p_skript:
        msg = f"{MODUL_NAME} {MODUL_VERSION} starteSQLSkript"
        dwmsg_melde_fehler(p_eintragsnr or "", "E", 196, msg)
        return 196

    # Step 4: Verify SQL script file exists and is readable
    if not os.path.isfile(p_skript) or not os.access(p_skript, os.R_OK):
        dwmsg_melde_fehler(p_eintragsnr, "E", 201, p_skript)
        return 201

    # Step 5: Log execution details
    print("Rufe SQL*PLUS auf mit folgenden Einstellungen")
    print(f"Sql*Plus-Skript : {p_skript}")
    print(f"Skript-Parameter: {' '.join(params)}")

    # Step 6: Acquire Oracle connection user from environment
    dw_orauser = os.environ.get("DW_ORAUSER")
    if not dw_orauser:
        raise SystemExit("DW_ORAUSER must be set by the calling Airflow task")

    # Step 7: Execute sqlplus via subprocess (handling set +e equivalent check=False)
    # Passing devnull to stdin to mirror </dev/null
    sqlplus_cmd = ["sqlplus", dw_orauser, f"@{p_skript}"] + list(params)
    
    try:
        result = subprocess.run(
            sqlplus_cmd, 
            stdin=subprocess.DEVNULL, 
            check=False
        )
        errcode = result.returncode
    except Exception as e:
        print(f"Execution of sqlplus failed: {str(e)}", file=sys.stderr)
        errcode = 1  # Fallback error status

    # Step 8: Return execution exit status
    return errcode


def main():
    import argparse
    parser = argparse.ArgumentParser(
        description="Wrapper for executing SQL*Plus scripts with validations."
    )
    # We use nargs='?' to gracefully handle missing arguments and handle them inside starte_sql_skript
    parser.add_argument("p_Eintragsnr", nargs="?", default="", help="Error entry number")
    parser.add_argument("p_Skript", nargs="?", default="", help="Path to SQL script")
    parser.add_argument("params", nargs="*", help="Dynamic parameters passed to SQL*Plus")
    
    args = parser.parse_args()
    
    errcode = starte_sql_skript(args.p_Eintragsnr, args.p_Skript, *args.params)
    return errcode


if __name__ == "__main__":
    sys.exit(main())