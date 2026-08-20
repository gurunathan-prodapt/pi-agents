#!/usr/bin/env python3
import os
import sys
import subprocess
import pathlib
import argparse

# Step 1: Initialize module-level identification variables
ModulName = "alis_sqlplus"
ModulVersion = "V1.1.3"

# Step 2: Define helper function to start SQL*Plus script
def starteSQLSkript(p_Eintragsnr, p_Skript, *args):
    """
    Starts an SQL*Plus script after validating arguments and file readability.
    
    :param p_Eintragsnr: Error entry number for DWMSG_MeldeFehler
    :param p_Skript: Path to the SQL script
    :param args: Additional parameters for the SQL script
    :return: Exit code from SQL*Plus or validation error code
    """
    # Step 3: Validate mandatory arguments
    if not p_Eintragsnr or not p_Skript:
        modul_name_err = os.environ.get("Modul_Name", ModulName)
        modul_version_err = os.environ.get("Modul_Version", ModulVersion)
        
        # REVIEW-STRUCT: launcher [DWMSG_MeldeFehler] invoked — internal behaviour not available in this extraction; confirm logging, error propagation, and credential handling before finalizing the conversion
        try:
            subprocess.run([
                "DWMSG_MeldeFehler", 
                p_Eintragsnr if p_Eintragsnr else "", 
                "E", 
                "196", 
                f"{modul_name_err} {modul_version_err} starteSQLSkript"
            ], check=True)
        except subprocess.CalledProcessError as e:
            print(f"ERROR: DWMSG_MeldeFehler failed with exit code {e.returncode}", file=sys.stderr)
        
        return 196

    # Step 4: Validate script file is readable
    script_path = pathlib.Path(p_Skript)
    if not script_path.is_file() or not os.access(script_path, os.R_OK):
        # REVIEW-STRUCT: launcher [DWMSG_MeldeFehler] invoked — internal behaviour not available in this extraction; confirm logging, error propagation, and credential handling before finalizing the conversion
        try:
            subprocess.run([
                "DWMSG_MeldeFehler", 
                p_Eintragsnr, 
                "E", 
                "201", 
                p_Skript
            ], check=True)
        except subprocess.CalledProcessError as e:
            print(f"ERROR: DWMSG_MeldeFehler failed with exit code {e.returncode}", file=sys.stderr)
        
        return 201

    # Step 5: Log run parameters
    print("Rufe SQL*PLUS auf mit folgenden Einstellungen")
    print(f"Sql*Plus-Skript : {p_Skript}")
    print(f"Skript-Parameter: {' '.join(args)}")

    # Step 6: Invoke sqlplus and capture exit code
    dw_orauser = os.environ.get("DW_ORAUSER")
    if not dw_orauser:
        raise SystemExit("DW_ORAUSER must be set by the calling environment")

    # REVIEW: target database platform is Oracle based on 'sqlplus' invocation; if migrating to a different target (e.g. BigQuery), this helper function and the SQL scripts it launches will require a complete redesign.
    # REVIEW-STRUCT: original launcher call preserved verbatim below — replace with the GCP-native equivalent once the launcher's internal behaviour (logging, error propagation, credential injection) is confirmed
    cmd = ["sqlplus", dw_orauser, f"@{p_Skript}"] + list(args)
    
    try:
        # Redirect stdin from /dev/null to match KSH behavior
        result = subprocess.run(cmd, stdin=subprocess.DEVNULL)
        errcode = result.returncode
    except FileNotFoundError:
        print("ERROR: sqlplus command not found in PATH", file=sys.stderr)
        errcode = 127
    except Exception as e:
        print(f"Error executing sqlplus: {e}", file=sys.stderr)
        errcode = 1
        
    return errcode

def main():
    parser = argparse.ArgumentParser(
        description="Helper script to run SQL*Plus scripts with validations."
    )
    parser.add_argument("p_Eintragsnr", nargs="?", default="", help="Error entry number")
    parser.add_argument("p_Skript", nargs="?", default="", help="SQL script path")
    parser.add_argument("sql_args", nargs=argparse.REMAINDER, help="Parameters for the SQL script")

    args = parser.parse_args()

    rem_args = args.sql_args
    if rem_args and rem_args[0] == '--':
        rem_args = rem_args[1:]

    return starteSQLSkript(args.p_Eintragsnr, args.p_Skript, *rem_args)

if __name__ == "__main__":
    sys.exit(main())