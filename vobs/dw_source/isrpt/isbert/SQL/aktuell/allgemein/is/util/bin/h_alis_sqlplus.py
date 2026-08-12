#!/usr/bin/env python3
import os
import sys
import argparse
import pathlib
import subprocess

# Step 1: Initialize module identifying parameters
# Source script defines ModulName/ModulVersion but references Modul_Name/Modul_Version.
# Defining both patterns here to maintain backward-compatibility and fix the original typo.
ModulName = "alis_sqlplus"
ModulVersion = "V1.1.3"
Modul_Name = ModulName
Modul_Version = ModulVersion


# Step 2: Define Python wrapper for legacy error utility
def call_dwmsg_meldefehler(eintragsnr, status_char, error_code, msg_text):
    """
    Invokes the external DWMSG_MeldeFehler log management script.
    """
    # REVIEW-STRUCT: original launcher call preserved verbatim below — replace with the GCP-native equivalent once the launcher's internal behaviour (logging, error propagation, credential injection) is confirmed
    cmd = [
        "DWMSG_MeldeFehler",
        str(eintragsnr),
        str(status_char),
        str(error_code),
        str(msg_text)
    ]
    try:
        subprocess.run(cmd, check=True)
    except subprocess.CalledProcessError as e:
        print(f"Failed to execute DWMSG_MeldeFehler: {e}", file=sys.stderr)


# Step 3: Define starteSQLSkript utility function
def starteSQLSkript(p_Eintragsnr, p_Skript, *script_args):
    """
    Verifies parameters and readability of a SQL file, then executes it via SQL*Plus.
    """
    # Step 3.1: Validate parameter inputs (replicates -z checks)
    if not p_Eintragsnr or not p_Skript:
        error_msg = f"{Modul_Name} {Modul_Version} starteSQLSkript"
        call_dwmsg_meldefehler(p_Eintragsnr, "E", 196, error_msg)
        return 196

    # Step 3.2: Check if script is readable on filesystem (replicates [ ! -r $p_Skript ])
    script_path = pathlib.Path(p_Skript)
    if not script_path.exists() or not os.access(script_path, os.R_OK):
        call_dwmsg_meldefehler(p_Eintragsnr, "E", 201, str(p_Skript))
        return 201

    # Step 3.3: Output parameters for standard logging
    print("Rufe SQL*PLUS auf mit folgenden Einstellungen")
    print(f"Sql*Plus-Skript : {p_Skript}")
    print(f"Skript-Parameter: {' '.join(script_args)}")

    # Step 3.4: Resolve DB user credentials from environment
    dw_orauser = os.environ.get("DW_ORAUSER", "")

    # Step 3.5: Execute SQL*Plus command line
    # REVIEW-STRUCT: original launcher call preserved verbatim below — replace with the GCP-native equivalent once the launcher's internal behaviour (logging, error propagation, credential injection) is confirmed
    sqlplus_cmd = ["sqlplus", dw_orauser, f"@{p_Skript}"] + list(script_args)
    
    try:
        # set +e / set -e simulation: we capture returncode without raising exception
        completed_process = subprocess.run(
            sqlplus_cmd,
            stdin=subprocess.DEVNULL,
            capture_output=False,
            text=True
        )
        errcode = completed_process.returncode
    except Exception as e:
        print(f"System execution error starting SQL*Plus: {e}", file=sys.stderr)
        errcode = -1  # Standard fallback error code

    # Step 3.6: Return captured execution code
    return errcode


def main():
    parser = argparse.ArgumentParser(description="Wrapper for executing SQL*Plus scripts.")
    parser.add_argument("eintragsnr", help="Fehlereintragsnummer")
    parser.add_argument("skript", help="Name of the script to execute")
    parser.add_argument("script_args", nargs="*", help="Optional script arguments")
    
    args = parser.parse_args()
    
    rc = starteSQLSkript(args.eintragsnr, args.skript, *args.script_args)
    return rc


if __name__ == "__main__":
    sys.exit(main())