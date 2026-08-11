#!/usr/bin/env python3
import os
import sys
import subprocess
from pathlib import Path

# Step 1: Initialize global module variables
MODUL_NAME = "alis_sqlplus"
MODUL_VERSION = "V1.1.3"


# Step 2: Define starteSQLSkript utility function
def starte_sql_skript(p_eintragsnr: str, p_skript: str, *args) -> int:
    """
    Python equivalent of the starteSQLSkript shell function.
    Validates arguments and file readability, then runs sqlplus.
    """
    # Step 3: Parameter validation
    if not p_eintragsnr or not p_skript:
        modul_name_u = os.environ.get("Modul_Name", MODUL_NAME)
        modul_version_u = os.environ.get("Modul_Version", MODUL_VERSION)
        
        # REVIEW-STRUCT: original launcher call preserved verbatim below — replace with the GCP-native equivalent once the launcher's internal behaviour (logging, error propagation, credential injection) is confirmed
        cmd_err = [ 
            "DWMSG_MeldeFehler",
            p_eintragsnr if p_eintragsnr else "",
            "E",
            "196",
            f"{modul_name_u} {modul_version_u} starteSQLSkript"
        ]
        try:
            subprocess.run(cmd_err, check=False)
        except Exception as e:
            print(f"Error calling DWMSG_MeldeFehler: {e}", file=sys.stderr)
        return 196

    # Step 4: Validate file accessibility
    script_path = Path(p_skript)
    if not script_path.is_file() or not os.access(script_path, os.R_OK):
        # REVIEW-STRUCT: original launcher call preserved verbatim below — replace with the GCP-native equivalent once the launcher's internal behaviour (logging, error propagation, credential injection) is confirmed
        cmd_err = [ 
            "DWMSG_MeldeFehler",
            p_eintragsnr,
            "E",
            "201",
            p_skript
        ]
        try:
            subprocess.run(cmd_err, check=False)
        except Exception as e:
            print(f"Error calling DWMSG_MeldeFehler: {e}", file=sys.stderr)
        return 201

    # Step 5: Log operation parameters
    print("Rufe SQL*PLUS auf mit folgenden Einstellungen")
    print(f"Sql*Plus-Skript : {p_skript}")
    print(f"Skript-Parameter: {' '.join(args)}")

    # Step 6: Execute external SQL*Plus program
    dw_orauser = os.environ.get("DW_ORAUSER")
    if not dw_orauser:
        raise SystemExit("DW_ORAUSER must be set by the calling environment")

    sqlplus_cmd = ["sqlplus", dw_orauser, f"@{p_skript}"] + list(args)

    # REVIEW-STRUCT: original launcher call preserved verbatim below — replace with the GCP-native equivalent once the launcher's internal behaviour (logging, error propagation, credential injection) is confirmed
    try:
        # Redirecting stdin from DEVNULL corresponds to </dev/null
        # check=False mimics 'set +e' logic, allowing manual capture and propagation of exit code
        result = subprocess.run(
            sqlplus_cmd,
            stdin=subprocess.DEVNULL,
            check=False
        )
        errcode = result.returncode
    except Exception as e:
        print(f"Fehler bei der Ausfuehrung von sqlplus: {e}", file=sys.stderr)
        errcode = 1  # Generic execution failure

    # Step 7: Return SQLPlus execution exit code
    return errcode


def main(argv=None):
    import argparse
    parser = argparse.ArgumentParser(description="Run SQL*Plus scripts using starteSQLSkript logic.")
    parser.add_argument("p_eintragsnr", help="Error tracking ID (Eintragsnummer)")
    parser.add_argument("p_skript", help="Path to the SQL script to be run")
    parser.add_argument("sql_args", nargs="*", help="Dynamic parameters passed to the SQL script")

    args = parser.parse_args(argv)

    # Fail loudly if required environment variable is missing
    dw_orauser = os.environ.get("DW_ORAUSER")
    if not dw_orauser:
        raise SystemExit("DW_ORAUSER must be set by the calling environment")

    rc = starte_sql_skript(args.p_eintragsnr, args.p_skript, *args.sql_args)
    return rc


if __name__ == "__main__":
    sys.exit(main())