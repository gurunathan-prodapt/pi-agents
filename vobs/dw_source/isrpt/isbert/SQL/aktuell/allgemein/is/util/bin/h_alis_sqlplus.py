#!/usr/bin/env python3
import os
import sys
import subprocess
import pathlib

# Step 1: Initialize module metadata
MODUL_NAME = "alis_sqlplus"
MODUL_VERSION = "V1.1.3"
# Supporting original typo in variable names from source shell script
MODUL__NAME = "alis_sqlplus"
MODUL__VERSION = "V1.1.3"

# REVIEW-STRUCT: external utility function DWMSG_MeldeFehler body not supplied — behavior stubbed; replace with migrated error handling module if available
def dwmsg_melde_fehler(p_eintragsnr, severity, code, message):
    """
    Stub for the external error-reporting utility DWMSG_MeldeFehler.
    """
    print(f"ERROR: [{p_eintragsnr}] {severity} {code}: {message}", file=sys.stderr)


# Step 2: Define starte_sql_skript function
def starte_sql_skript(p_eintragsnr: str, p_skript: str, *args) -> int:
    """
    Starts an SQL*Plus script with parameters and error handling.
    """
    # Step 3: Validate that required parameters are not empty
    if not p_eintragsnr or not p_skript:
        error_context = f"{MODUL__NAME} {MODUL__VERSION} starteSQLSkript"
        dwmsg_melde_fehler(p_eintragsnr, "E", "196", error_context)
        return 196

    # Step 4: Validate that the script file is readable
    script_path = pathlib.Path(p_skript)
    if not script_path.is_file() or not os.access(script_path, os.R_OK):
        dwmsg_melde_fehler(p_eintragsnr, "E", "201", str(p_skript))
        return 201

    # Step 5: Log execution details
    print("Rufe SQL*PLUS auf mit folgenden Einstellungen")
    print(f"Sql*Plus-Skript : {p_skript}")
    print(f"Skript-Parameter: {' '.join(args)}")

    # Step 6: Retrieve DB credentials from environment
    dw_orauser = os.environ.get("DW_ORAUSER")
    if not dw_orauser:
        raise SystemExit("DW_ORAUSER must be set by the calling environment")

    # Step 7: Execute SQL*Plus command
    # REVIEW-STRUCT: original launcher call preserved verbatim below — replace with the GCP-native equivalent once the launcher's internal behaviour (logging, error propagation, credential injection) is confirmed
    cmd = ["sqlplus", dw_orauser, f"@{p_skript}"] + list(args)
    
    try:
        # Redirect stdin from devnull equivalent to </dev/null
        result = subprocess.run(
            cmd,
            stdin=subprocess.DEVNULL,
            capture_output=False,
            check=False  # Do not raise exception automatically to mimic 'set +e' and return errcode
        )
        errcode = result.returncode
    except Exception as e:
        print(f"Execution failed: {str(e)}", file=sys.stderr)
        errcode = -1

    # Step 8: Return exit code
    return errcode


def main():
    # Map CLI arguments to match the original ksh positional parameter contract ($1, $2, and shift 2 for the rest)
    p_eintragsnr = sys.argv[1] if len(sys.argv) > 1 else ""
    p_skript = sys.argv[2] if len(sys.argv) > 2 else ""
    args = sys.argv[3:] if len(sys.argv) > 3 else []
    
    rc = starte_sql_skript(p_eintragsnr, p_skript, *args)
    return rc


if __name__ == "__main__":
    sys.exit(main())