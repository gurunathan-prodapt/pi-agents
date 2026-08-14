#!/usr/bin/env python3
import os
import sys
import subprocess

# Module constants
MODUL_NAME = "alis_sqlplus"
MODUL_VERSION = "V1.1.3"


# REVIEW: external function DWMSG_MeldeFehler body not supplied — behaviour and signature inferred from usage
def dwmsg_melde_fehler(eintrags_nr: str, severity: str, error_code: int, details: str) -> None:
    """
    Placeholder for the external DWMSG_MeldeFehler error logging system.
    """
    print(f"ERROR LOG: [{severity}] Code {error_code} - {details} (Entry ID: {eintrags_nr})", file=sys.stderr)


def starte_sql_skript(p_eintragsnr: str, p_skript: str, *args: str) -> int:
    """
    Validates and executes an external SQL*Plus script.
    """
    # Step 1: Validate required parameters
    if not p_eintragsnr or not p_skript:
        details = f"{MODUL_NAME} {MODUL_VERSION} starteSQLSkript"
        dwmsg_melde_fehler(p_eintragsnr, "E", 196, details)
        return 196

    # Step 2: Validate file readability
    if not p_skript or not os.path.exists(p_skript) or not os.access(p_skript, os.R_OK):
        dwmsg_melde_fehler(p_eintragsnr, "E", 201, p_skript)
        return 201

    # Step 3: Log execution metadata
    print("Rufe SQL*PLUS auf mit folgenden Einstellungen")
    print(f"Sql*Plus-Skript : {p_skript}")
    print(f"Skript-Parameter: {' '.join(args)}")

    # Step 4: Retrieve environment connection details
    dw_orauser = os.environ.get("DW_ORAUSER")
    if not dw_orauser:
        raise SystemExit("DW_ORAUSER must be set by the calling environment")

    # Step 5: Execute SQL*Plus (replicates set +e safety)
    cmd = ["sqlplus", dw_orauser, f"@{p_skript}"] + list(args)
    
    try: 
        # stdin=subprocess.DEVNULL mimics </dev/null
        result = subprocess.run(
            cmd,
            stdin=subprocess.DEVNULL,
            check=False
        )
        errcode = result.returncode
    except Exception as e:
        print(f"Exception raised during sqlplus execution: {e}", file=sys.stderr)
        errcode = -1

    # Step 6: Return the execution exit status
    return errcode


def main() -> int:
    # Validate that the environment is set up properly
    if "DW_ORAUSER" not in os.environ:
        raise SystemExit("DW_ORAUSER must be set by the calling environment")

    # Parsing positional arguments dynamically to preserve original shell behavior
    args = sys.argv[1:]
    p_eintragsnr = args[0] if len(args) > 0 else ""
    p_skript = args[1] if len(args) > 1 else ""
    script_args = args[2:] if len(args) > 2 else []

    rc = starte_sql_skript(p_eintragsnr, p_skript, *script_args)
    return rc


if __name__ == "__main__":
    sys.exit(main())