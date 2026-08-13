#!/usr/bin/env python3
import os
import sys
import subprocess
import pathlib
import argparse

# Module metadata variables
MODUL_NAME = "alis_sqlplus"
MODUL_VERSION = "V1.1.3"


def dwmsg_melde_fehler(p_eintragsnr: str, severity: str, error_id: int, message_or_arg: str):
    """
    Stub for the unresolved source component DWMSG_MeldeFehler.
    """
    raise NotImplementedError("DWMSG_MeldeFehler is an unresolved source component.")


def starte_sql_skript(p_eintragsnr: str, p_skript: str, *args) -> int:
    """
    Starts a SQL*Plus script after validating its readability.
    """
    # Parameter validation
    if not p_eintragsnr or not p_skript:
        dwmsg_melde_fehler(p_eintragsnr, "E", 196, f"{MODUL_NAME} {MODUL_VERSION} starteSQLSkript")
        return 196

    # File readability check
    script_path = pathlib.Path(p_skript)
    if not script_path.is_file() or not os.access(script_path, os.R_OK):
        dwmsg_melde_fehler(p_eintragsnr, "E", 201, p_skript)
        return 201

    # Print original-language details
    print("Rufe SQL*PLUS auf mit folgenden Einstellungen")
    print(f"Sql*Plus-Skript : {p_skript}")
    print(f"Skript-Parameter: {' '.join(args)}")

    # Get connection credentials from environment
    dw_orauser = os.environ.get("DW_ORAUSER")
    if not dw_orauser:
        raise ValueError("DW_ORAUSER environment variable is not set.")

    # Construct and execute command
    cmd = ["sqlplus", dw_orauser, f"@{p_skript}"] + list(args)

    try: 
        result = subprocess.run(
            cmd,
            stdin=subprocess.DEVNULL,
            capture_output=False,
            text=True,
            check=False
        )
        errcode = result.returncode
    except Exception as e:
        print(f"CRITICAL: Failed to spawn sqlplus process: {e}", file=sys.stderr)
        errcode = 1

    return errcode


def main():
    parser = argparse.ArgumentParser(description="Start SQL*Plus script wrapper.")
    parser.add_argument("p_eintragsnr", help="Error entry number")
    parser.add_argument("p_skript", help="Path to SQL script")
    parser.add_argument("script_args", nargs="*", help="Optional parameters for the SQL script")

    args = parser.parse_args()

    exit_code = starte_sql_skript(args.p_eintragsnr, args.p_skript, *args.script_args)
    return exit_code


if __name__ == "__main__":
    sys.exit(main())