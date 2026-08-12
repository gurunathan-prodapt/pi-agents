#!/usr/bin/env python3
import os
import sys
import shutil
import subprocess

# Step 1: Initialize module metadata
# NOTE: The legacy script declared ModulName/ModulVersion but used Modul_Name/Modul_Version
# in DWMSG_MeldeFehler. We resolve this mismatch here by defining both.
MODUL_NAME = "alis_sqlplus"
MODUL_VERSION = "V1.1.3"


# Step 2: Define the main utility function
def starteSQLSkript(entry_nr: str, script_path: str, *script_args: str) -> int:
    """
    Python equivalent of the legacy 'starteSQLSkript' shell function.
    Validates arguments and script file readability, then runs sqlplus.
    """

    # Step 3: Validate that required positional parameters are not empty
    if not entry_nr or not script_path:
        # REVIEW-STRUCT: original launcher call preserved verbatim below — replace with the GCP-native equivalent once the launcher's internal behaviour (logging, error propagation, credential injection) is confirmed
        subprocess.run(
            [
                "DWMSG_MeldeFehler",
                entry_nr,
                "E",
                "196",
                f"{MODUL_NAME} {MODUL_VERSION} starteSQLSkript",
            ],
            check=False,
        )
        return 196

    # Step 4: Validate that the SQL file exists and is readable
    if not os.path.isfile(script_path) or not os.access(script_path, os.R_OK):
        # REVIEW-STRUCT: original launcher call preserved verbatim below — replace with the GCP-native equivalent once the launcher's internal behaviour (logging, error propagation, credential injection) is confirmed
        subprocess.run(
            ["DWMSG_MeldeFehler", entry_nr, "E", "201", script_path],
            check=False,
        )
        return 201

    # Step 5: Log start details
    print("Rufe SQL*PLUS auf mit folgenden Einstellungen")
    print(f"Sql*Plus-Skript : {script_path}")
    print(f"Skript-Parameter: {' '.join(script_args)}")

    # Step 6: Verify sqlplus executable is available on PATH
    if shutil.which("sqlplus") is None:
        raise RuntimeError("sqlplus executable not found in PATH")

    # Step 7: Resolve Oracle connection credentials
    # Read them from environment variables named exactly as they appear in the design document
    dw_orauser = os.environ.get("DW_ORAUSER")
    if not dw_orauser:
        try:
            from airflow.models import Variable
            dw_orauser = Variable.get("DW_ORAUSER", default_var=None)
        except ImportError:
            pass

    if not dw_orauser:
        raise SystemExit(
            "DW_ORAUSER must be set by the calling environment or Airflow Variables"
        )

    # Step 8: Execute sqlplus with redirected input from DEVNULL
    # REVIEW-STRUCT: original launcher call preserved verbatim below — replace with the GCP-native equivalent once the launcher's internal behaviour (logging, error propagation, credential injection) is confirmed
    cmd = ["sqlplus", dw_orauser, f"@{script_path}"] + list(script_args)

    try:
        result = subprocess.run(
            cmd,
            stdin=subprocess.DEVNULL,
            check=False,
        )
        errcode = result.returncode
    except Exception as e:
        print(
            f"Critical error during SQL*Plus invocation: {e}",
            file=sys.stderr,
        )
        errcode = 1

    # Step 9: Return database execution status code
    return errcode


def main() -> int:
    # Read positional arguments matching original parameter contract ($1, $2, ...)
    args = sys.argv[1:]
    entry_nr = args[0] if len(args) > 0 else ""
    script_path = args[1] if len(args) > 1 else ""
    script_args = args[2:] if len(args) > 2 else []

    errcode = starteSQLSkript(entry_nr, script_path, *script_args)
    return errcode


if __name__ == "__main__":
    sys.exit(main())