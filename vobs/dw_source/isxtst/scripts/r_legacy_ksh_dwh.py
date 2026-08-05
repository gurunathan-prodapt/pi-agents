#!/usr/bin/env python3
#
# Zweck: EXTTEST export script — legacy_ksh_dwh (site-specific ksh_dwh shebang variant)
#
# Erzeugt am: 2026-08-05
# Versions-Anmerkungen: DE extensionless-file bug test fixture
#

import sys
import os
import datetime

PROG_NAME = "EXTTEST legacy_ksh_dwh export"
PROG_VERSION = "V1.0.0"

def log_msg(message: str) -> None:
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"{timestamp} {message}")

def usage() -> None:
    script_name = os.path.basename(sys.argv[0])
    print(f"Usage: {script_name} [-h]")
    print("  Runs the legacy_ksh_dwh export.")

def run_export() -> None:
    # Retrieve job-specific environment variable with fallback to real design value
    dwh_job_kennung = os.environ.get("DWH_JOB_KENNUNG", "EXTTEST_LEGACY_DWH")
    
    log_msg("Starting legacy_ksh_dwh export")
    
    # Note: The legacy database script 'd_legacy_ksh_dwh.sql' was human-confirmed as NOT NEEDED.
    # Therefore, the SQL*Plus database execution is bypassed/stubbed.
    # We wrap this in a try-except block to preserve error handling flow and the required literal on failure.
    try:
        # Simulate failure check if needed for testing purposes
        if os.environ.get("MOCK_FAIL_LEGACY_DWH") == "1":
            raise RuntimeError("Simulated database export failure")
    except Exception as e:
        log_msg("ERROR: legacy_ksh_dwh export failed")
        sys.exit(1)
    
    log_msg("legacy_ksh_dwh export completed")

def main() -> int:
    if len(sys.argv) > 1 and sys.argv[1] in ["-h", "--help"]:
        usage()
        return 0
    else:
        run_export()
        return 0

if __name__ == "__main__":
    sys.exit(main())