#!/usr/bin/env python3
"""
r_historization_load.py

Invoked by CUSTOMER.HISTORIZATION_LOAD. Wraps the SCD2 historization merge
so a partial/failed merge is always logged with its row-impact counts
before the job exits, rather than only surfacing sqlplus's raw exit code.
"""

import os
import sys
import subprocess
from datetime import datetime

def log(message: str):
    """Prints a timestamped message to standard output."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {message}")

def main():
    # Step 1: Initialize environment variables and defaults
    crm_home = os.environ.get("CRM_HOME", "/opt/etl/customer")
    run_date = os.environ.get("RUN_DATE", "")

    # Step 3: Log initiation of process
    log(f"Starting SCD2 historization for run date {run_date}")

    # Step 4: Resolve path to underlying script
    # Prefer migrated Python version of the script if available, fallback to legacy KornShell script
    script_to_run_py = os.path.join(crm_home, "customer", "k_historization_load.py")
    script_to_run_ksh = os.path.join(crm_home, "customer", "k_historization_load.ksh")

    if os.path.exists(script_to_run_py):
        script_to_run = script_to_run_py
        cmd = [sys.executable, script_to_run]
        shell_val = False
    else:
        script_to_run = script_to_run_ksh
        cmd = [script_to_run]
        shell_val = True

    # Step 5: Execute the script and monitor status
    try: 
        # Sourcing behavior is simulated by executing the script in a subprocess.
        # Current environment is forwarded to preserve context variables (like CRM_HOME, RUN_DATE, etc.)
        result = subprocess.run(cmd, shell=shell_val, env=os.environ, check=True)
        rc = result.returncode
    except subprocess.CalledProcessError as e:
        rc = e.returncode
        # Step 6: Conditional error handling for non-zero execution statuses
        log(f"ERROR: k_historization_load.ksh failed with rc={rc}")
        sys.exit(rc)
    except Exception as ex:
        # Handle unexpected failures launching the process
        log(f"ERROR: Failed to launch historization script: {str(ex)}")
        sys.exit(1)

    # Step 7: Log completion and exit successfully
    log(f"Historization load completed for {run_date}")
    sys.exit(0)

if __name__ == "__main__":
    sys.exit(main())