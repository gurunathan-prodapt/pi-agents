#!/usr/bin/env python3
import os
import sys
import datetime
import subprocess

def log(message: str) -> None:
    """
    Helper function to print timestamped log messages.
    """
    current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{current_time}] {message}")

def main() -> int:
    # Step 1: Environment setup and default initialization
    # CRM_HOME defaults to '/opt/etl/customer' if not set
    crm_home = os.environ.get("CRM_HOME", "/opt/etl/customer")
    run_date = os.environ.get("RUN_DATE", "UNKNOWN_DATE")

    # Step 3: Log initiation of SCD2 historization
    log(f"Starting SCD2 historization for run date {run_date}")

    # Step 4: Define path to the script to execute
    # REVIEW-STRUCT: environment file k_historization_load.ksh not supplied — variables it sets are unknown; do not guess their names or values
    script_to_run = os.path.join(crm_home, "customer", "k_historization_load.py")

    # Step 5: Execute the sourced script using python3 as the execution environment
    # REVIEW-STRUCT: original launcher call preserved verbatim below — replace with the GCP-native equivalent once the launcher's internal behaviour (logging, error propagation, credential injection) is confirmed
    try:
        # We invoke python3 to run the script to mimic the 'sourcing' behavior as closely as possible in a subshell
        result = subprocess.run(["python3", script_to_run], check=True)
        rc = result.returncode
    except subprocess.CalledProcessError as e:
        rc = e.returncode
        # Step 6: Handle non-zero exit codes from k_historization_load.ksh
        log(f"ERROR: k_historization_load.ksh failed with rc={rc}")
        return rc
    except Exception as e:
        log(f"ERROR: Failed to launch k_historization_load.py: {str(e)}")
        return 1

    # Step 7: Log completion and exit with success code
    log(f"Historization load completed for {run_date}")
    return 0

if __name__ == "__main__":
    sys.exit(main())