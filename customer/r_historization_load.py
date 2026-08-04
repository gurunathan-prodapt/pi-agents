#!/usr/bin/env python3
import os
import sys
import subprocess
from datetime import datetime

def log(message: str) -> None:
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {message}")

def main() -> int:
    # Step 1: Environment Setup & Parameter Retrieval
    CRM_HOME = os.environ.get("CRM_HOME", "/opt/etl/customer")
    RUN_DATE = os.environ.get("RUN_DATE", "UNKNOWN_DATE")

    # Step 2: Log initialization of the process
    log(f"Starting SCD2 historization for run date {RUN_DATE}")

    # Step 3: Resolve the script path
    script_path = os.path.join(CRM_HOME, "customer", "k_historization_load.ksh")
    target_script = script_path.replace(".ksh", ".py")

    # Step 4: Execute script and capture exit status
    try:
        result = subprocess.run(["python3", target_script], check=True)
        rc = result.returncode
    except subprocess.CalledProcessError as e:
        rc = e.returncode
        # Step 5: Error handling and exit propagation
        log(f"ERROR: k_historization_load.ksh failed with rc={rc}")
        sys.exit(rc)
    except Exception as ex:
        log(f"ERROR: Failed to launch execution of {target_script}. Error: {str(ex)}")
        sys.exit(1)

    # Step 6: Log completion and exit successfully
    log(f"Historization load completed for {RUN_DATE}")
    return 0

if __name__ == "__main__":
    sys.exit(main())