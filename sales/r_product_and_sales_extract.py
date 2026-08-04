#!/usr/bin/env python3
import argparse
import datetime
import os
import subprocess
import sys
from pathlib import Path


def log_message(log_file_path, message):
    """Helper function mimicking the original log() command:

    writes to stdout and appends to the log file.
    """
    current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    formatted_msg = f"[{current_time}] {message}"
    print(formatted_msg)
    try:
        with open(log_file_path, "a") as f:
            f.write(formatted_msg + "\n")
    except Exception as e:
        print(f"Failed to write to log file: {e}", file=sys.stderr)


def main():
    # Step 1: Parse command-line arguments to maintain original interface compatibility
    parser = argparse.ArgumentParser(
        description="Wrapper for product and sales extraction"
    )
    # No positional arguments required by the original script, but parser kept for interface contract.
    parser.parse_args()

    # Step 2: Define environment parameters and paths
    retail_home = os.environ.get("RETAIL_HOME", "/opt/etl/sales")
    log_dir = Path(retail_home) / "logs"

    # Step 3: Create log directory if it does not exist
    try:
        log_dir.mkdir(parents=True, exist_ok=True)
    except Exception as e:
        print(f"Failed to create log directory {log_dir}: {e}", file=sys.stderr)
        sys.exit(1)

    # Step 4: Define log file with current timestamp
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file_path = log_dir / f"product_and_sales_extract_{timestamp}.log"

    # Step 5: Define the logging function to write to stdout and log file
    # Defined globally as log_message for better structure.

    # Step 6: Validate RUN_DATE environment variable
    run_date = os.environ.get("RUN_DATE")
    if not run_date:
        log_message(log_file_path, "ERROR: RUN_DATE is not set - aborting")
        sys.exit(1)

    # Step 7: Log execution start
    log_message(
        log_file_path,
        f"Starting product-and-sales extract for run date {run_date}",
    )

    # Step 8: Invoke core extraction script
    # REVIEW-STRUCT: environment file [k_product_and_sales_extract.ksh] not supplied — variables it sets are unknown; do not guess their names or values.
    # REVIEW-STRUCT: original launcher call preserved verbatim below — replace with the GCP-native equivalent once the launcher's internal behaviour (logging, error propagation, credential injection) is confirmed
    core_script_path = (
        Path(retail_home) / "sales" / "k_product_and_sales_extract.py"
    )

    try: 
        # Sourcing a script inside Python is handled by executing it as a subprocess.
        # We pass the existing environment variables so that the child process inherits them.
        result = subprocess.run(
            [sys.executable, str(core_script_path)], env=os.environ, check=True
        )
        rc = result.returncode
    except subprocess.CalledProcessError as err:
        rc = err.returncode
        log_message(
            log_file_path,
            f"ERROR: k_product_and_sales_extract.ksh failed with exit code {rc}",
        )
        sys.exit(rc)
    except Exception as err:
        log_message(
            log_file_path, f"ERROR: Failed to execute {core_script_path}: {err}"
        )
        sys.exit(1)

    # Step 9: Validate the result and exit
    if rc != 0:
        log_message(
            log_file_path,
            f"ERROR: k_product_and_sales_extract.ksh failed with exit code {rc}",
        )
        sys.exit(rc)

    log_message(
        log_file_path,
        f"Product-and-sales extract completed successfully for {run_date}",
    )
    sys.exit(0)


if __name__ == "__main__":
    sys.exit(main())

# === CONVERSION CONFIDENCE REPORT ===
# Confidence score: 77/100
# Deductions:
#   -15: launcher [k_product_and_sales_extract.ksh] — internal behaviour unknown, preserved as TODO
#   -8: sourced environment file [k_product_and_sales_extract.ksh] — contents not supplied