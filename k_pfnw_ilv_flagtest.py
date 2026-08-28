#!/usr/bin/env python3
#
# Usage: Script returns exit 0 if the executed query produces no output, exit 1 otherwise
# Converted from KornShell to Python 3 for BigQuery / GCP target platform.

import os
import sys
import argparse
import logging
from google.cloud import bigquery
from google.cloud.exceptions import GoogleCloudError

# Step 1: Import required modules and define the CLI parser
def parse_arguments():
    default_job_id = os.environ.get("DWH_JOB_KENNUNG", "PFNW").strip()
    parser = argparse.ArgumentParser(
        description="Allgmeines Pruefskript ohne Mailversand",
        add_help=False
    )
    parser.add_argument("-q", dest="query_file", required=True, help="Path to the validation SQL query file")
    parser.add_argument("-j", dest="job_id", default=default_job_id, help="Job identifier")
    parser.add_argument("-v", dest="verbose", action="store_true", help="Verbose log output on error")
    parser.add_argument("-h", "--help", action="help", help="Show this help message and exit")
    return parser.parse_args()


def tee_print(message, log_file=None):
    """Prints message to stdout and appends it to the log file."""
    print(message)
    if log_file:
        try:
            with open(log_file, "a", encoding="utf-8") as lf:
                lf.write(message + "\n")
        except Exception as e:
            print(f"WARNING: Could not write to log file: {e}", file=sys.stderr)


def main():
    # Step 2: Define command-line interface
    args = parse_arguments()
    
    # NOTE: parameter accepted for interface compatibility; unused in original script logic under BigQuery
    dw_orauser = os.environ.get("DW_ORAUSER")

    # Step 3: Validate query file existence and readability
    if not os.path.isfile(args.query_file) or not os.access(args.query_file, os.R_OK):
        print(f"ERROR: Query-Datei {args.query_file} not readable", file=sys.stderr)
        sys.exit(1)
        
    # Step 4: Environment bootstrap and framework logging initialization
    dw_eintrags_nr = os.environ.get("DW_EintragsNr", "0")
    log_file_path = f"/tmp/validation_{args.job_id}_{dw_eintrags_nr}.log"
    
    # Setup native Python logger
    logging.basicConfig(
        filename=log_file_path,
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s"
    )
    
    # Echo header to stdout and logfile (replaces 'tee -a $LogDatei')
    tee_print("----------------- Allgemeines Pruefskript -----------------", log_file_path)
    tee_print(f"JobCode                : {args.job_id}", log_file_path)
    tee_print(f"SQL-Pruef-Query           : {args.query_file}", log_file_path)
    tee_print("-----------------------------------------------------------", log_file_path)
    
    tee_print("Execute the following query:", log_file_path)
    tee_print("Execute query...", log_file_path)
    
    # Step 5: Read query from file
    try:
        with open(args.query_file, 'r', encoding='utf-8') as f:
            query_text = f.read()
    except Exception as e:
        logging.error(f"Failed to read query file: {str(e)}")
        print(f"ERROR: Failed to read query file: {str(e)}", file=sys.stderr)
        sys.exit(1)
        
    tee_print(query_text, log_file_path)
    
    # Step 6: Initialize BigQuery client and execute query
    try:
        client = bigquery.Client()
        logging.info("Executing query on BigQuery...")
        query_job = client.query(query_text)
        results = query_job.result()  # Waits for query to complete
    except GoogleCloudError as dberr:
        # Step 7: Handle query execution errors
        logging.error(f"ERROR: Query reported an error: {str(dberr)}")
        tee_print("ERROR: Query reported an error", log_file_path)
        sys.exit(1)
        
    # Step 8: Analyze results
    row_count = results.total_rows
    
    # Print query results to log file for debugging
    try:
        with open(log_file_path, "a", encoding="utf-8") as lf:
            lf.write("Print query:\n")
            for row in results:
                lf.write(str(row.values()) + "\n")
            lf.write("\n")
    except Exception as e:
        logging.warning(f"Could not log query rows: {e}")

    if row_count is not None and row_count > 0:
        # If any row is returned, validation fails (Query reported an error)
        logging.warning(f"Query returned {row_count} rows. Validation failed.")
        print("Query reported an error - script aborts", file=sys.stderr)
        return_code = 1
    else:
        # If 0 rows are returned, validation succeeds
        logging.info("Query returned 0 rows. Validation succeeded.")
        tee_print("Query reported no errors - script ends without any errors", log_file_path)
        return_code = 0
        
    # Step 9: Final cleanup and exit
    sys.exit(return_code)


if __name__ == "__main__":
    sys.exit(main())