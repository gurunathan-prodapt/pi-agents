#!/usr/bin/env python3
import os
import sys
import argparse
import pathlib
import subprocess
from google.cloud import bigquery

# Step 1: Initialize module-level metadata variables
MODUL_NAME = "alis_sqlplus"
MODUL_VERSION = "V1.1.3"

# Global Environment Values
GCP_PROJECT = os.environ.get("GCP_PROJECT")
BQ_DATASET = os.environ.get("BQ_DATASET")
DW_ORAUSER = os.environ.get("DW_ORAUSER")

# Step 2: Define the central wrapper function
def starte_sql_skript(p_eintragsnr: str, p_skript: str, *args) -> int:
    """
    Python equivalent of starteSQLSkript function.
    Validates arguments and executes the specified SQL script.
    """
    
    # Step 3: Audit & Validate mandatory parameters
    # Original guard: if [ -z "$p_Eintragsnr" -o -z "$p_Skript" ]
    if not p_eintragsnr or not p_skript:
        # Call legacy error handler utility
        subprocess.run([
            "DWMSG_MeldeFehler", 
            p_eintragsnr, 
            "E", 
            "196", 
            f"{MODUL_NAME} {MODUL_VERSION} starteSQLSkript"
        ], check=False)
        return 196

    # Step 4: Audit & Validate that the script file is readable
    # Original guard: if [ ! -r $p_Skript ]
    script_path = pathlib.Path(p_skript)
    if not script_path.is_file() or not os.access(script_path, os.R_OK):
        subprocess.run([
            "DWMSG_MeldeFehler", 
            p_eintragsnr, 
            "E", 
            "201", 
            p_skript
        ], check=False)
        return 201

    # Step 5: Log invocation details to stdout
    print("Rufe SQL*PLUS auf mit folgenden Einstellungen")
    print(f"Sql*Plus-Skript : {p_skript}")
    print(f"Skript-Parameter: {' '.join(args)}")

    # Step 6: Execute the SQL script
    errcode = 0
    try:
        # Target database platform is BIGQUERY.
        # Execute the SQL script via the google-cloud-bigquery client.
        client = bigquery.Client(project=GCP_PROJECT)
        with open(script_path, "r", encoding="utf-8") as f:
            sql_query = f.read()
            
        if args:
            print(f"Warning: Positional arguments {args} were provided but BigQuery execution of raw SQL files does not natively map them without custom replacement.", file=sys.stderr)
            
        query_job = client.query(sql_query)
        query_job.result() # Waits for query to complete
        errcode = 0
    except Exception as e:
        print(f"Execution failed: {str(e)}", file=sys.stderr)
        errcode = 1  # Standard fallback error code

    # Step 7: Return the resulting exit code
    return errcode

def main():
    parser = argparse.ArgumentParser(description="Run BigQuery SQL scripts (formerly via SQL*Plus h_alis_sqlplus.ksh wrapper).")
    parser.add_argument("p_eintragsnr", help="Error tracking entry number.")
    parser.add_argument("p_skript", help="Path to the SQL script file.")
    parser.add_argument("args", nargs="*", help="Dynamic parameters passed to the SQL script.")
    
    parsed_args = parser.parse_args()
    
    rc = starte_sql_skript(parsed_args.p_eintragsnr, parsed_args.p_skript, *parsed_args.args)
    return rc

if __name__ == "__main__":
    sys.exit(main())