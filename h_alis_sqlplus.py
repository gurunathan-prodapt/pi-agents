#!/usr/bin/env python3
import os
import sys
import argparse
import google.api_core.exceptions
from google.cloud import bigquery
from f_alis_msgerr import dwmsg_melde_fehler

# Module metadata variables
MODUL_NAME = "alis_sqlplus"
MODUL_VERSION = "V1.1.3"

def starte_sql_skript(p_eintragsnr: str, p_skript: str, *params: str) -> int:
    """
    Helper function to validate and execute a SQL script in BigQuery.
    """
    # Step 1: Validate input parameters
    if not p_eintragsnr or not p_skript:
        dwmsg_melde_fehler(p_eintragsnr, "E", 196, f"{MODUL_NAME} {MODUL_VERSION} starteSQLSkript")
        return 196

    # Step 2: Validate that SQL script file is readable
    if not os.path.isfile(p_skript) or not os.access(p_skript, os.R_OK):
        dwmsg_melde_fehler(p_eintragsnr, "E", 201, p_skript)
        return 201

    # Step 3: Log execution settings
    print("Rufe SQL*PLUS auf mit folgenden Einstellungen")
    print(f"Sql*Plus-Skript : {p_skript}")
    print(f"Skript-Parameter: {' '.join(params)}")

    # Sourced environment check for BigQuery GCP Project
    gcp_project = os.environ.get("GCP_PROJECT")

    # Step 4: Execute SQL script in BigQuery
    try:
        with open(p_skript, 'r', encoding='utf-8') as f:
            sql_content = f.read()

        # Translate positional SQL*Plus parameter style (e.g. &1, &2) into BigQuery query parameters
        query_parameters = []
        for i, val in enumerate(params):
            placeholder = f"&{i+1}"
            param_name = f"param_{i+1}"
            if placeholder in sql_content:
                sql_content = sql_content.replace(placeholder, f"@{param_name}")
            query_parameters.append(
                bigquery.ScalarQueryParameter(param_name, "STRING", val)
            )

        # Initialize BigQuery Client
        client = bigquery.Client(project=gcp_project)
        
        print(f"Executing query from {p_skript} on BigQuery...")
        job_config = bigquery.QueryJobConfig(
            query_parameters=query_parameters if query_parameters else None
        )
        
        query_job = client.query(sql_content, job_config=job_config)
        query_job.result()  # Wait for query completion
        
        errcode = 0
    except (google.api_core.exceptions.GoogleAPIError, OSError) as e:
        print(f"Database/File execution error: {e}", file=sys.stderr)
        errcode = 1
    except Exception as e:
        print(f"Unexpected database execution error: {e}", file=sys.stderr)
        errcode = 1
        
    return errcode

def main() -> int:
    parser = argparse.ArgumentParser(description="Helper utility to start SQL scripts (BigQuery version)")
    parser.add_argument("eintragsnr", help="Error log entry number")
    parser.add_argument("skript", help="Path to the SQL script file")
    parser.add_argument("params", nargs="*", help="Optional parameters forwarded to the SQL script")
    
    args = parser.parse_args()
    
    rc = starte_sql_skript(args.eintragsnr, args.skript, *args.params)
    return rc

if __name__ == "__main__":
    sys.exit(main())