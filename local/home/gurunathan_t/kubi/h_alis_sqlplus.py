#!/usr/bin/env python3
import os
import sys
import argparse
from pathlib import Path
from google.cloud import bigquery
from google.cloud.exceptions import GoogleCloudError

# Module metadata variables
MODUL_NAME = "alis_sqlplus"
MODUL_VERSION = "V1.1.3"


# # REVIEW-STRUCT: launcher [DWMSG_MeldeFehler] invoked — internal behaviour not available in this extraction; confirm logging, error propagation, and credential handling before finalizing the conversion
def dwmsg_melde_fehler(eintrags_nr: str, msg_type: str, code: int, msg_text: str) -> None:
    """
    Mock/placeholder for the external DWMSG_MeldeFehler error-reporting utility.
    """
    print(f"ERROR_LOG [{eintrags_nr}] Type: {msg_type}, Code: {code}, Message: {msg_text}", file=sys.stderr)


def starte_sql_skript(p_eintragsnr: str, p_skript: str, *p_params: str) -> int:
    """
    Safely executes a SQL script file on BigQuery.
    
    Ported from KSH: starteSQLSkript()
    """
    # Step 1 & 2: Validate that required arguments are present
    # KSH Guard: if [ -z "$p_Eintragsnr" -o -z "$p_Skript" ]
    if not p_eintragsnr or not p_skript:
        # Replicates: DWMSG_MeldeFehler $p_Eintragsnr E 196 "${Modul_Name} ${Modul_Version} starteSQLSkript"
        module_info = f"{MODUL_NAME} {MODUL_VERSION} starteSQLSkript"
        dwmsg_melde_fehler(p_eintragsnr or "", "E", 196, module_info)
        return 196

    # Step 3: Check if the SQL script is readable
    # KSH Guard: if [ ! -r $p_Skript ]
    script_path = Path(p_skript)
    if not script_path.is_file() or not os.access(script_path, os.R_OK):
        # Replicates: DWMSG_MeldeFehler $p_Eintragsnr E 201 $p_Skript
        dwmsg_melde_fehler(p_eintragsnr, "E", 201, str(script_path))
        return 201

    # Step 4: Log invocation settings
    # Replicates echo statements verbatim
    print("Rufe SQL*PLUS auf mit folgenden Einstellungen")
    print(f"Sql*Plus-Skript : {p_skript}")
    print(f"Skript-Parameter: {' '.join(p_params)}")

    # Step 5: Execute the SQL target
    # The original script invoked Oracle SQL*Plus. Because the target platform is confirmed 
    # as BIGQUERY, we utilize the Google Cloud BigQuery client to run the migrated SQL file.
    try:
        # Retrieve GCP_PROJECT from environment variables
        gcp_project = os.environ.get("GCP_PROJECT")
        
        # Initialize the BigQuery client
        client = bigquery.Client(project=gcp_project)
        
        # Read the SQL query from the migrated script file
        with open(script_path, "r", encoding="utf-8") as sql_file:
            query_text = sql_file.read()

        # # REVIEW: Determine parameter parameterisation strategy (query parameters vs. templating).
        print(f"Executing Query in file '{p_skript}' via BigQuery Client...")
        
        # Run query job on BigQuery
        query_job = client.query(query_text)
        
        # Wait for the query to finish execution
        query_job.result()
        
        errcode = 0
    except GoogleCloudError as gcp_err:
        print(f"BigQuery execution failed: {gcp_err}", file=sys.stderr)
        # Propagate the GCP status code if available, otherwise default to 1
        errcode = gcp_err.code if hasattr(gcp_err, 'code') and gcp_err.code else 1
    except Exception as err:
        print(f"Execution failed: {err}", file=sys.stderr)
        errcode = 1

    # Step 6: Return exit status code
    return errcode


def main() -> int:
    parser = argparse.ArgumentParser(description="Helper routine to validate and run SQL scripts on BigQuery")
    parser.add_argument("eintragsnr", help="Error Entry ID (Fehlereintragsnummer)")
    parser.add_argument("skript", help="Path to the SQL script file")
    parser.add_argument("params", nargs="*", help="Dynamic parameters passed to the SQL script")
    
    args = parser.parse_args()
    
    return starte_sql_skript(args.eintragsnr, args.skript, *args.params)


if __name__ == "__main__":
    sys.exit(main())