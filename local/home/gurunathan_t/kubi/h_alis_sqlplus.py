#!/usr/bin/env python3
import os
import sys
import logging
from google.cloud import bigquery
from google.cloud.exceptions import GoogleCloudError

# Module Level Metadata
MODUL_NAME = "alis_sqlplus"
MODUL_VERSION = "V1.1.3"

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(MODUL_NAME)

# REVIEW-STRUCT: legacy Oracle status-logging package [DWMSG_MeldeFehler] replaced with native logging — confirm target logging destination (Cloud Logging / BigQuery table) before deploying
# REVIEW-STRUCT: external function [DWMSG_MeldeFehler] not supplied — behavior/implementation unknown; must be mapped to a standard Python logging/alerting library or custom equivalent.
def dwmsg_meldefehler(p_eintragsnr, severity, err_code, message):
    logger.error(f"ENTRY: {p_eintragsnr} | SEVERITY: {severity} | CODE: {err_code} | MSG: {message}")

def starte_sql_skript(p_eintragsnr, p_skript, additional_args):
    # Step 1: Validate required parameters
    # Legacy guard: if [ -z "$p_Eintragsnr" -o -z "$p_Skript" ]
    if not p_eintragsnr or not p_skript:
        dwmsg_meldefehler(p_eintragsnr, "E", 196, f"{MODUL_NAME} {MODUL_VERSION} starteSQLSkript")
        return 196

    # Step 2: Validate file existence and readability
    # Legacy guard: if [ ! -r $p_Skript ]
    if not os.path.isfile(p_skript) or not os.access(p_skript, os.R_OK):
        dwmsg_meldefehler(p_eintragsnr, "E", 201, p_skript)
        return 201

    # Step 3: Log invocation configurations (preserve original German literal output strings)
    print("Rufe SQL*PLUS auf mit folgenden Einstellungen")
    print(f"Sql*Plus-Skript : {p_skript}")
    params_str = " ".join(additional_args)
    print(f"Skript-Parameter: {params_str}")

    # Step 4: Execute SQL script content via BigQuery API
    try:
        # # REVIEW-STRUCT: connection parameters inferred from legacy environment — confirm target BigQuery project/dataset mapping and authenticate via Service Account or ADC.
        client = bigquery.Client()

        with open(p_skript, "r", encoding="utf-8") as f:
            sql_text = f.read()

        # # REVIEW: BigQuery standard SQL does not support SQL*Plus style positional command line arguments (&1, &2, etc.) natively; target SQL scripts must be adapted to use named query parameters or format placeholders.
        if additional_args:
            print("Warning: Positional arguments provided to BigQuery SQL script. Ensure query handles templating.", file=sys.stderr)

        query_job = client.query(sql_text)
        query_job.result()  # Wait for query execution to complete
        errcode = 0

    except GoogleCloudError as e:
        logger.error(f"BigQuery Error executing {p_skript}: {e}")
        errcode = e.code if hasattr(e, "code") and isinstance(e.code, int) else 1
    except Exception as e:
        logger.error(f"Unexpected error executing {p_skript}: {e}")
        errcode = 1

    return errcode

def main():
    import argparse
    parser = argparse.ArgumentParser(description="Python wrapper for h_alis_sqlplus")
    parser.add_argument("p_Eintragsnr", nargs="?", default="", help="Error registration number")
    parser.add_argument("p_Skript", nargs="?", default="", help="Path to SQL script")
    parser.add_argument("args", nargs="*", help="Dynamic script parameters")
    
    parsed_args = parser.parse_args()
    
    exit_code = starte_sql_skript(parsed_args.p_Eintragsnr, parsed_args.p_Skript, parsed_args.args)
    return exit_code

if __name__ == "__main__":
    sys.exit(main())