# PySpark Wrapper Script for DW.BERT_AUSD_V_TA_P_DISCOUNT_RR
# Replaces: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_p_discount_rr.ksh
# Replaces: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_p_discount_rr.ksh

import os
import sys
from google.cloud import bigquery

# --- Configuration ---
# TODO: Replace with your GCP Project ID. The dataset 'dw' is hardcoded in the SQL.
PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "your-gcp-project-id")

# Path to the BigQuery SQL transformation file.
# This path is relative to the working directory of the PySpark job,
# assuming it was staged by the DataprocSubmitJobOperator's file_uris.
BQ_SQL_FILE_PATH = "d_ausd_v_ta_p_discount_rr.sql.bq"

def main():
    """
    Main function to execute the BigQuery SQL transformation.
    Handles reading the SQL file and executing it via the BigQuery client.
    """
    print(f"Starting PySpark wrapper for DW.BERT_AUSD_V_TA_P_DISCOUNT_RR in project: {PROJECT_ID}")

    try:
        client = bigquery.Client(project=PROJECT_ID)

        # Read the BigQuery SQL from the staged file
        print(f"Reading BigQuery SQL from {BQ_SQL_FILE_PATH}...")
        with open(BQ_SQL_FILE_PATH, 'r') as f:
            bigquery_sql_query = f.read()

        print("Executing BigQuery SQL transformation:")
        print("-" * 80)
        print(bigquery_sql_query)
        print("-" * 80)

        # Execute the SQL query
        query_job = client.query(bigquery_sql_query)
        query_job.result()  # Waits for the job to complete.

        print("BigQuery SQL transformation completed successfully.")

    except Exception as e:
        print(f"Error during BigQuery SQL execution: {e}", file=sys.stderr)
        sys.exit(1) # Exit with a non-zero code to indicate failure

if __name__ == "__main__":
    main()