# Legacy source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_geschaeftspartner.ksh
# Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_geschaeftspartner.ksh

import argparse
import logging
from google.cloud import bigquery

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def run_bigquery_sql_script(sql_file_path, stichtag_yyyymmdd):
    """
    Reads an SQL script, substitutes parameters, and executes it on BigQuery.
    """
    logging.info(f"Starting BigQuery SQL script execution for {sql_file_path} with stichtag: {stichtag_yyyymmdd}")

    client = bigquery.Client()

    try:
        # Read the SQL script content from GCS (assuming sql_file_path is a GCS URI)
        # For local testing, you might need to adjust this to read from local filesystem.
        # In Dataproc, GCS paths are directly accessible for file operations if the Python runtime allows.
        # For simpler GCS file reading, you might use 'google-cloud-storage' library explicitly.
        # For this example, assuming file.read() can handle 'gs://' paths if running in a GCS-aware environment.
        # If not, a specific GCS client read would be needed:
        # from google.cloud import storage
        # storage_client = storage.Client()
        # bucket_name, blob_name = sql_file_path.replace("gs://", "").split("/", 1)
        # bucket = storage_client.bucket(bucket_name)
        # blob = bucket.blob(blob_name)
        # sql_script_content = blob.download_as_text()

        # Simplified reading for general execution environment
        with open(sql_file_path, 'r') as file:
            sql_script_content = file.read()

        # Substitute parameters. Note: BigQuery standard SQL uses @param_name
        # The script parameter is @p_stichtag_yyyymmdd
        sql_to_execute = sql_script_content.replace('@p_stichtag_yyyymmdd', stichtag_yyyymmdd)

        # Split the script into individual statements, assuming statements are separated by semicolons
        # and ignore comments. This is a simple parser and might not handle all edge cases (e.g., semicolons in strings).
        statements = [stmt.strip() for stmt in sql_to_execute.split(';') if stmt.strip() and not stmt.strip().startswith('--')]

        for i, statement in enumerate(statements):
            if not statement: # Skip empty statements that might result from splitting
                continue
            logging.info(f"Executing statement {i+1}/{len(statements)}: {statement[:100]}...") # Log first 100 chars
            query_job = client.query(statement)
            query_job.result() # Wait for the job to complete
            logging.info(f"Statement {i+1} completed successfully.")

        logging.info("All BigQuery SQL statements executed successfully.")

    except Exception as e:
        logging.error(f"Error executing BigQuery SQL script: {e}")
        raise

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Execute a BigQuery SQL script with dynamic parameters.")
    parser.add_argument("--sql_file_path", required=True, help="Path to the BigQuery SQL script file (e.g., gs://bucket/path/to/script.sql).")
    parser.add_argument("--stichtag_yyyymmdd", required=True, help="Processing date in YYYYMMDD format.")
    # Add more arguments as needed if the SQL script requires them.

    args = parser.parse_args()

    run_bigquery_sql_script(args.sql_file_path, args.stichtag_yyyymmdd)