# Migrated from KornShell scripts:
# - vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_cntrct_crs2.ksh
# - vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_cntrct_crs2.ksh
# Job: DW.BERT_AUSD_V_TA_CNTRCT_CRS2

import os
import sys
import logging
from google.cloud import bigquery

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def run_bigquery_sql_script(sql_script_path: str, project_id: str):
    """
    Executes a BigQuery SQL script.
    """
    logging.info(f"Starting execution of BigQuery SQL script: {sql_script_path}")
    try:
        client = bigquery.Client(project=project_id)
        with open(sql_script_path, 'r') as f:
            sql_content = f.read()

        # Split SQL content into statements if there are DECLARE, TRUNCATE, INSERT
        # BigQuery client.query can handle multiple statements if they are valid.
        # For simplicity, we'll execute the entire script as one job, which BigQuery usually handles.
        query_job = client.query(sql_content)
        query_job.result()  # Wait for the job to complete
        logging.info(f"BigQuery SQL script {sql_script_path} executed successfully.")
    except Exception as e:
        logging.error(f"Error executing BigQuery SQL script {sql_script_path}: {e}")
        raise

if __name__ == "__main__":
    # In a real-world scenario, project_id and sql_script_path would be passed as arguments
    # to the PySpark job, e.g., using spark-submit --conf spark.driver.args="..."
    # For now, we'll use environment variables or default values.
    GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "YOUR_GCP_PROJECT_ID")
    # This path assumes the SQL script is co-located or accessible from the PySpark job's working directory
    # In practice, it would likely be on GCS or passed directly as content.
    BIGQUERY_SQL_SCRIPT_PATH = os.environ.get("BIGQUERY_SQL_SCRIPT_PATH", "d_ausd_v_ta_cntrct_crs2_bq.sql")

    if GCP_PROJECT_ID == "YOUR_GCP_PROJECT_ID":
        logging.error("GCP_PROJECT_ID is not set. Please set the 'GCP_PROJECT_ID' environment variable or update the script.")
        sys.exit(1)

    logging.info(f"PySpark wrapper started for job DW.BERT_AUSD_V_TA_CNTRCT_CRS2")
    logging.info(f"Using GCP Project ID: {GCP_PROJECT_ID}")
    logging.info(f"Target BigQuery SQL Script: {BIGQUERY_SQL_SCRIPT_PATH}")

    # The actual execution of the BigQuery SQL
    # This assumes that 'd_ausd_v_ta_cntrct_crs2_bq.sql' is available in the Dataproc job's environment
    # (e.g., uploaded to GCS and specified in --files for spark-submit)
    try:
        run_bigquery_sql_script(BIGQUERY_SQL_SCRIPT_PATH, GCP_PROJECT_ID)
        logging.info("PySpark wrapper finished successfully.")
    except Exception:
        logging.error("PySpark wrapper finished with errors.")
        sys.exit(1)