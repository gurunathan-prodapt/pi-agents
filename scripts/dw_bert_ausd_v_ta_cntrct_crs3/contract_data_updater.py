# Python script to update contract data in BigQuery
# Legacy Sources:
# - vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_cntrct_crs3.ksh
# - vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_cntrct_crs3.ksh
# Job: DW.BERT_AUSD_V_TA_CNTRCT_CRS3
import argparse
import logging
from google.cloud import bigquery

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def run_bigquery_sql_file(project_id: str, sql_file_path: str):
    """
    Reads a SQL file and executes its content as a BigQuery query.
    """
    client = bigquery.Client(project=project_id)

    try:
        with open(sql_file_path, 'r') as file:
            sql_query = file.read()
        logging.info(f"Starting BigQuery job for SQL from: {sql_file_path}")
        query_job = client.query(sql_query)
        query_job.result()  # Waits for the job to complete.
        logging.info(f"BigQuery job {query_job.job_id} completed successfully.")
    except FileNotFoundError:
        logging.error(f"SQL file not found at: {sql_file_path}")
        raise
    except Exception as e:
        logging.error(f"Error executing BigQuery SQL: {e}")
        raise

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Update contract data in BigQuery.")
    parser.add_argument("--project_id", required=True, help="Your GCP project ID where BigQuery tables reside.")
    parser.add_argument("--sql_file", required=True,
                        help="Path to the BigQuery SQL file to execute (e.g., d_ausd_v_ta_cntrct_crs3_bq.sql).")
    args = parser.parse_args()

    try:
        logging.info("Starting contract data update process.")
        run_bigquery_sql_file(args.project_id, args.sql_file)
        logging.info("Contract data update process finished successfully.")
    except Exception as e:
        logging.critical(f"Contract data update process failed: {e}")
        exit(1)