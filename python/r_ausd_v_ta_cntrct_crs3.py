# Migrated from vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_cntrct_crs3.ksh and k_ausd_v_ta_cntrct_crs3.ksh
# Job: DW.BERT_AUSD_V_TA_CNTRCT_CRS3

import argparse
import logging
import os
from google.cloud import bigquery
from google.api_core.exceptions import GoogleAPIError

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def main():
    parser = argparse.ArgumentParser(description="Execute BigQuery SQL for DW.BERT_AUSD_V_TA_CNTRCT_CRS3 job.")
    parser.add_argument("--job_kennung", type=str, help="Job identifier (e.g., BERT_AUSD_V_TA_CNTRCT_CRS3).", required=True)
    parser.add_argument("--eintrags_nr", type=str, help="Entry number.", required=False, default="0")

    args = parser.parse_args()

    logging.info(f"Starting BigQuery job DW.BERT_AUSD_V_TA_CNTRCT_CRS3 with JobKennung: {args.job_kennung}, EintragsNr: {args.eintrags_nr}")

    client = bigquery.Client()
    
    try:
        # Assuming the SQL file 'd_ausd_v_ta_cntrct_crs3.bqsql' is available in the same directory
        # or a specified path accessible by the Dataproc worker.
        current_dir = os.path.dirname(os.path.abspath(__file__))
        sql_file_path = os.path.join(current_dir, "d_ausd_v_ta_cntrct_crs3.bqsql")
        with open(sql_file_path, 'r') as f:
            sql_query = f.read()
        
        logging.info("Executing BigQuery SQL...")
        query_job = client.query(sql_query)
        query_job.result() # Wait for the job to complete
        logging.info(f"BigQuery job {query_job.job_id} completed successfully.")
        logging.info("Processing completed without errors.")
    except FileNotFoundError:
        logging.error(f"SQL file not found at {sql_file_path}. Please ensure it is deployed correctly.")
        exit(1)
    except GoogleAPIError as e:
        logging.error(f"BigQuery API error: {e}")
        exit(1)
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
        exit(1)

if __name__ == "__main__":
    main()