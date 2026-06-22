# Python orchestration script to invoke BigQuery Stored Procedure
# Legacy source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bpr_optionen.ksh

from google.cloud import bigquery
import argparse
import datetime
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def main():
    parser = argparse.ArgumentParser(description="Invoke BigQuery Stored Procedure sp_d_ausd_bp_ta_bpr_optionen.")
    parser.add_argument("--job_kennung", required=True, help="Job identifier (e.g., 'BP_TA_BPR_OPTIONEN')")
    parser.add_argument("--eintrags_nr", required=True, help="Entry number")
    parser.add_argument("--stichtag", help="Key date for processing in DDMMYYYY format. Defaults to yesterday.")
    parser.add_argument("--wiederanlauf_wert", default="", help="Restart value (optional)")
    parser.add_argument("--project_id", required=True, help="Your GCP project ID")
    parser.add_argument("--dataset_id", required=True, help="Your BigQuery dataset ID for the stored procedure")
    parser.add_argument("--isbert_schema_dataset_id", required=True, help="Your BigQuery dataset ID for isbert_schema")

    args = parser.parse_args()

    client = bigquery.Client(project=args.project_id)

    # Determine stichtag if not provided
    if not args.stichtag:
        yesterday = datetime.date.today() - datetime.timedelta(days=1)
        args.stichtag = yesterday.strftime("%d%m%Y")
        logging.info(f"Stichtag not provided, defaulting to yesterday: {args.stichtag}")

    sp_full_path = f"`{args.project_id}.{args.dataset_id}.sp_d_ausd_bp_ta_bpr_optionen`"

    # Construct the SQL query to call the stored procedure
    # Note: BigQuery requires string literals to be quoted.
    query = f"""
    CALL {sp_full_path}(
        p_JobKennung => '{args.job_kennung}',
        p_EintragsNr => '{args.eintrags_nr}',
        p_Stichtag => '{args.stichtag}',
        p_wiederanlaufWert => '{args.wiederanlauf_wert}'
    );
    """

    logging.info(f"Executing BigQuery Stored Procedure: {sp_full_path}")
    logging.debug(f"SQL Query: {query}")

    try:
        query_job = client.query(query)
        query_job.result()  # Waits for the job to complete
        logging.info(f"Stored Procedure {sp_full_path} executed successfully.")
        logging.info(f"Job ID: {query_job.job_id}")
    except Exception as e:
        logging.error(f"Error executing Stored Procedure {sp_full_path}: {e}")
        # In a real-world scenario, you might want to log this to BigQuery error_log
        # or propagate the error for Airflow/Workflows to handle.
        raise

if __name__ == "__main__":
    main()