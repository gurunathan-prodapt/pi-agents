# Migrated from vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_p_basisprod.ksh
# Migrated from vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_p_basisprod.ksh
# Job: DW.BERT_AUSD_BP_TA_P_BASISPROD

import sys
import argparse
import logging
from datetime import datetime
from google.cloud import bigquery
from google.cloud import storage

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    """
    Main function to execute the PySpark application.
    Parses arguments, loads SQL from GCS, and executes BigQuery queries.
    """
    parser = argparse.ArgumentParser(description="PySpark application for DW.BERT_AUSD_BP_TA_P_BASISPROD.")
    parser.add_argument("--stichtag", type=str, help="Key date in YYYYMMDD format (e.g., 20231026). Defaults to current date if not provided.")
    parser.add_argument("--wiederanlaufwert", type=int, default=0, help="Restart value. Defaults to 0.")
    parser.add_argument("--sql_file_gcs_path", type=str, required=True, help="GCS path to the BigQuery SQL script (e.g., gs://YOUR_BUCKET_NAME/sql/d_ausd_bp_ta_p_basisprod_bq.sql).")
    parser.add_argument("--project_id", type=str, required=True, help="Google Cloud Project ID.")

    args = parser.parse_args()

    stichtag = args.stichtag
    if not stichtag:
        stichtag = datetime.now().strftime('%Y%m%d')
        logger.info(f"Stichtag not provided, defaulting to current date: {stichtag}")

    wiederanlaufwert = args.wiederanlaufwert
    sql_file_gcs_path = args.sql_file_gcs_path
    project_id = args.project_id

    logger.info(f"Starting PySpark job for DW.BERT_AUSD_BP_TA_P_BASISPROD")
    logger.info(f"Parameters: Stichtag={stichtag}, Wiederanlaufwert={wiederanlaufwert}")
    logger.info(f"SQL file GCS path: {sql_file_gcs_path}")
    logger.info(f"Project ID: {project_id}")

    try:
        # Initialize BigQuery client
        bq_client = bigquery.Client(project=project_id)
        # Initialize GCS client
        gcs_client = storage.Client(project=project_id)

        # Read SQL content from GCS
        # Assuming sql_file_gcs_path is like 'gs://bucket_name/path/to/file.sql'
        if not sql_file_gcs_path.startswith('gs://'):
            raise ValueError("SQL file GCS path must start with 'gs://'")
        
        path_parts = sql_file_gcs_path[5:].split('/', 1)
        bucket_name = path_parts[0]
        blob_path = path_parts[1]

        bucket = gcs_client.bucket(bucket_name)
        blob = bucket.blob(blob_path)
        sql_template = blob.download_as_text()
        logger.info("SQL script loaded from GCS.")

        # Replace placeholders in SQL
        sql_query_final = sql_template.replace("{{PROJECT_ID}}", project_id)
        
        # Split into individual statements and execute sequentially
        # Filter out empty strings that might result from splitting
        statements = [s.strip() for s in sql_query_final.split(';') if s.strip()]

        total_rows_affected = 0
        for i, statement in enumerate(statements):
            if not statement:
                continue

            logger.info(f"Executing statement {i+1}/{len(statements)}:\n{statement[:200]}...") # Log first 200 chars

            job_config = bigquery.QueryJobConfig()
            
            # Execute the query
            job = bq_client.query(statement, job_config=job_config)
            job.result() # Waits for the job to complete.
            
            if job.num_dml_affected_rows is not None:
                total_rows_affected += job.num_dml_affected_rows
                logger.info(f"Statement {i+1} completed. Rows affected: {job.num_dml_affected_rows}")
            else:
                logger.info(f"Statement {i+1} (query) completed.")
                # If it's the v_datum query, log its result
                if "v_datum" in statement.lower():
                    for row in job.result():
                        logger.info(f"Determined v_datum: {row.v_datum}")

        logger.info(f"PySpark job completed successfully. Total DML rows affected: {total_rows_affected}")

    except Exception as e:
        logger.error(f"PySpark job failed: {e}", exc_info=True)
        sys.exit(1)

    sys.exit(0)

if __name__ == "__main__":
    main()