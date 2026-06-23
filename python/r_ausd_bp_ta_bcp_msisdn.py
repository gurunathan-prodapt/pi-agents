# Python script for DW.BERT_AUSD_BP_TA_BCP_MSISDN
# Replaces: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bcp_msisdn.ksh
#           vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bcp_msisdn.ksh
#           vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_bp_ta_bcp_msisdn.sql

import argparse
import logging
import sys
from datetime import datetime

from google.cloud import bigquery

# Configure logging
logging.basicConfig(level=logging.INFO, stream=sys.stdout,
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_bq_client():
    """Returns a BigQuery client."""
    return bigquery.Client()

def execute_bigquery_query(client, query):
    """Executes a BigQuery query and handles potential errors."""
    try:
        query_job = client.query(query)
        # Wait for the job to complete
        results = query_job.result()
        logger.info(f"BigQuery query executed successfully. Job ID: {query_job.job_id}")
        return results
    except Exception as e:
        logger.error(f"Error executing BigQuery query: {e}")
        raise

def main():
    parser = argparse.ArgumentParser(
        description="Processes MSISDN basic product data in BigQuery."
    )
    parser.add_argument(
        "--gcp_project",
        type=str,
        required=True,
        help="Google Cloud Project ID."
    )
    parser.add_argument(
        "--bigquery_dataset",
        type=str,
        required=True,
        help="BigQuery dataset name for data tables (e.g., your_bigquery_dataset)."
    )
    parser.add_argument(
        "--metadata_dataset",
        type=str,
        default="isbert_schema",
        help="BigQuery dataset name for metadata tables (e.g., isbert_schema)."
    )
    parser.add_argument(
        "--stichtag",
        type=str,
        help="Processing date in YYYYMMDD format. If not provided, will be determined from metadata."
    )
    parser.add_argument(
        "--wiederanlaufwert",
        type=str,
        help="Wiederanlaufwert (restart value) - currently unused, kept for compatibility."
    )

    args = parser.parse_args()

    gcp_project = args.gcp_project
    bigquery_dataset = args.bigquery_dataset
    metadata_dataset = args.metadata_dataset
    stichtag_arg = args.stichtag

    logger.info(f"Starting data processing for GCP Project: {gcp_project}, Data Dataset: {bigquery_dataset}, Metadata Dataset: {metadata_dataset}")

    client = get_bq_client()

    # 1. Retrieve v_datum (equivalent to s_datum in Oracle)
    # The Oracle query:
    # SELECT NVL(TO_CHAR(MAX(m.timecreated),'YYYYMMDD'),'19000101') AS s_datum
    # FROM isbert_schema.dwtk_meldungen m
    # WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE';

    # If stichtag is provided, use it, otherwise derive from metadata
    if stichtag_arg:
        v_datum = stichtag_arg
        logger.info(f"Using Stichtag from arguments: {v_datum}")
    else:
        get_datum_query = f"""
            SELECT
                COALESCE(FORMAT_DATE('%Y%m%d', MAX(timecreated)), '19000101')
            FROM
                `{gcp_project}.{metadata_dataset}.dwtk_meldungen`
            WHERE
                job_kennung = 'BERT_DROP_TEMP_TABLE'
        """
        logger.info(f"Retrieving v_datum from BigQuery metadata. Query: {get_datum_query}")
        try:
            datum_results = execute_bigquery_query(client, get_datum_query)
            for row in datum_results:
                v_datum = row[0]
                break
            logger.info(f"Determined v_datum: {v_datum}")
        except Exception as e:
            logger.error(f"Failed to retrieve v_datum from metadata: {e}")
            sys.exit(1)

    # 2. Truncate target table
    truncate_query = f"""
        TRUNCATE TABLE `{gcp_project}.{bigquery_dataset}.sof_ta_bcp_msisdn`;
    """
    logger.info(f"Truncating target table: {truncate_query}")
    try:
        execute_bigquery_query(client, truncate_query)
        logger.info(f"Successfully truncated `{gcp_project}.{bigquery_dataset}.sof_ta_bcp_msisdn`.")
    except Exception as e:
        logger.error(f"Failed to truncate table: {e}")
        sys.exit(1)

    # 3. Insert transformed data
    insert_query = f"""
        INSERT INTO `{gcp_project}.{bigquery_dataset}.sof_ta_bcp_msisdn`
        (CNTRCT_ID, BPR_ID, CNTRCT_ID_REF, TN_TEL_MSISDN)
        SELECT DISTINCT
               bp.cntrct_id,
               bp.bpr_id,
               bp.cntrct_id_ref,
               rn.tn_tel_msisdn
        FROM `{gcp_project}.{bigquery_dataset}.sof_ta_bpr_bcp` AS bp
        JOIN `{gcp_project}.{bigquery_dataset}.sof_ta_rn_vertrag` AS rn
        ON bp.cntrct_id_ref = rn.cntrct_id;
    """
    logger.info(f"Inserting data into target table. Query: {insert_query}")
    try:
        execute_bigquery_query(client, insert_query)
        logger.info(f"Successfully inserted data into `{gcp_project}.{bigquery_dataset}.sof_ta_bcp_msisdn`.")
    except Exception as e:
        logger.error(f"Failed to insert data: {e}")
        sys.exit(1)

    logger.info("Data processing completed successfully.")

if __name__ == "__main__":
    main()