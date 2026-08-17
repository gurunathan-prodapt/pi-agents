# -*- coding: utf-8 -*-
"""
Python wrapper for executing migrated Oracle-to-BigQuery SQL script for d_abtn_x_smart_kubi.
"""

import argparse
import sys
import os
import logging
from google.cloud import bigquery
from google.cloud.exceptions import GoogleCloudError

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("d_abtn_x_smart_kubi_wrapper")

def main():
    parser = argparse.ArgumentParser(
        description="Run BigQuery SQL script for d_abtn_x_smart_kubi migration."
    )
    # Global environment parameters
    parser.add_argument("--project-id", dest="project_id", default=os.environ.get("GCP_PROJECT"), help="GCP Project ID")
    parser.add_argument("--dataset-id", dest="dataset_id", default=os.environ.get("BQ_DATASET"), help="BigQuery Dataset ID")
    
    # Job specific parameters
    parser.add_argument("--sql-file", dest="sql_file", default="local/home/gurunathan_t/kubi/d_abtn_x_smart_kubi.sql", help="Path to the BigQuery SQL file")
    parser.add_argument("--p-eintragsnr", dest="p_eintragsnr", type=int, help="Entry log number (EintragsNr)", required=True)
    parser.add_argument("--p-monats-id", dest="p_monats_id", type=int, help="Monthly partition ID (YYYYMM)", required=True)

    args = parser.parse_args()

    # Validate parameters
    if not args.project_id:
        logger.error("GCP Project ID is required. Pass --project-id or set GCP_PROJECT environment variable.")
        sys.exit(1)
    if not args.dataset_id:
        logger.error("BigQuery Dataset ID is required. Pass --dataset-id or set BQ_DATASET environment variable.")
        sys.exit(1)

    logger.info("Project ID: %s", args.project_id)
    logger.info("Dataset ID: %s", args.dataset_id)
    logger.info("Monats ID: %d", args.p_monats_id)
    logger.info("EintragsNr: %d", args.p_eintragsnr)

    # Read the SQL query from file
    try:
        with open(args.sql_file, "r", encoding="utf-8") as f:
            sql_content = f.read()
    except Exception as e:
        logger.error("Failed to read SQL file %s: %s", args.sql_file, str(e))
        sys.exit(1)

    # Initialize BigQuery Client
    try:
        client = bigquery.Client(project=args.project_id)
    except Exception as e:
        logger.error("Failed to initialize BigQuery client: %s", str(e))
        sys.exit(1)

    # Build ScalarQueryParameters
    query_parameters = [ 
        bigquery.ScalarQueryParameter("p_monats_id", "INT64", args.p_monats_id),
        bigquery.ScalarQueryParameter("p_eintragsnr", "INT64", args.p_eintragsnr)
    ]

    # Execute SQL script with default dataset configured
    logger.info("Submitting BigQuery job...")
    job_config = bigquery.QueryJobConfig(
        query_parameters=query_parameters,
        use_legacy_sql=False,
        default_dataset=f"{args.project_id}.{args.dataset_id}"
    )

    try:
        query_job = client.query(sql_content, job_config=job_config)
        logger.info("BigQuery job submitted successfully. Job ID: %s", query_job.job_id)
        
        # Wait for the query to complete
        results = query_job.result()
        logger.info("BigQuery job completed successfully.")
        
        # Print output/log if any from the script's SELECT statements
        for row in results:
            row_dict = dict(row.items())
            logger.info("QueryResult: %s", str(row_dict))

    except GoogleCloudError as gce:
        logger.error("Google Cloud Error occurred during BigQuery job execution: %s", str(gce))
        sys.exit(2)
    except Exception as e:
        logger.error("An unexpected error occurred: %s", str(e))
        sys.exit(3)

if __name__ == "__main__":
    main()