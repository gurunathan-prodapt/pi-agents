#!/usr/bin/env python3
import os
import sys
import time
from datetime import datetime
from google.cloud import storage
from google.cloud import bigquery

def log(message):
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {message}")

def gcs_file_exists(bucket_name, blob_name):
    try:
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        return blob.exists()
    except Exception as e:
        log(f"WARNING: Error checking GCS file existence: {e}")
        return False

def execute_bq_sql_file(file_path, params=None):
    client = bigquery.Client()
    with open(file_path, "r") as f:
        sql_content = f.read()
    
    # Setup query parameters if provided
    job_config = bigquery.QueryJobConfig()
    if params:
        query_params = []
        for name, val in params.items():
            query_params.append(bigquery.ScalarQueryParameter(name, "STRING", val))
        job_config.query_parameters = query_params

    query_job = client.query(sql_content, job_config=job_config)
    query_job.result()  # Wait for query to complete

def main():
    # Step 1: Initialization and Environment Setup
    retail_home = os.environ.get("RETAIL_HOME", "/opt/etl/sales")
    
    run_date = os.environ.get("RUN_DATE")
    if not run_date:
        log("ERROR: RUN_DATE environment variable is not defined.")
        sys.exit(1)

    # Global environment-wide values
    gcs_bucket = os.environ.get("GCS_BUCKET")
    if not gcs_bucket:
        log("ERROR: GCS_BUCKET environment variable is not defined.")
        sys.exit(1)

    # Target path in GCS replaces local filesystem checking
    source_marker_blob = f"inbound/pos_feed_{run_date}.done"

    try:
        max_wait_checks = int(os.environ.get("MAX_WAIT_CHECKS", 10))
    except ValueError:
        max_wait_checks = 10
        
    try:
        wait_interval_seconds = int(os.environ.get("WAIT_INTERVAL_SECONDS", 60))
    except ValueError:
        wait_interval_seconds = 60

    # Step 2: Poll for upstream POS feed landing marker
    check = 1
    while check <= max_wait_checks:
        if gcs_file_exists(gcs_bucket, source_marker_blob):
            log(f"Source feed marker found on check {check}/{max_wait_checks}")
            break
        
        log(f"Source feed marker not yet present (check {check}/{max_wait_checks}) - waiting {wait_interval_seconds}s")
        
        if check == max_wait_checks:
            log(f"ERROR: source feed marker never appeared after {max_wait_checks} checks - aborting")
            sys.exit(1)
            
        time.sleep(wait_interval_seconds)
        check += 1

    # Step 3: Execute product master dimension load (SCD Type 2 Merge)
    log("Loading product master dimension (SCD2 merge)")
    sql_script_product = os.path.join(retail_home, "sales", "d_product_master_load.sql")
    
    try:
        execute_bq_sql_file(sql_script_product)
    except Exception as e:
        log("ERROR: product master dimension load failed")
        sys.exit(2)

    # Step 4: Execute daily sales transaction extract
    log(f"Extracting daily sales transactions for {run_date}")
    sql_script_sales = os.path.join(retail_home, "sales", "d_daily_sales_extract.sql")
    
    try:
        # Pass parameters expected by the SQL script as per reviewer feedback
        execute_bq_sql_file(sql_script_sales, params={"input_date": run_date, "gcp_project": os.environ.get("GCP_PROJECT", ""), "bq_dataset": os.environ.get("BQ_DATASET", "")})
    except Exception as e:
        rc = 3
        log(f"ERROR: daily sales extract failed with rc={rc}")
        sys.exit(3)

    # Step 5: Successful exit
    log(f"Product master refreshed and daily sales extracted for {run_date}")
    sys.exit(0)

if __name__ == "__main__":
    sys.exit(main())