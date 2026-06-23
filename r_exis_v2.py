# Migrated from legacy configuration file: vobs/.../h_exis_apt_nna_voice.var
# Migrated from assumed functionality of shell script: r_exis_v2
# Job: EXIS_SD_APT_NNA_VOIC
# Description: PySpark job for data post-processing, header/trailer addition, and gzip compression.

import argparse
import logging
import os
import gzip
from datetime import datetime
from google.cloud import bigquery
from google.cloud import storage

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def add_header_trailer_and_compress(
    input_file_path: str,
    output_bucket: str,
    output_prefix: str,
    yyyymm: str,
    process_date_str: str,
) -> str:
    """
    Adds a custom header and trailer to the CSV data, then gzipps it,
    and uploads to Google Cloud Storage.
    """
    logging.info(f"Starting post-processing for YYYYMM: {yyyymm}")

    client = storage.Client()
    bucket = client.bucket(output_bucket)

    # Output file name format: DWHM_APT_NNA_Daten_<SYSDATE YYYYMMDDHH24MISS>.csv.gz
    timestamp_for_filename = datetime.now().strftime("%Y%m%d%H%M%S")
    output_blob_name = f"{output_prefix}/DWHM_APT_NNA_Daten_{timestamp_for_filename}.csv.gz"
    output_blob = bucket.blob(output_blob_name)

    # Build header and trailer
    # Example header/trailer format based on common ETL patterns:
    # Header: #H;YYYYMM;PROCESS_DATE
    # Trailer: #T;RECORD_COUNT;YYYYMM;PROCESS_DATE
    header = f"#H;{yyyymm};{process_date_str}\n"

    record_count = 0
    temp_uncompressed_file = f"/tmp/uncompressed_output_{timestamp_for_filename}.csv"
    
    try:
        with open(input_file_path, 'r') as infile, open(temp_uncompressed_file, 'w') as outfile:
            outfile.write(header)
            for line in infile:
                outfile.write(line)
                record_count += 1 # Count data rows
            trailer = f"#T;{record_count};{yyyymm};{process_date_str}\n"
            outfile.write(trailer)
        
        logging.info(f"Added header and trailer. Total data rows: {record_count}")

        # Compress the file
        with open(temp_uncompressed_file, 'rb') as f_in:
            with gzip.open(f"{temp_uncompressed_file}.gz", 'wb') as f_out:
                f_out.writelines(f_in)
        
        logging.info(f"File compressed: {temp_uncompressed_file}.gz")

        # Upload to GCS
        output_blob.upload_from_filename(f"{temp_uncompressed_file}.gz")
        logging.info(f"Uploaded {output_blob_name} to gs://{output_bucket}")

    except Exception as e:
        logging.error(f"Error during post-processing: {e}")
        raise
    finally:
        # Clean up temporary files
        if os.path.exists(input_file_path):
            os.remove(input_file_path)
        if os.path.exists(temp_uncompressed_file):
            os.remove(temp_uncompressed_file)
        if os.path.exists(f"{temp_uncompressed_file}.gz"):
            os.remove(f"{temp_uncompressed_file}.gz")

    return f"gs://{output_bucket}/{output_blob_name}"

def run_bigquery_export(
    project_id: str,
    query_file_path: str,
    yyyymm: str,
    temp_output_dir: str,
    dataset: str,
) -> str:
    """
    Executes a BigQuery SQL query, exports results to a temporary GCS location,
    then downloads the file locally.
    Returns the local path to the downloaded CSV file.
    """
    logging.info(f"Executing BigQuery export for YYYYMM: {yyyymm}")

    client = bigquery.Client(project=project_id)

    # Read the SQL query from the file
    with open(query_file_path, 'r') as f:
        query = f.read()

    # Parameterize the query
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("FROM_YYYYMM", "INT64", int(yyyymm)),
        ]
    )

    # Define a temporary GCS path for BigQuery export
    export_uri = f"gs://{temp_output_dir}/bq_export_{yyyymm}_{datetime.now().strftime('%Y%m%d%H%M%S')}-*.csv"
    
    # Create a temporary table to hold the query results
    temp_table_id = f"{dataset}.temp_exis_sd_apt_nna_voic_{yyyymm}_{datetime.now().strftime('%Y%m%d%H%M%S')}"
    
    # Run the query and store results in a temporary table
    query_job = client.query(query, job_config=job_config)
    query_job.destination = temp_table_id
    query_job.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
    
    logging.info(f"Running BigQuery query and saving to temporary table: {temp_table_id}")
    query_job.result() # Wait for the query to complete
    logging.info(f"BigQuery query completed. Results in {temp_table_id}")

    # Export the temporary table to GCS
    extract_job_config = bigquery.ExtractJobConfig()
    extract_job_config.destination_format = bigquery.DestinationFormat.CSV
    extract_job_config.print_header = False # Header will be added by post-processing script

    extract_job = client.extract_table(
        temp_table_id,
        export_uri,
        job_config=extract_job_config,
        location="US",  # Specify your BigQuery dataset's location
    )
    logging.info(f"Exporting data from {temp_table_id} to {export_uri}")
    extract_job.result() # Wait for the extract job to complete
    logging.info(f"Export to GCS completed.")

    # Download the exported CSV file(s) from GCS to a local temporary directory
    local_temp_dir = "/tmp/bq_download"
    os.makedirs(local_temp_dir, exist_ok=True)
    
    gcs_bucket_name = temp_output_dir.split('/')[0]
    gcs_prefix = '/'.join(temp_output_dir.split('/')[1:])
    if not gcs_prefix.endswith('/'):
      gcs_prefix += '/'

    storage_client = storage.Client()
    bucket = storage_client.bucket(gcs_bucket_name)
    blobs = bucket.list_blobs(prefix=gcs_prefix)

    downloaded_files = []
    for blob in blobs:
        if blob.name.endswith('.csv'):
            local_path = os.path.join(local_temp_dir, os.path.basename(blob.name))
            blob.download_to_filename(local_path)
            downloaded_files.append(local_path)
            logging.info(f"Downloaded {blob.name} to {local_path}")
            
    # Assuming only one CSV file is exported. If multiple, they need to be concatenated.
    if not downloaded_files:
        raise FileNotFoundError(f"No CSV files found in {export_uri}")
    
    # For simplicity, if multiple files, concatenate them. In production, consider export to single file or handle multipart.
    if len(downloaded_files) > 1:
        logging.warning("Multiple CSV files exported from BigQuery. Concatenating them.")
        concatenated_file_path = os.path.join(local_temp_dir, f"concatenated_bq_export_{yyyymm}.csv")
        with open(concatenated_file_path, 'w') as outfile:
            for fname in downloaded_files:
                with open(fname) as infile:
                    outfile.write(infile.read())
                os.remove(fname) # Clean up individual parts
        return concatenated_file_path
    else:
        return downloaded_files[0]


def main():
    parser = argparse.ArgumentParser(description="EXIS SD APT NNA VOIC PySpark/Python Job for BigQuery Export Post-Processing")
    parser.add_argument("--yyyymm", required=True, help="Processing month in YYYYMM format.")
    parser.add_argument("--project_id", required=True, help="Google Cloud Project ID.")
    parser.add_argument("--bq_query_file", required=True, help="Path to the BigQuery SQL query file.")
    parser.add_argument("--gcs_temp_bucket", required=True, help="GCS bucket for temporary BigQuery exports (e.g., your-bucket/temp_path).")
    parser.add_argument("--gcs_output_bucket", required=True, help="GCS bucket for final gzipped output (e.g., your-bucket).")
    parser.add_argument("--gcs_output_prefix", required=True, help="GCS path prefix within the output bucket (e.g., 'exis_data/nna_voice').")
    parser.add_argument("--bq_dataset", required=True, help="BigQuery dataset for temporary tables.")

    args = parser.parse_args()

    # Get current date for the header/trailer (equivalent to SYSDATE YYYYMMDDHH24MISS)
    process_date_str = datetime.now().strftime("%Y%m%d%H%M%S")

    local_csv_file = None
    try:
        # Step 1: Run BigQuery query and get local CSV file path
        local_csv_file = run_bigquery_export(
            project_id=args.project_id,
            query_file_path=args.bq_query_file,
            yyyymm=args.yyyymm,
            temp_output_dir=args.gcs_temp_bucket,
            dataset=args.bq_dataset,
        )

        # Step 2: Add header/trailer, compress, and upload to final GCS location
        final_gcs_path = add_header_trailer_and_compress(
            input_file_path=local_csv_file,
            output_bucket=args.gcs_output_bucket,
            output_prefix=args.gcs_output_prefix,
            yyyymm=args.yyyymm,
            process_date_str=process_date_str,
        )
        logging.info(f"Successfully processed and uploaded to: {final_gcs_path}")

    except Exception as e:
        logging.error(f"Job failed: {e}", exc_info=True)
        raise

if __name__ == "__main__":
    main()