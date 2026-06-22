# Python Post-processing Script
# Legacy Source: h_exis_apt_rabattdaten.var (nawk logic), gzip
# Job: EXIS_SD_APT_RABATT

import pandas as pd
import gzip
import io
import argparse
from datetime import datetime
from google.cloud import storage

def post_process_and_compress(input_gcs_path, output_gcs_path, project_id):
    """
    Reads a CSV from GCS, applies nawk-like post-processing (header/footer),
    and compresses the output to GZIP, writing it back to GCS.
    """
    storage_client = storage.Client(project=project_id)

    # Parse GCS paths
    input_bucket_name, input_blob_name = input_gcs_path.replace("gs://", "").split("/", 1)
    output_bucket_name, output_blob_name = output_gcs_path.replace("gs://", "").split("/", 1)

    input_bucket = storage_client.bucket(input_bucket_name)
    input_blob = input_bucket.blob(input_blob_name)

    # Read CSV from GCS into a DataFrame
    # Assuming the BigQuery export produces CSV without header by default for simplicity
    # If BigQuery exports with header, adjust pd.read_csv to include header=0
    df = pd.read_csv(io.BytesIO(input_blob.download_as_bytes()), header=None)

    # Nawk-like logic: add custom header and footer
    # Header format: X|<DESTINATION_FILE>|<FROM YYYYMMDD>|NR|V_S_Rabattreport|<SYSDATE YYYYMMDD>
    # DESTINATION_FILE will be the base name of the output_gcs_path
    # FROM YYYYMMDD and SYSDATE YYYYMMDD are assumed to be current date or execution date.
    # We'll use current date for simplicity, Airflow can pass actual execution date.
    
    current_date_yyyymmdd = datetime.now().strftime("%Y%m%d")
    output_filename = output_blob_name.split('/')[-1] # Get filename from output path

    # The design document indicates a `|` separator. Let's assume the CSV also uses `|` for consistency
    # and to simplify the header/footer structure.
    separator = "|" # This should ideally come from Airflow Variable or config.

    header_line = f"X{separator}{output_filename}{separator}{current_date_yyyymmdd}{separator}{df.shape[0]}{separator}V_S_Rabattreport{separator}{current_date_yyyymmdd}\n"
    footer_line = f"E{separator}{output_filename}{separator}{current_date_yyyymmdd}{separator}{df.shape[0]}{separator}V_S_Rabattreport{separator}{current_date_yyyymmdd}"

    # Convert DataFrame to CSV string (without header/index)
    output_csv_buffer = io.StringIO()
    df.to_csv(output_csv_buffer, index=False, header=False, sep=separator, encoding='utf-8')
    csv_content = output_csv_buffer.getvalue()

    # Prepend header and append footer
    final_content = header_line + csv_content + footer_line

    # Compress to GZIP
    compressed_output = io.BytesIO()
    with gzip.GzipFile(fileobj=compressed_output, mode='wb') as gz:
        gz.write(final_content.encode('utf-8'))
    
    # Upload to GCS
    output_bucket = storage_client.bucket(output_bucket_name)
    output_blob = output_bucket.blob(output_blob_name)
    output_blob.upload_from_string(compressed_output.getvalue(), content_type='application/gzip')

    print(f"Successfully processed and uploaded to {output_gcs_path}")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Post-process and compress discount data.")
    parser.add_argument("--input_gcs_path", required=True, help="GCS path to the input CSV file.")
    parser.add_argument("--output_gcs_path", required=True, help="GCS path for the compressed output .gz file.")
    parser.add_argument("--project_id", required=True, help="Your GCP Project ID.")
    args = parser.parse_args()

    post_process_and_compress(args.input_gcs_path, args.output_gcs_path, args.project_id)