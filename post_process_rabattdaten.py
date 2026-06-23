# Migrated from vobs/dw_source/isdwh/exporter/apt/cfg/h_exis_apt_rabattdaten.var
# and r_exis_v2 logic
# Job: EXIS_SD_APT_RABATT
#
# This script reads a CSV file from GCS, applies nawk-like post-processing (
# specifically adding a trailer line), compresses the file using gzip, and
# uploads the result back to GCS.

import pandas as pd
import gzip
import io
from datetime import datetime
from airflow.providers.google.cloud.hooks.gcs import GCSHook

def post_process_and_compress_rabattdaten(
    input_gcs_path: str,
    output_gcs_bucket: str,
    output_gcs_prefix: str,
    job_id: str,
    separator: str,
    ds_nodash: str, # Airflow execution date (e.g., '20231027')
    **context
):
    """
    Reads a CSV from GCS, adds a trailer line, compresses it with gzip,
    and uploads the compressed file to GCS.

    Args:
        input_gcs_path (str): Full GCS path to the input CSV file.
        output_gcs_bucket (str): GCS bucket for the output file.
        output_gcs_prefix (str): GCS prefix for the output file
                                  (e.g., 'DWH/DWH_KERN/RELEASEWECHSEL/').
        job_id (str): The job identifier, used in the trailer line.
        separator (str): The CSV separator.
        ds_nodash (str): Airflow's execution date, formatted as YYYYMMDD.
        context: Airflow context dictionary.
    """
    gcs_hook = GCSHook()

    # Get input file name (without path) for the trailer line
    input_filename = input_gcs_path.split('/')[-1]

    # Dynamically generate output filename with timestamp
    current_datetime_str = datetime.now().strftime('%Y%m%d%H%M%S')
    current_date_str = datetime.now().strftime('%Y%m%d')
    output_filename = f"DWHM_APT_RABATTREPORT_{current_datetime_str}.csv.gz"
    full_output_gcs_path = f"{output_gcs_prefix}/{output_filename}"

    # Download CSV content from GCS
    try:
        csv_content = gcs_hook.download(bucket_name=input_gcs_path.split('/')[2],
                                        object_name='/'.join(input_gcs_path.split('/')[3:])).decode('utf-8')
    except Exception as e:
        raise Exception(f"Failed to download input CSV from {input_gcs_path}: {e}")

    # Read data using pandas from the downloaded content
    df = pd.read_csv(io.StringIO(csv_content), sep=separator, dtype=str) # Read as string to preserve leading zeros

    # Generate trailer line based on the original nawk logic
    # Trailer format: X|DESTINATION_FILE|FROM YYYYMMDD|NR|V_S_Rabattreport|SYSDATE YYYYMMDD
    # V_S_Rabattreport is assumed to be a static value from the original script context
    trailer_line = (
        f"X|{output_filename.replace('.gz', '')}|{ds_nodash}|{len(df)}|V_S_Rabattreport|{current_date_str}"
    )

    # Prepare buffer for CSV content + trailer
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, sep=separator, index=False, header=True)
    csv_buffer.write("\n" + trailer_line + "\n") # Ensure a newline after the last data row, then trailer, then another newline

    # Compress the content in memory
    compressed_buffer = io.BytesIO()
    with gzip.GzipFile(fileobj=compressed_buffer, mode='wb') as gz_file:
        gz_file.write(csv_buffer.getvalue().encode('utf-8'))
    compressed_buffer.seek(0)

    # Upload compressed file to GCS
    try:
        gcs_hook.upload(
            bucket_name=output_gcs_bucket,
            object_name=full_output_gcs_path,
            data=compressed_buffer.getvalue(),
            mime_type='application/gzip'
        )
        print(f"Successfully uploaded processed and compressed file to gs://{output_gcs_bucket}/{full_output_gcs_path}")
    except Exception as e:
        raise Exception(f"Failed to upload compressed file to gs://{output_gcs_bucket}/{full_output_gcs_path}: {e}")

    # Push the full GCS path of the generated file to XCom for downstream tasks
    context['ti'].xcom_push(key='processed_compressed_gcs_path', value=f"gs://{output_gcs_bucket}/{full_output_gcs_path}")