# Airflow DAG for job EXIS_SD_APT_RABATT
# Replaces legacy UC4 job DW.DWH_EXIS_SD_APT_RABATT.xml and associated scripts.

import logging
import os
import gzip
from datetime import datetime, timedelta
from io import StringIO

from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryOperator
from airflow.providers.google.cloud.transfers.bigquery_to_gcs import BigQueryToGCSOperator
from airflow.providers.sftp.hooks.sftp import SftpHook
from google.cloud import storage

# --- Airflow Variables Setup (Recommended for external configuration) ---
# Ensure these Airflow Variables are set in your Airflow environment:
# - project_id: Your GCP project ID (e.g., 'your-gcp-project')
# - gcs_export_bucket: GCS bucket for raw and processed exports (e.g., 'your-export-bucket')
# - sftp_conn_id: Airflow SFTP Connection ID (e.g., 'sftp_default')
# - sftp_remote_path: Remote directory on SFTP server (e.g., '/remote/path/to/sftp/exports')
# - bq_dataset_temp: BigQuery dataset for temporary tables (e.g., 'dwh_temp')

# --- Helper function for post-processing and SFTP transfer ---
def _post_process_and_sftp(ti, **context):
    """
    Downloads the raw gzipped CSV from GCS, applies the 'D|' prefix to data rows
    and adds the 'X|' footer, re-compresses the file, uploads the processed file
    back to GCS (for archiving), and then transfers it to the SFTP server.
    """
    logging.info("Starting post-processing and SFTP transfer...")

    # Retrieve Airflow Variables
    project_id = context['var']['value'].get('project_id')
    gcs_export_bucket = context['var']['value'].get('gcs_export_bucket')
    sftp_conn_id = context['var']['value'].get('sftp_conn_id')
    sftp_remote_path = context['var']['value'].get('sftp_remote_path')
    bq_dataset_temp = context['var']['value'].get('bq_dataset_temp')

    if not all([project_id, gcs_export_bucket, sftp_conn_id, sftp_remote_path, bq_dataset_temp]):
        raise ValueError("Missing one or more required Airflow Variables.")

    execution_date = context['logical_date']
    current_datetime_str = execution_date.strftime('%Y%m%d%H%M%S')
    current_date_str = execution_date.strftime('%Y%m%d')

    # Filenames and paths
    base_filename = f"DWHM_APT_RABATTREPORT_{current_datetime_str}.csv.gz"
    gcs_raw_export_blob_name = f"raw/{base_filename}" # Path where BigQuery exported raw data
    gcs_processed_export_blob_name = base_filename # Path for the final processed/archived file in GCS

    # 1. Download raw file from GCS
    client = storage.Client(project=project_id)
    bucket = client.get_bucket(gcs_export_bucket)
    raw_blob = bucket.blob(gcs_raw_export_blob_name)

    if not raw_blob.exists():
        raise FileNotFoundError(f"Raw export file not found in GCS: gs://{gcs_export_bucket}/{gcs_raw_export_blob_name}")

    logging.info(f"Downloading raw file from GCS: gs://{gcs_export_bucket}/{gcs_raw_export_blob_name}")
    gzipped_content = raw_blob.download_as_bytes()

    # Decompress, process, and count rows
    try:
        decompressed_content_str = gzip.decompress(gzipped_content).decode('utf-8')
    except Exception as e:
        logging.error(f"Failed to decompress file. Assuming it's not gzipped or invalid: {e}")
        decompressed_content_str = gzipped_content.decode('utf-8') # Attempt to decode directly

    lines = decompressed_content_str.splitlines()
    processed_lines = []
    row_count = 0

    # Add "D|" prefix to each data row
    for line in lines:
        if line.strip(): # Only process non-empty lines
            processed_lines.append(f"D|{line}")
            row_count += 1

    # Construct the footer line (based on original nawk command)
    # Format: X|<DESTINATION_FILE>|<FROM YYYYMMDD>|NR|V_S_Rabattreport|<SYSDATE YYYYMMDD>
    footer_line = f"X|{base_filename}|{current_date_str}|{row_count}|V_S_Rabattreport|{current_date_str}"
    processed_lines.append(footer_line)

    final_content_str = "\n".join(processed_lines) + "\n" # Ensure trailing newline for file integrity

    # Re-compress the processed content
    compressed_final_content_bytes = gzip.compress(final_content_str.encode('utf-8'))
    logging.info(f"Post-processing complete. Total data rows: {row_count}")

    # 2. Upload processed content back to GCS (this serves as the archive)
    processed_blob = bucket.blob(gcs_processed_export_blob_name)
    processed_blob.upload_from_string(compressed_final_content_bytes, content_type='application/gzip')
    logging.info(f"Processed file uploaded to GCS (archive): gs://{gcs_export_bucket}/{gcs_processed_export_blob_name}")

    # 3. SFTP transfer
    sftp_hook = SftpHook(sftp_conn_id)

    # Use a temporary local file for SFTP transfer
    local_temp_file_path = f"/tmp/{base_filename}"
    with open(local_temp_file_path, "wb") as f:
        f.write(compressed_final_content_bytes)

    sftp_target_path = f"{sftp_remote_path}/{base_filename}"
    logging.info(f"Starting SFTP transfer to {sftp_target_path}")
    sftp_hook.store_file(sftp_target_path, local_temp_file_path)
    logging.info(f"File transferred via SFTP to: {sftp_target_path}")

    # Clean up local temporary file
    os.remove(local_temp_file_path)
    logging.info(f"Local temporary file deleted: {local_temp_file_path}")

# --- DAG Definition ---
with DAG(
    dag_id='dw_dwh_exis_sd_apt_rabatt',
    start_date=days_ago(1),
    schedule_interval='@daily', # Example schedule, adjust based on UC4 context
    catchup=False,
    tags=['exis', 'rabatt', 'bigquery', 'sftp'],
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    }
) as dag:
    # 1. BigQuery Transformation Task
    # Creates a temporary table with the transformed data.
    bq_transform_task = BigQueryOperator(
        task_id='execute_bq_transformation',
        sql='sql/dwh_exis_sd_apt_rabatt_transform.sql',
        destination_dataset_table=
            '`{{ var.value.project_id }}.{{ var.value.bq_dataset_temp }}.export_temp_rabattreport_{{ ds_nodash }}`',
        write_disposition='WRITE_TRUNCATE',
        use_legacy_sql=False,
        gcp_conn_id='google_cloud_default',
        params={
            'project_id': '{{ var.value.project_id }}' # Pass project_id for SQL template
        }
    )

    # 2. Export raw BigQuery data to GCS
    # The output is a gzipped CSV without the 'D|' prefix or footer.
    # It's placed in a 'raw/' subfolder temporarily.
    export_raw_to_gcs_task = BigQueryToGCSOperator(
        task_id='export_raw_to_gcs',
        source_project_dataset_table=
            '`{{ var.value.project_id }}.{{ var.value.bq_dataset_temp }}.export_temp_rabattreport_{{ ds_nodash }}`',
        destination_cloud_storage_uris=[
            'gs://{{ var.value.gcs_export_bucket }}/raw/DWHM_APT_RABATTREPORT_{{ ds_nodash }}_{{ macros.datetime.now().strftime("%H%M%S") }}.csv.gz'
        ],
        compression='GZIP',
        export_format='CSV',
        field_delimiter=',',
        print_header=False, # Do not print header, as nawk output does not expect it.
        gcp_conn_id='google_cloud_default',
    )

    # 3. Post-process the exported file (add D| prefix, footer) and SFTP transfer
    # This Python task handles the nawk-like logic and then sends the file via SFTP.
    post_process_and_sftp_task = PythonOperator(
        task_id='post_process_and_sftp',
        python_callable=_post_process_and_sftp,
        provide_context=True,
        op_kwargs={
            # No direct file paths passed, relies on GCS/SFTP hooks internally.
            # Airflow Variables are accessed within the callable using context['var']['value']
        }
    )

    # Define task dependencies
    bq_transform_task >> export_raw_to_gcs_task >> post_process_and_sftp_task