# Cloud Function for distributing exported files via SFTP.
# Triggered by new files in a specific Cloud Storage bucket.
# Legacy source: vobs/dw_source/isdwh/exporter/is/bin/r_exis_v2

import os
import json
import paramiko
from google.cloud import bigquery
from google.cloud import storage

# Initialize clients
bq_client = bigquery.Client()
storage_client = storage.Client()
DATASET_ID = 'dwh_exporter'
DISTRIBUTION_TABLE_ID = 'export_distribution'
READYFILES_TABLE_ID = 'export_readyfiles'

def distribute_file_sftp(event, context):
    """
    Triggered by a change to a Cloud Storage bucket, specifically when a new
    exported data file is created, or a 'ready file' indicates a file is ready
    for SFTP distribution.
    """
    file_data = event
    bucket_name = file_data['bucket']
    gcs_file_path = file_data['name'] # This is the object name in GCS

    print(f"Received GCS event for file: gs://{bucket_name}/{gcs_file_path}")

    # Determine job_name and actual data file name from gcs_file_path if needed
    # For simplicity, let's assume the distribution rule specifies the file_pattern.
    # We might need to query export_readyfiles to get job_id/run_id for logging.

    # 1. Look up distribution rules for this file pattern and 'SFTP' method
    query = f"""
        SELECT distribution_id, target_path, options_json
        FROM `{DATASET_ID}.{DISTRIBUTION_TABLE_ID}`
        WHERE distribution_method = 'SFTP'
          AND '{gcs_file_path}' LIKE REPLACE(file_pattern, '%', '%%') -- Basic pattern matching
          AND is_active = TRUE
        LIMIT 1
    """
    query_job = bq_client.query(query)
    results = query_job.result()

    sftp_target_path = None
    sftp_options = {}
    distribution_id = None

    for row in results:
        distribution_id = row.distribution_id
        sftp_target_path = row.target_path
        sftp_options = json.loads(row.options_json) if row.options_json else {}
        break

    if not sftp_target_path:
        print(f"No active SFTP distribution rule found for file: {gcs_file_path}")
        return

    # Extract SFTP connection details from options_json or environment variables
    sftp_host = sftp_options.get('sftp_host') or os.getenv('SFTP_HOST')
    sftp_port = int(sftp_options.get('sftp_port') or os.getenv('SFTP_PORT', '22'))
    sftp_user = sftp_options.get('sftp_user') or os.getenv('SFTP_USER')
    # Private key should be securely stored, e.g., in Secret Manager
    sftp_private_key_path = sftp_options.get('sftp_private_key_path') or os.getenv('SFTP_PRIVATE_KEY_PATH')

    if not all([sftp_host, sftp_user, sftp_private_key_path]):
        print("SFTP connection details are incomplete. Ensure SFTP_HOST, SFTP_USER, and SFTP_PRIVATE_KEY_PATH are set.")
        raise ValueError("SFTP connection details missing.")

    # Download file content from GCS
    blob = storage_client.bucket(bucket_name).blob(gcs_file_path)
    local_file_name = f"/tmp/{os.path.basename(gcs_file_path)}"
    blob.download_to_filename(local_file_name)
    print(f"Downloaded {gcs_file_path} to {local_file_name}")

    try:
        # Establish SFTP connection
        private_key = paramiko.RSAKey.from_private_key_file(sftp_private_key_path)
        transport = paramiko.Transport((sftp_host, sftp_port))
        transport.connect(username=sftp_user, pkey=private_key)
        sftp = paramiko.SFTPClient.from_transport(transport)

        remote_path = os.path.join(sftp_target_path, os.path.basename(gcs_file_path))
        sftp.put(local_file_name, remote_path)
        print(f"Successfully transferred {local_file_name} to {remote_path} via SFTP.")

        sftp.close()
        transport.close()

        # Update export_readyfiles status
        update_query = f"""
            UPDATE `{DATASET_ID}.{READYFILES_TABLE_ID}`
            SET status = 'DISTRIBUTED', distribution_end_time = CURRENT_TIMESTAMP()
            WHERE gcs_path = 'gs://{bucket_name}/{gcs_file_path}'
              AND status = 'CREATED' -- Only update if not already processed
        """
        bq_client.query(update_query).result()
        print(f"Updated status for gs://{bucket_name}/{gcs_file_path} in export_readyfiles to 'DISTRIBUTED'.")

    except Exception as e:
        print(f"SFTP distribution failed for {gcs_file_path}: {e}")
        # Optionally log to export_audit or update export_readyfiles with FAILED status
        update_query = f"""
            UPDATE `{DATASET_ID}.{READYFILES_TABLE_ID}`
            SET status = 'FAILED_DISTRIBUTION', distribution_end_time = CURRENT_TIMESTAMP(),
                metadata_json = JSON_SET(COALESCE(metadata_json, JSON '{}'), '$.distribution_error', '{str(e).replace("'", "\\'")}')
            WHERE gcs_path = 'gs://{bucket_name}/{gcs_file_path}'
              AND status = 'CREATED'
        """
        bq_client.query(update_query).result()
        raise

    finally:
        # Clean up local file
        if os.path.exists(local_file_name):
            os.remove(local_file_name)
            print(f"Cleaned up local file: {local_file_name}")