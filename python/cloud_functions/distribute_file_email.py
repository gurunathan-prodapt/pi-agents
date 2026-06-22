# Cloud Function for distributing exported files via Email.
# Triggered by new files in a specific Cloud Storage bucket.
# Legacy source: vobs/dw_source/isdwh/exporter/is/bin/r_exis_v2

import os
import json
import base64
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email import encoders
import smtplib

from google.cloud import bigquery
from google.cloud import storage

# Initialize clients
bq_client = bigquery.Client()
storage_client = storage.Client()
DATASET_ID = 'dwh_exporter'
DISTRIBUTION_TABLE_ID = 'export_distribution'
READYFILES_TABLE_ID = 'export_readyfiles'

def distribute_file_email(event, context):
    """
    Triggered by a change to a Cloud Storage bucket, specifically when a new
    exported data file is created, or a 'ready file' indicates a file is ready
    for email distribution.
    """
    file_data = event
    bucket_name = file_data['bucket']
    gcs_file_path = file_data['name'] # This is the object name in GCS

    print(f"Received GCS event for file: gs://{bucket_name}/{gcs_file_path}")

    # 1. Look up distribution rules for this file pattern and 'EMAIL' method
    query = f"""
        SELECT distribution_id, recipient, options_json
        FROM `{DATASET_ID}.{DISTRIBUTION_TABLE_ID}`
        WHERE distribution_method = 'EMAIL'
          AND '{gcs_file_path}' LIKE REPLACE(file_pattern, '%', '%%') -- Basic pattern matching
          AND is_active = TRUE
        LIMIT 1
    """
    query_job = bq_client.query(query)
    results = query_job.result()

    email_recipient = None
    email_options = {}
    distribution_id = None

    for row in results:
        distribution_id = row.distribution_id
        email_recipient = row.recipient
        email_options = json.loads(row.options_json) if row.options_json else {}
        break

    if not email_recipient:
        print(f"No active EMAIL distribution rule found for file: {gcs_file_path}")
        return

    # Extract email connection details and message content from options_json or environment variables
    sender_email = email_options.get('sender_email') or os.getenv('SENDER_EMAIL')
    smtp_server = email_options.get('smtp_server') or os.getenv('SMTP_SERVER')
    smtp_port = int(email_options.get('smtp_port') or os.getenv('SMTP_PORT', '587'))
    smtp_username = email_options.get('smtp_username') or os.getenv('SMTP_USERNAME')
    smtp_password = email_options.get('smtp_password') or os.getenv('SMTP_PASSWORD') # Should be from Secret Manager

    if not all([sender_email, smtp_server, smtp_username, smtp_password]):
        print("Email sender details are incomplete. Ensure SENDER_EMAIL, SMTP_SERVER, SMTP_USERNAME, SMTP_PASSWORD are set.")
        raise ValueError("Email sender details missing.")

    subject = email_options.get('subject', f"Exported file: {os.path.basename(gcs_file_path)}")
    body = email_options.get('body', f"Please find the attached exported file: {os.path.basename(gcs_file_path)}")

    # Download file content from GCS
    blob = storage_client.bucket(bucket_name).blob(gcs_file_path)
    local_file_name = f"/tmp/{os.path.basename(gcs_file_path)}"
    blob.download_to_filename(local_file_name)
    print(f"Downloaded {gcs_file_path} to {local_file_name}")

    try:
        # Create the email
        msg = MIMEMultipart()
        msg['From'] = sender_email
        msg['To'] = email_recipient
        msg['Subject'] = subject
        msg.attach(MIMEText(body, 'plain'))

        # Attach the file
        with open(local_file_name, 'rb') as attachment:
            part = MIMEBase('application', 'octet-stream')
            part.set_payload(attachment.read())
        encoders.encode_base64(part)
        part.add_header('Content-Disposition',
                        f"attachment; filename= {os.path.basename(gcs_file_path)}")
        msg.attach(part)

        # Send the email
        with smtplib.SMTP(smtp_server, smtp_port) as server:
            server.starttls() # Secure the connection
            server.login(smtp_username, smtp_password)
            server.send_message(msg)
        print(f"Successfully sent email with attachment {local_file_name} to {email_recipient}.")

        # Update export_readyfiles status
        update_query = f"""
            UPDATE `{DATASET_ID}.{READYFILES_TABLE_ID}`
            SET status = 'DISTRIBUTED', distribution_end_time = CURRENT_TIMESTAMP()
            WHERE gcs_path = 'gs://{bucket_name}/{gcs_file_path}'
              AND status = 'CREATED'
        """
        bq_client.query(update_query).result()
        print(f"Updated status for gs://{bucket_name}/{gcs_file_path} in export_readyfiles to 'DISTRIBUTED'.")

    except Exception as e:
        print(f"Email distribution failed for {gcs_file_path}: {e}")
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