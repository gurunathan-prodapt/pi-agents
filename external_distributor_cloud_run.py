"""
Cloud Run service for external file distribution (SFTP/SCP and Email).
Replaces: 'scp', 'sftp', 'mailx' commands in vobs/dw_source/isdwh/exporter/is/bin/r_exis_v2
This service is triggered by Airflow via a HTTP request (or Pub/Sub) and handles the
actual interaction with external systems.
"""

import os
import functions_framework
import logging
import json
from google.cloud import storage
from google.cloud import secretmanager
import paramiko # For SFTP/SCP
import smtplib
from email.mime.text import MIMEText

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize GCP clients
storage_client = storage.Client()
secretmanager_client = secretmanager.SecretManagerServiceClient()

# Helper function to get secrets from Secret Manager
def get_secret(secret_name):
    project_id = os.environ.get("GCP_PROJECT_ID")
    if not project_id:
        logger.error("GCP_PROJECT_ID environment variable not set.")
        raise ValueError("GCP_PROJECT_ID environment variable not set.")
    
    secret_path = f"projects/{project_id}/secrets/{secret_name}/versions/latest"
    response = secretmanager_client.access_secret_version(request={"name": secret_path})
    return response.payload.data.decode("UTF-8")

def update_distribution_status(queue_id, status, error_message=None):
    """Updates the status of a distribution task in BigQuery."""
    # This function would require a BigQuery client and appropriate DML
    # For a Cloud Run service, typically you'd trigger another service (e.g., Pub/Sub or dedicated BQ service)
    # or use a BigQuery client directly if allowed by IAM policies.
    logger.info(f"Updating distribution queue_id {queue_id} to status {status}. Error: {error_message}")
    # Example placeholder:
    # from google.cloud import bigquery
    # bq_client = bigquery.Client()
    # update_sql = f"""
    #    UPDATE `{os.environ.get('BIGQUERY_DATASET')}.exporter_distribution_queue`
    #    SET status = '{status}', processed_at = CURRENT_TIMESTAMP(), error_message = '{error_message}'
    #    WHERE queue_id = '{queue_id}';
    # """
    # bq_client.query(update_sql).result()
    pass


def handle_sftp_distribution(queue_id, gcs_file_path, distribution_config):
    logger.info(f"Handling SFTP distribution for {gcs_file_path}")
    
    try:
        sftp_host = distribution_config.get("sftp_host")
        sftp_port = distribution_config.get("sftp_port", 22)
        sftp_user = distribution_config.get("sftp_user")
        sftp_target_path = distribution_config.get("target_path")
        sftp_password_secret_name = distribution_config.get("sftp_password_secret_name")

        if not all([sftp_host, sftp_user, sftp_target_path, sftp_password_secret_name]):
            raise ValueError("Missing SFTP configuration details.")

        sftp_password = get_secret(sftp_password_secret_name)

        # Download file from GCS
        bucket_name, blob_name = gcs_file_path.replace("gs://", "").split("/", 1)
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        local_file_path = f"/tmp/{os.path.basename(gcs_file_path)}"
        blob.download_to_filename(local_file_path)
        logger.info(f"Downloaded {gcs_file_path} to {local_file_path}")

        # Connect and upload via SFTP
        transport = paramiko.Transport((sftp_host, sftp_port))
        transport.connect(username=sftp_user, password=sftp_password)
        sftp = paramiko.SFTPClient.from_transport(transport)

        remote_file_path = os.path.join(sftp_target_path, os.path.basename(local_file_path))
        sftp.put(local_file_path, remote_file_path)
        logger.info(f"Uploaded {local_file_path} to sftp://{sftp_host}:{sftp_port}{remote_file_path}")

        sftp.close()
        transport.close()
        os.remove(local_file_path)
        
        update_distribution_status(queue_id, "COMPLETED")
        return {"status": "success", "message": "SFTP transfer completed."}

    except Exception as e:
        logger.error(f"SFTP distribution failed for {gcs_file_path}: {e}", exc_info=True)
        update_distribution_status(queue_id, "FAILED", str(e))
        return {"status": "error", "message": f"SFTP transfer failed: {e}"}

def handle_email_notification(job_name, run_id, status, message, recipients):
    logger.info(f"Sending email notification for job {job_name}, run {run_id}")
    try:
        if not recipients:
            return {"status": "skipped", "message": "No email recipients specified."}

        sender_email = os.environ.get("SENDER_EMAIL", "noreply@your-domain.com")
        smtp_server = os.environ.get("SMTP_SERVER", "smtp.sendgrid.net")
        smtp_port = int(os.environ.get("SMTP_PORT", 587))
        smtp_user = os.environ.get("SMTP_USER", "apikey") # For SendGrid
        smtp_password_secret_name = os.environ.get("SMTP_PASSWORD_SECRET_NAME", "sendgrid-api-key")
        
        smtp_password = get_secret(smtp_password_secret_name)

        msg = MIMEText(f"Job Name: {job_name}\nRun ID: {run_id}\nStatus: {status}\nMessage: {message}")
        msg["Subject"] = f"r_exis_v2 Exporter Job Status: {status}"
        msg["From"] = sender_email
        msg["To"] = ", ".join(recipients)

        with smtplib.SMTP(smtp_server, smtp_port) as server:
            server.starttls()
            server.login(smtp_user, smtp_password)
            server.send_message(msg)
        
        logger.info(f"Email sent to {recipients} for job {job_name} with status {status}")
        return {"status": "success", "message": "Email notification sent."}
    except Exception as e:
        logger.error(f"Email notification failed for job {job_name}: {e}", exc_info=True)
        return {"status": "error", "message": f"Email notification failed: {e}"}


@functions_framework.http
def external_distributor_handler(request):
    """
    HTTP Cloud Function/Run handler for external distribution.
    Expects a JSON payload with distribution details.
    """
    if request.method == "POST":
        request_json = request.get_json(silent=True)
        if not request_json:
            return {"error": "Invalid JSON payload"}, 400

        logger.info(f"Received request: {request_json}")

        distribution_method = request_json.get("distribution_method") # For the queue entry
        config = request_json.get("distribution_config", {}) # Actual config from exporter_config
        queue_id = request_json.get("queue_id")

        if not distribution_method:
            return {"error": "Missing 'distribution_method' in payload."}, 400

        if distribution_method.upper() == "SFTP" or config.get("method", "").upper() == "SFTP":
            gcs_file_path = request_json.get("file_path_gcs")
            if not gcs_file_path:
                 return {"error": "Missing 'file_path_gcs' for SFTP distribution."}, 400
            result = handle_sftp_distribution(queue_id, gcs_file_path, config)
            return json.dumps(result), 200
        elif distribution_method.upper() == "EMAIL" or config.get("method", "").upper() == "EMAIL":
            job_name = request_json.get("job_name")
            run_id = request_json.get("run_id")
            status = request_json.get("status")
            message = request_json.get("message")
            recipients = config.get("email_recipients") # Should be a list or comma-separated string
            if isinstance(recipients, str):
                recipients = [r.strip() for r in recipients.split(',')]
            
            result = handle_email_notification(job_name, run_id, status, message, recipients)
            return json.dumps(result), 200
        else:
            return {"error": f"Unsupported distribution method: {distribution_method}"}, 400
    else:
        return {"error": "Only POST requests are accepted"}, 405