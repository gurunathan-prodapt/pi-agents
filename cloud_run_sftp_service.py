# Migrated SFTP distribution logic from legacy configuration: vobs/.../h_exis_apt_nna_voice.var
# Job: EXIS_SD_APT_NNA_VOIC
# Description: Cloud Run service to handle SFTP distribution of files from GCS.

import os
import logging
import paramiko
from google.cloud import storage
from flask import Flask, request, jsonify

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

app = Flask(__name__)

# SFTP configuration (should ideally be managed via Secret Manager or environment variables)
SFTP_HOST = os.environ.get("SFTP_HOST")
SFTP_PORT = int(os.environ.get("SFTP_PORT", 22))
SFTP_USERNAME = os.environ.get("SFTP_USERNAME")
SFTP_PASSWORD = os.environ.get("SFTP_PASSWORD") # Consider using SSH keys instead
SFTP_REMOTE_PATH = os.environ.get("SFTP_REMOTE_PATH", "/remote/incoming/") # Default remote path

def download_from_gcs(gcs_path: str, local_path: str) -> None:
    """Downloads a file from GCS to a local path."""
    client = storage.Client()
    bucket_name = gcs_path.split("gs://")[1].split("/")[0]
    blob_name = "/".join(gcs_path.split("gs://")[1].split("/")[1:])
    
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    
    logging.info(f"Downloading {gcs_path} to {local_path}")
    blob.download_to_filename(local_path)
    logging.info("Download complete.")

def upload_to_sftp(local_file_path: str, remote_file_path: str) -> None:
    """Uploads a file via SFTP."""
    if not all([SFTP_HOST, SFTP_USERNAME, SFTP_PASSWORD]):
        raise ValueError("SFTP credentials or host not fully configured.")

    transport = paramiko.Transport((SFTP_HOST, SFTP_PORT))
    transport.connect(username=SFTP_USERNAME, password=SFTP_PASSWORD)
    sftp = paramiko.SFTPClient.from_transport(transport)

    try:
        logging.info(f"Uploading {local_file_path} to sftp://{SFTP_HOST}:{SFTP_PORT}{remote_file_path}")
        sftp.put(local_file_path, remote_file_path)
        logging.info("SFTP upload complete.")
    finally:
        sftp.close()
        transport.close()

@app.route("/", methods=["POST"])
def sftp_transfer():
    """
    Cloud Run entry point. Expects a POST request with 'gcs_file_path' in the JSON body.
    """
    if not request.is_json:
        return jsonify({"status": "error", "message": "Request must be JSON"}), 400

    data = request.get_json()
    gcs_file_path = data.get("gcs_file_path")

    if not gcs_file_path:
        return jsonify({"status": "error", "message": "Missing 'gcs_file_path' in request body"}), 400

    local_temp_file_path = f"/tmp/{os.path.basename(gcs_file_path)}"
    remote_target_file_path = os.path.join(SFTP_REMOTE_PATH, os.path.basename(gcs_file_path))

    try:
        download_from_gcs(gcs_file_path, local_temp_file_path)
        upload_to_sftp(local_temp_file_path, remote_target_file_path)
        return jsonify({"status": "success", "message": f"File {gcs_file_path} transferred to SFTP"}), 200
    except Exception as e:
        logging.error(f"SFTP transfer failed for {gcs_file_path}: {e}", exc_info=True)
        return jsonify({"status": "error", "message": str(e)}), 500
    finally:
        if os.path.exists(local_temp_file_path):
            os.remove(local_temp_file_path) # Clean up local temp file

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))