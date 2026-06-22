# SFTP Transfer Utility for EXIS_SD_APT_NNA_VOIC
# Handles downloading a file from GCS and uploading it to an SFTP server.

import logging
import os
import tempfile

from google.cloud import storage
import paramiko

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def sftp_transfer_gcs_file(
    gcs_bucket: str,
    gcs_object_name: str,
    sftp_host: str,
    sftp_port: int,
    sftp_username: str,
    sftp_password: str = None, # Use password or key, not both.
    sftp_key_file_path: str = None, # Path to SSH private key file
    sftp_remote_path: str = "/",
    sftp_filename: str = None,
) -> None:
    """
    Downloads a file from GCS and uploads it to an SFTP server.

    Args:
        gcs_bucket (str): The name of the GCS bucket.
        gcs_object_name (str): The name of the object in the GCS bucket.
        sftp_host (str): The SFTP server hostname or IP address.
        sftp_port (int): The SFTP server port.
        sftp_username (str): The username for SFTP authentication.
        sftp_password (str, optional): The password for SFTP authentication.
                                       Defaults to None.
        sftp_key_file_path (str, optional): Path to the SSH private key file.
                                            Defaults to None.
        sftp_remote_path (str, optional): The remote directory on the SFTP server
                                          where the file will be uploaded. Defaults to "/".
        sftp_filename (str, optional): The name to use for the file on the SFTP server.
                                       If None, gcs_object_name is used.
    """
    if not sftp_filename:
        sftp_filename = gcs_object_name.split('/')[-1] # Extract filename from GCS object name

    local_temp_file = None
    try:
        # 1. Download from GCS
        logging.info(f"Downloading gs://{gcs_bucket}/{gcs_object_name} to local temporary file...")
        storage_client = storage.Client()
        bucket = storage_client.bucket(gcs_bucket)
        blob = bucket.blob(gcs_object_name)

        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            local_temp_file = temp_file.name
            blob.download_to_filename(local_temp_file)
        logging.info(f"Downloaded to {local_temp_file}")

        # 2. Upload to SFTP
        logging.info(f"Connecting to SFTP server: {sftp_host}:{sftp_port} as {sftp_username}")
        transport = paramiko.Transport((sftp_host, sftp_port))

        if sftp_key_file_path:
            # Assuming key file is passphrase-protected, if not, remove password arg
            # Or handle passphrase separately
            key = paramiko.RSAKey.from_private_key_file(sftp_key_file_path, password=sftp_password)
            transport.connect(username=sftp_username, pkey=key)
        elif sftp_password:
            transport.connect(username=sftp_username, password=sftp_password)
        else:
            raise ValueError("Either sftp_password or sftp_key_file_path must be provided for SFTP authentication.")

        sftp = paramiko.SFTPClient.from_transport(transport)
        logging.info("SFTP connection established.")

        remote_full_path = os.path.join(sftp_remote_path, sftp_filename).replace("\\", "/") # Ensure Unix-style path
        logging.info(f"Uploading {local_temp_file} to sftp://{sftp_host}{remote_full_path}")
        sftp.put(local_temp_file, remote_full_path)
        logging.info(f"Successfully uploaded {sftp_filename} to SFTP server.")

    except Exception as e:
        logging.error(f"SFTP transfer failed: {e}")
        raise
    finally:
        if 'sftp' in locals() and sftp:
            sftp.close()
        if 'transport' in locals() and transport:
            transport.close()
        if local_temp_file and os.path.exists(local_temp_file):
            os.remove(local_temp_file)
            logging.info(f"Cleaned up local temporary file: {local_temp_file}")

# Example usage (for local testing, not part of Airflow execution flow directly)
if __name__ == "__main__":
    # These would typically be passed by Airflow or retrieved from environment/secrets
    # For local testing, replace with actual values or mock them.
    TEST_GCS_BUCKET = "your-gcs-test-bucket"
    TEST_GCS_OBJECT = "test_data/my_exported_data.csv.gz"
    TEST_SFTP_HOST = "localhost" # or a test SFTP server
    TEST_SFTP_PORT = 22
    TEST_SFTP_USERNAME = "testuser"
    TEST_SFTP_PASSWORD = "testpassword"
    TEST_SFTP_REMOTE_PATH = "/upload"
    TEST_SFTP_FILENAME = "exported_voice_data_test.csv.gz"

    # Create a dummy file in GCS for testing (replace with actual GCS object if available)
    # This part would not be in a production sftp_exporter.py, but for `if __name__ == "__main__":`
    # print(f"Please ensure gs://{TEST_GCS_BUCKET}/{TEST_GCS_OBJECT} exists for local testing, or create a dummy file.")
    # You might need to set GOOGLE_APPLICATION_CREDENTIALS for GCS
    # storage_client = storage.Client()
    # bucket = storage_client.bucket(TEST_GCS_BUCKET)
    # blob = bucket.blob(TEST_GCS_OBJECT)
    # blob.upload_from_string("col1,col2\nval1,val2\n", content_type="application/gzip")
    # print(f"Dummy GCS object created: gs://{TEST_GCS_BUCKET}/{TEST_GCS_OBJECT}")

    try:
        logging.info("Starting local SFTP transfer test...")
        # sftp_transfer_gcs_file(
        #     gcs_bucket=TEST_GCS_BUCKET,
        #     gcs_object_name=TEST_GCS_OBJECT,
        #     sftp_host=TEST_SFTP_HOST,
        #     sftp_port=TEST_SFTP_PORT,
        #     sftp_username=TEST_SFTP_USERNAME,
        #     sftp_password=TEST_SFTP_PASSWORD,
        #     sftp_remote_path=TEST_SFTP_REMOTE_PATH,
        #     sftp_filename=TEST_SFTP_FILENAME,
        # )
        # print("Local SFTP transfer test finished. (Commented out for safety/requires setup)")
        pass
    except Exception as e:
        logging.error(f"Local test failed: {e}")