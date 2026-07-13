import os
import json
import ftplib
import functions_framework
from google.cloud import storage
from google.cloud import secretmanager

def resolve_secret(secret_id: str) -> str:
    """
    Fetches the plain text password from Google Secret Manager dynamically.
    Avoids plain text password parameter leakage.
    """
    try:
        client = secretmanager.SecretManagerServiceClient()
        # Assumes format "projects/{project_id}/secrets/{secret_name}/versions/latest"
        project_id = os.environ.get("GCP_PROJECT", "your_project_id")
        name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
        response = client.access_secret_version(request={"name": name})
        return response.payload.data.decode("UTF-8").strip()
    except Exception as e:
        raise RuntimeError(f"Secret Manager resolution error ({secret_id}): {str(e)}")

@functions_framework.http
def ftp_transfer_adapter(request):
    """
    HTTPS Google Cloud Function conforming to BigQuery External Connection spec.
    Processes a series of calls within a JSON list payload.
    """
    try:
        request_json = request.get_json(silent=True)
        if not request_json or 'calls' not in request_json:
            return json.dumps({"replies": []}), 400

        replies = []
        for call in request_json['calls']:
            # Positional arguments mapping directly from procedure schema signature:
            source_file_path = call[0]
            target_file_path = call[1]
            ftp_server       = call[2]
            ftp_user         = call[3]
            secret_key       = call[4]  # Used to dynamically query Secret Manager
            ftp_directory    = call[5]
            action_type      = call[6]  # 'SEND' or 'RENAME'

            try:
                # 1. Resolve credentials dynamically
                ftp_password = resolve_secret(secret_key)

                # 2. Establish connection to FTP
                ftp = ftplib.FTP(ftp_server)
                ftp.login(user=ftp_user, passwd=ftp_password)
                
                if ftp_directory:
                    ftp.cwd(ftp_directory)

                # 3. Action handling router
                if action_type == 'SEND':
                    # Parse gs:// URI schema (e.g. gs://bucket_name/path/to/file.csv)
                    bucket_name = source_file_path.replace("gs://", "").split("/")[0]
                    blob_name = "/".join(source_file_path.replace("gs://", "").split("/")[1:])

                    # Fetch file content from Google Cloud Storage
                    gcs_client = storage.Client()
                    bucket = gcs_client.bucket(bucket_name)
                    blob = bucket.blob(blob_name)

                    if not blob.exists():
                        raise FileNotFoundError(f"Source file {source_file_path} not found in GCS.")

                    with blob.open("rb") as data_stream:
                        ftp.storbinary(f'STOR {target_file_path}', data_stream)

                    replies.append({"status": "SUCCESS", "message": "File streamed successfully to target directory."})

                elif action_type == 'RENAME':
                    # target_file_path is current file (temp), source_file_path is ultimate destination name
                    ftp.rename(source_file_path, target_file_path)
                    replies.append({"status": "SUCCESS", "message": "Remote file renamed successfully."})

                else:
                    replies.append({"status": "ERROR", "message": f"Unsupported action type: {action_type}"})

                ftp.quit()

            except Exception as task_err:
                replies.append({"status": "ERROR", "message": str(task_err)})

        return json.dumps({"replies": [r for r in replies]}), 200

    except Exception as global_err:
        # Wrap any global failures into expected BQ JSON response structure
        return json.dumps({"replies": [{"status": "ERROR", "message": str(global_err)}]}), 200