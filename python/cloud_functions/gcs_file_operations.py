# Cloud Function for performing GCS file operations (move, copy, delete, compress/decompress).
# Triggered by new files in a specific Cloud Storage bucket, or by explicit invocation.
# Legacy source: vobs/dw_source/isdwh/exporter/is/bin/r_exis_v2

import os
import json
from google.cloud import storage
import gzip

storage_client = storage.Client()
DATASET_ID = 'dwh_exporter'
DISTRIBUTION_TABLE_ID = 'export_distribution' # To fetch rules for ops like 'GCS_MOVE', 'GCS_COPY', 'GCS_DELETE', 'GCS_COMPRESS'

def gcs_file_operations(event, context):
    """
    Triggered by a Cloud Storage event or explicitly invoked.
    Performs GCS operations based on the event data and distribution rules.
    """
    file_data = event
    source_bucket_name = file_data['bucket']
    source_blob_name = file_data['name'] # This is the object name in GCS

    print(f"Received GCS event for file: gs://{source_bucket_name}/{source_blob_name}")

    # Determine the operation based on metadata or a distribution rule
    # For a general function, we'd need a clear way to specify the desired operation.
    # For now, let's assume the trigger file name itself or custom metadata indicates the operation.
    # A more robust solution would be to have a Pub/Sub message with structured operation details.

    # Example: Look for a specific pattern indicating a 'move' operation
    # Or query dwh_exporter.export_distribution for a matching rule.
    # For this example, let's assume the operation is 'MOVE' to a hardcoded target.

    # Option 1: Explicit instruction via event data (e.g., from a Pub/Sub trigger)
    # If using direct GCS event, it means a file was created. We need to decide what to do with IT.
    # Let's mock a simple scenario: If a file is in 'processing' bucket, move it to 'archive' after processing.

    # For the purpose of the build plan, this function *replaces* shell commands.
    # So, it should be capable of:
    # 1. Moving a file
    # 2. Copying a file
    # 3. Deleting a file
    # 4. Compressing a file (gzip)

    # We need a way to pass the *operation* and *target* parameters.
    # This could be:
    # A) Triggered by a Pub/Sub message containing these parameters.
    # B) Inferring from file path conventions (e.g., file in 'to_compress' bucket).
    # C) Querying `export_distribution` for rules based on the `source_blob_name`.

    # Let's assume for now this function is triggered by an explicit invocation from
    # another cloud service (like Cloud Workflows or another Cloud Function) with `operation` and `target_path`.
    # For a GCS event trigger, we'll need to query export_distribution table.

    # For now, let's implement a simple GCS_MOVE example based on a dummy rule.
    # In a real system, the trigger (e.g., from a Cloud Workflow step) would specify the operation.

    # For a GCS trigger, we'll implement a 'move to archive' if a file lands in 'exported' bucket
    # after distribution. This is a post-distribution cleanup.

    if "exported" in source_bucket_name and not source_blob_name.startswith("archive/"):
        destination_bucket_name = source_bucket_name # or a separate archive bucket
        destination_blob_name = f"archive/{source_blob_name}"

        source_bucket = storage_client.bucket(source_bucket_name)
        source_blob = source_bucket.blob(source_blob_name)
        destination_blob = storage_client.bucket(destination_bucket_name).blob(destination_blob_name)

        # Copy the file to the destination
        source_bucket.copy_blob(source_blob, storage_client.bucket(destination_bucket_name), destination_blob_name)
        print(f"Copied gs://{source_bucket_name}/{source_blob_name} to gs://{destination_bucket_name}/{destination_blob_name}")

        # Delete the original file
        source_blob.delete()
        print(f"Deleted original file gs://{source_bucket_name}/{source_blob_name}")

        print(f"File moved from gs://{source_bucket_name}/{source_blob_name} to gs://{destination_bucket_name}/{destination_blob_name}")
    else:
        print(f"No specific GCS operation rule found for file: gs://{source_bucket_name}/{source_blob_name}")

    # Example for compression (if explicitly invoked or rule-based)
    # This part would be executed if the function was called with an explicit 'compress' command.
    # For a GCS event trigger, we'd need a rule like 'if file in X bucket, compress it'.
    if ".gz" not in source_blob_name and "compress" in source_blob_name: # Dummy condition
        source_bucket = storage_client.bucket(source_bucket_name)
        source_blob = source_bucket.blob(source_blob_name)
        compressed_blob_name = f"{source_blob_name}.gz"
        temp_file_path = f"/tmp/{source_blob_name}"
        compressed_file_path = f"/tmp/{compressed_blob_name}"

        source_blob.download_to_filename(temp_file_path)

        with open(temp_file_path, 'rb') as f_in:
            with gzip.open(compressed_file_path, 'wb') as f_out:
                f_out.writelines(f_in)

        storage_client.bucket(source_bucket_name).blob(compressed_blob_name).upload_from_filename(compressed_file_path)
        print(f"Compressed gs://{source_bucket_name}/{source_blob_name} to gs://{source_bucket_name}/{compressed_blob_name}")

        os.remove(temp_file_path)
        os.remove(compressed_file_path)
    # End of dummy compression example

    # For delete, copy (more direct versions):
    # blob = storage_client.bucket(bucket_name).blob(blob_name)
    # blob.delete()
    # source_blob.copy_to(destination_blob)
    # etc.

    # This Cloud Function needs to be highly parameterized based on how it's invoked.
    # The current GCS event trigger is for when a file is created/finalized.
    # A more complete solution would parse a message from Pub/Sub to get the actual operation.