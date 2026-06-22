# Cloud Function to parse existing config files and load data into dwh_exporter.config_kv.
# This function is designed to be triggered manually or via a Cloud Storage event
# when config files are uploaded.
# Legacy source: vobs/dw_source/isdwh/exporter/is/bin/r_exis_v2

import os
import json
import base64
from google.cloud import bigquery

# Initialize BigQuery client
client = bigquery.Client()
DATASET_ID = 'dwh_exporter'
TABLE_ID = 'config_kv'
TABLE_REF = client.dataset(DATASET_ID).table(TABLE_ID)

def parse_config_file_to_bigquery(event, context):
    """
    Triggered by a change to a Cloud Storage bucket.
    Reads the config file, parses key-value pairs, and inserts into BigQuery.
    """
    file_data = event
    bucket_name = file_data['bucket']
    file_name = file_data['name']
    job_name = os.getenv('JOB_NAME', 'r_exis_v2') # Default job name

    if not file_name.endswith(('.cfg', '.conf', '.txt')): # Adjust file extensions as needed
        print(f"Skipping file {file_name}: not a recognized config file extension.")
        return

    print(f"Processing file: {file_name} from bucket: {bucket_name}")

    try:
        # Download the file from GCS
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(file_name)
        content = blob.download_as_string().decode('utf-8')

        rows_to_insert = []
        for line in content.splitlines():
            line = line.strip()
            if not line or line.startswith('#') or '=' not in line:
                continue

            key, value = line.split('=', 1)
            key = key.strip()
            value = value.strip()

            # Infer type for basic types, or keep as STRING
            config_type = 'STRING'
            if value.isdigit():
                config_type = 'INT'
            elif value.lower() in ['true', 'false']:
                config_type = 'BOOLEAN'
            # Add more type inference if needed

            rows_to_insert.append({
                "job_name": job_name,
                "config_key": key,
                "config_value": value,
                "config_type": config_type,
                "description": f"Loaded from {file_name}",
                "updated_at": "CURRENT_TIMESTAMP()" # BigQuery will resolve this
            })

        if rows_to_insert:
            # BigQuery insert_rows_json expects 'updated_at' to be actual timestamp or NULL,
            # not a function. We'll set it in the BQ table directly, or omit from here
            # and let the default value apply if defined. For now, omitting it.
            # If the column has a default CURRENT_TIMESTAMP(), we don't need to send it.
            # Otherwise, we need to generate timestamps here.
            # Let's remove 'updated_at' from the dict for now, assuming BQ table has default.
            for row in rows_to_insert:
                row.pop("updated_at", None)

            errors = client.insert_rows_json(TABLE_REF, rows_to_insert)
            if errors:
                print(f"Encountered errors while inserting rows: {errors}")
            else:
                print(f"Successfully inserted {len(rows_to_insert)} rows from {file_name} into {DATASET_ID}.{TABLE_ID}")
        else:
            print(f"No valid key-value pairs found in {file_name}.")

    except Exception as e:
        print(f"Error processing file {file_name}: {e}")
        raise