import os
import logging
from datetime import datetime
from google.cloud import storage
from google.cloud import bigquery

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def load_parameters(gcs_bucket: str, source_blob: str, target_project: str, target_dataset: str, target_table: str):
    """
    Simulates SQL*Loader loading environment/DWH parameters into staging table.
    Reads parameter file from GCS, parses keys/values, and loads to BigQuery.
    """
    # Literal output messages preserved from legacy KornShell log requirements
    logger.info("Starting parameter load process...")
    logger.info("Environment variables loaded.")
    logger.info(f"Reading file from GCS: gs://{gcs_bucket}/{source_blob}")
    
    storage_client = storage.Client(project=target_project)
    bucket = storage_client.bucket(gcs_bucket)
    blob = bucket.blob(source_blob)
    
    try:
        content = blob.download_as_text()
    except Exception as e:
        logger.error(f"Failed to read file from GCS: {e}")
        raise e

    records = []
    loaded_at = datetime.utcnow().isoformat()
    
    lines = content.splitlines()
    for line in lines:
        line = line.strip()
        # Skip empty lines or comments
        if not line or line.startswith('#') or line.startswith('--'):
            continue
        
        # Split by first '=' or whitespace
        if '=' in line:
            parts = line.split('=', 1)
        else:
            parts = line.split(None, 1)
            
        if len(parts) == 2:
            key = parts[0].strip()
            value = parts[1].strip()
            records.append({
                "param_key": key,
                "param_value": value,
                "loaded_at": loaded_at
            })
            
    logger.info(f"Number of parameters parsed: {len(records)}")
    
    if not records:
        logger.warning("No parameters found to load.")
        return

    # Load into BigQuery (Truncates staging table to mirror SQL*Loader behavior)
    logger.info(f"Loading parameters to BigQuery table: {target_project}.{target_dataset}.{target_table}")
    bq_client = bigquery.Client(project=target_project)
    table_ref = bq_client.dataset(target_dataset).table(target_table)
    
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        schema=[
            bigquery.SchemaField("param_key", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("param_value", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("loaded_at", "TIMESTAMP", mode="REQUIRED"),
        ]
    )
    
    try:
        job = bq_client.load_table_from_json(records, table_ref, job_config=job_config)
        job.result()  # Wait for load job completion
        logger.info("Parameter staging load completed successfully.")
    except Exception as e:
        logger.error(f"Failed to load parameters to BigQuery: {e}")
        raise e

if __name__ == "__main__":
    bucket = os.environ.get("GCS_BUCKET")
    blob_path = os.environ.get("GCS_BLOB_PATH", "config/d_param_load.properties")
    project = os.environ.get("GCP_PROJECT")
    dataset = os.environ.get("BQ_DATASET_STG", "DWH_STG")
    table = os.environ.get("BQ_TABLE_STG", "PARAM_LOAD")
    
    load_parameters(bucket, blob_path, project, dataset, table)