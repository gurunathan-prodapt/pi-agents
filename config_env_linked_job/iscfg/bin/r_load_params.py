# config_env_linked_job/iscfg/bin/r_load_params.py
# Ingests staging parameter configurations from GCS into BigQuery.

import os
import sys
from datetime import datetime
from google.cloud import bigquery
from google.cloud import storage

def load_parameters(param_file_path: str, project_id: str, dataset_id: str, table_id: str):
    """
    Ingests staging parameter configurations from GCS into BigQuery.
    """
    # 1. Check if parameter file exists in Google Cloud Storage
    storage_client = storage.Client(project=project_id)
    
    if not param_file_path.startswith("gs://"):
        print(f"FEHLER: Ungültiger GCS-Pfad: {param_file_path}")
        sys.exit(1)
        
    bucket_name = param_file_path.split("/")[2]
    blob_name = "/".join(param_file_path.split("/")[3:])
    
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    
    if not blob.exists():
        # RULE: Preserve exact German message verbatim from KornShell
        print(f"FEHLER: Parameterdatei {param_file_path} nicht gefunden")
        sys.exit(1)
        
    # RULE: Preserve exact German message verbatim from KornShell
    print(f"Lade Parameter nach {dataset_id}.{table_id} ...")
    
    try: 
        # Download properties content
        data = blob.download_as_text()
        
        rows_to_insert = []
        current_time = datetime.utcnow().isoformat()
        
        for line in data.splitlines():
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            if '=' in line:
                key, val = line.split('=', 1)
                rows_to_insert.append({
                    "param_key": key.strip(),
                    "param_value": val.strip(),
                    "loaded_at": current_time
                })
        
        # Ingest parsed records into BigQuery
        if rows_to_insert:
            bq_client = bigquery.Client(project=project_id)
            table_ref = bq_client.dataset(dataset_id).table(table_id)
            
            # Truncate staging and write new values
            job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
            load_job = bq_client.load_table_from_json(rows_to_insert, table_ref, job_config=job_config)
            load_job.result()  # Waits for the job to complete
            
        # RULE: Preserve exact German message verbatim from KornShell
        print("Parameterladen erfolgreich abgeschlossen")
        
    except Exception as e:
        print(f"Exception encountered: {str(e)}")
        sys.exit(2)