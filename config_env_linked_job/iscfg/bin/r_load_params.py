import os
import sys
from datetime import datetime, timezone
from google.cloud import storage, bigquery

def main():
    print("Start Parameter-Load...")
    
    # Retrieve runtime configuration
    gcp_project = os.environ.get("GCP_PROJECT")
    gcs_bucket_name = os.environ.get("GCS_CONFIG_BUCKET")
    
    if not gcs_bucket_name:
        print("FEHLER: GCS_CONFIG_BUCKET Environment-Variable ist nicht gesetzt.")
        sys.exit(1)
        
    properties_file_path = "cfg/dwh_env.properties"
    
    # Dynamic staging table target schema
    if gcp_project:
        staging_table_id = f"{gcp_project}.DWH_STG.PARAM_LOAD"
    else:
        staging_table_id = "DWH_STG.PARAM_LOAD"

    # Read properties parameter file from GCS
    print("Lese Parameterdatei...")
    try:
        storage_client = storage.Client(project=gcp_project)
        bucket = storage_client.bucket(gcs_bucket_name)
        blob = bucket.blob(properties_file_path)
        
        if not blob.exists():
            print("FEHLER: Parameterdatei existiert nicht oder ist leer.")
            sys.exit(1)
            
        content = blob.download_as_text()
        if not content.strip():
            print("FEHLER: Parameterdatei existiert nicht oder ist leer.")
            sys.exit(1)
    except Exception as e:
        print(f"FEHLER: Fehler beim Lesen der Parameterdatei aus GCS: {e}")
        sys.exit(1)

    # Parse parameter key/value configurations
    rows_to_insert = []
    loaded_at_str = datetime.now(timezone.utc).isoformat()
    
    for line in content.splitlines():
        line = line.strip()
        if not line or line.startswith('#'):
            continue
        if '=' in line:
            key, val = line.split('=', 1)
            rows_to_insert.append({
                "param_key": key.strip(),
                "param_value": val.strip(),
                "loaded_at": loaded_at_str
            })

    if not rows_to_insert:
        print("FEHLER: Parameterdatei existiert nicht oder ist leer.")
        sys.exit(1)

    # Execute bulk load truncate-and-replace to BigQuery Staging
    try:
        bq_client = bigquery.Client(project=gcp_project)
        
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            schema=[
                bigquery.SchemaField("param_key", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("param_value", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("loaded_at", "TIMESTAMP", mode="NULLABLE"),
            ]
        )
        
        load_job = bq_client.load_table_from_json(rows_to_insert, staging_table_id, job_config=job_config)
        load_job.result()
        
        print("Laden in Staging-Tabelle erfolgreich.")
    except Exception as e:
        print(f"FEHLER: SQL*Loader/BigQuery Load fehlgeschlagen! {e}")
        sys.exit(1)

    print("Führe Post-Load SQL-Skript aus...")

if __name__ == "__main__":
    main()