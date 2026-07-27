#!/usr/bin/env python3
"""
Migrated from r_load_params.ksh
Purpose: Read environment parameters from properties file and load into BigQuery staging table.
"""

import os
import sys
import datetime
from pathlib import Path
from google.cloud import bigquery
from google.cloud import storage

def main():
    # Retrieve environment-wide global settings
    gcp_project = os.environ.get("GCP_PROJECT")
    gcs_bucket = os.environ.get("GCS_BUCKET")
    dwh_home = os.environ.get("DWH_HOME", "/opt/dwh")
    bq_dataset_stg = os.environ.get("BQ_DATASET_STG", "DWH_STG")

    # Determine properties path representation for display and loading
    props_local_path = Path(dwh_home) / "cfg" / "dwh_env.properties"
    props_display_path = f"gs://{gcs_bucket}/config_env_linked_job/iscfg/cfg/dwh_env.properties" if gcs_bucket else str(props_local_path)

    content = ""
    if gcs_bucket:
        try: 
            storage_client = storage.Client(project=gcp_project) if gcp_project else storage.Client()
            bucket = storage_client.bucket(gcs_bucket)
            blob = bucket.blob("config_env_linked_job/iscfg/cfg/dwh_env.properties")
            if blob.exists():
                content = blob.download_as_text(encoding="utf-8")
            else:
                # Fallback to local file if not found in GCS
                if props_local_path.is_file():
                    content = props_local_path.read_text(encoding="utf-8")
                else:
                    print(f"FEHLER: Parameterdatei {props_display_path} nicht gefunden", file=sys.stderr)
                    sys.exit(8)
        except Exception as e:
            # Fallback to local on connection or permission issues
            if props_local_path.is_file():
                content = props_local_path.read_text(encoding="utf-8")
            else:
                print(f"FEHLER: Parameterdatei {props_display_path} nicht gefunden", file=sys.stderr)
                sys.exit(8)
    else:
        if props_local_path.is_file():
            content = props_local_path.read_text(encoding="utf-8")
        else:
            print(f"FEHLER: Parameterdatei {props_display_path} nicht gefunden", file=sys.stderr)
            sys.exit(8)

    # Parse properties
    db_host = ""
    db_sid = ""
    stg_table = ""
    params_list = []

    for line in content.splitlines():
        line_stripped = line.strip()
        if not line_stripped or line_stripped.startswith("#"):
            continue
        if "=" in line_stripped:
            key, val = line_stripped.split("=", 1)
            key = key.strip()
            val = val.strip()
            params_list.append({
                "param_key": key,
                "param_value": val,
                "loaded_at": datetime.datetime.utcnow().isoformat()
            })
            
            if key == "db.host":
                db_host = val
            elif key == "db.sid":
                db_sid = val
            elif key == "stage.table":
                stg_table = val

    # Print log message in exact original language
    print(f"Lade Parameter nach {stg_table} auf {db_host}/{db_sid}")

    # Determine BigQuery staging table
    target_table_name = "PARAM_LOAD"
    if stg_table:
        if "." in stg_table:
            target_table_name = stg_table.split(".", 1)[1]
        else:
            target_table_name = stg_table

    # Initialize BigQuery Client
    try: 
        bq_client = bigquery.Client(project=gcp_project) if gcp_project else bigquery.Client()
        project_id = bq_client.project
        target_table_id = f"{project_id}.{bq_dataset_stg}.{target_table_name}"

        # Configure load job
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            schema=[
                bigquery.SchemaField("param_key", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("param_value", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("loaded_at", "TIMESTAMP", mode="REQUIRED"),
            ]
        )

        # Load parameters into BigQuery
        load_job = bq_client.load_table_from_json(
            params_list,
            target_table_id,
            job_config=job_config
        )
        load_job.result()  # Wait for loading to complete
    except Exception as e:
        print("FEHLER: sqlldr beendet mit RC=1", file=sys.stderr)
        sys.exit(1)

    print("Parameterladen erfolgreich abgeschlossen")
    sys.exit(0)

if __name__ == "__main__":
    main()