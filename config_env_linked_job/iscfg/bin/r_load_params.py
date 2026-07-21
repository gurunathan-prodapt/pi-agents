import os
import sys
import tempfile
import csv
from datetime import datetime
from google.cloud import bigquery
from airflow.models import Variable

def main():
    # Sourced from GLOBAL policies
    gcp_project = Variable.get("GCP_PROJECT")
    dataset_stg = Variable.get("BQ_DATASET_STG", default_var="DWH_STG")
    param_file_path = Variable.get("PARAM_FILE_PATH", default_var="job_params.properties")

    # 1. Check for local configuration files
    if not os.path.exists(param_file_path):
        # OUTPUT/PRINT LITERAL RULE: Retained verbatim from source
        print("FEHLER: Parameterdatei...")
        sys.exit(1)

    try: 
        # Parse properties file
        rows = []
        loaded_at = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f')
        with open(param_file_path, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith('#') or line.startswith('!'):
                    continue
                if '=' in line:
                    key, val = line.split('=', 1)
                    rows.append({
                        'param_key': key.strip(),
                        'param_value': val.strip(),
                        'loaded_at': loaded_at
                    })

        # 2. Replaces SQL*Loader with BigQuery Client API Load
        client = bigquery.Client(project=gcp_project)
        table_ref = f"{gcp_project}.{dataset_stg}.PARAM_LOAD"

        # Let's write to a temporary CSV file to use load_table_from_file
        with tempfile.NamedTemporaryFile(mode='w+', delete=False, newline='', encoding='utf-8') as tmp:
            writer = csv.DictWriter(tmp, fieldnames=['param_key', 'param_value', 'loaded_at'])
            writer.writeheader()
            for r in rows:
                writer.writerow(r)
            tmp_path = tmp.name

        try:
            job_config = bigquery.LoadJobConfig(
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
                source_format=bigquery.SourceFormat.CSV,
                skip_leading_rows=1,
                schema=[
                    bigquery.SchemaField("param_key", "STRING"),
                    bigquery.SchemaField("param_value", "STRING"),
                    bigquery.SchemaField("loaded_at", "TIMESTAMP"),
                ]
            )

            with open(tmp_path, "rb") as source_file:
                load_job = client.load_table_from_file(
                    source_file, table_ref, job_config=job_config
                )
            
            load_job.result()  # Wait for upload to complete
        finally:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)

    except Exception as e:
        # OUTPUT/PRINT LITERAL RULE: Retained verbatim from source
        print("FEHLER: sqlldr beendet...")
        print(f"Details: {str(e)}", file=sys.stderr)
        sys.exit(1)

    # OUTPUT/PRINT LITERAL RULE: Retained verbatim from source
    print("Parameterladen erfolgreich abgeschlossen")

if __name__ == "__main__":
    main()