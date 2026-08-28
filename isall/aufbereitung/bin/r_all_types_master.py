#!/usr/bin/env python3
import os
import sys
import subprocess
from datetime import datetime

def log_and_print(message: str, log_path: str):
    """Prints a message to stdout and appends it to the log file (simulating tee -a)."""
    print(message)
    try:
        with open(log_path, "a") as lf:
            lf.write(message + "\n")
    except Exception as e:
        print(f"Warning: Failed to write to log file {log_path}: {e}", file=sys.stderr)

def main():
    # Step 1: Env setup & verification
    all_dir_root = os.environ.get("ALL_DIR_ROOT")
    gcp_project = os.environ.get("GCP_PROJECT")
    bq_dataset = os.environ.get("BQ_DATASET")

    if not all_dir_root:
        print("Error: ALL_DIR_ROOT environment variable not set.", file=sys.stderr)
        sys.exit(1)

    if not gcp_project:
        print("Error: GCP_PROJECT environment variable not set.", file=sys.stderr)
        sys.exit(1)

    if not bq_dataset:
        print("Error: BQ_DATASET environment variable not set.", file=sys.stderr)
        sys.exit(1)

    # Step 2: Establish local orchestration variables
    job_kennung = "ALL_TYPES_MASTER"
    v_sysdate = datetime.now().strftime("%d%m%Y")
    log_datei = os.path.join(all_dir_root, "protokoll", f"all_types_master_{v_sysdate}.log")

    # Ensure log directory exists
    try:
        os.makedirs(os.path.dirname(log_datei), exist_ok=True)
    except Exception as e:
        print(f"Error creating log directory: {e}", file=sys.stderr)
        sys.exit(1)

    # Step 3: Print job run header details
    print(" ----------------- Job -----------------------")
    print(f" JobKennung: '{job_kennung}'")
    print(f" Logdatei  : '{log_datei}'")
    print(" ---------------------------------------------")

    # Step 4: Step 1 - Execute SQL Refresh (BigQuery)
    log_and_print("----Starte SQL-Refresh----", log_datei)
    
    sql_script_path = os.path.join(all_dir_root, "aufbereitung", "sql", "d_all_types.sql")
    
    try:
        with open(sql_script_path, "r") as sf:
            sql_text = sf.read()
    except Exception as e:
        log_and_print(f"ERROR: Failed to read SQL script {sql_script_path}: {e}", log_datei)
        sys.exit(1)

    try:
        from google.cloud import bigquery
        client = bigquery.Client(project=gcp_project)
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("gcp_project", "STRING", gcp_project),
                bigquery.ScalarQueryParameter("bq_dataset", "STRING", bq_dataset),
            ]
        )
        with open(log_datei, "a") as lf:
            lf.write(f"Executing BigQuery query from {sql_script_path}\n")
        
        query_job = client.query(sql_text, job_config=job_config)
        query_job.result()  # Wait for query to complete
        
        with open(log_datei, "a") as lf:
            lf.write("BigQuery query completed successfully.\n")
    except Exception as e:
        log_and_print(f"ERROR: BigQuery execution failed: {e}", log_datei)
        sys.exit(1)

    # Step 5: Step 2 - Execute AWK data transformation (Migrated Python script)
    log_and_print("----Starte AWK-Nachbearbeitung----", log_datei)
    
    input_csv_path = os.path.join(all_dir_root, "data", "all_types_export.csv")
    output_out_path = os.path.join(all_dir_root, "data", "all_types_export.out")
    migrated_awk_py = os.path.join(all_dir_root, "aufbereitung", "awk", "k_all_types_transform.py")

    # Ensure data directory exists
    try:
        os.makedirs(os.path.dirname(output_out_path), exist_ok=True)
    except Exception as e:
        log_and_print(f"ERROR: Failed to create output directory: {e}", log_datei)
        sys.exit(1)

    try:
        with open(output_out_path, "w") as out_f:
            subprocess.run(
                [sys.executable, migrated_awk_py, input_csv_path],
                stdout=out_f,
                stderr=subprocess.PIPE,
                text=True,
                check=True
            )
    except subprocess.CalledProcessError as e:
        log_and_print(f"ERROR: AWK-translated Python script failed with exit code {e.returncode}", log_datei)
        if e.stderr:
            log_and_print(f"STDERR: {e.stderr}", log_datei)
        sys.exit(e.returncode)
    except Exception as e:
        log_and_print(f"ERROR: Failed to execute migrated AWK script: {e}", log_datei)
        sys.exit(1)

    # Step 6: Success Logging
    log_and_print("Die Abarbeitung wurde ohne erkennbare Fehler beendet", log_datei)
    sys.exit(0)

if __name__ == "__main__":
    sys.exit(main())