#!/usr/bin/env python3
import os
import sys
import datetime
import subprocess
from google.cloud import bigquery

def main():
    # Step 1: Initialize environment and check source variables
    # Retrieve and validate required environment variables
    all_dir_root = os.environ.get("ALL_DIR_ROOT")
    if not all_dir_root:
        raise SystemExit("ALL_DIR_ROOT must be set by the calling Airflow task")
        
    dw_orauser = os.environ.get("DW_ORAUSER")
    if not dw_orauser:
        raise SystemExit("DW_ORAUSER must be set by the calling Airflow task")
        
    gcp_project = os.environ.get("GCP_PROJECT")
    if not gcp_project:
        raise SystemExit("GCP_PROJECT must be set by the calling Airflow task")
        
    bq_dataset = os.environ.get("BQ_DATASET")
    if not bq_dataset:
        raise SystemExit("BQ_DATASET must be set by the calling Airflow task")
        
    home = os.environ.get("HOME")
    
    # NOTE: parameter accepted for interface compatibility; unused in original script logic
    prog_name = "ALL_TYPES Showcase Rahmenskript"
    # NOTE: parameter accepted for interface compatibility; unused in original script logic
    prog_version = "V1.0.0"
    
    job_kennung = "ALL_TYPES_MASTER"
    v_sysdate = datetime.datetime.now().strftime("%d%m%Y")
    
    log_datei = os.path.join(all_dir_root, "protokoll", f"all_types_master_{v_sysdate}.log")
    
    # Ensure directory for log file exists
    os.makedirs(os.path.dirname(log_datei), exist_ok=True)
    
    # Step 2: Write header metadata to console and log file
    print(" ----------------- Job -----------------------")
    print(f" JobKennung: '{job_kennung}'")
    print(f" Logdatei  : '{log_datei}'")
    print(" ---------------------------------------------")
    
    try:
        with open(log_datei, "a") as log_file:
            log_file.write(" ----------------- Job -----------------------\n")
            log_file.write(f" JobKennung: '{job_kennung}'\n")
            log_file.write(f" Logdatei  : '{log_datei}'\n")
            log_file.write(" ---------------------------------------------\n")
    except Exception as e:
        print(f"Warning: Failed to write to log file: {e}", file=sys.stderr)

    # Step 3: Oracle SQL Refresh (BigQuery Execution)
    print("----Starte SQL-Refresh----")
    try:
        with open(log_datei, "a") as log_file:
            log_file.write("----Starte SQL-Refresh----\n")
    except Exception:
        pass
        
    sql_script_path = os.path.join(all_dir_root, "aufbereitung", "sql", "d_all_types.sql")
    
    try:
        with open(sql_script_path, "r") as sql_file:
            sql_text = sql_file.read()
    except Exception as e:
        error_msg = f"ERROR: Failed to read SQL file {sql_script_path}: {e}\n"
        print(error_msg, file=sys.stderr)
        try:
            with open(log_datei, "a") as log_file:
                log_file.write(error_msg)
        except Exception:
            pass
        sys.exit(1)
        
    try:
        client = bigquery.Client(project=gcp_project)
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("gcp_project", "STRING", gcp_project),
                bigquery.ScalarQueryParameter("bq_dataset", "STRING", bq_dataset)
            ]
        )
        query_job = client.query(sql_text, job_config=job_config)
        query_job.result()  # Wait for query to complete
    except Exception as e:
        error_msg = f"ERROR: BigQuery SQL execution failed: {e}\n"
        print(error_msg, file=sys.stderr)
        try:
            with open(log_datei, "a") as log_file:
                log_file.write(error_msg)
        except Exception:
            pass
        sys.exit(1)
        
    # Step 4: AWK Nachbearbeitung der Exportdatei (using migrated Python script)
    print("----Starte AWK-Nachbearbeitung----")
    try:
        with open(log_datei, "a") as log_file:
            log_file.write("----Starte AWK-Nachbearbeitung----\n")
    except Exception:
        pass
        
    awk_script_py = os.path.join(all_dir_root, "aufbereitung", "awk", "k_all_types_transform.py")
    csv_input = os.path.join(all_dir_root, "data", "all_types_export.csv")
    csv_output = os.path.join(all_dir_root, "data", "all_types_export.out")
    
    # Ensure data directory exists
    os.makedirs(os.path.dirname(csv_output), exist_ok=True)
    
    try:
        with open(csv_output, "w") as out_file:
            subprocess.run(
                ["python3", awk_script_py, csv_input],
                stdout=out_file,
                check=True
            )
    except subprocess.CalledProcessError as e:
        error_msg = f"ERROR: AWK post-processing failed with exit code {e.returncode}\n"
        print(error_msg, file=sys.stderr)
        try:
            with open(log_datei, "a") as log_file:
                log_file.write(error_msg)
        except Exception:
            pass
        sys.exit(e.returncode)
    except Exception as e:
        error_msg = f"ERROR: Failed to run AWK command: {e}\n"
        print(error_msg, file=sys.stderr)
        try:
            with open(log_datei, "a") as log_file:
                log_file.write(error_msg)
        except Exception:
            pass
        sys.exit(1)
        
    # Step 5: Success Logging & Exit
    success_msg = "Die Abarbeitung wurde ohne erkennbare Fehler beendet\n"
    print("Die Abarbeitung wurde ohne erkennbare Fehler beendet")
    try:
        with open(log_datei, "a") as log_file:
            log_file.write(success_msg)
    except Exception:
        pass
        
    sys.exit(0)

if __name__ == "__main__":
    main()