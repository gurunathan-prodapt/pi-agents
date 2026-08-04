#!/usr/bin/env python3
# =============================================================================
# Library : retry_handler.py
# Purpose : Shared ETL utility library providing:
#           - retry_command()     : execute with exponential backoff
#           - wait_for_event()    : poll for UC4 event marker file
#           - check_prereq_job()  : verify upstream job completed
#           - log_job_audit()     : write to ETL_JOB_AUDIT table
#
# Usage   : import lib.retry_handler as retry_handler
# Sourced by: customer/process_customer_data.py
# =============================================================================

import os
import sys
import time
import pathlib
import shutil
import subprocess
import socket
import uuid
from datetime import datetime
from google.cloud import bigquery

def ts():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

# ----------------------------------------------------------------------------
# retry_command(cmd, max_retries=3, base_wait_sec=30)
# Executes command with exponential backoff on failure.
# Returns 0 on success, 1 after max_retries failures.
# ----------------------------------------------------------------------------
def retry_command(cmd, max_retries=3, base_wait_sec=30):
    attempt = 0
    while attempt < max_retries:
        attempt += 1
        print(f"[{ts()}] [retry_command] Attempt {attempt}/{max_retries}: {cmd}")

        cp = subprocess.run(cmd, shell=True)
        rc = cp.returncode

        if rc == 0:
            print(f"[{ts()}] [retry_command] Success on attempt {attempt}.")
            return 0

        if attempt < max_retries:
            wait = base_wait_sec * attempt
            print(f"[{ts()}] [retry_command] Failed (rc={rc}). Waiting {wait}s...")
            time.sleep(wait)

    print(f"[{ts()}] [retry_command] All {max_retries} attempts failed.")
    return 1

# ----------------------------------------------------------------------------
# wait_for_event(event_name, event_value, max_polls=60, poll_interval_sec=60)
# Polls for UC4 event completion via marker file or UC4 API.
# Event marker files are created at: /opt/etl/events/<EVENT_NAME>_<VALUE>.done
# Returns 0 if event detected within timeout, 1 on timeout.
# ----------------------------------------------------------------------------
def wait_for_event(event_name, event_value, max_polls=60, poll_interval_sec=60):
    marker_dir = os.environ.get("ETL_EVENTS_DIR", "/opt/etl/events")
    marker_file = pathlib.Path(marker_dir) / f"{event_name}_{event_value}.done"
    attempt = 0

    print(f"[{ts()}] [wait_for_event] Waiting for {event_name}={event_value}")
    print(f"  Marker: {marker_file}")

    while attempt < max_polls:
        attempt += 1

        # Check marker file first (fastest, no API call)
        if marker_file.is_file():
            print(f"[{ts()}] [wait_for_event] Event detected via marker: {marker_file}")
            return 0

        # Fallback: query UC4 API (if uc4api is available)
        if shutil.which("uc4api"):
            cp = subprocess.run(
                ["uc4api", "check_event", event_name, f"value={event_value}"],
                capture_output=True,
                text=True,
                stderr=subprocess.DEVNULL
            )
            uc4_status = cp.stdout.strip()
            if uc4_status in ("COMPLETED", "RAISED"):
                print(f"[{ts()}] [wait_for_event] Event confirmed via UC4 API.")
                try:
                    marker_file.parent.mkdir(parents=True, exist_ok=True)
                    marker_file.touch()
                except Exception:
                    pass
                return 0

        if attempt < max_polls:
            print(f"[{ts()}] [wait_for_event] Poll {attempt}/{max_polls} - not yet. Sleeping {poll_interval_sec}s...")
            time.sleep(poll_interval_sec)

    print(f"[{ts()}] [wait_for_event] TIMEOUT: {event_name}={event_value} not detected after {max_polls} polls.")
    return 1

# ----------------------------------------------------------------------------
# check_prereq_job(job_name, run_date)
# Checks whether a prerequisite UC4 job completed successfully for the date.
# Queries etl_job_audit table in BigQuery.
# Returns 0 if prereq completed, 1 otherwise.
# ----------------------------------------------------------------------------
def check_prereq_job(job_name, run_date):
    gcp_project = os.environ.get("GCP_PROJECT")
    bq_dataset = os.environ.get("BQ_DATASET")

    if not gcp_project or not bq_dataset:
        print("[check_prereq_job] WARN: ORA_CONNECT not set. Skipping DB check.")
        return 0

    client = bigquery.Client(project=gcp_project)
    table_ref = f"{gcp_project}.{bq_dataset}.etl_job_audit"

    sql = f"""
        SELECT JOB_STATUS
        FROM `{table_ref}`
        WHERE JOB_NAME = @job_name
        AND RUN_DATE = CAST(@run_date AS DATE)
        ORDER BY AUDIT_TIMESTAMP DESC
        LIMIT 1
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("job_name", "STRING", job_name),
            bigquery.ScalarQueryParameter("run_date", "STRING", run_date),
        ]
    )

    try:
        query_job = client.query(sql, job_config=job_config)
        results = list(query_job.result())
        if results:
            status = results[0].JOB_STATUS
        else:
            status = "NOT_FOUND"
    except Exception as exc:
        print(f"ERROR: check_prereq_job failed: {exc}", file=sys.stderr)
        status = "NOT_FOUND"

    status = str(status).replace(" ", "")
    print(f"[{ts()}] [check_prereq_job] {job_name} @ {run_date} => {status}")

    if status in ("SUCCESS", "COMPLETED"):
        return 0
    return 1

# ----------------------------------------------------------------------------
# log_job_audit(job_name, run_date, job_status, rows_processed=0)
# Writes a job completion record to etl_job_audit table via MERGE (upsert).
# ----------------------------------------------------------------------------
def log_job_audit(job_name, run_date, job_status, rows_processed=0):
    gcp_project = os.environ.get("GCP_PROJECT")
    bq_dataset = os.environ.get("BQ_DATASET")

    if not gcp_project or not bq_dataset:
        print("[log_job_audit] WARN: ORA_CONNECT not set. Skipping audit write.")
        return 0

    client = bigquery.Client(project=gcp_project)
    table_ref = f"{gcp_project}.{bq_dataset}.etl_job_audit"

    host_name = socket.gethostname()
    audit_id = str(uuid.uuid4())

    sql = f"""
        MERGE `{table_ref}` tgt
        USING (SELECT @job_name AS JOB_NAME, CAST(@run_date AS DATE) AS RUN_DATE) src
        ON (tgt.JOB_NAME = src.JOB_NAME AND tgt.RUN_DATE = src.RUN_DATE)
        WHEN MATCHED THEN UPDATE SET
            tgt.JOB_STATUS      = @job_status,
            tgt.ROWS_PROCESSED  = @rows_processed,
            tgt.AUDIT_TIMESTAMP = CURRENT_TIMESTAMP(),
            tgt.HOST_NAME       = @host_name
        WHEN NOT MATCHED THEN INSERT (
            AUDIT_ID, JOB_NAME, RUN_DATE, JOB_STATUS,
            ROWS_PROCESSED, AUDIT_TIMESTAMP, HOST_NAME
        ) VALUES (
            @audit_id,
            @job_name,
            CAST(@run_date AS DATE),
            @job_status,
            @rows_processed,
            CURRENT_TIMESTAMP(),
            @host_name
        )
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("job_name", "STRING", job_name),
            bigquery.ScalarQueryParameter("run_date", "STRING", run_date),
            bigquery.ScalarQueryParameter("job_status", "STRING", job_status),
            bigquery.ScalarQueryParameter("rows_processed", "INT64", int(rows_processed)),
            bigquery.ScalarQueryParameter("host_name", "STRING", host_name),
            bigquery.ScalarQueryParameter("audit_id", "STRING", audit_id),
        ]
    )

    try:
        query_job = client.query(sql, job_config=job_config)
        query_job.result()
    except Exception as exc:
        print(f"ERROR: log_job_audit failed: {exc}", file=sys.stderr)
        return 1

    print(f"[{ts()}] [log_job_audit] {job_name} / {run_date} => {job_status} ({rows_processed} rows)")
    return 0