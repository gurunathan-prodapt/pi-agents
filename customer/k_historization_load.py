#!/usr/bin/env python3
"""
k_historization_load.py

Orchestrates an SCD Type 2 historization load for customer segments via BigQuery 
and performs a post-load sanity check on the percentage of changed rows.
"""

import os
import sys
from datetime import datetime
from google.cloud import bigquery

# Step 1: Environment Setup & Variable Initialization
CRM_HOME = os.environ.get("CRM_HOME", "/opt/etl/customer")
GCP_PROJECT = os.environ.get("GCP_PROJECT")
BQ_DATASET = os.environ.get("BQ_DATASET")

MAX_EXPECTED_CHANGE_PCT_ENV = os.environ.get("MAX_EXPECTED_CHANGE_PCT", "25")
try:
    MAX_EXPECTED_CHANGE_PCT = int(MAX_EXPECTED_CHANGE_PCT_ENV)
except ValueError:
    MAX_EXPECTED_CHANGE_PCT = 25

RUN_DATE = os.environ.get("RUN_DATE")
if not RUN_DATE:
    # Default to today's date formatted as YYYY-MM-DD if not supplied (corresponds to &$TODAY)
    RUN_DATE = datetime.now().strftime("%Y-%m-%d")

# Step 2: Custom Logging Definition
def log(message: str, file=sys.stdout):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {message}", file=file)

def main():
    # Construct paths to SQL scripts
    sql_load_path = os.path.join(CRM_HOME, "customer", "d_historization_load.sql")
    sql_qc_path = os.path.join(CRM_HOME, "customer", "d_segment_quality_check.sql")

    # Read the SQL files
    try:
        with open(sql_load_path, "r", encoding="utf-8") as f:
            sql_text = f.read()
    except Exception as e:
        log(f"ERROR: Failed to read SQL file {sql_load_path}: {e}", file=sys.stderr)
        return 1

    try:
        with open(sql_qc_path, "r", encoding="utf-8") as f:
            qc_sql_text = f.read()
    except Exception as e:
        log(f"ERROR: Failed to read SQL file {sql_qc_path}: {e}", file=sys.stderr)
        return 1

    # Initialize BigQuery Client
    try:
        client = bigquery.Client(project=GCP_PROJECT) if GCP_PROJECT else bigquery.Client()
    except Exception as e:
        log(f"ERROR: Failed to initialize BigQuery Client: {e}", file=sys.stderr)
        return 1

    # Set up QueryJobConfig for query parameter injection
    query_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("run_date", "STRING", RUN_DATE),
        ]
    )

    # Step 3: Run SCD2 Merge
    log("Running SCD2 merge for customer segment dimension")
    try:
        query_job = client.query(sql_text, job_config=query_config)
        query_job.result()  # Wait for the query job to complete
    except Exception as e:
        # Step 4: Handle Failure of SCD2 Merge
        log("ERROR: d_historization_load.sql failed with rc=1", file=sys.stderr)
        log(f"Details: {e}", file=sys.stderr)
        return 1

    # Step 5: Run Segment Quality Check
    try:
        qc_job = client.query(qc_sql_text, job_config=query_config)
        qc_results = qc_job.result()

        changed_pct_str = ""
        for row in qc_results:
            if len(row) > 0:
                val = row[0]
                if val is not None:
                    changed_pct_str = str(val).strip()
            break
    except Exception as e:
        log(f"ERROR: Quality check database execution failed: {e}", file=sys.stderr)
        changed_pct_str = ""

    # Step 7: Handle Empty Output Check (mimics empty stdout check)
    if not changed_pct_str:
        log("WARN: could not compute changed-row percentage - skipping sanity check")
        return 0

    # Step 8: Perform Safety Threshold Comparisons
    try:
        changed_pct = int(float(changed_pct_str))
        if changed_pct > MAX_EXPECTED_CHANGE_PCT:
            log(f"WARN: {changed_pct}% of customers changed segment this week (expected <= {MAX_EXPECTED_CHANGE_PCT}%) - flagging for review, not failing the job")
    except ValueError:
        # Suppress conversion errors (mimics 2>/dev/null in KSH)
        pass

    # Step 9: Process Completion
    log(f"Historization merge complete, {changed_pct_str}% of customers re-versioned")
    return 0

if __name__ == "__main__":
    sys.exit(main())