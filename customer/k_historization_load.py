#!/usr/bin/env python3
"""
k_historization_load.py

SCD Type 2 merge of this week's customer score/segment into the segment
dimension, followed by a sanity check that the number of newly-versioned
rows is not implausibly large (a common symptom of a bad join key causing
every row to look "changed").
"""

import os
import sys
import datetime
import re
from google.cloud import bigquery

def log(message: str) -> None:
    current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{current_time}] {message}")

def preprocess_query(query: str) -> str:
    # Replace ${VAR} or $VAR with values from environment variables
    def replacer(match):
        var_name = match.group(1) or match.group(2)
        return os.environ.get(var_name, match.group(0))
    
    # Match ${VAR} or $VAR
    query = re.sub(r'\$\{([A-Za-z0-9_]+)\}', replacer, query)
    query = re.sub(r'\$([A-Za-z0-9_]+)', replacer, query)
    return query

def execute_sql_file(client: bigquery.Client, file_path: str, run_date: str) -> bigquery.QueryJob:
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"SQL file not found at {file_path}")
    
    with open(file_path, 'r', encoding='utf-8') as f:
        query = f.read()
    
    # Preprocess the query to resolve environment variable placeholders (e.g. ${BQ_DATASET})
    query = preprocess_query(query)
    
    # Run query with RUN_DATE query parameter
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("RUN_DATE", "STRING", run_date)
        ]
    )
    
    query_job = client.query(query, job_config=job_config)
    query_job.result()  # Wait for the query to finish
    return query_job

def main() -> int:
    # Initialize environment parameters
    CRM_HOME = os.environ.get("CRM_HOME", "/opt/etl/customer")
    MAX_EXPECTED_CHANGE_PCT = 25
    
    # Retrieve RUN_DATE from command line argument if provided, else from environment
    RUN_DATE = sys.argv[1] if len(sys.argv) > 1 else os.environ.get("RUN_DATE", "")
    
    # Global environment configurations
    GCP_PROJECT = os.environ.get("GCP_PROJECT")
    GCP_REGION = os.environ.get("GCP_REGION")
    
    # Configure BigQuery Client
    client_kwargs = {}
    if GCP_PROJECT:
        client_kwargs["project"] = GCP_PROJECT
    if GCP_REGION:
        client_kwargs["location"] = GCP_REGION
        
    client = bigquery.Client(**client_kwargs)

    # 1. Run SCD2 merge for customer segment dimension
    log("Running SCD2 merge for customer segment dimension")
    sql_script_1 = os.path.join(CRM_HOME, "customer", "d_historization_load.sql")
    
    try:
        execute_sql_file(client, sql_script_1, RUN_DATE)
    except Exception as e:
        merge_rc = 1
        log(f"ERROR: d_historization_load.sql failed with rc={merge_rc}")
        log(f"Details: {str(e)}")
        sys.exit(1)

    # 2. Execute quality check SQL script and capture output
    sql_script_2 = os.path.join(CRM_HOME, "customer", "d_segment_quality_check.sql")
    try:
        qc_job = execute_sql_file(client, sql_script_2, RUN_DATE)
        rows = list(qc_job)
        if rows:
            changed_pct_val = rows[0][0]
            if changed_pct_val is not None:
                # Remove all whitespace equivalents (tr -d '[:space:]')
                changed_pct_str = "".join(str(changed_pct_val).split())
            else:
                changed_pct_str = ""
        else:
            changed_pct_str = ""
    except Exception as e:
        log(f"Details: {str(e)}")
        changed_pct_str = ""

    # 3. Validate quality check output
    if not changed_pct_str:
        log("WARN: could not compute changed-row percentage - skipping sanity check")
        sys.exit(0)

    # 4. Evaluate threshold breach
    try:
        changed_pct = int(changed_pct_str)
        if changed_pct > MAX_EXPECTED_CHANGE_PCT:
            log(f"WARN: {changed_pct}% of customers changed segment this week (expected <= {MAX_EXPECTED_CHANGE_PCT}%) - flagging for review, not failing the job")
    except ValueError:
        log(f"WARN: Quality check output '{changed_pct_str}' is not a valid integer - skipping validation check")
        # Ensure we still log completion with whatever value was returned if it can't be parsed as integer
        log(f"Historization merge complete, {changed_pct_str}% of customers re-versioned")
        sys.exit(0)

    # 5. Log completion and exit
    log(f"Historization merge complete, {changed_pct}% of customers re-versioned")
    sys.exit(0)

if __name__ == "__main__":
    main()