#!/usr/bin/env python3
import sys
import os
import datetime
from google.cloud import bigquery

def log(message: str):
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {message}")

def main():
    # Step 1: Initialize configuration parameters with defaults
    crm_home = os.environ.get("CRM_HOME", "/opt/etl/customer")
    
    max_expected_change_pct = 25
    max_expected_change_pct_env = os.environ.get("MAX_EXPECTED_CHANGE_PCT")
    if max_expected_change_pct_env is not None:
        try:
            max_expected_change_pct = int(max_expected_change_pct_env)
        except ValueError:
            pass

    # Read GCP specific parameters from environment
    gcp_project = os.environ.get("GCP_PROJECT")
    if not gcp_project:
        log("ERROR: GCP_PROJECT environment variable is not set.")
        sys.exit(1)

    bq_dataset = os.environ.get("BQ_DATASET")
    if not bq_dataset:
        log("ERROR: BQ_DATASET environment variable is not set.")
        sys.exit(1)

    # Step 2: Fetch RUN_DATE from the environment
    run_date = os.environ.get("RUN_DATE")
    if not run_date:
        log("ERROR: RUN_DATE environment variable is not set.")
        sys.exit(1)

    # Step 3: Log step start: "Running SCD2 merge for customer segment dimension"
    log("Running SCD2 merge for customer segment dimension")

    # Step 4: Execute d_historization_load.sql with parameter RUN_DATE
    sql_load_path = os.path.join(crm_home, "customer", "d_historization_load.sql")
    try:
        with open(sql_load_path, "r", encoding="utf-8") as f:
            sql_load_text = f.read()
    except Exception as e:
        log(f"ERROR: Failed to read {sql_load_path}: {e}")
        sys.exit(1)

    try:
        client = bigquery.Client(project=gcp_project)
        sql_load_text = sql_load_text.replace("ANALYTICS_SCHEMA", f"`{gcp_project}.{bq_dataset}`")
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("run_date_param", "STRING", run_date)
            ]
        )
        query_job = client.query(sql_load_text, job_config=job_config)
        query_job.result()
    except Exception as e:
        # Step 5: Capture execution return code. If non-zero, log error "ERROR: d_historization_load.sql failed with rc=..."
        merge_rc = 1
        log(f"ERROR: d_historization_load.sql failed with rc={merge_rc}")
        sys.exit(1)

    # Step 6: Execute d_segment_quality_check.sql
    sql_check_path = os.path.join(crm_home, "customer", "d_segment_quality_check.sql")
    try:
        with open(sql_check_path, "r", encoding="utf-8") as f:
            sql_check_text = f.read()
    except Exception as e:
        log("WARN: could not compute changed-row percentage - skipping sanity check")
        sys.exit(0)

    changed_pct_raw = ""
    try:
        sql_check_text = sql_check_text.replace("ANALYTICS_SCHEMA", f"`{gcp_project}.{bq_dataset}`")
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("run_date", "STRING", run_date)
            ]
        )
        query_job = client.query(sql_check_text, job_config=job_config)
        results = query_job.result()
        for row in results:
            changed_pct_raw = str(row[0])
            break
    except Exception as e:
        log("WARN: could not compute changed-row percentage - skipping sanity check")
        sys.exit(0)

    # Step 7: Strip whitespaces from the database output
    changed_pct_clean = "".join(changed_pct_raw.split())

    # Step 8: If changed_pct is empty/null, log "WARN: could not compute changed-row percentage - skipping sanity check" and exit 0.
    if not changed_pct_clean:
        log("WARN: could not compute changed-row percentage - skipping sanity check")
        sys.exit(0)

    # Step 9: Convert changed_pct to an integer
    try:
        changed_pct_val = int(float(changed_pct_clean))
    except ValueError:
        log("WARN: could not compute changed-row percentage - skipping sanity check")
        sys.exit(0)

    if changed_pct_val > max_expected_change_pct:
        log(f"WARN: {changed_pct_val}% of customers changed segment this week (expected <= {max_expected_change_pct}%) - flagging for review, not failing the job")

    # Step 10: Log completion message
    log(f"Historization merge complete, {changed_pct_val}% of customers re-versioned")
    sys.exit(0)

if __name__ == "__main__":
    main()