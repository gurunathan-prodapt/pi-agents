#!/usr/bin/env python3
# =============================================================================
# Script  : process_customer_data.py
# Purpose : Orchestrates the weekly CRM customer data load:
#           1. Extracts customer profiles from CRM source
#           2. Runs BigQuery SQL segment extract
#           3. Calls MASTER_CRM_LOAD stored procedure
#           4. Invokes Python customer scoring
#           Waits on upstream events: FINANCE_GL_CLOSE_COMPLETE,
#           RETAIL_DAILY_COMPLETE (from finance/ and sales/ pipelines).
#
# Usage   : process_customer_data.py <RUN_DATE> <SEGMENT> [FORCE_RELOAD]
# Example : process_customer_data.py 2024-01-15 ALL N
# =============================================================================

import argparse
import datetime as dt
import os
import subprocess
import sys
from pathlib import Path
from google.cloud import bigquery

# Centralized, migrated Python library imports
try:
    from etl_lib.retry_handler import wait_for_event, log_job_audit
except ImportError:
    # Fallback/Mock for local validation or environments without etl_lib installed
    def wait_for_event(event_name: str, run_date: str) -> int:
        print(f"[MOCK] wait_for_event called for {event_name} on {run_date}")
        return 0

    def log_job_audit(job_name: str, run_date: str, status: str, count: str) -> None:
        print(f"[MOCK] log_job_audit called: {job_name}, {run_date}, {status}, {count}")


def load_env_config() -> None:
    """Sources the crm environment properties if available."""
    env_config_dir = os.environ.get("ENV_CONFIG_DIR", "/opt/etl/config")
    env_config_path = os.path.join(env_config_dir, "env_crm.properties")
    if os.path.exists(env_config_path):
        with open(env_config_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                if "=" in line:
                    key, val = line.split("=", 1)
                    key = key.strip()
                    val = val.strip().strip('"').strip("'")
                    os.environ[key] = val


def main() -> int:
    parser = argparse.ArgumentParser(description="Process CRM customer data load.")
    parser.add_argument("run_date", help="RUN_DATE (YYYY-MM-DD) required")
    parser.add_argument("customer_segment", nargs="?", default="ALL", help="CUSTOMER_SEGMENT (default: ALL)")
    parser.add_argument("force_reload", nargs="?", default="N", help="FORCE_RELOAD (default: N)")
    args = parser.parse_args()

    # Source configurations
    load_env_config()

    # Retrieve Global Environment Configurations
    gcp_project = os.environ.get("GCP_PROJECT")
    bq_dataset = os.environ.get("BQ_DATASET")
    if not gcp_project:
        raise ValueError("GCP_PROJECT environment variable must be set.")
    if not bq_dataset:
        raise ValueError("BQ_DATASET environment variable must be set.")

    # Compute logging and naming formats
    run_date_fmt = args.run_date.replace("-", "")
    log_dir = os.environ.get("LOG_DIR", "/opt/etl/logs/crm")
    os.makedirs(log_dir, exist_ok=True)
    timestamp = dt.datetime.now().strftime("%H%M%S")
    log_file_path = os.path.join(log_dir, f"crm_load_{args.customer_segment}_{run_date_fmt}_{timestamp}.log")

    # Setup export variables
    os.environ["RUN_DATE"] = args.run_date
    os.environ["CUSTOMER_SEGMENT"] = args.customer_segment
    os.environ["FORCE_RELOAD"] = args.force_reload
    os.environ["RUN_DATE_FMT"] = run_date_fmt

    def log(msg: str) -> None:
        current_time = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        formatted_msg = f"[{current_time}] [{args.customer_segment}] {msg}"
        print(formatted_msg)
        try:
            with open(log_file_path, "a", encoding="utf-8") as lf:
                lf.write(formatted_msg + "\n")
        except Exception:
            pass

    log("=== process_customer_data.ksh START ===")
    log(f"RUN_DATE={args.run_date}  SEGMENT={args.customer_segment}  FORCE={args.force_reload}")

    # ----------------------------------------------------------------------------
    # 1. Wait for upstream event: FINANCE_GL_CLOSE_COMPLETE
    # ----------------------------------------------------------------------------
    log("Waiting for FINANCE_GL_CLOSE_COMPLETE event...")
    event_rc = wait_for_event("FINANCE_GL_CLOSE_COMPLETE", args.run_date)
    if event_rc != 0:
        log("ERROR: FINANCE_GL_CLOSE_COMPLETE did not complete. Aborting.")
        return 1
    log("FINANCE_GL_CLOSE_COMPLETE received.")

    # ----------------------------------------------------------------------------
    # 2. Wait for RETAIL_DAILY_COMPLETE
    # ----------------------------------------------------------------------------
    log("Waiting for RETAIL_DAILY_COMPLETE event...")
    retail_rc = wait_for_event("RETAIL_DAILY_COMPLETE", args.run_date)
    if retail_rc != 0:
        log("WARN: RETAIL_DAILY_COMPLETE timed out. Proceeding with available data.")
    log("RETAIL_DAILY_COMPLETE received (or timed out).")

    # Initialize BigQuery Client
    client = bigquery.Client(project=gcp_project)

    # ----------------------------------------------------------------------------
    # 3. BigQuery customer segment extract
    # ----------------------------------------------------------------------------
    log("Step 1: Running customer segment extract via SQL*Plus...")

    sqlplus_dir = os.environ.get("SQLPLUS_DIR", "")
    sql_file_path = os.path.join(sqlplus_dir, "customer_segment_extract.sql")
    bq_extract_log_path = os.path.join(log_dir, f"sqlplus_crm_extract_{run_date_fmt}.log")
    
    sqlplus_rc = 0
    try:
        if os.path.exists(sql_file_path):
            with open(sql_file_path, "r", encoding="utf-8") as sf:
                query_text = sf.read()
        else:
            raise FileNotFoundError(f"Transpiled customer_segment_extract.sql not found at: {sql_file_path}")

        with open(bq_extract_log_path, "w", encoding="utf-8") as bq_log:
            bq_log.write(f"Executing BigQuery customer segment extract for {args.run_date}\n")
            
            query_job = client.query(
                query_text,
                job_config=bigquery.QueryJobConfig(
                    query_parameters=[
                        bigquery.ScalarQueryParameter("run_date", "STRING", args.run_date),
                        bigquery.ScalarQueryParameter("customer_segment", "STRING", args.customer_segment),
                        bigquery.ScalarQueryParameter("batch_size", "INT64", int(os.environ.get("BATCH_SIZE", "5000"))),
                        bigquery.ScalarQueryParameter("region_code", "STRING", os.environ.get("REGION_CODE", "ALL")),
                        bigquery.ScalarQueryParameter("run_date_fmt", "STRING", run_date_fmt),
                    ]
                )
            )
            # Wait for execution to finish
            query_job.result()
            bq_log.write("BigQuery customer segment extract completed successfully.\n")
    except Exception as e:
        log(f"ERROR: Extract SQL execution failed: {e}")
        try:
            with open(bq_extract_log_path, "a", encoding="utf-8") as bq_log:
                bq_log.write(f"Execution failed with exception: {e}\n")
        except Exception:
            pass
        sqlplus_rc = 2

    if sqlplus_rc != 0:
        log(f"ERROR: SQL*Plus extract failed (rc={sqlplus_rc})")
        return 2
    log("Step 1: SQL*Plus extract complete.")

    # ----------------------------------------------------------------------------
    # 4. Validate staging counts
    # ----------------------------------------------------------------------------
    try:
        stg_count_query = f"""
            SELECT COUNT(*) as cnt FROM `{gcp_project}.{bq_dataset}.STG_CUSTOMER_PROFILE`
            WHERE LOAD_DATE = PARSE_DATE('%Y-%m-%d', @run_date)
            AND ETL_STATUS = 'PENDING'
        """
        query_job = client.query(
            stg_count_query,
            job_config=bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("run_date", "STRING", args.run_date)
                ]
            )
        )
        stg_count_res = list(query_job.result())
        stg_count = stg_count_res[0].cnt if stg_count_res else 0
    except Exception as e:
        log(f"ERROR: Failed to query STG_CUSTOMER_PROFILE counts: {e}")
        stg_count = 0

    log(f"STG_CUSTOMER_PROFILE PENDING rows: {stg_count}")

    if stg_count == 0:
        log(f"WARN: No customer staging rows for {args.run_date}")
        if args.force_reload != "Y":
            log("Exiting - no data to process (use FORCE_RELOAD=Y to override).")
            return 0

    # ----------------------------------------------------------------------------
    # 5. Stored Procedure Master Load
    # ----------------------------------------------------------------------------
    log("Step 2: Running PKG_CUSTOMER_HISTORIZATION.MASTER_CRM_LOAD...")
    plsql_rc = 0
    try:
        # Stored Procedure master load called natively from Python via BigQuery Client
        sp_query = f"""
            CALL `{gcp_project}.{bq_dataset}.MASTER_CRM_LOAD`(
                PARSE_DATE('%Y-%m-%d', @run_date),
                @segment
            )
        """
        query_job = client.query(
            sp_query,
            job_config=bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("run_date", "STRING", args.run_date),
                    bigquery.ScalarQueryParameter("segment", "STRING", args.customer_segment)
                ]
            )
        )
        query_job.result()
    except Exception as e:
        log(f"ERROR: MASTER_CRM_LOAD execution failed: {e}")
        plsql_rc = 3

    if plsql_rc != 0:
        log(f"ERROR: PKG_CUSTOMER_HISTORIZATION.MASTER_CRM_LOAD failed (rc={plsql_rc})")
        return 3
    log("Step 2: MASTER_CRM_LOAD complete.")

    # ----------------------------------------------------------------------------
    # 6. Python customer scoring
    # ----------------------------------------------------------------------------
    log("Step 3: Running Python customer scoring...")
    python_dir = os.environ.get("PYTHON_DIR", "/opt/etl/bin")
    scoring_script_path = os.path.join(python_dir, "customer_scoring.py")

    python_rc = 0
    try:
        with open(log_file_path, "a", encoding="utf-8") as lf:
            subprocess.run(
                [
                    "python3",
                    scoring_script_path,
                    "--run-date", args.run_date,
                    "--segment", args.customer_segment,
                    "--env", os.environ.get("ETL_ENV", "PROD"),
                ],
                stdout=lf,
                stderr=subprocess.STDOUT,
                check=True
            )
    except subprocess.CalledProcessError as e:
        python_rc = e.returncode
    except Exception as e:
        log(f"ERROR: Failed to invoke scoring script: {e}")
        python_rc = -1

    if python_rc != 0:
        log(f"WARN: Python scoring returned non-zero (rc={python_rc}) - non-fatal")
    log(f"Step 3: Python scoring complete (rc={python_rc}).")

    # ----------------------------------------------------------------------------
    # 7. Audit log
    # ----------------------------------------------------------------------------
    try:
        log_job_audit("CRM_CUSTOMER_LOAD", args.run_date, "SUCCESS", str(stg_count))
    except Exception as e:
        log(f"WARN: Audit logging failed: {e}")

    log(f"=== process_customer_data.ksh COMPLETED: {args.customer_segment} / {args.run_date} ===")
    return 0


if __name__ == "__main__":
    sys.exit(main())