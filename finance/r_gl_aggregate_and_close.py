#!/usr/bin/env python3
import os
import sys
import subprocess
import datetime
import smtplib
from email.message import EmailMessage

# Step 1: Initialize environment configuration and validate required inputs
FIN_HOME = os.environ.get("FIN_HOME", "/opt/etl/finance")
FIN_ORA_USER = os.environ.get("FIN_ORA_USER", "fin_etl")
FIN_ORA_PASS = os.environ.get("FIN_ORA_PASS", "changeit")
FIN_ORA_SID = os.environ.get("FIN_ORA_SID", "FINPRD")
NOTIFY_EMAIL = os.environ.get("NOTIFY_EMAIL", "finance-etl@example.com")

# GLOBAL environment-wide variables for GCP target infrastructure
GCP_PROJECT = os.environ.get("GCP_PROJECT")
DATAPROC_REGION = os.environ.get("DATAPROC_REGION")
DATAPROC_CLUSTER = os.environ.get("DATAPROC_CLUSTER")
GCS_BUCKET = os.environ.get("GCS_BUCKET")

PERIOD_NAME = os.environ.get("PERIOD_NAME")
if not PERIOD_NAME:
    print("ERROR: Environment variable 'PERIOD_NAME' must be set", file=sys.stderr)
    sys.exit(1)

FISCAL_YEAR = os.environ.get("FISCAL_YEAR")
if not FISCAL_YEAR:
    print("ERROR: Environment variable 'FISCAL_YEAR' must be set", file=sys.stderr)
    sys.exit(1)

try:
    from google.cloud import bigquery
    HAS_BIGQUERY = True
except ImportError:
    HAS_BIGQUERY = False


# Step 2: Define log utility function
def log(message: str):
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {message}")


def send_notification(period_name: str, fiscal_year: str, notify_email: str):
    log("Notifying stakeholders of month-end close completion")
    completed_at = datetime.datetime.now().strftime("%a %b %d %H:%M:%S %Z %Y")
    email_body = (
        f"Month-end close complete for period: {period_name}\n"
        f"Fiscal year: {fiscal_year}\n"
        f"Completed at: {completed_at}\n"
    )
    subject = f"[FINANCE-OK] Month-End Close {period_name}"

    # Try SMTP first if configured
    smtp_host = os.environ.get("SMTP_HOST")
    if smtp_host:
        try:
            smtp_port = int(os.environ.get("SMTP_PORT", "25"))
            msg = EmailMessage()
            msg.set_content(email_body)
            msg["Subject"] = subject
            msg["From"] = os.environ.get("SMTP_FROM", "finance-etl@example.com")
            msg["To"] = notify_email
            with smtplib.SMTP(smtp_host, smtp_port) as server:
                server.send_message(msg)
            log("Notification email sent successfully via SMTP")
            return
        except Exception as e:
            log(f"WARNING: Failed to send email via SMTP: {e}. Trying mailx fallback...")

    # Fallback to system mailx command
    try:
        subprocess.run(
            ["mailx", "-s", subject, notify_email],
            input=email_body,
            text=True,
            check=True
        )
        log("Notification email sent successfully via mailx")
    except Exception as e:
        log(f"WARNING: Email notification failed to send via mailx: {e}")


def main():
    # Step 3: Spark aggregation execution
    log(f"Submitting GL aggregation Spark job for period {PERIOD_NAME}, fiscal year {FISCAL_YEAR}")
    
    # Check if we should submit to Dataproc or run locally/YARN
    if DATAPROC_CLUSTER and DATAPROC_REGION and GCP_PROJECT:
        jar_path = f"gs://{GCS_BUCKET}/jobs/finance-gl-aggregation-assembly.jar" if GCS_BUCKET else "/opt/spark/jobs/finance-gl-aggregation-assembly.jar"
        spark_cmd = [
            "gcloud", "dataproc", "jobs", "submit", "spark",
            "--cluster", DATAPROC_CLUSTER,
            "--region", DATAPROC_REGION,
            "--project", GCP_PROJECT,
            "--jars", jar_path,
            "--properties", "spark.sql.shuffle.partitions=200",
            "--",
            "--period-name", PERIOD_NAME,
            "--fiscal-year", FISCAL_YEAR
        ]
    else:
        spark_cmd = [
            "spark-submit",
            "--master", "yarn",
            "--deploy-mode", "cluster",
            "--num-executors", "6",
            "--executor-memory", "6g",
            "--conf", "spark.sql.shuffle.partitions=200",
            "/opt/spark/jobs/finance-gl-aggregation-assembly.jar",
            "--period-name", PERIOD_NAME,
            "--fiscal-year", FISCAL_YEAR
        ]

    # Step 4: Spark execution status check
    try:
        subprocess.run(spark_cmd, check=True)
        log("GL aggregation completed successfully")
    except subprocess.CalledProcessError as e:
        log(f"ERROR: GL aggregation Spark job failed with rc={e.returncode} - close audit will NOT be written")
        sys.exit(1)

    # Step 5 & 6: Close audit record logging
    log(f"Writing close-audit record for period {PERIOD_NAME}")
    if HAS_BIGQUERY and GCP_PROJECT:
        try:
            sql_file_path = os.path.join(FIN_HOME, "finance", "d_gl_close_audit.sql")
            with open(sql_file_path, "r") as f:
                sql_text = f.read()
            
            client = bigquery.Client(project=GCP_PROJECT)
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("period_name", "STRING", PERIOD_NAME),
                    bigquery.ScalarQueryParameter("fiscal_year", "STRING", FISCAL_YEAR),
                    bigquery.ScalarQueryParameter("gcp_project", "STRING", GCP_PROJECT),
                    bigquery.ScalarQueryParameter("bq_dataset", "STRING", os.environ.get("BQ_DATASET", "ANALYTICS_SCHEMA"))
                ]
            )
            query_job = client.query(sql_text, job_config=job_config)
            query_job.result()
        except Exception:
            log("ERROR: failed to write close-audit record")
            sys.exit(2)
    else:
        # Fallback to legacy SQL*Plus execution
        sqlplus_cmd = [
            "sqlplus",
            "-s",
            f"{FIN_ORA_USER}/{FIN_ORA_PASS}@{FIN_ORA_SID}",
            f"@{os.path.join(FIN_HOME, 'finance', 'd_gl_close_audit.sql')}",
            PERIOD_NAME,
            FISCAL_YEAR
        ]
        try:
            subprocess.run(sqlplus_cmd, check=True)
        except subprocess.CalledProcessError:
            log("ERROR: failed to write close-audit record")
            sys.exit(2)

    # Step 7: Stakeholder email notification
    send_notification(PERIOD_NAME, FISCAL_YEAR, NOTIFY_EMAIL)

    # Step 8: Complete execution successfully
    log("FINANCE.GL_AGGREGATE_AND_CLOSE finished successfully")
    sys.exit(0)


if __name__ == "__main__":
    main()