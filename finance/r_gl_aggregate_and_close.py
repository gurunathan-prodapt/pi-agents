#!/usr/bin/env python3
import os
import sys
import subprocess
import smtplib
from email.mime.text import MIMEText
from datetime import datetime

try:
    from google.cloud import dataproc_v1 as dataproc
    from google.cloud import bigquery
    GCP_LIBS_AVAILABLE = True
except ImportError:
    GCP_LIBS_AVAILABLE = False

def log(message: str):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {message}")

def send_email(subject: str, body: str, recipient: str):
    smtp_host = os.environ.get("SMTP_HOST", "localhost")
    smtp_port = int(os.environ.get("SMTP_PORT", "25"))
    
    msg = MIMEText(body)
    msg["Subject"] = subject
    msg["From"] = os.environ.get("SENDER_EMAIL", "finance-etl@example.com")
    msg["To"] = recipient
    
    try:
        with smtplib.SMTP(smtp_host, smtp_port) as server:
            server.send_message(msg)
        log("Email sent successfully via SMTP.")
    except Exception as e:
        log(f"WARNING: Native SMTP delivery failed: {e}. Falling back to mailx CLI.")
        try:
            subprocess.run(
                ["mailx", "-s", subject, recipient],
                input=body,
                text=True,
                check=True
            )
            log("Email sent successfully via mailx.")
        except Exception as mailx_err:
            log(f"ERROR: Failed to send email via mailx fallback: {mailx_err}")

def run_dataproc_job(project_id, region, cluster_name, gcs_bucket, period_name, fiscal_year):
    jar_file_path = f"gs://{gcs_bucket}/jobs/finance-gl-aggregation-assembly.jar"
    args = ["--period-name", period_name, "--fiscal-year", fiscal_year]
    
    if GCP_LIBS_AVAILABLE:
        log("Using google-cloud-dataproc library to submit Spark job on Dataproc...")
        try:
            job_client = dataproc.JobControllerClient(
                client_options={"api_endpoint": f"{region}-dataproc.googleapis.com:443"}
            )
            job = {
                "placement": {"cluster_name": cluster_name},
                "spark_job": {
                    "main_jar_file_uri": jar_file_path,
                    "args": args,
                    "properties": {
                        "spark.sql.shuffle.partitions": "200"
                    }
                }
            }
            operation = job_client.submit_job_as_operation(
                request={"project_id": project_id, "region": region, "job": job}
            )
            operation.result()
            return 0
        except Exception as e:
            log(f"ERROR: Google Cloud Dataproc client error: {e}")
            return 1
    else:
        log("Google Cloud client libraries not available. Falling back to gcloud CLI...")
        gcloud_cmd = [
            "gcloud", "dataproc", "jobs", "submit", "spark",
            f"--cluster={cluster_name}",
            f"--region={region}",
            f"--project={project_id}",
            f"--jars={jar_file_path}",
            f"--properties=spark.sql.shuffle.partitions=200",
            "--",
            "--period-name", period_name,
            "--fiscal-year", fiscal_year
        ]
        try:
            result = subprocess.run(gcloud_cmd, check=False)
            return result.returncode
        except Exception as e:
            log(f"ERROR: Failed to execute gcloud dataproc command: {e}")
            return 1

def run_bigquery_audit(project_id, fin_home, period_name, fiscal_year):
    sql_path = os.path.join(fin_home, "finance/d_gl_close_audit.sql")
    if not os.path.exists(sql_path):
        log(f"ERROR: SQL file not found at {sql_path}")
        return 2
        
    try:
        with open(sql_path, "r") as f:
            sql_content = f.read()
    except Exception as e:
        log(f"ERROR: Failed to read SQL file: {e}")
        return 2

    if GCP_LIBS_AVAILABLE:
        log("Using google-cloud-bigquery library to run close audit script...")
        try:
            client = bigquery.Client(project=project_id)
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter('period_name', 'STRING', period_name),
                    bigquery.ScalarQueryParameter('fiscal_year', 'STRING', fiscal_year)
                ]
            )
            query_job = client.query(sql_content, job_config=job_config)
            query_job.result()
            return 0
        except Exception as e:
            log(f"ERROR: Google Cloud BigQuery client error: {e}")
            return 2
    else:
        log("Google Cloud client libraries not available. Falling back to bq CLI...")
        bq_cmd = [
            "bq", "query",
            f"--project_id={project_id}",
            "--use_legacy_sql=false",
            f"--parameter=period_name:STRING:{period_name}",
            f"--parameter=fiscal_year:STRING:{fiscal_year}",
            sql_content
        ]
        try:
            result = subprocess.run(bq_cmd, check=False)
            return result.returncode
        except Exception as e:
            log(f"ERROR: Failed to execute bq query command: {e}")
            return 2

def main():
    # Environment Setup
    fin_home = os.environ.get("FIN_HOME", "/opt/etl/finance")
    notify_email = os.environ.get("NOTIFY_EMAIL", "finance-etl@example.com")
    
    # GCP Environment Configuration
    gcp_project = os.environ.get("GCP_PROJECT")
    gcs_bucket = os.environ.get("GCS_BUCKET")
    dataproc_cluster = os.environ.get("DATAPROC_CLUSTER")
    dataproc_region = os.environ.get("DATAPROC_REGION", "us-central1")
    
    # Required parameters check
    period_name = os.environ.get("PERIOD_NAME")
    fiscal_year = os.environ.get("FISCAL_YEAR")
    
    if not period_name or not fiscal_year:
        log("ERROR: REQUIRED environment variables PERIOD_NAME or FISCAL_YEAR are missing.")
        sys.exit(1)

    # If GCP parameters are missing, fallback to local/legacy paths
    if not gcp_project or not gcs_bucket or not dataproc_cluster:
        log("WARNING: GCP environment variables (GCP_PROJECT, GCS_BUCKET, DATAPROC_CLUSTER) are not fully set.")
        log("Falling back to local spark-submit execution...")
        
        log(f"Submitting GL aggregation Spark job for period {period_name}, fiscal year {fiscal_year}")
        spark_cmd = [
            "spark-submit",
            "--master", "yarn",
            "--deploy-mode", "cluster",
            "--num-executors", "6",
            "--executor-memory", "6g",
            "--conf", "spark.sql.shuffle.partitions=200",
            "/opt/spark/jobs/finance-gl-aggregation-assembly.jar",
            "--period-name", period_name,
            "--fiscal-year", fiscal_year
        ]
        try:
            spark_result = subprocess.run(spark_cmd, check=False)
            spark_rc = spark_result.returncode
        except Exception as e:
            log(f"ERROR: Failed to run spark-submit process: {e}")
            sys.exit(1)
    else: 
        log(f"Submitting GL aggregation Spark job for period {period_name}, fiscal year {fiscal_year}")
        spark_rc = run_dataproc_job(
            project_id=gcp_project,
            region=dataproc_region,
            cluster_name=dataproc_cluster,
            gcs_bucket=gcs_bucket,
            period_name=period_name,
            fiscal_year=fiscal_year
        )

    # Evaluate Spark Execution Result
    if spark_rc != 0:
        log(f"ERROR: GL aggregation Spark job failed with rc={spark_rc} - close audit will NOT be written")
        sys.exit(1)
    log("GL aggregation completed successfully")

    log(f"Writing close-audit record for period {period_name}")
    
    # If GCP project is missing, execute via original SQL*Plus wrapper
    if not gcp_project:
        log("GCP_PROJECT not set, attempting execution via legacy sqlplus...")
        fin_ora_user = os.environ.get("FIN_ORA_USER", "fin_etl")
        fin_ora_pass = os.environ.get("FIN_ORA_PASS", "changeit")
        fin_ora_sid = os.environ.get("FIN_ORA_SID", "FINPRD")
        sqlplus_conn = f"{fin_ora_user}/{fin_ora_pass}@{fin_ora_sid}"
        sql_script_path = os.path.join(fin_home, "finance/d_gl_close_audit.sql")
        
        sql_cmd = [
            "sqlplus", "-s",
            sqlplus_conn,
            f"@{sql_script_path}",
            period_name,
            fiscal_year
        ]
        try:
            sql_result = subprocess.run(sql_cmd, check=False)
            sql_rc = sql_result.returncode
        except Exception as e:
            log(f"ERROR: Failed to run sqlplus process: {e}")
            sys.exit(2)
    else:
        sql_rc = run_bigquery_audit(
            project_id=gcp_project,
            fin_home=fin_home,
            period_name=period_name,
            fiscal_year=fiscal_year
        )

    # Evaluate Close Audit Execution Result
    if sql_rc != 0:
        log("ERROR: failed to write close-audit record")
        sys.exit(2)

    # Log completion and execute success notification email
    log("Notifying stakeholders of month-end close completion")
    
    email_subject = f"[FINANCE-OK] Month-End Close {period_name}"
    completed_at = datetime.now().strftime("%a %b %d %H:%M:%S %Z %Y").strip()
    
    email_body = (
        f"Month-end close complete for period: {period_name}\n"
        f"Fiscal year: {fiscal_year}\n"
        f"Completed at: {completed_at}\n"
    )
    
    try:
        send_email(email_subject, email_body, notify_email)
    except Exception as e:
        log(f"WARNING: Email notification failed to send: {e}")

    log("FINANCE.GL_AGGREGATE_AND_CLOSE finished successfully")
    sys.exit(0)

if __name__ == "__main__":
    main()