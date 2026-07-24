#!/usr/bin/env python3
import os
import sys
import logging
import re
import shutil
import subprocess
from datetime import datetime
from google.cloud import bigquery
from google.cloud import storage
from google.cloud import dataproc_v1
import google.api_core.exceptions

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Environment Sourcing
GCP_PROJECT = os.environ.get("GCP_PROJECT")
DATAPROC_REGION = os.environ.get("DATAPROC_REGION")
GCS_BUCKET = os.environ.get("GCS_BUCKET")
BQ_DATASET = os.environ.get("BQ_DATASET", "bq_dataset")

BHB_Eintragsnr = os.environ.get("BHB_Eintragsnr")

# SQL templates from source SQL statements
SQL_7 = """UPDATE DWH$TA_K_RECH_ABSGRP
SET   rechnung_datum = :rechnung_datum, 
      ladedatum = :ladedatum
WHERE  monats_id = :monats_id
AND    abs_grp = :abs_grp
AND    dateiname = :dateiname
AND    rechnungsteil = :rechnungsteil"""

SQL_8 = """INSERT INTO DWH$TA_K_RECH_ABSGRP (monats_id, abs_grp, dateiname,  rechnung_datum, rechnungsteil, ladedatum)
VALUES (:monats_id, :abs_grp, :dateiname,  :rechnung_datum, :rechnungsteil, :ladedatum)"""

SQL_9 = """update dwh$ta_k_meldungen 
set anzahl_ds_eof = :anzahl
  , dateiname = :dateiname
  , enderecord_text = :inhalt
  , zusatzinfo = :bemerkung 
where entrynr = :eintragsnr"""


def check_project_dir():
    """Checks PROJECT_DIR and expands symlinks of sys.argv[0]."""
    project_dir = os.environ.get("PROJECT_DIR")
    if not project_dir:
        arg0 = sys.argv[0]
        if os.path.islink(arg0):
            try:
                resolved = os.path.realpath(arg0)
                if not os.path.isfile(resolved):
                    print(f"Internal error: '{arg0}' is a symlink and some problem occurred expanding it.  Please define the environment variable PROJECT_DIR to be the project base directory before invoking this script.")
                    sys.exit(1)
            except Exception:
                print(f"Internal error: '{arg0}' is a symlink and some problem occurred expanding it.  Please define the environment variable PROJECT_DIR to be the project base directory before invoking this script.")
                sys.exit(1)


def check_reposit_tracking():
    """Simulates reposit tracking check and prints EME project path determination error if failing."""
    reposit_tracking = os.environ.get("AB_GRAPH_SCRIPT_REPOSIT_TRACKING", "default")
    is_reposit = False
    if reposit_tracking == "true":
        is_reposit = True
    elif (reposit_tracking == "default" or reposit_tracking == "<unset>" or not reposit_tracking) and len(sys.argv) > 1 and sys.argv[1] == "-reposit-tracking":
        is_reposit = True
        
    if is_reposit:
        project_dir = os.environ.get("PROJECT_DIR", "")
        try:
            if not shutil.which("air"):
                print("Error: cannot determine path to project in EME Datastore; exiting")
                sys.exit(1)
            res = subprocess.run(["air", "sandbox", "find", project_dir, "-project"], capture_output=True, text=True)
            if res.returncode != 0:
                print("Error: cannot determine path to project in EME Datastore; exiting")
                sys.exit(1)
        except Exception:
            print("Error: cannot determine path to project in EME Datastore; exiting")
            sys.exit(1)


def validate_parameters():
    """Validates parameter environment."""
    required_gcp_vars = ["GCP_PROJECT", "DATAPROC_REGION", "GCS_BUCKET"]
    missing_vars = [var for var in required_gcp_vars if not os.environ.get(var)]
    if missing_vars:
        logging.error(f"Missing required environment variables: {missing_vars}")
        sys.exit(1)

    required_legacy_vars = [
        "DB_TNS_NAME_DWH", "DB_USER_DWH", "DB_PASSWD_DWH",
        "DB_TNS_NAME_CRS", "DB_USER_CRS", "DB_PASSWD_CRS",
        "DB_TNS_NAME_SGM", "DB_USER_SGM", "DB_PASSWD_SGM",
        "DB_TNS_NAME_CADS", "DB_USER_CADS", "DB_PASSWD_CADS",
        "DB_TNS_NAME_CACM", "DB_USER_CACM", "DB_PASSWD_CACM",
        "BHB_Projektverzeichnis", "BHB_Graph", "BHB_Prozesstyp", "BHB_Eintragsnr",
        "BHB_Quellverzeichnis", "BHB_Zielverzeichnis", "BHB_Dateimaske",
        "BHB_Kopfdatensatzkennung", "BHB_Nutzdatensatzkennung", "BHB_Endedatensatzkennung",
        "BHB_Dateiname"
    ]
    for var in required_legacy_vars:
        if not os.environ.get(var):
            print(f"Error evaluating: 'parameter {var} of map_rpos_carmen_import', interpretation 'shell'")
            sys.exit(1)


def get_source_file():
    """Locates and returns GCS source file information matching the file mask."""
    logging.info("Searching for source file matching pattern on GCS...")
    storage_client = storage.Client(project=GCP_PROJECT)
    bucket = storage_client.bucket(GCS_BUCKET)
    
    prefix = "crs/work/"
    dateimaske = os.environ.get("BHB_Dateimaske", "CARMEN_B_*_pos.fix")
    regex_pattern = re.compile(dateimaske.replace("*", ".*"))
    
    blobs = bucket.list_blobs(prefix=prefix)
    for blob in blobs:
        filename = os.path.basename(blob.name)
        if regex_pattern.match(filename):
            logging.info(f"Located active transaction source: {blob.name}")
            return filename, prefix, blob.name
            
    raise FileNotFoundError(f"No source files matching mask '{dateimaske}' found in gs://{GCS_BUCKET}/{prefix}")


def execute_bq_query(client, sql, params):
    """Executes standard BigQuery query with named query parameter replacement."""
    bq_sql = sql
    query_params = []
    
    for key, val in params.items():
        placeholder = f":{key}"
        bq_placeholder = f"@{key}"
        bq_sql = bq_sql.replace(placeholder, bq_placeholder)
        
        if isinstance(val, int):
            param_type = "INT64"
        elif isinstance(val, float):
            param_type = "FLOAT64"
        else:
            param_type = "STRING"
            
        query_params.append(bigquery.ScalarQueryParameter(key, param_type, val))
        
    job_config = bigquery.QueryJobConfig(query_parameters=query_params)
    query_job = client.query(bq_sql, job_config=job_config)
    return query_job.result()


def submit_dataproc_serverless_batch(project_id, region, bucket_name, filename, resolved_dir):
    """Submits Dataproc Serverless PySpark batch job for processing."""
    client = dataproc_v1.BatchControllerClient(
        client_options={"api_endpoint": f"{region}-dataproc.googleapis.com"}
    )
    
    pyspark_script = f"gs://{bucket_name}/pyspark/map_rpos_carmen_import.py"
    
    batch = dataproc_v1.Batch(
        pyspark_batch=dataproc_v1.PySparkBatch(
            main_python_file_uri=pyspark_script,
            args=[
                f"--filename={filename}",
                f"--resolved_dir={resolved_dir}",
                f"--gcs_bucket={bucket_name}"
            ]
        )
    )
    
    batch_id = f"map-rpos-carmen-import-{datetime.now().strftime('%Y%m%d-%H%M%S')}"
    request = dataproc_v1.CreateBatchRequest(
        parent=f"projects/{project_id}/regions/{region}",
        batch=batch,
        batch_id=batch_id
    )
    
    try:
        operation = client.create_batch(request=request)
        logging.info(f"Batch creation operation started with ID: {batch_id}. Waiting for completion...")
        response = operation.result()
        logging.info("Dataproc Serverless PySpark batch job completed successfully.")
        return response
    except google.api_core.exceptions.GoogleAPIError as err:
        logging.error(f"Dataproc Serverless job submission failed: {err}")
        raise err


def parse_footer_and_audit(bucket_name, blob_path, dateiname, eintragsnr):
    """Parses file footer and executes final updates on audit tracking tables in BigQuery."""
    logging.info("Parsing footer from GCS file for auditing records...")
    
    storage_client = storage.Client(project=GCP_PROJECT)
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_path)
    
    content = blob.download_as_text(encoding="latin1")
    lines = content.splitlines()
    
    footer_indicator = os.environ.get("BHB_Endedatensatzkennung", "X")
    
    footer_line = None
    for line in lines:
        stripped = line.strip()
        if stripped.startswith(footer_indicator):
            footer_line = stripped
            break
            
    if not footer_line:
        logging.warning("No footer (Endedatensatz) located in file!")
        return
        
    logging.info(f"Found footer line: {footer_line}")
    fields = footer_line.split(";")
    if len(fields) < 5:
        logging.warning("Footer does not contain expected columns count!")
        return
        
    bemerkung = fields[1]
    stichtag_str = fields[2]
    anzahl = fields[3]
    inhalt = fields[4]
    
    # Parse monats_id: date_add_months((date("YYYYMM")) string_substring(in.stichtag,1,6),-1)
    if len(stichtag_str) >= 6:
        try:
            year = int(stichtag_str[0:4])
            month = int(stichtag_str[4:6])
            if month == 1:
                month = 12
                year -= 1
            else:
                month -= 1
            monats_id = int(f"{year:04d}{month:02d}")
        except ValueError:
            monats_id = 0
    else:
        monats_id = 0
        
    # abs_grp is string_substring(in.bemerkung, 10, 5)
    abs_grp = bemerkung[9:14] if len(bemerkung) >= 14 else bemerkung
    
    try:
        rechnung_datum = datetime.strptime(stichtag_str, "%%Y%m%d").date()
    except ValueError:
        rechnung_datum = datetime.now().date()
        
    ladedatum = datetime.now()
    
    bq_client = bigquery.Client(project=GCP_PROJECT)
    
    qualified_sql_7 = SQL_7.replace("DWH$TA_K_RECH_ABSGRP", f"{BQ_DATASET}.ta_k_rech_absgrp")
    qualified_sql_8 = SQL_8.replace("DWH$TA_K_RECH_ABSGRP", f"{BQ_DATASET}.ta_k_rech_absgrp")
    qualified_sql_9 = SQL_9.replace("dwh$ta_k_meldungen", f"{BQ_DATASET}.ta_k_meldungen")
    
    # Update auditing ABSGRP details
    try:
        # Check if record exists in DWH$TA_K_RECH_ABSGRP
        check_sql = f"""SELECT COUNT(1) as cnt FROM {BQ_DATASET}.ta_k_rech_absgrp 
                         WHERE monats_id = @monats_id AND abs_grp = @abs_grp 
                         AND dateiname = @dateiname AND rechnungsteil = @rechnungsteil"""
        
        check_params = {
            "monats_id": monats_id,
            "abs_grp": abs_grp,
            "dateiname": bemerkung,
            "rechnungsteil": "P"
        }
        
        check_res = execute_bq_query(bq_client, check_sql, check_params)
        record_exists = list(check_res)[0]["cnt"] > 0
        
        if record_exists:
            logging.info("Updating existing ABSGRP audit record...")
            update_params = {
                "rechnung_datum": rechnung_datum.strftime("%Y-%m-%d"),
                "ladedatum": ladedatum.strftime("%Y-%m-%d %H:%M:%S"),
                "monats_id": monats_id,
                "abs_grp": abs_grp,
                "dateiname": bemerkung,
                "rechnungsteil": "P"
            }
            execute_bq_query(bq_client, qualified_sql_7, update_params)
        else:
            logging.info("Inserting new ABSGRP audit record...")
            insert_params = {
                "monats_id": monats_id,
                "abs_grp": abs_grp,
                "dateiname": bemerkung,
                "rechnung_datum": rechnung_datum.strftime("%Y-%m-%d"),
                "rechnungsteil": "P",
                "ladedatum": ladedatum.strftime("%Y-%m-%d %H:%M:%S")
            }
            execute_bq_query(bq_client, qualified_sql_8, insert_params)
            
    except Exception as audit_err:
        logging.error(f"Error updating ABSGRP audit table: {audit_err}")
        raise audit_err
        
    # Update Job Control Message Audit table (DWH$TA_K_MELDUNGEN)
    try:
        logging.info("Updating job execution status in control directory table...")
        meld_params = {
            "anzahl": int(anzahl) if anzahl.isdigit() else 0,
            "dateiname": dateiname,
            "inhalt": inhalt,
            "bemerkung": bemerkung,
            "eintragsnr": int(eintragsnr) if eintragsnr and eintragsnr.isdigit() else 0
        }
        execute_bq_query(bq_client, qualified_sql_9, meld_params)
    except Exception as meld_err:
        logging.error(f"Error updating control message audit: {meld_err}")
        raise meld_err


def main():
    check_project_dir()
    check_reposit_tracking()
    validate_parameters()
    
    # Step 1: Discover input source file from GCS
    filename, resolved_dir, blob_path = get_source_file()
    
    # Step 2: Submit Dataproc Serverless PySpark Batch processing job
    submit_dataproc_serverless_batch(
        project_id=GCP_PROJECT,
        region=DATAPROC_REGION,
        bucket_name=GCS_BUCKET,
        filename=filename,
        resolved_dir=resolved_dir
    )
    
    # Step 3: Run final auditing validations and tracking updates in BigQuery
    parse_footer_and_audit(
        bucket_name=GCS_BUCKET,
        blob_path=blob_path,
        dateiname=filename,
        eintragsnr=BHB_Eintragsnr
    )
    
    logging.info("All orchestration processes for map_rpos_carmen_import completed successfully.")


if __name__ == "__main__":
    try:
        main()
        sys.exit(0)
    except Exception as e:
        logging.critical(f"Execution aborted due to fatal error: {e}")
        sys.exit(3)