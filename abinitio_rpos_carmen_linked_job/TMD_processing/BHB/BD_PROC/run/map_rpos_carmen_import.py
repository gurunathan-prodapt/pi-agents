#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Modernized Python 3 script replacing map_rpos_carmen_import legacy ksh orchestration pipeline

import os
import sys
import shutil
import tempfile
import platform
import subprocess
import traceback
import re
import argparse

try:
    from google.cloud import bigquery
    from google.cloud import dataproc_v1
    GCP_LIBS_AVAILABLE = True
except ImportError:
    GCP_LIBS_AVAILABLE = False

# Step 1: Initialize System & Environment Settings
def setup_environment():
    os.environ["AB_HOME"] = os.environ.get("AB_HOME", "/appl/local/abinitio/abinitio")
    os.environ["MPOWERHOME"] = os.environ.get("MPOWERHOME", os.environ["AB_HOME"])
    os.environ["AB_REPORT"] = os.environ.get("AB_REPORT", "monitor=60 processes scroll=true")
    os.environ["AB_AIR_HOME"] = os.environ.get("AB_AIR_HOME", "/appl/local/abinitio/abinitio-V2-14")
    os.environ["AB_COMPATIBILITY"] = "2.14.59"
    os.environ["AB_GRAPH_NAME"] = "map_rpos_carmen_import"
    os.environ["NLS_NUMERIC_CHARACTERS"] = ". "

# Step 2: Validate Required Environment Parameters
def check_parameters():
    required_params = [
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
    
    required_gcp_params = ["GCP_PROJECT", "BQ_DATASET", "GCS_BUCKET"]
    
    missing_params = []
    # Check business/legacy parameters to output exactly as the original shell evaluation did
    for param in required_params:
        if not os.environ.get(param):
            print(f"Error evaluating: 'parameter {param} of map_rpos_carmen_import', interpretation 'shell'", file=sys.stderr)
            missing_params.append(param)
            
    for param in required_gcp_params:
        if not os.environ.get(param):
            print(f"Error evaluating: 'parameter {param} of map_rpos_carmen_import', interpretation 'shell'", file=sys.stderr)
            missing_params.append(param)
            
    if missing_params:
        raise SystemExit(1)

# Step 3: Parse Command-Line Options & Expand Symlinks
def parse_arguments():
    # Expand symlinks to match legacy shell wrapper behavior exactly
    arg0 = sys.argv[0]
    while os.path.islink(arg0):
        if not os.path.exists(arg0):
            print("Internal error: '$0' is a symlink and some problem occurred expanding\nit.  Please define the environment variable PROJECT_DIR to be the project\nbase directory before invoking this script.", file=sys.stderr)
            sys.exit(1)
        # resolve the symlink
        target = os.readlink(arg0)
        if not os.path.isabs(target):
            arg0 = os.path.normpath(os.path.join(os.path.dirname(arg0), target))
        else:
            arg0 = target
            
    parser = argparse.ArgumentParser(description="Modernized map_rpos_carmen_import wrapper")
    parser.add_argument("mode", nargs="?", default=None, help="Execution mode (-help, -reposit-tracking, etc.)")
    
    args, unknown = parser.parse_known_args()
    
    if args.mode == "-help":
        print("Displaying Ab Initio Wrapper Help Options...", file=sys.stderr)
        sys.exit(1)
        
    elif args.mode == "-reposit-tracking":
        project_dir = os.environ.get("PROJECT_DIR", ".")
        try:
            res = subprocess.run(["air", "sandbox", "find", project_dir, "-project"], capture_output=True, text=True, check=True)
            project_name = res.stdout.strip()
            
            os.environ["AB_GRAPH_SCRIPT_REPOSIT_TRACKING"] = "false"
            run_cmd = [
                f"{os.environ['AB_HOME']}/bin/run-and-reposit",
                f"{project_name}/mp/map_rpos_carmen_import.mp",
                project_name,
                sys.argv[0]
            ] + unknown
            
            reposit_res = subprocess.run(run_cmd, check=True)
            sys.exit(reposit_res.returncode)
        except subprocess.CalledProcessError:
            print("Error: cannot determine path to project in EME Datastore; exiting", file=sys.stderr)
            sys.exit(1)

# BigQuery Execution Helper
def run_bq_query(sql_text, params):
    if not GCP_LIBS_AVAILABLE:
        print("WARNING: Google Cloud libraries not installed. Skipping BigQuery execution.", file=sys.stderr)
        return

    client = bigquery.Client()
    
    # Translate Oracle-style bind parameters ':name' to BigQuery '@name'
    translated_sql = re.sub(r':([a-zA-Z0-9_]+)', r'@\1', sql_text)
    
    bq_dataset = os.environ.get("BQ_DATASET")
    gcp_project = os.environ.get("GCP_PROJECT")
    
    tables_to_map = [
        "DWH$TA_F_RPOS_CARM",
        "DWH$TA_F_GPOS_FACT_CARM",
        "DWH$TA_F_RPOS_FACT_CARM",
        "DWH$TA_F_RPOS_RESELLING_CARM",
        "DWH$TA_T_RPOS_CARM",
        "DWH$TA_K_RECH_ABSGRP",
        "dwh$ta_k_meldungen",
        "DWH$TA_K_MELDUNGEN",
        "dwh$ta_c_vertrag",
        "DWH$TA_C_VERTRAG"
    ]
    
    for table_name in tables_to_map:
        clean_table_name = table_name.replace("$", "_")
        translated_sql = re.sub(
            rf"\b{re.escape(table_name)}\b",
            f"`{gcp_project}.{bq_dataset}.{clean_table_name}`",
            translated_sql,
            flags=re.IGNORECASE
        )

    query_params = []
    for key, val in params.items():
        if isinstance(val, int):
            p_type = "INT64"
        elif isinstance(val, float):
            p_type = "FLOAT64"
        else:
            p_type = "STRING"
        query_params.append(bigquery.ScalarQueryParameter(key, p_type, val))

    job_config = bigquery.QueryJobConfig(query_parameters=query_params)
    
    try: 
        query_job = client.query(translated_sql, job_config=job_config)
        query_job.result()
    except Exception as e:
        print(f"ERROR: BigQuery DML statement execution failed: {e}", file=sys.stderr)
        raise e

# Pre-load Idempotency deletes
def perform_idempotency_deletes():
    print("Executing Pre-load Idempotency Deletes against BigQuery...")
    params = {
        "rechnung_id": os.environ.get("RECHNUNG_ID", "0"),
        "rechnung_datum": os.environ.get("RECHNUNG_DATUM", "1970-01-01"),
        "standardvertrags_id": int(os.environ.get("STANDARDVERTRAGS_ID", "0")),
        "vertrags_id": int(os.environ.get("VERTRAGS_ID", "0")),
        "debitor_id": os.environ.get("DEBITOR_ID", "0")
    }
    
    delete_rpos_sql = """DELETE FROM DWH$TA_F_RPOS_CARM
WHERE  rechnung_id = :rechnung_id
AND    rechnung_datum = :rechnung_datum
AND    standardvertrags_id = :standardvertrags_id
AND    vertrags_id = :vertrags_id"""
    run_bq_query(delete_rpos_sql, params)
    
    delete_gpos_fact_sql = """DELETE FROM DWH$TA_F_GPOS_FACT_CARM
WHERE  rechnung_id = :rechnung_id
AND    rechnung_datum = :rechnung_datum
AND    standardvertrags_id = :standardvertrags_id
AND    vertrags_id = :vertrags_id"""
    run_bq_query(delete_gpos_fact_sql, params)
    
    delete_rpos_fact_sql = """DELETE FROM DWH$TA_F_RPOS_FACT_CARM
WHERE  rechnung_id = :rechnung_id
AND    rechnung_datum = :rechnung_datum
AND    standardvertrags_id = :standardvertrags_id
AND    vertrags_id = :vertrags_id"""
    run_bq_query(delete_rpos_fact_sql, params)
    
    delete_reselling_sql = """DELETE FROM DWH$TA_F_RPOS_RESELLING_CARM
WHERE  rechnung_id = :rechnung_id
AND    rechnung_datum = :rechnung_datum
AND    standardvertrags_id = :standardvertrags_id
AND    vertrags_id = :vertrags_id"""
    run_bq_query(delete_reselling_sql, params)
    
    delete_temp_rpos_sql = """DELETE FROM DWH$TA_T_RPOS_CARM
WHERE  debitor_id = :debitor_id
AND    rechnung_datum = :rechnung_datum
AND    rechnung_id = :rechnung_id"""
    run_bq_query(delete_temp_rpos_sql, params)

# Dataproc PySpark Job Submission
def submit_dataproc_serverless_pyspark_job():
    print("Submitting PySpark ETL job to Dataproc Serverless...")
    if not GCP_LIBS_AVAILABLE:
        print("WARNING: Dataproc client libraries unavailable. Skipping Serverless Batch Submission.", file=sys.stderr)
        return
        
    project_id = os.environ["GCP_PROJECT"]
    region = os.environ.get("DATAPROC_REGION", "europe-west3")
    bucket = os.environ["GCS_BUCKET"]
    
    client = dataproc_v1.BatchControllerClient(
        client_options={"api_endpoint": f"{region}-dataproc.googleapis.com:443"}
    )
    
    batch = dataproc_v1.Batch()
    batch.pyspark_batch.main_python_file_uri = f"gs://{bucket}/pyspark/map_rpos_carmen_import_pyspark.py"
    
    batch.pyspark_batch.args = [
        f"--bhb_projektverzeichnis={os.environ.get('BHB_Projektverzeichnis')}",
        f"--bhb_graph={os.environ.get('BHB_Graph')}",
        f"--bhb_prozesstyp={os.environ.get('BHB_Prozesstyp')}",
        f"--bhb_eintragsnr={os.environ.get('BHB_Eintragsnr')}",
        f"--bhb_quellverzeichnis={os.environ.get('BHB_Quellverzeichnis')}",
        f"--bhb_zielverzeichnis={os.environ.get('BHB_Zielverzeichnis')}",
        f"--bhb_dateimaske={os.environ.get('BHB_Dateimaske')}",
        f"--bhb_dateiname={os.environ.get('BHB_Dateiname')}",
        f"--gcp_project={project_id}",
        f"--bq_dataset={os.environ.get('BQ_DATASET')}",
        f"--gcs_bucket={bucket}"
    ]
    
    batch_id = f"map-rpos-carmen-{os.environ.get('BHB_Eintragsnr', '0')}"
    batch_id = re.sub(r'[^a-zA-Z0-9-]', '-', batch_id).lower()[:63]
    
    try:
        operation = client.create_batch(
            parent=f"projects/{project_id}/regions/{region}",
            batch=batch,
            batch_id=batch_id
        )
        result = operation.result()
        print(f"Dataproc Serverless job execution finished with state: {result.state}")
        if result.state != dataproc_v1.Batch.State.SUCCEEDED:
            raise RuntimeError(f"Dataproc Batch execution finished with state: {result.state}")
    except Exception as e:
        print(f"ERROR: Dataproc Serverless pipeline execution failed: {e}", file=sys.stderr)
        raise e

# Post-load audits
def perform_auditing_updates():
    print("Executing Auditing and Housekeeping Updates on BigQuery...")
    params_absgrp = {
        "monats_id": int(os.environ.get("MONATS_ID", "197001")),
        "abs_grp": os.environ.get("ABS_GRP", "UNKNOWN"),
        "dateiname": os.environ.get("BHB_Dateiname", "UNKNOWN"),
        "rechnung_datum": os.environ.get("RECHNUNG_DATUM", "1970-01-01"),
        "rechnungsteil": os.environ.get("RECHNUNGSTEIL", "P"),
        "ladedatum": os.environ.get("LADEDATUM", "1970-01-01")
    }
    
    update_absgrp_sql = """UPDATE DWH$TA_K_RECH_ABSGRP
SET   rechnung_datum = :rechnung_datum, 
      ladedatum = :ladedatum
WHERE  monats_id = :monats_id
AND    abs_grp = :abs_grp
AND    dateiname = :dateiname
AND    rechnungsteil = :rechnungsteil"""
    run_bq_query(update_absgrp_sql, params_absgrp)
    
    insert_absgrp_sql = """INSERT INTO DWH$TA_K_RECH_ABSGRP (monats_id, abs_grp, dateiname,  rechnung_datum, rechnungsteil, ladedatum)
VALUES (:monats_id, :abs_grp, :dateiname,  :rechnung_datum, :rechnungsteil, :ladedatum)"""
    run_bq_query(insert_absgrp_sql, params_absgrp)
    
    params_meldungen = {
        "anzahl": int(os.environ.get("ANZAHL_DS_EOF", "0")),
        "dateiname": os.environ.get("BHB_Dateiname", "UNKNOWN"),
        "inhalt": os.environ.get("ENDERECORD_TEXT", ""),
        "bemerkung": os.environ.get("ZUSATZINFO", ""),
        "eintragsnr": int(os.environ.get("BHB_Eintragsnr", "0"))
    }
    
    update_meldungen_sql = """update dwh$ta_k_meldungen 
set anzahl_ds_eof = :anzahl
  , dateiname = :dateiname
  , enderecord_text = :inhalt
  , zusatzinfo = :bemerkung 
where entrynr = :eintragsnr"""
    run_bq_query(update_meldungen_sql, params_meldungen)

def main():
    setup_environment()
    parse_arguments()
    check_parameters()
    
    base_temp_dir = tempfile.mkdtemp(prefix="map_rpos_carmen_import-ProxyDir-")
    exit_code = 0
    try:
        project_ksh = os.path.join(os.environ.get("PROJECT_DIR", "."), ".project.ksh")
        if os.path.exists(project_ksh):
            subprocess.run([project_ksh, os.environ.get("PROJECT_DIR", "."), "execute", "start"], check=True)
            
        perform_idempotency_deletes()
        submit_dataproc_serverless_pyspark_job()
        perform_auditing_updates()
        
        if os.path.exists(project_ksh):
            subprocess.run([project_ksh, os.environ.get("PROJECT_DIR", "."), "execute", "end"], check=True)
            
    except subprocess.CalledProcessError as err:
        print(f"Pipeline execution failed in subprocess execution stage: {err}", file=sys.stderr)
        traceback.print_exc()
        exit_code = err.returncode if err.returncode is not None else 1
    except Exception as general_err:
        print(f"Pipeline execution aborted due to critical runtime failure: {general_err}", file=sys.stderr)
        traceback.print_exc()
        exit_code = 1
    finally:
        if os.path.exists(base_temp_dir):
            shutil.rmtree(base_temp_dir)
            
    sys.exit(exit_code)

if __name__ == "__main__":
    main()