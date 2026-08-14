#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Migrated from: vobs/dw_source/isdwh/abinitio/ccm_proc/run/BHB_CCM_PROC_WriteContractMapLookup.ksh
Target Platform: Cloud Composer + Dataproc Serverless (PySpark / Python)
"""

import os
import sys
import tempfile
import shutil
import atexit
import argparse
from google.cloud import bigquery, storage
from pyspark.sql import SparkSession

# Cross-File Dependencies: Import from already-migrated template modules
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../../../istools/seu/template")))
try:
    import dw_global
    import dw_init
except ImportError:
    try:
        import importlib.util
        spec_global = importlib.util.spec_from_file_location(
            "dw_global", 
            os.path.join(os.path.dirname(__file__), "../../../../../istools/seu/template/.dw_global")
        )
        dw_global = importlib.util.module_from_spec(spec_global)
        spec_global.loader.exec_module(dw_global)
        
        spec_init = importlib.util.spec_from_file_location(
            "dw_init", 
            os.path.join(os.path.dirname(__file__), "../../../../../istools/seu/template/.dw_init")
        )
        dw_init = importlib.util.module_from_spec(spec_init)
        spec_init.loader.exec_module(dw_init)
    except Exception:
        # Fallback if modules not found during local/unconfigured execution
        class Dummy:
            pass
        dw_global = Dummy()
        dw_init = Dummy()

def main():
    exit_status = 0
    proxy_dir = None

    # Step 1: Initialize temporary directory equivalent to Ab Initio's _AB_PROXY_DIR
    proxy_dir = tempfile.mkdtemp(prefix="BHB_CCM_PROC_WriteContractMapLookup-ProxyDir-")
    
    # Step 2: Define and register cleanup routine
    def cleanup():
        if proxy_dir and os.path.exists(proxy_dir):
            try:
                shutil.rmtree(proxy_dir)
            except Exception as e:
                print(f"Error cleaning up temporary directory {proxy_dir}: {e}", file=sys.stderr)
                
    atexit.register(cleanup)

    try:
        # Step 3: Handle arguments
        parser = argparse.ArgumentParser(description="BHB_CCM_PROC_WriteContractMapLookup migrated job")
        parser.add_argument("-help", action="store_true", help="Show help and exit with status 1")
        args, unknown_args = parser.parse_known_args()

        if args.help:
            # Preserve the help exit behaviour
            sys.exit(1)

        # Step 4: Sourcing environment values (classify by GLOBAL / JOB-SPECIFIC role)
        gcp_project = os.environ.get("GCP_PROJECT")
        gcs_bucket = os.environ.get("GCS_BUCKET")
        bq_dataset = os.environ.get("BQ_DATASET", "DWH")

        # Job-Specific variables
        target_object_name = os.environ.get("BHB_CCM_PROC_TargetObjectName", "ContractMapLookup.txt")
        first_day = os.environ.get("BHB_CCM_PROC_FirstDay", "20050217")
        last_day_plus_1 = os.environ.get("BHB_CCM_PROC_LastDayPlus1", "20050218")

        output_filename = os.environ.get("CCM_PROC_ContractMapLookupFilename")
        if not output_filename:
            if gcs_bucket:
                output_filename = f"gs://{gcs_bucket}/ccm_proc/ContractMapLookup.txt"
            else:
                raise ValueError("Neither CCM_PROC_ContractMapLookupFilename nor GCS_BUCKET environment variable is defined.")

        # Step 5: Initialize SparkSession
        spark = SparkSession.builder \
            .appName("BHB_CCM_PROC_WriteContractMapLookup") \
            .getOrCreate()

        # Step 6: Load the source table DWH_TA_L_MAP_VT_CARM_DWH from BigQuery using Spark
        if gcp_project:
            table_ref = f"{gcp_project}.{bq_dataset}.DWH_TA_L_MAP_VT_CARM_DWH"
        else:
            table_ref = f"{bq_dataset}.DWH_TA_L_MAP_VT_CARM_DWH"

        print(f"INFO: Loading source table {table_ref} from BigQuery using PySpark")
        try:
            df = spark.read.format("bigquery").option("table", table_ref).load()
        except Exception as e:
            print(f"ERROR: Database extraction from {table_ref} failed: {e}", file=sys.stderr)
            sys.exit(1)

        # Step 7: Sort data by vertrags_id ascending using PySpark DataFrame operations
        print("INFO: Sorting data by vertrags_id ascending")
        sorted_df = df.select("vertrags_id", "dwh_vertrag_id").sort("vertrags_id")

        # Step 8: Write sorted records directly to GCS using PySpark's CSV writer with \x01 delimiter
        print(f"INFO: Writing sorted records directly to {output_filename}")
        try:
            sorted_df.coalesce(1).write \
                .mode("overwrite") \
                .option("delimiter", "\x01") \
                .option("header", "false") \
                .csv(output_filename)
            print(f"Successfully wrote sorted results to {output_filename}")
        except Exception as e:
            print(f"ERROR: Writing to {output_filename} failed: {e}", file=sys.stderr)
            sys.exit(1)

        # Step 9: Call BigQuery Stored Procedure replacing Oracle function
        print(f"Executing metadata registration for object {target_object_name} from {first_day} to {last_day_plus_1}...")
        try:
            client = bigquery.Client()
            if gcp_project:
                sp_ref = f"`{gcp_project}.{bq_dataset}.SetzeLadedatumAbInitio`"
            else:
                sp_ref = f"`{bq_dataset}.SetzeLadedatumAbInitio`"

            sp_query = f"CALL {sp_ref}('{target_object_name}', '{first_day}', '{last_day_plus_1}')"
            query_job = client.query(sp_query)
            query_job.result()
            print("BigQuery metadata update stored procedure executed successfully.")
        except Exception as e:
            print(f"WARNING: BigQuery metadata update stored procedure call failed: {e}", file=sys.stderr)

    except Exception as err:
        print(f"ERROR: Execution failed: {err}", file=sys.stderr)
        exit_status = 1

    sys.exit(exit_status)

if __name__ == "__main__":
    main()