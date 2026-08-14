#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
PySpark migration of Ab Initio graph BHB_CCM_PROC_WriteContractMapLookup.mp
Extracts contract map attributes from DWH$TA_L_MAP_VT_CARM_DWH, sorts them by vertrags_id,
writes the result as a text file with '\001' delimiter to GCS, and calls the
BigQuery stored procedure SetzeLadedatumAbInitio to update the loading timestamps.
"""

import os
import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from google.cloud import storage
from google.cloud import bigquery

def main():
    # 1. Parse Command Line Arguments
    parser = argparse.ArgumentParser(description="Run BHB_CCM_PROC_WriteContractMapLookup PySpark Job")
    parser.add_argument("--first_day", required=True, help="First day parameter (YYYYMMDD)")
    parser.add_argument("--last_day_plus_1", required=True, help="Last day plus 1 parameter (YYYYMMDD)")
    args, unknown = parser.parse_known_args()

    first_day = args.first_day
    last_day_plus_1 = args.last_day_plus_1

    # 2. Retrieve Global Environment Variables
    gcp_project = os.environ.get("GCP_PROJECT")
    gcs_bucket = os.environ.get("GCS_BUCKET")
    bq_dataset = os.environ.get("BQ_DATASET")

    if not gcp_project:
        raise ValueError("Environment variable GCP_PROJECT is required but not set.")
    if not gcs_bucket:
        raise ValueError("Environment variable GCS_BUCKET is required but not set.")
    if not bq_dataset:
        raise ValueError("Environment variable BQ_DATASET is required but not set.")

    target_object_name = "ContractMapLookup.txt"
    temp_gcs_prefix = "ccm_proc/temp_ContractMapLookup"
    final_gcs_blob_name = f"ccm_proc/{target_object_name}"

    print(f"Starting job with parameters:")
    print(f"  GCP_PROJECT        : {gcp_project}")
    print(f"  GCS_BUCKET         : {gcs_bucket}")
    print(f"  BQ_DATASET         : {bq_dataset}")
    print(f"  FIRST_DAY          : {first_day}")
    print(f"  LAST_DAY_PLUS_1    : {last_day_plus_1}")
    print(f"  TARGET_OBJECT_NAME : {target_object_name}")

    # 3. Initialize Spark Session
    spark = SparkSession.builder \
        .appName("BHB_CCM_PROC_WriteContractMapLookup") \
        .getOrCreate()

    try:
        # Step 1: Read Contract Map source data from BigQuery table dwh_ta_l_map_vt_carm_dwh
        source_table = f"{gcp_project}.{bq_dataset}.dwh_ta_l_map_vt_carm_dwh"
        print(f"Reading source data from BigQuery table: {source_table}")
        
        df_source = spark.read.format("bigquery") \
            .option("table", source_table) \
            .load()

        # Step 2: Extract attributes, sort by vertrags_id, and drop duplicates if necessary
        # The design document indicates: "Sorts the records by the contract ID in ascending order to prepare them for lookup serialization."
        # And the pseudocode mentions: "including dropDuplicates on key_id as per SORTS rule" (vertrags_id)
        df_processed = df_source \
            .select("vertrags_id", "dwh_vertrag_id") \
            .orderBy(F.col("vertrags_id").asc()) \
            .dropDuplicates(["vertrags_id"])

        # Step 3: Write processed dataframe to GCS as a single text file with '\001' delimiter
        # To ensure we get exactly one output file called ContractMapLookup.txt, we write to a temp folder,
        # locate the generated CSV/part file, and copy it to the final blob destination.
        print(f"Writing sorted contract map data to GCS temp location: gs://{gcs_bucket}/{temp_gcs_prefix}")
        
        # Write to GCS temp folder with \001 delimiter
        df_processed.coalesce(1).write \
            .mode("overwrite") \
            .option("delimiter", "\u0001") \
            .option("header", "false") \
            .csv(f"gs://{gcs_bucket}/{temp_gcs_prefix}")

        # Rename the single part file to final destination
        print("Consolidating part files to final destination...")
        storage_client = storage.Client(project=gcp_project)
        bucket = storage_client.bucket(gcs_bucket)
        blobs = list(bucket.list_blobs(prefix=temp_gcs_prefix))

        part_blob = None
        for blob in blobs:
            if blob.name.endswith(".csv") or "part-" in blob.name:
                part_blob = blob
                break

        if part_blob:
            # Copy to final destination
            bucket.copy_blob(part_blob, bucket, final_gcs_blob_name)
            print(f"File successfully written to: gs://{gcs_bucket}/{final_gcs_blob_name}")

            # Clean up the temporary folder
            print("Cleaning up temporary GCS files...")
            for blob in blobs:
                blob.delete()
        else:
            raise RuntimeError("Failed to find generated CSV partition file in temporary GCS folder.")

        # Step 4: Stored Procedure execution
        # Replaces the legacy "Update Loading Timestamps" subgraph containing "Join with DB"
        # that executes SetzeLadedatumAbInitio.
        print("Executing post-processing BigQuery stored procedure...")
        bq_client = bigquery.Client(project=gcp_project)
        sp_call_query = f"CALL `{gcp_project}.{bq_dataset}.SetzeLadedatumAbInitio`('{target_object_name}', '{first_day}', '{last_day_plus_1}')"
        print(f"Executing Query: {sp_call_query}")
        
        query_job = bq_client.query(sp_call_query)
        # Wait for stored procedure execution to complete
        query_job.result()
        print("Stored procedure SetzeLadedatumAbInitio executed successfully.")

    except Exception as e:
        print(f"Job execution failed with error: {str(e)}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()