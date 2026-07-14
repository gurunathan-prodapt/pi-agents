#!/usr/bin/env python
"""
Computational Script: exis_ikdb_stamm_r.py
Description: PySpark program run on Dataproc. Executes the master data extraction 
             queries from IKDB, exports raw outputs to GCS, and updates metadata tracking logs.
"""

import argparse
import sys
from datetime import datetime
from pyspark.sql import SparkSession


def parse_arguments():
    """Parses incoming runtime job arguments."""
    parser = argparse.ArgumentParser(description="Spark Master Data Export Executor")
    parser.add_argument("--query", required=True, help="SQL extraction query filename")
    parser.add_argument("--job_key", required=True, help="Standardized job identifier key")
    parser.add_argument("--file_type", required=True, help="Target file layout identifier signature")
    parser.add_argument("--numeric_param", required=True, type=str, help="Retroactive business day offset")
    parser.add_argument("--target_date", required=True, help="Target reporting date (Format: YYYYMMDD)")
    return parser.parse_args()


def get_bigquery_query(query_name: str, target_date: str, numeric_offset: str) -> str:
    """
    Translates legacy oracle queries into standard BigQuery-compliant SQL syntax.
    Maintains compatibility with business filter variables.
    """
    # Log incoming configurations
    print(f"Resolving query script template: {query_name} for target date: {target_date}")
    
    # Simple dictionary structure mapping files to analytical SQL definitions
    if query_name == "d_ikdb_exp_stamm.sql":
        return f"""
            SELECT 
                contract_id,
                partner_id,
                system_date,
                contract_status,
                '{target_date}' as reporting_date,
                CURRENT_TIMESTAMP() as export_timestamp
            FROM 
                `{target_date[:4]}_IKDB_SOURCE.contract_master_table`
            WHERE 
                system_date >= DATE_SUB(PARSE_DATE('%Y%m%d', '{target_date}'), INTERVAL {numeric_offset} DAY)
        """
    else:
        # Default fallback query structure
        return f"SELECT * FROM `{target_date[:4]}_IKDB_SOURCE.default_table` WHERE STICHTAG = '{target_date}'"


def write_metadata_status(spark: SparkSession, job_key: str, target_date: str, status_code: str):
    """
    Updates operational metadata table `DWTK_MELDUNGEN` in BigQuery to keep tracking 
    log synchronization intact (Status '2' indicates success).
    """
    print(f"Writing metadata tracking record. Job: {job_key}, Date: {target_date}, Status: {status_code}")
    
    # Configure record DataFrame
    schema_record = [{
        "JOB_KENNUNG": job_key,
        "STATUS_NR": status_code,
        "STICHTAG": target_date,
        "TSTAMP": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }]
    
    df = spark.createDataFrame(schema_record)
    
    # Write metadata status row to BigQuery audit log
    df.write \
      .format("bigquery") \
      .option("table", "metadata_dataset.DWTK_MELDUNGEN") \
      .mode("append") \
      .save()


def main():
    args = parse_arguments()
    
    # Initialize PySpark Session
    spark = SparkSession.builder \
        .appName(f"Exporter-{args.job_key}") \
        .getOrCreate()
    
    # Retrieve configuration variables from Spark Context properties
    gcs_bucket = spark.conf.get("spark.hadoop.fs.gs.system.bucket", "your-target-gcs-bucket")
    
    print("--- [STARTING PYSPARK PROCESSING STEP] ---")
    print(f"Zuweisung erfolgt: Job Key = {args.job_key}")
    print(f"Nachlieferungsmodus Check: Offset = {args.numeric_param}")
    
    try:
        # 1. Resolve target data extraction query
        sql_query = get_bigquery_query(args.query, args.target_date, args.numeric_param)
        
        # 2. Extract dataset utilizing the Spark BigQuery connector
        df_export = spark.read.format("bigquery").option("query", sql_query).load()
        
        # 3. Output extracted dataset to GCP Cloud Storage
        output_directory_path = f"gs://{gcs_bucket}/exports/{args.file_type}/{args.target_date}/"
        print(f"Writing dataset file targets: {output_directory_path}")
        
        df_export.write \
            .mode("overwrite") \
            .option("header", "true") \
            .csv(output_directory_path)
            
        print("Dataset successfully saved to Cloud Storage.")
        
        # 4. Write run status '2' (Successful execution) to operational metadata audit table
        write_metadata_status(spark, args.job_key, args.target_date, status_code="2")
        
        print("Die Abarbeitung des Rahmenskriptes ist ohne erkennbare Fehler beendet.")
        
    except Exception as e:
        print(f"CRITICAL EXCEPTION encountered inside Spark processing task: {str(e)}", file=sys.stderr)
        # Attempt to register failure status '3' in the metadata audit log
        try:
            write_metadata_status(spark, args.job_key, args.target_date, status_code="3")
        except Exception as write_err:
            print(f"Failed to write metadata error status: {str(write_err)}", file=sys.stderr)
        sys.exit(1)
        
    finally:
        spark.stop()


if __name__ == "__main__":
    main()