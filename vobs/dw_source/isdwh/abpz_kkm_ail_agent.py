#!/usr/bin/env python
"""
PySpark Script: abpz_kkm_ail_agent.py
Description: Reads source agent reference datasets, runs lookups, and outputs 
             the lookup file AgentADSLookup.txt to Cloud Storage for downstream
             DWH$VI_S_SDM_AGENT_ADS usage.
"""

import argparse
import json
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def parse_arguments():
    parser = argparse.ArgumentParser(description="Process Agent ADS Lookups in PySpark.")
    parser.add_argument("--config", required=True, help="Relative GCS Path to the JSON configuration mapping file.")
    parser.add_argument("--output", required=True, help="Target GCS URI path to write AgentADSLookup.txt.")
    parser.add_argument("--rueckblick", required=True, type=int, help="Historical rollback window in days.")
    parser.add_argument("--gcs_bucket", required=True, help="Cloud Storage Bucket name.")
    parser.add_argument("--project_id", required=True, help="GCP Project ID housing BigQuery datasets.")
    parser.add_argument("--bhb_projektverzeichnis", required=False, help="Legacy metadata property.")
    parser.add_argument("--bhb_graph", required=False, help="Legacy metadata graph name.")
    parser.add_argument("--bhb_version", required=False, help="Legacy Graph Version code.")
    parser.add_argument("--bhb_prozesstyp", required=False, help="Legacy execution process type.")
    return parser.parse_args()

def load_config(spark: SparkSession, bucket: str, config_path: str) -> dict:
    gcs_uri = f"gs://{bucket}/{config_path}"
    try:
        sc = spark.sparkContext
        config_rdd = sc.textFile(gcs_uri)
        config_str = "".join(config_rdd.collect())
        return json.loads(config_str)
    except Exception as e:
        print(f"Warning: Could not read config file from {gcs_uri} due to error: {e}. Utilizing defaults.")
        return {
            "source_dataset": "dwh_kern_bi",
            "source_table": "agent_raw_data",
            "lookup_rules": {
                "active_status_codes": [1, 2, 3],
                "restricted_types": ["TEST", "DUMMY"]
            }
        }

def run_transformation(spark: SparkSession, args: argparse.Namespace, config: dict) -> None:
    project = args.project_id
    src_dataset = config.get("source_dataset", "dwh_kern_bi")
    src_table = config.get("source_table", "agent_raw_data")
    
    cutoff_date = (F.current_date() - F.expr(f"INTERVAL {args.rueckblick} DAYS"))

    print(f"Reading source database table: {project}.{src_dataset}.{src_table}")
    agent_df = spark.read \
        .format("bigquery") \
        .option("table", f"{project}:{src_dataset}.{src_table}") \
        .load()

    filtered_df = agent_df.filter(
        (F.col("last_modified_date") >= cutoff_date) &
        (F.col("agent_status").isin(config["lookup_rules"]["active_status_codes"])) &
        (~F.col("agent_type").isin(config["lookup_rules"]["restricted_types"]))
    )

    output_df = filtered_df.select(
        F.col("agent_id").alias("AGENT_ID"),
        F.col("agent_name").alias("AGENT_NAME"),
        F.col("agent_status").alias("STATUS_CODE"),
        F.col("region_id").alias("REGION_ID"),
        F.col("last_modified_date").alias("LOAD_DATETIME")
    )

    record_count = output_df.count()
    print(f"Processing complete: Compiled {record_count} lookup records using historical lookback window limit of {args.rueckblick} days.")

    output_df.coalesce(1).write \
        .mode("overwrite") \
        .option("header", "true") \
        .option("delimiter", "|") \
        .format("csv") \
        .save(args.output)

def main():
    args = parse_arguments()

    spark = SparkSession.builder \
        .appName("dw_dwh_abpz_kkm_ail_agent_pyspark") \
        .getOrCreate()

    config = load_config(spark, args.gcs_bucket, args.config)
    run_transformation(spark, args, config)
    spark.stop()

if __name__ == "__main__":
    main()