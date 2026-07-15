"""
PySpark Task: tmp5bupf309_write_agent_lookup
Migrated from: BHB_CCM_PROC_WriteAgentADSLookup.cfg (Ab Initio Graph tmp5bupf309)
Purpose: Extracts data from the BigQuery view DWH$VI_S_SDM_AGENT_ADS based on lookback days,
         formats the output, and writes the AgentADSLookup.txt file to GCS.
"""

import argparse
import sys
import os
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def parse_arguments():
    """Parses incoming command-line execution parameters."""
    parser = argparse.ArgumentParser(
        description="Extract and construct the Agent Lookup file."
    )
    parser.add_argument(
        "--lookback-days",
        type=int,
        default=84,
        help="Number of relative lookback days for filtering historical records.",
    )
    parser.add_argument(
        "--output-path",
        type=str,
        required=True,
        help="Target GCS URI where the AgentADSLookup.txt file should be written.",
    )
    parser.add_argument(
        "--job-kennung",
        type=str,
        default="ABPZ_KKM_AIL_AGENT",
        help="The legacy job identifier for execution tracing.",
    )
    return parser.parse_args()

def initialize_spark_session(app_name: str) -> SparkSession:
    """Initializes and returns a configured Spark Session."""
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )

def calculate_date_boundary(lookback_days: int) -> str:
    """Computes the ISO date boundary back from today's system run date."""
    boundary_date = datetime.utcnow() - timedelta(days=lookback_days)
    return boundary_date.strftime("%Y-%m-%d")

def extract_agent_data(
    spark: SparkSession, project_id: str, dataset: str, date_filter: str
):
    """
    Reads from the Migrated BigQuery View/Table representing DWH$VI_S_SDM_AGENT_ADS.
    Applies lookup temporal constraints.
    """
    source_table_ref = f"{project_id}.{dataset}.DWH$VI_S_SDM_AGENT_ADS"
    print(f"[INFO] Reading from {source_table_ref} for records modified since {date_filter}.")

    df_raw = (
        spark.read.format("bigquery")
        .option("table", source_table_ref)
        .load()
    )

    if "modification_date" in df_raw.columns:
        df_filtered = df_raw.filter(F.col("modification_date") >= date_filter)
    elif "run_date" in df_raw.columns:
        df_filtered = df_raw.filter(F.col("run_date") >= date_filter)
    else: 
        df_filtered = df_raw

    return df_filtered

def transform_data(df):
    """
    Transforms and normalizes structure mapping rules.
    """
    df_transformed = df.select(
        F.coalesce(F.col("agent_id"), F.lit("")).alias("AgentID"),
        F.coalesce(F.col("agent_name"), F.lit("")).alias("AgentName"),
        F.coalesce(F.col("agent_status"), F.lit("")).alias("AgentStatus"),
        F.coalesce(F.col("region_code"), F.lit("")).alias("RegionCode"),
        F.current_timestamp().alias("ProcessedTimestamp"),
    )
    return df_transformed

def write_to_gcs_flat_file(df, output_path: str):
    """Writes dataframe out as a clean, single tab-separated flat-file."""
    print(f"[INFO] Writing lookup output file to GCS: {output_path}")
    (
        df.coalesce(1)
        .write.mode("overwrite")
        .option("header", "true")
        .option("delimiter", "\t")
        .format("csv")
        .save(output_path)
    )

def main():
    args = parse_arguments()
    spark = initialize_spark_session(f"{args.job_kennung}_migration")

    project_id = os.environ.get("GCP_PROJECT")
    dataset = os.environ.get("BQ_DATASET")

    date_limit = calculate_date_boundary(args.lookback_days)

    try:
        df_raw = extract_agent_data(spark, project_id, dataset, date_limit)
        df_transformed = transform_data(df_raw)
        write_to_gcs_flat_file(df_transformed, args.output_path)
        print("[SUCCESS] Core PySpark step completed successfully.")
    except Exception as error:
        print(f"[ERROR] Pipeline execution failed: {str(error)}")
        sys.exit(1)

if __name__ == "__main__":
    main()