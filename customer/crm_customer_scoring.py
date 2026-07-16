"""
PySpark Application: crm_customer_scoring
Legacy Origin: crm_customer_scoring.mp (Ab Initio Graph)
"""

import argparse
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, coalesce, lit

# Retrieve global config
GCS_BUCKET = os.environ.get("GCS_BUCKET")

def execute_transformations(run_date: str, segment: str, parallelism: str):
    spark = SparkSession.builder \
        .appName("CRM_AbInitio_Transform") \
        .config("spark.sql.shuffle.partitions", parallelism) \
        .getOrCreate()
        
    print(f"Executing transformation pipeline for date: {run_date} with dynamic parallelism {parallelism}")

    gcs_input_pattern = f"gs://{GCS_BUCKET}/staging/PROD/customer_extracts/*/{run_date}/*.parquet"
    extracted_df = spark.read.parquet(gcs_input_pattern)

    regional_summary_df = spark.read.format("bigquery") \
        .option("table", "DW_OWNER.FACT_REGIONAL_SUMMARY") \
        .load()

    joined_df = extracted_df.join(
        regional_summary_df, 
        on="region", 
        how="left"
    )

    processed_df = joined_df.withColumn(
        "customer_score",
        when(col("sales_amount") >= 100000, col("sales_amount") * 1.15)
        .when((col("sales_amount") < 100000) & (col("sales_amount") >= 25000), col("sales_amount") * 1.05)
        .otherwise(col("sales_amount"))
    ).withColumn(
        "data_quality_flag",
        when(col("customer_name").isNull() | (col("customer_name") == ""), lit("INVALID"))
        .otherwise(lit("VALID"))
    )

    output_target = f"gs://{GCS_BUCKET}/analytical/PROD/customer_scores/{run_date}/"
    processed_df.write.mode("overwrite").parquet(output_target)
    
    print(f"Ab Initio Transform Completed. Target: {output_target}")
    spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ab Initio Rebuilt PySpark Job")
    parser.add_argument("--run-date", required=True, help="Execution execution date")
    parser.add_argument("--customer-segment", default="ALL", help="Target Segment")
    parser.add_argument("--parallelism", default="4", help="Shuffle partitioning factor")
    
    args = parser.parse_args()
    execute_transformations(args.run_date, args.customer_segment, args.parallelism)