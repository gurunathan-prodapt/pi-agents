"""
PySpark Application: process_customer_data
Legacy Origin: process_customer_data.ksh
"""

import argparse
import os
from pyspark.sql import SparkSession

# Retrieve global config
GCS_BUCKET = os.environ.get("GCS_BUCKET")

def run_extraction(run_date: str, segment: str, env: str, batch_size: int):
    spark = SparkSession.builder \
        .appName(f"CRM_Extract_{segment}") \
        .getOrCreate()
        
    print(f"Beginning extract run for Date: {run_date}, Segment: {segment}, Env: {env}, Batch Size: {batch_size}")
    
    query = f"""
        SELECT 
            customer_id, 
            customer_name, 
            sales_amount, 
            region,
            '{run_date}' as partition_date
        FROM `DW_OWNER.STG_CUSTOMER_SALES`
        WHERE UPPER(segment) = '{segment.upper()}'
        LIMIT {batch_size}
    """
    
    df = spark.read.format("bigquery").option("query", query).load()
    
    gcs_output_path = f"gs://{GCS_BUCKET}/staging/{env}/customer_extracts/{segment}/{run_date}/"
    df.write.mode("overwrite").parquet(gcs_output_path)
    
    print(f"Extraction job for segment {segment} completed and saved to {gcs_output_path}")
    spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process Customer Data Spark Job")
    parser.add_argument("--run-date", required=True, help="Execution date (YYYY-MM-DD)")
    parser.add_argument("--segment", required=True, help="VIP, RETAIL, or WHOLESALE")
    parser.add_argument("--env", default="PROD", help="Working environment target")
    parser.add_argument("--batch-size", type=int, default=5000, help="Query limit limit")
    
    args = parser.parse_args()
    run_extraction(args.run_date, args.segment, args.env, args.batch_size)