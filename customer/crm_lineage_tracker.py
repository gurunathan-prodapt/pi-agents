"""
PySpark Application: crm_lineage_tracker
Legacy Origin: crm_lineage_tracker.py (Python lineage port)
"""

import argparse
import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

# Retrieve global config
GCS_BUCKET = os.environ.get("GCS_BUCKET")

def record_lineage(run_date: str, env: str):
    spark = SparkSession.builder \
        .appName("CRM_Lineage_Tracker") \
        .getOrCreate()
        
    print(f"Tracking operational lineages for execution: {run_date}")

    schema = StructType([
        StructField("pipeline_id", StringType(), False),
        StructField("run_date", StringType(), False),
        StructField("execution_timestamp", TimestampType(), False),
        StructField("target_environment", StringType(), False),
        StructField("lineage_status", StringType(), False)
    ])

    telemetry_data = [(
        "CRM_WEEKLY_WORKFLOW",
        run_date,
        datetime.datetime.now(),
        env,
        "SUCCESS_COMPLETED"
    )]

    lineage_df = spark.createDataFrame(telemetry_data, schema=schema)

    lineage_df.write \
        .format("bigquery") \
        .option("table", "DW_OWNER.CRM_PIPELINE_LINEAGE_LOGS") \
        .option("temporaryGcsBucket", GCS_BUCKET) \
        .mode("append") \
        .save()

    print("Pipeline execution and lineage metadata successfully captured in BigQuery.")
    spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Operational Lineage Tracker Script")
    parser.add_argument("--run-date", required=True)
    parser.add_argument("--env", default="PROD")
    
    args = parser.parse_args()
    record_lineage(args.run_date, args.env)