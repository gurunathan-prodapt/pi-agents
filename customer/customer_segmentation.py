"""
PySpark Application: customer_segmentation
Legacy Origin: customer_segmentation.scala (Scala Assembly Class)
"""

import argparse
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit

# Retrieve global config
GCS_BUCKET = os.environ.get("GCS_BUCKET")

def perform_segmentation(run_date: str, env: str):
    spark = SparkSession.builder \
        .appName("CRM_Spark_Segmentation") \
        .getOrCreate()
        
    print(f"Beginning Segment Matrix Assignment: Run Date={run_date} on Environment={env}")

    scores_path = f"gs://{GCS_BUCKET}/analytical/{env}/customer_scores/{run_date}/*.parquet"
    scores_df = spark.read.parquet(scores_path)

    finance_df = spark.read.format("bigquery") \
        .option("table", "FINANCE_SCHEMA.FACT_PERIOD_RECONCILIATION") \
        .load()

    reconciled_df = scores_df.join(
        finance_df,
        scores_df.customer_id == finance_df.customer_id,
        "inner"
    )

    final_segmented_df = reconciled_df.filter(col("data_quality_flag") == "VALID") \
        .withColumn(
            "cohort_group",
            col("customer_score") / lit(1000)
        )

    target_table = f"DW_OWNER.FACT_CUSTOMER_SEGMENTATION_{run_date.replace('-', '_')}"
    final_segmented_df.write \
        .format("bigquery") \
        .option("table", target_table) \
        .option("temporaryGcsBucket", GCS_BUCKET) \
        .mode("overwrite") \
        .save()

    print(f"Segmentation finalized and written to: {target_table}")
    spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Customer Segmentation PySpark Job")
    parser.add_argument("--run-date", required=True)
    parser.add_argument("--env", default="PROD")
    
    args = parser.parse_args()
    perform_segmentation(args.run_date, args.env)