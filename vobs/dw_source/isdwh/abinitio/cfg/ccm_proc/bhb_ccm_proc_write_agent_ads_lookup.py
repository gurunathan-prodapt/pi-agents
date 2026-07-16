#!/usr/bin/env python3
import argparse
import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

def execute_agent_lookup_pipeline(project_id: str, dataset_id: str, output_uri: str) -> None:
    spark = SparkSession.builder \
        .appName("BHB_CCM_PROC_WriteAgentADSLookup") \
        .getOrCreate()

    source_view = f"{project_id}.{dataset_id}.DWH$VI_S_SDM_AGENT_ADS"
    
    try:
        raw_df = spark.read.format("bigquery").load(source_view)
        
        window_spec = Window.partitionBy("AgentId").orderBy(F.col("LastModifiedTimestamp").desc())
        
        processed_df = raw_df.withColumn("row_num", F.row_number().over(window_spec)) \
            .filter(F.col("row_num") == 1) \
            .select(
                F.col("AgentId").alias("agent_id"),
                F.col("SAMAccountName").alias("sam_account"),
                F.col("DisplayName").alias("display_name"),
                F.col("Department").alias("department"),
                F.col("ManagerId").alias("manager_id"),
                F.col("IsActive").alias("is_active")
            )
            
        output_df = processed_df.select(
            F.concat_ws(
                "|", 
                F.coalesce(F.col("agent_id"), F.lit("")),
                F.coalesce(F.col("sam_account"), F.lit("")),
                F.coalesce(F.col("display_name"), F.lit("")),
                F.coalesce(F.col("department"), F.lit("")),
                F.coalesce(F.col("manager_id"), F.lit("")),
                F.coalesce(F.col("is_active").cast("string"), F.lit(""))
            ).alias("formatted_row")
        )
        
        output_df.coalesce(1).write.mode("overwrite").text(output_uri)
        
    except Exception as err:
        sys.stderr.write(f"!FEHLER gemeldet! Pipeline execution failed: {str(err)}\n")
        sys.exit(1)
    finally:
        spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process Agent Active Directory Lookup structures.")
    parser.add_argument("--gcp_project", required=True, help="Target GCP Project")
    parser.add_argument("--bq_dataset", required=True, help="Target BigQuery Dataset")
    parser.add_argument("--output_uri", required=True, help="Destination lookup path GCS URI")
    
    args = parser.parse_args()
    execute_agent_lookup_pipeline(args.gcp_project, args.bq_dataset, args.output_uri)