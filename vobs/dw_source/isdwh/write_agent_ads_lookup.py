#!/usr/bin/env python
"""
Migration of Ab Initio Graph: BHB_CCM_PROC_WriteAgentADSLookup
Target Platform: Dataproc Serverless / PySpark
Source: BigQuery View (dwh_views.vi_s_sdm_agent_ads)
Targets: 
  1. GCS Lookup File (gs://{GCS_BUCKET}/lookups/AgentADSLookup.txt)
  2. BigQuery Table (dw_lookups.agent_ads_lookup)
"""

import logging
import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException

# Configure logging to align with DWH operational standards
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(filename)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("write_agent_ads_lookup")


def get_spark_session(app_name: str = "BHB_CCM_PROC_WriteAgentADSLookup") -> SparkSession:
    """Initializes and returns a Spark Session configured for BigQuery connectivity."""
    logger.info("Initializing Spark Session...")
    return SparkSession.builder \
        .appName(app_name) \
        .getOrCreate()


def read_source_view(spark: SparkSession, project_id: str, dataset_name: str, view_name: str) -> "DataFrame":
    """Reads the source data from BigQuery view."""
    source_table_path = f"{project_id}.{dataset_name}.{view_name}"
    logger.info(f"Reading source data from BigQuery: {source_table_path}")
    try:
        df = spark.read.format("bigquery").option("table", source_table_path).load()
        return df
    except AnalysisException as e:
        logger.error(f"Failed to read from BigQuery view {source_table_path}: {str(e)}")
        raise


def apply_date_partition_filter(df: "DataFrame", first_day: str, last_day_plus_1: str) -> "DataFrame":
    """
    Applies the historical time window filter based on the 84-day lookback logic.
    Filters records where 'stichtag' falls within [FirstDay, LastDayPlus1).
    """
    logger.info(f"Filtering records with stichtag between {first_day} (inclusive) and {last_day_plus_1} (exclusive)")
    try:
        # Convert YYYYMMDD string parameters to Dates for precise filtering
        df_filtered = df.filter(
            (F.col("stichtag") >= F.to_date(F.lit(first_day), "yyyyMMdd")) &
            (F.col("stichtag") < F.to_date(F.lit(last_day_plus_1), "yyyyMMdd"))
        )
        return df_filtered
    except Exception as e:
        logger.error(f"Failed to apply date filter: {str(e)}")
        raise


def write_to_gcs_flat_file(df: "DataFrame", target_gcs_path: str):
    """Writes the processed DataFrame as a pipe-delimited flat-file in GCS."""
    logger.info(f"Writing flat-file target to GCS path: {target_gcs_path}")
    try:
        # Repartitioning to 1 to produce a single lookup file as expected downstream
        df.coalesce(1).write \
            .mode("overwrite") \
            .option("header", "true") \
            .option("delimiter", "|") \
            .option("encoding", "UTF-8") \
            .csv(target_gcs_path)
        logger.info("Successfully exported lookup file to GCS.")
    except Exception as e:
        logger.error(f"Failed writing lookup flat file to GCS: {str(e)}")
        raise


def write_to_bigquery_table(df: "DataFrame", target_bq_table: str):
    """Mirrors the output dataset in a BigQuery table for relational lookup access."""
    logger.info(f"Writing target dataset to BigQuery table: {target_bq_table}")
    try:
        df.write \
            .format("bigquery") \
            .option("table", target_bq_table) \
            .mode("overwrite") \
            .save()
        logger.info("Successfully wrote dataset to BigQuery.")
    except Exception as e:
        logger.error(f"Failed writing lookup data to BigQuery: {str(e)}")
        raise


def main():
    # 1. Resolve environment configurations
    gcp_project = os.environ.get("GCP_PROJECT")
    gcs_bucket = os.environ.get("GCS_BUCKET")
    first_day = os.environ.get("BHB_CCM_PROC_FirstDay")
    last_day_plus_1 = os.environ.get("BHB_CCM_PROC_LastDayPlus1")

    # Validation of parameters
    if not all([gcp_project, gcs_bucket, first_day, last_day_plus_1]):
        logger.error(
            f"Missing required environment configurations. "
            f"GCP_PROJECT: {gcp_project}, GCS_BUCKET: {gcs_bucket}, "
            f"FirstDay: {first_day}, LastDayPlus1: {last_day_plus_1}"
        )
        sys.exit(1)

    source_dataset = "dwh_views"
    source_view_name = "vi_s_sdm_agent_ads"
    target_gcs_path = f"gs://{gcs_bucket}/lookups/AgentADSLookup.txt"
    target_bq_table = f"{gcp_project}.dw_lookups.agent_ads_lookup"

    # Initialize Spark
    spark = get_spark_session()

    try:
        # Execution flow
        df_raw = read_source_view(spark, gcp_project, source_dataset, source_view_name)
        df_filtered = apply_date_partition_filter(df_raw, first_day, last_day_plus_1)
        
        # Parallel Target Generation
        write_to_gcs_flat_file(df_filtered, target_gcs_path)
        write_to_bigquery_table(df_filtered, target_bq_table)
        
        logger.info("Process completed successfully.")
        
    except Exception as err:
        logger.critical(f"Pipeline execution failed: {str(err)}")
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()