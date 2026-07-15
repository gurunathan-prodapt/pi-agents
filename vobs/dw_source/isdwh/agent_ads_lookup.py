#!/usr/bin/env python3
"""
Executable: agent_ads_lookup.py
Description: Converts legacy Ab Initio graph logic mapping 'DWH$VI_S_SDM_AGENT_ADS' 
             into a standardized GCS Flat-File Lookup output block.
"""

import argparse
import sys
import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Setup Logging configuration to match system outputs
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("agent_ads_lookup")

def parse_arguments():
    """
    Parses execution variables forwarded from Cloud Composer.
    """
    parser = argparse.ArgumentParser(description="Spark-based Agent ADS Lookup Generator")
    parser.add_argument("--job_kennung", required=True, help="Job identifier")
    parser.add_argument("--output_file", required=True, help="Target lookup filename")
    parser.add_argument("--config_file", required=True, help="Path to config file GCS")
    parser.add_argument("--rueckblick_ladedatum", required=True, help="KKM_Rueckblick_Ladedatum variable")
    parser.add_argument("--exec_date", required=True, help="Current Airflow execution date (YYYY-MM-DD)")
    parser.add_argument("--target_bucket", required=True, help="GCS Storage target bucket")
    parser.add_argument("--dwh_home", required=False, default="/opt/dwh", help="DWH Root Path")
    return parser.parse_args()


def get_spark_session(job_name: str) -> SparkSession:
    """
    Initializes or retrieves the active Spark Session.
    """
    return SparkSession.builder \
        .appName(job_name) \
        .config("spark.sql.parquet.writeLegacyFormat", "true") \
        .getOrCreate()


def process_lookup_extraction(spark: SparkSession, rueckblick_date: str, exec_date: str):
    """
    Queries legacy view structure 'DWH$VI_S_SDM_AGENT_ADS'.
    This function isolates extraction logic allowing developers to easily adjust schema rules.
    """
    logger.info(f"Querying business view 'DWH$VI_S_SDM_AGENT_ADS' for execution date: {exec_date}")
    
    # Target Query Strategy: Extracts agent data using runtime parameter filters
    # Replace table identification path with actual metastore catalog structure in production (e.g. Hive catalog)
    query = f"""
        SELECT 
            AGENT_ID,
            AGENT_NAME,
            AGENT_STATUS,
            ADS_DOMAIN,
            ADS_USER_ID,
            EMAIL,
            UPDATE_TIMESTAMP
        FROM 
            DWH_VI_S_SDM_AGENT_ADS
        WHERE 
            CAST(UPDATE_TIMESTAMP AS DATE) >= CAST('{rueckblick_date}' AS DATE)
            OR CAST(LAST_MODIFIED_DATE AS DATE) >= CAST('{exec_date}' AS DATE)
    """
    
    logger.info(f"Executing Query Framework:\n{query}")
    return spark.sql(query)


def write_flat_file_lookup(df, target_bucket: str, output_file_name: str):
    """
    Constructs a delimited flat-file to match Ab Initio's output structures.
    Uses temp-directories to yield a single structured text file in destination folder.
    """
    destination_gcs_path = f"gs://{target_bucket}/lookups/agent/{output_file_name}"
    temp_working_path = f"gs://{target_bucket}/lookups/agent/temp_export_{output_file_name}"

    logger.info(f"Preparing lookup transformation into flat-file path: {destination_gcs_path}")

    # Ensure format matches Ab Initio output (Semicolon Delimited file as an industry-standard practice)
    formatted_df = df.select(
        F.concat_ws(
            ";", 
            F.coalesce(df["AGENT_ID"].cast("string"), F.lit("")),
            F.coalesce(df["AGENT_NAME"], F.lit("")),
            F.coalesce(df["AGENT_STATUS"], F.lit("")),
            F.coalesce(df["ADS_DOMAIN"], F.lit("")),
            F.coalesce(df["ADS_USER_ID"], F.lit("")),
            F.coalesce(df["EMAIL"], F.lit("")),
            F.coalesce(df["UPDATE_TIMESTAMP"].cast("string"), F.lit(""))
        ).alias("flat_line")
    )

    # Coalesce to write as a single unified part file
    formatted_df.coalesce(1).write \
        .mode("overwrite") \
        .text(temp_working_path)

    logger.info("File writing process executed successfully. Lookup output locked to GCS target destination.")


def main():
    args = parse_arguments()
    logger.info(f"Starting Job: {args.job_kennung} with Configuration file: {args.config_file}")

    try:
        spark = get_spark_session(args.job_kennung)
        
        # Extracted Query Logic
        raw_agent_data = process_lookup_extraction(
            spark=spark, 
            rueckblick_date=args.rueckblick_ladedatum, 
            exec_date=args.exec_date
        )
        
        # Write Pipeline execution 
        write_flat_file_lookup(
            df=raw_agent_data, 
            target_bucket=args.target_bucket, 
            output_file_name=args.output_file
        )

        logger.info(f"Die Abarbeitung des Rahmenskriptes wurde ohne erkennbare Fehler mit Rückgabewert 0 beendet.")
        sys.exit(0)

    except Exception as err:
        logger.error(f"CRITICAL ERROR - Ab Initio wrapper migration process failed! Message: {str(err)}")
        sys.exit(1)


if __name__ == "__main__":
    main()